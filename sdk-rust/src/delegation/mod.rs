//! Delegations: an account's owner (any Sui signer: a keypair, a multisig, a
//! passkey, zkLogin) grants an agent's Sui address part of the account's
//! rights by signing a plain-text personal message; the agent may grant part
//! of that to another address, and so on (at most [`MAX_CHAIN`] links).
//! Nothing is registered: an agent exchanges its [`Chain`] for a short-lived
//! API key (`POST /v1/token`, signed by the agent: see
//! [`auth::TokenSource`](crate::auth::TokenSource)), and revoking a
//! delegation's id ([`auth::revoke`](crate::auth::revoke)) refuses every
//! token below it.
//!
//! The message is what a wallet shows the owner:
//!
//! ```text
//! mirai.cloud delegation v1
//! audience: https://api.mainnet.mirai.cloud
//! account: 0x<owner>
//! issuer: 0x<owner | parent agent>
//! subject: 0x<agent>
//! parent: <delegation id | ->
//! scopes: stream:read rpc:read keys:write
//! networks: mainnet
//! origins: -
//! expires: 2026-12-06T00:00:00Z
//! limits: monthlyRequests=1000000 monthlyBytes=-
//! nonce: 3f9a…
//! ```
//!
//! Canonical form (what [`Delegation::message`] writes, and the only form
//! [`Delegation::parse`] accepts): lines joined by `\n`, no trailing newline;
//! addresses `0x` + 64 lowercase hex; scopes and networks in the order of
//! [`Scope::ALL`] and [`Network::ALL`], at least one each; origins sorted, or
//! `-`; expiry to the second in UTC, or `-` (never); limits as non-negative
//! integers, or `-` (none); nonce 16–64 lowercase hex.
//!
//! - **Id** ([`delegation_id`]): the first 10 bytes of BLAKE2b-256 of the
//!   message's UTF-8 bytes, as 16 base32 characters (like a key id): tokens
//!   carry it in their ancestors, and revocation names it.
//! - **Chain** ([`Chain::encode`]): `mcd1_` + base64url (no padding) of BCS
//!   `vector<struct { message: vector<u8>, signature: vector<u8> }>`, root
//!   first; `signature` is the serialized Sui signature's bytes.
//! - **Rules** ([`Chain::check`]): the root's issuer is the account and its
//!   parent `-`; each later issuer is the previous subject, its parent the
//!   previous id, and the previous delegation must include `keys:write`
//!   (here: "may delegate further"). Rights only narrow: scopes, networks
//!   and origins within the previous, expiry no later, limits no looser.
//!   Signatures are verified by the server.
//!
//! ```no_run
//! use miraicloud::delegation::{delegate, DelegateRequest, KeypairSigner, Scope, Signer};
//! # async fn run() -> Result<(), miraicloud::delegation::Error> {
//! let owner = KeypairSigner::from_suiprivkey(&std::env::var("OWNER_KEY").unwrap())?;
//! let agent = KeypairSigner::generate_ed25519();
//! let chain = delegate(
//!     &owner,
//!     &DelegateRequest::to(agent.address()).scopes([Scope::StreamRead]),
//!     None,
//!     None,
//! )
//! .await?;
//! println!("{chain}"); // mcd1_…: give it to the agent
//! # Ok(()) }
//! ```

mod signer;
mod time;

use std::fmt;
use std::str::FromStr;

use base64::Engine;
use blake2::digest::consts::U32;
use blake2::{Blake2b, Digest};

#[cfg(feature = "keypair")]
pub use signer::KeypairSigner;
pub use signer::{SignFuture, Signer, SignerError};

/// The first line of every delegation message.
pub const DELEGATION_HEADER: &str = "mirai.cloud delegation v1";
/// The text form of a [`Chain`] starts with this.
pub const CHAIN_PREFIX: &str = "mcd1_";
/// At most this many delegations in a chain (the account's family depth).
pub const MAX_CHAIN: usize = 8;
/// A chain longer than this (as text) is refused before decoding.
pub const MAX_CHAIN_LENGTH: usize = 16_384;
/// Delegations expire after this by default (never later than their parent), in ms.
pub const DEFAULT_DELEGATION_LIFETIME_MS: i64 = 30 * 24 * 3_600_000;
/// The default audience: the Mainnet API.
pub const DEFAULT_AUDIENCE: &str = crate::DEFAULT_ENDPOINT;

/// Why a delegation, a chain or a delegation request is refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// A message that is not a delegation in canonical form.
    #[error("invalid delegation: {0}")]
    Message(String),
    /// Chain text that does not decode.
    #[error("invalid delegation chain: {0}")]
    Encoding(String),
    /// A chain with no or too many links.
    #[error("a chain has 1 to {MAX_CHAIN} delegations, not {0}")]
    Length(usize),
    /// Link `index` (1-based) is not a valid delegation.
    #[error("delegation {index}: {reason}")]
    InvalidLink { index: usize, reason: String },
    /// Link `index` is for another API than the one expected.
    #[error("delegation {index}: for {found}, not {expected}")]
    Audience {
        index: usize,
        found: String,
        expected: String,
    },
    /// Link `index` does not follow from the previous one (issuer, parent,
    /// account, `keys:write`).
    #[error("delegation {index}: {reason}")]
    Broken { index: usize, reason: String },
    /// Link `index` grants more than the previous one.
    #[error("delegation {index}: {reason}")]
    Wider { index: usize, reason: String },
    /// Link `index` has expired.
    #[error("delegation {index} expired at {}", time::iso(*expires_at))]
    Expired { index: usize, expires_at: i64 },
    /// A delegation request that cannot be signed.
    #[error("cannot delegate: {0}")]
    Request(String),
    #[error(transparent)]
    Signer(#[from] SignerError),
}

macro_rules! names {
    ($(#[$meta:meta])* $name:ident { $($variant:ident = $text:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name {
            $(#[doc = concat!("`", $text, "`")] $variant),+
        }

        impl $name {
            /// Every value, in canonical order.
            pub const ALL: &[$name] = &[$($name::$variant),+];

            /// The wire name.
            pub fn as_str(self) -> &'static str {
                match self {
                    $($name::$variant => $text),+
                }
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(s: &str) -> Result<Self, String> {
                match s {
                    $($text => Ok($name::$variant),)+
                    _ => Err(format!(
                        "{s:?}: one of {}",
                        [$($text),+].join(" ")
                    )),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let s = <std::borrow::Cow<'de, str>>::deserialize(d)?;
                s.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}

names!(
    /// An API scope.
    Scope {
        StreamRead = "stream:read",
        RpcRead = "rpc:read",
        RpcWrite = "rpc:write",
        KeysRead = "keys:read",
        KeysWrite = "keys:write",
        UsageRead = "usage:read",
    }
);

names!(
    /// A Sui network.
    Network {
        Mainnet = "mainnet",
        Testnet = "testnet",
    }
);

/// Monthly limits; `None`: none.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Limits {
    pub monthly_requests: Option<u64>,
    pub monthly_bytes: Option<u64>,
}

/// What a delegation grants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rights {
    pub scopes: Vec<Scope>,
    pub networks: Vec<Network>,
    /// Allowed browser origins (`https://app.example.com`); empty: any.
    pub origins: Vec<String>,
    /// Unix ms (to the second in a message); `None`: never.
    pub expires_at: Option<i64>,
    pub limits: Limits,
}

impl Rights {
    /// What an account's owner holds: every scope but `keys:write`, every
    /// network, no expiry, no limits (the defaults of a root delegation).
    pub fn owner() -> Self {
        Rights {
            scopes: Scope::ALL
                .iter()
                .copied()
                .filter(|&s| s != Scope::KeysWrite)
                .collect(),
            networks: Network::ALL.to_vec(),
            origins: Vec::new(),
            expires_at: None,
            limits: Limits::default(),
        }
    }

    /// Why `self` (a child's rights) is not within `parent`'s, if it isn't.
    pub fn attenuation_error(&self, parent: &Rights) -> Option<String> {
        let join = |v: &mut dyn Iterator<Item = &str>| v.collect::<Vec<_>>().join(" ");
        if !subset(&self.scopes, &parent.scopes) {
            let p = join(&mut parent.scopes.iter().map(|s| s.as_str()));
            return Some(format!("scopes must be within {p}"));
        }
        if !subset(&self.networks, &parent.networks) {
            let p = join(&mut parent.networks.iter().map(|s| s.as_str()));
            return Some(format!("networks must be within {p}"));
        }
        if !parent.origins.is_empty()
            && (self.origins.is_empty() || !subset(&self.origins, &parent.origins))
        {
            let p = join(&mut parent.origins.iter().map(String::as_str));
            return Some(format!("origins must be within {p}"));
        }
        if let Some(p) = parent.expires_at
            && self.expires_at.is_none_or(|c| c > p)
        {
            return Some(format!("expiry cannot be later than {}", time::iso(p)));
        }
        for (name, p, c) in [
            (
                "monthlyRequests",
                parent.limits.monthly_requests,
                self.limits.monthly_requests,
            ),
            (
                "monthlyBytes",
                parent.limits.monthly_bytes,
                self.limits.monthly_bytes,
            ),
        ] {
            if let Some(p) = p
                && c.is_none_or(|c| c > p)
            {
                return Some(format!("{name} cannot exceed {p}"));
            }
        }
        None
    }
}

fn subset<T: PartialEq>(child: &[T], parent: &[T]) -> bool {
    child.iter().all(|x| parent.contains(x))
}

/// One delegation (its message's fields).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delegation {
    /// The API's origin, e.g. `https://api.mainnet.mirai.cloud`.
    pub audience: String,
    pub account: String,
    pub issuer: String,
    pub subject: String,
    /// The previous delegation's id; `None` for the root.
    pub parent: Option<String>,
    pub rights: Rights,
    pub nonce: String,
}

fn is_hex_lower(s: &str) -> bool {
    s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// JavaScript's `\s` (the reference implementation's regular expressions).
fn is_js_space(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n' | '\u{b}' | '\u{c}' | '\r' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

fn is_origin(s: &str) -> bool {
    let rest = s
        .strip_prefix("https://")
        .or_else(|| s.strip_prefix("http://"));
    rest.is_some_and(|r| !r.is_empty() && !r.contains(|c| c == '/' || is_js_space(c)))
}

fn is_address(s: &str) -> bool {
    s.len() == 66 && s.starts_with("0x") && is_hex_lower(&s[2..])
}

fn is_id(s: &str) -> bool {
    s.len() == 16 && s.bytes().all(|b| matches!(b, b'a'..=b'z' | b'2'..=b'7'))
}

fn is_nonce(s: &str) -> bool {
    (16..=64).contains(&s.len()) && is_hex_lower(s)
}

/// A limit's value: `-` (none), or a non-negative safe integer without
/// leading zeros.
fn count(raw: &str) -> Option<Option<u64>> {
    if raw == "-" {
        return Some(None);
    }
    let digits = raw.bytes().all(|b| b.is_ascii_digit());
    if !digits || raw.is_empty() || raw.len() > 16 || (raw.len() > 1 && raw.starts_with('0')) {
        return None;
    }
    let n: u64 = raw.parse().ok()?;
    // At most `Number.MAX_SAFE_INTEGER`.
    (n < (1 << 53)).then_some(Some(n))
}

/// A Sui address in its canonical form (`0x` + 64 lowercase hex), if valid
/// (`0x` and 1 to 64 hex digits, any case).
pub fn parse_address(raw: &str) -> Option<String> {
    let lower = raw.trim_matches(is_js_space).to_ascii_lowercase();
    let hex = lower.strip_prefix("0x")?;
    if hex.is_empty() || hex.len() > 64 || !is_hex_lower(hex) {
        return None;
    }
    Some(format!("0x{hex:0>64}"))
}

impl Delegation {
    /// The delegation's message, in canonical form.
    pub fn message(&self) -> String {
        let r = &self.rights;
        let list = |names: Vec<&str>| names.join(" ");
        let scopes = list(
            Scope::ALL
                .iter()
                .filter(|s| r.scopes.contains(s))
                .map(|s| s.as_str())
                .collect(),
        );
        let networks = list(
            Network::ALL
                .iter()
                .filter(|n| r.networks.contains(n))
                .map(|n| n.as_str())
                .collect(),
        );
        let origins = if r.origins.is_empty() {
            "-".to_owned()
        } else {
            let mut o: Vec<&str> = r.origins.iter().map(String::as_str).collect();
            // JavaScript's default sort: by UTF-16 code units.
            o.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
            o.dedup();
            o.join(" ")
        };
        let limit = |n: Option<u64>| n.map_or_else(|| "-".to_owned(), |n| n.to_string());
        [
            DELEGATION_HEADER.to_owned(),
            format!("audience: {}", self.audience),
            format!("account: {}", self.account),
            format!("issuer: {}", self.issuer),
            format!("subject: {}", self.subject),
            format!("parent: {}", self.parent.as_deref().unwrap_or("-")),
            format!("scopes: {scopes}"),
            format!("networks: {networks}"),
            format!("origins: {origins}"),
            format!(
                "expires: {}",
                r.expires_at.map_or_else(|| "-".to_owned(), time::iso)
            ),
            format!(
                "limits: monthlyRequests={} monthlyBytes={}",
                limit(r.limits.monthly_requests),
                limit(r.limits.monthly_bytes)
            ),
            format!("nonce: {}", self.nonce),
        ]
        .join("\n")
    }

    /// The delegation's id ([`delegation_id`] of its message).
    pub fn id(&self) -> String {
        delegation_id(&self.message())
    }

    /// A delegation from its message; the message must be in canonical form.
    pub fn parse(text: &str) -> Result<Self, Error> {
        let fail = |m: &str| Err(Error::Message(m.to_owned()));
        let lines: Vec<&str> = text.split('\n').collect();
        if lines.len() != 12 || lines[0] != DELEGATION_HEADER {
            return fail(&format!("not a {DELEGATION_HEADER} message"));
        }
        const NAMES: [&str; 11] = [
            "audience", "account", "issuer", "subject", "parent", "scopes", "networks", "origins",
            "expires", "limits", "nonce",
        ];
        let mut fields = [""; 11];
        for (i, name) in NAMES.iter().enumerate() {
            match lines[i + 1]
                .strip_prefix(name)
                .and_then(|l| l.strip_prefix(": "))
            {
                Some(v) => fields[i] = v,
                None => return fail("a line is missing or out of order"),
            }
        }
        let [
            audience,
            account,
            issuer,
            subject,
            parent,
            scopes,
            networks,
            origins,
            expires,
            limits,
            nonce,
        ] = fields;
        if !is_origin(audience) {
            return fail("audience: an origin, e.g. https://api.mainnet.mirai.cloud");
        }
        for (name, a) in [
            ("account", account),
            ("issuer", issuer),
            ("subject", subject),
        ] {
            if !is_address(a) {
                return fail(&format!("{name}: 0x and 64 lowercase hex digits"));
            }
        }
        if parent != "-" && !is_id(parent) {
            return fail("parent: a delegation id or -");
        }
        let Ok(scope_list) = scopes
            .split(' ')
            .map(Scope::from_str)
            .collect::<Result<Vec<_>, _>>()
        else {
            let all: Vec<_> = Scope::ALL.iter().map(|s| s.as_str()).collect();
            return fail(&format!("scopes: from {}", all.join(" ")));
        };
        let Ok(network_list) = networks
            .split(' ')
            .map(Network::from_str)
            .collect::<Result<Vec<_>, _>>()
        else {
            let all: Vec<_> = Network::ALL.iter().map(|s| s.as_str()).collect();
            return fail(&format!("networks: from {}", all.join(" ")));
        };
        let origin_list: Vec<String> = if origins == "-" {
            Vec::new()
        } else {
            origins.split(' ').map(str::to_owned).collect()
        };
        if !origin_list.iter().all(|o| is_origin(o)) {
            return fail("origins: like https://app.example.com, or -");
        }
        if origin_list.len() > 8 {
            return fail("origins: at most 8");
        }
        let expires_at = if expires == "-" {
            None
        } else {
            match time::parse_iso(expires) {
                Err(time::ParseError::Format) => {
                    return fail("expires: YYYY-MM-DDTHH:MM:SSZ or -");
                }
                Err(time::ParseError::Date) => return fail("expires: not a date"),
                Ok(ms) => Some(ms),
            }
        };
        let parsed_limits = limits
            .strip_prefix("monthlyRequests=")
            .and_then(|l| l.split_once(" monthlyBytes="))
            .and_then(|(r, b)| Some((count(r)?, count(b)?)));
        let Some((monthly_requests, monthly_bytes)) = parsed_limits else {
            return fail("limits: monthlyRequests=<n|-> monthlyBytes=<n|->");
        };
        if !is_nonce(nonce) {
            return fail("nonce: 16 to 64 lowercase hex digits");
        }
        let d = Delegation {
            audience: audience.to_owned(),
            account: account.to_owned(),
            issuer: issuer.to_owned(),
            subject: subject.to_owned(),
            parent: (parent != "-").then(|| parent.to_owned()),
            rights: Rights {
                scopes: scope_list,
                networks: network_list,
                origins: origin_list,
                expires_at,
                limits: Limits {
                    monthly_requests,
                    monthly_bytes,
                },
            },
            nonce: nonce.to_owned(),
        };
        if d.rights.scopes.is_empty() || scopes.is_empty() || networks.is_empty() {
            return fail("a delegation needs at least one scope and one network");
        }
        // One text per delegation: anything else (order, duplicates,
        // spacing) would change its id.
        if d.message() != text {
            return fail(
                "not in canonical form (order, duplicates, spacing or a time with milliseconds)",
            );
        }
        Ok(d)
    }
}

impl fmt::Display for Delegation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message())
    }
}

impl FromStr for Delegation {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        Delegation::parse(s)
    }
}

const BASE32: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

fn base32(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 8 / 5 + 1);
    let mut buffer: u32 = 0;
    let mut bits = 0;
    for &b in bytes {
        buffer = ((buffer << 8) | u32::from(b)) & 0xfff;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(BASE32[((buffer >> bits) & 31) as usize] as char);
        }
    }
    out
}

/// A delegation's id: BLAKE2b-256 of its message, the first 10 bytes, in
/// base32 (16 characters).
pub fn delegation_id(message: &str) -> String {
    let digest = Blake2b::<U32>::digest(message.as_bytes());
    base32(&digest[..10])
}

/// A delegation and its issuer's signature.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedDelegation {
    /// The delegation's message, in canonical form.
    pub message: String,
    /// The serialized Sui signature, standard base64.
    pub signature: String,
}

impl SignedDelegation {
    /// The delegation's id.
    pub fn id(&self) -> String {
        delegation_id(&self.message)
    }

    /// The delegation, parsed.
    pub fn delegation(&self) -> Result<Delegation, Error> {
        Delegation::parse(&self.message)
    }
}

/// One link of a checked chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Link {
    pub delegation: Delegation,
    pub id: String,
    pub message: String,
    pub signature: String,
}

/// A chain that passed [`Chain::check`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckedChain {
    pub links: Vec<Link>,
    pub account: String,
    /// The last delegation (its subject is the agent the chain is for).
    pub leaf: Delegation,
    /// The delegations' ids, nearest first (as an exchanged token's ancestors).
    pub ancestors: Vec<String>,
}

/// Delegations, root first. Its text form ([`Display`](fmt::Display),
/// [`FromStr`]) is [`Chain::encode`] / [`Chain::decode`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Chain(pub Vec<SignedDelegation>);

/// Forgiving base64 (like `atob`): ASCII whitespace ignored, padding
/// optional, trailing bits ignored.
fn from_base64(text: &str) -> Option<Vec<u8>> {
    use base64::alphabet::STANDARD;
    use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
    const FORGIVING: GeneralPurpose = GeneralPurpose::new(
        &STANDARD,
        GeneralPurposeConfig::new()
            .with_decode_padding_mode(DecodePaddingMode::Indifferent)
            .with_decode_allow_trailing_bits(true),
    );
    let compact: String = text
        .chars()
        .filter(|c| !matches!(c, '\t' | '\n' | '\u{c}' | '\r' | ' '))
        .collect();
    FORGIVING.decode(compact).ok()
}

fn from_base64url(text: &str) -> Option<Vec<u8>> {
    use base64::alphabet::URL_SAFE;
    use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
    const URL: GeneralPurpose = GeneralPurpose::new(
        &URL_SAFE,
        GeneralPurposeConfig::new()
            .with_decode_padding_mode(DecodePaddingMode::RequireNone)
            .with_decode_allow_trailing_bits(true),
    );
    if !text
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return None;
    }
    URL.decode(text).ok()
}

fn uleb(mut n: usize, out: &mut Vec<u8>) {
    loop {
        let mut b = (n & 0x7f) as u8;
        n >>= 7;
        if n != 0 {
            b |= 0x80;
        }
        out.push(b);
        if n == 0 {
            break;
        }
    }
}

impl Chain {
    pub fn new(links: Vec<SignedDelegation>) -> Self {
        Chain(links)
    }

    pub fn links(&self) -> &[SignedDelegation] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The chain as text: `mcd1_` + base64url(BCS). A signature that is not
    /// base64 is encoded as empty.
    pub fn encode(&self) -> String {
        let mut bytes = Vec::new();
        uleb(self.0.len(), &mut bytes);
        for link in &self.0 {
            let m = link.message.as_bytes();
            let s = from_base64(&link.signature).unwrap_or_default();
            uleb(m.len(), &mut bytes);
            bytes.extend_from_slice(m);
            uleb(s.len(), &mut bytes);
            bytes.extend_from_slice(&s);
        }
        let mut out = String::from(CHAIN_PREFIX);
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode_string(&bytes, &mut out);
        out
    }

    /// A chain from its text (shape only: see [`Chain::check`]).
    pub fn decode(text: &str) -> Result<Self, Error> {
        let fail = |m: String| Err(Error::Encoding(m));
        let t = text.trim_matches(is_js_space);
        let Some(body) = t
            .strip_prefix(CHAIN_PREFIX)
            .filter(|_| t.encode_utf16().count() <= MAX_CHAIN_LENGTH)
        else {
            return fail(format!("a delegation chain starts with {CHAIN_PREFIX}"));
        };
        let Some(bytes) = from_base64url(body) else {
            return fail("not base64url".into());
        };
        let mut at = 0usize;
        // ULEB128, at most 5 bytes, truncated to 32 bits (as the reference does).
        let read = |at: &mut usize| -> Option<usize> {
            let mut n: u32 = 0;
            for shift in (0..32).step_by(7) {
                let b = *bytes.get(*at)?;
                *at += 1;
                n |= u32::from(b & 0x7f).wrapping_shl(shift);
                if b & 0x80 == 0 {
                    return Some(n as usize);
                }
            }
            None
        };
        let vec = |at: &mut usize| -> Option<&[u8]> {
            let n = read(at)?;
            let end = at.checked_add(n).filter(|&e| e <= bytes.len())?;
            let v = &bytes[*at..end];
            *at = end;
            Some(v)
        };
        let n = match read(&mut at) {
            Some(n) if (1..=MAX_CHAIN).contains(&n) => n,
            _ => return fail(format!("a chain has 1 to {MAX_CHAIN} delegations")),
        };
        let mut links = Vec::with_capacity(n);
        for _ in 0..n {
            let (Some(m), Some(s)) = (vec(&mut at), vec(&mut at)) else {
                return fail("truncated chain".into());
            };
            let Ok(message) = std::str::from_utf8(m) else {
                return fail("a message is not UTF-8".into());
            };
            links.push(SignedDelegation {
                message: message.to_owned(),
                signature: base64::engine::general_purpose::STANDARD.encode(s),
            });
        }
        if at != bytes.len() {
            return fail("trailing bytes after the chain".into());
        }
        Ok(Chain(links))
    }

    /// A chain's shape and rights: links, issuers and parents, attenuation,
    /// and expiry at `now_ms` (unix ms); `audience` (if given) must be every
    /// delegation's. The signatures are not checked here (the server does).
    pub fn check(&self, now_ms: i64, audience: Option<&str>) -> Result<CheckedChain, Error> {
        if self.0.is_empty() || self.0.len() > MAX_CHAIN {
            return Err(Error::Length(self.0.len()));
        }
        let mut links: Vec<Link> = Vec::with_capacity(self.0.len());
        for (i, link) in self.0.iter().enumerate() {
            let index = i + 1;
            let d = Delegation::parse(&link.message).map_err(|e| Error::InvalidLink {
                index,
                reason: match e {
                    Error::Message(m) => m,
                    other => other.to_string(),
                },
            })?;
            let id = delegation_id(&link.message);
            if let Some(expected) = audience
                && d.audience != expected
            {
                return Err(Error::Audience {
                    index,
                    found: d.audience,
                    expected: expected.to_owned(),
                });
            }
            let broken = |reason: String| Err(Error::Broken { index, reason });
            match links.last() {
                None => {
                    if d.issuer != d.account {
                        return broken("the first delegation must be issued by the account".into());
                    }
                    if d.parent.is_some() {
                        return broken("the first delegation has no parent".into());
                    }
                }
                Some(prev) => {
                    let p = &prev.delegation;
                    if d.account != p.account || d.audience != p.audience {
                        return broken("another account or audience".into());
                    }
                    if d.issuer != p.subject {
                        return broken(format!(
                            "issued by {}, but delegation {i} was to {}",
                            d.issuer, p.subject
                        ));
                    }
                    if d.parent.as_deref() != Some(prev.id.as_str()) {
                        return broken(format!("its parent is not delegation {i}"));
                    }
                    if !p.rights.scopes.contains(&Scope::KeysWrite) {
                        return broken(format!(
                            "delegation {i} does not allow delegating further (keys:write)"
                        ));
                    }
                    if let Some(reason) = d.rights.attenuation_error(&p.rights) {
                        return Err(Error::Wider { index, reason });
                    }
                }
            }
            if let Some(expires_at) = d.rights.expires_at
                && expires_at <= now_ms
            {
                return Err(Error::Expired { index, expires_at });
            }
            links.push(Link {
                delegation: d,
                id,
                message: link.message.clone(),
                signature: link.signature.clone(),
            });
        }
        let account = links[0].delegation.account.clone();
        let leaf = links[links.len() - 1].delegation.clone();
        let ancestors = links.iter().rev().map(|l| l.id.clone()).collect();
        Ok(CheckedChain {
            links,
            account,
            leaf,
            ancestors,
        })
    }

    /// [`check`](Chain::check) at the current time.
    pub fn check_now(&self, audience: Option<&str>) -> Result<CheckedChain, Error> {
        self.check(time::now_ms(), audience)
    }

    /// The chain's delegations, decoded, with their ids (for display);
    /// links that do not parse are skipped.
    pub fn describe(&self) -> Vec<(String, Delegation)> {
        self.0
            .iter()
            .filter_map(|l| Some((l.id(), l.delegation().ok()?)))
            .collect()
    }
}

impl fmt::Display for Chain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.encode())
    }
}

impl FromStr for Chain {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        Chain::decode(s)
    }
}

impl From<Vec<SignedDelegation>> for Chain {
    fn from(links: Vec<SignedDelegation>) -> Self {
        Chain(links)
    }
}

/// When a delegation expires.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Expiry {
    /// The issuer's expiry, at most [`DEFAULT_DELEGATION_LIFETIME_MS`] from now.
    #[default]
    Default,
    /// Never (only if the issuer's delegation never expires).
    Never,
    /// At this unix ms (rounded down to the second).
    At(i64),
}

/// What to delegate; anything left out is the issuer's (the owner's: every
/// scope but `keys:write`, every network), the expiry min(the issuer's,
/// 30 days).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DelegateRequest {
    /// The agent's Sui address.
    pub to: String,
    pub scopes: Option<Vec<Scope>>,
    pub networks: Option<Vec<Network>>,
    pub origins: Option<Vec<String>>,
    pub expires: Expiry,
    /// Each limit left `None` is the issuer's.
    pub limits: Limits,
}

impl DelegateRequest {
    /// A delegation to the agent at `address`, with the issuer's rights.
    pub fn to(address: impl Into<String>) -> Self {
        DelegateRequest {
            to: address.into(),
            ..Default::default()
        }
    }

    pub fn scopes(mut self, scopes: impl IntoIterator<Item = Scope>) -> Self {
        self.scopes = Some(scopes.into_iter().collect());
        self
    }

    pub fn networks(mut self, networks: impl IntoIterator<Item = Network>) -> Self {
        self.networks = Some(networks.into_iter().collect());
        self
    }

    pub fn origins<S: Into<String>>(mut self, origins: impl IntoIterator<Item = S>) -> Self {
        self.origins = Some(origins.into_iter().map(Into::into).collect());
        self
    }

    /// Expire at this unix ms (rounded down to the second).
    pub fn expires_at(mut self, unix_ms: i64) -> Self {
        self.expires = Expiry::At(unix_ms);
        self
    }

    /// Expire this long from now.
    pub fn expires_in(self, d: std::time::Duration) -> Self {
        let ms = i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
        self.expires_at(time::now_ms().saturating_add(ms))
    }

    /// Never expire (only if the issuer's delegation never expires).
    pub fn never_expires(mut self) -> Self {
        self.expires = Expiry::Never;
        self
    }

    pub fn monthly_requests(mut self, n: u64) -> Self {
        self.limits.monthly_requests = Some(n);
        self
    }

    pub fn monthly_bytes(mut self, n: u64) -> Self {
        self.limits.monthly_bytes = Some(n);
        self
    }
}

/// A fresh nonce: 16 random bytes, hex.
pub(crate) fn random_hex16() -> String {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes).expect("the OS random number generator");
    hex(&bytes)
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    use fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Sign a delegation to `request.to` with `signer`: as the account's owner,
/// or, with `parent`, as the agent the parent chain is for. `audience`: the
/// API's origin (default [`DEFAULT_AUDIENCE`]). Returns the new chain (the
/// parent's links, then the new one), checked as the server will.
pub async fn delegate<S: Signer + ?Sized>(
    signer: &S,
    request: &DelegateRequest,
    parent: Option<&Chain>,
    audience: Option<&str>,
) -> Result<Chain, Error> {
    delegate_at(signer, request, parent, audience, time::now_ms()).await
}

/// [`delegate`] at `now_ms` (unix ms) instead of the current time.
pub async fn delegate_at<S: Signer + ?Sized>(
    signer: &S,
    request: &DelegateRequest,
    parent: Option<&Chain>,
    audience: Option<&str>,
    now_ms: i64,
) -> Result<Chain, Error> {
    let fail = |m: String| Err(Error::Request(m));
    let Some(to) = parse_address(&request.to) else {
        return fail(format!("not a Sui address: {}", request.to));
    };
    let address = signer.address();
    let audience = audience.unwrap_or(DEFAULT_AUDIENCE).trim_end_matches('/');
    let (account, parent_id, rights, parent_rights) = match parent {
        None => (address.clone(), None, Rights::owner(), None),
        Some(chain) => {
            let checked = chain
                .check(now_ms, Some(audience))
                .map_err(|e| Error::Request(e.to_string()))?;
            let leaf = checked.leaf;
            if leaf.subject != address {
                return fail(format!(
                    "the parent chain is for {}, not this signer ({address})",
                    leaf.subject
                ));
            }
            if !leaf.rights.scopes.contains(&Scope::KeysWrite) {
                return fail(
                    "the parent delegation does not allow delegating further (keys:write)".into(),
                );
            }
            (
                checked.account,
                checked.ancestors.into_iter().next(),
                leaf.rights.clone(),
                Some(leaf.rights),
            )
        }
    };
    let default_expiry = rights
        .expires_at
        .unwrap_or(i64::MAX)
        .min(now_ms.saturating_add(DEFAULT_DELEGATION_LIFETIME_MS));
    let expires_at = match request.expires {
        Expiry::Default => Some(default_expiry),
        Expiry::Never => None,
        Expiry::At(ms) => Some(ms),
    };
    let d = Delegation {
        audience: audience.to_owned(),
        account,
        issuer: address,
        subject: to,
        parent: parent_id,
        rights: Rights {
            scopes: request.scopes.clone().unwrap_or(rights.scopes),
            networks: request.networks.clone().unwrap_or(rights.networks),
            origins: request
                .origins
                .as_ref()
                .unwrap_or(&rights.origins)
                .iter()
                .map(|o| o.trim_end_matches('/').to_owned())
                .collect(),
            expires_at: expires_at.map(|ms| ms.div_euclid(1000) * 1000),
            limits: Limits {
                monthly_requests: request
                    .limits
                    .monthly_requests
                    .or(rights.limits.monthly_requests),
                monthly_bytes: request.limits.monthly_bytes.or(rights.limits.monthly_bytes),
            },
        },
        nonce: random_hex16(),
    };
    if d.rights.scopes.is_empty() || d.rights.networks.is_empty() {
        return fail("a delegation needs at least one scope and one network".into());
    }
    if d.rights.expires_at.is_some_and(|e| e <= now_ms) {
        return fail("expiry must be in the future".into());
    }
    if let Some(p) = &parent_rights
        && let Some(reason) = d.rights.attenuation_error(p)
    {
        return fail(reason);
    }
    let message = d.message();
    if let Err(e) = Delegation::parse(&message) {
        return fail(match e {
            Error::Message(m) => m,
            other => other.to_string(),
        });
    }
    let signature = signer.sign_personal_message(message.as_bytes()).await?;
    let mut links = parent.map(|c| c.0.clone()).unwrap_or_default();
    links.push(SignedDelegation { message, signature });
    Ok(Chain(links))
}

pub(crate) use time::now_ms;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base32_and_ids() {
        assert_eq!(base32(&[0; 10]), "aaaaaaaaaaaaaaaa");
        assert_eq!(base32(&[0xff; 10]), "7777777777777777");
        assert_eq!(delegation_id("").len(), 16);
    }

    #[test]
    fn addresses() {
        assert_eq!(
            parse_address(" 0x2 ").as_deref(),
            Some("0x0000000000000000000000000000000000000000000000000000000000000002")
        );
        assert_eq!(parse_address("0XAB").map(|a| a.len()), Some(66));
        assert_eq!(parse_address("0x"), None);
        assert_eq!(parse_address("2"), None);
        assert_eq!(parse_address(&format!("0x{}", "1".repeat(65))), None);
    }

    #[test]
    fn counts() {
        assert_eq!(count("-"), Some(None));
        assert_eq!(count("0"), Some(Some(0)));
        assert_eq!(count("9007199254740991"), Some(Some(9_007_199_254_740_991)));
        assert_eq!(count("9007199254740992"), None);
        assert_eq!(count("01"), None);
        assert_eq!(count(""), None);
        assert_eq!(count("+1"), None);
    }
}
