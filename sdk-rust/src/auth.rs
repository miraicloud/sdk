//! Authentication: an API key, or a delegated [`TokenSource`] that
//! exchanges a delegation [`Chain`] for short-lived API keys.
//!
//! - [`Auth::ApiKey`]: a `mc1_…` key (from the `miraicloud` CLI), sent as
//!   `authorization: Bearer <key>`.
//! - [`Auth::Delegated`]: the agent holds a chain (`mcd1_…`) and its own Sui
//!   key. [`TokenSource`] exchanges the chain at `POST /v1/token` (a signed
//!   request: the agent proves control of its address), caches the token,
//!   and renews it before it expires; concurrent callers share one exchange.
//!   An open stream renews its token while it streams, then reconnects with
//!   the fresh one (resuming from its cursor: no gaps, no duplicates).
//!
//! ```no_run
//! use miraicloud::auth::{Auth, TokenOptions, TokenSource};
//! use miraicloud::delegation::{Chain, KeypairSigner};
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let chain: Chain = std::env::var("MIRAICLOUD_CHAIN")?.parse()?;
//! let agent = KeypairSigner::from_suiprivkey(&std::env::var("AGENT_KEY")?)?;
//! let tokens = TokenSource::new(
//!     miraicloud::DEFAULT_ENDPOINT,
//!     chain,
//!     agent,
//!     TokenOptions::default(),
//! )?;
//! let bearer = tokens.current().await?; // mc1_…, valid for a while
//! let auth = Auth::from(tokens); // for streams::Builder::auth
//! # Ok(()) }
//! ```
//!
//! Signed requests ([`signed_headers`]): the management API authenticates an
//! address's owner by a Sui personal-message signature over
//!
//! ```text
//! mirai.cloud request
//! audience: https://api.mainnet.mirai.cloud
//! address: 0x…
//! method: POST
//! path: /v1/token
//! body-sha256: <hex SHA-256 of the body>
//! timestamp: <unix ms>
//! nonce: <32 hex>
//! ```
//!
//! sent as the `x-miraicloud-{address,timestamp,nonce,signature}` headers.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use reqwest::StatusCode;
use sha2::{Digest, Sha256};
use tokio::time::Instant;

use crate::delegation::{self, Chain, Scope, Signer, SignerError, hex, now_ms, random_hex16};

/// Exchanged tokens live this long unless [`TokenOptions::expires_in`] says
/// otherwise (never longer than the chain).
pub const DEFAULT_TOKEN_LIFETIME: Duration = Duration::from_secs(3600);
/// The longest lifetime the exchange grants.
pub const MAX_TOKEN_LIFETIME: Duration = Duration::from_secs(86_400);
/// The token exchange's path.
pub const TOKEN_PATH: &str = "/v1/token";

/// The signed-request headers.
pub mod headers {
    pub const ADDRESS: &str = "x-miraicloud-address";
    pub const TIMESTAMP: &str = "x-miraicloud-timestamp";
    pub const NONCE: &str = "x-miraicloud-nonce";
    pub const SIGNATURE: &str = "x-miraicloud-signature";
}

/// Authentication failures.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error(transparent)]
    Delegation(#[from] delegation::Error),
    #[error(transparent)]
    Signer(#[from] SignerError),
    /// The API refused for good (`400 Invalid`, `401 Unauthorized`,
    /// `403 Forbidden`, …): `tag` and `message` are its JSON error's.
    #[error("refused: HTTP {status} {tag}: {message}")]
    Refused {
        status: u16,
        tag: String,
        message: String,
    },
    /// The API could not be reached or kept answering `429`/`5xx`.
    #[error("unavailable: {0}")]
    Unavailable(String),
}

impl Error {
    /// Whether trying again later may succeed (the API was unavailable).
    pub fn is_transient(&self) -> bool {
        matches!(self, Error::Unavailable(_))
    }
}

/// The message a signed request signs.
pub fn request_message(
    audience: &str,
    address: &str,
    method: &str,
    path: &str,
    body_sha256: &str,
    timestamp: i64,
    nonce: &str,
) -> String {
    [
        "mirai.cloud request".to_owned(),
        format!("audience: {audience}"),
        format!("address: {address}"),
        format!("method: {}", method.to_uppercase()),
        format!("path: {path}"),
        format!("body-sha256: {body_sha256}"),
        format!("timestamp: {timestamp}"),
        format!("nonce: {nonce}"),
    ]
    .join("\n")
}

/// Lowercase hex SHA-256 (`body-sha256`).
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// The signed-request headers for a request to `audience` (the API's
/// origin), signed by `signer`: `(name, value)` pairs. `path` includes the
/// query, if any.
pub async fn signed_headers<S: Signer + ?Sized>(
    signer: &S,
    audience: &str,
    method: &str,
    path: &str,
    body: &[u8],
) -> Result<Vec<(&'static str, String)>, SignerError> {
    let address = signer.address();
    let timestamp = now_ms();
    let nonce = random_hex16();
    let message = request_message(
        audience,
        &address,
        method,
        path,
        &sha256_hex(body),
        timestamp,
        &nonce,
    );
    let signature = signer.sign_personal_message(message.as_bytes()).await?;
    Ok(vec![
        (headers::ADDRESS, address),
        (headers::TIMESTAMP, timestamp.to_string()),
        (headers::NONCE, nonce),
        (headers::SIGNATURE, signature),
    ])
}

/// Between half and all of `d`, at random.
fn jittered(d: Duration) -> Duration {
    use std::hash::BuildHasher;
    let bits = std::collections::hash_map::RandomState::new().hash_one(std::time::Instant::now());
    let unit = (bits >> 11) as f64 / (1u64 << 53) as f64;
    d.mul_f64(0.5 + 0.5 * unit)
}

/// Signed `POST`s to the management API, retrying `429`/`5xx`.
struct Api {
    client: reqwest::Client,
    base: String,
    audience: String,
}

/// One successful response.
struct Reply {
    body: bytes::Bytes,
    /// The server's clock (its `Date` header), if it said.
    date: Option<SystemTime>,
}

enum Attempt {
    Retry(String, Option<Duration>),
    Fatal(Error),
}

impl Api {
    fn new(endpoint: &str, timeout: Duration) -> Result<Self, Error> {
        let base = endpoint.trim_end_matches('/').to_owned();
        let url = reqwest::Url::parse(&base)
            .map_err(|e| Error::Config(format!("endpoint {endpoint:?}: {e}")))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(Error::Config(format!("endpoint {endpoint:?}: not http(s)")));
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(timeout)
            .build()
            .map_err(|e| Error::Config(format!("http client: {e}")))?;
        Ok(Api {
            client,
            audience: url.origin().ascii_serialization(),
            base,
        })
    }

    async fn post_once<S: Signer + ?Sized>(
        &self,
        signer: &S,
        path: &str,
        body: &[u8],
    ) -> Result<Reply, Attempt> {
        let signed = signed_headers(signer, &self.audience, "POST", path, body)
            .await
            .map_err(|e| Attempt::Fatal(e.into()))?;
        let mut req = self
            .client
            .post(format!("{}{path}", self.base))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_vec());
        for (name, value) in signed {
            req = req.header(name, value);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| Attempt::Retry(format!("POST {path}: {e}"), None))?;
        let status = resp.status();
        let date = resp
            .headers()
            .get(reqwest::header::DATE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| httpdate::parse_http_date(v).ok());
        let retry_after = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        let body = resp
            .bytes()
            .await
            .map_err(|e| Attempt::Retry(format!("POST {path}: body: {e}"), None))?;
        if status.is_success() {
            return Ok(Reply { body, date });
        }
        #[derive(serde::Deserialize)]
        struct Refusal {
            #[serde(rename = "_tag")]
            tag: Option<String>,
            message: Option<String>,
        }
        let (tag, message) = match serde_json::from_slice::<Refusal>(&body) {
            Ok(r) => (
                r.tag.unwrap_or_else(|| format!("HTTP {}", status.as_u16())),
                r.message.unwrap_or_default(),
            ),
            Err(_) => {
                let text = String::from_utf8_lossy(&body);
                (
                    format!("HTTP {}", status.as_u16()),
                    text.chars().take(500).collect(),
                )
            }
        };
        let retryable = matches!(
            status,
            StatusCode::TOO_MANY_REQUESTS
                | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        );
        Err(if retryable {
            Attempt::Retry(
                format!("POST {path}: HTTP {} {tag}: {message}", status.as_u16()),
                retry_after,
            )
        } else {
            Attempt::Fatal(Error::Refused {
                status: status.as_u16(),
                tag,
                message,
            })
        })
    }

    /// `POST` with backoff (500 ms doubling to 8 s, at least `Retry-After`)
    /// for at most `attempts` tries.
    async fn post<S: Signer + ?Sized>(
        &self,
        signer: &S,
        path: &str,
        body: &[u8],
        attempts: u32,
    ) -> Result<Reply, Error> {
        let mut failures = 0u32;
        loop {
            match self.post_once(signer, path, body).await {
                Ok(reply) => return Ok(reply),
                Err(Attempt::Fatal(e)) => return Err(e),
                Err(Attempt::Retry(msg, retry_after)) => {
                    failures += 1;
                    if failures >= attempts.max(1) {
                        return Err(Error::Unavailable(msg));
                    }
                    let backoff = Duration::from_millis(500)
                        .saturating_mul(1 << (failures - 1).min(4))
                        .min(Duration::from_secs(8));
                    let wait = jittered(backoff).max(retry_after.unwrap_or_default());
                    tracing::warn!(?wait, "{msg}; retrying");
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }
}

/// How a [`TokenSource`] exchanges and renews.
#[derive(Clone, Debug)]
pub struct TokenOptions {
    /// The tokens' lifetime (default [`DEFAULT_TOKEN_LIFETIME`], at most
    /// [`MAX_TOKEN_LIFETIME`]; never beyond the chain's expiry).
    pub expires_in: Option<Duration>,
    /// Narrow the tokens' scopes (default: the chain's).
    pub scopes: Option<Vec<Scope>>,
    /// Renew when less than max(`refresh_margin`, `refresh_fraction` of the
    /// token's lifetime) remains (default 60 s and 10 %; at most half the
    /// lifetime).
    pub refresh_margin: Duration,
    pub refresh_fraction: f64,
    /// Tries per exchange on `429`/`5xx`/network errors (default 5).
    pub max_attempts: u32,
    /// Per request (default 10 s).
    pub timeout: Duration,
}

impl Default for TokenOptions {
    fn default() -> Self {
        TokenOptions {
            expires_in: None,
            scopes: None,
            refresh_margin: Duration::from_secs(60),
            refresh_fraction: 0.1,
            max_attempts: 5,
            timeout: Duration::from_secs(10),
        }
    }
}

/// An exchanged API key.
#[derive(Clone)]
pub struct Token {
    /// The key (`mc1_…`), sent as `authorization: Bearer <token>`.
    pub token: String,
    /// Its key id.
    pub id: String,
    /// Unix ms (the server's clock).
    pub expires_at: i64,
    /// The chain's delegation ids, nearest first.
    pub ancestors: Vec<String>,
    refresh_at: Instant,
    /// Not handed out after this (a little before `expires`).
    usable_until: Instant,
    expires: Instant,
}

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Token")
            .field("id", &self.id)
            .field("expires_at", &self.expires_at)
            .field("ancestors", &self.ancestors)
            .finish_non_exhaustive()
    }
}

impl Token {
    /// When the token source renews it (local clock).
    pub fn refresh_at(&self) -> Instant {
        self.refresh_at
    }

    /// When it expires (local clock, corrected for the server's).
    pub fn expires(&self) -> Instant {
        self.expires
    }
}

/// A token is not handed out this close to its expiry (at most a tenth of
/// its lifetime).
const MIN_VALIDITY: Duration = Duration::from_secs(5);
/// After a failed renewal (with the old token still valid), wait this long
/// before trying again.
const RENEW_RETRY: Duration = Duration::from_secs(5);

#[derive(Default)]
struct State {
    token: Option<Token>,
    /// No renewal attempt before this (after a failed one).
    retry_at: Option<Instant>,
}

struct Inner {
    api: Api,
    chain: Chain,
    body: Vec<u8>,
    signer: Arc<dyn Signer>,
    options: TokenOptions,
    /// Never held across an await: callers with a valid token don't wait
    /// on an exchange.
    state: std::sync::Mutex<State>,
    /// Held for each exchange: one at a time.
    exchanging: tokio::sync::Mutex<()>,
}

/// Exchanges a delegation chain for short-lived API keys, caches them and
/// renews them before they expire. Cheap to clone (clones share the cache).
#[derive(Clone)]
pub struct TokenSource {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for TokenSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenSource")
            .field("endpoint", &self.inner.api.base)
            .field("address", &self.inner.signer.address())
            .finish_non_exhaustive()
    }
}

impl TokenSource {
    /// `endpoint`: the API (e.g. [`DEFAULT_ENDPOINT`](crate::DEFAULT_ENDPOINT));
    /// its origin must be the chain's audience. `chain`: the delegations
    /// to `signer`'s address. Checks the chain (shape, audience, expiry,
    /// subject); exchanges nothing until the first [`current`](Self::current).
    pub fn new(
        endpoint: impl AsRef<str>,
        chain: Chain,
        signer: impl Signer + 'static,
        options: TokenOptions,
    ) -> Result<Self, Error> {
        let api = Api::new(endpoint.as_ref(), options.timeout)?;
        let checked = chain.check_now(Some(&api.audience))?;
        let address = signer.address();
        if checked.leaf.subject != address {
            return Err(Error::Config(format!(
                "the chain is for {}, not this signer ({address})",
                checked.leaf.subject
            )));
        }
        if let Some(d) = options.expires_in
            && (d < Duration::from_secs(1) || d > MAX_TOKEN_LIFETIME)
        {
            return Err(Error::Config(format!(
                "expires_in: 1 s to {} s",
                MAX_TOKEN_LIFETIME.as_secs()
            )));
        }
        if !(0.0..1.0).contains(&options.refresh_fraction) {
            return Err(Error::Config("refresh_fraction: in [0, 1)".into()));
        }
        #[derive(serde::Serialize)]
        struct Body<'a> {
            chain: String,
            #[serde(rename = "expiresIn", skip_serializing_if = "Option::is_none")]
            expires_in: Option<u64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            scopes: Option<&'a [Scope]>,
        }
        let body = serde_json::to_vec(&Body {
            chain: chain.encode(),
            expires_in: options.expires_in.map(|d| d.as_secs()),
            scopes: options.scopes.as_deref(),
        })
        .expect("serializable");
        Ok(TokenSource {
            inner: Arc::new(Inner {
                api,
                chain,
                body,
                signer: Arc::new(signer),
                options,
                state: Default::default(),
                exchanging: Default::default(),
            }),
        })
    }

    /// The API's base URL.
    pub fn endpoint(&self) -> &str {
        &self.inner.api.base
    }

    /// The chain exchanged.
    pub fn chain(&self) -> &Chain {
        &self.inner.chain
    }

    /// The agent's address (the signer's).
    pub fn address(&self) -> String {
        self.inner.signer.address()
    }

    /// A valid bearer token (`mc1_…`).
    pub async fn current(&self) -> Result<String, Error> {
        Ok(self.token().await?.token)
    }

    /// A valid token, exchanged if none is cached or the cached one is due
    /// for renewal. Concurrent callers share one exchange; while the cached
    /// token is valid, only the caller renewing it waits. If a renewal fails
    /// while the cached token is still valid, the cached token is returned
    /// (and renewal tried again a few seconds later).
    pub async fn token(&self) -> Result<Token, Error> {
        let now = Instant::now();
        let due = {
            let state = self.state();
            match &state.token {
                Some(t) if now < t.refresh_at => return Ok(t.clone()),
                Some(t) if now < t.usable_until => {
                    if state.retry_at.is_some_and(|r| now < r) {
                        return Ok(t.clone());
                    }
                    Some(t.clone())
                }
                _ => None,
            }
        };
        if let Some(cached) = due {
            // A renewal in progress: use the valid one meanwhile.
            let Ok(_exchanging) = self.inner.exchanging.try_lock() else {
                return Ok(cached);
            };
            return Ok(self.renew(&cached.token).await.unwrap_or(cached));
        }
        let _exchanging = self.inner.exchanging.lock().await;
        // Exchanged by the caller that held the lock.
        if let Some(t) = self.valid(None) {
            return Ok(t);
        }
        self.state().token = None;
        let fresh = self.exchange(self.inner.options.max_attempts).await?;
        self.store(&fresh);
        Ok(fresh)
    }

    /// A valid cached token, other than `except`.
    fn valid(&self, except: Option<&str>) -> Option<Token> {
        let now = Instant::now();
        self.state()
            .token
            .as_ref()
            .filter(|t| now < t.usable_until && except.is_none_or(|e| t.token != e))
            .cloned()
    }

    /// A token other than `used`: the cached one, else one exchange
    /// attempt (`None`: failed, or failing recently). The caller holds
    /// `exchanging`.
    async fn renew(&self, used: &str) -> Option<Token> {
        if let Some(t) = self.valid(Some(used)) {
            return Some(t);
        }
        if self.state().retry_at.is_some_and(|r| Instant::now() < r) {
            return None;
        }
        match self.exchange(1).await {
            Ok(fresh) => {
                self.store(&fresh);
                Some(fresh)
            }
            Err(e) => {
                tracing::warn!("token renewal failed (the current one is still valid): {e}");
                self.state().retry_at = Some(Instant::now() + RENEW_RETRY);
                None
            }
        }
    }

    /// When a connection authorized with `used` should reconnect: once
    /// another token is held (renewed from `used`'s refresh time on, while
    /// the connection carries on), or, if renewal keeps failing, just
    /// before `used` expires. A connection is never dropped for a token
    /// that doesn't exist yet.
    pub(crate) async fn renewal(self, used: Token) {
        let mut at = used.refresh_at;
        while at < used.usable_until {
            tokio::time::sleep_until(at).await;
            let renewed = {
                let _exchanging = self.inner.exchanging.lock().await;
                self.renew(&used.token).await
            };
            if renewed.is_some() {
                return;
            }
            at = Instant::now() + RENEW_RETRY;
        }
        tokio::time::sleep_until(used.usable_until).await;
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn store(&self, fresh: &Token) {
        let mut state = self.state();
        state.token = Some(fresh.clone());
        state.retry_at = None;
    }

    /// Forget the cached token: the next call exchanges again.
    pub async fn invalidate(&self) {
        *self.state() = State::default();
    }

    /// Forget `token` if it is the cached one (the API refused it).
    pub(crate) async fn reject(&self, token: &str) {
        let mut state = self.state();
        if state.token.as_ref().is_some_and(|t| t.token == token) {
            *state = State::default();
        }
    }

    async fn exchange(&self, attempts: u32) -> Result<Token, Error> {
        let inner = &*self.inner;
        let reply = inner
            .api
            .post(&*inner.signer, TOKEN_PATH, &inner.body, attempts)
            .await?;
        let received = Instant::now();
        #[derive(serde::Deserialize)]
        struct Exchanged {
            token: String,
            id: String,
            #[serde(rename = "expiresAt")]
            expires_at: i64,
            #[serde(default)]
            ancestors: Vec<String>,
        }
        let e: Exchanged = serde_json::from_slice(&reply.body)
            .map_err(|e| Error::Unavailable(format!("unexpected token response: {e}")))?;
        // The lifetime on the server's clock when ours is off by more than
        // its `Date` header's resolution.
        let local = now_ms();
        let server_now = match reply.date {
            Some(date) => {
                let date_ms = date
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis() as i64);
                if (date_ms - 1000..=date_ms + 2000).contains(&local) {
                    local
                } else {
                    date_ms + 500
                }
            }
            None => local,
        };
        let lifetime = Duration::from_millis(u64::try_from(e.expires_at - server_now).unwrap_or(0));
        if lifetime.is_zero() {
            return Err(Error::Unavailable(
                "the exchanged token has already expired (is the clock right?)".into(),
            ));
        }
        let o = &inner.options;
        // At most half the lifetime (short-lived tokens).
        let margin = o
            .refresh_margin
            .max(lifetime.mul_f64(o.refresh_fraction))
            .min(lifetime / 2);
        tracing::debug!(id = %e.id, ?lifetime, "exchanged a delegation chain for a token");
        Ok(Token {
            token: e.token,
            id: e.id,
            expires_at: e.expires_at,
            ancestors: e.ancestors,
            refresh_at: received + lifetime.saturating_sub(margin),
            usable_until: received + lifetime - MIN_VALIDITY.min(lifetime / 10),
            expires: received + lifetime,
        })
    }
}

/// Revoke delegation `id`: every token below it is refused from then on
/// (within a minute). `chain`: a chain that contains `id` (it proves which
/// account the delegation belongs to; expired delegations can be revoked
/// too). `signer`: the account's owner, or the subject of the delegation or
/// of one above it.
pub async fn revoke<S: Signer + ?Sized>(
    endpoint: &str,
    signer: &S,
    id: &str,
    chain: &Chain,
) -> Result<(), Error> {
    let valid_id = id.len() == 16 && id.bytes().all(|b| matches!(b, b'a'..=b'z' | b'2'..=b'7'));
    if !valid_id {
        return Err(Error::Config(format!("not a delegation id: {id:?}")));
    }
    let api = Api::new(endpoint, Duration::from_secs(10))?;
    let body =
        serde_json::to_vec(&serde_json::json!({ "chain": chain.encode() })).expect("serializable");
    let reply = api
        .post(signer, &format!("/v1/delegations/{id}/revoke"), &body, 5)
        .await?;
    #[derive(serde::Deserialize)]
    struct Revoked {
        revoked: bool,
    }
    match serde_json::from_slice::<Revoked>(&reply.body) {
        Ok(Revoked { revoked: true }) => Ok(()),
        _ => Err(Error::Unavailable(format!(
            "unexpected revocation response: {}",
            String::from_utf8_lossy(&reply.body)
        ))),
    }
}

/// How requests authenticate.
#[derive(Clone)]
pub enum Auth {
    /// An API key (`mc1_…`).
    ApiKey(String),
    /// Short-lived keys exchanged for a delegation chain.
    Delegated(TokenSource),
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Auth::ApiKey(_) => f.write_str("ApiKey(…)"),
            Auth::Delegated(t) => f.debug_tuple("Delegated").field(t).finish(),
        }
    }
}

impl From<TokenSource> for Auth {
    fn from(t: TokenSource) -> Self {
        Auth::Delegated(t)
    }
}

/// A credential for one connection.
pub(crate) struct Credential {
    pub token: String,
    /// Resolves when to reconnect with a fresh token (already held by
    /// then, unless renewal is failing).
    pub renew: Option<futures::future::BoxFuture<'static, ()>>,
}

impl Auth {
    pub fn api_key(key: impl Into<String>) -> Self {
        Auth::ApiKey(key.into())
    }

    /// The bearer token to send now.
    pub async fn bearer(&self) -> Result<String, Error> {
        Ok(self.credential().await?.token)
    }

    pub(crate) async fn credential(&self) -> Result<Credential, Error> {
        match self {
            Auth::ApiKey(key) => Ok(Credential {
                token: key.clone(),
                renew: None,
            }),
            Auth::Delegated(source) => {
                let t = source.token().await?;
                Ok(Credential {
                    token: t.token.clone(),
                    renew: Some(Box::pin(source.clone().renewal(t))),
                })
            }
        }
    }

    /// The API refused `token`: forget it if it was exchanged.
    pub(crate) async fn reject(&self, token: &str) {
        if let Auth::Delegated(source) = self {
            source.reject(token).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_message_matches_the_reference() {
        let m = request_message(
            "https://api.mainnet.mirai.cloud",
            "0xab",
            "post",
            "/v1/token",
            &sha256_hex(b""),
            1_793_836_000_000,
            "00",
        );
        assert_eq!(
            m,
            "mirai.cloud request\naudience: https://api.mainnet.mirai.cloud\naddress: 0xab\nmethod: POST\npath: /v1/token\nbody-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\ntimestamp: 1793836000000\nnonce: 00"
        );
    }
}
