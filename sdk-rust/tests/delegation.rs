//! Delegations against the reference implementation's vectors
//! (`tests/fixtures/delegation-vectors.json`, a copy of
//! `core/test/fixtures/delegation-vectors.json`): messages, ids, signatures,
//! chain encoding and the chain rules; then `delegate`'s defaults and
//! refusals.

use miraicloud::delegation::{
    Chain, DelegateRequest, Delegation, Error, KeypairSigner, Limits, Network, Rights, Scope,
    SignedDelegation, Signer, delegate, delegate_at, delegation_id,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Key {
    seed: String,
    address: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Keys {
    owner: Key,
    agent: Key,
    sub_agent: Key,
}

#[derive(Deserialize)]
struct Link {
    message: String,
    id: String,
    signature: String,
}

#[derive(Deserialize)]
struct Named {
    name: String,
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Vectors {
    now: i64,
    audience: String,
    keys: Keys,
    delegations: Vec<Link>,
    chain: String,
    ancestors: Vec<String>,
    invalid_second_links: Vec<Named>,
    invalid_messages: Vec<Named>,
}

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/delegation-vectors.json"
);

fn vectors() -> Vectors {
    serde_json::from_str(&std::fs::read_to_string(FIXTURE).unwrap()).unwrap()
}

fn seed(hex: &str) -> [u8; 32] {
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    bytes.try_into().unwrap()
}

fn chain_of(v: &Vectors) -> Chain {
    Chain::new(
        v.delegations
            .iter()
            .map(|l| SignedDelegation {
                message: l.message.clone(),
                signature: l.signature.clone(),
            })
            .collect(),
    )
}

/// The copy is the reference's, when built inside the platform repository.
#[test]
fn fixture_is_the_reference() {
    let reference = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../core/test/fixtures/delegation-vectors.json"
    );
    match std::fs::read_to_string(reference) {
        Ok(text) => assert_eq!(text, std::fs::read_to_string(FIXTURE).unwrap()),
        Err(e) => eprintln!("skipped: {reference}: {e}"),
    }
}

#[test]
fn messages_parse_render_and_hash_like_the_reference() {
    let v = vectors();
    for link in &v.delegations {
        let d = Delegation::parse(&link.message).unwrap();
        assert_eq!(d.message(), link.message);
        assert_eq!(d.to_string(), link.message);
        assert_eq!(d.id(), link.id);
        assert_eq!(delegation_id(&link.message), link.id);
    }
    let d1 = Delegation::parse(&v.delegations[0].message).unwrap();
    assert_eq!(d1.audience, v.audience);
    assert_eq!(d1.account, v.keys.owner.address);
    assert_eq!(d1.issuer, v.keys.owner.address);
    assert_eq!(d1.subject, v.keys.agent.address);
    assert_eq!(d1.parent, None);
    assert_eq!(
        d1.rights,
        Rights {
            scopes: vec![Scope::StreamRead, Scope::RpcRead, Scope::KeysWrite],
            networks: vec![Network::Mainnet],
            origins: vec![],
            expires_at: Some(1_893_456_000_000),
            limits: Limits {
                monthly_requests: Some(1_000_000),
                monthly_bytes: None,
            },
        }
    );
    assert_eq!(d1.nonce, "00112233445566778899aabbccddeeff");
    let d2 = Delegation::parse(&v.delegations[1].message).unwrap();
    assert_eq!(d2.parent.as_deref(), Some(v.delegations[0].id.as_str()));
    assert_eq!(d2.subject, v.keys.sub_agent.address);
}

#[test]
fn keys_and_signatures_match_the_reference_byte_for_byte() {
    let v = vectors();
    let owner = KeypairSigner::ed25519(seed(&v.keys.owner.seed));
    let agent = KeypairSigner::ed25519(seed(&v.keys.agent.seed));
    let sub = KeypairSigner::ed25519(seed(&v.keys.sub_agent.seed));
    assert_eq!(owner.address(), v.keys.owner.address);
    assert_eq!(agent.address(), v.keys.agent.address);
    assert_eq!(sub.address(), v.keys.sub_agent.address);
    // Ed25519 is deterministic.
    for (signer, link) in [(&owner, &v.delegations[0]), (&agent, &v.delegations[1])] {
        let sig = signer
            .sign_personal_message_sync(link.message.as_bytes())
            .unwrap();
        assert_eq!(sig, link.signature);
    }
}

#[test]
fn chain_encodes_and_decodes_like_the_reference() {
    let v = vectors();
    let chain = chain_of(&v);
    assert_eq!(chain.encode(), v.chain);
    assert_eq!(chain.to_string(), v.chain);
    let decoded = Chain::decode(&v.chain).unwrap();
    assert_eq!(decoded, chain);
    assert_eq!(format!("  {}\n", v.chain).parse::<Chain>().unwrap(), chain);
    let checked = decoded.check(v.now, Some(&v.audience)).unwrap();
    assert_eq!(checked.ancestors, v.ancestors);
    assert_eq!(checked.account, v.keys.owner.address);
    assert_eq!(checked.leaf.subject, v.keys.sub_agent.address);
    assert_eq!(checked.links.len(), 2);
    // Without an audience to match.
    assert!(decoded.check(v.now, None).is_ok());
    assert!(matches!(
        decoded.check(v.now, Some("https://api.testnet.mirai.cloud")),
        Err(Error::Audience { index: 1, .. })
    ));
}

#[test]
fn malformed_chains_are_refused() {
    let v = vectors();
    let body = v.chain.strip_prefix("mcd1_").unwrap();
    for (name, text) in [
        ("prefix", format!("mcd2_{body}")),
        ("not base64url", format!("mcd1_{body}+")),
        ("truncated", format!("mcd1_{}", &body[..body.len() - 8])),
        ("trailing bytes", {
            use base64::Engine;
            let e = base64::engine::general_purpose::URL_SAFE_NO_PAD;
            let mut bytes = e.decode(body).unwrap();
            bytes.push(0);
            format!("mcd1_{}", e.encode(bytes))
        }),
        ("empty", "mcd1_AA".to_owned()),
        ("too long", format!("mcd1_{}", "A".repeat(16_384))),
    ] {
        assert!(
            matches!(Chain::decode(&text), Err(Error::Encoding(_))),
            "{name}"
        );
    }
    assert!(matches!(
        Chain::default().check(v.now, None),
        Err(Error::Length(0))
    ));
}

#[test]
fn invalid_second_links_fail_the_check() {
    let v = vectors();
    let first = chain_of(&v).0[0].clone();
    assert_eq!(v.invalid_second_links.len(), 6);
    for bad in &v.invalid_second_links {
        // Each is a valid message on its own.
        Delegation::parse(&bad.message).unwrap();
        let chain = Chain::new(vec![
            first.clone(),
            SignedDelegation {
                message: bad.message.clone(),
                signature: v.delegations[1].signature.clone(),
            },
        ]);
        let err = chain.check(v.now, Some(&v.audience)).unwrap_err();
        let expected = match bad.name.as_str() {
            "scopes wider than the parent"
            | "expiry later than the parent"
            | "limit looser than the parent" => matches!(err, Error::Wider { index: 2, .. }),
            "wrong parent" | "issuer is not the previous subject" => {
                matches!(err, Error::Broken { index: 2, .. })
            }
            "other audience" => matches!(err, Error::Audience { index: 2, .. }),
            other => panic!("unknown vector {other}"),
        };
        assert!(expected, "{}: {err}", bad.name);
        // Without an audience to match, the other audience still breaks
        // the chain.
        assert!(chain.check(v.now, None).is_err(), "{}", bad.name);
    }
}

#[test]
fn invalid_messages_fail_parsing() {
    let v = vectors();
    assert_eq!(v.invalid_messages.len(), 5);
    for bad in &v.invalid_messages {
        assert!(
            matches!(Delegation::parse(&bad.message), Err(Error::Message(_))),
            "{}",
            bad.name
        );
    }
}

/// The first vector with one line replaced.
fn with_line(v: &Vectors, prefix: &str, line: &str) -> String {
    v.delegations[0]
        .message
        .split('\n')
        .map(|l| if l.starts_with(prefix) { line } else { l })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn non_canonical_and_malformed_messages() {
    let v = vectors();
    for (name, prefix, line) in [
        (
            "duplicate scope",
            "scopes:",
            "scopes: stream:read stream:read",
        ),
        ("no scope", "scopes:", "scopes: "),
        ("unknown scope", "scopes:", "scopes: stream:write"),
        (
            "networks out of order",
            "networks:",
            "networks: testnet mainnet",
        ),
        (
            "origins unsorted",
            "origins:",
            "origins: https://b.example https://a.example",
        ),
        (
            "origin with a path",
            "origins:",
            "origins: https://a.example/x",
        ),
        ("not a date", "expires:", "expires: 2030-13-01T00:00:00Z"),
        ("no such day", "expires:", "expires: 2030-02-30T00:00:00Z"),
        (
            "leading zero",
            "limits:",
            "limits: monthlyRequests=01 monthlyBytes=-",
        ),
        (
            "unsafe integer",
            "limits:",
            "limits: monthlyRequests=9007199254740992 monthlyBytes=-",
        ),
        ("short nonce", "nonce:", "nonce: 00"),
        (
            "uppercase nonce",
            "nonce:",
            "nonce: 00112233445566778899AABBCCDDEEFF",
        ),
        ("bad parent", "parent:", "parent: 0123456789abcdef"),
        (
            "audience with a path",
            "audience:",
            "audience: https://api.mainnet.mirai.cloud/",
        ),
        ("short address", "subject:", "subject: 0x2"),
    ] {
        let text = with_line(&v, prefix, line);
        assert_ne!(text, v.delegations[0].message, "{name}");
        assert!(
            matches!(Delegation::parse(&text), Err(Error::Message(_))),
            "{name}"
        );
    }
    // Canonical variants parse.
    for (prefix, line) in [
        (
            "origins:",
            "origins: https://a.example http://b.example:8080",
        ),
        ("networks:", "networks: mainnet testnet"),
        ("limits:", "limits: monthlyRequests=- monthlyBytes=0"),
        ("expires:", "expires: -"),
    ] {
        let text = with_line(&v, prefix, line);
        let canonical = if prefix == "origins:" {
            with_line(
                &v,
                prefix,
                "origins: http://b.example:8080 https://a.example",
            )
        } else {
            text.clone()
        };
        if text == canonical {
            assert_eq!(Delegation::parse(&text).unwrap().message(), text);
        } else {
            assert!(Delegation::parse(&text).is_err());
            assert_eq!(Delegation::parse(&canonical).unwrap().message(), canonical);
        }
    }
}

const DAY: i64 = 86_400_000;

fn owner() -> KeypairSigner {
    KeypairSigner::ed25519([1; 32])
}

fn agent() -> KeypairSigner {
    KeypairSigner::ed25519([2; 32])
}

fn sub() -> KeypairSigner {
    KeypairSigner::ed25519([3; 32])
}

#[tokio::test]
async fn delegate_as_owner_uses_the_defaults() {
    let now = 1_793_923_200_123;
    let chain = delegate_at(
        &owner(),
        &DelegateRequest::to(agent().address()),
        None,
        None,
        now,
    )
    .await
    .unwrap();
    assert_eq!(chain.len(), 1);
    let checked = chain
        .check(now, Some("https://api.mainnet.mirai.cloud"))
        .unwrap();
    let d = &checked.leaf;
    assert_eq!(d.account, owner().address());
    assert_eq!(d.issuer, owner().address());
    assert_eq!(d.subject, agent().address());
    assert_eq!(d.parent, None);
    let mut expected = Rights::owner();
    // 30 days, rounded down to the second.
    expected.expires_at = Some((now + 30 * DAY) / 1000 * 1000);
    assert_eq!(d.rights, expected);
    assert!(!d.rights.scopes.contains(&Scope::KeysWrite));
    assert_eq!(d.nonce.len(), 32);
    // The signature is the owner's over the message.
    let link = &chain.0[0];
    assert_eq!(
        owner()
            .sign_personal_message_sync(link.message.as_bytes())
            .unwrap(),
        link.signature
    );
    // Fresh nonces: two delegations of the same rights differ.
    let again = delegate_at(
        &owner(),
        &DelegateRequest::to(agent().address()),
        None,
        None,
        now,
    )
    .await
    .unwrap();
    assert_ne!(again.0[0].id(), link.id());
}

#[tokio::test]
async fn delegate_further_attenuates_and_inherits() {
    let audience = "http://127.0.0.1:9";
    let root = delegate(
        &owner(),
        &DelegateRequest::to(agent().address())
            .scopes([Scope::StreamRead, Scope::RpcRead, Scope::KeysWrite])
            .networks([Network::Mainnet])
            .origins(["https://app.example/"])
            .expires_in(std::time::Duration::from_secs(3600))
            .monthly_requests(1000),
        None,
        Some(audience),
    )
    .await
    .unwrap();
    let root_d = root.0[0].delegation().unwrap();
    assert_eq!(root_d.rights.origins, ["https://app.example"]);

    // Inherits everything left out.
    let chain = delegate(
        &agent(),
        &DelegateRequest::to(sub().address()).scopes([Scope::StreamRead]),
        Some(&root),
        Some(audience),
    )
    .await
    .unwrap();
    let checked = chain.check_now(Some(audience)).unwrap();
    assert_eq!(checked.links.len(), 2);
    let leaf = &checked.leaf;
    assert_eq!(leaf.parent.as_deref(), Some(root.0[0].id().as_str()));
    assert_eq!(leaf.account, owner().address());
    assert_eq!(leaf.issuer, agent().address());
    assert_eq!(leaf.rights.scopes, [Scope::StreamRead]);
    assert_eq!(leaf.rights.origins, root_d.rights.origins);
    assert_eq!(leaf.rights.expires_at, root_d.rights.expires_at);
    assert_eq!(leaf.rights.limits.monthly_requests, Some(1000));
    assert_eq!(checked.ancestors, [chain.0[1].id(), chain.0[0].id()]);

    // Attenuation violations are refused before signing.
    for (name, request) in [
        (
            "wider scopes",
            DelegateRequest::to(sub().address()).scopes([Scope::RpcWrite]),
        ),
        (
            "other network",
            DelegateRequest::to(sub().address()).networks([Network::Testnet]),
        ),
        (
            "other origin",
            DelegateRequest::to(sub().address()).origins(["https://evil.example"]),
        ),
        (
            "any origin",
            DelegateRequest::to(sub().address()).origins(Vec::<String>::new()),
        ),
        (
            "later expiry",
            DelegateRequest::to(sub().address()).expires_in(std::time::Duration::from_secs(7200)),
        ),
        (
            "never expires",
            DelegateRequest::to(sub().address()).never_expires(),
        ),
        (
            "looser limit",
            DelegateRequest::to(sub().address()).monthly_requests(1001),
        ),
    ] {
        let err = delegate(&agent(), &request, Some(&root), Some(audience))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Request(_)), "{name}: {err}");
    }

    // Only the chain's subject may extend it; only with keys:write.
    let err = delegate(
        &sub(),
        &DelegateRequest::to(owner().address()),
        Some(&root),
        Some(audience),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("not this signer"), "{err}");
    let err = delegate(
        &sub(),
        &DelegateRequest::to(owner().address()),
        Some(&chain),
        Some(audience),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("keys:write"), "{err}");
    // Another audience than the parent's.
    let err = delegate(
        &agent(),
        &DelegateRequest::to(sub().address()),
        Some(&root),
        None,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("not https://api.mainnet.mirai.cloud"),
        "{err}"
    );
}

#[tokio::test]
async fn delegate_refusals() {
    let now = 1_793_923_200_000;
    for (name, request) in [
        ("bad address", DelegateRequest::to("agent")),
        (
            "past expiry",
            DelegateRequest::to(agent().address()).expires_at(now - 1),
        ),
        (
            "expiry rounds down to now",
            DelegateRequest::to(agent().address()).expires_at(now + 999),
        ),
        (
            "no scopes",
            DelegateRequest::to(agent().address()).scopes([]),
        ),
        (
            "bad origin",
            DelegateRequest::to(agent().address()).origins(["app.example"]),
        ),
    ] {
        let err = delegate_at(&owner(), &request, None, None, now)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Request(_)), "{name}: {err}");
    }
    // Short addresses are normalized.
    let chain = delegate_at(&owner(), &DelegateRequest::to("0x2"), None, None, now)
        .await
        .unwrap();
    assert_eq!(
        chain.0[0].delegation().unwrap().subject,
        format!("0x{:0>64}", "2")
    );
}

#[tokio::test]
async fn expiry_is_checked_at_now() {
    let now = 1_793_923_200_000;
    let chain = delegate_at(
        &owner(),
        &DelegateRequest::to(agent().address()).expires_at(now + 60_000),
        None,
        None,
        now,
    )
    .await
    .unwrap();
    assert!(chain.check(now + 59_999, None).is_ok());
    assert!(matches!(
        chain.check(now + 60_000, None),
        Err(Error::Expired { index: 1, expires_at }) if expires_at == now + 60_000
    ));
    // A child may not outlive an expired parent either.
    let v = vectors();
    let after = 1_893_456_000_000; // the vectors' root expiry
    assert!(matches!(
        chain_of(&v).check(after, None),
        Err(Error::Expired { index: 1, .. })
    ));
}

#[test]
fn secp256r1_and_suiprivkey() {
    let r1 = KeypairSigner::secp256r1([7; 32]);
    assert!(r1.address().starts_with("0x") && r1.address().len() == 66);
    let sig = r1.sign_personal_message_sync(b"hello").unwrap();
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&sig)
        .unwrap();
    // flag 0x02 || 64-byte signature || 33-byte compressed key.
    assert_eq!((bytes[0], bytes.len()), (2, 98));
    let text = r1.to_suiprivkey().unwrap();
    assert!(text.starts_with("suiprivkey1"));
    assert_eq!(
        KeypairSigner::from_suiprivkey(&text).unwrap().address(),
        r1.address()
    );
    let ed = owner();
    assert_eq!(
        KeypairSigner::from_suiprivkey(&ed.to_suiprivkey().unwrap())
            .unwrap()
            .address(),
        ed.address()
    );
    assert!(KeypairSigner::from_suiprivkey("suiprivkey1nope").is_err());
}
