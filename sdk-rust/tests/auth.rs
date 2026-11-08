//! The token exchange and delegated streams against a local mock API that
//! verifies signed requests (the agent's Ed25519 signature) and delegation
//! chains (each link's signature), and issues short-lived tokens:
//! renewal before expiry, single-flight, retries, refusals, revocation; a
//! stream whose token expires mid-stream reconnects with a fresh token,
//! without gaps or duplicates, before the server would have to cut it.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use base64::Engine;
use futures::StreamExt;
use miraicloud::auth::{self, Auth, TokenOptions, TokenSource};
use miraicloud::delegation::{Chain, DelegateRequest, KeypairSigner, Scope, Signer, delegate};
use miraicloud::streams::{Checkpoint, Event};
use prost::Message;
use sha2::Digest;
use sui_crypto::SuiVerifier;
use sui_crypto::simple::SimpleVerifier;
use sui_rpc::proto::sui::rpc::v2::SubscribeCheckpointsResponse;
use sui_sdk_types::{PersonalMessage, SimpleSignature, UserSignature};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Verify a serialized Sui signature (base64) over `message` and return
/// the signer's address.
fn verify(message: &[u8], signature: &str) -> Result<String, String> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(signature)
        .map_err(|e| format!("signature base64: {e}"))?;
    let sig = UserSignature::from_bytes(&bytes).map_err(|e| format!("signature: {e}"))?;
    SimpleVerifier
        .verify_personal_message(&PersonalMessage(message.into()), &sig)
        .map_err(|e| format!("bad signature: {e}"))?;
    match sig {
        UserSignature::Simple(SimpleSignature::Ed25519 { public_key, .. }) => {
            Ok(public_key.derive_address().to_string())
        }
        UserSignature::Simple(SimpleSignature::Secp256r1 { public_key, .. }) => {
            Ok(public_key.derive_address().to_string())
        }
        _ => Err("unexpected scheme".into()),
    }
}

#[derive(Default)]
struct Mock {
    audience: String,
    /// Lifetime of issued tokens.
    lifetime_ms: i64,
    /// Delay before answering an exchange.
    delay: Duration,
    /// Refusals to answer before succeeding: (status, tag, Retry-After).
    script: VecDeque<(StatusCode, &'static str, Option<&'static str>)>,
    /// Exchange requests received, and successful ones.
    attempts: usize,
    exchanges: usize,
    /// Issued tokens and their expiry (unix ms).
    issued: HashMap<String, i64>,
    nonces: HashSet<String>,
    /// Verification failures (must stay empty).
    errors: Vec<String>,
    /// Stream requests: (from, token).
    streams: Vec<(Option<u64>, String)>,
    /// A frame was due after its connection's token had expired (the
    /// server would have had to cut the stream).
    cut: bool,
    revoked: Vec<String>,
}

type Shared = Arc<Mutex<Mock>>;

fn refusal(status: StatusCode, tag: &str, message: &str) -> Response {
    let body = serde_json::json!({ "_tag": tag, "message": message }).to_string();
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// Checks the signed-request headers for `path` and returns the signer's
/// address.
fn check_signed(
    m: &mut Mock,
    headers: &HeaderMap,
    path: &str,
    body: &[u8],
) -> Result<String, String> {
    let h = |n: &str| {
        headers
            .get(n)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
            .ok_or(format!("missing {n}"))
    };
    let address = h("x-miraicloud-address")?;
    let timestamp = h("x-miraicloud-timestamp")?;
    let nonce = h("x-miraicloud-nonce")?;
    let signature = h("x-miraicloud-signature")?;
    let ts: i64 = timestamp.parse().map_err(|_| "timestamp")?;
    if (now_ms() - ts).abs() > 120_000 {
        return Err("timestamp out of window".into());
    }
    if nonce.len() != 32
        || !nonce
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(format!("nonce {nonce}"));
    }
    if !m.nonces.insert(nonce.clone()) {
        return Err("nonce reused".into());
    }
    let body_sha256: String = sha2::Sha256::digest(body)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let message = format!(
        "mirai.cloud request\naudience: {}\naddress: {address}\nmethod: POST\npath: {path}\nbody-sha256: {body_sha256}\ntimestamp: {timestamp}\nnonce: {nonce}",
        m.audience
    );
    let signer = verify(message.as_bytes(), &signature)?;
    if signer != address {
        return Err(format!("signed by {signer}, not {address}"));
    }
    Ok(address)
}

/// Shape, rules and every link's signature.
fn check_chain(
    m: &Mock,
    text: &str,
    address: Option<&str>,
) -> Result<miraicloud::delegation::CheckedChain, String> {
    let chain = Chain::decode(text).map_err(|e| e.to_string())?;
    let checked = chain
        .check(now_ms(), Some(&m.audience))
        .map_err(|e| e.to_string())?;
    for link in &checked.links {
        let signer = verify(link.message.as_bytes(), &link.signature)?;
        if signer != link.delegation.issuer {
            return Err(format!("delegation {} not signed by its issuer", link.id));
        }
    }
    if let Some(a) = address
        && checked.leaf.subject != a
    {
        return Err("the chain is for another address".into());
    }
    Ok(checked)
}

async fn token(State(s): State<Shared>, headers: HeaderMap, body: Bytes) -> Response {
    let delay = s.lock().unwrap().delay;
    tokio::time::sleep(delay).await;
    let mut m = s.lock().unwrap();
    m.attempts += 1;
    let verified = check_signed(&mut m, &headers, "/v1/token", &body).and_then(|address| {
        let v: serde_json::Value = serde_json::from_slice(&body).map_err(|e| e.to_string())?;
        let chain = v["chain"].as_str().ok_or("no chain")?;
        check_chain(&m, chain, Some(&address))
    });
    let checked = match verified {
        Ok(c) => c,
        Err(e) => {
            m.errors.push(e.clone());
            return refusal(StatusCode::UNAUTHORIZED, "Unauthorized", &e);
        }
    };
    if let Some((status, tag, retry_after)) = m.script.pop_front() {
        let mut r = refusal(status, tag, "scripted");
        if let Some(v) = retry_after {
            r.headers_mut()
                .insert(header::RETRY_AFTER, v.parse().unwrap());
        }
        return r;
    }
    m.exchanges += 1;
    let token = format!("mc1_test_{}", m.exchanges);
    let expires_at = now_ms() + m.lifetime_ms;
    m.issued.insert(token.clone(), expires_at);
    axum::Json(serde_json::json!({
        "token": token,
        "id": format!("{:a>16}", m.exchanges),
        "expiresAt": expires_at,
        "ancestors": checked.ancestors,
    }))
    .into_response()
}

async fn revoke(
    State(s): State<Shared>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut m = s.lock().unwrap();
    let path = format!("/v1/delegations/{id}/revoke");
    let result = check_signed(&mut m, &headers, &path, &body).and_then(|address| {
        let v: serde_json::Value = serde_json::from_slice(&body).map_err(|e| e.to_string())?;
        let text = v["chain"].as_str().ok_or("no chain")?;
        let checked = check_chain(&m, text, None)?;
        let at = checked
            .links
            .iter()
            .position(|l| l.id == id)
            .ok_or("not in the chain")?;
        let allowed = std::iter::once(&checked.account)
            .chain(checked.links[..=at].iter().map(|l| &l.delegation.subject))
            .any(|a| *a == address);
        if allowed {
            Ok(address)
        } else {
            Err("may not revoke".into())
        }
    });
    match result {
        Ok(_) => {
            m.revoked.push(id.clone());
            axum::Json(serde_json::json!({ "id": id, "revoked": true })).into_response()
        }
        Err(e) => refusal(StatusCode::FORBIDDEN, "Forbidden", &e),
    }
}

fn frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut f = vec![kind];
    f.extend((body.len() as u32).to_be_bytes());
    f.extend(body);
    zstd::bulk::compress(&f, 3).unwrap()
}

fn encoded(seq: u64) -> Vec<u8> {
    let mut c = Checkpoint::default();
    c.sequence_number = Some(seq);
    c.digest = Some(format!("d{seq}"));
    c.encode_to_vec()
}

fn cp(seq: u64) -> Vec<u8> {
    let mut body = SubscribeCheckpointsResponse::default();
    body.cursor = Some(seq);
    let mut bytes = body.encode_to_vec();
    let cp = encoded(seq);
    bytes.push(0x12);
    prost::encoding::encode_varint(cp.len() as u64, &mut bytes);
    bytes.extend(cp);
    frame(0, &bytes)
}

#[derive(serde::Deserialize)]
struct Params {
    from: Option<u64>,
}

/// One checkpoint every 50 ms from `from`, for as long as the client stays
/// (or until the token expires: then the stream is cut and `cut` set).
async fn stream(State(s): State<Shared>, headers: HeaderMap, Query(q): Query<Params>) -> Response {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or_default()
        .to_owned();
    let expires_at = {
        let mut m = s.lock().unwrap();
        m.streams.push((q.from, bearer.clone()));
        if headers.get_all(header::AUTHORIZATION).iter().count() != 1 {
            m.errors.push("not exactly one authorization header".into());
        }
        if bearer == "mc1_static" {
            i64::MAX
        } else {
            match m.issued.get(&bearer) {
                Some(&e) if e > now_ms() => e,
                _ => return refusal(StatusCode::UNAUTHORIZED, "Unauthorized", "invalid token"),
            }
        }
    };
    let start = q.from.unwrap_or(1000);
    let keepalive = futures::stream::once(async { Ok::<_, std::io::Error>(frame(2, &[])) });
    let checkpoints = futures::stream::unfold(start, move |seq| {
        let s = s.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if now_ms() >= expires_at {
                s.lock().unwrap().cut = true;
                return None;
            }
            Some((Ok(cp(seq)), seq + 1))
        }
    });
    let mut r = Response::new(Body::from_stream(keepalive.chain(checkpoints)));
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/vnd.miraicloud.checkpoints.v1+proto"
            .parse()
            .unwrap(),
    );
    r.headers_mut()
        .insert(header::CONTENT_ENCODING, "zstd".parse().unwrap());
    r
}

/// The mock API's origin.
async fn serve(lifetime_ms: i64) -> (String, Shared) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let state = Arc::new(Mutex::new(Mock {
        audience: origin.clone(),
        lifetime_ms,
        ..Default::default()
    }));
    let app = Router::new()
        .route("/v1/token", post(token))
        .route("/v1/delegations/{id}/revoke", post(revoke))
        .route("/v1/stream/checkpoints", get(stream))
        .with_state(state.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (origin, state)
}

fn owner() -> KeypairSigner {
    KeypairSigner::ed25519([1; 32])
}

fn agent() -> KeypairSigner {
    KeypairSigner::ed25519([2; 32])
}

fn sub() -> KeypairSigner {
    KeypairSigner::ed25519([3; 32])
}

/// owner → agent (stream:read, keys:write) → sub-agent (stream:read).
async fn chains(audience: &str) -> (Chain, Chain) {
    let root = delegate(
        &owner(),
        &DelegateRequest::to(agent().address()).scopes([Scope::StreamRead, Scope::KeysWrite]),
        None,
        Some(audience),
    )
    .await
    .unwrap();
    let leaf = delegate(
        &agent(),
        &DelegateRequest::to(sub().address()).scopes([Scope::StreamRead]),
        Some(&root),
        Some(audience),
    )
    .await
    .unwrap();
    (root, leaf)
}

fn options() -> TokenOptions {
    TokenOptions {
        refresh_margin: Duration::from_millis(600),
        ..Default::default()
    }
}

#[tokio::test]
async fn exchanges_once_and_renews_before_expiry() {
    let (origin, mock) = serve(2000).await;
    let (_, chain) = chains(&origin).await;
    let tokens = TokenSource::new(&origin, chain.clone(), sub(), options()).unwrap();
    let t0 = tokio::time::Instant::now();
    let first = tokens.token().await.unwrap();
    assert_eq!(first.token, "mc1_test_1");
    assert_eq!(first.ancestors, chain.check_now(None).unwrap().ancestors);
    // Renewal is due when less than max(600 ms, 10 %) of 2 s remains.
    assert_eq!(
        first.expires() - first.refresh_at(),
        Duration::from_millis(600)
    );
    let lifetime = first.expires() - t0;
    assert!(
        (Duration::from_millis(1800)..=Duration::from_millis(2100)).contains(&lifetime),
        "{lifetime:?}"
    );
    assert_eq!(tokens.current().await.unwrap(), "mc1_test_1");
    assert_eq!(mock.lock().unwrap().exchanges, 1);

    tokio::time::sleep_until(first.refresh_at() + Duration::from_millis(50)).await;
    // The first token is still valid on the server, but due for renewal.
    assert!(mock.lock().unwrap().issued["mc1_test_1"] > now_ms());
    assert_eq!(tokens.current().await.unwrap(), "mc1_test_2");
    assert_eq!(tokens.current().await.unwrap(), "mc1_test_2");
    let m = mock.lock().unwrap();
    assert_eq!((m.attempts, m.exchanges), (2, 2));
    assert!(m.errors.is_empty(), "{:?}", m.errors);
}

#[tokio::test]
async fn concurrent_callers_share_one_exchange() {
    let (origin, mock) = serve(60_000).await;
    mock.lock().unwrap().delay = Duration::from_millis(300);
    let (_, chain) = chains(&origin).await;
    let tokens = TokenSource::new(&origin, chain, sub(), options()).unwrap();
    let tasks: Vec<_> = (0..16)
        .map(|_| {
            let t = tokens.clone();
            tokio::spawn(async move { t.current().await.unwrap() })
        })
        .collect();
    for task in tasks {
        assert_eq!(task.await.unwrap(), "mc1_test_1");
    }
    let m = mock.lock().unwrap();
    assert_eq!((m.attempts, m.exchanges), (1, 1));
    assert!(m.errors.is_empty(), "{:?}", m.errors);
}

#[tokio::test]
async fn retries_429_and_503_then_gives_up_or_refuses() {
    let (origin, mock) = serve(60_000).await;
    let (root, chain) = chains(&origin).await;
    mock.lock().unwrap().script = [
        (StatusCode::TOO_MANY_REQUESTS, "RateLimited", Some("0")),
        (StatusCode::SERVICE_UNAVAILABLE, "Unavailable", None),
    ]
    .into();
    let tokens = TokenSource::new(&origin, chain.clone(), sub(), options()).unwrap();
    assert_eq!(tokens.current().await.unwrap(), "mc1_test_1");
    assert_eq!(mock.lock().unwrap().attempts, 3);

    // Final refusals are not retried.
    mock.lock().unwrap().script = [(StatusCode::FORBIDDEN, "Forbidden", None)].into();
    let tokens = TokenSource::new(&origin, chain.clone(), sub(), options()).unwrap();
    match tokens.current().await {
        Err(auth::Error::Refused {
            status: 403, tag, ..
        }) => assert_eq!(tag, "Forbidden"),
        other => panic!("{other:?}"),
    }
    assert_eq!(mock.lock().unwrap().attempts, 4);

    // Unavailable after `max_attempts`.
    mock.lock().unwrap().script = [
        (StatusCode::SERVICE_UNAVAILABLE, "Unavailable", None),
        (StatusCode::SERVICE_UNAVAILABLE, "Unavailable", None),
    ]
    .into();
    let o = TokenOptions {
        max_attempts: 2,
        ..options()
    };
    let tokens = TokenSource::new(&origin, chain, sub(), o).unwrap();
    let err = tokens.current().await.unwrap_err();
    assert!(err.is_transient(), "{err}");
    assert_eq!(mock.lock().unwrap().attempts, 6);

    // The agent of the root chain exchanges too.
    let tokens = TokenSource::new(&origin, root, agent(), options()).unwrap();
    assert_eq!(tokens.current().await.unwrap(), "mc1_test_2");
    assert!(mock.lock().unwrap().errors.is_empty());
}

#[tokio::test]
async fn a_failed_renewal_keeps_the_valid_token() {
    let (origin, mock) = serve(2000).await;
    let (_, chain) = chains(&origin).await;
    let tokens = TokenSource::new(&origin, chain, sub(), options()).unwrap();
    let first = tokens.token().await.unwrap();
    assert_eq!(first.token, "mc1_test_1");
    mock.lock().unwrap().script = [(StatusCode::SERVICE_UNAVAILABLE, "Unavailable", None)].into();
    tokio::time::sleep_until(first.refresh_at() + Duration::from_millis(50)).await;
    // One attempt (no retries while the token is valid), then the cached one.
    assert_eq!(tokens.current().await.unwrap(), "mc1_test_1");
    assert_eq!(tokens.current().await.unwrap(), "mc1_test_1");
    assert_eq!(mock.lock().unwrap().attempts, 2);
    // Too close to its expiry: exchanged again.
    tokio::time::sleep_until(first.expires() - Duration::from_millis(100)).await;
    assert_eq!(tokens.current().await.unwrap(), "mc1_test_2");
}

#[tokio::test]
async fn a_renewal_in_progress_holds_up_only_its_caller() {
    let (origin, mock) = serve(2000).await;
    let (_, chain) = chains(&origin).await;
    let tokens = TokenSource::new(&origin, chain, sub(), options()).unwrap();
    let first = tokens.token().await.unwrap();
    mock.lock().unwrap().delay = Duration::from_millis(400);
    tokio::time::sleep_until(first.refresh_at() + Duration::from_millis(50)).await;
    let renewing = tokio::spawn({
        let t = tokens.clone();
        async move { t.current().await.unwrap() }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    // The valid token, at once, while the exchange runs.
    let t0 = tokio::time::Instant::now();
    assert_eq!(tokens.current().await.unwrap(), "mc1_test_1");
    assert!(
        t0.elapsed() < Duration::from_millis(100),
        "{:?}",
        t0.elapsed()
    );
    assert_eq!(renewing.await.unwrap(), "mc1_test_2");
    assert_eq!(tokens.current().await.unwrap(), "mc1_test_2");
    assert_eq!(mock.lock().unwrap().exchanges, 2);
}

#[tokio::test]
async fn construction_checks_the_chain() {
    let (origin, _mock) = serve(60_000).await;
    let (root, chain) = chains(&origin).await;
    // Another signer than the chain's subject.
    let err = TokenSource::new(&origin, chain.clone(), agent(), options()).unwrap_err();
    assert!(err.to_string().contains("not this signer"), "{err}");
    // Another API than the chain's audience.
    let err = TokenSource::new(
        "https://api.mainnet.mirai.cloud",
        chain.clone(),
        sub(),
        options(),
    )
    .unwrap_err();
    assert!(matches!(err, auth::Error::Delegation(_)), "{err}");
    // An endpoint with a trailing slash is the same origin.
    assert!(TokenSource::new(format!("{origin}/"), root, agent(), options()).is_ok());
    // The exchange grants 60 s to 24 h.
    for secs in [59, 86_401] {
        let o = TokenOptions {
            expires_in: Some(Duration::from_secs(secs)),
            ..options()
        };
        assert!(
            TokenSource::new(&origin, chain.clone(), sub(), o).is_err(),
            "{secs}"
        );
    }
    let o = TokenOptions {
        expires_in: Some(Duration::from_secs(60)),
        ..options()
    };
    assert!(TokenSource::new(&origin, chain, sub(), o).is_ok());
}

#[tokio::test]
async fn revocation_is_a_signed_request() {
    let (origin, mock) = serve(60_000).await;
    let (root, chain) = chains(&origin).await;
    let leaf_id = chain.0[1].id();
    // With a chain that does not contain the id: refused.
    let err = auth::revoke(&origin, &agent(), &leaf_id, &root)
        .await
        .unwrap_err();
    assert!(
        matches!(err, auth::Error::Refused { status: 403, .. }),
        "{err}"
    );
    // Only the owner or a subject at or above the delegation.
    let err = auth::revoke(&origin, &sub(), &root.0[0].id(), &chain)
        .await
        .unwrap_err();
    assert!(
        matches!(err, auth::Error::Refused { status: 403, .. }),
        "{err}"
    );
    // By the leaf's subject; by the root's subject; by the owner.
    auth::revoke(&origin, &sub(), &leaf_id, &chain)
        .await
        .unwrap();
    auth::revoke(&origin, &agent(), &leaf_id, &chain)
        .await
        .unwrap();
    auth::revoke(&origin, &owner(), &root.0[0].id(), &root)
        .await
        .unwrap();
    assert!(
        auth::revoke(&origin, &owner(), "not-an-id", &root)
            .await
            .is_err()
    );
    let m = mock.lock().unwrap();
    assert_eq!(m.revoked, [leaf_id.clone(), leaf_id, root.0[0].id()]);
    assert!(m.errors.is_empty());
}

#[tokio::test]
async fn a_stream_renews_its_token_without_gaps_or_duplicates() {
    // 2 s tokens, renewed 600 ms before expiry: the stream reconnects
    // about every 1.4 s.
    let (origin, mock) = serve(2000).await;
    let (_, chain) = chains(&origin).await;
    let tokens = TokenSource::new(&origin, chain, sub(), options()).unwrap();
    let mut sub = miraicloud::streams::Client::builder()
        .url(format!("{origin}/v1/stream/checkpoints"))
        .no_archive()
        .start_at(100)
        // Replaced by the exchanged tokens.
        .api_key("mc1_ignored")
        .delegation(tokens)
        .build()
        .unwrap()
        .subscribe();
    let mut seqs = vec![];
    let t0 = tokio::time::Instant::now();
    while t0.elapsed() < Duration::from_millis(4500) {
        let ev = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .expect("progress")
            .expect("open");
        match ev.unwrap() {
            Event::Checkpoint { seq, checkpoint } => {
                assert_eq!(checkpoint.digest, Some(format!("d{seq}")));
                seqs.push(seq);
            }
            Event::Gap { from, to } => panic!("gap {from}-{to}"),
            other => panic!("unexpected {other:?}"),
        }
    }
    drop(sub);
    let expected: Vec<u64> = (100..100 + seqs.len() as u64).collect();
    assert_eq!(seqs, expected, "in order, no gaps, no duplicates");
    assert!(seqs.len() > 40, "{}", seqs.len());
    let m = mock.lock().unwrap();
    assert!(
        !m.cut,
        "the server had to cut a stream with an expired token"
    );
    assert!(m.errors.is_empty(), "{:?}", m.errors);
    assert!(m.streams.len() >= 3, "{:?}", m.streams);
    // Each connection with a fresh token, resuming where the last stopped.
    let tokens: HashSet<_> = m.streams.iter().map(|(_, t)| t.clone()).collect();
    assert_eq!(tokens.len(), m.streams.len(), "{:?}", m.streams);
    assert_eq!(m.streams[0].0, Some(100));
    for w in m.streams.windows(2) {
        assert!(w[1].0 > w[0].0, "{:?}", m.streams);
    }
}

#[tokio::test]
async fn a_stream_keeps_flowing_while_its_token_is_renewed() {
    // Slow exchanges (400 ms): renewed while the old connection streams,
    // so a reconnect never waits on one.
    let (origin, mock) = serve(2000).await;
    mock.lock().unwrap().delay = Duration::from_millis(400);
    let (_, chain) = chains(&origin).await;
    let tokens = TokenSource::new(&origin, chain, sub(), options()).unwrap();
    let mut sub = miraicloud::streams::Client::builder()
        .url(format!("{origin}/v1/stream/checkpoints"))
        .no_archive()
        .start_at(100)
        .delegation(tokens)
        .build()
        .unwrap()
        .subscribe();
    let mut seqs = vec![];
    let mut longest = Duration::ZERO;
    let mut last = None;
    let t0 = tokio::time::Instant::now();
    while t0.elapsed() < Duration::from_millis(4500) {
        let ev = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .expect("progress")
            .expect("open");
        match ev.unwrap() {
            Event::Checkpoint { seq, .. } => seqs.push(seq),
            Event::Gap { from, to } => panic!("gap {from}-{to}"),
            other => panic!("unexpected {other:?}"),
        }
        let now = tokio::time::Instant::now();
        if let Some(last) = last {
            longest = longest.max(now - last);
        }
        last = Some(now);
    }
    drop(sub);
    let expected: Vec<u64> = (100..100 + seqs.len() as u64).collect();
    assert_eq!(seqs, expected, "in order, no gaps, no duplicates");
    let m = mock.lock().unwrap();
    assert!(m.streams.len() >= 3, "{:?}", m.streams);
    assert!(!m.cut);
    // Frames come every 50 ms; a reconnect waiting on the exchange would
    // pause them for 400 ms.
    assert!(longest < Duration::from_millis(250), "{longest:?}");
}

#[tokio::test]
async fn a_stream_with_an_api_key_auth() {
    let (origin, mock) = serve(2000).await;
    let mut sub = miraicloud::streams::Client::builder()
        .url(format!("{origin}/v1/stream/checkpoints"))
        .no_archive()
        .start_at(7)
        .auth(Auth::api_key("mc1_static"))
        .build()
        .unwrap()
        .subscribe();
    for want in 7..10 {
        match sub.next().await.unwrap().unwrap() {
            Event::Checkpoint { seq, .. } => assert_eq!(seq, want),
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(
        mock.lock().unwrap().streams,
        [(Some(7), "mc1_static".to_owned())]
    );
}

#[tokio::test]
async fn a_refused_exchange_ends_the_stream() {
    let (origin, mock) = serve(2000).await;
    let (_, chain) = chains(&origin).await;
    mock.lock().unwrap().script = [(StatusCode::FORBIDDEN, "Forbidden", None)].into();
    let tokens = TokenSource::new(&origin, chain, sub(), options()).unwrap();
    let mut sub = miraicloud::streams::Client::builder()
        .url(format!("{origin}/v1/stream/checkpoints"))
        .no_archive()
        .delegation(tokens)
        .build()
        .unwrap()
        .subscribe();
    let err = sub.next().await.unwrap().unwrap_err();
    assert!(
        matches!(
            err,
            miraicloud::streams::Error::Auth(auth::Error::Refused { status: 403, .. })
        ),
        "{err}"
    );
    assert!(sub.next().await.is_none());
}
