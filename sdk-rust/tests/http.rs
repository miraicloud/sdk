//! The client against a local mock gateway: resume positions, encodings,
//! reconnects, retryable vs fatal statuses, `410` without an archive. Each
//! scenario runs in typed and raw mode. (Archive backfill is covered by the
//! driver's unit tests and, against Mainnet, by `live.rs`.)

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures::StreamExt;
use miraicloud::streams::{Checkpoint, Event, RawEvent};
use prost::Message;
use sui_rpc::proto::sui::rpc::v2::SubscribeCheckpointsResponse;

/// One scripted response.
#[derive(Clone)]
enum Reply {
    /// 200 with these frames (uncompressed; each one compressed as its own
    /// zstd frame), then close (or hang if `hang`).
    Frames {
        frames: Vec<Vec<u8>>,
        hang: bool,
    },
    Status(StatusCode, Option<&'static str>),
    /// A JSON refusal, as the gateway sends.
    Json(StatusCode, &'static str),
    /// 410 with `Oldest-Checkpoint`.
    TooOld(u64),
}

/// (from, read_mask, Accept-Encoding, Accept) of a request.
type Seen = (Option<String>, Option<String>, String, String);

#[derive(Clone, Default)]
struct Mock {
    replies: Arc<Mutex<VecDeque<Reply>>>,
    requests: Arc<Mutex<Vec<Seen>>>,
    /// Response bodies dropped (the client went away or the body ended).
    closed: Arc<AtomicUsize>,
}

/// Counts the body as closed when dropped.
struct Guard(Arc<AtomicUsize>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(serde::Deserialize)]
struct Params {
    from: Option<String>,
    read_mask: Option<String>,
}

async fn handler(State(m): State<Mock>, headers: HeaderMap, Query(p): Query<Params>) -> Response {
    let h = |n: &str| headers.get(n).map(|v| v.to_str().unwrap().to_owned());
    let accept = h("accept-encoding").unwrap_or_default();
    m.requests.lock().unwrap().push((
        p.from,
        p.read_mask,
        accept.clone(),
        h("accept").unwrap_or_default(),
    ));
    let reply = m.replies.lock().unwrap().pop_front();
    match reply {
        None => {
            // Hang without bytes.
            Body::from_stream(futures::stream::pending::<Result<Vec<u8>, std::io::Error>>())
                .into_response()
        }
        Some(Reply::Status(code, retry_after)) => {
            let mut r = (code, "nope").into_response();
            if let Some(v) = retry_after {
                r.headers_mut()
                    .insert(header::RETRY_AFTER, v.parse().unwrap());
            }
            r
        }
        Some(Reply::Json(code, body)) => {
            let mut r = (code, body).into_response();
            r.headers_mut()
                .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
            r
        }
        Some(Reply::TooOld(oldest)) => {
            let mut r = (StatusCode::GONE, "too old").into_response();
            r.headers_mut()
                .insert("oldest-checkpoint", oldest.to_string().parse().unwrap());
            r
        }
        Some(Reply::Frames { frames, hang }) => {
            // Like the gateway: zstd only, one zstd frame per stream frame.
            if !accept.contains("zstd") {
                return (StatusCode::BAD_REQUEST, "zstd only").into_response();
            }
            let mut bytes = vec![];
            for f in frames {
                bytes.extend(zstd::bulk::compress(&f, 3).unwrap());
            }
            let first = futures::stream::iter([Ok::<_, std::io::Error>(bytes)]);
            let guard = Guard(m.closed.clone());
            let stream = if hang {
                first.chain(futures::stream::pending()).boxed()
            } else {
                first.boxed()
            }
            .map(move |c| {
                let _ = &guard;
                c
            });
            let mut r = Response::new(Body::from_stream(stream));
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
    }
}

impl Mock {
    async fn serve(&self, replies: Vec<Reply>) -> String {
        *self.replies.lock().unwrap() = replies.into();
        let app = Router::new()
            .route("/v1/checkpoints", get(handler))
            .with_state(self.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}/v1/checkpoints")
    }
}

/// The encoded checkpoint for `seq`, with an unknown field (newer server
/// protos) that must be skipped.
fn encoded(seq: u64) -> Vec<u8> {
    let mut c = Checkpoint::default();
    c.sequence_number = Some(seq);
    c.digest = Some(format!("d{seq}"));
    let mut bytes = c.encode_to_vec();
    // Field 999, varint 1.
    bytes.extend([0xb8, 0x3e, 0x01]);
    bytes
}

fn frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut f = vec![kind];
    f.extend((body.len() as u32).to_be_bytes());
    f.extend(body);
    f
}

fn cp(seq: u64) -> Vec<u8> {
    let mut body = SubscribeCheckpointsResponse::default();
    body.cursor = Some(seq);
    let mut bytes = body.encode_to_vec();
    // `checkpoint` (field 2) as the exact bytes above.
    let cp = encoded(seq);
    bytes.push(0x12);
    prost::encoding::encode_varint(cp.len() as u64, &mut bytes);
    bytes.extend(cp);
    frame(0, &bytes)
}

fn gap(from: u64, to: u64) -> Vec<u8> {
    let mut body = from.to_be_bytes().to_vec();
    body.extend(to.to_be_bytes());
    frame(1, &body)
}

fn keepalive() -> Vec<u8> {
    frame(2, &[])
}

fn frames(list: &[Vec<u8>], hang: bool) -> Reply {
    let mut all = vec![keepalive()];
    all.extend_from_slice(list);
    Reply::Frames { frames: all, hang }
}

/// A typed or raw subscription.
enum Sub {
    Typed(miraicloud::streams::Subscription),
    Raw(miraicloud::streams::RawSubscription),
}

impl Sub {
    fn new(builder: miraicloud::streams::Builder, raw: bool) -> Self {
        let client = builder.no_archive().build().unwrap();
        if raw {
            Sub::Raw(client.subscribe_raw())
        } else {
            Sub::Typed(client.subscribe())
        }
    }

    /// The next item as a string; checks the payload of checkpoints.
    async fn next(&mut self) -> Option<String> {
        let show_err = |e: miraicloud::streams::Error| format!("error {e}");
        Some(match self {
            Sub::Typed(s) => match s.next().await? {
                Ok(Event::Checkpoint { seq, checkpoint }) => {
                    assert_eq!(
                        checkpoint.digest.as_deref(),
                        Some(format!("d{seq}").as_str())
                    );
                    seq.to_string()
                }
                Ok(Event::Gap { from, to }) => format!("gap {from}-{to}"),
                Ok(other) => panic!("unexpected {other:?}"),
                Err(e) => show_err(e),
            },
            Sub::Raw(s) => match s.next().await? {
                Ok(RawEvent::Checkpoint { seq, data }) => {
                    assert_eq!(data, encoded(seq), "exact bytes of {seq}");
                    seq.to_string()
                }
                Ok(RawEvent::Gap { from, to }) => format!("gap {from}-{to}"),
                Ok(other) => panic!("unexpected {other:?}"),
                Err(e) => show_err(e),
            },
        })
    }

    fn last_delivered(&self) -> Option<u64> {
        match self {
            Sub::Typed(s) => s.last_delivered(),
            Sub::Raw(s) => s.last_delivered(),
        }
    }

    fn cursor(&self) -> Option<u64> {
        match self {
            Sub::Typed(s) => s.cursor(),
            Sub::Raw(s) => s.cursor(),
        }
    }
}

async fn take(sub: &mut Sub, n: usize) -> Vec<String> {
    let mut out = vec![];
    while out.len() < n {
        let ev = tokio::time::timeout(Duration::from_secs(20), sub.next())
            .await
            .expect("progress")
            .expect("open");
        out.push(ev);
    }
    out
}

/// `#[tokio::test]`s `<name>` (typed) and `<name>_raw` for `async fn name(raw: bool)`.
macro_rules! both_modes {
    ($($name:ident),* $(,)?) => {
        mod typed {
            $(#[tokio::test]
            async fn $name() {
                super::$name(false).await
            })*
        }
        mod raw {
            $(#[tokio::test]
            async fn $name() {
                super::$name(true).await
            })*
        }
    };
}

both_modes!(
    masks_are_applied_locally,
    resumes_with_from_across_closes_and_gaps,
    tip_start,
    too_old_without_archive_is_a_gap,
    non_zstd_accept_encoding_is_fatal,
    retry_after_is_honoured_and_400_is_fatal,
    over_quota_is_fatal_a_full_stream_is_retried,
    stall_triggers_reconnect,
    dropping_the_subscription_closes_the_connection,
    slow_consumer_is_not_a_stall,
);

async fn masks_are_applied_locally(raw: bool) {
    let mock = Mock::default();
    let url = mock.serve(vec![frames(&[cp(10)], true)]).await;
    let client = miraicloud::streams::Client::builder()
        .url(url)
        .read_mask(["sequence_number"])
        .start_at(10)
        .no_archive()
        .build()
        .unwrap();
    let checkpoint = if raw {
        let mut sub = client.subscribe_raw();
        match sub.next().await.unwrap().unwrap() {
            RawEvent::Checkpoint { seq, data } => {
                assert_eq!(seq, 10);
                Checkpoint::decode(&data[..]).unwrap()
            }
            other => panic!("{other:?}"),
        }
    } else {
        let mut sub = client.subscribe();
        match sub.next().await.unwrap().unwrap() {
            Event::Checkpoint { checkpoint, .. } => *checkpoint,
            other => panic!("{other:?}"),
        }
    };
    let mut want = Checkpoint::default();
    want.sequence_number = Some(10);
    assert_eq!(checkpoint, want, "digest and unknown fields pruned");
    let reqs = mock.requests.lock().unwrap().clone();
    assert_eq!(reqs[0].1, None, "the mask is not sent");
}

async fn resumes_with_from_across_closes_and_gaps(raw: bool) {
    let mock = Mock::default();
    let url = mock
        .serve(vec![
            frames(&[cp(10), cp(11)], false),
            Reply::Status(StatusCode::SERVICE_UNAVAILABLE, None),
            frames(&[cp(11), cp(12), gap(13, 15)], false),
            frames(&[cp(16)], true),
        ])
        .await;
    let mut sub = Sub::new(
        miraicloud::streams::Client::builder().url(url).start_at(10),
        raw,
    );
    assert_eq!(
        take(&mut sub, 5).await,
        ["10", "11", "12", "gap 13-15", "16"]
    );
    assert_eq!(sub.last_delivered(), Some(16));
    assert_eq!(sub.cursor(), Some(16));
    let reqs = mock.requests.lock().unwrap().clone();
    let from: Vec<_> = reqs.iter().map(|r| r.0.clone()).collect();
    assert_eq!(
        from,
        [
            Some("10".into()),
            Some("12".into()),
            Some("12".into()),
            Some("16".into())
        ]
    );
    for r in &reqs {
        assert_eq!(r.1, None, "masks are applied locally, never sent");
        assert_eq!(r.2, "zstd");
        assert_eq!(r.3, "application/vnd.miraicloud.checkpoints.v1+proto");
    }
}

async fn tip_start(raw: bool) {
    let mock = Mock::default();
    let url = mock
        .serve(vec![frames(&[cp(100)], false), frames(&[cp(101)], true)])
        .await;
    let mut sub = Sub::new(miraicloud::streams::Client::builder().url(url), raw);
    assert_eq!(take(&mut sub, 2).await, ["100", "101"]);
    let reqs = mock.requests.lock().unwrap().clone();
    assert_eq!(reqs[0].0, None);
    assert_eq!(reqs[1].0.as_deref(), Some("101"));
    // No mask: full checkpoint.
    assert_eq!(reqs[0].1, None);
}

async fn too_old_without_archive_is_a_gap(raw: bool) {
    let mock = Mock::default();
    let url = mock
        .serve(vec![Reply::TooOld(20), frames(&[cp(20), cp(21)], true)])
        .await;
    let mut sub = Sub::new(
        miraicloud::streams::Client::builder().url(url).start_at(5),
        raw,
    );
    assert_eq!(take(&mut sub, 3).await, ["gap 5-19", "20", "21"]);
    let reqs = mock.requests.lock().unwrap().clone();
    assert_eq!(reqs[1].0.as_deref(), Some("20"));
}

async fn non_zstd_accept_encoding_is_fatal(raw: bool) {
    let mock = Mock::default();
    let url = mock.serve(vec![frames(&[cp(1), cp(2)], true)]).await;
    let mut sub = Sub::new(
        miraicloud::streams::Client::builder()
            .url(url)
            .header("accept-encoding", "gzip")
            .start_at(1),
        raw,
    );
    let got = take(&mut sub, 1).await;
    assert!(
        got[0].starts_with("error stream rejected the request: HTTP 400"),
        "{got:?}"
    );
    assert!(sub.next().await.is_none(), "ends after a fatal error");
    assert_eq!(mock.requests.lock().unwrap().len(), 1, "not retried");
}

async fn retry_after_is_honoured_and_400_is_fatal(raw: bool) {
    let mock = Mock::default();
    let url = mock
        .serve(vec![
            Reply::Status(StatusCode::TOO_MANY_REQUESTS, Some("2")),
            frames(&[cp(5)], false),
            Reply::Status(StatusCode::BAD_REQUEST, None),
        ])
        .await;
    let t0 = std::time::Instant::now();
    let mut sub = Sub::new(
        miraicloud::streams::Client::builder().url(url).start_at(5),
        raw,
    );
    let got = take(&mut sub, 2).await;
    assert_eq!(got[0], "5");
    assert!(
        t0.elapsed() >= Duration::from_secs(2),
        "waited for Retry-After"
    );
    assert!(got[1].starts_with("error stream rejected the request: HTTP 400"));
    assert!(sub.next().await.is_none(), "ends after a fatal error");
}

async fn over_quota_is_fatal_a_full_stream_is_retried(raw: bool) {
    let mock = Mock::default();
    let url = mock
        .serve(vec![
            Reply::Json(
                StatusCode::TOO_MANY_REQUESTS,
                r#"{"_tag":"Full","message":"stream limit reached, retry shortly"}"#,
            ),
            frames(&[cp(5)], false),
            Reply::Json(
                StatusCode::TOO_MANY_REQUESTS,
                r#"{"_tag":"QuotaExceeded","error":"QuotaExceeded","message":"over quota"}"#,
            ),
        ])
        .await;
    let mut sub = Sub::new(
        miraicloud::streams::Client::builder()
            .url(url)
            .api_key("mc1_test")
            .start_at(5),
        raw,
    );
    let got = take(&mut sub, 2).await;
    assert_eq!(got[0], "5");
    assert!(
        got[1].starts_with("error stream rejected the request: HTTP 429"),
        "{got:?}"
    );
    assert!(sub.next().await.is_none(), "ends after a fatal error");
    assert_eq!(mock.requests.lock().unwrap().len(), 3);
}

async fn stall_triggers_reconnect(raw: bool) {
    let mock = Mock::default();
    // The first connection sends one checkpoint and then nothing.
    let url = mock
        .serve(vec![frames(&[cp(7)], true), frames(&[cp(8)], true)])
        .await;
    let mut sub = Sub::new(
        miraicloud::streams::Client::builder()
            .url(url)
            .start_at(7)
            .stall_timeout(Duration::from_millis(500)),
        raw,
    );
    assert_eq!(take(&mut sub, 2).await, ["7", "8"]);
    assert_eq!(mock.requests.lock().unwrap()[1].0.as_deref(), Some("8"));
}

async fn dropping_the_subscription_closes_the_connection(raw: bool) {
    let mock = Mock::default();
    let url = mock.serve(vec![frames(&[cp(1)], true)]).await;
    let mut sub = Sub::new(
        miraicloud::streams::Client::builder().url(url).start_at(1),
        raw,
    );
    assert_eq!(take(&mut sub, 1).await, ["1"]);
    assert_eq!(mock.closed.load(Ordering::SeqCst), 0);
    drop(sub);
    let t0 = std::time::Instant::now();
    while mock.closed.load(Ordering::SeqCst) == 0 {
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "connection still open"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(mock.requests.lock().unwrap().len(), 1);
}

async fn slow_consumer_is_not_a_stall(raw: bool) {
    let mock = Mock::default();
    let url = mock.serve(vec![frames(&[cp(1), cp(2), cp(3)], true)]).await;
    let mut sub = Sub::new(
        miraicloud::streams::Client::builder()
            .url(url)
            .stall_timeout(Duration::from_millis(100)),
        raw,
    );
    // Slower than the stall timeout between items, but done well before the
    // earliest reconnect once the (hanging) body really stalls: 100 ms + the
    // first backoff, jittered down to 500 ms.
    for (i, want) in ["1", "2", "3"].into_iter().enumerate() {
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        assert_eq!(take(&mut sub, 1).await, [want]);
    }
    assert_eq!(mock.requests.lock().unwrap().len(), 1);
}

/// The recorded lean fixture through the mock: raw payloads are the recorded
/// bytes, typed checkpoints their decoding.
#[tokio::test]
async fn serves_recorded_fixture() {
    let path = format!(
        "{}/tests/fixtures/lean.frames.zst",
        env!("CARGO_MANIFEST_DIR")
    );
    let body = zstd::decode_all(std::fs::File::open(path).unwrap()).unwrap();
    // Split into stream frames; (seq, encoded checkpoint) as recorded.
    let (mut list, mut want) = (vec![], vec![]);
    let mut rest = &body[..];
    while !rest.is_empty() {
        let len = u32::from_be_bytes(rest[1..5].try_into().unwrap()) as usize;
        let (f, tail) = rest.split_at(5 + len);
        let r = SubscribeCheckpointsResponse::decode(&f[5..]).unwrap();
        want.push((r.cursor.unwrap(), r.checkpoint.unwrap()));
        list.push(f.to_vec());
        rest = tail;
    }
    assert_eq!(want.len(), 24);
    let mock = Mock::default();
    let reply = Reply::Frames {
        frames: list,
        hang: true,
    };
    let url = mock.serve(vec![reply.clone(), reply]).await;
    let client = miraicloud::streams::Client::builder()
        .url(url)
        .no_archive()
        .start_at(want[0].0)
        .build()
        .unwrap();
    let mut raw = client.subscribe_raw();
    for (seq, cp) in &want {
        let ev = tokio::time::timeout(Duration::from_secs(20), raw.next()).await;
        let Ok(Some(Ok(RawEvent::Checkpoint { seq: s, data }))) = ev else {
            panic!("{seq}: {ev:?}")
        };
        assert_eq!(s, *seq);
        assert_eq!(Checkpoint::decode(&data[..]).unwrap(), *cp);
    }
    let mut typed = client.subscribe();
    for (seq, cp) in &want {
        let ev = tokio::time::timeout(Duration::from_secs(20), typed.next()).await;
        assert_eq!(
            ev.unwrap().unwrap().unwrap(),
            Event::Checkpoint {
                seq: *seq,
                checkpoint: Box::new(cp.clone())
            }
        );
    }
}

/// A `404` before the stream has once answered (a process started during a
/// deploy) is retried a few times before it is final.
#[tokio::test]
async fn not_found_before_the_first_answer_is_retried() {
    let not_found = || {
        Reply::Json(
            StatusCode::NOT_FOUND,
            r#"{"_tag":"NotFound","message":"no route"}"#,
        )
    };
    let mock = Mock::default();
    let url = mock
        .serve(vec![not_found(), not_found(), frames(&[cp(5)], true)])
        .await;
    let mut sub = Sub::new(
        miraicloud::streams::Client::builder().url(url).start_at(5),
        false,
    );
    assert_eq!(take(&mut sub, 1).await, ["5"]);
    assert_eq!(mock.requests.lock().unwrap().len(), 3);
}

/// ... and final after `NOT_FOUND_ATTEMPTS` (6) in all, with the error's tag.
#[tokio::test]
async fn not_found_is_final_after_six_attempts_with_its_tag() {
    use miraicloud::streams::Error;
    let not_found = || {
        Reply::Json(
            StatusCode::NOT_FOUND,
            r#"{"_tag":"NotFound","message":"no route"}"#,
        )
    };
    let mock = Mock::default();
    let url = mock.serve((0..7).map(|_| not_found()).collect()).await;
    let mut sub = miraicloud::streams::Client::builder()
        .url(url)
        .start_at(5)
        .build()
        .unwrap()
        .subscribe();
    let err = tokio::time::timeout(Duration::from_secs(60), sub.next())
        .await
        .expect("final within a minute")
        .expect("an error")
        .expect_err("refused");
    match err {
        Error::Http { status, tag, .. } => {
            assert_eq!(status, 404);
            assert_eq!(tag.as_deref(), Some("NotFound"));
        }
        other => panic!("{other:?}"),
    }
    assert!(sub.next().await.is_none(), "ends after a fatal error");
    assert_eq!(mock.requests.lock().unwrap().len(), 6);
}

/// A builder's `Debug` names headers but never prints their values.
#[test]
fn builder_debug_hides_the_api_key() {
    let b = miraicloud::streams::Client::builder()
        .api_key("mc1_secret_key")
        .header("x-other", "also-secret");
    let shown = format!("{b:?}");
    assert!(
        shown.contains("authorization") && shown.contains("x-other"),
        "{shown}"
    );
    assert!(!shown.contains("secret"), "{shown}");
}
