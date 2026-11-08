//! Fault injection against a local mock, with the production wire shape:
//! one open zstd frame spanning several checkpoints, flushed per checkpoint,
//! 3-byte empty raw blocks inside it, raw-block keepalive frames (as the
//! server's `wire.ts` builds them), a 16 MiB window; a reset mid-frame; a
//! `TooManyStreams` 429; a corrupt frame.

use std::collections::VecDeque;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures::StreamExt;
use miraicloud::streams::{Checkpoint, Event};
use prost::Message;
use sui_rpc::proto::sui::rpc::v2::SubscribeCheckpointsResponse;

#[derive(Clone)]
enum Piece {
    Bytes(Vec<u8>),
    Sleep(u64),
    Error,
}

#[derive(Clone)]
enum Reply {
    /// 200, `content-encoding: zstd`, these pieces, then close (or hang).
    Body {
        pieces: Vec<Piece>,
        hang: bool,
    },
    Json(StatusCode, &'static str, Option<&'static str>),
}

/// (`from`, when the request arrived).
type Seen = (Option<String>, Instant);

#[derive(Clone, Default)]
struct Mock {
    replies: Arc<Mutex<VecDeque<Reply>>>,
    requests: Arc<Mutex<Vec<Seen>>>,
}

#[derive(serde::Deserialize)]
struct Params {
    from: Option<String>,
}

async fn handler(State(m): State<Mock>, headers: HeaderMap, Query(p): Query<Params>) -> Response {
    m.requests.lock().unwrap().push((p.from, Instant::now()));
    let accept = headers
        .get("accept-encoding")
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_default();
    assert!(accept.contains("zstd"), "accept-encoding: {accept}");
    match m.replies.lock().unwrap().pop_front() {
        None => Body::from_stream(futures::stream::pending::<Result<Vec<u8>, std::io::Error>>())
            .into_response(),
        Some(Reply::Json(code, body, retry_after)) => {
            let mut r = (code, body).into_response();
            r.headers_mut()
                .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
            if let Some(v) = retry_after {
                r.headers_mut()
                    .insert(header::RETRY_AFTER, v.parse().unwrap());
            }
            r
        }
        Some(Reply::Body { pieces, hang }) => {
            let stream = futures::stream::iter(pieces)
                .then(|p| async move {
                    match p {
                        Piece::Bytes(b) => Some(Ok::<_, std::io::Error>(b)),
                        Piece::Sleep(ms) => {
                            tokio::time::sleep(Duration::from_millis(ms)).await;
                            None
                        }
                        Piece::Error => Some(Err(std::io::Error::other("reset"))),
                    }
                })
                .filter_map(futures::future::ready);
            let stream = if hang {
                stream.chain(futures::stream::pending()).boxed()
            } else {
                stream.boxed()
            };
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

    fn froms(&self) -> Vec<Option<String>> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.0.clone())
            .collect()
    }
}

fn frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut f = vec![kind];
    f.extend((body.len() as u32).to_be_bytes());
    f.extend(body);
    f
}

/// A checkpoint frame with a payload of about `size` bytes (compressible but not trivially).
fn cp(seq: u64, size: usize) -> Vec<u8> {
    let mut c = Checkpoint::default();
    c.sequence_number = Some(seq);
    c.digest = Some(format!("d{seq}"));
    let mut summary = sui_rpc::proto::sui::rpc::v2::CheckpointSummary::default();
    summary.previous_digest = Some(
        (0..size)
            .map(|i| char::from(b'a' + ((i * 7 + seq as usize) % 26) as u8))
            .collect(),
    );
    c.summary = Some(summary);
    let mut body = SubscribeCheckpointsResponse::default();
    body.cursor = Some(seq);
    body.checkpoint = Some(c);
    frame(0, &body.encode_to_vec())
}

/// `wire.ts`'s `zstdRaw`: one zstd frame holding `data` uncompressed, single segment.
fn zstd_raw(data: &[u8]) -> Vec<u8> {
    let mut out = vec![0x28, 0xb5, 0x2f, 0xfd, 0xa0];
    out.extend((data.len() as u32).to_le_bytes());
    let header = ((data.len() as u32) << 3) | 1;
    out.extend(&header.to_le_bytes()[..3]);
    out.extend(data);
    out
}

fn keepalive_frame() -> Vec<u8> {
    zstd_raw(&frame(2, &[]))
}

/// `wire.ts`'s `EMPTY_BLOCK`: a zstd empty raw block, not the last of its frame.
const EMPTY_BLOCK: [u8; 3] = [0, 0, 0];

/// One zstd frame (window 2^`window_log`) over `frames`, flushed after each:
/// the chunks the server writes, the frame still open after the last.
fn open_frame(frames: &[Vec<u8>], window_log: u32) -> Vec<Vec<u8>> {
    let mut enc = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
    enc.set_parameter(zstd::stream::raw::CParameter::WindowLog(window_log))
        .unwrap();
    let mut chunks = vec![];
    for f in frames {
        enc.write_all(f).unwrap();
        enc.flush().unwrap();
        chunks.push(std::mem::take(enc.get_mut()));
    }
    chunks
}

async fn take(sub: &mut miraicloud::streams::Subscription, n: usize) -> Vec<String> {
    let mut out = vec![];
    while out.len() < n {
        let ev = tokio::time::timeout(Duration::from_secs(20), sub.next())
            .await
            .expect("progress")
            .expect("open");
        out.push(match ev {
            Ok(Event::Checkpoint { seq, checkpoint }) => {
                assert_eq!(checkpoint.sequence_number, Some(seq));
                assert_eq!(
                    checkpoint.digest.as_deref(),
                    Some(format!("d{seq}").as_str())
                );
                seq.to_string()
            }
            Ok(Event::Gap { from, to }) => format!("gap {from}-{to}"),
            Ok(other) => panic!("unexpected {other:?}"),
            Err(e) => format!("error {e}"),
        });
    }
    out
}

fn client(url: &str) -> miraicloud::streams::Builder {
    miraicloud::streams::Client::builder()
        .url(url)
        .api_key("mc1_test")
        .no_archive()
        .stall_timeout(Duration::from_secs(2))
}

/// Checkpoints inside one open 16 MiB-window zstd frame, flushed per
/// checkpoint with empty blocks in between, must each be delivered as soon
/// as their chunk arrives (not when the frame closes).
#[tokio::test]
async fn open_frame_streams_per_checkpoint_with_empty_blocks() {
    let frames: Vec<Vec<u8>> = (1..=5).map(|s| cp(s, 200_000)).collect();
    let chunks = open_frame(&frames, 24);
    let mut pieces = vec![Piece::Bytes(keepalive_frame())];
    // A fresh zstd frame can only start after the open one is closed; the
    // server does so by 16 checkpoints. Here: the open frame runs to the end.
    for (i, c) in chunks.into_iter().enumerate() {
        pieces.push(Piece::Bytes(c));
        pieces.push(Piece::Bytes(EMPTY_BLOCK.to_vec()));
        pieces.push(Piece::Sleep(100));
        pieces.push(Piece::Bytes(EMPTY_BLOCK.to_vec()));
        if i == 2 {
            // A long pause: only empty blocks arrive (no decompressed bytes).
            pieces.push(Piece::Sleep(600));
        }
    }
    let m = Mock::default();
    let url = m.serve(vec![Reply::Body { pieces, hang: true }]).await;
    let mut sub = client(&url).start_at(1).build().unwrap().subscribe();
    let t0 = Instant::now();
    let got = take(&mut sub, 5).await;
    assert_eq!(got, ["1", "2", "3", "4", "5"]);
    // Five checkpoints across ~1.1 s of pauses: delivered as they came (not after a stall/reconnect).
    assert!(t0.elapsed() < Duration::from_secs(4), "{:?}", t0.elapsed());
    assert_eq!(m.froms(), [Some("1".into())], "no reconnect");
    assert_eq!(sub.cursor(), Some(5));
}

/// The connection resets half-way through a checkpoint's bytes: resume from
/// the cursor, the overlap dropped, nothing skipped or repeated.
#[tokio::test]
async fn reset_mid_frame_resumes_from_the_cursor() {
    let frames: Vec<Vec<u8>> = (1..=3).map(|s| cp(s, 50_000)).collect();
    let chunks = open_frame(&frames, 20);
    let mut pieces = vec![
        Piece::Bytes(chunks[0].clone()),
        Piece::Bytes(chunks[1].clone()),
    ];
    let half = chunks[2].len() / 2;
    pieces.push(Piece::Bytes(chunks[2][..half].to_vec()));
    pieces.push(Piece::Sleep(50));
    pieces.push(Piece::Error);
    let second: Vec<Vec<u8>> = (2..=4).map(|s| cp(s, 50_000)).collect();
    let second = open_frame(&second, 20);
    let m = Mock::default();
    let url = m
        .serve(vec![
            Reply::Body {
                pieces,
                hang: false,
            },
            Reply::Body {
                pieces: second.into_iter().map(Piece::Bytes).collect(),
                hang: true,
            },
        ])
        .await;
    let mut sub = client(&url).start_at(1).build().unwrap().subscribe();
    let got = take(&mut sub, 4).await;
    assert_eq!(got, ["1", "2", "3", "4"]);
    assert_eq!(m.froms(), [Some("1".into()), Some("3".into())]);
}

/// A truncated body (close mid-frame, no error) is also just a reconnect.
#[tokio::test]
async fn truncated_close_mid_frame_resumes_from_the_cursor() {
    let chunks = open_frame(&[cp(1, 50_000), cp(2, 50_000)], 20);
    let half = chunks[1].len() / 2;
    let pieces = vec![
        Piece::Bytes(chunks[0].clone()),
        Piece::Bytes(chunks[1][..half].to_vec()),
    ];
    let second = open_frame(&[cp(2, 50_000), cp(3, 50_000)], 20);
    let m = Mock::default();
    let url = m
        .serve(vec![
            Reply::Body {
                pieces,
                hang: false,
            },
            Reply::Body {
                pieces: second.into_iter().map(Piece::Bytes).collect(),
                hang: true,
            },
        ])
        .await;
    let mut sub = client(&url).start_at(1).build().unwrap().subscribe();
    assert_eq!(take(&mut sub, 3).await, ["1", "2", "3"]);
    assert_eq!(m.froms(), [Some("1".into()), Some("2".into())]);
}

/// The account's concurrent-stream cap (`429 TooManyStreams`, `Retry-After: 1`) is retried, not final, after the wait.
#[tokio::test]
async fn too_many_streams_is_retried_after_retry_after() {
    let m = Mock::default();
    let url = m
        .serve(vec![
            Reply::Json(
                StatusCode::TOO_MANY_REQUESTS,
                r#"{"_tag":"TooManyStreams","message":"this account has 2 checkpoint streams open","open":2,"limit":2}"#,
                Some("1"),
            ),
            Reply::Body {
                pieces: open_frame(&[cp(7, 1000)], 20).into_iter().map(Piece::Bytes).collect(),
                hang: true,
            },
        ])
        .await;
    let mut sub = client(&url).start_at(7).build().unwrap().subscribe();
    assert_eq!(take(&mut sub, 1).await, ["7"]);
    let reqs = m.requests.lock().unwrap().clone();
    assert_eq!(reqs.len(), 2);
    let waited = reqs[1].1 - reqs[0].1;
    assert!(waited >= Duration::from_secs(1), "{waited:?}");
}

/// The gateway's connect rate limit (`429 RateLimited`, `Retry-After: 60`): retried, waiting the full minute.
#[tokio::test]
async fn rate_limited_connect_waits_retry_after() {
    tokio::time::pause();
    let m = Mock::default();
    let url = m
        .serve(vec![
            Reply::Json(
                StatusCode::TOO_MANY_REQUESTS,
                r#"{"_tag":"RateLimited","error":"RateLimited","message":"too many new stream connections"}"#,
                Some("60"),
            ),
            Reply::Body {
                pieces: open_frame(&[cp(7, 1000)], 20).into_iter().map(Piece::Bytes).collect(),
                hang: true,
            },
        ])
        .await;
    let mut sub = client(&url).start_at(7).build().unwrap().subscribe();
    tokio::time::resume();
    let t0 = Instant::now();
    // Real time: the mock is real I/O. Give the first request time to land, then check nothing came back early.
    let early = tokio::time::timeout(Duration::from_secs(3), sub.next()).await;
    assert!(early.is_err(), "retried before Retry-After: {early:?}");
    assert_eq!(m.froms().len(), 1);
    let _ = t0;
}

/// A corrupt frame (a gap frame of the wrong length) after a good checkpoint: reconnect from the cursor, deliver on.
#[tokio::test]
async fn corrupt_frame_reconnects_from_the_cursor() {
    let bad = frame(1, &[1, 2, 3]);
    let first = open_frame(&[cp(1, 1000), bad], 20);
    let second = open_frame(&[cp(2, 1000), cp(3, 1000)], 20);
    let m = Mock::default();
    let url = m
        .serve(vec![
            Reply::Body {
                pieces: first.into_iter().map(Piece::Bytes).collect(),
                hang: true,
            },
            Reply::Body {
                pieces: second.into_iter().map(Piece::Bytes).collect(),
                hang: true,
            },
        ])
        .await;
    let mut sub = client(&url).start_at(1).build().unwrap().subscribe();
    assert_eq!(take(&mut sub, 3).await, ["1", "2", "3"]);
    assert_eq!(m.froms(), [Some("1".into()), Some("2".into())]);
}

/// Corrupt zstd bytes (not a frame-level error): reconnect, not a terminal error.
#[tokio::test]
async fn corrupt_zstd_reconnects() {
    let mut first = open_frame(&[cp(1, 1000)], 20);
    first.push(vec![0xde, 0xad, 0xbe, 0xef, 0x11, 0x22, 0x33, 0x44, 0x55]);
    let second = open_frame(&[cp(2, 1000)], 20);
    let m = Mock::default();
    let url = m
        .serve(vec![
            Reply::Body {
                pieces: first.into_iter().map(Piece::Bytes).collect(),
                hang: true,
            },
            Reply::Body {
                pieces: second.into_iter().map(Piece::Bytes).collect(),
                hang: true,
            },
        ])
        .await;
    let mut sub = client(&url).start_at(1).build().unwrap().subscribe();
    assert_eq!(take(&mut sub, 2).await, ["1", "2"]);
    assert_eq!(m.froms(), [Some("1".into()), Some("2".into())]);
}

/// A deploy: the server closes every stream at once (clean close, no error). Reconnect from the cursor within ~1 s.
#[tokio::test]
async fn clean_close_reconnects_within_a_second() {
    let first = open_frame(&[cp(1, 1000), cp(2, 1000)], 20);
    let second = open_frame(&[cp(3, 1000)], 20);
    let m = Mock::default();
    let url = m
        .serve(vec![
            Reply::Body {
                pieces: first.into_iter().map(Piece::Bytes).collect(),
                hang: false,
            },
            Reply::Body {
                pieces: second.into_iter().map(Piece::Bytes).collect(),
                hang: true,
            },
        ])
        .await;
    let mut sub = client(&url).start_at(1).build().unwrap().subscribe();
    assert_eq!(take(&mut sub, 3).await, ["1", "2", "3"]);
    let reqs = m.requests.lock().unwrap().clone();
    let gap = reqs[1].1 - reqs[0].1;
    assert!(
        gap >= Duration::from_millis(400) && gap <= Duration::from_millis(1500),
        "{gap:?}"
    );
}
