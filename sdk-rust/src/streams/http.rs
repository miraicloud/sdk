//! The stream connection: the reqwest-based [`Source`] and the body →
//! events adapter (zstd is decoded by reqwest; frames by [`Parser`]).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use crate::auth::Auth;
use crate::streams::Error;
use crate::streams::frame::{CONTENT_TYPE, Frame, Parser};

/// An event from one connection; checkpoints are not decoded yet.
#[derive(Debug)]
pub(crate) enum StreamEvent {
    /// Sequence number (the frame's `cursor`) and the encoded `Checkpoint`.
    Checkpoint(u64, Bytes),
    Gap {
        from: u64,
        to: u64,
    },
}

/// Why a connection ended or failed.
#[derive(Debug)]
pub(crate) enum ConnError {
    /// Reconnect after backoff (at least `retry_after` if set).
    Retry(String, Option<Duration>),
    /// `410`: the position is older than the stream holds, which starts
    /// at `oldest`.
    TooOld { oldest: u64 },
    /// Give up: the request itself is wrong.
    Fatal(Error),
    /// The connection's token is about to expire: reconnect now, with a
    /// fresh one (no backoff).
    Renew,
}

pub(crate) type EventStream = BoxStream<'static, Result<StreamEvent, ConnError>>;

/// Opens connections. Abstracted for tests.
pub(crate) trait Source: Send + Sync + 'static {
    /// Connect starting at checkpoint `next` (inclusive); `None` = tip.
    fn connect(&self, next: Option<u64>) -> BoxFuture<'static, Result<EventStream, ConnError>>;
}

/// The real endpoint.
pub(crate) struct HttpSource {
    client: reqwest::Client,
    url: String,
    headers: HeaderMap,
    stall_timeout: Duration,
    /// Set once the stream has answered `200`: after that a `404` is the
    /// service's transient state (a deploy, a route moving), not a wrong
    /// URL, so it is retried rather than final.
    served: Arc<AtomicBool>,
    /// Computes `authorization` per connection (overrides `headers`).
    auth: Option<Auth>,
    /// Consecutive `401`s with an exchanged token: the first one renews the
    /// token and retries, the next is final.
    refused: Arc<AtomicU32>,
}

impl HttpSource {
    pub fn new(
        url: String,
        headers: &[(String, String)],
        auth: Option<Auth>,
        stall_timeout: Duration,
    ) -> Result<Self, Error> {
        let mut map = HeaderMap::new();
        for (k, v) in headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| Error::Config(format!("header name {k:?}: {e}")))?;
            let mut value = HeaderValue::from_str(v)
                .map_err(|e| Error::Config(format!("header {k} value: {e}")))?;
            value.set_sensitive(true);
            map.insert(name, value);
        }
        if auth.is_some() {
            // Sent per connection instead.
            map.remove(reqwest::header::AUTHORIZATION);
        }
        // reqwest (zstd feature only) sends `Accept-Encoding: zstd`, the
        // stream's encoding, and decodes the concatenated zstd frames
        // transparently.
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| Error::Config(format!("http client: {e}")))?;
        reqwest::Url::parse(&url).map_err(|e| Error::Config(format!("stream url {url:?}: {e}")))?;
        if stall_timeout.is_zero() {
            return Err(Error::Config("stall timeout must be positive".into()));
        }
        Ok(Self {
            client,
            url,
            headers: map,
            stall_timeout,
            served: Arc::new(AtomicBool::new(false)),
            auth,
            refused: Arc::new(AtomicU32::new(0)),
        })
    }
}

impl HttpSource {
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Source for HttpSource {
    fn connect(&self, next: Option<u64>) -> BoxFuture<'static, Result<EventStream, ConnError>> {
        let mut req = self
            .client
            .get(&self.url)
            .headers(self.headers.clone())
            .header(reqwest::header::ACCEPT, CONTENT_TYPE);
        if let Some(next) = next {
            req = req.query(&[("from", next)]);
        }
        let stall = self.stall_timeout;
        let served = self.served.clone();
        let auth = self.auth.clone();
        let refused = self.refused.clone();
        Box::pin(async move {
            // The token this connection presents, and when to renew it.
            let mut credential = None;
            if let Some(auth) = &auth {
                let c = match auth.credential().await {
                    Ok(c) => c,
                    Err(e) if e.is_transient() => {
                        return Err(ConnError::Retry(format!("auth: {e}"), None));
                    }
                    Err(e) => return Err(ConnError::Fatal(Error::Auth(e))),
                };
                let mut value = HeaderValue::from_str(&format!("Bearer {}", c.token))
                    .map_err(|e| ConnError::Fatal(Error::Config(format!("token: {e}"))))?;
                value.set_sensitive(true);
                req = req.header(reqwest::header::AUTHORIZATION, value);
                credential = Some(c);
            }
            let resp = tokio::time::timeout(stall, req.send())
                .await
                .map_err(|_| ConnError::Retry("no response headers".into(), None))?
                .map_err(|e| ConnError::Retry(format!("request: {e}"), None))?;
            let status = resp.status();
            if status == StatusCode::GONE {
                let oldest = resp
                    .headers()
                    .get("oldest-checkpoint")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.trim().parse::<u64>().ok());
                let body = resp.text().await.unwrap_or_default();
                return Err(match oldest {
                    Some(oldest) => ConnError::TooOld { oldest },
                    None => ConnError::Retry(
                        format!("HTTP 410 without oldest-checkpoint: {body}"),
                        None,
                    ),
                });
            }
            if !status.is_success() {
                let retry_after = resp
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .map(Duration::from_secs);
                let body = resp.text().await.unwrap_or_default();
                // An exchanged token refused (revoked meanwhile, or a
                // clock off): renew it once before giving up.
                if status == StatusCode::UNAUTHORIZED
                    && let (Some(auth @ Auth::Delegated(_)), Some(c)) = (&auth, &credential)
                    && refused.fetch_add(1, Ordering::Relaxed) == 0
                {
                    auth.reject(&c.token).await;
                    return Err(ConnError::Retry(
                        format!("HTTP {status}: {body}; renewing the token"),
                        None,
                    ));
                }
                // A 429 is final only when the account or key is over its
                // quota (the gateway's JSON `{"_tag":"QuotaExceeded",…}`);
                // otherwise (the stream is full) it is retried.
                let fatal = matches!(
                    status,
                    StatusCode::BAD_REQUEST | StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
                ) || (status == StatusCode::NOT_FOUND
                    && !served.load(Ordering::Relaxed))
                    || (status == StatusCode::TOO_MANY_REQUESTS && is_quota_exceeded(&body));
                return Err(if fatal {
                    ConnError::Fatal(Error::Http {
                        status: status.as_u16(),
                        body,
                    })
                } else {
                    ConnError::Retry(format!("HTTP {status}: {body}"), retry_after)
                });
            }
            served.store(true, Ordering::Relaxed);
            refused.store(0, Ordering::Relaxed);
            tracing::debug!(
                encoding = ?resp.headers().get("content-encoding"),
                ?next,
                "stream connected"
            );
            let events = events(
                resp.bytes_stream().map_err(|e| e.to_string()).boxed(),
                stall,
            );
            Ok(match credential.and_then(|c| c.renew) {
                Some(renew) => renew_when(events, renew),
                None => events,
            })
        })
    }
}

/// Whether a refusal's body is the gateway's `QuotaExceeded` error.
fn is_quota_exceeded(body: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| {
            v.get("_tag")
                .and_then(|t| t.as_str())
                .map(|t| t == "QuotaExceeded")
        })
        .unwrap_or(false)
}

/// `events` until `renew` resolves (it renews the token meanwhile), then
/// [`ConnError::Renew`] and the end.
pub(crate) fn renew_when(events: EventStream, renew: BoxFuture<'static, ()>) -> EventStream {
    futures::stream::unfold(
        (events, renew, false),
        |(mut events, mut renew, done)| async move {
            if done {
                return None;
            }
            tokio::select! {
                biased;
                _ = &mut renew => Some((Err(ConnError::Renew), (events, renew, true))),
                item = events.next() => item.map(|item| (item, (events, renew, false))),
            }
        },
    )
    .boxed()
}

/// Turn a body into events, failing on `stall` without any bytes.
pub(crate) fn events(
    body: BoxStream<'static, Result<Bytes, String>>,
    stall: Duration,
) -> EventStream {
    struct State {
        body: BoxStream<'static, Result<Bytes, String>>,
        parser: Parser,
        /// Frames parsed from the last chunk, in reverse (pop from the end).
        ready: Vec<Frame>,
        done: bool,
    }
    let state = State {
        body,
        parser: Parser::default(),
        ready: Vec::new(),
        done: false,
    };
    futures::stream::unfold(state, move |mut st| async move {
        loop {
            if st.done {
                return None;
            }
            while let Some(frame) = st.ready.pop() {
                match frame {
                    Frame::Checkpoint { seq, checkpoint } => {
                        return Some((Ok(StreamEvent::Checkpoint(seq, checkpoint)), st));
                    }
                    Frame::Gap { from, to } => {
                        return Some((Ok(StreamEvent::Gap { from, to }), st));
                    }
                    Frame::Other => {}
                }
            }
            let chunk = match tokio::time::timeout(stall, st.body.next()).await {
                Err(_) => {
                    st.done = true;
                    let msg = format!("stalled: no data for {stall:?}");
                    return Some((Err(ConnError::Retry(msg, None)), st));
                }
                Ok(None) => return None,
                Ok(Some(Err(e))) => {
                    st.done = true;
                    return Some((Err(ConnError::Retry(format!("body: {e}"), None)), st));
                }
                Ok(Some(Ok(c))) => c,
            };
            if let Err(e) = st.parser.push(&chunk, &mut st.ready) {
                st.done = true;
                return Some((Err(ConnError::Retry(e, None)), st));
            }
            st.ready.reverse();
        }
    })
    .boxed()
}
