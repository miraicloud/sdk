//! Streams: the checkpoint stream
//! (`https://api.mainnet.mirai.cloud/v1/stream/checkpoints`, an API
//! key with `stream:read`, or a delegation: [`Builder::auth`]).
//!
//! A [`Subscription`] yields [`Event`]s strictly in sequence order, without
//! duplicates, from any starting point:
//!
//! - the stream serves live checkpoints and the most recent ones; anything
//!   older is read from the archive (`archive.mainnet.sui.io`, every
//!   checkpoint since genesis) by this client, then the stream takes over,
//!   without holes or duplicates;
//! - reconnects from the cursor with jittered exponential backoff (1 s
//!   doubling to 30 s, at least the server's `Retry-After`), and treats 45 s without
//!   bytes (keepalives arrive every 15 s) as a dead connection;
//! - surfaces checkpoints that cannot be delivered as [`Event::Gap`]; the
//!   subscription then continues after the gap.
//!
//! The stream is zstd-compressed protobuf: each checkpoint is a
//! `sui.rpc.v2.Checkpoint`, the same type Sui's gRPC API returns
//! ([`Checkpoint`]). [`Client::subscribe_raw`] gives the same guarantees but
//! yields each checkpoint's encoded bytes ([`RawEvent`]), for callers that
//! decode them themselves or pass them on.
//!
//! ```no_run
//! use futures::StreamExt;
//! use miraicloud::streams::{Client, Event};
//! # async fn run() -> Result<(), miraicloud::streams::Error> {
//! let mut sub = Client::builder()
//!     .api_key(std::env::var("MIRAICLOUD_API_KEY").unwrap())
//!     .read_mask(["sequence_number", "digest"])
//!     .resume_after(334_000_000)
//!     .build()?
//!     .subscribe();
//! while let Some(event) = sub.next().await {
//!     match event? {
//!         Event::Checkpoint { seq, checkpoint } => {
//!             println!("{seq} {:?}", checkpoint.digest);
//!         }
//!         Event::Gap { from, to } => eprintln!("lost {from}..={to}"),
//!         _ => {} // kinds added in later releases
//!     }
//!     // persist sub.cursor() to resume later with resume_after()
//! }
//! # Ok(()) }
//! ```

mod archive;
mod driver;
mod frame;
mod http;
mod mask;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

pub use crate::auth::Auth;
pub use archive::BCS_PATHS;
pub use bytes::Bytes;
pub use sui_rpc::proto::sui::rpc::v2::Checkpoint;

/// Production Mainnet stream.
pub const DEFAULT_STREAM_URL: &str = "https://api.mainnet.mirai.cloud/v1/stream/checkpoints";

/// Mainnet archive (gRPC `LedgerService`), for history older than the
/// stream holds.
pub use archive::DEFAULT_ARCHIVE_URL;

/// Errors. Any error yielded by a [`Subscription`] is terminal: the stream
/// ends after it. Transient failures (network, 5xx, 429, stalls, malformed
/// frames, archive outages) are retried internally and logged with
/// `tracing`.
///
/// New variants and fields may be added in minor releases: match with a
/// wildcard arm.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("invalid configuration: {0}")]
    Config(String),
    /// The stream refused the request for good: 400, 401, 403, 404 (until
    /// the stream has once answered, after a few attempts), or 429 over
    /// quota (`QuotaExceeded`).
    #[error("stream rejected the request: HTTP {status}: {body}")]
    #[non_exhaustive]
    Http {
        status: u16,
        /// The response body.
        body: String,
        /// The error's `_tag` when the body is the API's JSON error
        /// (`{"_tag": "QuotaExceeded", "message": …}`): `Unauthorized`,
        /// `Forbidden`, `Invalid`, `NotFound`, `QuotaExceeded`, …
        tag: Option<String>,
    },
    /// No token: the exchange refused the delegation chain (revoked,
    /// expired, invalid) or the signer failed.
    #[error("authentication failed: {0}")]
    Auth(#[from] crate::auth::Error),
}

/// One item of a [`Subscription`]. New kinds may be added in minor
/// releases: match with a wildcard arm.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Event {
    /// The next checkpoint. `seq` is set even if the read mask omits
    /// `sequence_number`.
    Checkpoint {
        seq: u64,
        checkpoint: Box<Checkpoint>,
    },
    /// Checkpoints `from..=to` cannot be delivered; the stream continues
    /// with `to + 1`.
    Gap { from: u64, to: u64 },
}

/// One item of a [`RawSubscription`]. New kinds may be added in minor
/// releases: match with a wildcard arm.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RawEvent {
    /// The next checkpoint. `data`: the encoded protobuf
    /// `sui.rpc.v2.Checkpoint`, not decoded (zero-copy from the stream
    /// buffer, which it keeps alive: copy it to retain only the payload;
    /// re-encoded for checkpoints read from the archive).
    Checkpoint { seq: u64, data: Bytes },
    /// Checkpoints `from..=to` cannot be delivered; the stream continues
    /// with `to + 1`.
    Gap { from: u64, to: u64 },
}

/// A configured stream; build with [`Client::builder`]. Cheap to clone; each
/// [`subscribe`](Client::subscribe) opens an independent subscription.
#[derive(Clone)]
pub struct Client {
    source: Arc<http::HttpSource>,
    history: Option<Arc<dyn archive::History>>,
    start: Option<u64>,
    /// Prunes live checkpoints to the read mask (`None`: keep everything).
    mask: Option<Arc<sui_rpc::field::FieldMaskTree>>,
    deadlines: driver::Deadlines,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("url", &self.source.url())
            .field("start", &self.start)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct Builder {
    url: String,
    mask: Option<Vec<String>>,
    start: Option<u64>,
    headers: Vec<(String, String)>,
    auth: Option<Auth>,
    archive_url: Option<String>,
    archive_concurrency: usize,
    stall_timeout: Duration,
    progress_timeout: Duration,
}

/// Header names only: their values (an API key in `authorization`) never
/// reach a log.
impl std::fmt::Debug for Builder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let headers: Vec<&str> = self.headers.iter().map(|(k, _)| k.as_str()).collect();
        f.debug_struct("Builder")
            .field("url", &self.url)
            .field("mask", &self.mask)
            .field("start", &self.start)
            .field("headers", &headers)
            .field("auth", &self.auth)
            .field("archive_url", &self.archive_url)
            .field("archive_concurrency", &self.archive_concurrency)
            .field("stall_timeout", &self.stall_timeout)
            .field("progress_timeout", &self.progress_timeout)
            .finish()
    }
}

impl Client {
    pub fn builder() -> Builder {
        Builder {
            url: DEFAULT_STREAM_URL.to_owned(),
            mask: None,
            start: None,
            headers: Vec::new(),
            auth: None,
            archive_url: Some(DEFAULT_ARCHIVE_URL.to_owned()),
            archive_concurrency: 2,
            stall_timeout: Duration::from_secs(45),
            progress_timeout: driver::Deadlines::default().progress,
        }
    }

    /// Start streaming. Must be called within a Tokio runtime; dropping the
    /// [`Subscription`] stops all background work.
    pub fn subscribe(&self) -> Subscription {
        Subscription {
            inner: Sub::new(self),
        }
    }

    /// Like [`subscribe`](Client::subscribe), but yields each checkpoint's
    /// encoded bytes, not decoded.
    pub fn subscribe_raw(&self) -> RawSubscription {
        RawSubscription {
            inner: Sub::new(self),
        }
    }
}

impl Builder {
    /// Stream endpoint (default [`DEFAULT_STREAM_URL`]).
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    /// `sui.rpc.v2.Checkpoint` field paths to keep, applied locally: the
    /// stream sends every client the same full checkpoint (every canonical
    /// field, BCS encodings included) and this SDK prunes it, live and from
    /// the archive alike.
    ///
    /// Default (and what `*` and parent paths stand for): the whole
    /// checkpoint, BCS included. The BCS encodings ([`BCS_PATHS`], e.g.
    /// `transactions.effects.bcs`) can be named like any other field; to
    /// skip them, name the decoded fields you need.
    /// [`build`](Builder::build) rejects invalid paths and fields no node
    /// renders consistently (event and object JSON, object display).
    pub fn read_mask<I, S>(mut self, paths: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.mask = Some(paths.into_iter().map(Into::into).collect());
        self
    }

    /// Start at this checkpoint (inclusive). Default: the live tip.
    pub fn start_at(mut self, seq: u64) -> Self {
        self.start = Some(seq);
        self
    }

    /// Start after this checkpoint, e.g. a persisted
    /// [`Subscription::cursor`].
    pub fn resume_after(self, seq: u64) -> Self {
        self.start_at(seq.saturating_add(1))
    }

    /// A mirai.cloud API key (`mc1_…`) with the `stream:read` scope, sent
    /// as `authorization: Bearer <key>`.
    pub fn api_key(self, key: impl AsRef<str>) -> Self {
        let value = format!("Bearer {}", key.as_ref());
        self.header("authorization", value)
    }

    /// How to authenticate: an API key, or a delegated
    /// [`TokenSource`](crate::auth::TokenSource), whose token is fetched
    /// for every connection (and renewed: an open stream reconnects with a
    /// fresh token shortly before its token expires, resuming from its
    /// cursor). Overrides an `authorization` [`header`](Builder::header).
    pub fn auth(mut self, auth: impl Into<Auth>) -> Self {
        self.auth = Some(auth.into());
        self
    }

    /// Authenticate with short-lived keys exchanged for a delegation chain
    /// (the same as [`auth`](Builder::auth) with `Auth::Delegated`).
    pub fn delegation(self, tokens: crate::auth::TokenSource) -> Self {
        self.auth(Auth::Delegated(tokens))
    }

    /// Header for the stream request (e.g. `authorization`). The response
    /// is always zstd-compressed; keep the default `accept-encoding: zstd`.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// gRPC `LedgerService` for positions older than the stream holds
    /// (default [`DEFAULT_ARCHIVE_URL`]).
    pub fn archive(mut self, url: impl Into<String>) -> Self {
        self.archive_url = Some(url.into());
        self
    }

    /// Do not read history: positions older than the stream holds are
    /// reported as an [`Event::Gap`].
    pub fn no_archive(mut self) -> Self {
        self.archive_url = None;
        self
    }

    /// Archive pages (`ListCheckpoints`, 50 checkpoints each) in flight
    /// (default 2). Each page in flight is held in memory, decoded and
    /// masked: with the whole checkpoint (no read mask) that is ~250 MB a
    /// page at today's Mainnet sizes (a deep backfill peaks at ~600 MB with
    /// 2, ~1.2 GB with 4, ~370 MB with 1, reading ~65, ~90 and ~40
    /// checkpoints a second); with a read mask naming the fields you need,
    /// a few MB.
    pub fn archive_concurrency(mut self, n: usize) -> Self {
        self.archive_concurrency = n;
        self
    }

    /// Reconnect when no bytes arrive for this long (default 45 s; the
    /// server sends keepalives every 15 s).
    pub fn stall_timeout(mut self, timeout: Duration) -> Self {
        self.stall_timeout = timeout;
        self
    }

    /// Reconnect (resuming from the cursor) when a connection delivers no
    /// checkpoint or gap for this long, even if keepalives still arrive
    /// (default 30 s, inside the stream's ~49 s ring; Mainnet makes ~4
    /// checkpoints a second). A server
    /// that stays connected but stops sending checkpoints can't freeze a
    /// subscription.
    pub fn progress_timeout(mut self, timeout: Duration) -> Self {
        self.progress_timeout = timeout;
        self
    }

    /// Validates the URLs and headers.
    pub fn build(self) -> Result<Client, Error> {
        if self.progress_timeout.is_zero() {
            return Err(Error::Config("progress timeout must be positive".into()));
        }
        let paths = match &self.mask {
            Some(paths) => mask::resolve(paths).map_err(Error::Config)?,
            None => None,
        };
        let history = match &self.archive_url {
            None => None,
            Some(url) => {
                let archive =
                    archive::Archive::new(url, paths.as_deref(), self.archive_concurrency)
                        .map_err(Error::Config)?;
                Some(Arc::new(archive) as Arc<dyn archive::History>)
            }
        };
        let source = http::HttpSource::new(self.url, &self.headers, self.auth, self.stall_timeout)?;
        Ok(Client {
            source: Arc::new(source),
            history,
            start: self.start,
            mask: paths.as_deref().map(|p| Arc::new(mask::tree(p))),
            deadlines: driver::Deadlines {
                progress: self.progress_timeout,
                ..driver::Deadlines::default()
            },
        })
    }
}

/// Items buffered between the connection and the consumer.
const BUFFERED: usize = 4;

/// Ordered events. Ends only after yielding an [`Error`]. Bounded: when the
/// consumer falls behind, reading from the network pauses.
pub struct Subscription {
    inner: Sub<Event>,
}

/// Ordered raw events, with the same guarantees as a [`Subscription`].
pub struct RawSubscription {
    inner: Sub<RawEvent>,
}

/// The common part of both subscriptions: the driver task and the position.
struct Sub<E> {
    rx: tokio::sync::mpsc::Receiver<Result<E, Error>>,
    task: tokio::task::JoinHandle<()>,
    last: Option<u64>,
    cursor: Option<u64>,
}

impl<E: driver::Item> Sub<E> {
    fn new(client: &Client) -> Self {
        // Few items: memory is bounded by items, and an epoch-change
        // checkpoint is 10-20 MB decoded (32 of them OOM-killed a 256 MB
        // machine).
        let (tx, rx) = tokio::sync::mpsc::channel(BUFFERED);
        let task = tokio::spawn(driver::run(
            client.source.clone(),
            client.history.clone(),
            client.start,
            client.mask.clone(),
            tx,
            client.deadlines,
        ));
        Self {
            rx,
            task,
            last: None,
            cursor: client.start.and_then(|s| s.checked_sub(1)),
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<E, Error>>> {
        let item = std::task::ready!(self.rx.poll_recv(cx));
        if let Some(Ok(item)) = &item {
            let (delivered, cursor) = item.position();
            self.last = delivered.or(self.last);
            self.cursor = Some(cursor);
        }
        Poll::Ready(item)
    }
}

impl<E> Drop for Sub<E> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

macro_rules! subscription {
    ($name:ident, $item:ty) => {
        impl $name {
            /// Sequence number of the last checkpoint yielded.
            pub fn last_delivered(&self) -> Option<u64> {
                self.inner.last
            }

            /// Everything up to and including this checkpoint has been
            /// yielded, either as a checkpoint or inside a gap. Persist it
            /// and pass it to [`Builder::resume_after`] to continue later.
            pub fn cursor(&self) -> Option<u64> {
                self.inner.cursor
            }
        }

        impl futures::Stream for $name {
            type Item = Result<$item, Error>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                self.inner.poll_next(cx)
            }
        }
    };
}

subscription!(Subscription, Event);
subscription!(RawSubscription, RawEvent);

/// The server's field lists (`suistream/crates/common/src/lib.rs`), when
/// built inside the platform repository: the SDK's lists are checked
/// against them.
#[cfg(test)]
pub(crate) fn server_common_lib() -> Option<String> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../suistream/crates/common/src/lib.rs"
    );
    match std::fs::read_to_string(path) {
        Ok(src) => Some(src),
        Err(e) => {
            eprintln!("skipped: {path}: {e}");
            None
        }
    }
}

/// Not public API: internals exposed for benchmarks and tests. May change
/// without notice.
#[doc(hidden)]
pub mod __private {
    pub use crate::streams::driver::decode_checkpoint;
    pub use crate::streams::frame::{Frame, Parser};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_validates_masks() {
        assert!(Client::builder().read_mask(["*"]).build().is_ok());
        assert!(
            Client::builder()
                .read_mask(["summary", "transactions.digest"])
                .build()
                .is_ok()
        );
        // BCS encodings are served like any other field.
        assert!(
            Client::builder()
                .read_mask(["sequence_number", "transactions.effects.bcs"])
                .build()
                .is_ok()
        );
        for bad in [
            "transactions.efects",
            "objects.objects.json",
            "objects.objects.display",
        ] {
            assert!(
                matches!(
                    Client::builder().read_mask([bad]).build(),
                    Err(Error::Config(_))
                ),
                "{bad}"
            );
        }
    }

    #[test]
    fn build_validates() {
        assert!(matches!(
            Client::builder().url("not a url").build(),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            Client::builder().header("bad header", "x").build(),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            Client::builder().header("x-token", "a\nb").build(),
            Err(Error::Config(_))
        ));
        assert!(Client::builder().header("x-token", "t").build().is_ok());
        assert!(matches!(
            Client::builder().archive("not a url").build(),
            Err(Error::Config(_))
        ));
        assert!(Client::builder().no_archive().build().is_ok());
    }
}
