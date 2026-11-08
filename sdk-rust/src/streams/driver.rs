//! Keeps one stream connection alive and turns it into an ordered sequence:
//! reconnect from the cursor, drop duplicates, surface gaps. Positions older
//! than the stream holds (`410`) are read from the archive up to its own
//! tip (it keeps up with the chain), so the stream is rejoined well within
//! its ring; reading only up to the ring's oldest position would chase a
//! ring that moves on meanwhile. If the stream still answers `410`, this
//! repeats. Checkpoints are decoded only after deduplication, so duplicates
//! are never parsed.
//!
//! Nothing here waits without a deadline ([`Deadlines`]): connecting, the
//! archive's tip, each archive item, and the connection itself, which must
//! deliver a checkpoint or a gap (keepalives don't count) within
//! `progress` or is dropped and resumed. A subscription can be slow, but it
//! never stops moving silently.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use prost::Message;
use sui_rpc::field::FieldMaskTree;
use sui_rpc::merge::Merge;
use tokio::sync::mpsc;

use crate::streams::archive::{History, Item as HistoryItem};
use crate::streams::http::{ConnError, Source, StreamEvent};
use crate::streams::{Checkpoint, Error, Event, RawEvent};

/// How long each step may take before it counts as failed (and is retried).
#[derive(Clone, Copy, Debug)]
pub(crate) struct Deadlines {
    /// A connection that delivers no checkpoint or gap for this long is
    /// dropped and resumed (keepalives don't count).
    pub progress: Duration,
    /// Opening a connection, through its first response.
    pub connect: Duration,
    /// The archive's tip; without it, a backfill runs up to the ring.
    pub tip: Duration,
    /// Each archive item; a page holds 50, and epoch-change checkpoints are
    /// 10-20 MB.
    pub history_item: Duration,
}

impl Default for Deadlines {
    fn default() -> Self {
        Deadlines {
            progress: Duration::from_secs(60),
            connect: Duration::from_secs(60),
            tip: Duration::from_secs(15),
            history_item: Duration::from_secs(150),
        }
    }
}

const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

fn backoff(failures: u32) -> Duration {
    BACKOFF_MIN
        .saturating_mul(1 << failures.min(5))
        .min(BACKOFF_MAX)
}

/// Between half and all of `d`, at random: clients dropped together (a
/// deploy, an outage) don't all come back in the same instant.
fn jittered(d: Duration) -> Duration {
    use std::hash::BuildHasher;
    // A fresh `RandomState` is randomly keyed: random enough for spreading
    // reconnects, without a dependency.
    let bits = std::collections::hash_map::RandomState::new().hash_one(std::time::Instant::now());
    let unit = (bits >> 11) as f64 / (1u64 << 53) as f64;
    d.mul_f64(0.5 + 0.5 * unit)
}

/// Decode an encoded `Checkpoint`; `seq` is its frame's cursor.
pub fn decode_checkpoint(seq: u64, data: &[u8]) -> Result<Box<Checkpoint>, String> {
    let cp = Box::new(Checkpoint::decode(data).map_err(|e| format!("checkpoint {seq}: {e}"))?);
    if cp.sequence_number.is_some_and(|s| s != seq) {
        return Err(format!(
            "cursor {seq} != sequence_number {:?}",
            cp.sequence_number
        ));
    }
    Ok(cp)
}

/// An item a subscription yields.
pub(crate) trait Item: Send + Sized + 'static {
    /// Decode checkpoint `seq` from the stream, pruned to `mask` if set; an
    /// error is a malformed frame.
    fn checkpoint(seq: u64, data: Bytes, mask: Option<&FieldMaskTree>) -> Result<Self, String>;
    /// Checkpoint `seq` from the archive.
    fn from_history(seq: u64, checkpoint: Box<Checkpoint>) -> Self;
    fn gap(from: u64, to: u64) -> Self;
    /// (checkpoint delivered, cursor) once this item is yielded.
    fn position(&self) -> (Option<u64>, u64);
}

impl Item for Event {
    fn checkpoint(seq: u64, data: Bytes, mask: Option<&FieldMaskTree>) -> Result<Self, String> {
        let mut checkpoint = decode_checkpoint(seq, &data)?;
        if let Some(mask) = mask {
            *checkpoint = Checkpoint::merge_from(&*checkpoint, mask);
        }
        Ok(Event::Checkpoint { seq, checkpoint })
    }

    fn from_history(seq: u64, checkpoint: Box<Checkpoint>) -> Self {
        Event::Checkpoint { seq, checkpoint }
    }

    fn gap(from: u64, to: u64) -> Self {
        Event::Gap { from, to }
    }

    fn position(&self) -> (Option<u64>, u64) {
        match *self {
            Event::Checkpoint { seq, .. } => (Some(seq), seq),
            Event::Gap { to, .. } => (None, to),
        }
    }
}

impl Item for RawEvent {
    fn checkpoint(seq: u64, data: Bytes, mask: Option<&FieldMaskTree>) -> Result<Self, String> {
        let Some(mask) = mask else {
            return Ok(RawEvent::Checkpoint { seq, data });
        };
        let checkpoint = decode_checkpoint(seq, &data)?;
        let data = Checkpoint::merge_from(&*checkpoint, mask)
            .encode_to_vec()
            .into();
        Ok(RawEvent::Checkpoint { seq, data })
    }

    fn from_history(seq: u64, checkpoint: Box<Checkpoint>) -> Self {
        RawEvent::Checkpoint {
            seq,
            data: checkpoint.encode_to_vec().into(),
        }
    }

    fn gap(from: u64, to: u64) -> Self {
        RawEvent::Gap { from, to }
    }

    fn position(&self) -> (Option<u64>, u64) {
        match *self {
            RawEvent::Checkpoint { seq, .. } => (Some(seq), seq),
            RawEvent::Gap { to, .. } => (None, to),
        }
    }
}

/// Read from `next` up to history's tip (at least to `oldest`, the ring's
/// first position) into `out`, in order. Returns the new `next`, and whether
/// it got there (else: retry after backoff). Only a missing run followed by
/// a checkpoint is a gap: a trailing one may be history lagging its own tip.
async fn backfill<E: Item>(
    history: &dyn History,
    next: u64,
    oldest: u64,
    out: &mpsc::Sender<Result<E, Error>>,
    deadlines: Deadlines,
) -> Option<(u64, bool)> {
    // An unknown tip (or one that doesn't answer in time): up to the ring,
    // as far as is known to be needed.
    let tip = match tokio::time::timeout(deadlines.tip, history.tip()).await {
        Ok(tip) => tip,
        Err(_) => {
            tracing::warn!(after = ?deadlines.tip, "archive tip: no answer; backfilling up to the stream's oldest");
            None
        }
    };
    let end = tip.map_or(oldest, |tip| (tip + 1).max(oldest));
    tracing::info!(from = next, to = end - 1, "backfilling from the archive");
    let mut items = history.checkpoints(next..end);
    let mut next = next;
    let mut missing: Option<(u64, u64)> = None;
    macro_rules! send {
        ($item:expr) => {
            tokio::select! {
                r = out.send(Ok($item)) => if r.is_err() { return None },
                _ = out.closed() => return None,
            }
        };
    }
    loop {
        let item = tokio::select! {
            item = tokio::time::timeout(deadlines.history_item, items.next()) => item,
            _ = out.closed() => return None,
        };
        let Ok(item) = item else {
            tracing::warn!(next, after = ?deadlines.history_item, "archive backfill: no answer; retrying");
            return Some((next, false));
        };
        match item {
            None => break,
            Some(Err(e)) => {
                // A pending missing run isn't a gap yet: `next` stays before it.
                tracing::warn!(next, "archive backfill failed: {e}");
                return Some((next, false));
            }
            Some(Ok(HistoryItem::Missing(seq))) => {
                // `next` moves past it only once it is reported as a gap.
                missing = Some(match missing {
                    Some((a, _)) => (a, seq),
                    None => (seq, seq),
                });
            }
            Some(Ok(HistoryItem::Checkpoint(seq, cp))) => {
                if let Some((a, b)) = missing.take() {
                    send!(E::gap(a, b));
                }
                send!(E::from_history(seq, cp));
                next = seq + 1;
            }
        }
    }
    // Not reported as a gap: from the ring's oldest on, the stream serves
    // them; older, history is asked again.
    if let Some((a, b)) = missing {
        tracing::info!(from = a, to = b, "not in the archive yet");
        return Some((a, a >= oldest));
    }
    Some((next, true))
}

/// `start`: first checkpoint wanted (`None` = live tip). `history`: where
/// positions older than the stream holds come from (`None`: they are
/// reported as a gap). `mask`: prunes live checkpoints (history is pruned
/// by the archive itself).
pub(crate) async fn run<E: Item>(
    source: Arc<dyn Source>,
    history: Option<Arc<dyn History>>,
    start: Option<u64>,
    mask: Option<Arc<FieldMaskTree>>,
    out: mpsc::Sender<Result<E, Error>>,
    deadlines: Deadlines,
) {
    // Next checkpoint owed to the caller.
    let mut next = start;
    // Consecutive connections that delivered nothing.
    let mut failures = 0u32;
    'connect: loop {
        let mut delay = None;
        let connected = tokio::select! {
            r = tokio::time::timeout(deadlines.connect, source.connect(next)) => r,
            _ = out.closed() => return,
        };
        let connected = connected.unwrap_or_else(|_| {
            Err(ConnError::Retry(
                format!("no response in {:?}", deadlines.connect),
                None,
            ))
        });
        match connected {
            Err(ConnError::Fatal(e)) => {
                let _ = out.send(Err(e)).await;
                return;
            }
            Err(ConnError::TooOld { oldest }) => match next.filter(|&n| n < oldest) {
                // Not actually older (e.g. a misbehaving proxy): back off.
                None => tracing::warn!(?next, oldest, "unexpected 410"),
                Some(n) => match &history {
                    None => {
                        tracing::warn!(
                            from = n,
                            to = oldest - 1,
                            "older than the gateway holds; no archive"
                        );
                        if out.send(Ok(E::gap(n, oldest - 1))).await.is_err() {
                            return;
                        }
                        next = Some(oldest);
                        failures = 0;
                        continue;
                    }
                    Some(h) => match backfill(h.as_ref(), n, oldest, &out, deadlines).await {
                        None => return,
                        Some((reached, done)) => {
                            if reached > n {
                                failures = 0;
                            }
                            next = Some(reached);
                            if done {
                                // Close to the ring now: reconnect right away.
                                continue;
                            }
                        }
                    },
                },
            },
            Err(ConnError::Retry(msg, retry_after)) => {
                tracing::warn!("stream connection failed: {msg}");
                delay = retry_after;
            }
            Err(ConnError::Renew) => continue,
            Ok(mut events) => {
                tracing::debug!(?next, "connected");
                // The progress watchdog: reset by each checkpoint or gap
                // received (duplicates included: the server is moving), not
                // by keepalives, and not while the consumer is slow to take
                // what was delivered.
                let mut progress_by = tokio::time::Instant::now() + deadlines.progress;
                loop {
                    let item = tokio::select! {
                        item = events.next() => item,
                        _ = tokio::time::sleep_until(progress_by) => {
                            tracing::warn!(?next, after = ?deadlines.progress, "no checkpoints on this connection; reconnecting");
                            break;
                        }
                        _ = out.closed() => return,
                    };
                    if matches!(item, Some(Ok(_))) {
                        progress_by = tokio::time::Instant::now() + deadlines.progress;
                    }
                    let event = match item {
                        None => {
                            tracing::debug!(?next, "stream closed by server");
                            break;
                        }
                        Some(Err(ConnError::Retry(msg, _))) => {
                            tracing::warn!(?next, "stream interrupted: {msg}");
                            break;
                        }
                        Some(Err(ConnError::TooOld { .. })) => break,
                        Some(Err(ConnError::Renew)) => {
                            // Before the token expires: the server never
                            // has to cut the stream.
                            tracing::debug!(?next, "renewing the connection's token");
                            continue 'connect;
                        }
                        Some(Err(ConnError::Fatal(e))) => {
                            let _ = out.send(Err(e)).await;
                            return;
                        }
                        Some(Ok(event)) => event,
                    };
                    let event = match (event, next) {
                        (StreamEvent::Checkpoint(seq, _), Some(n)) if seq < n => continue,
                        (StreamEvent::Gap { to, .. }, Some(n)) if to < n => continue,
                        (StreamEvent::Checkpoint(seq, _), Some(n))
                        | (StreamEvent::Gap { from: seq, .. }, Some(n))
                            if seq > n =>
                        {
                            // The server never skips silently; reconnect
                            // rather than guess.
                            tracing::warn!(next = n, got = seq, "out of order; reconnecting");
                            break;
                        }
                        (StreamEvent::Checkpoint(seq, data), _) => {
                            match E::checkpoint(seq, data, mask.as_deref()) {
                                Ok(item) => {
                                    next = Some(seq + 1);
                                    item
                                }
                                Err(e) => {
                                    tracing::warn!(?next, "malformed event: {e}; reconnecting");
                                    break;
                                }
                            }
                        }
                        (StreamEvent::Gap { from, to }, n) => {
                            next = Some(to + 1);
                            E::gap(n.map_or(from, |n| n.max(from)), to)
                        }
                    };
                    failures = 0;
                    if out.send(Ok(event)).await.is_err() {
                        return;
                    }
                    // Time spent waiting on the consumer isn't the server's.
                    progress_by = tokio::time::Instant::now() + deadlines.progress;
                }
            }
        }
        let wait = jittered(backoff(failures)).max(delay.unwrap_or_default());
        failures += 1;
        tracing::info!(?wait, ?next, "reconnecting");
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = out.closed() => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streams::http::EventStream;
    use futures::future::BoxFuture;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    type Script = Result<Vec<Result<StreamEvent, ConnError>>, ConnError>;

    /// Plays one scripted connection per `connect`, recording the resume
    /// positions; after the script, connections hang.
    struct Scripted {
        conns: Mutex<VecDeque<Script>>,
        resumes: Mutex<Vec<Option<u64>>>,
    }

    impl Source for Scripted {
        fn connect(&self, next: Option<u64>) -> BoxFuture<'static, Result<EventStream, ConnError>> {
            self.resumes.lock().unwrap().push(next);
            let script = self.conns.lock().unwrap().pop_front();
            Box::pin(async move {
                match script {
                    None => Ok(futures::stream::pending().boxed()),
                    Some(Err(e)) => Err(e),
                    Some(Ok(events)) => Ok(futures::stream::iter(events).boxed()),
                }
            })
        }
    }

    fn checkpoint(seq: u64) -> Checkpoint {
        let mut c = Checkpoint::default();
        c.sequence_number = Some(seq);
        c
    }

    /// The encoded checkpoint the scripts use for `seq`.
    fn encoded(seq: u64) -> Vec<u8> {
        checkpoint(seq).encode_to_vec()
    }

    fn cp(seq: u64) -> Result<StreamEvent, ConnError> {
        cp_data(seq, &encoded(seq))
    }

    fn cp_data(seq: u64, data: &[u8]) -> Result<StreamEvent, ConnError> {
        Ok(StreamEvent::Checkpoint(seq, Bytes::copy_from_slice(data)))
    }

    /// Serves `[from, to)` with `missing` absent, failing at `fail_at` once;
    /// `tip`: its newest checkpoint, if it says.
    struct FakeHistory {
        missing: Vec<u64>,
        fail_at: Mutex<Option<u64>>,
        calls: Mutex<Vec<(u64, u64)>>,
        tip: Option<u64>,
    }

    impl FakeHistory {
        fn new(missing: &[u64], fail_at: Option<u64>) -> Arc<Self> {
            Self::with_tip(missing, fail_at, None)
        }

        fn with_tip(missing: &[u64], fail_at: Option<u64>, tip: Option<u64>) -> Arc<Self> {
            Arc::new(Self {
                missing: missing.to_vec(),
                fail_at: Mutex::new(fail_at),
                calls: Mutex::new(vec![]),
                tip,
            })
        }
    }

    impl History for FakeHistory {
        fn tip(&self) -> futures::future::BoxFuture<'static, Option<u64>> {
            Box::pin(futures::future::ready(self.tip))
        }

        fn checkpoints(
            &self,
            range: std::ops::Range<u64>,
        ) -> futures::stream::BoxStream<'static, Result<HistoryItem, String>> {
            self.calls.lock().unwrap().push((range.start, range.end));
            let fail = self.fail_at.lock().unwrap().take();
            let missing = self.missing.clone();
            futures::stream::iter(range)
                .map(move |seq| {
                    if Some(seq) == fail {
                        Err("archive down".to_owned())
                    } else if missing.contains(&seq) {
                        Ok(HistoryItem::Missing(seq))
                    } else {
                        Ok(HistoryItem::Checkpoint(seq, Box::new(checkpoint(seq))))
                    }
                })
                .take_while({
                    let mut failed = false;
                    move |r| {
                        let keep = !failed;
                        failed |= r.is_err();
                        futures::future::ready(keep)
                    }
                })
                .boxed()
        }
    }

    fn too_old(oldest: u64) -> ConnError {
        ConnError::TooOld { oldest }
    }

    fn gap(from: u64, to: u64) -> Result<StreamEvent, ConnError> {
        Ok(StreamEvent::Gap { from, to })
    }

    fn retry() -> ConnError {
        ConnError::Retry("boom".into(), None)
    }

    /// Items as strings: `seq` for a checkpoint whose payload is `json(seq)`,
    /// else `seq <payload>`.
    trait Show: Item {
        fn show(self) -> String;
    }

    impl Show for Event {
        fn show(self) -> String {
            match self {
                Event::Checkpoint { seq, checkpoint } => {
                    assert_eq!(checkpoint.sequence_number, Some(seq));
                    seq.to_string()
                }
                Event::Gap { from, to } => format!("gap {from}-{to}"),
            }
        }
    }

    impl Show for RawEvent {
        fn show(self) -> String {
            match self {
                RawEvent::Checkpoint { seq, data } if data == encoded(seq) => seq.to_string(),
                RawEvent::Checkpoint { seq, data } => format!("{seq} {data:?}"),
                RawEvent::Gap { from, to } => format!("gap {from}-{to}"),
            }
        }
    }

    async fn play<E: Show>(
        start: Option<u64>,
        conns: Vec<Script>,
        n: usize,
    ) -> (Vec<String>, Vec<Option<u64>>) {
        play_with::<E>(None, start, conns, n).await
    }

    async fn play_with<E: Show>(
        history: Option<Arc<dyn History>>,
        start: Option<u64>,
        conns: Vec<Script>,
        n: usize,
    ) -> (Vec<String>, Vec<Option<u64>>) {
        let source = Arc::new(Scripted {
            conns: Mutex::new(conns.into()),
            resumes: Mutex::new(vec![]),
        });
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn(run::<E>(
            source.clone(),
            history,
            start,
            None,
            tx,
            Deadlines::default(),
        ));
        let mut got = vec![];
        while got.len() < n {
            got.push(match rx.recv().await.unwrap() {
                Ok(item) => item.show(),
                Err(e) => format!("error {e}"),
            });
        }
        task.abort();
        let resumes = source.resumes.lock().unwrap().clone();
        (got, resumes)
    }

    /// `play` in typed and raw mode; both must agree.
    async fn play_both(
        start: Option<u64>,
        conns: impl Fn() -> Vec<Script>,
        n: usize,
    ) -> (Vec<String>, Vec<Option<u64>>) {
        let typed = play::<Event>(start, conns(), n).await;
        let raw = play::<RawEvent>(start, conns(), n).await;
        assert_eq!(typed, raw, "typed vs raw");
        typed
    }

    #[tokio::test(start_paused = true)]
    async fn dedups_reconnects_and_resumes_from_cursor() {
        let (got, resumes) = play_both(
            Some(10),
            || {
                vec![
                    Ok(vec![cp(10), cp(11), Err(retry())]),
                    Err(retry()),
                    // Overlap after reconnect is dropped.
                    Ok(vec![cp(11), cp(12), cp(13)]),
                    // Clean close by the server: reconnect from 14.
                    Ok(vec![cp(14)]),
                ]
            },
            5,
        )
        .await;
        assert_eq!(got, ["10", "11", "12", "13", "14"]);
        assert_eq!(resumes, [Some(10), Some(12), Some(12), Some(14)]);
    }

    #[tokio::test(start_paused = true)]
    async fn gaps_are_surfaced_once_and_move_the_cursor() {
        let (got, resumes) = play_both(
            Some(5),
            || {
                vec![
                    // History gap, then a live gap followed by a close.
                    Ok(vec![gap(5, 7), cp(8), gap(9, 10)]),
                    // The reconnect resumes after the gap; a repeated report
                    // of it is dropped.
                    Ok(vec![gap(9, 10), cp(11)]),
                ]
            },
            4,
        )
        .await;
        assert_eq!(got, ["gap 5-7", "8", "gap 9-10", "11"]);
        assert_eq!(resumes, [Some(5), Some(11)]);
    }

    #[tokio::test(start_paused = true)]
    async fn partial_gap_overlap_is_trimmed() {
        let (got, _) = play_both(Some(5), || vec![Ok(vec![cp(5), gap(3, 7), cp(8)])], 3).await;
        assert_eq!(got, ["5", "gap 6-7", "8"]);
    }

    #[tokio::test(start_paused = true)]
    async fn silent_skip_reconnects() {
        let (got, resumes) = play_both(
            Some(1),
            || vec![Ok(vec![cp(1), cp(3)]), Ok(vec![cp(2), cp(3)])],
            3,
        )
        .await;
        assert_eq!(got, ["1", "2", "3"]);
        assert_eq!(resumes, [Some(1), Some(2)]);
    }

    #[tokio::test(start_paused = true)]
    async fn tip_start_and_fatal_error() {
        let (got, resumes) = play_both(
            None,
            || {
                vec![
                    Ok(vec![cp(100), cp(101)]),
                    Err(ConnError::Fatal(Error::Http {
                        status: 400,
                        body: "bad mask".into(),
                        tag: None,
                    })),
                ]
            },
            3,
        )
        .await;
        assert_eq!(got[..2], ["100", "101"]);
        assert!(got[2].starts_with("error stream rejected the request: HTTP 400"));
        assert_eq!(resumes, [None, Some(102)]);
    }

    async fn timed_backoff<E: Show>() {
        let t0 = tokio::time::Instant::now();
        let (got, _) = play::<E>(
            Some(1),
            vec![
                Err(ConnError::Retry("429".into(), Some(Duration::from_secs(7)))),
                Err(retry()),
                Ok(vec![cp(1)]),
            ],
            1,
        )
        .await;
        assert_eq!(got, ["1"]);
        // max(1 s jittered, Retry-After 7 s) + 2 s jittered (1–2 s).
        let secs = t0.elapsed().as_secs_f64();
        assert!((8.0..=9.0).contains(&secs), "waited {secs} s");
    }

    #[tokio::test(start_paused = true)]
    async fn renewal_reconnects_at_once_from_the_cursor() {
        for raw in [false, true] {
            let t0 = tokio::time::Instant::now();
            let conns = vec![
                Ok(vec![cp(1), cp(2), Err(ConnError::Renew)]),
                Ok(vec![cp(3), Err(ConnError::Renew)]),
                Ok(vec![cp(4)]),
            ];
            let (got, resumes) = if raw {
                play::<RawEvent>(Some(1), conns, 4).await
            } else {
                play::<Event>(Some(1), conns, 4).await
            };
            assert_eq!(got, ["1", "2", "3", "4"]);
            assert_eq!(resumes, [Some(1), Some(3), Some(4)]);
            // No backoff between renewals.
            assert!(
                t0.elapsed() < Duration::from_millis(100),
                "{:?}",
                t0.elapsed()
            );
        }
    }

    #[test]
    fn jitter_stays_within_half_to_all() {
        for _ in 0..1000 {
            let j = jittered(Duration::from_secs(8));
            assert!(
                j >= Duration::from_secs(4) && j <= Duration::from_secs(8),
                "{j:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_grows_and_honours_retry_after() {
        assert_eq!(backoff(0), Duration::from_secs(1));
        assert_eq!(backoff(3), Duration::from_secs(8));
        assert_eq!(backoff(10), Duration::from_secs(30));
        timed_backoff::<Event>().await;
        timed_backoff::<RawEvent>().await;
    }

    #[test]
    fn decodes_checkpoints() {
        let mut c = checkpoint(7);
        c.digest = Some("abc".into());
        let cp = decode_checkpoint(7, &c.encode_to_vec()).unwrap();
        assert_eq!(cp.digest.as_deref(), Some("abc"));
        assert_eq!(cp.sequence_number, Some(7));
        // The mask may omit the sequence number.
        assert!(decode_checkpoint(8, b"").is_ok());
        assert!(decode_checkpoint(8, &encoded(9)).is_err());
        assert!(decode_checkpoint(8, &[0x0a, 0x05, 1]).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn old_positions_are_backfilled_from_history_then_streamed() {
        let history = FakeHistory::new(&[3, 4], None);
        let (got, resumes) = play_with::<Event>(
            Some(history.clone()),
            Some(1),
            vec![
                // The ring starts at 6; then it moved on to 8 meanwhile.
                Err(too_old(6)),
                Err(too_old(8)),
                Ok(vec![cp(8), cp(9)]),
            ],
            7,
        )
        .await;
        assert_eq!(got, ["1", "2", "gap 3-4", "5", "6", "7", "8"]);
        assert_eq!(resumes, [Some(1), Some(6), Some(8)]);
        assert_eq!(*history.calls.lock().unwrap(), [(1, 6), (6, 8)]);
    }

    #[tokio::test(start_paused = true)]
    async fn backfill_runs_to_the_history_tip_then_streams_after_it() {
        let history = FakeHistory::with_tip(&[], None, Some(8));
        let (got, resumes) = play_with::<Event>(
            Some(history.clone()),
            Some(1),
            vec![Err(too_old(6)), Ok(vec![cp(9), cp(10)])],
            10,
        )
        .await;
        assert_eq!(got, ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]);
        assert_eq!(resumes, [Some(1), Some(9)]);
        assert_eq!(*history.calls.lock().unwrap(), [(1, 9)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_history_tip_behind_the_ring_backfills_to_the_ring() {
        let history = FakeHistory::with_tip(&[], None, Some(3));
        let (got, _) = play_with::<RawEvent>(
            Some(history.clone()),
            Some(1),
            vec![Err(too_old(6)), Ok(vec![cp(6)])],
            6,
        )
        .await;
        assert_eq!(got, ["1", "2", "3", "4", "5", "6"]);
        assert_eq!(*history.calls.lock().unwrap(), [(1, 6)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lagging_history_tail_is_not_a_gap() {
        // History's tip says 8 but it serves only up to 5 (ListCheckpoints
        // behind GetServiceInfo): 6..=8 are left to the stream.
        let history = FakeHistory::with_tip(&[6, 7, 8], None, Some(8));
        let (got, resumes) = play_with::<Event>(
            Some(history.clone()),
            Some(1),
            vec![Err(too_old(4)), Ok(vec![cp(6), cp(7), cp(8), cp(9)])],
            9,
        )
        .await;
        assert_eq!(got, ["1", "2", "3", "4", "5", "6", "7", "8", "9"]);
        assert_eq!(resumes, [Some(1), Some(6)]);

        // Lagging behind the ring's oldest too: asked again after backoff,
        // still no gap.
        let history = FakeHistory::with_tip(&[4, 5], None, Some(5));
        let source = Arc::new(Scripted {
            conns: Mutex::new(vec![Err(too_old(6)), Err(too_old(6))].into()),
            resumes: Mutex::new(vec![]),
        });
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn(run::<Event>(
            source.clone(),
            Some(history.clone()),
            Some(1),
            None,
            tx,
            Deadlines::default(),
        ));
        for want in ["1", "2", "3"] {
            assert_eq!(rx.recv().await.unwrap().unwrap().show(), want);
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert!(rx.try_recv().is_err(), "nothing more, no gap");
        task.abort();
        assert_eq!(
            source.resumes.lock().unwrap()[..3],
            [Some(1), Some(4), Some(4)]
        );
        assert_eq!(*history.calls.lock().unwrap(), [(1, 6), (4, 6)]);
    }

    #[tokio::test(start_paused = true)]
    async fn history_failure_does_not_report_a_pending_missing_run() {
        let history = FakeHistory::new(&[3], Some(4));
        let (got, resumes) = play_with::<RawEvent>(
            Some(history.clone()),
            Some(1),
            vec![Err(too_old(6)), Err(too_old(6)), Ok(vec![cp(6)])],
            5,
        )
        .await;
        // Retried from 3: then 3 is followed by 4, so it is a gap.
        assert_eq!(got, ["1", "2", "gap 3-3", "4", "5"]);
        assert_eq!(resumes[..2], [Some(1), Some(3)]);
        assert_eq!(*history.calls.lock().unwrap(), [(1, 6), (3, 6)]);
    }

    #[tokio::test(start_paused = true)]
    async fn history_failure_resumes_where_it_stopped() {
        let history = FakeHistory::new(&[], Some(3));
        let (got, resumes) = play_with::<RawEvent>(
            Some(history.clone()),
            Some(1),
            vec![Err(too_old(5)), Err(too_old(5)), Ok(vec![cp(5)])],
            5,
        )
        .await;
        assert_eq!(got, ["1", "2", "3", "4", "5"]);
        assert_eq!(resumes, [Some(1), Some(3), Some(5)]);
        assert_eq!(*history.calls.lock().unwrap(), [(1, 5), (3, 5)]);
    }

    #[tokio::test(start_paused = true)]
    async fn without_history_old_positions_are_a_gap() {
        let (got, resumes) =
            play_both(Some(1), || vec![Err(too_old(6)), Ok(vec![cp(6), cp(7)])], 3).await;
        assert_eq!(got, ["gap 1-5", "6", "7"]);
        assert_eq!(resumes, [Some(1), Some(6)]);
    }

    #[tokio::test(start_paused = true)]
    async fn typed_malformed_checkpoint_reconnects_from_it() {
        let (got, resumes) = play::<Event>(
            Some(10),
            vec![
                Ok(vec![cp(10), cp_data(11, &[0x0a, 0x05, 1]), cp(12)]),
                Ok(vec![cp(10), cp(11), cp(12)]),
            ],
            3,
        )
        .await;
        assert_eq!(got, ["10", "11", "12"]);
        assert_eq!(resumes, [Some(10), Some(11)]);
        // A mismatched sequence number is malformed too.
        let (got, resumes) = play::<Event>(
            Some(10),
            vec![Ok(vec![cp_data(10, &encoded(9))]), Ok(vec![cp(10)])],
            1,
        )
        .await;
        assert_eq!(got, ["10"]);
        assert_eq!(resumes, [Some(10), Some(10)]);
    }

    #[tokio::test(start_paused = true)]
    async fn malformed_duplicate_is_dropped_unparsed() {
        let (got, resumes) = play_both(
            Some(10),
            || {
                vec![
                    Ok(vec![cp(10), cp(11), cp_data(11, &[0x0a, 0x05, 1]), cp(12)]),
                    Ok(vec![cp(13)]),
                ]
            },
            4,
        )
        .await;
        assert_eq!(got, ["10", "11", "12", "13"]);
        // Only the clean close after 12 reconnects.
        assert_eq!(resumes, [Some(10), Some(13)]);
    }

    #[tokio::test(start_paused = true)]
    async fn raw_delivers_malformed_bytes_untouched() {
        let (got, resumes) = play::<RawEvent>(
            Some(10),
            vec![
                Ok(vec![cp(10), cp_data(11, &[0x0a, 0x05, 1]), cp(12)]),
                Ok(vec![cp(13)]),
            ],
            4,
        )
        .await;
        assert_eq!(got, ["10", "11 b\"\\n\\x05\\x01\"", "12", "13"]);
        assert_eq!(resumes, [Some(10), Some(13)]);
    }

    // Nothing waits without a deadline (2026-11-08: after a hub restart,
    // resuming clients waited on the archive forever, alive but frozen).

    /// History whose tip never answers, and whose first `checkpoints` call
    /// hangs after `hang_after` items (`None`: never hangs).
    struct HangingHistory {
        hang_after: Mutex<Option<usize>>,
    }

    impl History for HangingHistory {
        fn tip(&self) -> futures::future::BoxFuture<'static, Option<u64>> {
            Box::pin(futures::future::pending())
        }

        fn checkpoints(
            &self,
            range: std::ops::Range<u64>,
        ) -> futures::stream::BoxStream<'static, Result<HistoryItem, String>> {
            let items =
                range.map(|seq| Ok(HistoryItem::Checkpoint(seq, Box::new(checkpoint(seq)))));
            match self.hang_after.lock().unwrap().take() {
                Some(n) => futures::stream::iter(items.take(n).collect::<Vec<_>>())
                    .chain(futures::stream::pending())
                    .boxed(),
                None => futures::stream::iter(items.collect::<Vec<_>>()).boxed(),
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_archive_tip_that_never_answers_does_not_freeze() {
        let history: Arc<dyn History> = Arc::new(HangingHistory {
            hang_after: Mutex::new(None),
        });
        let started = tokio::time::Instant::now();
        let (got, resumes) = play_with::<Event>(
            Some(history),
            Some(1),
            vec![Err(too_old(4)), Ok(vec![cp(4), cp(5)])],
            5,
        )
        .await;
        assert_eq!(got, ["1", "2", "3", "4", "5"]);
        assert_eq!(resumes, [Some(1), Some(4)]);
        // Paused time jumps to any timer, however far: bound it explicitly.
        assert!(
            started.elapsed() <= Duration::from_secs(20),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_archive_that_stalls_mid_backfill_is_retried() {
        let history: Arc<dyn History> = Arc::new(HangingHistory {
            hang_after: Mutex::new(Some(1)),
        });
        let started = tokio::time::Instant::now();
        let (got, resumes) = play_with::<Event>(
            Some(history),
            Some(1),
            vec![Err(too_old(4)), Err(too_old(4)), Ok(vec![cp(4)])],
            4,
        )
        .await;
        assert_eq!(got, ["1", "2", "3", "4"]);
        assert_eq!(resumes, [Some(1), Some(2), Some(4)]);
        assert!(
            started.elapsed() <= Duration::from_secs(5 * 60),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_connection_with_only_keepalives_is_dropped_and_resumed() {
        // The first connection delivers 1, then stays open with nothing
        // (keepalives never reach the driver): the watchdog resumes from 2.
        let source = Arc::new(Scripted {
            conns: Mutex::new(vec![Ok(vec![cp(1)])].into()),
            resumes: Mutex::new(vec![]),
        });
        struct Then(Arc<Scripted>, Mutex<VecDeque<Script>>);
        impl Source for Then {
            fn connect(
                &self,
                next: Option<u64>,
            ) -> BoxFuture<'static, Result<EventStream, ConnError>> {
                self.0.resumes.lock().unwrap().push(next);
                let first = self.0.conns.lock().unwrap().pop_front();
                let later = if first.is_none() {
                    self.1.lock().unwrap().pop_front()
                } else {
                    None
                };
                Box::pin(async move {
                    match (first, later) {
                        // Delivers its events, then hangs (open, silent).
                        (Some(Ok(events)), _) => Ok(futures::stream::iter(events)
                            .chain(futures::stream::pending())
                            .boxed()),
                        (_, Some(Ok(events))) => Ok(futures::stream::iter(events).boxed()),
                        _ => Ok(futures::stream::pending().boxed()),
                    }
                })
            }
        }
        let then = Arc::new(Then(
            source.clone(),
            Mutex::new(vec![Ok(vec![cp(2), cp(3)])].into()),
        ));
        let (tx, mut rx) = mpsc::channel(8);
        let started = tokio::time::Instant::now();
        let task = tokio::spawn(run::<Event>(
            then,
            None,
            Some(1),
            None,
            tx,
            Deadlines::default(),
        ));
        let mut got = vec![];
        while got.len() < 3 {
            got.push(rx.recv().await.unwrap().unwrap().show());
        }
        task.abort();
        assert_eq!(got, ["1", "2", "3"]);
        assert_eq!(&source.resumes.lock().unwrap()[..2], [Some(1), Some(2)]);
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_secs(60) && waited < Duration::from_secs(120),
            "{waited:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_connect_that_never_answers_is_retried() {
        struct HangOnce(Mutex<bool>, Mutex<Vec<Option<u64>>>);
        impl Source for HangOnce {
            fn connect(
                &self,
                next: Option<u64>,
            ) -> BoxFuture<'static, Result<EventStream, ConnError>> {
                self.1.lock().unwrap().push(next);
                let hang = std::mem::replace(&mut *self.0.lock().unwrap(), false);
                Box::pin(async move {
                    if hang {
                        futures::future::pending::<()>().await;
                    }
                    Ok(futures::stream::iter(vec![cp(7), cp(8)]).boxed())
                })
            }
        }
        let source = Arc::new(HangOnce(Mutex::new(true), Mutex::new(vec![])));
        let started = tokio::time::Instant::now();
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn(run::<Event>(
            source.clone(),
            None,
            Some(7),
            None,
            tx,
            Deadlines::default(),
        ));
        let mut got = vec![];
        while got.len() < 2 {
            got.push(rx.recv().await.unwrap().unwrap().show());
        }
        task.abort();
        assert_eq!(got, ["7", "8"]);
        assert_eq!(&source.1.lock().unwrap()[..2], [Some(7), Some(7)]);
        assert!(
            started.elapsed() <= Duration::from_secs(3 * 60),
            "{:?}",
            started.elapsed()
        );
    }
}
