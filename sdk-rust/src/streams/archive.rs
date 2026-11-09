//! Backfill from the archive (`https://archive.mainnet.sui.io`, every
//! checkpoint since genesis, kept up to the tip) for positions older than
//! the gateway's ring: `LedgerService.ListCheckpoints`, 50 checkpoints a
//! page (the archive's maximum), a few pages at a time, in order. The
//! archive also reports its tip, so a backfill can run all the way up to it
//! and the stream is rejoined within its ring, instead of chasing a ring
//! that moves on while history is read.
//!
//! Checkpoints come out exactly as the gateway would serve them: the
//! request asks for what the gateway sends under the caller's mask,
//! and the object types fullnodes fill in effects (the archive does not)
//! are filled from the checkpoint's objects; helper fields fetched only for
//! that are masked away again.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use sui_rpc::field::{FieldMask, FieldMaskTree, FieldMaskUtil};
use sui_rpc::merge::Merge;
use sui_rpc::proto::sui::rpc::v2::{GetServiceInfoRequest, ListCheckpointsRequest, QueryOptions};

use tokio::sync::mpsc;

use crate::streams::Checkpoint;

pub const DEFAULT_ARCHIVE_URL: &str = "https://archive.mainnet.sui.io";

/// Every field the gateway holds (its `common::CANONICAL_READ_MASK`): what
/// the stream sends every client, BCS encodings included, and what `*` and
/// parent paths stand for.
pub const CANONICAL: &[&str] = &[
    "sequence_number",
    "digest",
    "summary",
    "signature",
    "contents",
    "transactions.digest",
    "transactions.transaction",
    "transactions.signatures",
    "transactions.effects",
    "transactions.checkpoint",
    "transactions.timestamp",
    "transactions.balance_changes",
    "transactions.events.bcs",
    "transactions.events.digest",
    "transactions.events.events.package_id",
    "transactions.events.events.module",
    "transactions.events.events.sender",
    "transactions.events.events.event_type",
    "transactions.events.events.contents",
    "objects.objects.bcs",
    "objects.objects.object_id",
    "objects.objects.version",
    "objects.objects.digest",
    "objects.objects.owner",
    "objects.objects.object_type",
    "objects.objects.has_public_transfer",
    "objects.objects.contents",
    "objects.objects.package",
    "objects.objects.previous_transaction",
    "objects.objects.storage_rebate",
    "objects.objects.balance",
];

/// The BCS encodings within the canonical checkpoint (the server's `BCS_PATHS`): each
/// repeats decoded fields as the node's BCS bytes. Served like any other
/// field; a read mask may name them.
pub const BCS_PATHS: &[&str] = &[
    "summary.bcs",
    "contents.bcs",
    "transactions.transaction.bcs",
    "transactions.signatures.bcs",
    "transactions.effects.bcs",
    "transactions.events.bcs",
    "objects.objects.bcs",
];

/// The decoded fields (`common::DEFAULT_READ_MASK`): [`CANONICAL`] minus
/// [`BCS_PATHS`].
#[cfg(test)]
pub const DEFAULT: &[&str] = &[
    "sequence_number",
    "digest",
    "summary.digest",
    "summary.epoch",
    "summary.sequence_number",
    "summary.total_network_transactions",
    "summary.content_digest",
    "summary.previous_digest",
    "summary.epoch_rolling_gas_cost_summary",
    "summary.timestamp",
    "summary.commitments",
    "summary.end_of_epoch_data",
    "summary.version_specific_data",
    "signature",
    "contents.digest",
    "contents.version",
    "contents.transactions",
    "transactions.digest",
    "transactions.transaction.digest",
    "transactions.transaction.version",
    "transactions.transaction.kind",
    "transactions.transaction.sender",
    "transactions.transaction.gas_payment",
    "transactions.transaction.expiration",
    "transactions.signatures.scheme",
    "transactions.signatures.simple",
    "transactions.signatures.multisig",
    "transactions.signatures.zklogin",
    "transactions.signatures.passkey",
    "transactions.effects.digest",
    "transactions.effects.version",
    "transactions.effects.status",
    "transactions.effects.epoch",
    "transactions.effects.gas_used",
    "transactions.effects.transaction_digest",
    "transactions.effects.gas_object",
    "transactions.effects.events_digest",
    "transactions.effects.dependencies",
    "transactions.effects.lamport_version",
    "transactions.effects.changed_objects",
    "transactions.effects.unchanged_consensus_objects",
    "transactions.effects.auxiliary_data_digest",
    "transactions.effects.unchanged_loaded_runtime_objects",
    "transactions.checkpoint",
    "transactions.timestamp",
    "transactions.balance_changes",
    "transactions.events.digest",
    "transactions.events.events.package_id",
    "transactions.events.events.module",
    "transactions.events.events.sender",
    "transactions.events.events.event_type",
    "transactions.events.events.contents",
    "objects.objects.object_id",
    "objects.objects.version",
    "objects.objects.digest",
    "objects.objects.owner",
    "objects.objects.object_type",
    "objects.objects.has_public_transfer",
    "objects.objects.contents",
    "objects.objects.package",
    "objects.objects.previous_transaction",
    "objects.objects.storage_rebate",
    "objects.objects.balance",
];

const TYPED: &[&str] = &[
    "transactions.effects.changed_objects.object_type",
    "transactions.effects.unchanged_consensus_objects.object_type",
];
const TYPE_SOURCES: &[&str] = &[
    "objects.objects.object_id",
    "objects.objects.version",
    "objects.objects.object_type",
];

/// Whether mask path `parent` includes field path `path`.
pub(crate) fn covers(parent: &str, path: &str) -> bool {
    path == parent
        || path
            .strip_prefix(parent)
            .is_some_and(|rest| rest.starts_with('.'))
}

/// What to ask the archive for, and how to finish its answer.
#[derive(Debug, PartialEq)]
pub(crate) struct Plan {
    /// Request paths: what the gateway sends for the caller's mask.
    pub request: Vec<String>,
    /// Fill object types in effects.
    pub fill: bool,
    /// Re-apply the caller's mask afterwards: the archive adds fields of
    /// its own (e.g. object ids and versions), and helper fields may have
    /// been requested for the object types.
    pub remask: Vec<String>,
}

/// The fields kept for `paths`: their overlap with [`CANONICAL`] (what the
/// stream sends); paths outside [`CANONICAL`] (always empty) as they are.
/// No paths: [`CANONICAL`].
pub(crate) fn effective(paths: &[String]) -> Vec<String> {
    let served: &[&str] = CANONICAL;
    if paths.is_empty() {
        return served.iter().map(|s| (*s).to_owned()).collect();
    }
    let mut out: Vec<String> = Vec::new();
    let mut add = |p: &str| {
        if !out.iter().any(|r| covers(r, p)) {
            out.retain(|r| !covers(p, r));
            out.push(p.to_owned());
        }
    };
    for p in paths {
        let mut hit = false;
        for s in served {
            if p == "*" || covers(p, s) {
                add(s);
                hit = true;
            } else if covers(s, p) {
                add(p);
                hit = true;
            }
        }
        if !hit && !CANONICAL.iter().any(|c| covers(p, c) || covers(c, p)) {
            add(p);
        }
    }
    out
}

/// `mask`: the caller's paths (`None`: the whole checkpoint), already
/// validated by [`crate::streams::mask::resolve`].
pub(crate) fn plan(mask: Option<&[String]>) -> Plan {
    let mut request = effective(mask.unwrap_or(&[]));
    let fill = TYPED
        .iter()
        .any(|t| request.iter().any(|r| covers(r, t) || covers(t, r)));
    let remask = request.clone();
    if fill {
        for src in TYPE_SOURCES {
            if !request.iter().any(|r| covers(r, src)) {
                request.push((*src).to_owned());
            }
        }
    }
    Plan {
        request,
        fill,
        remask,
    }
}

/// Fill `object_type` of changed and unchanged consensus objects from the
/// checkpoint's own objects, as fullnodes do: the object at the input
/// version, or at the output version for created objects.
pub(crate) fn fill_object_types(checkpoint: &mut Checkpoint) {
    let Some(objects) = &checkpoint.objects else {
        return;
    };
    let types: HashMap<(&str, u64), &str> = objects
        .objects
        .iter()
        .filter_map(|o| {
            Some((
                (o.object_id.as_deref()?, o.version?),
                o.object_type.as_deref()?,
            ))
        })
        .collect();
    if types.is_empty() {
        return;
    }
    for effects in checkpoint
        .transactions
        .iter_mut()
        .filter_map(|tx| tx.effects.as_mut())
    {
        for changed in &mut effects.changed_objects {
            let (Some(id), Some(version)) = (
                changed.object_id.as_deref(),
                changed.input_version.or(changed.output_version),
            ) else {
                continue;
            };
            if let Some(t) = types.get(&(id, version)) {
                changed.object_type = Some((*t).to_owned());
            }
        }
        for unchanged in &mut effects.unchanged_consensus_objects {
            let (Some(id), Some(version)) = (unchanged.object_id.as_deref(), unchanged.version)
            else {
                continue;
            };
            if let Some(t) = types.get(&(id, version)) {
                unchanged.object_type = Some((*t).to_owned());
            }
        }
    }
}

/// One step of an ordered backfill.
#[derive(Debug)]
pub(crate) enum Item {
    Checkpoint(u64, Box<Checkpoint>),
    /// Not in the archive.
    Missing(u64),
    /// Past the archive's tip (`LEDGER_TIP`): not there yet, so not a gap.
    /// Nothing after it is asked for.
    NotYet(u64),
}

/// Ordered history for a range. Abstracted for tests.
pub(crate) trait History: Send + Sync + 'static {
    /// `range` in order; an error ends the stream (the caller retries).
    fn checkpoints(&self, range: Range<u64>) -> BoxStream<'static, Result<Item, String>>;

    /// The newest checkpoint history holds, if it can say: a backfill runs up to it.
    fn tip(&self) -> BoxFuture<'static, Option<u64>> {
        Box::pin(async { None })
    }
}

/// Checkpoints per `ListCheckpoints` page: the archive's maximum.
pub(crate) const PAGE: u64 = 50;

/// A `ListCheckpoints` call's first response, enforced here (tonic only
/// sends `grpc-timeout` to the server): an archive that doesn't answer is
/// retried (and logged) within 30 s, not after a page's whole budget.
const FIRST_RESPONSE_DEADLINE: Duration = Duration::from_secs(30);
/// Between two messages of one `ListCheckpoints` answer: a backstop only.
/// A stalled answer is caught by sui-rpc's body watchdog (no HTTP/2 frame
/// for 30 s: bytes, not messages), so a single epoch-change checkpoint
/// (10-20 MB) on a slow but working link is not cut off and re-read forever.
const MESSAGE_DEADLINE: Duration = CALL_DEADLINE;
/// A whole call (`grpc-timeout`, which sui-rpc also enforces): generous, so
/// a slow but working link isn't cut off mid-page (a retry continues after
/// what arrived anyway).
const CALL_DEADLINE: Duration = Duration::from_secs(600);
/// Asks of a short page before its absent checkpoints count as missing: a
/// gap is final for the caller, so a hole must persist (~30 s).
const SHORT_ASKS: u32 = 5;
/// One `GetServiceInfo` (the archive's tip).
const TIP_DEADLINE: Duration = Duration::from_secs(5);

/// `QueryEndReason::ItemLimit`: the server stopped at its own item limit; resume after the cursor.
const ITEM_LIMIT: i32 = 1;
/// `QueryEndReason::LedgerTip`: the range runs past what the archive holds.
const LEDGER_TIP: i32 = 5;

pub(crate) struct Archive {
    url: String,
    /// Created on first use: it needs a Tokio runtime, `build()` does not.
    client: std::sync::OnceLock<sui_rpc::Client>,
    request: FieldMask,
    fill: bool,
    remask: Arc<FieldMaskTree>,
    concurrency: usize,
}

impl Archive {
    pub fn new(url: &str, mask: Option<&[String]>, concurrency: usize) -> Result<Self, String> {
        match url.parse::<http::Uri>() {
            Ok(uri) if uri.scheme().is_some() && uri.host().is_some() => {}
            _ => return Err(format!("archive url {url:?}: not an absolute URL")),
        }
        let plan = plan(mask);
        Ok(Self {
            url: url.to_owned(),
            client: std::sync::OnceLock::new(),
            // Pages are placed by sequence number: always ask for it (masked away again if unwanted).
            request: FieldMask::from_paths(
                plan.request.iter().map(String::as_str).chain(
                    (!plan.request.iter().any(|r| covers(r, "sequence_number")))
                        .then_some("sequence_number"),
                ),
            ),
            fill: plan.fill,
            remask: Arc::new(FieldMaskTree::from_field_mask(&FieldMask::from_paths(
                &plan.remask,
            ))),
            concurrency: concurrency.max(1),
        })
    }

    /// One `ListCheckpoints` call for `range`, continued from its cursor if
    /// the server stops at an item limit of its own; each checkpoint is
    /// filled, masked and handed to `rel` as it arrives (which releases it
    /// downstream as soon as everything before it in the page is known), so
    /// a slow link delivers steadily, and a retry continues after what
    /// arrived. `Ok(reason)`: the stream's `QueryEnd` reason; a stream
    /// without one, a cursor that doesn't move, or more items than the range
    /// holds are errors.
    async fn list(
        mut client: sui_rpc::Client,
        mask: FieldMask,
        range: Range<u64>,
        fill: bool,
        remask: &FieldMaskTree,
        rel: &mut Release,
    ) -> Result<i32, tonic::Status> {
        let len = (range.end - range.start) as usize;
        let mut after: Option<prost::bytes::Bytes> = None;
        // Checkpoints this call received (re-reads included): bounded by the range.
        let mut received = 0usize;
        loop {
            let mut request = ListCheckpointsRequest::default();
            request.read_mask = Some(mask.clone());
            request.start_checkpoint = Some(range.start);
            request.end_checkpoint = Some(range.end);
            let mut options = QueryOptions::default();
            options.limit = Some(len as u32);
            options.after = after.clone();
            request.options = Some(options);
            let mut request = tonic::Request::new(request);
            // The grpc-timeout header only asks the server; the client
            // enforces its own deadlines. A stall mid-answer is sui-rpc's
            // body watchdog's (no HTTP/2 frame for 30 s: bytes, not whole
            // messages, so one 20 MB checkpoint on a slow link isn't cut).
            request.set_timeout(CALL_DEADLINE);
            let mut frames = tokio::time::timeout(
                FIRST_RESPONSE_DEADLINE,
                client.ledger_client().list_checkpoints(request),
            )
            .await
            .map_err(|_| tonic::Status::deadline_exceeded("ListCheckpoints: no response"))??
            .into_inner();
            let (mut reason, mut cursor, mut last) = (None, None, None);
            let before = received;
            while let Some(frame) = tokio::time::timeout(MESSAGE_DEADLINE, frames.message())
                .await
                .map_err(|_| {
                    tonic::Status::deadline_exceeded("ListCheckpoints: stalled mid-stream")
                })??
            {
                if let Some(mut cp) = frame.checkpoint {
                    if received >= len {
                        return Err(tonic::Status::internal(format!(
                            "ListCheckpoints {}..{}: more than {len} checkpoints",
                            range.start,
                            range.end - 1
                        )));
                    }
                    received += 1;
                    last = cp.sequence_number;
                    if fill {
                        fill_object_types(&mut cp);
                    }
                    // Placed by its sequence number, which the mask may drop.
                    rel.push(cp.sequence_number, Checkpoint::merge_from(&cp, remask))
                        .map_err(tonic::Status::internal)?;
                }
                if let Some(c) = frame.watermark.and_then(|w| w.cursor) {
                    cursor = Some(c);
                }
                if let Some(end) = frame.end {
                    reason = end.reason;
                }
            }
            let Some(reason) = reason else {
                // `end` is set exactly once on a stream that finished: one
                // without it was cut short, whatever its status.
                return Err(tonic::Status::unavailable(
                    "ListCheckpoints: ended without QueryEnd",
                ));
            };
            if reason != ITEM_LIMIT || last.is_some_and(|l: u64| l + 1 >= range.end) || rel.done() {
                return Ok(reason);
            }
            // Continue after the cursor, only if it moved and brought something.
            if cursor.is_none() || cursor == after || received == before {
                return Err(tonic::Status::unavailable(
                    "ListCheckpoints: item limit without progress",
                ));
            }
            after = cursor;
        }
    }

    /// One page into `tx`, in order, each checkpoint as soon as everything
    /// before it is known: failures retried (6 attempts in all, each
    /// continuing after what arrived), each one logged; absent checkpoints
    /// (a short page, or holes below what a `LEDGER_TIP` answer held) asked
    /// again `SHORT_ASKS` times over ~30 s before they are `Missing` (the
    /// driver decides when missing is a gap); after the last one held of a
    /// `LEDGER_TIP` answer: `NotYet`, and the page ends there.
    async fn page(
        client: sui_rpc::Client,
        mask: FieldMask,
        range: Range<u64>,
        fill: bool,
        remask: Arc<FieldMaskTree>,
        tx: mpsc::UnboundedSender<Result<Item, String>>,
    ) {
        let mut rel = Release::new(range.clone(), tx);
        let mut backoff = Duration::from_millis(250);
        let (mut errors, mut short) = (0u32, 0u32);
        loop {
            if rel.done() {
                return;
            }
            // Everything before `next` is released; what arrived after it is held.
            let start = rel.next;
            let result = Self::list(
                client.clone(),
                mask.clone(),
                start..range.end,
                fill,
                &remask,
                &mut rel,
            )
            .await;
            if rel.done() || rel.closed() {
                return;
            }
            let failed = result.is_err();
            match result {
                // Past the archive's tip, and nothing held after the gap: the rest isn't there yet.
                Ok(LEDGER_TIP) if rel.pending.is_empty() => {
                    rel.not_yet();
                    return;
                }
                Ok(reason) if short >= SHORT_ASKS => {
                    // Holes that persisted: missing (up to the last held; after it, a
                    // `LEDGER_TIP` answer's rest isn't there yet).
                    rel.give_up(reason == LEDGER_TIP);
                    return;
                }
                Ok(_) => {
                    short += 1;
                    tracing::warn!(
                        "archive ListCheckpoints {start}..{}: checkpoint {start} absent ({} after it held); asking again ({short} of {SHORT_ASKS})",
                        range.end - 1,
                        rel.pending.len(),
                    );
                }
                Err(s) => {
                    errors += 1;
                    let e = format!(
                        "archive ListCheckpoints {}..{}: {:?}: {}",
                        start,
                        range.end - 1,
                        s.code(),
                        s.message()
                    );
                    if errors >= 6 {
                        rel.fail(e);
                        return;
                    }
                    tracing::warn!("{e}; retrying (attempt {errors} of 6)");
                }
            }
            // A short page waits 1, 2, 4, 8, 16 s whatever errors came before.
            tokio::time::sleep(if !failed {
                Duration::from_secs(1 << (short - 1))
            } else {
                backoff
            })
            .await;
            if failed {
                backoff = (backoff * 2).min(Duration::from_secs(4));
            }
        }
    }
}

/// A page's checkpoints, released in order as they arrive.
struct Release {
    range: Range<u64>,
    /// Next position owed downstream: everything before it is released.
    next: u64,
    /// Arrived after `next`, waiting for what comes before.
    pending: std::collections::BTreeMap<u64, Checkpoint>,
    tx: mpsc::UnboundedSender<Result<Item, String>>,
}

impl Release {
    fn new(range: Range<u64>, tx: mpsc::UnboundedSender<Result<Item, String>>) -> Self {
        Self {
            next: range.start,
            range,
            pending: Default::default(),
            tx,
        }
    }
    fn done(&self) -> bool {
        self.next >= self.range.end
    }
    /// The consumer is gone (the backfill stopped).
    fn closed(&self) -> bool {
        self.tx.is_closed()
    }
    fn send(&self, item: Item) {
        let _ = self.tx.send(Ok(item));
    }
    /// One arrival: outside the page or without a sequence number is the
    /// archive misbehaving; one already held is a re-read (dropped).
    fn push(&mut self, seq: Option<u64>, cp: Checkpoint) -> Result<(), String> {
        let Some(seq) = seq.filter(|s| self.range.contains(s)) else {
            return Err(format!(
                "archive returned checkpoint {seq:?} for {}..{}",
                self.range.start,
                self.range.end - 1
            ));
        };
        if seq < self.next || self.pending.contains_key(&seq) {
            return Ok(());
        }
        self.pending.insert(seq, cp);
        while let Some(cp) = self.pending.remove(&self.next) {
            self.send(Item::Checkpoint(self.next, Box::new(cp)));
            self.next += 1;
        }
        Ok(())
    }
    /// The rest isn't in the archive yet.
    fn not_yet(&mut self) {
        self.send(Item::NotYet(self.next));
        self.next = self.range.end;
    }
    /// Absent ones are missing, up to the last held; after it, missing too,
    /// or (`past_tip`) not there yet.
    fn give_up(&mut self, past_tip: bool) {
        while !self.done() {
            match self.pending.remove(&self.next) {
                Some(cp) => self.send(Item::Checkpoint(self.next, Box::new(cp))),
                None if past_tip && self.pending.is_empty() => return self.not_yet(),
                None => self.send(Item::Missing(self.next)),
            }
            self.next += 1;
        }
    }
    fn fail(&mut self, e: String) {
        let _ = self.tx.send(Err(e));
        self.next = self.range.end;
    }
}

/// A page task, aborted when the backfill is dropped.
struct PageTask(tokio::task::JoinHandle<()>);
impl Drop for PageTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Archive {
    fn client(&self) -> Result<sui_rpc::Client, String> {
        if let Some(c) = self.client.get() {
            return Ok(c.clone());
        }
        let c = sui_rpc::Client::new(self.url.as_str())
            .map_err(|e| format!("archive {:?}: {e}", self.url))?
            .with_max_decoding_message_size(256 * 1024 * 1024);
        Ok(self.client.get_or_init(|| c).clone())
    }
}

impl History for Archive {
    fn checkpoints(&self, range: Range<u64>) -> BoxStream<'static, Result<Item, String>> {
        let client = match self.client() {
            Ok(c) => c,
            Err(e) => return futures::stream::once(async move { Err(e) }).boxed(),
        };
        let (mask, fill, remask, concurrency) = (
            self.request.clone(),
            self.fill,
            self.remask.clone(),
            self.concurrency,
        );
        // Pages in order, `concurrency` in flight, each read by its own task
        // (so it keeps reading while an earlier one is consumed) and handed
        // over item by item; an error ends the stream, and dropping it
        // aborts the pages in flight.
        struct State {
            next: u64,
            end: u64,
            inflight: std::collections::VecDeque<(
                mpsc::UnboundedReceiver<Result<Item, String>>,
                PageTask,
            )>,
        }
        let st = State {
            next: range.start,
            end: range.end,
            inflight: Default::default(),
        };
        futures::stream::unfold(st, move |mut st| {
            let (client, mask, remask) = (client.clone(), mask.clone(), remask.clone());
            async move {
                loop {
                    while st.inflight.len() < concurrency && st.next < st.end {
                        let page = st.next..(st.next + PAGE).min(st.end);
                        st.next = page.end;
                        let (tx, rx) = mpsc::unbounded_channel();
                        let task = tokio::spawn(Self::page(
                            client.clone(),
                            mask.clone(),
                            page,
                            fill,
                            remask.clone(),
                            tx,
                        ));
                        st.inflight.push_back((rx, PageTask(task)));
                    }
                    let (rx, _) = st.inflight.front_mut()?;
                    match rx.recv().await {
                        Some(item) => return Some((item, st)),
                        None => {
                            st.inflight.pop_front();
                        }
                    }
                }
            }
        })
        .boxed()
    }

    fn tip(&self) -> BoxFuture<'static, Option<u64>> {
        let client = self.client();
        Box::pin(async move {
            let mut client = client.ok()?;
            for attempt in 0..3u32 {
                let mut request = tonic::Request::new(GetServiceInfoRequest::default());
                request.set_timeout(TIP_DEADLINE);
                match tokio::time::timeout(
                    TIP_DEADLINE,
                    client.ledger_client().get_service_info(request),
                )
                .await
                {
                    Ok(Ok(r)) => return r.into_inner().checkpoint_height,
                    Ok(Err(s)) => tracing::debug!("archive tip: {:?}: {}", s.code(), s.message()),
                    Err(_) => tracing::debug!("archive tip: no answer in {TIP_DEADLINE:?}"),
                }
                tokio::time::sleep(Duration::from_millis(250 << attempt)).await;
            }
            None
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(mask: &[&str]) -> Plan {
        plan(Some(
            &mask.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        ))
    }

    /// The lists match the server's (`suistream/crates/common`, same
    /// repository; skipped outside it).
    #[test]
    fn lists_match_the_server() {
        let Some(common) = crate::streams::server_common_lib() else {
            return;
        };
        let common = common.as_str();
        let list = |name: &str| -> Vec<String> {
            let start = common
                .find(&format!("pub const {name}: &[&str] = &["))
                .unwrap();
            let body = &common[start..];
            let body = &body[body.find('[').unwrap() + 4..body.find("];").unwrap()];
            body.split('"')
                .skip(1)
                .step_by(2)
                .map(str::to_owned)
                .collect()
        };
        assert_eq!(list("CANONICAL_READ_MASK"), CANONICAL);
        assert_eq!(list("BCS_PATHS"), BCS_PATHS);
        assert_eq!(list("DEFAULT_READ_MASK"), DEFAULT);
    }

    #[test]
    fn plans_requests() {
        // No mask: the full canonical checkpoint (BCS included), like the
        // live stream; types filled from its objects.
        let default = plan(None);
        assert_eq!(default.request, CANONICAL);
        assert_eq!(default.remask, CANONICAL);
        assert!(default.fill);
        assert_eq!(p(&["*"]), default);
        // Parents expand to what the stream sends: BCS, never `events.json`.
        let events = p(&["transactions.events"]);
        assert!(
            events
                .request
                .contains(&"transactions.events.events.event_type".into())
        );
        assert!(events.request.contains(&"transactions.events.bcs".into()));
        assert!(!events.request.iter().any(|r| r.ends_with("json")));
        assert!(!events.fill);
        let bcs = p(&["transactions.events", "transactions.events.bcs"]);
        assert_eq!(bcs.request, events.request);
        // BCS paths are requested when named.
        assert_eq!(p(&["summary.bcs"]).request, ["summary.bcs"]);
        let effects_bcs = p(&["sequence_number", "transactions.effects.bcs"]);
        assert_eq!(
            effects_bcs.request,
            ["sequence_number", "transactions.effects.bcs"]
        );
        assert!(!effects_bcs.fill);
        // The decoded fields alone: no BCS requested.
        assert!(!p(DEFAULT).request.iter().any(|r| r.ends_with(".bcs")));
        // Narrower than a served path: kept as is.
        let status = p(&["transactions.effects.status", "digest"]);
        assert_eq!(status.request, ["transactions.effects.status", "digest"]);
        assert!(!status.fill);
        // Changed objects need the object set for their types, then the
        // helper fields are masked away again.
        let changed = p(&["transactions.effects.changed_objects", "sequence_number"]);
        assert!(changed.fill);
        assert_eq!(
            changed.remask,
            ["transactions.effects.changed_objects", "sequence_number"]
        );
        assert!(
            changed
                .request
                .contains(&"objects.objects.object_type".into())
        );
        // Already requested: nothing added.
        let both = p(&["transactions.effects", "objects"]);
        assert!(both.fill);
        assert_eq!(both.request, both.remask);
    }

    #[test]
    fn remask_drops_helper_fields() {
        use sui_rpc::proto::sui::rpc::v2::{Object, ObjectSet};
        let changed = p(&["sequence_number", "transactions.effects.changed_objects"]);
        let tree = FieldMaskTree::from_field_mask(&FieldMask::from_paths(changed.remask));
        let mut cp = Checkpoint::default();
        cp.sequence_number = Some(1);
        let mut set = ObjectSet::default();
        set.objects = vec![Object::default()];
        cp.objects = Some(set);
        let out = Checkpoint::merge_from(&cp, &tree);
        assert_eq!(out.sequence_number, Some(1));
        assert!(out.objects.is_none());
    }

    #[test]
    fn pages_are_released_in_order_with_missing() {
        let show = |rx: &mut mpsc::UnboundedReceiver<Result<Item, String>>| -> Vec<String> {
            let mut out = vec![];
            while let Ok(i) = rx.try_recv() {
                out.push(match i {
                    Ok(Item::Checkpoint(s, _)) => format!("{s}"),
                    Ok(Item::Missing(s)) => format!("missing {s}"),
                    Ok(Item::NotYet(s)) => format!("not yet {s}"),
                    Err(e) => format!("error {e}"),
                });
            }
            out
        };
        let cp = Checkpoint::default;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut rel = Release::new(10..14, tx);
        // Out of order: held until what comes before it is known.
        rel.push(Some(13), cp()).unwrap();
        rel.push(Some(10), cp()).unwrap();
        assert_eq!(show(&mut rx), ["10"]);
        rel.push(Some(12), cp()).unwrap();
        // A re-read: dropped.
        rel.push(Some(10), cp()).unwrap();
        rel.push(Some(12), cp()).unwrap();
        assert_eq!(show(&mut rx), Vec::<String>::new());
        rel.give_up(false);
        assert_eq!(show(&mut rx), ["missing 11", "12", "13"]);
        assert!(rel.done());
        // Past the archive's tip, what follows the last one held isn't missing: it isn't there yet.
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut rel = Release::new(10..14, tx);
        rel.push(Some(10), cp()).unwrap();
        rel.push(Some(12), cp()).unwrap();
        rel.give_up(true);
        assert_eq!(show(&mut rx), ["10", "missing 11", "12", "not yet 13"]);
        // Outside the page, or without a sequence number: the archive misbehaved.
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut rel = Release::new(10..14, tx);
        assert!(rel.push(Some(14), cp()).is_err());
        assert!(rel.push(None, cp()).is_err());
    }

    /// Opt-in: archive backfill under a mask equals the gateway's render of
    /// the same checkpoint (fullnode `GetCheckpoint`, canonical mask, then
    /// the caller's mask), apart from the aggregated signature.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn archive_matches_fullnode_under_masks() {
        use sui_rpc::proto::sui::rpc::v2::{
            GetCheckpointRequest, get_checkpoint_request::CheckpointId,
        };
        let fullnode_url = "https://fullnode.mainnet.sui.io";
        let mut fullnode = sui_rpc::Client::new(fullnode_url).unwrap();
        let head = fullnode
            .ledger_client()
            .get_service_info(GetServiceInfoRequest::default())
            .await
            .unwrap()
            .into_inner()
            .checkpoint_height
            .unwrap();
        let range = (head - 3_000)..(head - 2_995);
        let canonical = FieldMask::from_paths(CANONICAL);
        let masks: [Option<&[&str]>; 4] = [
            None,
            Some(&[
                "sequence_number",
                "transactions.effects.changed_objects",
                "transactions.events",
            ]),
            Some(&["transactions.effects.status", "objects.objects.object_type"]),
            Some(&["transactions.effects", "summary"]),
        ];
        for mask in masks {
            let owned = mask.map(|m| m.iter().map(|s| s.to_string()).collect::<Vec<_>>());
            let archive = Archive::new(DEFAULT_ARCHIVE_URL, owned.as_deref(), 8).unwrap();
            let got: Vec<_> = archive.checkpoints(range.clone()).collect().await;
            for (seq, item) in range.clone().zip(got) {
                let Ok(Item::Checkpoint(s, mut got)) = item else {
                    panic!("{seq}: {item:?}")
                };
                assert_eq!(s, seq);
                let mut request = GetCheckpointRequest::default();
                request.read_mask = Some(canonical.clone());
                request.checkpoint_id = Some(CheckpointId::SequenceNumber(seq));
                let expected = fullnode
                    .ledger_client()
                    .get_checkpoint(request)
                    .await
                    .unwrap()
                    .into_inner()
                    .checkpoint
                    .unwrap();
                let served = effective(owned.as_deref().unwrap_or(&[]));
                let mut expected = Checkpoint::merge_from(
                    &expected,
                    &FieldMaskTree::from_field_mask(&FieldMask::from_paths(&served)),
                );
                got.signature = None;
                expected.signature = None;
                // Sponsored transactions: signature order may differ.
                for tx in got
                    .transactions
                    .iter_mut()
                    .chain(expected.transactions.iter_mut())
                {
                    tx.signatures.sort_by_key(|s| format!("{s:?}"));
                }
                assert!(*got == expected, "{seq} under {mask:?} differs");
            }
            eprintln!("{mask:?}: {} checkpoints equal", range.end - range.start);
        }
    }
}
