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
/// Between two messages of one `ListCheckpoints` answer.
const MESSAGE_DEADLINE: Duration = Duration::from_secs(30);
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

    /// What the archive has in `range`, each filled and masked as it
    /// arrives (a page holds only what the caller keeps): one
    /// `ListCheckpoints` call, continued from its cursor if the server stops
    /// at an item limit of its own. Whatever arrived is returned even when
    /// the call fails, so a retry continues after it rather than starting
    /// over (a slow link still makes progress). `Ok(reason)`: the stream's
    /// `QueryEnd` reason; a stream without one, a cursor that doesn't move,
    /// or more items than the range holds are errors.
    async fn list(
        mut client: sui_rpc::Client,
        mask: FieldMask,
        range: Range<u64>,
        fill: bool,
        remask: &FieldMaskTree,
        out: &mut Vec<(Option<u64>, Checkpoint)>,
    ) -> Result<i32, tonic::Status> {
        let len = (range.end - range.start) as usize;
        let mut after: Option<prost::bytes::Bytes> = None;
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
            // Epoch-change checkpoints are 10-20 MB and slow to serve. The
            // grpc-timeout header only asks the server; the client enforces
            // its own deadlines (`FIRST_RESPONSE_DEADLINE`, `MESSAGE_DEADLINE`).
            request.set_timeout(CALL_DEADLINE);
            // `ledger_client()` asks for zstd responses (~6x smaller).
            let mut frames = tokio::time::timeout(
                FIRST_RESPONSE_DEADLINE,
                client.ledger_client().list_checkpoints(request),
            )
            .await
            .map_err(|_| tonic::Status::deadline_exceeded("ListCheckpoints: no response"))??
            .into_inner();
            let (mut reason, mut cursor) = (None, None);
            let before = out.len();
            while let Some(frame) = tokio::time::timeout(MESSAGE_DEADLINE, frames.message())
                .await
                .map_err(|_| {
                    tonic::Status::deadline_exceeded("ListCheckpoints: stalled mid-stream")
                })??
            {
                if let Some(mut cp) = frame.checkpoint {
                    if out.len() >= len {
                        return Err(tonic::Status::internal(format!(
                            "ListCheckpoints {}..{}: more than {len} checkpoints",
                            range.start,
                            range.end - 1
                        )));
                    }
                    if fill {
                        fill_object_types(&mut cp);
                    }
                    // Placed by its sequence number, which the mask may drop.
                    out.push((cp.sequence_number, Checkpoint::merge_from(&cp, remask)));
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
            let last = out.last().and_then(|(seq, _)| *seq);
            if reason != ITEM_LIMIT || last.is_some_and(|l| l + 1 >= range.end) {
                return Ok(reason);
            }
            // Continue after the cursor, only if it moved and brought something.
            if cursor.is_none() || cursor == after || out.len() == before {
                return Err(tonic::Status::unavailable(
                    "ListCheckpoints: item limit without progress",
                ));
            }
            after = cursor;
        }
    }

    /// One page: failures retried (6 attempts in all, each continuing after
    /// what already arrived), each one logged; a short page asked again
    /// `SHORT_ASKS` times over ~30 s before its absent checkpoints count as
    /// missing (a gap is final for the caller). Returns what it holds, and whether the archive said
    /// the rest is past its tip (`LEDGER_TIP`: not missing, not there yet).
    async fn page(
        client: sui_rpc::Client,
        mask: FieldMask,
        range: Range<u64>,
        fill: bool,
        remask: &FieldMaskTree,
    ) -> Result<(Vec<(Option<u64>, Checkpoint)>, bool), String> {
        let mut backoff = Duration::from_millis(250);
        let (mut errors, mut short) = (0u32, 0u32);
        let mut got: Vec<(Option<u64>, Checkpoint)> = Vec::new();
        loop {
            // After the run that arrived from the page's start (whatever
            // came out of order is checked by `items`).
            let held: std::collections::HashSet<u64> =
                got.iter().filter_map(|(seq, _)| *seq).collect();
            let mut start = range.start;
            while held.contains(&start) {
                start += 1;
            }
            if start >= range.end {
                return Ok((got, false));
            }
            let mut more = Vec::new();
            let result = Self::list(
                client.clone(),
                mask.clone(),
                start..range.end,
                fill,
                remask,
                &mut more,
            )
            .await;
            // A retry after a hole re-reads what follows it: keep each once.
            got.extend(
                more.into_iter()
                    .filter(|(seq, _)| seq.is_none_or(|s| !held.contains(&s))),
            );
            let complete = got.len() as u64 == range.end - range.start;
            match result {
                Ok(_) if complete => return Ok((got, false)),
                Ok(LEDGER_TIP) => return Ok((got, true)),
                Ok(_) if short >= SHORT_ASKS => return Ok((got, false)),
                Ok(_) => {
                    short += 1;
                    tracing::warn!(
                        "archive ListCheckpoints {start}..{}: {} of {} checkpoints; asking again (holes become gaps after {SHORT_ASKS} asks)",
                        range.end - 1,
                        got.len(),
                        range.end - range.start
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
                        return Err(e);
                    }
                    tracing::warn!("{e}; retrying (attempt {errors} of 6)");
                }
            }
            tokio::time::sleep(if short > 0 && errors == 0 {
                Duration::from_secs(1 << (short - 1)) // 1, 2, 4, 8, 16 s
            } else {
                backoff
            })
            .await;
            backoff = (backoff * 2).min(Duration::from_secs(4));
        }
    }

    /// `range` as items: each checkpoint (already filled and masked) or
    /// `Missing`.
    fn items(
        range: Range<u64>,
        got: Vec<(Option<u64>, Checkpoint)>,
        past_tip: bool,
    ) -> Result<Vec<Item>, String> {
        let mut by_seq = HashMap::with_capacity(got.len());
        for (seq, cp) in got {
            match seq {
                Some(seq) if range.contains(&seq) && !by_seq.contains_key(&seq) => {
                    by_seq.insert(seq, cp);
                }
                other => {
                    return Err(format!(
                        "archive returned checkpoint {other:?} for {}..{}",
                        range.start,
                        range.end - 1
                    ));
                }
            }
        }
        // Past the tip: what follows the last one held isn't there yet.
        let held_to = by_seq.keys().max().copied();
        Ok(range
            .map(|seq| match by_seq.remove(&seq) {
                None if past_tip && held_to.is_none_or(|h| seq > h) => Item::NotYet(seq),
                None => Item::Missing(seq),
                Some(cp) => Item::Checkpoint(seq, Box::new(cp)),
            })
            .collect())
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
        let (mask, fill, remask) = (self.request.clone(), self.fill, self.remask.clone());
        let pages = (range.start..range.end)
            .step_by(PAGE as usize)
            .map(move |start| start..(start + PAGE).min(range.end));
        futures::stream::iter(pages)
            .map(move |page| {
                let (client, mask, remask) = (client.clone(), mask.clone(), remask.clone());
                async move {
                    let (got, past_tip) =
                        Self::page(client, mask, page.clone(), fill, &remask).await?;
                    Self::items(page, got, past_tip)
                }
            })
            // Pages in order, `concurrency` ahead; an error ends the stream.
            .buffered(self.concurrency)
            .flat_map(|page| match page {
                Ok(items) => futures::stream::iter(items.into_iter().map(Ok).collect::<Vec<_>>())
                    .left_stream(),
                Err(e) => futures::stream::iter(vec![Err(e)]).right_stream(),
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
    fn pages_become_items_in_order_with_missing() {
        // Placed by the sequence number recorded before masking.
        let cp = |seq| (Some(seq), Checkpoint::default());
        let show = |items: Vec<Item>| -> Vec<String> {
            items
                .iter()
                .map(|i| match i {
                    Item::Checkpoint(s, _) => format!("{s}"),
                    Item::Missing(s) => format!("missing {s}"),
                    Item::NotYet(s) => format!("not yet {s}"),
                })
                .collect()
        };
        let items = Archive::items(10..14, vec![cp(13), cp(10), cp(12)], false).unwrap();
        assert_eq!(show(items), ["10", "missing 11", "12", "13"]);
        // Past the archive's tip, what follows the last one held isn't missing: it isn't there yet.
        let items = Archive::items(10..14, vec![cp(10), cp(12)], true).unwrap();
        assert_eq!(show(items), ["10", "missing 11", "12", "not yet 13"]);
        // Outside the page, or twice: the archive misbehaved.
        assert!(Archive::items(10..14, vec![cp(14)], false).is_err());
        assert!(Archive::items(10..14, vec![cp(10), cp(10)], false).is_err());
        assert!(Archive::items(10..14, vec![(None, Checkpoint::default())], false).is_err());
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
