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

/// `QueryEndReason::ItemLimit`: the server stopped at its own item limit; resume after the cursor.
const ITEM_LIMIT: i32 = 1;

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

    /// The checkpoints the archive has in `range`, ascending: one
    /// `ListCheckpoints` call, continued from its cursor if the server stops
    /// at an item limit of its own before the end.
    async fn list(
        mut client: sui_rpc::Client,
        mask: FieldMask,
        range: Range<u64>,
    ) -> Result<Vec<Checkpoint>, tonic::Status> {
        let mut out = Vec::new();
        let mut after = None;
        loop {
            let mut request = ListCheckpointsRequest::default();
            request.read_mask = Some(mask.clone());
            request.start_checkpoint = Some(range.start);
            request.end_checkpoint = Some(range.end);
            let mut options = QueryOptions::default();
            options.limit = Some((range.end - range.start) as u32);
            options.after = after.take();
            request.options = Some(options);
            let mut request = tonic::Request::new(request);
            // Epoch-change checkpoints are 10-20 MB and slow to serve.
            request.set_timeout(Duration::from_secs(120));
            // `ledger_client()` asks for zstd responses (~6x smaller).
            let mut frames = client
                .ledger_client()
                .list_checkpoints(request)
                .await?
                .into_inner();
            let (mut reason, mut cursor) = (None, None);
            while let Some(frame) = frames.message().await? {
                if let Some(cp) = frame.checkpoint {
                    out.push(cp);
                }
                if let Some(c) = frame.watermark.and_then(|w| w.cursor) {
                    cursor = Some(c);
                }
                if let Some(end) = frame.end {
                    reason = end.reason;
                }
            }
            let last = out.last().and_then(|cp: &Checkpoint| cp.sequence_number);
            if reason != Some(ITEM_LIMIT)
                || cursor.is_none()
                || last.is_some_and(|l| l + 1 >= range.end)
            {
                return Ok(out);
            }
            after = cursor;
        }
    }

    /// One page: failures retried (6 attempts in all); a short page asked
    /// again once before its absent checkpoints count as missing (the
    /// archive can lag the tip).
    async fn page(
        client: sui_rpc::Client,
        mask: FieldMask,
        range: Range<u64>,
    ) -> Result<Vec<Checkpoint>, String> {
        let mut backoff = Duration::from_millis(250);
        let (mut errors, mut short) = (0u32, false);
        loop {
            match Self::list(client.clone(), mask.clone(), range.clone()).await {
                Ok(got) if got.len() as u64 == range.end - range.start || short => return Ok(got),
                Ok(_) => short = true,
                Err(s) => {
                    errors += 1;
                    let e = format!(
                        "archive ListCheckpoints {}..{}: {:?}: {}",
                        range.start,
                        range.end - 1,
                        s.code(),
                        s.message()
                    );
                    if errors >= 6 {
                        return Err(e);
                    }
                    tracing::debug!("{e}; retrying");
                }
            }
            tokio::time::sleep(if short {
                Duration::from_millis(250)
            } else {
                backoff
            })
            .await;
            backoff = (backoff * 2).min(Duration::from_secs(4));
        }
    }

    /// `range` as items: each checkpoint (filled and masked) or `Missing`.
    fn items(
        range: Range<u64>,
        got: Vec<Checkpoint>,
        fill: bool,
        remask: &FieldMaskTree,
    ) -> Result<Vec<Item>, String> {
        let mut by_seq = HashMap::with_capacity(got.len());
        for cp in got {
            match cp.sequence_number {
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
        Ok(range
            .map(|seq| match by_seq.remove(&seq) {
                None => Item::Missing(seq),
                Some(mut cp) => {
                    if fill {
                        fill_object_types(&mut cp);
                    }
                    Item::Checkpoint(seq, Box::new(Checkpoint::merge_from(&cp, remask)))
                }
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
                    let got = Self::page(client, mask, page.clone()).await?;
                    Self::items(page, got, fill, &remask)
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
                request.set_timeout(Duration::from_secs(30));
                match client.ledger_client().get_service_info(request).await {
                    Ok(r) => return r.into_inner().checkpoint_height,
                    Err(s) => tracing::debug!("archive tip: {:?}: {}", s.code(), s.message()),
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
        let cp = |seq| {
            let mut c = Checkpoint::default();
            c.sequence_number = Some(seq);
            c
        };
        let all = FieldMaskTree::from_field_mask(&FieldMask::from_paths(["sequence_number"]));
        let items = Archive::items(10..14, vec![cp(13), cp(10), cp(12)], false, &all).unwrap();
        let got: Vec<_> = items
            .iter()
            .map(|i| match i {
                Item::Checkpoint(s, _) => format!("{s}"),
                Item::Missing(s) => format!("missing {s}"),
            })
            .collect();
        assert_eq!(got, ["10", "missing 11", "12", "13"]);
        // Outside the page, or twice: the archive misbehaved.
        assert!(Archive::items(10..14, vec![cp(14)], false, &all).is_err());
        assert!(Archive::items(10..14, vec![cp(10), cp(10)], false, &all).is_err());
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
