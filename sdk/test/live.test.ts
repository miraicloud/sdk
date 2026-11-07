/**
 * Against production (Mainnet). Opt-in:
 *
 *   MIRAICLOUD_LIVE=1 MIRAICLOUD_API_KEY=mc1_… bun test test/live.test.ts --timeout 900000
 *
 * (or `npm run test:live`). The key needs `stream:read`;
 * `MIRAICLOUD_ENDPOINT` overrides the endpoint, `MIRAICLOUD_LIVE_BACK` how
 * far behind the tip the backfill test starts (default 5000). `archive`
 * needs no key: it compares the archive with a fullnode.
 */
import { describe, expect, test } from "bun:test";
import { GrpcTypes, SuiGrpcClient } from "@mysten/sui/grpc";
import { Effect, Redacted, Stream } from "effect";
import { Auth } from "../src/auth.js";
import { type CheckpointItem, layerFetch, MiraiCloudStream } from "../src/streams.js";
import { archiveHistory, CANONICAL, plan, prune } from "../src/streams/archive.js";

const live = process.env.MIRAICLOUD_LIVE === "1";
const key = process.env.MIRAICLOUD_API_KEY ?? "";
const endpoint = process.env.MIRAICLOUD_ENDPOINT;
const mask = ["sequence_number", "summary.timestamp"];

/** Read until an item less than `fresh` seconds old (after `min` items), or `limit` items. */
const run = async (opts: { from?: bigint; limit: number; fresh?: number; min?: number }) => {
  const items: CheckpointItem[] = [];
  const stream = Stream.unwrap(Effect.map(MiraiCloudStream, (s) => s.checkpoints({ readMask: mask, from: opts.from }))).pipe(
    Stream.provide(layerFetch({ endpoint, auth: Auth.Key({ token: Redacted.make(key) }) })),
  );
  for await (const item of Stream.toAsyncIterable(stream)) {
    items.push(item);
    if (items.length >= opts.limit) break;
    if (item._tag === "Checkpoint" && opts.fresh !== undefined && items.length >= (opts.min ?? 2)) {
      const age = Date.now() / 1000 - Number(item.checkpoint.summary!.timestamp!.seconds);
      if (age < opts.fresh) break;
    }
  }
  return items;
};

const contiguous = (items: CheckpointItem[]) => {
  let prev: bigint | undefined;
  for (const i of items) {
    const [a, b] = i._tag === "Gap" ? [i.from, i.to] : [i.seq, i.seq];
    if (prev !== undefined && a !== prev + 1n) return false;
    prev = b;
  }
  return true;
};

const tip = async () => {
  const [first] = await run({ limit: 1 });
  return (first as { seq: bigint }).seq;
};

describe.skipIf(!live || key === "")("live", () => {
  test("tip: in order", async () => {
    const items = await run({ limit: 40 });
    expect(items.length).toBe(40);
    expect(items.every((i) => i._tag === "Checkpoint")).toBe(true);
    expect(contiguous(items)).toBe(true);
  }, 60_000);

  test("resume older than the stream holds: from the archive, contiguous into live", async () => {
    const from = (await tip()) - BigInt(process.env.MIRAICLOUD_LIVE_BACK ?? 5000);
    const items = await run({ from, limit: 100_000, fresh: 5, min: 10 });
    const first = items[0]!;
    expect(first._tag === "Gap" ? first.from : first.seq).toBe(from);
    expect(contiguous(items)).toBe(true);
    expect(items.every((i) => i._tag === "Checkpoint")).toBe(true);
  }, 600_000);
});

describe.skipIf(!live)("archive", () => {
  /**
   * Archive backfill under masks equals the stream's checkpoint: fullnode
   * `GetCheckpoint` (canonical mask, object types filled by the fullnode),
   * restricted to the mask; apart from the aggregated signature and
   * sponsored-transaction signature order.
   */
  test("matches fullnode under masks", async () => {
    const fullnode = new SuiGrpcClient({ network: "mainnet", baseUrl: "https://fullnode.mainnet.sui.io" });
    const { response: info } = await fullnode.ledgerService.getServiceInfo({});
    const start = info.checkpointHeight! - 3000n;
    const normalize = (cp: GrpcTypes.Checkpoint) => {
      const c = GrpcTypes.Checkpoint.clone(cp);
      c.signature = undefined;
      for (const tx of c.transactions) {
        tx.signatures.sort((a, b) => Buffer.compare(GrpcTypes.UserSignature.toBinary(a), GrpcTypes.UserSignature.toBinary(b)));
      }
      return GrpcTypes.Checkpoint.toJson(c);
    };
    for (const mask of [
      undefined,
      ["sequence_number", "transactions.effects.changed_objects", "transactions.events"],
      ["transactions.effects.status", "objects.objects.object_type"],
      ["transactions.effects", "summary"],
    ]) {
      const history = archiveHistory({ readMask: mask, concurrency: 8 });
      let n = 0;
      for await (const item of Stream.toAsyncIterable(history.checkpoints(start, start + 5n))) {
        if (item.type !== "checkpoint") throw new Error(`missing ${item.seq}`);
        const { response } = await fullnode.ledgerService.getCheckpoint({
          checkpointId: { oneofKind: "sequenceNumber", sequenceNumber: item.seq },
          readMask: { paths: [...CANONICAL] },
        });
        const expected = prune(response.checkpoint!, plan(mask).prune);
        expect(normalize(item.checkpoint)).toEqual(normalize(expected));
        n++;
      }
      expect(n).toBe(5);
      history.close?.();
    }
  }, 120_000);
});
