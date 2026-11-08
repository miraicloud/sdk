/**
 * The checkpoint stream: frames, read masks, archive backfill, the driver
 * (order, dedup, gaps, backoff, 410), the HTTP source (zstd, refusals,
 * stalls), and the whole client against a local server.
 */

import { existsSync } from "node:fs";
import { afterAll, describe, expect, test } from "bun:test";
import { Ed25519Keypair } from "@mysten/sui/keypairs/ed25519";
import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { GrpcTypes } from "@mysten/sui/grpc";
import { Effect, Exit, Layer, Option, Redacted, Result, Stream } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { Auth } from "../src/auth.js";
import { Invalid, NotFound, QuotaExceeded, Unauthorized } from "@miraicloud/core/errors";
import { HEADERS } from "@miraicloud/core/signed";
import { fromSuiSigner } from "@miraicloud/core/signer";
import { type CheckpointItem, type CheckpointStreamOptions, Diagnostic, layerFetch, MiraiCloudStream, type RawCheckpointItem } from "../src/streams.js";
import { ArchiveError, archiveHistory, BCS_PATHS, CANONICAL, DEFAULT, effective, fillObjectTypes, type HistoryItem, type HistorySource, plan, prune } from "../src/streams/archive.js";
import { decoded, fetchDecodesZstd, httpSource, keepFields, masked, NOT_FOUND_ATTEMPTS, nativeZstdDecoder, raw, type SourceOptions } from "../src/streams/checkpoints.js";
import { type Decoder, drive, type LiveSource, Retry, TooOld } from "../src/streams/driver.js";
import { FrameParser, splitResponse } from "../src/streams/frame.js";
import { EXCLUDED_PATHS, resolveMask } from "../src/streams/mask.js";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
type Item = CheckpointItem | RawCheckpointItem;
const label = (i: Item) => (i._tag === "Gap" ? `gap ${i.from}-${i.to}` : Number(i.seq));

/** Every stream scenario runs in both modes. */
const modes = ["typed", "raw"] as const;
type Mode = (typeof modes)[number];

/** The checkpoint the test sources send for `seq`. */
const checkpoint = (seq: number | bigint): GrpcTypes.Checkpoint => GrpcTypes.Checkpoint.create({ sequenceNumber: BigInt(seq), digest: `d${seq}` });
const encoded = (seq: number | bigint) => GrpcTypes.Checkpoint.toBinary(checkpoint(seq));

/** `label`, after checking a checkpoint has this mode's shape and content (raw: exactly the bytes sent). */
const checked = (mode: Mode, i: Item) => {
  if (i._tag === "Checkpoint") {
    if (mode === "raw") expect("data" in i && [...i.data]).toEqual([...encoded(i.seq)]);
    else expect("checkpoint" in i && i.checkpoint.digest).toBe(`d${i.seq}`);
  }
  return label(i);
};

/**
 * The common `common/src/lib.rs` of the stream's server crates, when the
 * server's source is alongside (the platform repository); elsewhere the
 * tests that compare against it are skipped.
 */
const COMMON_LIB = new URL("../../suistream/crates/common/src/lib.rs", import.meta.url);
const hasServer = existsSync(COMMON_LIB);
const commonLib = () => Bun.file(COMMON_LIB).text();

// ---- frames ----

const frame = (kind: number, body: Uint8Array) => {
  const out = new Uint8Array(5 + body.length);
  out[0] = kind;
  new DataView(out.buffer).setUint32(1, body.length);
  out.set(body, 5);
  return out;
};
const response = (seq: number | bigint, cp: Uint8Array = encoded(seq)) =>
  GrpcTypes.SubscribeCheckpointsResponse.toBinary({ cursor: BigInt(seq), checkpoint: GrpcTypes.Checkpoint.fromBinary(cp) });
const cpFrame = (seq: number | bigint) => frame(0, response(seq));
const gapFrame = (from: number, to: number) => {
  const body = new Uint8Array(16);
  const v = new DataView(body.buffer);
  v.setBigUint64(0, BigInt(from));
  v.setBigUint64(8, BigInt(to));
  return frame(1, body);
};
const keepalive = () => frame(2, new Uint8Array(0));
const concat = (parts: Uint8Array[]) => {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
};
const zstd = (bytes: Uint8Array) => new Uint8Array(Bun.zstdCompressSync(bytes));

describe("mask", () => {
  test("resolve: whole checkpoint, narrowed paths, locally validated", () => {
    const ok = (paths?: string[]) => Result.getOrThrow(resolveMask(paths));
    const error = (paths: string[]) => {
      const r = resolveMask(paths);
      return Result.isFailure(r) ? r.failure.message : "valid";
    };
    expect(ok()).toBeUndefined();
    expect(ok([" ", ""])).toBeUndefined();
    expect(ok(["*", " *"])).toBeUndefined();
    expect(ok([...CANONICAL])).toBeUndefined();
    expect(ok(["*", "summary.bcs"])).toBeUndefined();
    // The decoded fields alone leave out the BCS encodings: a narrowing mask.
    const decodedOnly = ok([...DEFAULT])!;
    expect(decodedOnly.some((p) => p.endsWith(".bcs"))).toBe(false);
    expect(ok([" digest ", "digest", "transactions.digest"])).toEqual(["digest", "transactions.digest"]);
    // A parent path stands for the whole field, BCS included.
    expect(ok(["summary"])).toEqual(["summary"]);
    // BCS paths are served like any other field.
    expect(ok(["sequence_number", "transactions.effects.bcs"])).toEqual(["sequence_number", "transactions.effects.bcs"]);
    expect(ok(["summary.bcs.value"])).toEqual(["summary.bcs.value"]);
    for (const b of BCS_PATHS) expect(ok([b])).toEqual([b]);
    expect(error(["transactions.efects"])).toMatch(/invalid/);
    expect(error(["objects.objects.json"])).toMatch(/excluded/);
  });

  test("masked decoding skips the fields the mask doesn't reach, with the same result as decoding everything", () => {
    const cp = GrpcTypes.Checkpoint.create({
      sequenceNumber: 7n,
      digest: "d7",
      summary: { sequenceNumber: 7n, timestamp: { seconds: 1n, nanos: 2 }, epoch: 3n },
      transactions: [{ digest: "t".repeat(5000) }, { digest: "u" }],
      objects: { objects: [{ objectId: "0x1", version: 4n }] },
    });
    const bytes = GrpcTypes.Checkpoint.toBinary(cp);
    for (const paths of [["sequence_number", "summary.timestamp"], ["summary"], ["transactions.digest"], ["digest", "objects.objects.version"]]) {
      const item = masked(decoded, paths).frame(7n, bytes);
      expect(item._tag === "Checkpoint" && item.checkpoint).toEqual(prune(cp, paths));
    }
    // Only fields 1 (sequence_number) and 3 (summary) are kept, as bytes, in order.
    expect(GrpcTypes.Checkpoint.fromBinary(keepFields(bytes, new Set([1, 3])))).toEqual(GrpcTypes.Checkpoint.create({ sequenceNumber: 7n, summary: cp.summary }));
    // Truncated input is refused (a broken connection), not decoded short.
    expect(() => masked(decoded, ["summary"]).frame(7n, bytes.subarray(0, bytes.length - 3))).toThrow(/undecodable/);
  });

  test.skipIf(!hasServer)("excluded paths match the server", async () => {
    const src = await commonLib();
    for (const p of EXCLUDED_PATHS) expect(src).toContain(`"${p}"`);
  });
});

describe("frame parser", () => {
  const stream = concat([cpFrame(5), keepalive(), gapFrame(6, 9), frame(9, new Uint8Array([1, 2])), frame(0, GrpcTypes.SubscribeCheckpointsResponse.toBinary({ cursor: 10n }))]);
  const parse = (chunks: Uint8Array[]): unknown[] => {
    const p = new FrameParser();
    // Plain objects (own fields only) for comparison.
    return chunks.flatMap((c) => p.push(c)).map((f) => (f._tag === "Checkpoint" ? { ...f, checkpoint: [...f.checkpoint] } : { ...f }));
  };
  const expected = [
    { _tag: "Checkpoint", seq: 5n, checkpoint: [...encoded(5)] },
    { _tag: "Other" },
    { _tag: "Gap", from: 6n, to: 9n },
    { _tag: "Other" },
    { _tag: "Checkpoint", seq: 10n, checkpoint: [] },
  ];

  test("every split point and byte by byte", () => {
    expect(parse([stream])).toEqual(expected);
    for (let i = 0; i <= stream.length; i++) {
      expect(parse([stream.subarray(0, i), new Uint8Array(0), stream.subarray(i)])).toEqual(expected);
    }
    expect(parse([...stream].map((b) => new Uint8Array([b])))).toEqual(expected);
  });

  test("a frame within one chunk is a view of it; a ~1 MB frame in small chunks", () => {
    const one = cpFrame(5);
    const [f] = new FrameParser().push(one);
    expect(f?._tag === "Checkpoint" && f.checkpoint.buffer).toBe(one.buffer);
    const big = GrpcTypes.Checkpoint.toBinary(GrpcTypes.Checkpoint.create({ sequenceNumber: 1n, digest: "x".repeat(1_000_000) }));
    const body = frame(0, response(1, big));
    const p = new FrameParser();
    const out = [];
    for (let i = 0; i < body.length; i += 4096) out.push(...p.push(body.subarray(i, i + 4096)));
    expect(out.length).toBe(1);
    expect(out[0]!._tag === "Checkpoint" && out[0]!.checkpoint.length).toBe(big.length);
  });

  test("corrupt frames throw", () => {
    expect(() => new FrameParser().push(new Uint8Array([0, 0xff, 0xff, 0xff, 0xff]))).toThrow();
    expect(() => new FrameParser().push(frame(1, new Uint8Array(3)))).toThrow();
    expect(() => new FrameParser().push(frame(0, new Uint8Array(0)))).toThrow("cursor");
    expect(() => splitResponse(new Uint8Array([0x12, 0x05, 1]))).toThrow("truncated");
  });

  test("decoders: typed decodes and validates; raw passes bytes through; malformed is retried", () => {
    const ev = decoded.frame(7n, encoded(7));
    expect(ev.checkpoint.digest).toBe("d7");
    // The mask may omit the sequence number.
    expect(decoded.frame(8n, new Uint8Array(0)).checkpoint.sequenceNumber).toBeUndefined();
    for (const [seq, data] of [
      [8n, encoded(9)],
      [8n, new Uint8Array([0x0a, 0x05, 1])],
    ] as const) {
      expect(() => decoded.frame(seq, data)).toThrow(Retry);
    }
    const bytes = new Uint8Array([0x0a, 0x05, 1]);
    const r = raw.frame(7n, bytes);
    expect([r._tag, r.seq, r.data]).toEqual(["Checkpoint", 7n, bytes]);
    // From the archive: raw re-encodes.
    expect([...raw.message(7n, checkpoint(7)).data]).toEqual([...encoded(7)]);
  });

  test("Bun decodes zstd in fetch and has node:zlib zstd", () => {
    expect(fetchDecodesZstd()).toBe(true);
    expect(nativeZstdDecoder()).toBeFunction();
  });
});

// ---- archive ----

/**
 * A `SuiGrpcClient` whose ledger service answers `ListCheckpoints` (and
 * `GetServiceInfo`) like the archive. Pages finish in scrambled order.
 * `fail(start, ask)`: that ask of a page fails; `missing(seq)`: absent;
 * `cap`: a server item limit (ITEM_LIMIT, resume after the cursor);
 * `shift`: answers with the wrong checkpoints. `log`: each call's range.
 */
const fakeArchive = (
  o: { fail?: (start: bigint, ask: number) => boolean; missing?: (seq: bigint) => boolean; cap?: number; shift?: bigint; tip?: bigint },
  log: string[],
) => {
  const asks = new Map<bigint, number>();
  const cursor = (seq: bigint) => new TextEncoder().encode(String(seq));
  return {
    ledgerService: {
      getServiceInfo: () => ({ response: Promise.resolve({ checkpointHeight: o.tip ?? 100n }) }),
      listCheckpoints: (input: { startCheckpoint: bigint; endCheckpoint: bigint; readMask?: { paths: string[] }; options?: { limit?: number; after?: Uint8Array } }) => {
        // Like the archive: no sequence number unless the mask asks for it.
        const withSeq = !input.readMask || input.readMask.paths.includes("sequence_number");
        const { startCheckpoint: start, endCheckpoint: end } = input;
        const after = input.options?.after ? BigInt(new TextDecoder().decode(input.options.after)) : undefined;
        if (after === undefined) asks.set(start, (asks.get(start) ?? 0) + 1);
        const ask = asks.get(start)!;
        log.push(`${start}-${end}${after === undefined ? "" : ` after ${after}`}`);
        async function* responses() {
          await sleep(Number(7n - ((start / 50n) % 7n))); // later pages finish first
          if (o.fail?.(start, ask)) throw Object.assign(new Error("unavailable"), { code: "UNAVAILABLE" });
          let n = 0;
          for (let seq = after === undefined ? start : after + 1n; seq < end; seq++) {
            if (o.missing?.(seq)) continue;
            const s = seq + (o.shift ?? 0n);
            n++;
            const checkpoint = GrpcTypes.Checkpoint.create({ digest: `d${s}`, objects: { objects: [{ objectId: "0x1", version: 1n }] } });
            if (withSeq) checkpoint.sequenceNumber = s;
            const frame = { checkpoint, watermark: { cursor: cursor(seq) } };
            if (o.cap !== undefined && n === o.cap && seq < end - 1n) {
              yield { ...frame, end: { reason: 1 } };
              return;
            }
            yield frame;
          }
          yield { watermark: { cursor: cursor(end - 1n) }, end: { reason: 3 } };
        }
        return { responses: responses() };
      },
    },
  } as unknown as SuiGrpcClient;
};

describe("archive", () => {
  test.skipIf(!hasServer)("lists match the server's (crates/common)", async () => {
    const common = await commonLib();
    const list = (name: string) => {
      const body = common.slice(common.indexOf(`pub const ${name}: &[&str] = &[`));
      return [...body.slice(body.indexOf("[") + 4, body.indexOf("];")).matchAll(/"([^"]+)"/g)].map((m) => m[1]);
    };
    expect(list("CANONICAL_READ_MASK")).toEqual([...CANONICAL]);
    expect(list("BCS_PATHS")).toEqual([...BCS_PATHS]);
    expect(list("DEFAULT_READ_MASK")).toEqual([...DEFAULT]);
  });

  test("plan: what the stream sends for the mask (BCS included), helpers for object types", () => {
    // No mask: the full canonical checkpoint, like the live stream.
    const def = plan(undefined);
    expect(def).toEqual({ request: [...CANONICAL], fill: true, prune: [...CANONICAL] });
    expect(def.request).toEqual(expect.arrayContaining(["summary", "contents", "transactions.effects", "objects.objects.bcs"]));
    expect(plan(["*"])).toEqual(def);
    // Parents expand to what the stream sends: BCS, never `events.json`.
    const events = plan(["transactions.events"]);
    expect(events.request).toContain("transactions.events.events.event_type");
    expect(events.request).toContain("transactions.events.bcs");
    expect(events.request.some((r) => r.endsWith("json"))).toBe(false);
    expect(events.fill).toBe(false);
    expect(plan(["transactions.events", "transactions.events.bcs"]).request).toEqual(events.request);
    // BCS paths are requested when named.
    expect(effective(["summary.bcs"])).toEqual(["summary.bcs"]);
    expect(plan(["sequence_number", "transactions.effects.bcs"])).toEqual({
      request: ["sequence_number", "transactions.effects.bcs"],
      fill: false,
      prune: ["sequence_number", "transactions.effects.bcs"],
    });
    // The decoded fields alone: no BCS requested.
    expect(plan([...DEFAULT]).request.some((r) => r.endsWith(".bcs"))).toBe(false);
    expect(effective(["transactions.objects"])).toEqual(["transactions.objects"]);
    expect(plan(["transactions.effects.status", "digest"]).request).toEqual(["transactions.effects.status", "digest"]);
    const changed = plan(["transactions.effects.changed_objects", "sequence_number"]);
    expect(changed.fill).toBe(true);
    expect(changed.prune).toEqual(["transactions.effects.changed_objects", "sequence_number"]);
    expect(changed.request).toContain("objects.objects.object_type");
  });

  test("object types come from the checkpoint's objects", () => {
    const cp = GrpcTypes.Checkpoint.create({
      transactions: [
        {
          effects: {
            changedObjects: [
              { objectId: "0xa", inputVersion: 1n, outputVersion: 2n },
              { objectId: "0xb", outputVersion: 2n },
              { objectId: "0xc", inputVersion: 1n, outputVersion: 2n },
            ],
            unchangedConsensusObjects: [{ objectId: "0x6", version: 7n }],
          },
        },
      ],
      objects: {
        objects: [
          { objectId: "0xa", version: 1n, objectType: "0x2::coin::Coin<0x2::sui::SUI>" },
          { objectId: "0xa", version: 2n, objectType: "wrong version" },
          { objectId: "0xb", version: 2n, objectType: "0x3::m::B" },
          { objectId: "0x6", version: 7n, objectType: "0x2::clock::Clock" },
        ],
      },
    });
    fillObjectTypes(cp);
    const effects = cp.transactions[0]!.effects!;
    expect(effects.changedObjects.map((c) => c.objectType)).toEqual(["0x2::coin::Coin<0x2::sui::SUI>", "0x3::m::B", undefined]);
    expect(effects.unchangedConsensusObjects[0]!.objectType).toBe("0x2::clock::Clock");
  });

  test("prune keeps exactly the mask: leaves, nested and repeated messages, oneofs", () => {
    const cp = GrpcTypes.Checkpoint.create({
      sequenceNumber: 1n,
      digest: "d",
      summary: { epoch: 3n, digest: "s" },
      transactions: [
        {
          digest: "t",
          effects: { status: { success: true }, changedObjects: [{ objectId: "0xa" }] },
          signatures: [{ bcs: { value: new Uint8Array([1]) }, signature: { oneofKind: "simple", simple: { signature: new Uint8Array([2]) } } }],
        },
      ],
      objects: { objects: [{ objectId: "0xa", version: 2n, objectType: "T" }] },
    });
    const out = prune(cp, ["sequence_number", "summary.epoch", "transactions.effects.status", "transactions.signatures.simple", "objects.objects.object_type"]);
    expect(out.sequenceNumber).toBe(1n);
    expect(out.digest).toBeUndefined();
    expect(out.summary).toEqual(GrpcTypes.CheckpointSummary.create({ epoch: 3n }));
    expect(out.transactions[0]!.digest).toBeUndefined();
    expect(out.transactions[0]!.effects!.status).toEqual({ success: true });
    expect(out.transactions[0]!.effects!.changedObjects).toEqual([]);
    expect(out.transactions[0]!.signatures[0]!.bcs).toBeUndefined();
    expect(out.transactions[0]!.signatures[0]!.signature).toEqual({ oneofKind: "simple", simple: { signature: new Uint8Array([2]) } });
    expect(out.objects!.objects).toEqual([GrpcTypes.Object.create({ objectType: "T" })]);
    // Round-trips like a decoded message.
    expect(GrpcTypes.Checkpoint.fromBinary(GrpcTypes.Checkpoint.toBinary(out))).toEqual(out);
  });

  const collect = async (h: HistorySource, from: bigint, to: bigint) => {
    const out: string[] = [];
    for await (const i of Stream.toAsyncIterable(h.checkpoints(from, to))) {
      out.push(i.type === "missing" ? `missing ${i.seq}` : `${i.seq} ${i.checkpoint.digest} ${i.checkpoint.objects?.objects.length ?? "-"}`);
    }
    return out;
  };

  test("history: pages in order with concurrency; a short page re-asked, then missing; errors retried; pruned", async () => {
    const log: string[] = [];
    // Page 60 fails twice; 113 is absent on every ask.
    const client = fakeArchive({ fail: (start, n) => start === 60n && n <= 2, missing: (seq) => seq === 113n }, log);
    const h = archiveHistory({ client, readMask: ["sequence_number", "digest"], concurrency: 3 });
    const out = await collect(h, 10n, 117n);
    expect(out.length).toBe(107);
    expect(out.slice(0, 2)).toEqual(["10 d10 -", "11 d11 -"]);
    expect(out.slice(102)).toEqual(["112 d112 -", "missing 113", "114 d114 -", "115 d115 -", "116 d116 -"]);
    expect(out.every((line, i) => line.startsWith(`${10 + i} `) || line === `missing ${10 + i}`)).toBe(true);
    // Pages of 50 with explicit bounds; the failing page asked 3 times, the short one twice.
    expect(log.filter((l) => l === "10-60").length).toBe(1);
    expect(log.filter((l) => l === "60-110").length).toBe(3);
    expect(log.filter((l) => l === "110-117").length).toBe(2);
    // Full mask: no pruning.
    const full = archiveHistory({ client: fakeArchive({}, []), readMask: undefined });
    expect(await collect(full, 1n, 2n)).toEqual(["1 d1 1"]);
  });

  test("history: a mask without sequence_number still places each checkpoint (asked for, then pruned)", async () => {
    const h = archiveHistory({ client: fakeArchive({}, []), readMask: ["digest"] });
    const items: HistoryItem[] = [];
    for await (const i of Stream.toAsyncIterable(h.checkpoints(5n, 8n))) items.push(i);
    expect(items.map((i) => (i.type === "checkpoint" ? [i.seq, i.checkpoint.digest, i.checkpoint.sequenceNumber] : i.type))).toEqual([
      [5n, "d5", undefined],
      [6n, "d6", undefined],
      [7n, "d7", undefined],
    ]);
  });

  test("history: a server item limit below the page is continued from the cursor (no false gaps)", async () => {
    const log: string[] = [];
    const h = archiveHistory({ client: fakeArchive({ cap: 7 }, log), readMask: ["sequence_number", "digest"] });
    const out = await collect(h, 0n, 20n);
    expect(out).toEqual(Array.from({ length: 20 }, (_, i) => `${i} d${i} -`));
    expect(log).toEqual(["0-20", "0-20 after 6", "0-20 after 13"]);
  });

  test("history: a checkpoint outside the page is an error; the tip is the archive's height", async () => {
    const h = archiveHistory({ client: fakeArchive({ shift: 1n, tip: 1234n }, []), readMask: ["sequence_number"] });
    const failed = await Effect.runPromise(Stream.runCollect(h.checkpoints(0n, 3n)).pipe(Effect.flip));
    expect(failed.message).toContain("archive returned checkpoint");
    expect(await Effect.runPromise(h.tip!)).toBe(1234n);
  }, 20_000);
});

// ---- driver over a scripted source ----

type AnyCheckpoint = Extract<Item, { _tag: "Checkpoint" }>;
type AnyGap = Extract<Item, { _tag: "Gap" }>;
/** Each connection replays one script entry: items, then end / fail; then hang. */
type Step = { items: (AnyCheckpoint | AnyGap)[]; error?: Retry | TooOld | Invalid };
const decoderOf = (mode: Mode) => (mode === "raw" ? raw : decoded) as unknown as Decoder<AnyCheckpoint, AnyGap>;
const cpItem = (mode: Mode, seq: number): AnyCheckpoint => decoderOf(mode).message(BigInt(seq), checkpoint(seq));
const gapItem = (from: number, to: number): AnyGap => decoded.gap(BigInt(from), BigInt(to));
const tooOld = (oldest: number) => new TooOld({ message: "HTTP 410", oldest: BigInt(oldest) });
const scripted = (steps: Step[], connects: (string | undefined)[]): LiveSource<AnyCheckpoint, AnyGap, Invalid> => ({
  connect: (after) =>
    Stream.suspend(() => {
      connects.push(after?.toString());
      const step = steps.shift();
      // Out of script: the connection stays open with nothing to say.
      if (!step) return Stream.never;
      const items = Stream.fromIterable(step.items);
      return step.error ? Stream.concat(items, Stream.fail(step.error)) : items;
    }),
});
/** History serving `[from, to)` with `missing` absent, failing once at `failAt`; `tip`: its newest checkpoint, if it says. */
const fakeHistory = (missing: number[], failAt: number | undefined, calls: string[], tip?: number): HistorySource => ({
  ...(tip === undefined ? {} : { tip: Effect.succeed(BigInt(tip)) }),
  checkpoints: (from, to) =>
    Stream.suspend(() => {
      calls.push(`${from}-${to}`);
      const seqs: bigint[] = [];
      for (let s = from; s < to; s++) seqs.push(s);
      return Stream.fromIterable(seqs).pipe(
        Stream.mapEffect((s) => {
          if (failAt !== undefined && s === BigInt(failAt)) {
            failAt = undefined;
            return Effect.fail(new ArchiveError({ message: "archive down" }));
          }
          return Effect.succeed((missing.includes(Number(s)) ? { type: "missing", seq: s } : { type: "checkpoint", seq: s, checkpoint: checkpoint(s) }) as HistoryItem);
        }),
      );
    }),
});
const run = async (mode: Mode, live: LiveSource<AnyCheckpoint, AnyGap, Invalid>, n: number, startAt?: number, onDiagnostic?: (d: Diagnostic) => void, history?: HistorySource) => {
  const out: (string | number)[] = [];
  const items = drive({
    live,
    decode: decoderOf(mode),
    history,
    startAt: startAt === undefined ? undefined : BigInt(startAt),
    onDiagnostic,
    backoffBaseMs: 5,
    backoffMaxMs: 30,
    random: () => 1,
  });
  for await (const item of Stream.toAsyncIterable(items)) {
    out.push(checked(mode, item));
    if (out.length >= n) break;
  }
  return out;
};

for (const mode of modes)
  describe(`driver (${mode})`, () => {
    const cp = (seq: number) => cpItem(mode, seq);

    test("tip start, in order; replays after reconnect are dropped", async () => {
      const connects: (string | undefined)[] = [];
      const src = scripted([{ items: [cp(10), cp(11)] }, { items: [cp(11), cp(12)], error: new Retry({ message: "reset" }) }, { items: [cp(12), cp(13)] }], connects);
      expect(await run(mode, src, 4)).toEqual([10, 11, 12, 13]);
      expect(connects).toEqual([undefined, "11", "12"]);
    });

    test("start positions: startAt n → after n-1; 0 → genesis (-1)", async () => {
      const connects: (string | undefined)[] = [];
      await run(mode, scripted([{ items: [cp(5)] }], connects), 1, 5);
      await run(mode, scripted([{ items: [cp(0)] }], connects), 1, 0);
      expect(connects).toEqual(["4", "-1"]);
    });

    test("gaps are surfaced once and move the cursor (live gap closes the stream)", async () => {
      const connects: (string | undefined)[] = [];
      const src = scripted(
        [{ items: [gapItem(100, 149), cp(150)] }, { items: [gapItem(120, 149), cp(150), cp(151), gapItem(152, 153)] }, { items: [gapItem(152, 153), cp(154)] }],
        connects,
      );
      expect(await run(mode, src, 5, 100)).toEqual(["gap 100-149", 150, 151, "gap 152-153", 154]);
      expect(connects).toEqual(["99", "150", "153"]);
    });

    test("a gap overlapping the cursor is trimmed", async () => {
      expect(await run(mode, scripted([{ items: [cp(10), gapItem(5, 12), cp(13)] }], []), 3)).toEqual([10, "gap 11-12", 13]);
    });

    test("a silent skip is never delivered: reconnect from the cursor", async () => {
      const connects: (string | undefined)[] = [];
      const diags: Diagnostic[] = [];
      const src = scripted([{ items: [cp(10), cp(12)] }, { items: [cp(11), cp(12)] }], connects);
      expect(await run(mode, src, 3, undefined, (d) => diags.push(d))).toEqual([10, 11, 12]);
      expect(connects).toEqual([undefined, "10"]);
      expect(diags.some((d) => d._tag === "Disconnect" && d.reason.includes("skips 11"))).toBe(true);
    });

    test("backoff doubles while failing, is capped, resets on progress, honours Retry-After", async () => {
      const diags: Diagnostic[] = [];
      const fail = () => ({ items: [], error: new Retry({ message: "down" }) });
      const busy = { items: [], error: new Retry({ message: "429", retryAfterMs: 50 }) };
      const src = scripted([fail(), fail(), fail(), fail(), { items: [cp(1)] }, busy, { items: [cp(2)] }], []);
      await run(mode, src, 2, undefined, (d) => diags.push(d));
      const waits = diags.flatMap((d) => (d._tag === "Disconnect" ? [d.retryInMs] : []));
      // The driver runs up to a small buffer ahead of the consumer, so it may
      // already have seen the next connection end: compare the first six.
      expect(waits.slice(0, 6)).toEqual([5, 10, 20, 30, 5, 50]);
    });

    test("backoff is jittered: between half and all of it, never below Retry-After", async () => {
      const diags: Diagnostic[] = [];
      const fail = () => ({ items: [], error: new Retry({ message: "down" }) });
      const busy = { items: [], error: new Retry({ message: "429", retryAfterMs: 50 }) };
      const src = scripted([fail(), fail(), busy, { items: [cp(1)] }], []);
      const items = drive({ live: src, decode: decoderOf(mode), startAt: undefined, onDiagnostic: (d) => diags.push(d), backoffBaseMs: 100, backoffMaxMs: 1_000, random: () => 0 });
      for await (const _ of Stream.toAsyncIterable(items)) break;
      const waits = diags.flatMap((d) => (d._tag === "Disconnect" ? [d.retryInMs] : []));
      expect(waits.slice(0, 3)).toEqual([50, 100, 200]);
    });

    test("diagnostics: connect after the cursor (none at the tip)", async () => {
      const diags: Diagnostic[] = [];
      await run(mode, scripted([{ items: [cp(10)] }, { items: [cp(11)] }], []), 2, undefined, (d) => diags.push(d));
      expect(diags.filter((d) => d._tag === "Connect").map((d) => (d._tag === "Connect" ? Option.getOrUndefined(d.after) : "?"))).toEqual([undefined, 10n]);
    });

    test("final refusals end the stream", async () => {
      const src = scripted([{ items: [cp(1)], error: new Invalid({ message: "bad from" }) }], []);
      const out: (string | number)[] = [];
      let error: unknown;
      try {
        for await (const item of Stream.toAsyncIterable(drive({ live: src, decode: decoderOf(mode), startAt: undefined, backoffBaseMs: 1, backoffMaxMs: 1 }))) {
          out.push(checked(mode, item));
        }
      } catch (e) {
        error = e;
      }
      expect(out).toEqual([1]);
      expect(error).toBeInstanceOf(Invalid);
    });

    test("410: backfilled from history (missing → gap), then the stream; the stream moved meanwhile", async () => {
      const connects: (string | undefined)[] = [];
      const calls: string[] = [];
      const diags: Diagnostic[] = [];
      const src = scripted([{ items: [], error: tooOld(6) }, { items: [], error: tooOld(8) }, { items: [cp(8), cp(9)] }], connects);
      const out = await run(mode, src, 7, 1, (d) => diags.push(d), fakeHistory([3, 4], undefined, calls));
      expect(out).toEqual([1, 2, "gap 3-4", 5, 6, 7, 8]);
      expect(connects).toEqual(["0", "5", "7"]);
      expect(calls).toEqual(["1-6", "6-8"]);
      expect(diags.filter((d) => d._tag === "Backfill").length).toBe(2);
    });

    test("410 with a history tip: backfilled up to the tip, then the stream after it (no chasing)", async () => {
      const connects: (string | undefined)[] = [];
      const calls: string[] = [];
      const diags: Diagnostic[] = [];
      const src = scripted([{ items: [], error: tooOld(6) }, { items: [cp(9), cp(10)] }], connects);
      const out = await run(mode, src, 10, 1, (d) => diags.push(d), fakeHistory([], undefined, calls, 8));
      expect(out).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
      expect(calls).toEqual(["1-9"]);
      expect(connects).toEqual(["0", "8"]);
      expect(diags.filter((d) => d._tag === "Backfill")).toEqual([Diagnostic.Backfill({ from: 1n, to: 8n })]);
    });

    test("410 with a history tip behind the stream: up to the stream's oldest", async () => {
      const calls: string[] = [];
      const src = scripted([{ items: [], error: tooOld(6) }, { items: [cp(6)] }], []);
      expect(await run(mode, src, 6, 1, undefined, fakeHistory([], undefined, calls, 3))).toEqual([1, 2, 3, 4, 5, 6]);
      expect(calls).toEqual(["1-6"]);
    });

    test("410: a history failure resumes where it stopped, after backoff", async () => {
      const connects: (string | undefined)[] = [];
      const calls: string[] = [];
      const src = scripted([{ items: [], error: tooOld(5) }, { items: [], error: tooOld(5) }, { items: [cp(5)] }], connects);
      expect(await run(mode, src, 5, 1, undefined, fakeHistory([], 3, calls))).toEqual([1, 2, 3, 4, 5]);
      expect(connects).toEqual(["0", "2", "4"]);
      expect(calls).toEqual(["1-5", "3-5"]);
    });

    test("410: checkpoints missing at the end of the backfill are history lagging, not a gap", async () => {
      // History's tip says 9, but it doesn't list 8 and 9 yet; the stream starts at 6: the stream serves them.
      const connects: (string | undefined)[] = [];
      const calls: string[] = [];
      const src = scripted([{ items: [], error: tooOld(6) }, { items: [cp(6), cp(7), cp(8), cp(9), cp(10)] }], connects);
      expect(await run(mode, src, 10, 1, undefined, fakeHistory([8, 9], undefined, calls, 9))).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
      expect(connects).toEqual(["0", "7"]);
      // Missing before the stream's oldest, at the end: asked again (after backoff), not skipped.
      const connects2: (string | undefined)[] = [];
      const calls2: string[] = [];
      const lagging = fakeHistory([4, 5], undefined, calls2);
      let asked = 0;
      const catchingUp: HistorySource = { checkpoints: (from, to) => (asked++ === 0 ? lagging.checkpoints(from, to) : fakeHistory([], undefined, calls2).checkpoints(from, to)) };
      const src2 = scripted([{ items: [], error: tooOld(6) }, { items: [], error: tooOld(6) }, { items: [cp(6)] }], connects2);
      expect(await run(mode, src2, 6, 1, undefined, catchingUp)).toEqual([1, 2, 3, 4, 5, 6]);
      expect(calls2).toEqual(["1-6", "4-6"]);
      expect(connects2).toEqual(["0", "3", "5"]);
    });

        test("410 without history: a gap up to the stream's oldest", async () => {
      const connects: (string | undefined)[] = [];
      expect(await run(mode, scripted([{ items: [], error: tooOld(6) }, { items: [cp(6), cp(7)] }], connects), 3, 1)).toEqual(["gap 1-5", 6, 7]);
      expect(connects).toEqual(["0", "5"]);
    });
  });

// ---- the whole client against a local server ----

describe("the checkpoint stream against a local server (Bun.serve)", () => {
  type Handler = (req: Request, n: number) => Response | Promise<Response>;
  let handler: Handler = () => new Response("unset", { status: 500 });
  /** Copies: Bun recycles `Request` objects after the handler returns. */
  let requests: { url: string; headers: Headers; aborted: boolean }[] = [];
  const aborted = () => requests.filter((r) => r.aborted).length;
  const strays: string[] = [];
  let testNo = 0;
  let endpoint = "";
  const server = Bun.serve({
    port: 0,
    idleTimeout: 0,
    fetch(req) {
      const r = { url: req.url, headers: new Headers(req.headers), aborted: false };
      // Each test has its own path, so a late request from an earlier test
      // (which would be a leak) is caught instead of confusing this one.
      if (new URL(req.url).pathname !== `${new URL(endpoint).pathname}/v1/stream/checkpoints`) {
        strays.push(req.url);
        return new Response("stray", { status: 500 });
      }
      requests.push(r);
      req.signal.addEventListener("abort", () => (r.aborted = true));
      return handler(req, requests.length);
    },
  });
  afterAll(() => {
    server.stop(true);
    expect(strays).toEqual([]);
  });
  /** A body like the service's: each frame its own zstd frame; then close, or stay open without keepalives. */
  const body = (frames: Uint8Array[], hold = false) =>
    new Response(
      new ReadableStream<Uint8Array>({
        start(c) {
          for (const f of [keepalive(), ...frames]) c.enqueue(zstd(f));
          if (!hold) c.close();
        },
      }),
      { headers: { "content-type": "application/vnd.miraicloud.checkpoints.v1+proto", "content-encoding": "zstd" } },
    );
  const reset = (h: Handler) => {
    handler = h;
    requests = [];
    endpoint = `http://127.0.0.1:${server.port}/t${++testNo}`;
  };
  const from = (i: number) => new URL(requests[i]!.url).searchParams.get("from");
  const keyAuth = Auth.Key({ token: Redacted.make("mc1_test") });
  /** The stream for `options`, as the service from `layerFetch` serves it. */
  const streamOf = (mode: Mode, options: CheckpointStreamOptions = {}, auth: Auth = keyAuth): Stream.Stream<Item, unknown> =>
    Stream.unwrap(
      Effect.map(MiraiCloudStream, (s): Stream.Stream<Item, unknown> => (mode === "raw" ? s.rawCheckpoints({ archive: false, ...options }) : s.checkpoints({ archive: false, ...options }))),
    ).pipe(
      Stream.provide(layerFetch({ endpoint, auth })),
    );
  const collect = (mode: Mode, n: number, options: CheckpointStreamOptions = {}) =>
    Effect.runPromise(streamOf(mode, options).pipe(Stream.take(n), Stream.runCollect)).then((items) => [...items].map((i) => checked(mode, i)));
  const failure = (mode: Mode, options: CheckpointStreamOptions = {}) => Effect.runPromise(streamOf(mode, options).pipe(Stream.runCollect, Effect.flip));

  for (const mode of modes) {
    describe(mode, () => {
      test("resume with from= across closes; mask applied locally; accept, encoding and key sent; gaps surfaced", async () => {
        reset((_, n) => (n === 1 ? body([gapFrame(90, 99), cpFrame(100), cpFrame(101)]) : body([cpFrame(101), cpFrame(102), gapFrame(103, 104)])));
        // The mask keeps every field these checkpoints have: same content (raw: same bytes).
        expect(await collect(mode, 5, { from: 90, readMask: ["sequence_number", "digest"] })).toEqual(["gap 90-99", 100, 101, 102, "gap 103-104"]);
        expect([from(0), from(1)]).toEqual(["90", "102"]);
        expect(new URL(requests[0]!.url).searchParams.get("read_mask")).toBeNull();
        expect(requests[0]!.headers.get("accept-encoding")).toBe("zstd");
        expect(requests[0]!.headers.get("accept")).toBe("application/vnd.miraicloud.checkpoints.v1+proto");
        expect(requests[0]!.headers.get("authorization")).toBe("Bearer mc1_test");
      });

      test("from 0; after n; tip start", async () => {
        reset(() => body([cpFrame(0)]));
        await collect(mode, 1, { from: 0 });
        expect(from(0)).toBe("0");
        reset(() => body([cpFrame(8)]));
        await collect(mode, 1, { after: 7n });
        expect(from(0)).toBe("8");
        reset(() => body([cpFrame(7)]));
        await collect(mode, 1);
        expect(new URL(requests[0]!.url).search).toBe("");
      });

      test("invalid options fail with Invalid, before any request", async () => {
        reset(() => body([cpFrame(1)]));
        for (const options of [{ from: 1, after: 2 }, { stallTimeoutMs: 0 }, { progressTimeoutMs: 0 }, { from: -1 }, { from: 1.5 }, { readMask: ["nope"] }, { readMask: ["objects.objects.json"] }]) {
          expect(await failure(mode, options)).toBeInstanceOf(Invalid);
        }
        expect(requests.length).toBe(0);
      });

      test("a narrowing mask prunes stream checkpoints", async () => {
        reset(() => body([cpFrame(7)]));
        const [item] = await Effect.runPromise(streamOf(mode, { readMask: ["sequence_number"] }).pipe(Stream.take(1), Stream.runCollect));
        const cp = item?._tag === "Checkpoint" ? ("data" in item ? GrpcTypes.Checkpoint.fromBinary(item.data) : item.checkpoint) : undefined;
        expect(cp?.sequenceNumber).toBe(7n);
        expect(cp?.digest).toBeUndefined();
      });

      test("410 with Oldest-Checkpoint: backfilled from the archive client, then streamed", async () => {
        reset((_, n) => (n === 1 ? new Response("too old", { status: 410, headers: { "oldest-checkpoint": "20" } }) : body([cpFrame(20), cpFrame(21)], true)));
        const log: string[] = [];
        // The archive's tip is 19: backfilled to it, then the stream from 20.
        const client = fakeArchive({ tip: 19n }, log);
        expect(await collect(mode, 6, { from: 16, archive: { client }, readMask: ["sequence_number", "digest"] })).toEqual([16, 17, 18, 19, 20, 21]);
        expect(log).toEqual(["16-20"]);
        expect([from(0), from(1)]).toEqual(["16", "20"]);
      }, 10_000);

      test("410 without an archive: a gap", async () => {
        reset((_, n) => (n === 1 ? new Response("too old", { status: 410, headers: { "oldest-checkpoint": "20" } }) : body([cpFrame(20)], true)));
        expect(await collect(mode, 2, { from: 5 })).toEqual(["gap 5-19", 20]);
      });

      test("429 with Retry-After is honoured, then succeeds", async () => {
        reset((_, n) => (n === 1 ? new Response("stream limit reached", { status: 429, headers: { "retry-after": "2" } }) : body([cpFrame(5)])));
        const t0 = Date.now();
        expect(await collect(mode, 1, { from: 5 })).toEqual([5]);
        expect(Date.now() - t0).toBeGreaterThanOrEqual(1950);
        expect(requests.length).toBe(2);
      });

      test("refusals are final, as the API's errors", async () => {
        reset(() => new Response("bad request: invalid from", { status: 400 }));
        const invalid = await failure(mode);
        expect(invalid).toBeInstanceOf(Invalid);
        expect((invalid as Invalid).message).toBe("bad request: invalid from");
        expect(requests.length).toBe(1);
        reset(() => Response.json({ _tag: "Unauthorized", error: "Unauthorized", message: "unknown key" }, { status: 401 }));
        expect(await failure(mode)).toEqual(new Unauthorized({ message: "unknown key" }));
        reset(() => Response.json({ _tag: "QuotaExceeded", error: "QuotaExceeded", message: "over the monthly limit" }, { status: 429 }));
        expect(await failure(mode)).toEqual(new QuotaExceeded({ message: "over the monthly limit" }));
        expect(requests.length).toBe(1);
      });

      test("stall: no bytes → reconnect from the cursor", async () => {
        reset((_, n) => (n === 1 ? body([cpFrame(1)], true) : body([cpFrame(2)], true)));
        const t0 = Date.now();
        expect(await collect(mode, 2, { stallTimeoutMs: 300 })).toEqual([1, 2]);
        expect(Date.now() - t0).toBeGreaterThanOrEqual(780); // stall + the first backoff (1 s, jittered down to half)
        expect(from(1)).toBe("2");
      });

      test("progress: keepalives but no checkpoints → reconnect from the cursor", async () => {
        // The first connection delivers 1, then only keepalives (it never
        // stalls): the progress watchdog resumes from 2 (2026-11-08).
        let timer: ReturnType<typeof setInterval> | undefined;
        const alive = (frames: Uint8Array[]) =>
          new Response(
            new ReadableStream<Uint8Array>({
              start(c) {
                for (const f of [keepalive(), ...frames]) c.enqueue(zstd(f));
                timer = setInterval(() => c.enqueue(zstd(keepalive())), 50);
              },
              cancel() {
                clearInterval(timer);
              },
            }),
            { headers: { "content-type": "application/vnd.miraicloud.checkpoints.v1+proto", "content-encoding": "zstd" } },
          );
        reset((_, n) => (n === 1 ? alive([cpFrame(1)]) : body([cpFrame(2)], true)));
        const t0 = Date.now();
        expect(await collect(mode, 2, { stallTimeoutMs: 300, progressTimeoutMs: 500 })).toEqual([1, 2]);
        clearInterval(timer);
        expect(Date.now() - t0).toBeGreaterThanOrEqual(500);
        expect(from(1)).toBe("2");
      });

      test("a slow consumer is not a stall", async () => {
        reset(() => body([cpFrame(1), cpFrame(2), cpFrame(3)], true));
        const out: (string | number)[] = [];
        for await (const item of Stream.toAsyncIterable(streamOf(mode, { stallTimeoutMs: 100 }))) {
          out.push(checked(mode, item));
          if (out.length === 3) break;
          await sleep(300);
        }
        expect(out).toEqual([1, 2, 3]);
        expect(requests.length).toBe(1);
      });

      test("ending during a pending read returns promptly and closes the connection", async () => {
        reset(() => body([cpFrame(1)], true));
        const it = Stream.toAsyncIterable(streamOf(mode))[Symbol.asyncIterator]();
        expect(checked(mode, (await it.next()).value)).toBe(1);
        const pending = it.next();
        await sleep(50);
        const t0 = Date.now();
        await it.return?.();
        expect((await pending).done).toBe(true);
        expect(Date.now() - t0).toBeLessThan(500);
        await sleep(50);
        expect(aborted()).toBe(1);
        expect(requests.length).toBe(1);
      });

      test("interrupting the Effect stream closes the connection", async () => {
        reset(() => body([gapFrame(1, 2), cpFrame(3), cpFrame(4)], true));
        expect(await collect(mode, 3, { from: 1 })).toEqual(["gap 1-2", 3, 4]);
        await sleep(50);
        expect(aborted()).toBe(1);
      });

      test("an undecodable checkpoint: raw passes it through unchanged, typed reconnects", async () => {
        const junk = frame(0, concat([GrpcTypes.SubscribeCheckpointsResponse.toBinary({ cursor: 1n }), new Uint8Array([0x12, 0x03, 0x0a, 0x05, 1])]));
        reset((_, n) => body([n === 1 ? junk : cpFrame(1), cpFrame(2)], true));
        const diags: Diagnostic[] = [];
        const items = await Effect.runPromise(streamOf(mode, { from: 1, onDiagnostic: (d) => diags.push(d) }).pipe(Stream.take(2), Stream.runCollect));
        expect([...items].map(label)).toEqual([1, 2]);
        if (mode === "raw") {
          expect([...items].map((i) => (i._tag === "Checkpoint" && "data" in i ? [...i.data] : []))).toEqual([[0x0a, 0x05, 1], [...encoded(2)]]);
          expect(requests.length).toBe(1);
        } else {
          expect(requests.length).toBe(2);
          expect(from(1)).toBe("1");
          expect(diags.some((d) => d._tag === "Disconnect" && d.reason.includes("undecodable"))).toBe(true);
        }
      });
    });
  }

  test("as the owner, every connection is signed afresh (path and query included)", async () => {
    reset((_, n) => (n === 1 ? body([cpFrame(5)]) : body([cpFrame(6)])));
    const signer = fromSuiSigner(Ed25519Keypair.generate());
    await Effect.runPromise(streamOf("typed", { from: 5 }, Auth.Owner({ signer })).pipe(Stream.take(2), Stream.runCollect));
    expect(requests.map((r) => r.headers.get(HEADERS.address))).toEqual([signer.address, signer.address]);
    const nonces = requests.map((r) => r.headers.get(HEADERS.nonce));
    expect(new Set(nonces).size).toBe(2);
    expect(requests.every((r) => r.headers.get(HEADERS.signature) !== null && r.headers.get("authorization") === null)).toBe(true);
  });
});

// ---- the HTTP source over a stand-in client ----

describe("http source", () => {
  /** An `HttpClient` that answers with the body as sent: `chunks`, not decoded. */
  const rawClient = (chunks: Uint8Array[], seen?: Record<string, string>[], status = 200, headers: Record<string, string> = {}) =>
    HttpClient.make((request, url) =>
      Effect.sync(() => {
        seen?.push({ url: url.toString(), ...request.headers });
        return HttpClientResponse.fromWeb(
          request,
          new Response(
            status !== 200
              ? "refused"
              : new ReadableStream<Uint8Array>({
                  async start(c) {
                    for (const chunk of chunks) {
                      c.enqueue(chunk);
                      await sleep(5);
                    }
                    c.close();
                  },
                }),
            { status, headers: { "content-type": "application/vnd.miraicloud.checkpoints.v1+proto", "content-encoding": "zstd", ...headers } },
          ),
        );
      }),
    );
  const options = (client: HttpClient.HttpClient, extra: Partial<SourceOptions> = {}): SourceOptions => ({
    client,
    endpoint: "https://gateway.test",
    auth: Auth.None(),
    zstdDecoder: undefined,
    stallTimeoutMs: 1000,
    progressTimeoutMs: 60_000,
    ...extra,
  });
  const drain = (src: ReturnType<typeof httpSource<AnyCheckpoint, AnyGap>>, after?: bigint) => Effect.runPromiseExit(Stream.runCollect(src.connect(after)));
  const typed = decoded as unknown as Decoder<AnyCheckpoint, AnyGap>;

  test("the URL is the endpoint's /v1/stream/checkpoints; from = after + 1", async () => {
    const seen: Record<string, string>[] = [];
    const exit = await drain(httpSource(options(rawClient([cpFrame(7)], seen)), typed), 6n);
    expect(Exit.isSuccess(exit) && exit.value.length).toBe(1);
    expect(seen[0]!.url).toBe("https://gateway.test/v1/stream/checkpoints?from=7");
  });

  test("a body the client left zstd-compressed is decoded with zstdDecoder, concatenated frames, even when the magic is split", async () => {
    const [first, ...rest] = [keepalive(), cpFrame(1), cpFrame(2)].map(zstd);
    const chunks = [...[...first!].map((b) => new Uint8Array([b])), ...rest];
    const exit = await drain(httpSource(options(rawClient(chunks), { zstdDecoder: nativeZstdDecoder() }), typed));
    expect(Exit.isSuccess(exit) && exit.value.map((e) => (e as AnyCheckpoint).seq)).toEqual([1n, 2n]);
    // Several zstd frames in one chunk decode as one stream too.
    const joined = await drain(httpSource(options(rawClient([concat([keepalive(), cpFrame(3), gapFrame(4, 5), cpFrame(6)].map(zstd))]), { zstdDecoder: nativeZstdDecoder() }), typed));
    expect(Exit.isSuccess(joined) && joined.value.map((e) => label(e))).toEqual([3, "gap 4-5", 6]);
  });

  test("a compressed body without a zstd decoder is a defect", async () => {
    const exit = await drain(httpSource(options(rawClient([zstd(cpFrame(1))])), typed));
    expect(Exit.isFailure(exit) && String(exit.cause)).toContain("zstdDecoder");
    expect(Exit.isFailure(exit) && exit.cause.reasons.some((r) => r._tag === "Die")).toBe(true);
  });

  test("decoded bodies pass through, including ones shorter than the magic", async () => {
    const whole = concat([keepalive(), cpFrame(3)]);
    for (const chunks of [[], [new Uint8Array([2, 0])], [whole.subarray(0, 1), whole.subarray(1)]]) {
      const exit = await drain(httpSource(options(rawClient(chunks), { zstdDecoder: nativeZstdDecoder() }), typed));
      expect(Exit.isSuccess(exit) && exit.value.length).toBe(chunks.length === 2 ? 1 : 0);
    }
  });

  test("410 carries the oldest checkpoint; 503 is retried after Retry-After", async () => {
    const gone = await drain(httpSource(options(rawClient([], undefined, 410, { "oldest-checkpoint": "123" })), typed), 5n);
    expect(Exit.isFailure(gone) && Option.getOrUndefined(Exit.findErrorOption(gone))).toEqual(new TooOld({ message: "refused", oldest: 123n }));
    const cold = await drain(httpSource(options(rawClient([], undefined, 503, { "retry-after": "2" })), typed));
    const error = Exit.isFailure(cold) ? Option.getOrUndefined(Exit.findErrorOption(cold)) : undefined;
    expect(error).toBeInstanceOf(Retry);
    expect((error as Retry).retryAfterMs).toBe(2000);
  });

  test("a 404 before the stream has once answered is retried NOT_FOUND_ATTEMPTS times in all, then final", async () => {
    const src = httpSource(options(rawClient([], undefined, 404)), typed);
    for (let attempt = 1; attempt <= NOT_FOUND_ATTEMPTS; attempt++) {
      const exit = await Effect.runPromiseExit(Stream.runCollect(src.connect(undefined)));
      const error = Exit.isFailure(exit) ? Option.getOrUndefined(Exit.findErrorOption(exit)) : undefined;
      if (attempt < NOT_FOUND_ATTEMPTS) expect(error).toBeInstanceOf(Retry);
      else expect(error).toBeInstanceOf(NotFound);
    }
  });

  test("stall while waiting for headers → retried", async () => {
    const hanging = HttpClient.make(() => Effect.never);
    const exit = await drain(httpSource(options(hanging, { stallTimeoutMs: 50 }), typed));
    const error = Exit.isFailure(exit) ? Option.getOrUndefined(Exit.findErrorOption(exit)) : undefined;
    expect(error).toBeInstanceOf(Retry);
    expect((error as Retry).message).toContain("no data");
  });
});
