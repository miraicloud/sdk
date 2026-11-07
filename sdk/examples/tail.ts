/**
 * Tail the checkpoint stream and check continuity.
 *
 *   MIRAICLOUD_API_KEY=mc1_… bun examples/tail.ts [--endpoint U] [--mask a,b] [--from N | --ago N] [--seconds S]
 *
 * `--ago N` starts N checkpoints below the current tip (read from the stream
 * itself); what is older than the stream holds comes from the archive.
 * Prints a JSON summary: counts, order violations, duplicates, gaps,
 * reconnects, backfills, memory.
 */

import { Effect, Redacted, Stream } from "effect";
import { Auth, Streams } from "../src/index.js";
import { fetchDecodesZstd } from "../src/streams/checkpoints.js";

const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i]!.replace(/^--/, ""), process.argv[i + 1] ?? "");

const endpoint = args.get("endpoint");
const mask = args.get("mask")?.split(",");
const seconds = Number(args.get("seconds") ?? 60);
const layer = Streams.layerFetch({ endpoint, auth: Auth.Key({ token: Redacted.make(process.env.MIRAICLOUD_API_KEY ?? "") }) });
/** The checkpoint stream for `options`, as an async iterable. */
const items = (options: Streams.CheckpointStreamOptions) =>
  Stream.toAsyncIterable(Stream.unwrap(Effect.map(Streams.MiraiCloudStream, (s) => s.checkpoints(options))).pipe(Stream.provide(layer)));

let from: bigint | undefined = args.has("from") ? BigInt(args.get("from")!) : undefined;
if (args.has("ago")) {
  for await (const item of items({ readMask: ["sequence_number"] })) {
    if (item._tag === "Checkpoint") {
      from = item.seq - BigInt(args.get("ago")!);
      break;
    }
  }
}

const started = Date.now();
const t = () => ((Date.now() - started) / 1000).toFixed(1);
const stats = {
  runtime: typeof Bun !== "undefined" ? `bun ${Bun.version}` : `node ${process.version}`,
  zstd: fetchDecodesZstd() ? "fetch" : "node:zlib",
  endpoint: endpoint ?? Streams.DEFAULT_ENDPOINT,
  mask: mask ?? "full",
  from: from?.toString(),
  delivered: 0,
  first: undefined as string | undefined,
  last: undefined as string | undefined,
  orderViolations: 0,
  duplicates: 0,
  gaps: [] as string[],
  connects: 0,
  disconnects: [] as string[],
  backfills: [] as string[],
  caughtUpAfterS: undefined as number | undefined,
  lagS: undefined as number | undefined,
  memMiB: [] as string[],
};
const onDiagnostic = (d: Streams.Diagnostic) =>
  Streams.Diagnostic.$match(d, {
    Connect: () => void stats.connects++,
    Backfill: ({ from, to }) => void stats.backfills.push(`${t()}s ${from}..=${to}`),
    Disconnect: ({ reason, retryInMs }) => void stats.disconnects.push(`${t()}s ${reason.slice(0, 120)} (retry in ${retryInMs} ms)`),
  });

const iterator = items({ readMask: mask, from, onDiagnostic })[Symbol.asyncIterator]();
let prev: bigint | undefined;
const deadline = Date.now() + seconds * 1000;
const progress = setInterval(() => {
  const m = process.memoryUsage();
  const rss = Math.round(m.rss / 2 ** 20);
  const heap = Math.round(m.heapUsed / 2 ** 20);
  stats.memMiB.push(`${t()}s rss ${rss} heap ${heap}`);
  console.error(`[${t()}s] delivered ${stats.delivered} last ${prev} rss ${rss} MiB heap ${heap} MiB`);
}, 30_000);
try {
  // Ends the iteration at the deadline. (Not a `Promise.race` against a
  // timer on every item: each race leaves a reaction on the timer's promise
  // that keeps that item alive until the timer fires.)
  const stop = setTimeout(() => void iterator.return?.(), Math.max(0, deadline - Date.now()));
  stop.unref?.();
  while (Date.now() < deadline) {
    const next = await iterator.next();
    if (next.done) break;
    const item = next.value;
    if (item._tag === "Gap") {
      console.error(`[${t()}s] gap ${item.from}..=${item.to}`);
      stats.gaps.push(`${item.from}..=${item.to}`);
      if (prev !== undefined && item.from !== prev + 1n) stats.orderViolations++;
      prev = item.to;
      continue;
    }
    const seq = item.seq;
    if (item.checkpoint.sequenceNumber !== undefined && item.checkpoint.sequenceNumber !== seq) stats.orderViolations++;
    if (prev !== undefined && seq <= prev) stats.duplicates++;
    else if (prev !== undefined && seq !== prev + 1n) {
      stats.orderViolations++;
      console.error(`ORDER: ${seq} after ${prev}`);
    }
    prev = seq;
    stats.delivered++;
    stats.first ??= seq.toString();
    stats.last = seq.toString();
    const ts = item.checkpoint.summary?.timestamp;
    if (ts) {
      const lag = Date.now() / 1000 - Number(ts.seconds);
      stats.lagS = Math.round(lag * 10) / 10;
      if (lag < 10 && stats.caughtUpAfterS === undefined) stats.caughtUpAfterS = Number(t());
    }
  }
} catch (error) {
  console.error("terminated:", error);
  process.exitCode = 1;
} finally {
  clearInterval(progress);
  void iterator.return?.();
}
const covered = stats.first && stats.last ? BigInt(stats.last) - BigInt(stats.first) + 1n : 0n;
console.log(
  JSON.stringify(
    {
      ...stats,
      elapsedS: (Date.now() - started) / 1000,
      expected: covered.toString(),
      complete: stats.gaps.length === 0 && stats.orderViolations === 0 && covered === BigInt(stats.delivered),
      cursor: prev?.toString(),
    },
    null,
    2,
  ),
);
process.exit();
