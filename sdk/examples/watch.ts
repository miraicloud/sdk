/**
 * Print checkpoints as they arrive, one line each.
 *
 *   MIRAICLOUD_API_KEY=mc1_… bun examples/watch.ts [--from N]
 *
 * The key needs the `stream:read` scope
 * (`miraicloud keys create --name watch --scope stream:read`).
 */

import { Console, Effect, Redacted, Stream } from "effect";
import { Auth, Streams } from "../src/index.js";

const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i]!.replace(/^--/, ""), process.argv[i + 1] ?? "");

const key = process.env.MIRAICLOUD_API_KEY;
if (!key) {
  console.error("set MIRAICLOUD_API_KEY to an API key with the stream:read scope");
  process.exit(1);
}

const line = (seq: bigint, c: Streams.Checkpoint) => {
  const ts = c.summary?.timestamp;
  const ms = ts ? Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1e6) : undefined;
  const time = ms === undefined ? "?" : new Date(ms).toISOString().replace("T", " ").replace("Z", "");
  const lag = ms === undefined ? "?" : `${((Date.now() - ms) / 1000).toFixed(1)}s`;
  return `${seq.toString().padEnd(14)} ${String(c.summary?.epoch ?? "?").padEnd(6)} ${time.padEnd(24)} ${String(c.transactions.length).padStart(4)}  ${lag.padStart(5)}`;
};

const program = Effect.gen(function* () {
  const live = yield* Streams.MiraiCloudStream;
  yield* Console.log("checkpoint     epoch  time (UTC)               txs   lag");
  yield* live
    .checkpoints({
      from: args.has("from") ? BigInt(args.get("from")!) : undefined,
      readMask: ["sequence_number", "summary.epoch", "summary.timestamp", "transactions.digest"],
      onDiagnostic: (d) =>
        Streams.Diagnostic.$match(d, {
          Connect: () => undefined,
          Backfill: ({ from, to }) => console.error(`# reading ${from}..=${to} from the archive`),
          Disconnect: ({ reason, retryInMs }) => console.error(`# disconnected: ${reason} (retrying in ${retryInMs} ms)`),
        }),
    })
    .pipe(Stream.runForEach((item) => (item._tag === "Gap" ? Console.error(`# gap ${item.from}..=${item.to}`) : Console.log(line(item.seq, item.checkpoint)))));
});

Effect.runPromise(program.pipe(Effect.provide(Streams.layerFetch({ auth: Auth.Key({ token: Redacted.make(key) }) })))).catch((e) => {
  console.error(e instanceof Error ? e.message : String(e));
  process.exitCode = 1;
});
