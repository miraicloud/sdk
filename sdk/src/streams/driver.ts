/**
 * The checkpoint stream's client loop: one connection at a time, resumed
 * after the cursor, with capped, jittered exponential backoff. Positions older than
 * the stream holds (`410`) are read from history (the archive) up to
 * history's own tip when it reports one (the archive keeps up with the
 * chain), so the stream is rejoined well within what it holds; reading only
 * up to the stream's oldest position would chase a window that moves on
 * while history is read. If the stream still answers `410` (history
 * lagging), this repeats.
 *
 * Written as one Effect loop feeding a bounded queue (`Stream.callback`):
 * the consumer pulling from the stream is what lets the loop continue, so
 * backpressure reaches the network.
 *
 * Cursor: the last sequence number delivered, as a checkpoint or as the end
 * of a gap. Connections resume after it, so replays are dropped and a gap is
 * reported once. Anything that would skip past the cursor without a gap (a
 * protocol violation) is treated as a broken connection: reconnect.
 */

import type { GrpcTypes } from "@mysten/sui/grpc";
import { Data, Effect, Option, Queue, Stream } from "effect";
import type { HistorySource } from "./archive.js";

/** A gap: checkpoints `from..=to` cannot be delivered. */
export type GapLike = { readonly _tag: "Gap"; readonly from: bigint; readonly to: bigint };
/** A checkpoint as the driver needs it: ordered and deduplicated by `seq`. */
export type CheckpointLike = { readonly _tag: "Checkpoint"; readonly seq: bigint };

/**
 * How checkpoints and gaps are delivered: decoded (typed) or left encoded
 * (raw), from the stream's frames or from the archive's decoded messages.
 */
export interface Decoder<C extends CheckpointLike, G extends GapLike> {
  /** A checkpoint frame's encoded `Checkpoint`; throws `Retry` if it is malformed. */
  readonly frame: (seq: bigint, data: Uint8Array) => C;
  /** A checkpoint read from the archive. */
  readonly message: (seq: bigint, checkpoint: GrpcTypes.Checkpoint) => C;
  readonly gap: (from: bigint, to: bigint) => G;
}

/**
 * Where a connection starts: after `seq` (sent as `from=<seq + 1>`; `-1n`
 * is genesis), or at the tip (`undefined`).
 */
export type ResumeAfter = bigint | undefined;

/** The connection's key is about to expire: reconnect at once, with a fresh one (no backoff, nothing lost). */
export class Renew extends Data.TaggedError("Renew")<{ readonly message: string }> {}

/** A connection broke, or was refused for now: reconnect after backoff (at least `retryAfterMs`). */
export class Retry extends Data.TaggedError("Retry")<{ readonly message: string; readonly retryAfterMs?: number | undefined; readonly cause?: unknown }> {}

/** `410`: the position is older than the stream holds; it starts at `oldest`. */
export class TooOld extends Data.TaggedError("TooOld")<{ readonly message: string; readonly oldest: bigint }> {}

/**
 * Opens connections: one stream per connection, in the order the server
 * sends. It ends when the connection closes and fails when it breaks (the
 * driver reconnects) or is refused for good (`E`, which ends the
 * subscription). Replaceable for tests.
 */
export interface LiveSource<C extends CheckpointLike, G extends GapLike, E> {
  readonly connect: (after: ResumeAfter) => Stream.Stream<C | G, Retry | Renew | TooOld | E>;
}

/** Connection lifecycle, for logging and metrics. */
export type Diagnostic = Data.TaggedEnum<{
  /** A connection is opening, resuming after `after` (none: at the live tip). */
  Connect: { readonly after: Option.Option<bigint> };
  /** Reading `from..=to` from the archive. */
  Backfill: { readonly from: bigint; readonly to: bigint };
  /** A connection ended; the next one opens in `retryInMs`. */
  Disconnect: { readonly reason: string; readonly retryInMs: number };
}>;
export const Diagnostic = Data.taggedEnum<Diagnostic>();

export interface DriverConfig<C extends CheckpointLike, G extends GapLike, E> {
  readonly live: LiveSource<C, G, E>;
  readonly decode: Decoder<C, G>;
  /** Where positions older than the stream come from; undefined → reported as a gap. */
  readonly history?: HistorySource | undefined;
  /** First checkpoint to deliver; undefined → start at the live tip. */
  readonly startAt: bigint | undefined;
  readonly onDiagnostic?: ((d: Diagnostic) => void) | undefined;
  /** Reconnect backoff: starts here, doubles up to `backoffMaxMs`, resets on progress. */
  readonly backoffBaseMs: number;
  readonly backoffMaxMs: number;
  /**
   * A number in [0, 1) for jitter (default `Math.random`). Each wait is
   * between half and all of the backoff, so clients dropped together (a
   * deploy, an outage) don't all come back in the same instant.
   */
  readonly random?: (() => number) | undefined;
}

/** Why a connection ended, and what to do next. */
type Outcome =
  | { readonly _tag: "Retry"; readonly reason: string; readonly retryAfterMs: number }
  | { readonly _tag: "Renew"; readonly reason: string }
  /** `410`: positions before `oldest` are no longer on the stream. */
  | { readonly _tag: "TooOld"; readonly oldest: bigint; readonly reason: string };

/** A frame that would skip past the cursor without a gap: treat the connection as broken. */
class Skip extends Data.TaggedError("Skip")<{ readonly reason: string }> {}

/** Items buffered between the driver and the consumer; beyond this, reading pauses. */
const BUFFER = 16;

/**
 * Ordered, deduplicated items. Fails only with `E` (a final refusal);
 * everything else is retried. Interrupting the stream closes the connection.
 */
export const drive = <C extends CheckpointLike, G extends GapLike, E>(cfg: DriverConfig<C, G, E>): Stream.Stream<C | G, E> =>
  Stream.callback<C | G, E>(
    (queue) =>
      Effect.gen(function* () {
        // -1n = "from genesis" (the HTTP source sends `from=<after + 1>`).
        let cursor: ResumeAfter = cfg.startAt === undefined ? undefined : cfg.startAt - 1n;
        let failures = 0;
        const diagnose = (d: Diagnostic) => Effect.sync(() => cfg.onDiagnostic?.(d));
        /** Yield an item: the cursor moves, and progress resets the backoff. */
        const emit = (item: C | G) =>
          Effect.suspend(() => {
            cursor = item._tag === "Checkpoint" ? (item as C).seq : (item as G).to;
            failures = 0;
            return Queue.offer(queue, item);
          });

        /** One connection, until it ends; replays are dropped, skips break it. */
        const connection = (after: ResumeAfter): Effect.Effect<Outcome, E> =>
          cfg.live.connect(after).pipe(
            Stream.runForEach((ev) =>
              Effect.suspend(() => {
                const expected = cursor === undefined ? undefined : cursor + 1n;
                if (ev._tag === "Checkpoint") {
                  const seq = (ev as C).seq;
                  if (expected !== undefined && seq < expected) return Effect.void; // replayed
                  if (expected !== undefined && seq > expected) return Effect.fail(new Skip({ reason: `checkpoint ${seq} skips ${expected}` }));
                  return emit(ev);
                }
                const gap = ev as G;
                if (expected !== undefined && gap.to < expected) return Effect.void; // already reported
                const from = expected !== undefined && gap.from < expected ? expected : gap.from;
                if (expected !== undefined && from > expected) return Effect.fail(new Skip({ reason: `gap ${from}..=${gap.to} skips ${expected}` }));
                return emit(from === gap.from ? gap : cfg.decode.gap(from, gap.to));
              }),
            ),
            Effect.as<Outcome>({ _tag: "Retry", reason: "stream closed", retryAfterMs: 0 }),
            Effect.catch((e: Skip | Retry | Renew | TooOld | E): Effect.Effect<Outcome, E> => {
              if (e instanceof Skip) return Effect.succeed({ _tag: "Retry", reason: e.reason, retryAfterMs: 0 });
              if (e instanceof Renew) return Effect.succeed({ _tag: "Renew", reason: e.message });
              if (e instanceof Retry) return Effect.succeed({ _tag: "Retry", reason: e.message, retryAfterMs: e.retryAfterMs ?? 0 });
              if (e instanceof TooOld) return Effect.succeed({ _tag: "TooOld", oldest: e.oldest, reason: e.message });
              return Effect.fail(e);
            }),
          );

        /**
         * From `from` up to history's tip (at least to `oldest`, the
         * stream's first position), in order; runs of missing checkpoints
         * become gaps, but only those with a checkpoint after them: missing
         * ones at the end are history lagging its own tip (ListCheckpoints
         * behind GetServiceInfo), so it stops before them. Returns why it
         * stopped early, if it did (nothing: the stream has the rest).
         */
        const backfill = (from: bigint, oldest: bigint, history: HistorySource) =>
          Effect.gen(function* () {
            // History's tip, if it can say (an unknown tip: up to the stream, as far as is known to be needed).
            const tip = history.tip ? yield* history.tip.pipe(Effect.option) : Option.none<bigint>();
            const end = Option.match(tip, { onNone: () => oldest, onSome: (t) => (t + 1n > oldest ? t + 1n : oldest) });
            yield* diagnose(Diagnostic.Backfill({ from, to: end - 1n }));
            let missing: [bigint, bigint] | undefined;
            const flushMissing = Effect.suspend(() => {
              if (!missing) return Effect.void;
              const [a, b] = missing;
              missing = undefined;
              return emit(cfg.decode.gap(a, b));
            });
            const result = yield* history.checkpoints(from, end).pipe(
              Stream.runForEach((item) => {
                if (item.type === "missing") {
                  missing = missing ? [missing[0], item.seq] : [item.seq, item.seq];
                  return Effect.void;
                }
                return flushMissing.pipe(Effect.andThen(emit(cfg.decode.message(item.seq, item.checkpoint))));
              }),
              Effect.result,
            );
            if (result._tag === "Failure") return `archive: ${result.failure.message}`;
            // Not reported as a gap: the cursor stays before them. From the stream's oldest on, the stream serves them; older, history is asked again.
            if (missing !== undefined && missing[0] < oldest) return `archive: checkpoints ${missing[0]}..=${missing[1]} not in history yet`;
            return undefined;
          });

        for (;;) {
          yield* diagnose(Diagnostic.Connect({ after: Option.fromUndefinedOr(cursor) }));
          const outcome = yield* connection(cursor);
          if (outcome._tag === "Renew") {
            yield* diagnose(Diagnostic.Disconnect({ reason: outcome.reason, retryInMs: 0 }));
            continue;
          }
          let reason = outcome.reason;
          let retryAfterMs = outcome._tag === "Retry" ? outcome.retryAfterMs : 0;
          if (outcome._tag === "TooOld") {
            const from = cursor === undefined ? undefined : cursor + 1n;
            if (from !== undefined && from < outcome.oldest) {
              if (!cfg.history) {
                // No archive: report the older positions as a gap and go on.
                yield* emit(cfg.decode.gap(from, outcome.oldest - 1n));
                continue;
              }
              const stopped = yield* backfill(from, outcome.oldest, cfg.history);
              // Within reach of the stream now: reconnect right away.
              if (stopped === undefined) continue;
              reason = stopped;
              retryAfterMs = 0;
            }
          }
          const backoff = Math.min(cfg.backoffBaseMs * 2 ** failures, cfg.backoffMaxMs);
          const wait = Math.max(Math.round(backoff * (0.5 + 0.5 * (cfg.random ?? Math.random)())), retryAfterMs);
          failures++;
          yield* diagnose(Diagnostic.Disconnect({ reason, retryInMs: wait }));
          yield* Effect.sleep(wait);
        }
      }),
    { bufferSize: BUFFER, strategy: "suspend" },
  );
