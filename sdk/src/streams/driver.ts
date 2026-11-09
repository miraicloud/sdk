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
import { ArchiveError, type HistorySource } from "./archive.js";

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
  /** An archive page failed; it is asked again in `retryInMs`. */
  ArchiveRetry: { readonly reason: string; readonly retryInMs: number };
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
   * A connection that delivers nothing new (no checkpoint or gap past the
   * cursor; keepalives and replays don't count) for this long is dropped
   * and resumed. Time waiting on the consumer doesn't count.
   */
  readonly progressTimeoutMs: number;
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

/**
 * Items buffered between the driver and the consumer; beyond this, reading
 * pauses. Few: memory is bounded by items, and an epoch-change checkpoint
 * is 10-20 MB of bytes and several times that as decoded objects.
 */
const BUFFER = 2;

/** The highest sequence number there is: nothing can follow it. */
const MAX_SEQ = 2n ** 64n - 1n;

/**
 * Positions within this of the highest known one (delivered, or the start)
 * are believed as they are: a 410's oldest after a short absence, a gap
 * after an ingest outage (`maxHole` is 1,000). Beyond, a probe of the live
 * tip must confirm them (as the Rust SDK's `believable`).
 */
const NEAR = 1_000n;
/** A claimed position may be this far past the live tip a probe saw. */
const TIP_SLACK = 100n;
/** Longest wait for a probe's first frame. */
const PROBE_MS = 45_000;

/** A clock that only moves forward (ms): the watchdog can't be stopped by a wall-clock step. */
const monotonic = Effect.sync(() => performance.now());
/** Longest wait for the archive's next item (a page holds 50; epoch-change checkpoints are 10-20 MB). */
// A backstop only, past the archive client's own per-call limit (stalls are caught there:
// 30 s to the first response, 30 s between messages); a slow but working link must not lose its page.
const HISTORY_ITEM_MS = 660_000;
/** As `checkpoints.ts`: the most a `Retry-After` can make a subscription wait. */
const MAX_RETRY_AFTER_MS = 30_000;

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
        // The highest position known (delivered, the start, an accepted
        // Oldest-Checkpoint, or a probed tip): what a 410 or a frame may claim
        // without a probe (`believable`).
        let known: bigint | undefined = cursor;
        // The progress watchdog's clock: the last time the cursor moved, and
        // whether an item is waiting on the consumer (that time isn't the server's).
        let progressedMs = 0;
        let offering = false;
        const diagnose = (d: Diagnostic) => Effect.sync(() => cfg.onDiagnostic?.(d));
        /** Yield an item: the cursor moves, and progress resets the backoff and the watchdog. */
        const emit = (item: C | G) =>
          Effect.suspend(() => {
            cursor = item._tag === "Checkpoint" ? (item as C).seq : (item as G).to;
            failures = 0;
            offering = true;
            return Queue.offer(queue, item).pipe(
              Effect.ensuring(
                Effect.flatMap(monotonic, (now) =>
                  Effect.sync(() => {
                    offering = false;
                    progressedMs = now;
                    if (known === undefined || cursor! > known) known = cursor;
                  }),
                ),
              ),
            );
          });
        /** Fails once the connection has delivered nothing new for `progressTimeoutMs` (checked each second). */
        const watchdog = Effect.gen(function* () {
          progressedMs = yield* monotonic;
          for (;;) {
            yield* Effect.sleep(Math.min(1000, cfg.progressTimeoutMs));
            const now = yield* monotonic;
            if (offering) progressedMs = now;
            else if (now - progressedMs >= cfg.progressTimeoutMs)
              return yield* new Skip({ reason: `no new checkpoints for ${cfg.progressTimeoutMs} ms` });
          }
        });
        /** The live tip: the first frame of a connection opened at the tip (undefined: no answer in time). */
        const probeTip: Effect.Effect<bigint | undefined> = cfg.live.connect(undefined).pipe(
          Stream.runHead,
          Effect.map(Option.match({ onNone: () => undefined, onSome: (ev) => (ev._tag === "Checkpoint" ? (ev as C).seq : (ev as G).to) })),
          Effect.timeoutOrElse({ duration: PROBE_MS, orElse: () => Effect.succeed(undefined) }),
          Effect.orElseSucceed(() => undefined),
        );
        /**
         * Whether position `claimed` can be true: near what's known, yes;
         * beyond, only if a probe of the live tip confirms it. `tip` caches
         * the probe for one connection (null: asked, no answer).
         */
        let tip: bigint | null | undefined;
        const believable = (claimed: bigint) =>
          Effect.gen(function* () {
            if (known !== undefined && claimed <= known + NEAR) return true;
            if (tip === undefined) tip = (yield* probeTip) ?? null;
            if (tip !== null && known === undefined) known = tip;
            else if (tip !== null && tip > known!) known = tip;
            return tip !== null && claimed <= tip + TIP_SLACK;
          });

        /** One connection, until it ends; replays are dropped, skips break it. */
        const connection = (after: ResumeAfter): Effect.Effect<Outcome, E> =>
          cfg.live.connect(after).pipe(
            Stream.runForEach((ev) =>
              Effect.suspend(() => {
                const expected = cursor === undefined ? undefined : cursor + 1n;
                if (ev._tag === "Gap" && ((ev as G).from > (ev as G).to || (ev as G).to >= MAX_SEQ))
                  return Effect.fail(new Skip({ reason: `malformed gap ${(ev as G).from}..=${(ev as G).to}` }));
                if (ev._tag === "Checkpoint" && (ev as C).seq >= MAX_SEQ) return Effect.fail(new Skip({ reason: `checkpoint ${(ev as C).seq}: nothing can follow it` }));
                // A frame far beyond anything known must be confirmed by the live tip:
                // taken as it is, it would move the cursor there for good.
                const at = ev._tag === "Checkpoint" ? (ev as C).seq : (ev as G).to;
                if (known !== undefined && at > known + NEAR)
                  return Effect.flatMap(believable(at), (ok) =>
                    ok ? deliver(ev, expected) : Effect.fail(new Skip({ reason: `a frame at ${at} is beyond the live tip (${tip ?? "unknown"})` })),
                  );
                return deliver(ev, expected);
              }),
            ),
            Effect.as<Outcome>({ _tag: "Retry", reason: "stream closed", retryAfterMs: 0 }),
            // The connection and the watchdog: whichever ends first (interrupting the other closes the request).
            (run) => Effect.raceFirst(run, watchdog),
            Effect.catch((e: Skip | Retry | Renew | TooOld | E): Effect.Effect<Outcome, E> => {
              if (e instanceof Skip) return Effect.succeed({ _tag: "Retry", reason: e.reason, retryAfterMs: 0 });
              if (e instanceof Renew) return Effect.succeed({ _tag: "Renew", reason: e.message });
              if (e instanceof Retry) return Effect.succeed({ _tag: "Retry", reason: e.message, retryAfterMs: e.retryAfterMs ?? 0 });
              if (e instanceof TooOld) return Effect.succeed({ _tag: "TooOld", oldest: e.oldest, reason: e.message });
              return Effect.fail(e);
            }),
          );

        /** A frame in order: delivered; a replay: dropped; one that skips past the cursor: the connection is broken. */
        const deliver = (ev: C | G, expected: bigint | undefined): Effect.Effect<void, Skip> => {
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
        };

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
              // As the Rust SDK's `history_item` deadline: an archive that stops answering is retried, never waited on forever.
              Stream.timeoutOrElse({ duration: HISTORY_ITEM_MS, orElse: () => Stream.fail(new ArchiveError({ message: `no answer in ${HISTORY_ITEM_MS / 1000} s` })) }),
              // Past the archive's tip: stop (trailing missing ones aren't gaps).
              Stream.takeWhile((item) => item.type !== "notYet"),
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
            // Short of the stream's oldest (the archive lags it): back off before asking the stream again.
            if (missing === undefined && (cursor === undefined || cursor + 1n < oldest)) return `archive: behind the stream (next ${cursor === undefined ? from : cursor + 1n}, stream from ${oldest})`;
            return undefined;
          });

        for (;;) {
          yield* diagnose(Diagnostic.Connect({ after: Option.fromUndefinedOr(cursor) }));
          tip = undefined; // a probe answers for one connection
          const outcome = yield* connection(cursor);
          if (outcome._tag === "Renew") {
            yield* diagnose(Diagnostic.Disconnect({ reason: outcome.reason, retryInMs: 0 }));
            continue;
          }
          let reason = outcome.reason;
          let retryAfterMs = outcome._tag === "Retry" ? outcome.retryAfterMs : 0;
          if (outcome._tag === "TooOld" && !(yield* believable(outcome.oldest))) {
            // Beyond the live tip: believing it would report a gap that never happened. Ask again.
            reason = `implausible Oldest-Checkpoint ${outcome.oldest} (live tip ${tip ?? "unknown"})`;
          } else if (outcome._tag === "TooOld") {
            if (known === undefined || outcome.oldest > known) known = outcome.oldest;
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
          const wait = Math.max(Math.round(backoff * (0.5 + 0.5 * (cfg.random ?? Math.random)())), Math.min(retryAfterMs, MAX_RETRY_AFTER_MS));
          failures++;
          yield* diagnose(Diagnostic.Disconnect({ reason, retryInMs: wait }));
          yield* Effect.sleep(wait);
        }
      }),
    { bufferSize: BUFFER, strategy: "suspend" },
  );
