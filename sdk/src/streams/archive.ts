/**
 * Backfill from the archive (`archive.mainnet.sui.io`, every checkpoint
 * since genesis, kept up to the tip) for positions older than the
 * checkpoint stream holds: `LedgerService.ListCheckpoints`, 50 checkpoints
 * a page (the archive's maximum), a few pages at a time, in order. The
 * archive also reports its tip, so a backfill can run all the way up to it:
 * the stream is then rejoined well within what it holds, instead of chasing
 * a window that moves on while history is read.
 *
 * Checkpoints come out exactly as the stream would serve them: the request
 * asks for what the stream sends under the caller's mask, the object types
 * fullnodes fill in effects (the archive does not) are filled from the
 * checkpoint's objects, and the result is pruned to the caller's mask (the
 * archive adds fields of its own, and helper fields may have been requested
 * for the object types).
 *
 * The archive speaks native gRPC only (no gRPC-web), so the default client
 * uses `@grpc/grpc-js` (Node, Bun; not browsers).
 */

import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { GrpcTypes } from "@mysten/sui/grpc";
import type { IMessageType } from "@protobuf-ts/runtime";
import { Cause, Data, Effect, Queue, Schedule, Stream } from "effect";

type Checkpoint = GrpcTypes.Checkpoint;

/** The archive kept failing for a checkpoint (after retries). Retried by the driver. */
export class ArchiveError extends Data.TaggedError("ArchiveError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

export const MAINNET_ARCHIVE = "archive.mainnet.sui.io:443";

/** Every field the stream carries (the server's `CANONICAL_READ_MASK`): what it sends every client, BCS encodings included, and what `*` and parent paths stand for. */
export const CANONICAL: readonly string[] = [
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

/** The BCS encodings within `CANONICAL`: each repeats decoded fields as the node's BCS bytes. Served like any other field; a read mask may name them. */
export const BCS_PATHS: readonly string[] = [
  "summary.bcs",
  "contents.bcs",
  "transactions.transaction.bcs",
  "transactions.signatures.bcs",
  "transactions.effects.bcs",
  "transactions.events.bcs",
  "objects.objects.bcs",
];

/** The decoded fields: `CANONICAL` minus `BCS_PATHS` (the server's `DEFAULT_READ_MASK`). */
export const DEFAULT: readonly string[] = [
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

const TYPED = [
  "transactions.effects.changed_objects.object_type",
  "transactions.effects.unchanged_consensus_objects.object_type",
];
const TYPE_SOURCES = [
  "objects.objects.object_id",
  "objects.objects.version",
  "objects.objects.object_type",
];

/** Whether mask path `parent` includes field path `path`. */
export const covers = (parent: string, path: string) =>
  path === parent || path.startsWith(`${parent}.`);

export interface Plan {
  /** Request paths: what the stream sends for the caller's mask (+ helpers). */
  readonly request: readonly string[];
  /** Fill object types in effects. */
  readonly fill: boolean;
  /** Prune to these paths afterwards: the archive adds fields of its own, and helpers may have been requested. */
  readonly prune: readonly string[];
}

/**
 * The fields kept for `paths`: their overlap with `CANONICAL` (what the
 * stream sends); paths outside `CANONICAL` (always empty) as they are. No
 * paths: `CANONICAL`.
 */
export const effective = (paths: readonly string[]): string[] => {
  const served = CANONICAL;
  if (paths.length === 0) return [...served];
  const out: string[] = [];
  const add = (p: string) => {
    if (out.some((r) => covers(r, p))) return;
    for (let i = out.length - 1; i >= 0; i--)
      if (covers(p, out[i]!)) out.splice(i, 1);
    out.push(p);
  };
  for (const p of paths) {
    let hit = false;
    for (const s of served) {
      if (p === "*" || covers(p, s)) {
        add(s);
        hit = true;
      } else if (covers(s, p)) {
        add(p);
        hit = true;
      }
    }
    if (!hit && !CANONICAL.some((c) => covers(p, c) || covers(c, p))) add(p);
  }
  return out;
};

/** `mask`: the caller's paths (undefined: the whole checkpoint), already validated by `resolveMask`. */
export const plan = (mask: readonly string[] | undefined): Plan => {
  const request = effective(mask ?? []);
  const fill = TYPED.some((t) =>
    request.some((r) => covers(r, t) || covers(t, r)),
  );
  const prune = [...request];
  if (fill)
    for (const s of TYPE_SOURCES)
      if (!request.some((r) => covers(r, s))) request.push(s);
  return { request, fill, prune };
};

/**
 * Fill `objectType` of changed and unchanged consensus objects from the
 * checkpoint's own objects, as fullnodes do: the object at the input
 * version, or at the output version for created objects.
 */
export const fillObjectTypes = (checkpoint: Checkpoint): void => {
  const types = new Map<string, string>();
  for (const o of checkpoint.objects?.objects ?? []) {
    if (
      o.objectId !== undefined &&
      o.version !== undefined &&
      o.objectType !== undefined
    ) {
      types.set(`${o.objectId}@${o.version}`, o.objectType);
    }
  }
  if (types.size === 0) return;
  for (const tx of checkpoint.transactions) {
    const effects = tx.effects;
    if (!effects) continue;
    for (const c of effects.changedObjects) {
      const version = c.inputVersion ?? c.outputVersion;
      if (c.objectId === undefined || version === undefined) continue;
      const t = types.get(`${c.objectId}@${version}`);
      if (t !== undefined) c.objectType = t;
    }
    for (const u of effects.unchangedConsensusObjects) {
      if (u.objectId === undefined || u.version === undefined) continue;
      const t = types.get(`${u.objectId}@${u.version}`);
      if (t !== undefined) u.objectType = t;
    }
  }
};

type Tree = Map<string, Tree>;

const tree = (paths: readonly string[]): Tree => {
  const root: Tree = new Map();
  for (const path of paths) {
    let node = root;
    const parts = path.split(".");
    for (let i = 0; i < parts.length; i++) {
      const existing = node.get(parts[i]!);
      // An empty map is a leaf: the whole field. A leaf absorbs deeper paths.
      if (existing && existing.size === 0) break;
      if (i === parts.length - 1) {
        node.set(parts[i]!, new Map());
        break;
      }
      const next = existing ?? new Map();
      node.set(parts[i]!, next);
      node = next;
    }
  }
  return root;
};

type Message = Record<string, unknown>;

/**
 * `msg` with only the fields in `t` (proto field names), like a
 * `FieldMaskTree` merge: leaves copy the whole field, inner nodes recurse
 * into messages and each element of repeated messages.
 */
/** Each message type's fields by proto name, built once (pruning runs per checkpoint, per nested message). */
const byName = new WeakMap<
  IMessageType<object>,
  ReadonlyMap<string, IMessageType<object>["fields"][number]>
>();
const fieldsOf = (type: IMessageType<object>) => {
  let m = byName.get(type);
  if (m === undefined)
    byName.set(type, (m = new Map(type.fields.map((f) => [f.name, f]))));
  return m;
};

const pruneMessage = (
  type: IMessageType<object>,
  msg: Message,
  t: Tree,
): Message => {
  const out = type.create() as Message;
  const fields = fieldsOf(type);
  for (const [name, sub] of t) {
    const f = fields.get(name);
    if (!f) continue;
    const child = (v: unknown) =>
      sub.size === 0 || f.kind !== "message" || v === undefined
        ? v
        : pruneMessage(f.T() as IMessageType<object>, v as Message, sub);
    if (f.oneof) {
      const group = msg[f.oneof] as { oneofKind?: string } & Message;
      if (group?.oneofKind === f.localName)
        out[f.oneof] = {
          oneofKind: f.localName,
          [f.localName]: child(group[f.localName]),
        };
      continue;
    }
    const v = msg[f.localName];
    out[f.localName] = f.repeat && Array.isArray(v) ? v.map(child) : child(v);
  }
  return out;
};

/** `checkpoint` restricted to the compiled paths. */
const pruneTo = (checkpoint: Checkpoint, t: Tree): Checkpoint =>
  pruneMessage(
    GrpcTypes.Checkpoint as unknown as IMessageType<object>,
    checkpoint as unknown as Message,
    t,
  ) as unknown as Checkpoint;

/** `checkpoint` restricted to `paths`. */
export const prune = (
  checkpoint: Checkpoint,
  paths: readonly string[],
): Checkpoint => pruneTo(checkpoint, tree(paths));

/** One step of an ordered backfill. */
export type HistoryItem =
  | {
      readonly type: "checkpoint";
      readonly seq: bigint;
      readonly checkpoint: Checkpoint;
    }
  /** Not in the archive. */
  | { readonly type: "missing"; readonly seq: bigint }
  /** Past the archive's tip (`LEDGER_TIP`): not there yet, so not a gap. */
  | { readonly type: "notYet"; readonly seq: bigint };

/** Ordered history for a range. Replaceable for tests. */
export interface HistorySource {
  /** `[from, to)` in order; fails when the archive keeps failing (the driver retries). */
  readonly checkpoints: (
    from: bigint,
    to: bigint,
  ) => Stream.Stream<HistoryItem, ArchiveError>;
  /** The newest checkpoint history holds, if it can say: a backfill runs up to it. */
  readonly tip?: Effect.Effect<bigint, ArchiveError>;
  /** Release connections this source opened. */
  readonly close?: () => void;
  /** How long a checkpoint may be absent (with later ones held) before it is a gap (default: the driver's, 300 s). */
  readonly holeWindowMs?: number;
}

/** Largest archive response accepted (as in the Rust SDK). */
const MAX_MESSAGE_BYTES = 256 * 1024 * 1024;

/** A `SuiGrpcClient` over native gRPC (`@grpc/grpc-js`), loaded on first use. */
const defaultClient = async (
  host: string,
): Promise<{ client: SuiGrpcClient; close: () => void }> => {
  let modules;
  try {
    modules = await Promise.all([
      import("@protobuf-ts/grpc-transport"),
      import("@grpc/grpc-js"),
      import("@mysten/sui/grpc"),
    ]);
  } catch (cause) {
    throw new Error(
      "reading history needs `@grpc/grpc-js` and `@protobuf-ts/grpc-transport` (the archive has no gRPC-web): " +
        String(cause),
    );
  }
  const [{ GrpcTransport }, { ChannelCredentials }, { SuiGrpcClient: Client }] =
    modules;
  const transport = new GrpcTransport({
    host,
    channelCredentials: ChannelCredentials.createSsl(),
    // grpc-js caps responses at 4 MiB by default; checkpoints are larger at
    // times (epoch changes reach tens of MB). Same limit as the Rust SDK.
    clientOptions: {
      "grpc.max_receive_message_length": MAX_MESSAGE_BYTES,
      // HTTP/2 keepalive during calls (as the Rust SDK's 5 s / 20 s): a
      // connection that died silently (a network change, NAT rebinding: no
      // FIN or RST ever arrives) is noticed in seconds, not when the kernel
      // gives up (~15 min).
      "grpc.keepalive_time_ms": 10_000,
      "grpc.keepalive_timeout_ms": 10_000,
    },
  });
  return {
    client: new Client({ network: "mainnet", transport }),
    close: () => transport.close(),
  };
};

export interface ArchiveOptions {
  /** A `SuiGrpcClient` whose ledger service has the history (it must speak native gRPC for `archive.mainnet.sui.io`). */
  readonly client?: SuiGrpcClient;
  /** `host:port` of the archive when no `client` is given. Default: Mainnet. */
  readonly host?: string;
  /** The caller's read mask (undefined: full checkpoint). */
  readonly readMask: readonly string[] | undefined;
  /** How long a checkpoint may be absent (with later ones held) before it is a gap (default 300 s). For tests. */
  readonly holeWindowMs?: number | undefined;
  /** First wait before a short page is asked again (doubling; default 1000 ms). For tests. */
  readonly shortRetryBaseMs?: number | undefined;
  /** Each failed page attempt (it is retried after `retryInMs`): for diagnostics. */
  readonly onRetry?: ((reason: string, retryInMs: number) => void) | undefined;
  /**
   * Pages (`PAGE` checkpoints each) in flight. Default 2: each page in
   * flight holds its checkpoints in memory (with the whole checkpoint, a few
   * hundred MB a page), and more read faster only up to the link's
   * bandwidth and the decoding.
   */
  readonly concurrency?: number;
}

/** Checkpoints per `ListCheckpoints` page: the archive's maximum. */
export const PAGE = 50n;

/** `QueryEndReason.ITEM_LIMIT`: the server stopped at its item limit; resume after the cursor. */
const ITEM_LIMIT = 1;
/** `QueryEndReason.LEDGER_TIP`: the range runs past what the archive holds. */
const LEDGER_TIP = 5;

/** Longest wait for a `ListCheckpoints` call's response headers (as the Rust SDK). */
const FIRST_RESPONSE_MS = 30_000;
/**
 * Longest wait between two messages of a `ListCheckpoints` answer, at first.
 * grpc-js shows whole messages only (no byte progress), and one epoch-change
 * checkpoint is 10-20 MB: when an attempt is cut by this without bringing
 * anything, the next attempt at that page waits twice as long (up to
 * MESSAGE_DEADLINE_MAX_MS), so a large message on a slow link gets through,
 * while a stalled answer is still retried in 30 s. A dead connection is the
 * HTTP/2 keepalive's (10 s + 10 s).
 */
const MESSAGE_DEADLINE_MS = 30_000;
// 60 s at most: an attempt waiting on a hung backend must not outlive it by
// minutes (at 480 s, a 300 s archive stall left the client frozen 156 s after it cleared).
const MESSAGE_DEADLINE_MAX_MS = 60_000;
/** Asks of a short page before its absent checkpoints count as missing (~30 s): a gap is final for the caller. */
const SHORT_ASKS = 5;

const sameBytes = (a: Uint8Array, b: Uint8Array): boolean =>
  a.length === b.length && a.every((x, i) => x === b[i]);

/** Between attempts at one page: 250 ms, doubling, at most 4 s. */
const retrySchedule = Schedule.min([
  Schedule.exponential("250 millis"),
  Schedule.spaced("4 seconds"),
]);

/** History from the archive, in order, `concurrency` pages ahead. */
export const archiveHistory = (options: ArchiveOptions): HistorySource => {
  const p = plan(options.readMask);
  const kept = tree(p.prune);
  // Pages are placed by sequence number: always ask for it (pruned again if unwanted).
  const paths = p.request.some((r) => covers(r, "sequence_number"))
    ? [...p.request]
    : [...p.request, "sequence_number"];
  const concurrency = Math.max(1, options.concurrency ?? 2);
  let client:
    Promise<{ client: SuiGrpcClient; close?: () => void }> | undefined =
    options.client ? Promise.resolve({ client: options.client }) : undefined;

  /** Drop the default client (closing its connection): the next call opens a new one. A caller's own client is kept. */
  const resetClient = () => {
    if (options.client) return;
    const old = client;
    client = undefined;
    void old?.then((c) => c.close?.()).catch(() => {});
  };

  /** The client, created on first use (a failed creation is retried next time). */
  const getClient = Effect.tryPromise({
    try: () =>
      (client ??= defaultClient(options.host ?? MAINNET_ARCHIVE).catch((e) => {
        client = undefined;
        throw e;
      })),
    catch: (cause) => new ArchiveError({ message: String(cause), cause }),
  });

  /**
   * One `ListCheckpoints` call for `[start, end)`, continued from its cursor
   * if the server stops at an item limit of its own; each checkpoint goes to
   * `onCheckpoint` as it arrives (it is released downstream as soon as
   * everything before it in the page is known, so a slow link delivers
   * steadily, and a retry continues after what arrived). Succeeds with the
   * stream's `QueryEnd` reason; a stream without one, a cursor that doesn't
   * move, or more checkpoints than the range holds are errors.
   */
  const list = (
    start: bigint,
    end: bigint,
    onCheckpoint: (c: Checkpoint) => void,
    messageDeadlineMs: number = MESSAGE_DEADLINE_MS,
  ): Effect.Effect<number, ArchiveError> =>
    Effect.gen(function* () {
      const { client: c } = yield* getClient;
      const len = Number(end - start);
      return yield* Effect.tryPromise({
        try: async (signal) => {
          let after: Uint8Array | undefined;
          // Checkpoints this call received (re-reads included): bounded by the range.
          let received = 0;
          // To the response headers: FIRST_RESPONSE_MS; then between two messages:
          // MESSAGE_DEADLINE_MS (a stalled answer is aborted).
          const idle = new AbortController();
          signal.addEventListener("abort", () => idle.abort(signal.reason), {
            once: true,
          });
          let idleTimer: ReturnType<typeof setTimeout> | undefined;
          // Why the idle timer aborted the call (grpc-js reports only "Cancelled on client").
          let stalled: string | undefined;
          const touch = (ms: number, what: string) => {
            clearTimeout(idleTimer);
            idleTimer = setTimeout(() => {
              stalled = `stalled: ${what} for ${ms / 1000} s`;
              idle.abort(new Error(stalled));
            }, ms);
          };
          try {
            for (;;) {
              touch(FIRST_RESPONSE_MS, "no response");
              const call = c.ledgerService.listCheckpoints(
                {
                  startCheckpoint: start,
                  endCheckpoint: end,
                  readMask: { paths },
                  options: { limit: len, ...(after ? { after } : {}) },
                },
                // The whole call: generous (a slow link isn't cut off mid-page); stalls are the idle timer's.
                { abort: idle.signal, timeout: 600_000 },
              );
              void call.headers?.then?.(
                () => touch(messageDeadlineMs, "no message"),
                () => {},
              );
              let reason: number | undefined;
              let cursor: Uint8Array | undefined;
              let last: bigint | undefined;
              const before = received;
              for await (const m of call.responses) {
                touch(messageDeadlineMs, "no message");
                if (m.checkpoint) {
                  if (received >= len) throw new Error(`more than ${len} checkpoints`);
                  received++;
                  last = m.checkpoint.sequenceNumber;
                  onCheckpoint(m.checkpoint);
                }
                if (m.watermark?.cursor) cursor = m.watermark.cursor;
                if (m.end) reason = m.end.reason ?? 0;
              }
              // `end` is set exactly once on a stream that finished: one without it was cut short.
              if (reason === undefined) throw new Error("ended without QueryEnd");
              if (reason !== ITEM_LIMIT || (last !== undefined && last >= end - 1n)) return reason;
              // Continue after the cursor, only if it moved and brought something.
              if (cursor === undefined || (after !== undefined && sameBytes(cursor, after)) || received === before)
                throw new Error("item limit without progress");
              after = cursor;
            }
          } catch (e) {
            throw stalled !== undefined ? new Error(stalled) : e;
          } finally {
            clearTimeout(idleTimer);
          }
        },
        catch: (cause) => {
          const message = cause instanceof Error ? cause.message : String(cause);
          // A call that timed out or stalled may be on a dead connection: the next attempt gets a new one.
          if (/deadline|stalled|abort|cancel/i.test(message)) resetClient();
          return new ArchiveError({ message: `checkpoints ${start}..${end - 1n}: ${message}`, cause });
        },
      }).pipe(
        // Enforced here too, whatever the transport does with `timeout`
        // (interrupting aborts the call): nothing waits without a deadline.
        Effect.timeoutOrElse({
          duration: "610 seconds",
          orElse: () =>
            Effect.fail(new ArchiveError({ message: `checkpoints ${start}..${end - 1n}: no answer in 610 s` })),
        }),
      );
    });

  /**
   * One page, into `emit` in order, each checkpoint (filled and pruned) as
   * soon as everything before it is known: failures retried (6 attempts in
   * all, each continuing after what arrived, each reported to `onRetry`);
   * absent checkpoints (a short page, or holes below what a `LEDGER_TIP`
   * answer held) asked again `SHORT_ASKS` times (~30 s) before they are
   * `missing` (the driver decides when missing is a gap); after the last one
   * held of a `LEDGER_TIP` answer: `notYet`, and the page ends there.
   */
  const page = (
    start: bigint,
    end: bigint,
    emit: (item: HistoryItem) => void,
  ): Effect.Effect<void, ArchiveError> =>
    Effect.gen(function* () {
      // Everything before `next` is released; `pending`: arrived after it.
      let next = start;
      const pending = new Map<bigint, Checkpoint>();
      const release = (seq: bigint, checkpoint: Checkpoint) => {
        if (p.fill) fillObjectTypes(checkpoint);
        emit({ type: "checkpoint", seq, checkpoint: pruneTo(checkpoint, kept) });
      };
      const onCheckpoint = (checkpoint: Checkpoint) => {
        const seq = checkpoint.sequenceNumber;
        if (seq === undefined || seq < start || seq >= end)
          throw new Error(`archive returned checkpoint ${seq} for ${start}..${end - 1n}`);
        if (seq < next || pending.has(seq)) return; // a re-read
        pending.set(seq, checkpoint);
        for (let c = pending.get(next); c !== undefined; c = pending.get(next)) {
          pending.delete(next);
          release(next, c);
          next++;
        }
      };
      /** Absent ones are missing, up to the last held; after it, missing too, or (`pastTip`) not there yet. */
      const giveUp = (pastTip: boolean) => {
        for (; next < end; next++) {
          const c = pending.get(next);
          if (c !== undefined) {
            pending.delete(next);
            release(next, c);
          } else if (pastTip && pending.size === 0) {
            emit({ type: "notYet", seq: next });
            next = end;
            return;
          } else emit({ type: "missing", seq: next });
        }
      };
      let errors = 0;
      let short = 0;
      let backoffMs = 250;
      let messageDeadlineMs = MESSAGE_DEADLINE_MS;
      for (;;) {
        if (next >= end) return;
        const from = next;
        const held = pending.size;
        const result = yield* Effect.result(list(from, end, onCheckpoint, messageDeadlineMs));
        if (next >= end) return;
        // Cut between messages with nothing new: perhaps one large message on a slow link; give it longer.
        const progressed = next > from || pending.size > held;
        if (result._tag === "Failure" && !progressed && /stalled: no message/.test(result.failure.message))
          messageDeadlineMs = Math.min(messageDeadlineMs * 2, MESSAGE_DEADLINE_MAX_MS);
        else if (progressed) messageDeadlineMs = MESSAGE_DEADLINE_MS;
        let waitMs: number;
        if (result._tag === "Success") {
          // Past the archive's tip, and nothing held after the gap: the rest isn't there yet.
          if (result.success === LEDGER_TIP && pending.size === 0) {
            emit({ type: "notYet", seq: next });
            return;
          }
          if (short >= SHORT_ASKS) return giveUp(result.success === LEDGER_TIP);
          short++;
          waitMs = (options.shortRetryBaseMs ?? 1000) * 2 ** (short - 1); // 1, 2, 4, 8, 16 s
          yield* Effect.sync(() =>
            options.onRetry?.(`checkpoints ${from}..${end - 1n}: ${from} absent (${pending.size} after it held); asking again (${short} of ${SHORT_ASKS})`, waitMs),
          );
        } else {
          if (++errors >= 6) return yield* Effect.fail(result.failure);
          waitMs = backoffMs;
          backoffMs = Math.min(backoffMs * 2, 4_000);
          yield* Effect.sync(() => options.onRetry?.(`${result.failure.message} (attempt ${errors} of 6)`, waitMs));
        }
        yield* Effect.sleep(waitMs);
      }
    });
  return {
    ...(options.holeWindowMs === undefined ? {} : { holeWindowMs: options.holeWindowMs }),
    close() {
      void client?.then((c) => c.close?.()).catch(() => {});
      client = undefined;
    },
    tip: getClient.pipe(
      Effect.flatMap(({ client: c }) =>
        Effect.tryPromise({
          try: (signal) =>
            c.ledgerService.getServiceInfo(
              {},
              { abort: signal, timeout: 5_000 },
            ).response,
          catch: (cause) =>
            new ArchiveError({
              message: `archive tip: ${String(cause)}`,
              cause,
            }),
        }).pipe(
          Effect.timeoutOrElse({
            duration: "5 seconds",
            orElse: () =>
              Effect.fail(
                new ArchiveError({ message: "archive tip: no answer in 5 s" }),
              ),
          }),
        ),
      ),
      Effect.retry({ schedule: retrySchedule, times: 2 }),
      Effect.flatMap((info) =>
        info.checkpointHeight === undefined
          ? Effect.fail(
              new ArchiveError({
                message: "archive tip: no checkpoint height",
              }),
            )
          : Effect.succeed(info.checkpointHeight),
      ),
    ),
    // Pages in order, `concurrency` in flight, each read by its own fiber
    // (so it keeps reading while an earlier one is consumed) and handed over
    // item by item; a failure ends the stream, and ending it interrupts the
    // pages in flight.
    checkpoints: (from, to) =>
      Stream.callback<HistoryItem, ArchiveError>(
        (out) =>
          Effect.gen(function* () {
            const inflight: Queue.Queue<HistoryItem, ArchiveError | Cause.Done>[] = [];
            let nextStart = from;
            for (;;) {
              while (inflight.length < concurrency && nextStart < to) {
                const start = nextStart;
                const end = start + PAGE < to ? start + PAGE : to;
                nextStart = end;
                const q = yield* Queue.unbounded<HistoryItem, ArchiveError | Cause.Done>();
                yield* page(start, end, (item) => void Queue.offerUnsafe(q, item)).pipe(
                  Effect.matchEffect({ onSuccess: () => Queue.end(q), onFailure: (e) => Queue.fail(q, e) }),
                  Effect.forkScoped,
                );
                inflight.push(q);
              }
              const q = inflight[0];
              if (q === undefined) return yield* Queue.end(out);
              const r = yield* Effect.result(Queue.take(q));
              if (r._tag === "Success") yield* Queue.offer(out, r.success);
              else if (Cause.isDone(r.failure)) inflight.shift();
              else return yield* Queue.fail(out, r.failure);
            }
          }),
        { bufferSize: 16, strategy: "suspend" },
      ),
  };
};
