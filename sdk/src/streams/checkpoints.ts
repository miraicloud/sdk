/**
 * The checkpoint stream (`GET /v1/stream/checkpoints`): every Sui
 * checkpoint, complete, as the node's own `SubscribeCheckpoints` responses,
 * zstd-compressed frames (see `frame.ts`). The stream holds the last few
 * minutes; older positions are read from the archive
 * (`archive.mainnet.sui.io`) by this client, which then hands over to the
 * stream, without holes or duplicates.
 */

import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { GrpcTypes } from "@mysten/sui/grpc";
import { Data, Effect, Exit, Result, Stream } from "effect";
import { type HttpClient, HttpClientRequest, type HttpClientResponse } from "effect/http";
import type { Auth } from "../auth.js";
import { Forbidden, Invalid, NotFound, QuotaExceeded, Unauthorized } from "@miraicloud/core/errors";
import { archiveHistory, prune } from "./archive.js";
import { type CheckpointLike, type Decoder, Diagnostic, drive, type GapLike, type LiveSource, Renew, Retry, TooOld } from "./driver.js";
import { CONTENT_TYPE, type Frame, FrameParser } from "./frame.js";
import { resolveMask } from "./mask.js";
import { authorize } from "./request.js";

export { Diagnostic };

/** A checkpoint as Sui's gRPC API returns it (`sui.rpc.v2.Checkpoint`, what `SuiGrpcClient` returns). */
export type Checkpoint = GrpcTypes.Checkpoint;

/** Where the checkpoint stream is served, under the endpoint. */
export const CHECKPOINT_STREAM_PATH = "/v1/stream/checkpoints";

/** What `checkpoints` yields, in strict sequence order. */
export type CheckpointItem = Data.TaggedEnum<{
  /** The next checkpoint. `seq` is set even when the read mask leaves out `sequence_number`. */
  Checkpoint: { readonly seq: bigint; readonly checkpoint: Checkpoint };
  /** Checkpoints `from..=to` can't be delivered (lost, or older than the stream without an archive); the stream goes on with `to + 1`. */
  Gap: { readonly from: bigint; readonly to: bigint };
}>;
export const CheckpointItem = Data.taggedEnum<CheckpointItem>();

/**
 * What `rawCheckpoints` yields: like `CheckpointItem`, each checkpoint as
 * its encoded protobuf `sui.rpc.v2.Checkpoint`, not decoded (a view of the
 * stream's buffer; re-encoded for checkpoints read from the archive).
 */
export type RawCheckpointItem = Data.TaggedEnum<{
  Checkpoint: { readonly seq: bigint; readonly data: Uint8Array };
  Gap: { readonly from: bigint; readonly to: bigint };
}>;
export const RawCheckpointItem = Data.taggedEnum<RawCheckpointItem>();

/**
 * What ends a checkpoint stream: a refusal (a missing or wrong key, no
 * `stream:read`, over quota) or invalid options. Everything else is retried.
 */
export type CheckpointStreamError = Unauthorized | Forbidden | Invalid | NotFound | QuotaExceeded;

/** Where positions older than the stream are read. */
export interface ArchiveConfig {
  /**
   * A `SuiGrpcClient` with the history. Default: one over native gRPC
   * (`@grpc/grpc-js`) to `host`; the archive has no gRPC-web, so the
   * default needs Node or Bun, not a browser.
   */
  readonly client?: SuiGrpcClient | undefined;
  /** `host:port` when no `client` is given. Default `MAINNET_ARCHIVE`. */
  readonly host?: string | undefined;
  /**
   * Pages (50 checkpoints each) in flight. Default 2. Each page in flight is
   * held in memory, decoded: with the whole checkpoint (no read mask) a few
   * hundred MB a page at today's Mainnet sizes; with a read mask naming the
   * fields you need, a few MB.
   */
  readonly concurrency?: number | undefined;
}

export interface CheckpointStreamOptions {
  /** The first checkpoint to deliver (inclusive). Default: the live tip. */
  readonly from?: bigint | number | undefined;
  /** Resume after this checkpoint (e.g. the last `seq`, or a gap's `to`, you processed). At most one of `from` and `after`. */
  readonly after?: bigint | number | undefined;
  /**
   * `sui.rpc.v2.Checkpoint` field paths to keep, applied by this SDK: the
   * stream sends every client the whole checkpoint (every field, BCS
   * encodings included), and the mask prunes it, live and from the archive
   * alike. Default (and what `*` and parent paths stand for): the whole
   * checkpoint. The BCS encodings (`BCS_PATHS`, e.g.
   * `transactions.effects.bcs`) can be named like any other field. Unknown
   * paths and fields no node renders consistently (event and object JSON,
   * object display) fail the stream with `Invalid`.
   */
  readonly readMask?: ReadonlyArray<string> | undefined;
  /** History older than the stream. Default: the Mainnet archive. `false`: such positions are reported as a `Gap`. */
  readonly archive?: ArchiveConfig | false | undefined;
  /** Reconnect when no bytes arrive for this long (keepalives come every 15 s). Default 45 000. */
  readonly stallTimeoutMs?: number | undefined;
  /**
   * Decompresses a body the HTTP client left zstd-compressed. Default:
   * `node:zlib` (Node ≥ 22.15, Bun); pass one (e.g. a WASM zstd as a
   * `TransformStream`) on runtimes without it.
   */
  readonly zstdDecoder?: ZstdDecoder | undefined;
  /** Connections, backfills and reconnects, for logging and metrics. */
  readonly onDiagnostic?: ((d: Diagnostic) => void) | undefined;
}

// ---------------------------------------------------------------------------
// Decoding

type Typed = Extract<CheckpointItem, { _tag: "Checkpoint" }>;
type TypedGap = Extract<CheckpointItem, { _tag: "Gap" }>;
type Raw = Extract<RawCheckpointItem, { _tag: "Checkpoint" }>;
type RawGap = Extract<RawCheckpointItem, { _tag: "Gap" }>;

/** A malformed frame is a broken connection (retried). */
const malformed = (message: string, cause?: unknown) => new Retry({ message, cause });

/** Checkpoints as `GrpcTypes.Checkpoint`. */
export const decoded: Decoder<Typed, TypedGap> = {
  frame: (seq, data) => {
    let checkpoint: Checkpoint;
    try {
      checkpoint = GrpcTypes.Checkpoint.fromBinary(data);
    } catch (cause) {
      throw malformed(`checkpoint ${seq}: undecodable protobuf`, cause);
    }
    if (checkpoint.sequenceNumber !== undefined && checkpoint.sequenceNumber !== seq) {
      throw malformed(`cursor ${seq} != sequenceNumber ${checkpoint.sequenceNumber}`);
    }
    return CheckpointItem.Checkpoint({ seq, checkpoint });
  },
  message: (seq, checkpoint) => CheckpointItem.Checkpoint({ seq, checkpoint }),
  gap: (from, to) => CheckpointItem.Gap({ from, to }),
};

/** Checkpoints left as encoded bytes (never decoded from the stream). */
export const raw: Decoder<Raw, RawGap> = {
  frame: (seq, data) => RawCheckpointItem.Checkpoint({ seq, data }),
  message: (seq, checkpoint) => RawCheckpointItem.Checkpoint({ seq, data: GrpcTypes.Checkpoint.toBinary(checkpoint) }),
  gap: (from, to) => RawCheckpointItem.Gap({ from, to }),
};

/** A protobuf varint at `p`: [value, next position]; throws past the end. */
const varintAt = (b: Uint8Array, p: number): [number, number] => {
  let r = 0;
  for (let s = 0; s < 64; s += 7) {
    if (p >= b.length) throw new RangeError("truncated varint");
    const x = b[p++]!;
    r += (x & 0x7f) * 2 ** s;
    if (x < 0x80) return [r, p];
  }
  throw new RangeError("varint too long");
};

/**
 * The top-level fields of an encoded message whose numbers are in `keep`,
 * copied out in order (still a valid encoding of the same message type).
 * Skipping the rest unparsed is what makes a narrow mask cheap: a Mainnet
 * checkpoint is ~0.5 MB, and `sequence_number,summary` is ~0.5 KB of it
 * (decoding 1.8 ms -> 0.013 ms per checkpoint, Node 24).
 */
export const keepFields = (b: Uint8Array, keep: ReadonlySet<number>): Uint8Array => {
  const spans: [number, number][] = [];
  let size = 0;
  for (let p = 0; p < b.length; ) {
    const start = p;
    const [key, q] = varintAt(b, p);
    p = q;
    switch (key & 7) {
      case 0:
        p = varintAt(b, p)[1];
        break;
      case 1:
        p += 8;
        break;
      case 2: {
        const [n, r] = varintAt(b, p);
        p = r + n;
        break;
      }
      case 5:
        p += 4;
        break;
      default:
        throw new RangeError(`wire type ${key & 7}`);
    }
    if (p > b.length) throw new RangeError("truncated field");
    if (keep.has(Math.floor(key / 8))) {
      spans.push([start, p]);
      size += p - start;
    }
  }
  const out = new Uint8Array(size);
  let i = 0;
  for (const [a, z] of spans) {
    out.set(b.subarray(a, z), i);
    i += z - a;
  }
  return out;
};

/** The top-level `sui.rpc.v2.Checkpoint` field numbers `paths` reach into. */
const topLevelFields = (paths: ReadonlyArray<string>): ReadonlySet<number> => {
  const fields = (GrpcTypes.Checkpoint as unknown as { fields: ReadonlyArray<{ no: number; name: string }> }).fields;
  return new Set(paths.flatMap((path) => fields.filter((f) => f.name === path.split(".")[0]).map((f) => f.no)));
};

/**
 * `decode` with each stream checkpoint pruned to `paths` (the read mask):
 * only the top-level fields the mask reaches are decoded (the rest are
 * skipped as bytes), pruned, then delivered like an archive checkpoint
 * (typed, or re-encoded for raw streams).
 */
export const masked = <C extends CheckpointLike, G extends GapLike>(decode: Decoder<C, G>, paths: ReadonlyArray<string>): Decoder<C, G> => {
  const keep = topLevelFields(paths);
  return {
    ...decode,
    frame: (seq, data) => {
      let narrow: Uint8Array;
      try {
        narrow = keepFields(data, keep);
      } catch (cause) {
        throw malformed(`checkpoint ${seq}: undecodable protobuf`, cause);
      }
      return decode.message(seq, prune(decoded.frame(seq, narrow).checkpoint, paths));
    },
  };
};

// ---------------------------------------------------------------------------
// zstd

/**
 * A streaming zstd decoder: compressed bytes in, decompressed bytes out (a
 * `TransformStream` is one). Used when the HTTP client hands over the body
 * still compressed.
 */
export type ZstdDecoder = () => ReadableWritablePair<Uint8Array, Bytes>;

/** Body bytes. */
type Bytes = Uint8Array<ArrayBuffer>;

type Runtime = {
  Bun?: { zstdDecompressSync?: unknown };
  process?: { versions?: Record<string, string | undefined>; getBuiltinModule?: (id: string) => unknown };
};
const runtime = globalThis as Runtime;
const builtin = <T>(id: string): T | undefined => runtime.process?.getBuiltinModule?.(id) as T | undefined;

const atLeast = (version: string, major: number, minor: number) => {
  const [a = 0, b = 0] = version.split(".").map(Number);
  return a > major || (a === major && b >= minor);
};

/** zstd from `node:zlib` (Node ≥ 22.15, Bun) as a web stream pair, or undefined where the runtime has none. */
export const nativeZstdDecoder = (): ZstdDecoder | undefined => {
  const zlib = builtin<{ createZstdDecompress?: () => unknown }>("node:zlib");
  const Duplex = builtin<{ Duplex?: { toWeb?: (duplex: unknown) => ReadableWritablePair<Uint8Array, Bytes> } }>("node:stream")?.Duplex;
  if (typeof zlib?.createZstdDecompress !== "function" || typeof Duplex?.toWeb !== "function") return undefined;
  return () => Duplex.toWeb!(zlib.createZstdDecompress!());
};

/** Whether the global `fetch` decodes zstd itself (Bun; Node with undici ≥ 7.11). */
export const fetchDecodesZstd = (): boolean => {
  if (runtime.Bun) return typeof runtime.Bun.zstdDecompressSync === "function";
  const undici = runtime.process?.versions?.undici;
  const zlib = builtin<{ createZstdDecompress?: unknown }>("node:zlib");
  return undici !== undefined && atLeast(undici, 7, 11) && typeof zlib?.createZstdDecompress === "function";
};

/** Every zstd frame starts with these bytes; a stream frame (kind 0–2) can't. */
const ZSTD_MAGIC = [0x28, 0xb5, 0x2f, 0xfd] as const;

/** Reads until the first 4 bytes are known (or the body ends). */
const readHead = async (reader: ReadableStreamDefaultReader<Uint8Array>): Promise<Bytes> => {
  let head = new Uint8Array(0);
  while (head.length < ZSTD_MAGIC.length) {
    const r = await reader.read();
    if (r.done) break;
    const joined = new Uint8Array(head.length + r.value.length);
    joined.set(head);
    joined.set(r.value, head.length);
    head = joined;
  }
  return head;
};

/** `head`, then the rest of `reader`. Pulled on demand, so backpressure reaches the network. */
const prepend = (head: Bytes, reader: ReadableStreamDefaultReader<Uint8Array>): ReadableStream<Bytes> =>
  new ReadableStream<Bytes>(
    {
      start(controller) {
        if (head.length > 0) controller.enqueue(head);
      },
      async pull(controller) {
        const r = await reader.read();
        if (r.done) controller.close();
        else controller.enqueue(r.value as Bytes);
      },
      cancel: (reason) => reader.cancel(reason),
    },
    { highWaterMark: 0 },
  );

// ---------------------------------------------------------------------------
// Connections

export interface SourceOptions {
  readonly client: HttpClient.HttpClient;
  /** The endpoint, without a trailing slash. */
  readonly endpoint: string;
  readonly auth: Auth;
  /**
   * Decompresses a body the client left zstd-compressed (detected by the
   * zstd magic number). Without one, such a body is a defect.
   */
  readonly zstdDecoder: ZstdDecoder | undefined;
  /** No bytes (keepalives included) for this long → reconnect. */
  readonly stallTimeoutMs: number;
}

/**
 * Attempts a `404` gets before the stream has once answered (with the
 * reconnect backoff between them: 15–30 s in all): a process started during
 * a deploy waits it out; a wrong URL is final after them.
 */
export const NOT_FOUND_ATTEMPTS = 6;

/**
 * A refused connection: final (`CheckpointStreamError`), too old (`TooOld`),
 * or worth another attempt (`Retry`). `retryNotFound`: a `404` is retried
 * rather than final (the subscription has been served before, so it is the
 * service's transient state, a deploy or a route moving; or it hasn't had
 * `NOT_FOUND_ATTEMPTS` yet).
 */
const refusal = (response: HttpClientResponse.HttpClientResponse, retryNotFound: boolean): Effect.Effect<never, CheckpointStreamError | TooOld | Retry> =>
  Effect.gen(function* () {
    const text = (yield* response.text.pipe(Effect.orElseSucceed(() => ""))).slice(0, 500);
    // The gateway's refusals are JSON (`{ _tag, message }`); the stream's own are plain text.
    let tag: unknown;
    let detail: unknown;
    try {
      const body = JSON.parse(text) as unknown;
      if (typeof body === "object" && body !== null) ({ _tag: tag, message: detail } = body as { _tag?: unknown; message?: unknown });
    } catch {}
    const message = typeof detail === "string" ? detail : text.trim() !== "" ? text.trim() : `HTTP ${response.status}`;
    const retryAfter = Number(response.headers["retry-after"]);
    const retryAfterMs = Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter * 1000 : undefined;
    switch (response.status) {
      case 400:
        return yield* new Invalid({ message });
      case 401:
        return yield* new Unauthorized({ message });
      case 403:
        return yield* new Forbidden({ message });
      case 404:
        if (retryNotFound) return yield* new Retry({ message: `HTTP 404: ${message}`, retryAfterMs });
        return yield* new NotFound({ message });
      case 410: {
        // Older than the stream holds, which starts here.
        const oldest = response.headers["oldest-checkpoint"]?.trim();
        if (oldest !== undefined && /^\d+$/.test(oldest)) return yield* new TooOld({ message, oldest: BigInt(oldest) });
        return yield* new Retry({ message: `HTTP 410: ${message}` });
      }
      case 429:
        if (tag === "QuotaExceeded") return yield* new QuotaExceeded({ message });
        return yield* new Retry({ message: `HTTP 429: ${message}`, retryAfterMs });
      default:
        return yield* new Retry({ message: `HTTP ${response.status}: ${message}`, retryAfterMs });
    }
  });

/** The checkpoint stream over HTTP; `decode` picks typed or raw checkpoints. */
export const httpSource = <C extends CheckpointLike, G extends GapLike>(options: SourceOptions, decode: Decoder<C, G>): LiveSource<C, G, CheckpointStreamError> => {
  let served = false;
  let notFound = 0;
  return {
    connect: (after) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const url = new URL(`${options.endpoint}${CHECKPOINT_STREAM_PATH}`);
          if (after !== undefined) url.searchParams.set("from", (after + 1n).toString());
          const { request, renew } = yield* authorize(
            HttpClientRequest.get(url).pipe(
              HttpClientRequest.setHeaders({
                accept: CONTENT_TYPE,
                // The only encoding the stream is served in.
                "accept-encoding": "zstd",
              }),
            ),
            options.auth,
            url.origin,
            url.pathname + url.search,
            options,
          ).pipe(
            // A refused delegation is final; a rate limit, an outage or a failing signer is retried.
            Effect.catchTags({
              RateLimited: (e) => Effect.fail(new Retry({ message: `token exchange: ${e.message}` })),
              Unavailable: (e) => Effect.fail(new Retry({ message: `token exchange: ${e.message}` })),
              SignerError: (e) => Effect.fail(new Retry({ message: `token exchange: ${e.message}` })),
            }),
          );
          const stall = options.stallTimeoutMs;
          const noData = () => new Retry({ message: `no data for ${stall} ms` });
          const failed = (what: string) => (cause: unknown) => (cause instanceof Retry ? cause : new Retry({ message: `${what}: ${messageOf(cause)}`, cause }));
          // The stall timer runs only while waiting for the network (headers,
          // or the next chunk while the consumer is pulling), never while the
          // consumer holds an item. Interruption aborts the request.
          const response = yield* options.client.execute(request).pipe(
            Effect.mapError(failed("connect failed")),
            Effect.timeoutOrElse({ duration: stall, orElse: () => Effect.fail(noData()) }),
          );
          if (response.status !== 200) return yield* refusal(response, served || (response.status === 404 && ++notFound < NOT_FOUND_ATTEMPTS));
          served = true;
          const reader = Stream.toReadableStream(response.stream).getReader();
          // The client may or may not have decoded the zstd body (Bun's fetch
          // and newer undici do; older undici and other clients pass it on).
          const head = yield* Effect.tryPromise({ try: () => readHead(reader), catch: failed("read failed") }).pipe(
            Effect.timeoutOrElse({ duration: stall, orElse: () => Effect.fail(noData()) }),
            Effect.onExit((exit) => (Exit.isSuccess(exit) ? Effect.void : Effect.promise(() => reader.cancel().catch(() => {})))),
          );
          let bytes = prepend(head, reader);
          if (ZSTD_MAGIC.every((b, i) => head[i] === b)) {
            if (!options.zstdDecoder) {
              yield* Effect.promise(() => bytes.cancel().catch(() => {}));
              return yield* Effect.die(new TypeError("the checkpoint stream is zstd-compressed and neither the HTTP client nor this runtime decodes zstd: pass `zstdDecoder`"));
            }
            // Decoders emit fresh (never shared-memory) buffers.
            bytes = bytes.pipeThrough(options.zstdDecoder()) as ReadableStream<Bytes>;
          }
          const parser = new FrameParser();
          const items = (chunk: Uint8Array): (C | G)[] => {
            let frames: Frame[];
            try {
              frames = parser.push(chunk);
            } catch (cause) {
              throw malformed(`malformed frame: ${messageOf(cause)}`, cause);
            }
            const out: (C | G)[] = [];
            for (const frame of frames) {
              if (frame._tag === "Checkpoint") out.push(decode.frame(frame.seq, frame.checkpoint));
              else if (frame._tag === "Gap") out.push(decode.gap(frame.from, frame.to));
            }
            return out;
          };
          // Reading ends (and cancels the body) when the stream does.
          const read = Stream.fromReadableStream({ evaluate: () => bytes, onError: failed("read failed") }).pipe(
            Stream.timeoutOrElse({ duration: stall, orElse: () => Stream.fail(noData()) }),
            Stream.mapEffect((chunk) => Effect.try({ try: () => items(chunk), catch: failed("malformed frame") })),
            Stream.flattenIterable,
          );
          // A delegated key: reconnect once a fresh one exists (resuming after the cursor: nothing lost).
          if (renew === undefined) return read;
          return read.pipe(Stream.interruptWhen(Effect.andThen(renew, Effect.fail(new Renew({ message: "renewing the delegated key" })))));
        }),
      ),
  };
};

const messageOf = (cause: unknown): string => (cause instanceof Error ? cause.message : typeof cause === "object" && cause !== null && "message" in cause ? String(cause.message) : String(cause));

// ---------------------------------------------------------------------------
// Subscriptions

const BACKOFF_BASE_MS = 1_000;
const BACKOFF_MAX_MS = 30_000;
const DEFAULT_STALL_MS = 45_000;

/** Options, validated. */
interface Resolved {
  readonly start: bigint | undefined;
  readonly stallTimeoutMs: number;
  readonly readMask: readonly string[] | undefined;
}

const toBigInt = (n: bigint | number, name: string): Result.Result<bigint, Invalid> => {
  if (typeof n === "bigint") return n >= 0n ? Result.succeed(n) : Result.fail(new Invalid({ message: `\`${name}\` must not be negative` }));
  return Number.isSafeInteger(n) && n >= 0 ? Result.succeed(BigInt(n)) : Result.fail(new Invalid({ message: `\`${name}\` must be a checkpoint number, got ${n}` }));
};

/** Validate `options` (no I/O). */
export const resolve = (o: CheckpointStreamOptions): Result.Result<Resolved, Invalid> =>
  Result.gen(function* () {
    if (o.from !== undefined && o.after !== undefined) return yield* Result.fail(new Invalid({ message: "set at most one of `from` and `after`" }));
    const start = o.from !== undefined ? yield* toBigInt(o.from, "from") : o.after !== undefined ? (yield* toBigInt(o.after, "after")) + 1n : undefined;
    const stallTimeoutMs = o.stallTimeoutMs ?? DEFAULT_STALL_MS;
    if (!(stallTimeoutMs > 0)) return yield* Result.fail(new Invalid({ message: "`stallTimeoutMs` must be positive" }));
    const readMask = yield* resolveMask(o.readMask);
    return { start, stallTimeoutMs, readMask };
  });

export interface Connection {
  readonly client: HttpClient.HttpClient;
  /** The endpoint, without a trailing slash. */
  readonly endpoint: string;
  readonly auth: Auth;
}

/**
 * The checkpoint stream for `options`: the one implementation behind
 * `checkpoints` and `rawCheckpoints`. Each run is one subscription; the
 * connection and the archive client close with it.
 */
export const subscribe = <C extends CheckpointLike, G extends GapLike>(
  connection: Connection,
  options: CheckpointStreamOptions,
  decode: Decoder<C, G>,
): Stream.Stream<C | G, CheckpointStreamError> =>
  Stream.suspend(() => {
    const resolved = resolve(options);
    if (Result.isFailure(resolved)) return Stream.fail(resolved.failure);
    const r = resolved.success;
    const archive = options.archive ?? {};
    const history = archive === false ? undefined : archiveHistory({ client: archive.client, host: archive.host, concurrency: archive.concurrency, readMask: r.readMask });
    const live = httpSource(
      { ...connection, zstdDecoder: options.zstdDecoder ?? nativeZstdDecoder(), stallTimeoutMs: r.stallTimeoutMs },
      r.readMask ? masked(decode, r.readMask) : decode,
    );
    return drive({
      live,
      decode,
      history,
      startAt: r.start,
      onDiagnostic: options.onDiagnostic,
      backoffBaseMs: BACKOFF_BASE_MS,
      backoffMaxMs: BACKOFF_MAX_MS,
    }).pipe(Stream.ensuring(Effect.sync(() => history?.close?.())));
  });

export const checkpoints = (connection: Connection, options: CheckpointStreamOptions = {}): Stream.Stream<CheckpointItem, CheckpointStreamError> =>
  subscribe(connection, options, decoded);

export const rawCheckpoints = (connection: Connection, options: CheckpointStreamOptions = {}): Stream.Stream<RawCheckpointItem, CheckpointStreamError> =>
  subscribe(connection, options, raw);
