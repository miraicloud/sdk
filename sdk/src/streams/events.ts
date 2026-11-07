/**
 * The event streams: every checkpoint (`GET /v1/checkpoints/stream`), or
 * the transactions that match a filter (`GET /v1/transactions/stream`), as
 * the HTTP read API's JSON (`Checkpoint`, `Transaction`), over Server-Sent
 * Events. Needs a key with `stream:read`.
 *
 * Connections are kept up for you: a dropped or stalled connection, or one
 * the service ends, reconnects and resumes after the last event received
 * (`Last-Event-ID`), without holes or duplicates. Each event carries its
 * `cursor`: keep one to resume later (`{ cursor }`) across restarts.
 *
 * What a stream can't resume from:
 * - **A gap** (`Gap { from, to }`): checkpoints the stream will never carry.
 *   The stream goes on after it; read them from `/v1/checkpoints/:n` (and
 *   their transactions from `/v1/transactions/:digest`) if you need them.
 * - **`Gone { from, oldest }`**: the start is older than the stream holds
 *   (a few minutes). The stream fails; read `from` … `oldest - 1` from
 *   `/v1/checkpoints/:n` (immutable, cached), then stream again with
 *   `{ from: oldest }`.
 *
 * Other refusals end the stream with the API's errors: `Unauthorized`,
 * `Forbidden` (e.g. no `stream:read`), `Invalid` (a malformed filter),
 * `QuotaExceeded`. Anything temporary is retried with backoff.
 *
 * Events' JSON is the read API's, byte for byte, with two additions in
 * transactions: each event's `bcs` (the Move value's BCS bytes) is always
 * present (`/v1/transactions/:digest` sends it with `?include=bcs`), and
 * `index` (the position in the checkpoint) is always set (the read API has
 * null). Move events' `fields` are rendered as the read API renders them.
 */

import { Data, Duration, Effect, Option, Schedule, Stream } from "effect";
import { type HttpClient, HttpClientRequest, type HttpClientResponse } from "effect/http";
import type { Auth } from "../auth.js";
import { Forbidden, Invalid, NotFound, QuotaExceeded, Unauthorized } from "@miraicloud/core/errors";
import type * as S from "@miraicloud/core/sui";
import { authorize } from "./request.js";

/**
 * Which transactions to receive. Each field takes one value or several:
 * several values of one field match any of them; different fields must all
 * match. Addresses may be short (`0x2`).
 */
export interface TransactionFilter {
  /** The sender. */
  readonly sender?: string | ReadonlyArray<string> | undefined;
  /** A Move call into the package. */
  readonly package?: string | ReadonlyArray<string> | undefined;
  /** A Move call to `pkg::module::function`, or to any function of `pkg::module`. */
  readonly function?: string | ReadonlyArray<string> | undefined;
  /** An emitted event's type, or a prefix of it: `pkg`, `pkg::module`, `pkg::module::Name`, or a full type with its arguments. */
  readonly event?: string | ReadonlyArray<string> | undefined;
  /** A changed object (created, mutated, deleted, wrapped, …). */
  readonly object?: string | ReadonlyArray<string> | undefined;
  /** A balance change of the address, or a changed object it owned or owns. */
  readonly address?: string | ReadonlyArray<string> | undefined;
}

export interface EventStreamOptions {
  /** The first checkpoint; default: from the next one. */
  readonly from?: number | undefined;
  /** Resume after the event with this cursor (wins over `from`). */
  readonly cursor?: string | undefined;
}

export interface TransactionStreamOptions extends EventStreamOptions {
  readonly filter?: TransactionFilter | undefined;
  /** Also emit `Progress` after each checkpoint: a resume point that keeps up even when the filter rarely matches. */
  readonly progress?: boolean | undefined;
}

/** Checkpoints `from` … `to` will never be on the stream (read them from `/v1/checkpoints/:n`); it continues after them. */
type Gap = { readonly cursor: string; readonly from: number; readonly to: number };

export type CheckpointEvent = Data.TaggedEnum<{ Checkpoint: { readonly cursor: string; readonly checkpoint: S.Checkpoint }; Gap: Gap }>;
export const CheckpointEvent = Data.taggedEnum<CheckpointEvent>();

export type TransactionEvent = Data.TaggedEnum<{
  Transaction: { readonly cursor: string; readonly transaction: S.Transaction };
  Gap: Gap;
  /** Checkpoint `checkpoint` is done (`progress: true` only). */
  Progress: { readonly cursor: string; readonly checkpoint: number };
}>;
export const TransactionEvent = Data.taggedEnum<TransactionEvent>();

/** The start is older than the stream holds: read `from` … `oldest - 1` from `/v1/checkpoints/:n`, then stream `{ from: oldest }`. */
export class Gone extends Data.TaggedError("Gone")<{ readonly message: string; readonly from: number; readonly oldest: number }> {}

/** What ends an event stream. */
export type EventStreamError = Unauthorized | Forbidden | Invalid | NotFound | QuotaExceeded | Gone;

/** The event streams. */
export interface EventStreams {
  /** Every checkpoint, as the read API's JSON. */
  readonly checkpoints: (options?: EventStreamOptions) => Stream.Stream<CheckpointEvent, EventStreamError>;
  /** The transactions matching `filter` (all of them without one), as the read API's JSON. */
  readonly transactions: (options?: TransactionStreamOptions) => Stream.Stream<TransactionEvent, EventStreamError>;
}

// ---------------------------------------------------------------------------
// Server-Sent Events

/** A dispatched SSE event, or a block with only an id (a resume point, nothing to dispatch). */
export type SseRecord =
  | { readonly _tag: "Event"; readonly id: string | undefined; readonly event: string; readonly data: string }
  | { readonly _tag: "Cursor"; readonly id: string }
  | { readonly _tag: "Retry"; readonly ms: number };

/** An incremental SSE parser (the WHATWG rules: CR, LF or CRLF; comments; `data` lines joined). */
export const sseParser = () => {
  let pending = "";
  let skipLf = false;
  let data: string[] = [];
  let event = "";
  let id: string | undefined;
  let sawId = false;
  const line = (l: string, out: SseRecord[]) => {
    if (l === "") {
      if (data.length > 0) out.push({ _tag: "Event", id, event: event === "" ? "message" : event, data: data.join("\n") });
      else if (sawId && id !== undefined) out.push({ _tag: "Cursor", id });
      data = [];
      event = "";
      sawId = false;
      return;
    }
    if (l.startsWith(":")) return;
    const colon = l.indexOf(":");
    const field = colon < 0 ? l : l.slice(0, colon);
    const value = colon < 0 ? "" : l.slice(l.charCodeAt(colon + 1) === 32 ? colon + 2 : colon + 1);
    if (field === "data") data.push(value);
    else if (field === "event") event = value;
    else if (field === "id" && !value.includes("\0")) {
      id = value;
      sawId = true;
    } else if (field === "retry" && /^\d+$/.test(value)) out.push({ _tag: "Retry", ms: Number(value) });
  };
  return (chunk: string): SseRecord[] => {
    const out: SseRecord[] = [];
    let text = pending + chunk;
    pending = "";
    if (skipLf && text.startsWith("\n")) text = text.slice(1);
    skipLf = false;
    let start = 0;
    for (let i = 0; i < text.length; i++) {
      const c = text.charCodeAt(i);
      if (c !== 10 && c !== 13) continue;
      line(text.slice(start, i), out);
      if (c === 13) {
        if (i + 1 === text.length) skipLf = true;
        else if (text.charCodeAt(i + 1) === 10) i++;
      }
      start = i + 1;
    }
    pending = text.slice(start);
    return out;
  };
};

// ---------------------------------------------------------------------------
// Connecting, and staying connected

/** A refusal or failure worth another attempt. */
/** Worth another attempt, after backoff (at least `retryAfterMs`, from `Retry-After`). */
class Transient extends Data.TaggedError("Transient")<{ readonly message: string; readonly retryAfterMs?: number | undefined }> {}

/** What the service sends when the stream ends (or none): `retry:` default. */
const DEFAULT_RETRY_MS = 2_000;
const MAX_BACKOFF_MS = 30_000;
/** Keepalives come every 15 s: this long without a byte is a dead connection. */
const STALL = Duration.seconds(45);

const values = (v: string | ReadonlyArray<string> | undefined): ReadonlyArray<string> => (v === undefined ? [] : typeof v === "string" ? [v] : v);

const FILTER_FIELDS = ["sender", "package", "function", "event", "object", "address"] as const satisfies ReadonlyArray<keyof TransactionFilter>;

/** The query of a stream request. */
export const queryOf = (options: TransactionStreamOptions, resuming: boolean): URLSearchParams => {
  const q = new URLSearchParams();
  if (!resuming && options.from !== undefined) q.set("from", String(options.from));
  for (const field of FILTER_FIELDS) for (const v of values(options.filter?.[field])) q.append(field, v);
  return q;
};

/**
 * A refused connection. `served`: this stream has been served before, so a
 * `404` is the service's transient state (a deploy, a route moving), not a
 * wrong URL: retried rather than final.
 */
const refusal = (response: HttpClientResponse.HttpClientResponse, served: boolean) =>
  Effect.gen(function* () {
    const body = yield* response.json.pipe(Effect.orElseSucceed(() => ({}) as unknown));
    const b = (typeof body === "object" && body !== null ? body : {}) as { readonly _tag?: unknown; readonly message?: unknown; readonly from?: unknown; readonly oldest?: unknown };
    const message = typeof b.message === "string" ? b.message : `HTTP ${response.status}`;
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
        if (served) return yield* new Transient({ message, retryAfterMs });
        return yield* new NotFound({ message });
      case 410: {
        const oldest = typeof b.oldest === "number" ? b.oldest : Number(response.headers["oldest-checkpoint"] ?? Number.NaN);
        return yield* new Gone({ message, from: typeof b.from === "number" ? b.from : Number.NaN, oldest });
      }
      case 429:
        if (b._tag === "QuotaExceeded") return yield* new QuotaExceeded({ message });
        return yield* new Transient({ message, retryAfterMs });
      default:
        return yield* new Transient({ message, retryAfterMs });
    }
  });

/** One SSE stream, reconnected for as long as it is consumed. */
const connect = <A>(
  client: HttpClient.HttpClient,
  base: string,
  auth: Auth,
  path: string,
  options: TransactionStreamOptions,
  /** A data event → what to emit (and whether it counts as progress). */
  decode: (event: string, data: string, cursor: string) => Option.Option<A>,
  gap: (cursor: string, from: number, to: number) => A,
  progress: Option.Option<(cursor: string, checkpoint: number) => A>,
): Stream.Stream<A, EventStreamError> =>
  Stream.suspend(() => {
    const audience = new URL(base).origin;
    let cursor = Option.fromNullishOr(options.cursor);
    let retryMs = DEFAULT_RETRY_MS;
    let failures = 0;
    let immediate = false;
    let served = false;
    /** The service's `Retry-After` on the last refusal, a floor for the next wait. */
    let retryAfterMs = 0;

    const request = Effect.suspend(() => {
      const query = queryOf(options, Option.isSome(cursor)).toString();
      const target = `${path}${query === "" ? "" : `?${query}`}`;
      let r = HttpClientRequest.get(`${base}${target}`).pipe(HttpClientRequest.setHeader("accept", "text/event-stream"));
      if (Option.isSome(cursor)) r = HttpClientRequest.setHeader(r, "last-event-id", cursor.value);
      return authorize(r, auth, audience, target, { client, endpoint: base }).pipe(
        // A refused delegation is final; a rate limit, an outage or a failing signer is retried.
        Effect.catchTags({
          RateLimited: (e) => Effect.fail(new Transient({ message: `token exchange: ${e.message}` })),
          Unavailable: (e) => Effect.fail(new Transient({ message: `token exchange: ${e.message}` })),
          SignerError: (e) => Effect.fail(new Transient({ message: `token exchange: ${e.message}` })),
        }),
      );
    });

    const handle = (record: SseRecord): Option.Option<A> => {
      switch (record._tag) {
        case "Retry":
          retryMs = record.ms;
          return Option.none();
        case "Cursor": {
          cursor = Option.some(record.id);
          const seq = Number(record.id);
          return Option.isSome(progress) && Number.isSafeInteger(seq) ? Option.some(progress.value(record.id, seq)) : Option.none();
        }
        case "Event": {
          if (record.event === "error") return Option.none(); // the service is ending the stream; it resumes after the last id
          if (record.id === undefined) return Option.none();
          // Decoded before the cursor moves: an event that can't be decoded
          // throws (a broken connection, retried), and the next connection
          // resumes before it rather than skipping it.
          let item: Option.Option<A>;
          if (record.event === "gap") {
            const { from, to } = JSON.parse(record.data) as { from: number; to: number };
            // The stream ends after a gap: come straight back.
            immediate = true;
            item = Option.some(gap(record.id, from, to));
          } else {
            item = decode(record.event, record.data, record.id);
          }
          cursor = Option.some(record.id);
          failures = 0;
          return item;
        }
      }
    };

    const attempt: Stream.Stream<A, EventStreamError | Transient> = Stream.unwrap(
      Effect.gen(function* () {
        immediate = false;
        retryAfterMs = 0;
        const authorized = yield* request;
        const response = yield* client.execute(authorized.request).pipe(Effect.mapError((e) => new Transient({ message: e.message })));
        if (response.status !== 200) return yield* refusal(response, served);
        served = true;
        const parse = sseParser();
        // A delegated key: end this connection once a fresh key exists and come straight back with it.
        const renew = authorized.renew === undefined ? Effect.never : authorized.renew.pipe(Effect.tap(() => Effect.sync(() => void (immediate = true))));
        return response.stream.pipe(
          Stream.interruptWhen(renew),
          Stream.mapError((e) => new Transient({ message: e.message })),
          Stream.timeout(STALL),
          Stream.decodeText(),
          Stream.flatMap((chunk) => Stream.fromIterable(parse(chunk))),
          // One record at a time, so a record that fails leaves every earlier one delivered.
          Stream.mapEffect((record) => Effect.try({ try: () => Option.toArray(handle(record)), catch: (e) => new Transient({ message: `undecodable event: ${e instanceof Error ? e.message : String(e)}` }) })),
          Stream.flattenIterable,
        );
      }),
    );

    /** Before the next attempt: at once after a gap, `retry:` after an end, backing off while failing. */
    const pause = Stream.fromEffect(
      Effect.suspend(() => {
        if (immediate) return Effect.void;
        const ms = failures === 0 ? retryMs : Math.min(MAX_BACKOFF_MS, retryMs * 2 ** (failures - 1));
        return Effect.sleep(Duration.millis(Math.max(ms * (0.75 + Math.random() * 0.5), retryAfterMs)));
      }),
    ).pipe(Stream.drain);

    const once = attempt.pipe(
      Stream.catchTag("Transient", (e) =>
        Stream.fromEffect(
          Effect.sync(() => {
            failures++;
            retryAfterMs = e.retryAfterMs ?? 0;
          }),
        ).pipe(Stream.drain),
      ),
      Stream.concat(pause),
    );
    return once.pipe(Stream.repeat(Schedule.forever));
  });

/** The event streams over `client`, at `base` (the endpoint, without a trailing slash). */
export const events = (client: HttpClient.HttpClient, base: string, auth: Auth): EventStreams => {
  const gapOf = <T>(make: (args: Gap) => T) => (cursor: string, from: number, to: number) => make({ cursor, from, to });
  return {
    checkpoints: (o = {}) =>
      connect(
        client,
        base,
        auth,
        "/v1/checkpoints/stream",
        o,
        (event, data, cursor) => (event === "checkpoint" ? Option.some(CheckpointEvent.Checkpoint({ cursor, checkpoint: JSON.parse(data) as S.Checkpoint })) : Option.none()),
        gapOf(CheckpointEvent.Gap),
        Option.none(),
      ),
    transactions: (o = {}) =>
      connect(
        client,
        base,
        auth,
        "/v1/transactions/stream",
        o,
        (event, data, cursor) => (event === "transaction" ? Option.some(TransactionEvent.Transaction({ cursor, transaction: JSON.parse(data) as S.Transaction })) : Option.none()),
        gapOf(TransactionEvent.Gap),
        o.progress ? Option.some((cursor, checkpoint) => TransactionEvent.Progress({ cursor, checkpoint })) : Option.none(),
      ),
  };
};
