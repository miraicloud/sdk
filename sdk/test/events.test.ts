/** The event streams (SSE) against a stand-in server: parsing, resume, gaps, refusals, retries. */

import { describe, expect, test } from "bun:test";
import { Effect, Layer, Redacted, Ref, Stream } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { Auth } from "../src/auth.js";
import { layer, MiraiCloudStream, type TransactionEvent } from "../src/streams.js";
import { queryOf, sseParser } from "../src/streams/events.js";

const ENDPOINT = "https://gateway.test";

describe("SSE parsing", () => {
  test("events across chunks, CRLF and CR, comments, id-only cursors, retry", () => {
    const parse = sseParser();
    const out = [
      ...parse("retry: 1500\n\n: keepalive\n\nid: 5\nevent: checkpoint\nda"),
      ...parse('ta: {"a":1}\r\n\r'),
      ...parse("\nid: 6\n\nevent: transaction\r"),
      ...parse("data: x\rdata: y\r\r"),
    ];
    expect(out).toEqual([
      { _tag: "Retry", ms: 1500 },
      { _tag: "Event", id: "5", event: "checkpoint", data: '{"a":1}' },
      { _tag: "Cursor", id: "6" },
      // The id is sticky (as in EventSource).
      { _tag: "Event", id: "6", event: "transaction", data: "x\ny" },
    ]);
  });

  test("filters become repeated query parameters; from only when not resuming", () => {
    const q = queryOf({ from: 9, filter: { sender: ["0x1", "0x2"], event: "0x2::coin" } }, false);
    expect(q.toString()).toBe("from=9&sender=0x1&sender=0x2&event=0x2%3A%3Acoin");
    expect(queryOf({ from: 9 }, true).toString()).toBe("");
  });
});

interface Seen {
  readonly url: string;
  readonly headers: Readonly<Record<string, string>>;
}

/** A server answering each request in turn with `responses` (the last repeats). */
const server = (responses: ReadonlyArray<() => Response>) =>
  Effect.gen(function* () {
    const seen = yield* Ref.make<ReadonlyArray<Seen>>([]);
    const client = HttpClient.make((request, url) =>
      Effect.gen(function* () {
        const n = (yield* Ref.getAndUpdate(seen, (all) => [...all, { url: url.toString(), headers: request.headers }])).length;
        return HttpClientResponse.fromWeb(request, responses[Math.min(n, responses.length - 1)]!());
      }),
    );
    return { seen, http: Layer.succeed(HttpClient.HttpClient, client) };
  });

const sse = (body: string) => () => new Response(body, { headers: { "content-type": "text/event-stream" } });
const json = (status: number, body: object) => () => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
const auth = Auth.Key({ token: Redacted.make("mc1_test") });
const tx = (seq: number, index: number) => `id: ${seq}:${index}\nevent: transaction\ndata: ${JSON.stringify({ digest: `d${seq}-${index}`, index })}\n\n`;

const run = <A, E>(responses: ReadonlyArray<() => Response>, f: (s: typeof MiraiCloudStream.Service) => Stream.Stream<A, E>, take: number) =>
  Effect.gen(function* () {
    const { seen, http } = yield* server(responses);
    const events = yield* Effect.gen(function* () {
      const s = yield* MiraiCloudStream;
      return yield* f(s).pipe(Stream.take(take), Stream.runCollect, Effect.result);
    }).pipe(Effect.provide(layer({ endpoint: ENDPOINT, auth }).pipe(Layer.provide(http))));
    return { events, seen: yield* Ref.get(seen) };
  }).pipe(Effect.runPromise);

describe("the event streams", () => {
  test("reconnects after the stream ends, resuming after the last event (Last-Event-ID)", async () => {
    const { events, seen } = await run(
      [sse(`retry: 1\n\n${tx(10, 0)}${tx(10, 1)}id: 10\n\n`), sse(`${tx(11, 3)}event: error\ndata: {"_tag":"reset","message":"moved"}\n\n`), sse(tx(12, 0))],
      (s) => s.events.transactions({ from: 10, filter: { sender: "0x1" }, progress: true }),
      5,
    );
    expect(events._tag).toBe("Success");
    const got = (events as { success: ReadonlyArray<TransactionEvent> }).success;
    expect(got.map((e) => [e._tag, e.cursor])).toEqual([
      ["Transaction", "10:0"],
      ["Transaction", "10:1"],
      ["Progress", "10"],
      ["Transaction", "11:3"],
      ["Transaction", "12:0"],
    ]);
    expect(seen.map((s) => [new URL(s.url).pathname + new URL(s.url).search, s.headers["last-event-id"]])).toEqual([
      ["/v1/transactions/stream?from=10&sender=0x1", undefined],
      ["/v1/transactions/stream?sender=0x1", "10"],
      ["/v1/transactions/stream?sender=0x1", "11:3"],
    ]);
    expect(seen[0]!.headers["authorization"]).toBe("Bearer mc1_test");
    expect(seen[0]!.headers["accept"]).toBe("text/event-stream");
  });

  test("a gap is an event; the stream continues after it at once", async () => {
    const { events, seen } = await run(
      [sse(`id: 5\nevent: checkpoint\ndata: {"sequenceNumber":5}\n\nid: 9\nevent: gap\ndata: {"from":6,"to":9}\n\n`), sse(`id: 10\nevent: checkpoint\ndata: {"sequenceNumber":10}\n\n`)],
      (s) => s.events.checkpoints(),
      3,
    );
    expect((events as { success: ReadonlyArray<{ _tag: string }> }).success).toEqual([
      expect.objectContaining({ _tag: "Checkpoint", cursor: "5", checkpoint: { sequenceNumber: 5 } }),
      expect.objectContaining({ _tag: "Gap", cursor: "9", from: 6, to: 9 }),
      expect.objectContaining({ _tag: "Checkpoint", cursor: "10" }),
    ]);
    expect(seen[1]!.headers["last-event-id"]).toBe("9");
  });

  test("410: Gone with the oldest checkpoint (backfill from /v1/checkpoints/:n, then from it)", async () => {
    const { events } = await run([json(410, { _tag: "Gone", message: "too old", from: 3, oldest: 100 })], (s) => s.events.checkpoints({ from: 3 }), 1);
    expect(events._tag).toBe("Failure");
    expect((events as { failure: unknown }).failure).toMatchObject({ _tag: "Gone", from: 3, oldest: 100 });
  });

  test("refusals are final; temporary failures are retried", async () => {
    const forbidden = await run([json(403, { _tag: "Forbidden", message: "needs stream:read" })], (s) => s.events.checkpoints(), 1);
    expect((forbidden.events as { failure: unknown }).failure).toMatchObject({ _tag: "Forbidden", message: "needs stream:read" });
    const invalid = await run([json(400, { _tag: "Invalid", message: "bad sender" })], (s) => s.events.transactions({ filter: { sender: "bob" } }), 1);
    expect((invalid.events as { failure: unknown }).failure).toMatchObject({ _tag: "Invalid" });
    const quota = await run([json(429, { _tag: "QuotaExceeded", message: "over" })], (s) => s.events.checkpoints(), 1);
    expect((quota.events as { failure: unknown }).failure).toMatchObject({ _tag: "QuotaExceeded" });
    const flaky = await run(
      [sse(`retry: 1\n\nid: 1\nevent: checkpoint\ndata: {}\n\n`), json(503, { _tag: "Unavailable", message: "later" }), json(429, { _tag: "Full", message: "full" }), sse(`id: 2\nevent: checkpoint\ndata: {}\n\n`)],
      (s) => s.events.checkpoints(),
      2,
    );
    expect((flaky.events as { success: ReadonlyArray<{ cursor: string }> }).success.map((e) => e.cursor)).toEqual(["1", "2"]);
    expect(flaky.seen.slice(1).map((s) => s.headers["last-event-id"])).toEqual(["1", "1", "1"]);
  });
});
