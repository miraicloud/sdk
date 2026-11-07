/**
 * `Auth.Delegated` against a local server: keys exchanged and refreshed
 * before they expire, one exchange however many callers, and streams that
 * move onto the fresh key without losing or repeating an event. (The
 * delegation format and `delegate` are tested in `@miraicloud/core`.)
 */

import { afterAll, describe, expect, test } from "bun:test";
import { Ed25519Keypair } from "@mysten/sui/keypairs/ed25519";
import { verifyPersonalMessageSignature } from "@mysten/sui/verify";
import { decodeChain, delegate, delegationId, encodeChain, parseDelegation } from "@miraicloud/core/delegation";
import { message as requestMessage } from "@miraicloud/core/signed";
import { fromSuiSigner } from "@miraicloud/core/signer";
import { Effect, Fiber, Result, Stream } from "effect";
import { Auth, layerFetch, MiraiCloud } from "../src/client.js";
import { layerFetch as streamsLayer, MiraiCloudStream } from "../src/streams.js";
import { refreshAt } from "../src/token.js";

describe("Auth.Delegated (a local server)", () => {
  const issued: Array<{ token: string; at: number; expiresAt: number }> = [];
  const used: string[] = [];
  const streamTokens: string[] = [];
  let lifetimeMs = 60 * 60_000;
  let delay = 0;
  /** Exchanges asked for (after the signature check), by agent address, and whether they're refused with a 429. */
  const exchanges = new Map<string, number>();
  let rateLimited = false;
  let endpoint = "";
  const server = Bun.serve({
    port: 0,
    idleTimeout: 0,
    async fetch(req) {
      const url = new URL(req.url);
      if (url.pathname === "/v1/token" && req.method === "POST") {
        const body = await req.text();
        const address = req.headers.get("x-miraicloud-address")!;
        const digest = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(body))), (b) => b.toString(16).padStart(2, "0")).join("");
        const text = requestMessage({ audience: url.origin, address, method: "POST", path: "/v1/token", bodySha256: digest, timestamp: Number(req.headers.get("x-miraicloud-timestamp")), nonce: req.headers.get("x-miraicloud-nonce")! });
        try {
          await verifyPersonalMessageSignature(new TextEncoder().encode(text), req.headers.get("x-miraicloud-signature")!, { address });
        } catch {
          return Response.json({ _tag: "Unauthorized", message: "bad signature" }, { status: 401 });
        }
        const chain = Result.getOrThrow(decodeChain(JSON.parse(body).chain));
        if (Result.getOrThrow(parseDelegation(chain.at(-1)!.message)).subject !== address) return Response.json({ _tag: "Forbidden", message: "not your chain" }, { status: 403 });
        exchanges.set(address, (exchanges.get(address) ?? 0) + 1);
        if (rateLimited) return Response.json({ _tag: "RateLimited", message: "slow down" }, { status: 429, headers: { "retry-after": "1" } });
        if (delay > 0) await Bun.sleep(delay);
        const now = Date.now();
        const token = `mc1_t${issued.length + 1}`;
        issued.push({ token, at: now, expiresAt: now + lifetimeMs });
        return Response.json({ token, id: `id${issued.length}`, expiresAt: now + lifetimeMs, ancestors: chain.map((l) => delegationId(l.message)).reverse() });
      }
      const bearer = req.headers.get("authorization")?.replace(/^Bearer /, "") ?? "";
      if (url.pathname === "/v1/whoami") {
        used.push(bearer);
        const live = issued.find((t) => t.token === bearer && t.expiresAt > Date.now());
        if (live === undefined) return Response.json({ _tag: "Unauthorized", message: "this API key has expired" }, { status: 401 });
        return Response.json({ as: "key", key: null, account: { id: `0x${"a".repeat(64)}`, label: null, status: "active", limits: { monthlyRequests: null, monthlyBytes: null }, createdAt: 0 } });
      }
      if (url.pathname === "/v1/transactions/stream") {
        streamTokens.push(bearer);
        // From Last-Event-ID (or 0), one event every 50 ms, never closing.
        let n = Number((req.headers.get("last-event-id") ?? "0").split(":")[0]);
        const stream = new ReadableStream<Uint8Array>({
          async pull(c) {
            await Bun.sleep(50);
            n++;
            c.enqueue(new TextEncoder().encode(`id: ${n}:0\nevent: transaction\ndata: ${JSON.stringify({ digest: `d${n}`, index: 0 })}\n\n`));
          },
        });
        return new Response(stream, { headers: { "content-type": "text/event-stream" } });
      }
      return new Response("not found", { status: 404 });
    },
  });
  afterAll(() => void server.stop(true));
  endpoint = `http://127.0.0.1:${server.port}`;

  const chainFor = async (agent: Ed25519Keypair) =>
    encodeChain(await Effect.runPromise(delegate(fromSuiSigner(new Ed25519Keypair()), { to: agent.toSuiAddress(), scopes: ["stream:read", "keys:read"] }, { audience: endpoint })));

  test("one exchange for concurrent callers; the key reused until it's due for refresh; then a new one", async () => {
    issued.length = 0;
    used.length = 0;
    lifetimeMs = 2_000;
    delay = 100;
    const agent = new Ed25519Keypair();
    const auth = Auth.Delegated({ chain: await chainFor(agent), signer: fromSuiSigner(agent) });
    const whoami = Effect.flatMap(MiraiCloud, (c) => c.account.whoami()).pipe(Effect.provide(layerFetch({ endpoint, auth })));
    await Promise.all(Array.from({ length: 5 }, () => Effect.runPromise(whoami)));
    expect(issued.length).toBe(1);
    expect(new Set(used)).toEqual(new Set([issued[0]!.token]));
    // Due for refresh at max(60 s, 10 %) before expiry, at most half its life: here after 1 s.
    expect(refreshAt(0, 2_000)).toBe(1_000);
    await Bun.sleep(1_100);
    // Due but valid: used at once while the refresh runs.
    await Effect.runPromise(whoami);
    expect(used.at(-1)).toBe(issued[0]!.token);
    await Bun.sleep(200);
    await Effect.runPromise(whoami);
    expect(issued.length).toBe(2);
    expect(used.at(-1)).toBe(issued[1]!.token);
    delay = 0;
  });

  test("a refresh that fails (429, Retry-After) leaves the valid key in use: requests don't wait, a stream isn't dropped", async () => {
    issued.length = 0;
    used.length = 0;
    streamTokens.length = 0;
    lifetimeMs = 3_000; // due for refresh after 1.5 s
    const agent = new Ed25519Keypair();
    const auth = Auth.Delegated({ chain: await chainFor(agent), signer: fromSuiSigner(agent) });
    const whoami = Effect.flatMap(MiraiCloud, (c) => c.account.whoami()).pipe(Effect.provide(layerFetch({ endpoint, auth })));
    try {
      await Effect.runPromise(whoami);
      rateLimited = true;
      const t0 = Date.now();
      // 50 events, ~2.5 s: past the refresh time, before the key expires.
      const events = await Effect.runPromise(
        Effect.flatMap(MiraiCloudStream, (s) => s.events.transactions({ from: 0 }).pipe(Stream.take(50), Stream.runCollect)).pipe(Effect.provide(streamsLayer({ endpoint, auth }))),
      );
      expect(events.length).toBe(50);
      expect(Date.now() - t0).toBeLessThan(2_900);
      expect(streamTokens).toEqual([issued[0]!.token]); // one connection throughout
      expect(exchanges.get(agent.toSuiAddress())).toBeGreaterThan(1); // the refresh was tried
      const t1 = Date.now();
      await Effect.runPromise(whoami);
      expect(Date.now() - t1).toBeLessThan(500);
      expect(used.at(-1)).toBe(issued[0]!.token);
    } finally {
      rateLimited = false;
    }
  }, 30_000);

  test("an exchange nobody waits for any more stops (its retries don't run on)", async () => {
    rateLimited = true;
    const agent = new Ed25519Keypair();
    const auth = Auth.Delegated({ chain: await chainFor(agent), signer: fromSuiSigner(agent) });
    try {
      const fiber = Effect.runFork(Effect.flatMap(MiraiCloud, (c) => c.account.whoami()).pipe(Effect.provide(layerFetch({ endpoint, auth }))));
      await Bun.sleep(200);
      await Effect.runPromise(Fiber.interrupt(fiber));
      expect(exchanges.get(agent.toSuiAddress())).toBe(1);
      await Bun.sleep(1_300); // past the 1 s Retry-After
      expect(exchanges.get(agent.toSuiAddress())).toBe(1);
    } finally {
      rateLimited = false;
    }
  });

  test("a stream moves onto each fresh key before the old one expires: no event lost or repeated", async () => {
    issued.length = 0;
    streamTokens.length = 0;
    lifetimeMs = 1_000;
    const agent = new Ed25519Keypair();
    const auth = Auth.Delegated({ chain: await chainFor(agent), signer: fromSuiSigner(agent) });
    const events = await Effect.runPromise(
      Effect.flatMap(MiraiCloudStream, (s) => s.events.transactions({ from: 0 }).pipe(Stream.take(40), Stream.runCollect)).pipe(Effect.provide(streamsLayer({ endpoint, auth }))),
    );
    const digests = [...events].flatMap((e) => (e._tag === "Transaction" ? [(e.transaction as { digest: string }).digest] : []));
    expect(digests).toEqual(Array.from({ length: 40 }, (_, i) => `d${i + 1}`));
    // Several connections, each with the key at hand then; never an expired one.
    expect(streamTokens.length).toBeGreaterThan(1);
    for (const token of streamTokens) expect(issued.some((t) => t.token === token)).toBe(true);
    expect(new Set(streamTokens).size).toBe(streamTokens.length);
  }, 30_000);

  test("a refused chain is final (401/403), not retried forever", async () => {
    const agent = new Ed25519Keypair();
    const stranger = new Ed25519Keypair();
    const auth = Auth.Delegated({ chain: await chainFor(agent), signer: fromSuiSigner(stranger) });
    const error = await Effect.runPromise(Effect.flatMap(MiraiCloud, (c) => c.account.whoami()).pipe(Effect.provide(layerFetch({ endpoint, auth })), Effect.flip));
    expect((error as { _tag: string })._tag).toBe("Forbidden");
  });
});
