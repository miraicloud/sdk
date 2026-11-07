/**
 * The SDK against a stand-in server: what goes on the wire (signed requests,
 * bearer tokens) and what comes back (records, typed errors).
 */

import { describe, expect, test } from "bun:test";
import { Ed25519Keypair } from "@mysten/sui/keypairs/ed25519";
import { verifyPersonalMessageSignature } from "@mysten/sui/verify";
import { Effect, Layer, Redacted, Ref } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { Auth, include, layer, MiraiCloud, MiraiCloudRead } from "../src/client.js";
import * as Errors from "@miraicloud/core/errors";
import type { KeyInfo } from "@miraicloud/core/model";
import type { SuiObject, Transaction, ValidatorSet } from "@miraicloud/core/sui";
import { HEADERS, message, sha256Hex } from "@miraicloud/core/signed";
import { fromSuiSigner } from "@miraicloud/core/signer";

const ENDPOINT = "https://api.example.test";

const KEY: KeyInfo = {
  id: "abcdefghijklmnop",
  accountId: `0x${"1".repeat(64)}`,
  name: "ci",
  scopes: ["stream:read"],
  networks: ["mainnet"],
  origins: [],
  limits: { monthlyRequests: null, monthlyBytes: null },
  createdAt: 1,
  expiresAt: null,
  revokedAt: null,
  parentId: null,
  ancestors: [],
  kid: 1,
  tokenIssuedAt: 1,
};

interface Seen {
  readonly method: string;
  readonly url: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly body: Uint8Array;
}

/** A stand-in server: records each request, answers with `respond`. */
const server = (respond: (seen: Seen) => Response) =>
  Effect.gen(function* () {
    const seen = yield* Ref.make<ReadonlyArray<Seen>>([]);
    const client = HttpClient.make((request, url) =>
      Effect.gen(function* () {
        const entry: Seen = {
          method: request.method,
          url: url.toString(),
          headers: request.headers,
          body: request.body._tag === "Uint8Array" ? request.body.body : new Uint8Array(),
        };
        yield* Ref.update(seen, (all) => [...all, entry]);
        return HttpClientResponse.fromWeb(request, respond(entry));
      }),
    );
    return { seen, http: Layer.succeed(HttpClient.HttpClient, client) };
  });

const json = (status: number, body: unknown) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

const run = <A, E>(effect: Effect.Effect<A, E, MiraiCloud | MiraiCloudRead>, auth: Auth, respond: (seen: Seen) => Response) =>
  Effect.gen(function* () {
    const { seen, http } = yield* server(respond);
    const result = yield* effect.pipe(
      Effect.provide(layer({ endpoint: ENDPOINT, auth }).pipe(Layer.provide(http))),
      Effect.result,
    );
    return { result, seen: yield* Ref.get(seen) };
  }).pipe(Effect.runPromise);

describe("client", () => {
  test("as the owner: every request is signed, and the signature verifies for the account's address", async () => {
    const keypair = new Ed25519Keypair();
    const signer = fromSuiSigner(keypair);
    const { result, seen } = await run(
      Effect.flatMap(MiraiCloud, (api) => api.keys.create({ payload: { name: "ci", scopes: ["stream:read"] } })),
      Auth.Owner({ signer }),
      () => json(200, { token: "mc1_x", key: KEY }),
    );
    expect(result._tag).toBe("Success");
    const [request] = seen;
    expect(request!.url).toBe(`${ENDPOINT}/v1/keys`);
    expect(request!.headers[HEADERS.address]).toBe(keypair.toSuiAddress());
    const signed = message({
      audience: ENDPOINT,
      address: request!.headers[HEADERS.address]!,
      method: "POST",
      path: "/v1/keys",
      bodySha256: await Effect.runPromise(sha256Hex(request!.body)),
      timestamp: Number(request!.headers[HEADERS.timestamp]),
      nonce: request!.headers[HEADERS.nonce]!,
    });
    const publicKey = await verifyPersonalMessageSignature(new TextEncoder().encode(signed), request!.headers[HEADERS.signature]!);
    expect(publicKey.toSuiAddress()).toBe(keypair.toSuiAddress());
  });

  test("as the owner: the query string is part of what is signed", async () => {
    const signer = fromSuiSigner(new Ed25519Keypair());
    const { seen } = await run(Effect.flatMap(MiraiCloud, (api) => api.keys.list({ query: { revoked: "true" } })), Auth.Owner({ signer }), () => json(200, []));
    const request = seen[0]!;
    const signed = message({
      audience: ENDPOINT,
      address: signer.address,
      method: "GET",
      path: "/v1/keys?revoked=true",
      bodySha256: await Effect.runPromise(sha256Hex("")),
      timestamp: Number(request.headers[HEADERS.timestamp]),
      nonce: request.headers[HEADERS.nonce]!,
    });
    const publicKey = await verifyPersonalMessageSignature(new TextEncoder().encode(signed), request.headers[HEADERS.signature]!);
    expect(publicKey.toSuiAddress()).toBe(signer.address);
  });

  test("as a key: a bearer token, and no signature", async () => {
    const { result, seen } = await run(Effect.flatMap(MiraiCloud, (api) => api.keys.list({ query: {} })), Auth.Key({ token: Redacted.make("mc1_secret") }), () =>
      json(200, [KEY]),
    );
    expect(result._tag === "Success" && result.success).toEqual([KEY]);
    expect(seen[0]!.headers.authorization).toBe("Bearer mc1_secret");
    expect(seen[0]!.headers[HEADERS.signature]).toBeUndefined();
  });

  test("errors come back as the SDK's classes, by tag", async () => {
    const { result } = await run(
      Effect.flatMap(MiraiCloud, (api) => api.keys.get({ params: { id: "nope" } })),
      Auth.Key({ token: Redacted.make("mc1_x") }),
      () => json(404, { _tag: "NotFound", message: "no key nope" }),
    );
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure") {
      expect(result.failure).toBeInstanceOf(Errors.NotFound);
      expect(result.failure).toMatchObject({ _tag: "NotFound", message: "no key nope" });
    }
  });

  test("the endpoint's trailing slash doesn't matter", async () => {
    const { seen } = await Effect.gen(function* () {
      const { seen, http } = yield* server(() => json(200, "ok"));
      yield* Effect.flatMap(MiraiCloud, (api) => api.system.health()).pipe(
        Effect.provide(layer({ endpoint: `${ENDPOINT}/`, auth: Auth.None() }).pipe(Layer.provide(http))),
      );
      return { seen: yield* Ref.get(seen) };
    }).pipe(Effect.runPromise);
    expect(seen[0]!.url).toBe(`${ENDPOINT}/healthz`);
  });

  test("statuses", () => {
    expect(Errors.statusOf(new Errors.QuotaExceeded({ message: "" }))).toBe(429);
    expect(Errors.statusOf({ _tag: "Other" })).toBe(500);
  });
});


const ADDRESS = `0x${"2".repeat(64)}`;
const TX: Transaction = {
  digest: "2gb7RepDK8R2HCWhrxvJLfYsELNk2BAU5oJZyuEBzmYn",
  checkpoint: 341924418,
  timestamp: 1793884447176,
  epoch: 1302,
  index: null,
  kind: "programmable",
  sender: ADDRESS,
  status: "success",
  error: null,
  gas: {
    payer: ADDRESS,
    sponsored: false,
    budget: "150000000",
    price: "100",
    payment: [],
    used: { computation: "100000", storage: "7000800", storageRebate: "6997991", nonRefundable: "70686", fee: "102809" },
    object: null,
  },
  expiration: { kind: "none" },
  inputs: [{ kind: "pure", base64: "AA==" }, { kind: "shared", objectId: ADDRESS, initialVersion: 1, access: "read" }],
  commands: [{ kind: "moveCall", target: "0x2::coin::zero", package: ADDRESS, module: "coin", function: "zero", typeArguments: [], arguments: [{ kind: "input", index: 0 }] }],
  system: null,
  objectChanges: [],
  balanceChanges: [{ address: ADDRESS, coinType: "0x2::sui::SUI", amount: "-102809" }],
  events: [{ index: 0, type: "0x2::x::E", package: ADDRESS, module: "x", sender: ADDRESS, fields: { price: "913618816341183", ok: true } }],
  unchangedConsensusObjects: [],
  dependencies: [],
  effectsDigest: "6N4pKNnbH4ZbbBA3BhmmoNknL6CNLU7VuYc48oVNVP3u",
  eventsDigest: null,
  lamportVersion: 1073105609,
};

describe("read client", () => {
  test("transactions: path, includes and a decoded record", async () => {
    const { result, seen } = await run(
      Effect.flatMap(MiraiCloudRead, (sui) => sui.transactions.get({ params: { digest: TX.digest }, query: { include: include(["signatures", "bcs", "bcs"]) } })),
      Auth.Key({ token: Redacted.make("mc1_secret") }),
      () => json(200, TX),
    );
    expect(seen[0]!.url).toBe(`${ENDPOINT}/v1/transactions/${TX.digest}?include=bcs%2Csignatures`);
    expect(seen[0]!.headers.authorization).toBe("Bearer mc1_secret");
    expect(result._tag === "Success" && result.success).toEqual(TX);
  });

  test("objects at a version; owners decode as their kind", async () => {
    const object: SuiObject = {
      objectId: ADDRESS,
      version: 7,
      digest: "Bd8bjCKsyGebPcmbHqfKK5d5ow7czbJNBCVWXAV2gqE7",
      type: "0x2::coin::Coin<0x2::sui::SUI>",
      owner: { kind: "address", address: ADDRESS },
      previousTransaction: TX.digest,
      storageRebate: "988000",
      fields: { balance: "1" },
      balance: "1",
      package: null,
    };
    const { result, seen } = await run(Effect.flatMap(MiraiCloudRead, (sui) => sui.objects.get({ params: { objectId: ADDRESS }, query: { version: "7" } })), Auth.None(), () =>
      json(200, object),
    );
    expect(seen[0]!.url).toBe(`${ENDPOINT}/v1/objects/${ADDRESS}?version=7`);
    expect(result._tag === "Success" && result.success.owner).toEqual({ kind: "address", address: ADDRESS });
  });

  test("an owner-signed read is signed over its path and query", async () => {
    const signer = fromSuiSigner(new Ed25519Keypair());
    const { seen } = await run(Effect.flatMap(MiraiCloudRead, (sui) => sui.checkpoints.get({ params: { checkpoint: "latest" }, query: { include: "signature" } })), Auth.Owner({ signer }), () =>
      json(404, { _tag: "NotFound", message: "x" }),
    );
    const request = seen[0]!;
    const signed = message({
      audience: ENDPOINT,
      address: signer.address,
      method: "GET",
      path: "/v1/checkpoints/latest?include=signature",
      bodySha256: await Effect.runPromise(sha256Hex("")),
      timestamp: Number(request.headers[HEADERS.timestamp]),
      nonce: request.headers[HEADERS.nonce]!,
    });
    const publicKey = await verifyPersonalMessageSignature(new TextEncoder().encode(signed), request.headers[HEADERS.signature]!);
    expect(publicKey.toSuiAddress()).toBe(signer.address);
  });

  test("validators, and the gateway's quota refusal as a typed error", async () => {
    const set: ValidatorSet = {
      epoch: 1302,
      epochStart: 1793837144184,
      protocolVersion: 139,
      referenceGasPrice: "100",
      totalStake: "7071071007889239212",
      count: 0,
      nakamotoCoefficient: 0,
      thresholds: { minJoiningStake: "1", lowStake: "1", veryLowStake: "1", lowStakeGracePeriodEpochs: 7, maxValidators: 150 },
      apyWindow: null,
      validators: [],
    };
    const ok = await run(Effect.flatMap(MiraiCloudRead, (sui) => sui.validators.list()), Auth.None(), () => json(200, set));
    expect(ok.seen[0]!.url).toBe(`${ENDPOINT}/v1/validators`);
    expect(ok.result._tag === "Success" && ok.result.success.totalStake).toBe("7071071007889239212");
    const refused = await run(Effect.flatMap(MiraiCloudRead, (sui) => sui.validators.list()), Auth.None(), () => json(429, { _tag: "QuotaExceeded", message: "over the limit" }));
    expect(refused.result._tag === "Failure" && refused.result.failure).toBeInstanceOf(Errors.QuotaExceeded);
  });
});
