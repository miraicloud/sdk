# @miraicloud/sdk

The [mirai.cloud](https://mirai.cloud) API from TypeScript: Sui transactions, objects, checkpoints and validators as typed JSON, the checkpoint stream, plus your account, API keys and usage. Built on [Effect](https://effect.website).

**Your account is your Sui address.** There's no sign-up and no password: you authorize management calls by signing each request with your address's key, as a Sui personal message. Your first signed request creates the account. API keys (`mc1_…`) are what your applications use. Each carries scopes, networks, browser origins, an expiry and limits; only the owner creates them. Agents get their own Sui address and a **delegation** instead: you sign a grant of part of your account to it, and it exchanges that for short-lived keys.

```sh
npm install @miraicloud/sdk effect @mysten/sui
```

## Create an API key

```ts
import { Auth, Client, MiraiCloud, Signer } from "@miraicloud/sdk";
import { Ed25519Keypair } from "@mysten/sui/keypairs/ed25519";
import { Effect } from "effect";

const signer = Signer.fromSuiSigner(Ed25519Keypair.fromSecretKey(process.env.SUI_SECRET_KEY!));

const program = Effect.gen(function* () {
  const api = yield* MiraiCloud;
  const { token, key } = yield* api.keys.create({
    payload: { name: "indexer", scopes: ["stream:read", "rpc:read"], networks: ["mainnet"] },
  });
  // `token` is shown only here: store it as a secret.
  return { token, id: key.id };
});

Effect.runPromise(program.pipe(Effect.provide(Client.layerFetch({ auth: Auth.Owner({ signer }) }))));
```

Any `@mysten/sui` signer works: a keypair, a multisig or passkey signer, or your own.

Keys no longer create keys: `keys:write` is retired for API keys (no new key gets it; keys that already hold it can still revoke and rotate themselves and the keys they created). A key you create without `expiresAt` never expires.

## Delegate to an agent

An agent (a CI job, a bot, a worker) gets its own Sui address. The owner signs a delegation to it, offline, with any `@mysten/sui` signer; nothing is stored until the agent uses it:

```ts
import { Auth, Client, Delegation, Signer, Streams } from "@miraicloud/sdk";
import { Ed25519Keypair } from "@mysten/sui/keypairs/ed25519";
import { Effect } from "effect";

// The owner: what the agent may do, for how long, up to how much.
const chain = await Effect.runPromise(
  Delegation.delegate(signer, {
    to: "0xAGENT…",
    scopes: ["stream:read", "rpc:read"],   // default: every scope but keys:write
    expiresAt: Date.now() + 30 * 86_400_000, // default: min(the issuer's expiry, 30 days)
    limits: { monthlyRequests: 1_000_000 },  // bounds everything below this delegation
  }),
);
const text = Delegation.encodeChain(chain); // mcd1_…: hand it to the agent

// The agent: its own key; the chain is exchanged for short-lived API keys, refreshed before they expire.
const agent = Signer.fromSuiSigner(Ed25519Keypair.fromSecretKey(process.env.AGENT_SECRET_KEY!));
const auth = Auth.Delegated({ chain: text, signer: agent });
const api = Client.layerFetch({ auth });   // MiraiCloud, MiraiCloudRead
const live = Streams.layerFetch({ auth }); // MiraiCloudStream: reconnects onto each fresh key from its position
```

- **The delegation** is a plain-text Sui personal message (what a wallet shows the owner); its id (16 characters) is what revocation names and what exchanged keys carry as ancestors. A chain (`mcd1_…`) is the delegations from the owner to the agent, at most 8.
- **Delegating further:** an agent whose delegation includes `keys:write` passes part of it on: `Delegation.delegate(agent, { to: "0xSUB…" }, { parent: chain })`. Rights only narrow.
- **`Auth.Delegated({ chain, signer, lifetimeSeconds?, scopes? })`** exchanges the chain at `POST /v1/token`, signed by the agent: keys last an hour by default (at most 24 h, never past the chain), are refreshed when max(60 s, 10 %) of their life remains, one exchange for every caller; `429` and `503` are retried. Exchanged keys aren't listed among the account's keys.
- **Revoking** a delegation stops every key exchanged under it and everything delegated from it, within 45 s (open streams within another 30 s): `api.delegation.revoke({ params: { id }, payload: { chain } })`, as the owner (`Auth.Owner`) or an agent above it (`Auth.Delegated`).
- **Owners:** any Sui signer, including multisigs, passkeys and zkLogin. Keys exchanged from a chain with a zkLogin signature expire by the end of the proof's `max_epoch` (about 30 days at most): re-sign before then.
- `checkChain`, `describeChain`, `decodeChain` and `delegationId` inspect chains. The format, exchange and revocation are documented at [docs.mirai.cloud](https://docs.mirai.cloud/getting-started/delegation/).

## Reading Sui data

`MiraiCloudRead` reads the chain over HTTP with an API key that has `rpc:read`:

```ts
import { Auth, Client, MiraiCloudRead } from "@miraicloud/sdk";
import { Effect, Redacted } from "effect";

const program = Effect.gen(function* () {
  const sui = yield* MiraiCloudRead;
  const tx = yield* sui.transactions.get({ params: { digest: "2gb7RepDK8R2HCWhrxvJLfYsELNk2BAU5oJZyuEBzmYn" }, query: {} });
  // The gas coin as it is now (`NotFound` once it's deleted or wrapped).
  const gasCoin = tx.gas.object === null ? null : yield* sui.objects.get({ params: { objectId: tx.gas.object.objectId }, query: {} }).pipe(Effect.catchTag("NotFound", () => Effect.succeed(null)));
  const latest = yield* sui.checkpoints.get({ params: { checkpoint: "latest" }, query: {} });
  const { validators } = yield* sui.validators.list();
  return { status: tx.status, fee: tx.gas.used.fee, checkpoint: latest.sequenceNumber, top: validators[0]?.name };
});

Effect.runPromise(program.pipe(Effect.provide(Client.layerFetch({ auth: Auth.Key({ token: Redacted.make(process.env.MIRAICLOUD_API_KEY!) }) }))));
```

| call | what | cached |
|---|---|---|
| `sui.transactions.get({ params: { digest } })` | a finalized transaction: sender, status and error, gas, inputs and commands, object and balance changes, events | for good |
| `sui.objects.get({ params: { objectId } })` | an object's latest state | never (always live) |
| `sui.objects.get({ params: { objectId }, query: { version: "123" } })` | the object at a version | for good |
| `sui.checkpoints.get({ params: { checkpoint } })` | a checkpoint by number, digest, or `"latest"`, with a summary of each transaction | for good (`latest`: always live) |
| `sui.validators.list()` | the active validator set: stake, voting power, commission, APY, keys, addresses | never (always live) |
| `sui.validators.get({ params: { validatorId } })` | one validator, by address or staking pool id | never (always live) |

Expansions: `query: { include: Client.include(["signatures", "objects", "bcs"]) }` for transactions, `["display", "bcs", "checkpoint"]` for objects, `["signature", "bcs"]` for checkpoints.

Conventions:
- Sequence numbers, versions, epochs and times (unix ms) are numbers.
- Coin amounts are decimal strings (`Amount`; `Sui.toBigInt` converts).
- `null` means "doesn't exist".
- Move values are the chain's own JSON rendering.

The types are in `@miraicloud/sdk/sui`.

## Streams

`MiraiCloudStream` delivers Sui data as it happens, as Effect `Stream`s, with an API key that has `stream:read`: **the checkpoint stream** (`checkpoints`, `rawCheckpoints`), every checkpoint, complete (every field the node serves, BCS included), as Sui's gRPC `Checkpoint` from `@mysten/sui/grpc`. You can start from any checkpoint since genesis.

### The checkpoint stream

```ts
import { Auth, Streams } from "@miraicloud/sdk";
import { Console, Effect, Redacted, Stream } from "effect";

const program = Effect.gen(function* () {
  const live = yield* Streams.MiraiCloudStream;
  yield* live.checkpoints({ from: 341_000_000n, readMask: ["sequence_number", "transactions.digest", "transactions.effects.status"] }).pipe(
    Stream.runForEach((item) =>
      item._tag === "Gap"
        ? Console.warn(`checkpoints ${item.from}..=${item.to} can't be delivered`)
        : Console.log(item.seq, item.checkpoint.transactions.length),
    ),
  );
});

Effect.runPromise(program.pipe(Effect.provide(Streams.layerFetch({ auth: Auth.Key({ token: Redacted.make(process.env.MIRAICLOUD_API_KEY!) }) }))));
```

| option | |
|---|---|
| `from` | the first checkpoint (inclusive); default: the live tip |
| `after` | resume after this checkpoint: the last `seq` (or a gap's `to`) you processed. At most one of `from` and `after` |
| `readMask` | `sui.rpc.v2.Checkpoint` field paths to keep, e.g. `["sequence_number", "transactions.effects"]`; default: everything. BCS encodings (`Streams.BCS_PATHS`) can be named like any other field |
| `archive` | where history comes from: `{ client?, host?, concurrency? }`, default the Mainnet archive; `false`: no history (older positions become a `Gap`) |
| `stallTimeoutMs` | reconnect when nothing arrives for this long (default 45 000; keepalives come every 15 s) |
| `zstdDecoder` | for runtimes without `node:zlib` zstd |
| `onDiagnostic` | `Connect`, `Backfill` and `Disconnect` notices, for logs and metrics |

Items come strictly in order, each checkpoint once: `Checkpoint { seq, checkpoint }`, or `Gap { from, to }` for checkpoints that can't be delivered (the stream goes on after it). `rawCheckpoints` gives the same items with each checkpoint as its encoded protobuf bytes (`Checkpoint { seq, data }`), for storing, forwarding or decoding selectively.

**History.** The stream holds the last few minutes. When you start further back, the SDK reads the older checkpoints from the Sui archive (`archive.mainnet.sui.io`, over native gRPC with `@grpc/grpc-js`, so in Node or Bun), in the same shape and under the same mask, then joins the stream without a hole or a duplicate.

**Staying connected.** A dropped or stalled connection reconnects by itself (1 s, doubling to 30 s, at least what the server asks for) and resumes after the last checkpoint delivered. To survive a restart, store the last `seq` you processed and pass it as `after`.

**Errors.** The stream fails only when it's refused for good: `Unauthorized`, `Forbidden` (e.g. no `stream:read`), `NotFound`, `QuotaExceeded`, or `Invalid` (also for invalid options, such as an unknown `readMask` path).

**Runtimes.** The stream is zstd-compressed; Node 22.15 or later and Bun decode it out of the box.

### Without Effect

Every stream is an Effect `Stream`; `Stream.toAsyncIterable` turns one into a plain async iterable:

```ts
const layer = Streams.layerFetch({ auth: Auth.Key({ token: Redacted.make(process.env.MIRAICLOUD_API_KEY!) }) });
const checkpoints = Stream.unwrap(Effect.map(Streams.MiraiCloudStream, (live) => live.checkpoints({ after: lastProcessed }))).pipe(Stream.provide(layer));

for await (const item of Stream.toAsyncIterable(checkpoints)) {
  if (item._tag === "Checkpoint") await save(item.seq, item.checkpoint);
}
```

Leaving the loop (`break`, `return`, an exception) closes the connection.

## Account and keys

| call | what |
|---|---|
| `api.account.whoami()` | who you are signed in as, and your account |
| `api.account.usage()` | this month's usage and limits; per-key usage for keys and delegations with limits of their own |
| `api.keys.list({ query: { revoked: "true" } })` | the account's keys (revoked ones too, if asked) |
| `api.keys.create({ payload })` | a new key and its token (shown once); owner only |
| `api.keys.get({ params: { id } })` | one key |
| `api.keys.rotate({ params: { id }, payload: { graceSeconds } })` | a new token for the key; the old one keeps working for the grace period |
| `api.keys.revoke({ params: { id } })` | revoke the key and every key it created |
| `api.delegation.token({ payload: { chain, expiresIn?, scopes? } })` | exchange a delegation chain for a key, signed as the agent (`Auth.Delegated` does it for you) |
| `api.delegation.revoke({ params: { id }, payload: { chain } })` | revoke a delegation |

Errors are typed: each call fails with the classes in `@miraicloud/sdk/errors` (`Unauthorized`, `Forbidden`, `NotFound`, `Invalid`, `QuotaExceeded`, `RateLimited`, `Unavailable`), so you can handle them with `Effect.catchTag`. `RateLimited` (429, with `Retry-After`) is a rate limit: 1,000 requests per 10 seconds per key and 2,000 per account, 120 a minute per IP address on the account and key calls, and the account's caps on key creation (10 a second, bursts of 100) and token exchanges (60 a minute). These calls don't retry it for you (`Auth.Delegated`'s exchanges do): wait for the `Retry-After` the response carries (60 seconds for the per-key, per-account and per-IP limits) and retry. An IP address that presents 20 invalid keys or signatures within a minute is refused (`RateLimited`) for a minute. `Forbidden` also covers the account's caps on how many keys it holds (10,000 live keys, 1,000 children per key, chains 8 deep).

New accounts start on the free tier: 100,000 requests and 5 GB a month (`api.account.whoami()` shows the limits; `api.account.usage()` the usage).

## Entry points

| import | what | loads `Schema` |
|---|---|---|
| `@miraicloud/sdk` | everything below, as namespaces | yes |
| `@miraicloud/sdk/client` | `MiraiCloud`, `MiraiCloudRead`, `Auth`, `include`, `layer` (over any `HttpClient`), `layerFetch` | yes |
| `@miraicloud/sdk/streams` | `MiraiCloudStream` (the checkpoint stream), options, item types, `layer`, `layerFetch` | no |
| `@miraicloud/sdk/contract` | the APIs' `HttpApi` definitions (`Api`, `ReadApi`) and schemas, to derive your own client or OpenAPI document | yes |
| `@miraicloud/sdk/sui` | Sui types: `Transaction`, `SuiObject`, `Checkpoint`, `Validator`, `Owner`, `Amount`, … | no |
| `@miraicloud/sdk/model` | types and constants: `KeyInfo`, `Account`, `Usage`, `SCOPES`, `NETWORKS`, … | no |
| `@miraicloud/sdk/errors` | the error classes and their HTTP statuses | no |
| `@miraicloud/sdk/signed` | the signed-request format (headers and message) | no |
| `@miraicloud/sdk/signer` | `Signer`, `fromSuiSigner` | no |
| `@miraicloud/sdk/delegation` | delegations: `delegate`, `encodeChain` / `decodeChain`, `checkChain`, `describeChain`, ids | no |

`effect` and `@mysten/sui` are peer dependencies, so the SDK uses your copies. `@grpc/grpc-js` is loaded only when the checkpoint stream reads history from the archive.

From the command line, see the [`miraicloud` CLI](https://www.npmjs.com/package/@miraicloud/cli).

## License

Apache-2.0
