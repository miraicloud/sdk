/**
 * Delegations: the format against its test vectors (shared with the Rust
 * SDK), and `delegate` with any `@mysten/sui` signer. (`Auth.Delegated`
 * against a local server is tested in the SDK.)
 */

import { describe, expect, test } from "bun:test";
import { Ed25519Keypair } from "@mysten/sui/keypairs/ed25519";
import { Secp256r1Keypair } from "@mysten/sui/keypairs/secp256r1";
import { verifyPersonalMessageSignature } from "@mysten/sui/verify";
import { Effect, Result } from "effect";
import {
  checkChain,
  type Chain,
  decodeChain,
  DEFAULT_DELEGATION_LIFETIME_MS,
  delegate,
  delegationId,
  delegationMessage,
  encodeChain,
  MAX_CHAIN,
  parseDelegation,
} from "../src/delegation.js";
import { fromSuiSigner } from "../src/signer.js";
import vectors from "./fixtures/delegation-vectors.json" with { type: "json" };

const seeded = (byte: number) => Ed25519Keypair.fromSecretKey(new Uint8Array(32).fill(byte));

describe("the format: test vectors", () => {
  test("messages, ids, signatures and the chain reproduce byte for byte", async () => {
    const [owner, agent] = [seeded(1), seeded(2)];
    expect([owner.toSuiAddress(), agent.toSuiAddress(), seeded(3).toSuiAddress()]).toEqual([vectors.keys.owner.address, vectors.keys.agent.address, vectors.keys.subAgent.address]);
    for (const [i, v] of vectors.delegations.entries()) {
      const d = Result.getOrThrow(parseDelegation(v.message));
      expect(delegationMessage(d)).toBe(v.message);
      expect(delegationId(v.message)).toBe(v.id);
      const signer = i === 0 ? owner : agent;
      expect((await signer.signPersonalMessage(new TextEncoder().encode(v.message))).signature).toBe(v.signature);
    }
    const chain = Result.getOrThrow(decodeChain(vectors.chain));
    expect(chain.map((l) => l.message)).toEqual(vectors.delegations.map((d) => d.message));
    expect(encodeChain(chain)).toBe(vectors.chain);
    expect(Result.getOrThrow(checkChain(chain, { now: vectors.now, audience: vectors.audience })).ancestors).toEqual(vectors.ancestors);
  });

  test("invalid links and messages are refused", () => {
    const chain = Result.getOrThrow(decodeChain(vectors.chain));
    for (const v of vectors.invalidSecondLinks) {
      expect([v.name, Result.isFailure(checkChain([chain[0]!, { message: v.message, signature: chain[1]!.signature }], { now: vectors.now, audience: vectors.audience }))]).toEqual([v.name, true]);
    }
    for (const v of vectors.invalidMessages) expect([v.name, Result.isFailure(parseDelegation(v.message))]).toEqual([v.name, true]);
    // Expired at `now`.
    expect(Result.isFailure(checkChain(chain, { now: Date.UTC(2031, 0, 1), audience: vectors.audience }))).toBe(true);
  });
});

describe("delegate", () => {
  test("owner defaults: every scope but keys:write, both networks, 30 days (to the second); sub-delegation within the parent; at most 8 deep", async () => {
    const owner = new Secp256r1Keypair();
    const agent = new Ed25519Keypair();
    const before = Date.now();
    const d1 = await Effect.runPromise(delegate(fromSuiSigner(owner), { to: agent.toSuiAddress() }));
    const parsed = Result.getOrThrow(parseDelegation(d1[0]!.message));
    expect(parsed).toMatchObject({ audience: "https://api.mainnet.mirai.cloud", account: owner.toSuiAddress(), issuer: owner.toSuiAddress(), parent: null, networks: ["mainnet", "testnet"] });
    expect(parsed.scopes).not.toContain("keys:write");
    expect(parsed.expiresAt! % 1000).toBe(0);
    expect(parsed.expiresAt! - before).toBeLessThanOrEqual(DEFAULT_DELEGATION_LIFETIME_MS);
    expect(await verifyPersonalMessageSignature(new TextEncoder().encode(d1[0]!.message), d1[0]!.signature, { address: owner.toSuiAddress() })).toBeDefined();

    let chain: Chain = await Effect.runPromise(delegate(fromSuiSigner(owner), { to: agent.toSuiAddress(), scopes: ["stream:read", "keys:write"], limits: { monthlyRequests: 1000 } }));
    let holder = agent;
    for (let i = 1; i < MAX_CHAIN; i++) {
      const next = new Ed25519Keypair();
      chain = await Effect.runPromise(delegate(fromSuiSigner(holder), { to: next.toSuiAddress() }, { parent: chain }));
      holder = next;
    }
    expect(chain.length).toBe(MAX_CHAIN);
    const leaf = Result.getOrThrow(parseDelegation(chain.at(-1)!.message));
    expect(leaf).toMatchObject({ scopes: ["stream:read", "keys:write"], limits: { monthlyRequests: 1000, monthlyBytes: null } });
    const ninth = await Effect.runPromise(Effect.flip(delegate(fromSuiSigner(holder), { to: agent.toSuiAddress() }, { parent: chain })));
    expect(ninth.message).toContain("at most 8 deep");
    // Wider than the parent, or by someone the chain isn't for.
    expect((await Effect.runPromise(Effect.flip(delegate(fromSuiSigner(holder), { to: agent.toSuiAddress(), limits: { monthlyRequests: 5000 } }, { parent: chain.slice(0, 2) })))).message).toContain("parent chain is for");
  });
});
