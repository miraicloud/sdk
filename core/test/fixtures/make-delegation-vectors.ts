// Delegation test vectors (deterministic: Ed25519 keys from fixed seeds, fixed nonces and times).
import { Ed25519Keypair } from "@mysten/sui/keypairs/ed25519";
import { checkChain, type Delegation, delegationId, delegationMessage, encodeChain, decodeChain } from "../../src/delegation.js";
import { Result } from "effect";
const kp = (n: number) => Ed25519Keypair.fromSecretKey(new Uint8Array(32).fill(n));
const owner = kp(1), agent = kp(2), sub = kp(3);
const audience = "https://api.mainnet.mirai.cloud";
const expires = Date.UTC(2030, 0, 1);
const d1: Delegation = { audience, account: owner.toSuiAddress(), issuer: owner.toSuiAddress(), subject: agent.toSuiAddress(), parent: null, scopes: ["stream:read", "rpc:read", "keys:write"], networks: ["mainnet"], origins: [], expiresAt: expires, limits: { monthlyRequests: 1000000, monthlyBytes: null }, nonce: "00112233445566778899aabbccddeeff" };
const m1 = delegationMessage(d1);
const s1 = (await owner.signPersonalMessage(new TextEncoder().encode(m1))).signature;
const d2: Delegation = { ...d1, issuer: agent.toSuiAddress(), subject: sub.toSuiAddress(), parent: delegationId(m1), scopes: ["stream:read"], expiresAt: Date.UTC(2029, 5, 1, 12, 30, 15), limits: { monthlyRequests: 5000, monthlyBytes: 1000000000 }, nonce: "ffeeddccbbaa99887766554433221100" };
const m2 = delegationMessage(d2);
const s2 = (await agent.signPersonalMessage(new TextEncoder().encode(m2))).signature;
const chain = [{ message: m1, signature: s1 }, { message: m2, signature: s2 }];
const encoded = encodeChain(chain);
const now = Date.UTC(2026, 10, 6);
const ok = checkChain(chain, { now, audience });
if (Result.isFailure(ok)) throw new Error(ok.failure);
if (encodeChain(Result.getOrThrow(decodeChain(encoded))) !== encoded) throw new Error("roundtrip");
const wider = { ...d2, scopes: ["stream:read", "rpc:write"] as Delegation["scopes"] };
const later = { ...d2, expiresAt: Date.UTC(2031, 0, 1) };
const looser = { ...d2, limits: { monthlyRequests: 2000000, monthlyBytes: null } };
const vectors = {
  description: "Delegation vectors: Ed25519 keys from seeds [1;32] (owner), [2;32] (agent), [3;32] (sub-agent); check with now = 2026-11-06T00:00:00Z and audience https://api.mainnet.mirai.cloud",
  now,
  audience,
  keys: { owner: { seed: "01".repeat(32), address: owner.toSuiAddress() }, agent: { seed: "02".repeat(32), address: agent.toSuiAddress() }, subAgent: { seed: "03".repeat(32), address: sub.toSuiAddress() } },
  delegations: [
    { message: m1, id: delegationId(m1), signature: s1 },
    { message: m2, id: delegationId(m2), signature: s2 },
  ],
  chain: encoded,
  ancestors: Result.getOrThrow(ok).ancestors,
  /** Each must fail checkChain when it replaces the second link (signed by the agent). */
  invalidSecondLinks: [
    { name: "scopes wider than the parent", message: delegationMessage(wider) },
    { name: "expiry later than the parent", message: delegationMessage(later) },
    { name: "limit looser than the parent", message: delegationMessage(looser) },
    { name: "wrong parent", message: delegationMessage({ ...d2, parent: "aaaaaaaaaaaaaaaa" }) },
    { name: "issuer is not the previous subject", message: delegationMessage({ ...d2, issuer: sub.toSuiAddress() }) },
    { name: "other audience", message: delegationMessage({ ...d2, audience: "https://example.com" }) },
  ],
  /** Each must fail parsing (not canonical or malformed). */
  invalidMessages: [
    { name: "scopes out of order", message: m1.replace("scopes: stream:read rpc:read keys:write", "scopes: rpc:read stream:read keys:write") },
    { name: "trailing newline", message: m1 + "\n" },
    { name: "milliseconds in expiry", message: m1.replace("2030-01-01T00:00:00Z", "2030-01-01T00:00:00.000Z") },
    { name: "uppercase address", message: m1.replace(owner.toSuiAddress().slice(2, 10), owner.toSuiAddress().slice(2, 10).toUpperCase()) },
    { name: "missing nonce", message: m1.split("\n").slice(0, 11).join("\n") },
  ],
};
for (const v of vectors.invalidSecondLinks) {
  const r = checkChain([chain[0]!, { message: v.message, signature: s2 }], { now, audience });
  if (Result.isSuccess(r)) throw new Error(`should fail: ${v.name}`);
}
await Bun.write(process.argv[2]!, JSON.stringify(vectors, null, 2) + "\n");
console.log(m1); console.log("id", delegationId(m1)); console.log(encoded.length, encoded.slice(0, 60));
