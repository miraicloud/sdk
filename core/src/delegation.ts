/**
 * Delegations: an account's owner (any Sui signer: a keypair, a multisig, a
 * passkey, zkLogin) grants an agent's Sui address part of the account's
 * rights by signing a plain-text personal message; the agent may grant part
 * of that to another address, and so on (at most 8 links). Nothing is
 * registered: an agent exchanges its chain for a short-lived API key
 * (`POST /v1/token`, signed by the agent), and revoking a delegation's id
 * (`POST /v1/delegations/<id>/revoke`) refuses every token below it.
 *
 * The message is what a wallet shows the owner:
 *
 *   mirai.cloud delegation v1
 *   audience: https://api.mainnet.mirai.cloud
 *   account: 0x<owner>
 *   issuer: 0x<owner | parent agent>
 *   subject: 0x<agent>
 *   parent: <delegation id | ->
 *   scopes: stream:read rpc:read keys:write
 *   networks: mainnet
 *   origins: -
 *   expires: 2026-12-06T00:00:00Z
 *   limits: monthlyRequests=1000000 monthlyBytes=-
 *   nonce: 3f9a…
 *
 * Canonical form (what `delegationMessage` writes, and the only form
 * `parseDelegation` accepts): lines joined by "\n", no trailing newline;
 * addresses `0x` + 64 lowercase hex; scopes and networks in the order of
 * `SCOPES` and `NETWORKS`, at least one each; origins sorted, or `-`;
 * expiry to the second in UTC, or `-` (never); limits as non-negative
 * integers, or `-` (none); nonce 16–64 lowercase hex.
 *
 * - **Id**: the first 10 bytes of BLAKE2b-256 of the message's UTF-8 bytes,
 *   as 16 base32 characters (like a key id): tokens carry it in their
 *   ancestors, and revocation names it.
 * - **Chain**: `mcd1_` + base64url (no padding) of BCS
 *   `vector<struct { message: vector<u8>, signature: vector<u8> }>`, root
 *   first; `signature` is the serialized Sui signature's bytes.
 * - **Rules**: the root's issuer is the account and its parent `-`; each
 *   later issuer is the previous subject, its parent the previous id, and
 *   the previous delegation must include `keys:write` (here: "may delegate
 *   further"). Rights only narrow: scopes, networks and origins within the
 *   previous, expiry no later, limits no looser. `checkChain` applies them
 *   (signatures are verified by the server).
 */

import { blake2b } from "@noble/hashes/blake2.js";
import { Data, Effect, Option, Result } from "effect";
import { hex } from "./bytes.js";
import { type Limits, NETWORKS, type Network, SCOPES, type Scope } from "./model.js";
import { parseAddress } from "./signed.js";
import type { Signer, SignerError } from "./signer.js";

export const DELEGATION_HEADER = "mirai.cloud delegation v1";
export const CHAIN_PREFIX = "mcd1_";
/** At most this many delegations in a chain (the account's family depth). */
export const MAX_CHAIN = 8;
/** A chain longer than this (as text) is refused before decoding. */
export const MAX_CHAIN_LENGTH = 16_384;
/** Delegations expire after this by default (never later than their parent). */
export const DEFAULT_DELEGATION_LIFETIME_MS = 30 * 24 * 3600_000;

export class DelegationError extends Data.TaggedError("DelegationError")<{ readonly message: string }> {}

/** What a delegation grants. */
export interface Rights {
  readonly scopes: ReadonlyArray<Scope>;
  readonly networks: ReadonlyArray<Network>;
  readonly origins: ReadonlyArray<string>;
  /** Unix ms, to the second; null: never. */
  readonly expiresAt: number | null;
  readonly limits: Limits;
}

export interface Delegation extends Rights {
  /** The API's origin, e.g. `https://api.mainnet.mirai.cloud`. */
  readonly audience: string;
  readonly account: string;
  readonly issuer: string;
  readonly subject: string;
  /** The previous delegation's id; null for the root. */
  readonly parent: string | null;
  readonly nonce: string;
}

/** A delegation and its issuer's signature (the serialized Sui signature, base64). */
export interface SignedDelegation {
  readonly message: string;
  readonly signature: string;
}

export type Chain = ReadonlyArray<SignedDelegation>;

const ORIGIN = /^https?:\/\/[^/\s]+$/;
const ADDRESS = /^0x[0-9a-f]{64}$/;
const ID = /^[a-z2-7]{16}$/;
const NONCE = /^[0-9a-f]{16,64}$/;
const EXPIRES = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/;
const UTF8 = new TextEncoder();

const iso = (ms: number) => new Date(ms).toISOString().replace(/\.\d{3}Z$/, "Z");
const limit = (n: number | null) => (n === null ? "-" : String(n));

/** The delegation's message, in canonical form. */
export const delegationMessage = (d: Delegation): string =>
  [
    DELEGATION_HEADER,
    `audience: ${d.audience}`,
    `account: ${d.account}`,
    `issuer: ${d.issuer}`,
    `subject: ${d.subject}`,
    `parent: ${d.parent ?? "-"}`,
    `scopes: ${SCOPES.filter((s) => d.scopes.includes(s)).join(" ")}`,
    `networks: ${NETWORKS.filter((n) => d.networks.includes(n)).join(" ")}`,
    `origins: ${d.origins.length === 0 ? "-" : [...new Set(d.origins)].sort().join(" ")}`,
    `expires: ${d.expiresAt === null ? "-" : iso(d.expiresAt)}`,
    `limits: monthlyRequests=${limit(d.limits.monthlyRequests)} monthlyBytes=${limit(d.limits.monthlyBytes)}`,
    `nonce: ${d.nonce}`,
  ].join("\n");

const field = (line: string | undefined, name: string): string | undefined => (line?.startsWith(`${name}: `) ? line.slice(name.length + 2) : undefined);
const count = (raw: string): number | null | undefined => (raw === "-" ? null : /^(0|[1-9]\d{0,15})$/.test(raw) && Number.isSafeInteger(Number(raw)) ? Number(raw) : undefined);

/** A delegation from its message; the message must be in canonical form. */
export const parseDelegation = (text: string): Result.Result<Delegation, string> => {
  const lines = text.split("\n");
  if (lines.length !== 12 || lines[0] !== DELEGATION_HEADER) return Result.fail(`not a ${DELEGATION_HEADER} message`);
  const [audience, account, issuer, subject, parent, scopes, networks, origins, expires, limits, nonce] = [
    "audience",
    "account",
    "issuer",
    "subject",
    "parent",
    "scopes",
    "networks",
    "origins",
    "expires",
    "limits",
    "nonce",
  ].map((name, i) => field(lines[i + 1], name));
  if ([audience, account, issuer, subject, parent, scopes, networks, origins, expires, limits, nonce].some((v) => v === undefined)) return Result.fail("a line is missing or out of order");
  if (!ORIGIN.test(audience!)) return Result.fail("audience: an origin, e.g. https://api.mainnet.mirai.cloud");
  for (const [name, a] of [["account", account], ["issuer", issuer], ["subject", subject]] as const) if (!ADDRESS.test(a!)) return Result.fail(`${name}: 0x and 64 lowercase hex digits`);
  if (parent !== "-" && !ID.test(parent!)) return Result.fail("parent: a delegation id or -");
  const scopeList = scopes!.split(" ");
  if (!scopeList.every((s) => (SCOPES as ReadonlyArray<string>).includes(s))) return Result.fail(`scopes: from ${SCOPES.join(" ")}`);
  const networkList = networks!.split(" ");
  if (!networkList.every((n) => (NETWORKS as ReadonlyArray<string>).includes(n))) return Result.fail(`networks: from ${NETWORKS.join(" ")}`);
  const originList = origins === "-" ? [] : origins!.split(" ");
  if (!originList.every((o) => ORIGIN.test(o))) return Result.fail("origins: like https://app.example.com, or -");
  if (originList.length > 8) return Result.fail("origins: at most 8");
  if (expires !== "-" && !EXPIRES.test(expires!)) return Result.fail("expires: YYYY-MM-DDTHH:MM:SSZ or -");
  const expiresAt = expires === "-" ? null : Date.parse(expires!);
  if (expiresAt !== null && Number.isNaN(expiresAt)) return Result.fail("expires: not a date");
  const lm = /^monthlyRequests=(\S+) monthlyBytes=(\S+)$/.exec(limits!);
  const requests = lm === null ? undefined : count(lm[1]!);
  const bytes = lm === null ? undefined : count(lm[2]!);
  if (requests === undefined || bytes === undefined) return Result.fail("limits: monthlyRequests=<n|-> monthlyBytes=<n|->");
  if (!NONCE.test(nonce!)) return Result.fail("nonce: 16 to 64 lowercase hex digits");
  const d: Delegation = {
    audience: audience!,
    account: account!,
    issuer: issuer!,
    subject: subject!,
    parent: parent === "-" ? null : parent!,
    scopes: scopeList as Scope[],
    networks: networkList as Network[],
    origins: originList,
    expiresAt,
    limits: { monthlyRequests: requests, monthlyBytes: bytes },
    nonce: nonce!,
  };
  if (scopeList.length === 0 || scopes === "" || networks === "") return Result.fail("a delegation needs at least one scope and one network");
  // One text per delegation: anything else (order, duplicates, spacing) would change its id.
  if (delegationMessage(d) !== text) return Result.fail("not in canonical form (order, duplicates, spacing or a time with milliseconds)");
  return Result.succeed(d);
};

const BASE32 = "abcdefghijklmnopqrstuvwxyz234567";
const base32 = (bytes: Uint8Array): string => {
  let out = "";
  let buffer = 0;
  let bits = 0;
  for (const b of bytes) {
    buffer = ((buffer << 8) | b) & 0xfff;
    bits += 8;
    while (bits >= 5) {
      bits -= 5;
      out += BASE32[(buffer >>> bits) & 31];
    }
  }
  return out;
};

/** A delegation's id: BLAKE2b-256 of its message, the first 10 bytes, in base32 (16 characters). */
export const delegationId = (message: string): string => base32(blake2b(UTF8.encode(message), { dkLen: 32 }).subarray(0, 10));

// base64 / base64url, without relying on Uint8Array.fromBase64 (not everywhere yet).
const toBase64 = (bytes: Uint8Array): string => {
  let s = "";
  for (let i = 0; i < bytes.length; i += 0x8000) s += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(s);
};
const fromBase64 = (text: string): Uint8Array | undefined => {
  try {
    const s = atob(text);
    return Uint8Array.from(s, (c) => c.charCodeAt(0));
  } catch {
    return undefined;
  }
};
const toBase64Url = (bytes: Uint8Array) => toBase64(bytes).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
const fromBase64Url = (text: string) => (/^[A-Za-z0-9_-]*$/.test(text) ? fromBase64(text.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(text.length / 4) * 4, "=")) : undefined);

const uleb = (n: number): number[] => {
  const out: number[] = [];
  do {
    let b = n & 0x7f;
    n >>>= 7;
    if (n !== 0) b |= 0x80;
    out.push(b);
  } while (n !== 0);
  return out;
};

/** A chain as text: `mcd1_` + base64url(BCS). */
export const encodeChain = (chain: Chain): string => {
  const parts: number[] = [...uleb(chain.length)];
  for (const link of chain) {
    const m = UTF8.encode(link.message);
    const s = fromBase64(link.signature) ?? new Uint8Array();
    parts.push(...uleb(m.length), ...m, ...uleb(s.length), ...s);
  }
  return CHAIN_PREFIX + toBase64Url(Uint8Array.from(parts));
};

const STRICT_UTF8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

/** A chain from its text (shape only: see `checkChain`). */
export const decodeChain = (text: string): Result.Result<Chain, string> => {
  const t = text.trim();
  if (t.length > MAX_CHAIN_LENGTH || !t.startsWith(CHAIN_PREFIX)) return Result.fail(`a delegation chain starts with ${CHAIN_PREFIX}`);
  const bytes = fromBase64Url(t.slice(CHAIN_PREFIX.length));
  if (bytes === undefined) return Result.fail("not base64url");
  let at = 0;
  const read = (): number | undefined => {
    let n = 0;
    for (let shift = 0; shift < 32; shift += 7) {
      const b = bytes[at++];
      if (b === undefined) return undefined;
      n |= (b & 0x7f) << shift;
      if ((b & 0x80) === 0) return n >>> 0;
    }
    return undefined;
  };
  const vec = (): Uint8Array | undefined => {
    const n = read();
    if (n === undefined || at + n > bytes.length) return undefined;
    const v = bytes.subarray(at, at + n);
    at += n;
    return v;
  };
  const n = read();
  if (n === undefined || n === 0 || n > MAX_CHAIN) return Result.fail(`a chain has 1 to ${MAX_CHAIN} delegations`);
  const chain: SignedDelegation[] = [];
  for (let i = 0; i < n; i++) {
    const m = vec();
    const s = vec();
    if (m === undefined || s === undefined) return Result.fail("truncated chain");
    let message: string;
    try {
      message = STRICT_UTF8.decode(m);
    } catch {
      return Result.fail("a message is not UTF-8");
    }
    chain.push({ message, signature: toBase64(s) });
  }
  if (at !== bytes.length) return Result.fail("trailing bytes after the chain");
  return Result.succeed(chain);
};

const subset = <A>(child: ReadonlyArray<A>, parent: ReadonlyArray<A>) => child.every((c) => parent.includes(c));

/** Why `child` isn't within `parent` (attenuation), if it isn't. */
export const attenuationError = (parent: Rights, child: Rights): string | undefined => {
  if (!subset(child.scopes, parent.scopes)) return `scopes must be within ${parent.scopes.join(" ")}`;
  if (!subset(child.networks, parent.networks)) return `networks must be within ${parent.networks.join(" ")}`;
  if (parent.origins.length > 0 && (child.origins.length === 0 || !subset(child.origins, parent.origins))) return `origins must be within ${parent.origins.join(" ")}`;
  if (parent.expiresAt !== null && (child.expiresAt === null || child.expiresAt > parent.expiresAt)) return `expiry cannot be later than ${iso(parent.expiresAt)}`;
  for (const k of ["monthlyRequests", "monthlyBytes"] as const) {
    const p = parent.limits[k];
    const c = child.limits[k];
    if (p !== null && (c === null || c > p)) return `${k} cannot exceed ${p}`;
  }
  return undefined;
};

/** One link of a checked chain. */
export interface Link {
  readonly delegation: Delegation;
  readonly id: string;
  readonly message: string;
  readonly signature: string;
}

export interface CheckedChain {
  readonly links: ReadonlyArray<Link>;
  readonly account: string;
  /** The last delegation (its subject is the agent the chain is for). */
  readonly leaf: Delegation;
  /** The delegations' ids, nearest first (as an exchanged token's ancestors). */
  readonly ancestors: ReadonlyArray<string>;
}

/**
 * A chain's shape and rights: links, issuers and parents, attenuation, and
 * expiry at `now`; `audience` (if given) must be every delegation's. The
 * signatures are not checked here.
 */
export const checkChain = (chain: Chain, options: { readonly now: number; readonly audience?: string }): Result.Result<CheckedChain, string> => {
  if (chain.length === 0 || chain.length > MAX_CHAIN) return Result.fail(`a chain has 1 to ${MAX_CHAIN} delegations`);
  const links: Link[] = [];
  for (const [i, link] of chain.entries()) {
    const parsed = parseDelegation(link.message);
    if (Result.isFailure(parsed)) return Result.fail(`delegation ${i + 1}: ${parsed.failure}`);
    const d = parsed.success;
    const id = delegationId(link.message);
    const prev = links.at(-1);
    if (options.audience !== undefined && d.audience !== options.audience) return Result.fail(`delegation ${i + 1}: for ${d.audience}, not ${options.audience}`);
    if (prev === undefined) {
      if (d.issuer !== d.account) return Result.fail("the first delegation must be issued by the account");
      if (d.parent !== null) return Result.fail("the first delegation has no parent");
    } else {
      const p = prev.delegation;
      if (d.account !== p.account || d.audience !== p.audience) return Result.fail(`delegation ${i + 1}: another account or audience`);
      if (d.issuer !== p.subject) return Result.fail(`delegation ${i + 1}: issued by ${d.issuer}, but delegation ${i} was to ${p.subject}`);
      if (d.parent !== prev.id) return Result.fail(`delegation ${i + 1}: its parent is not delegation ${i}`);
      if (!p.scopes.includes("keys:write")) return Result.fail(`delegation ${i} does not allow delegating further (keys:write)`);
      const wider = attenuationError(p, d);
      if (wider !== undefined) return Result.fail(`delegation ${i + 1}: ${wider}`);
    }
    if (d.expiresAt !== null && d.expiresAt <= options.now) return Result.fail(`delegation ${i + 1} expired at ${iso(d.expiresAt)}`);
    links.push({ delegation: d, id, message: link.message, signature: link.signature });
  }
  return Result.succeed({ links, account: links[0]!.delegation.account, leaf: links.at(-1)!.delegation, ancestors: links.map((l) => l.id).reverse() });
};

/** What to delegate; anything left out is the issuer's (the owner's: every scope but keys:write), the networks mainnet, the expiry min(the issuer's, 30 days). */
export interface DelegateRequest {
  /** The agent's Sui address. */
  readonly to: string;
  readonly scopes?: ReadonlyArray<Scope> | undefined;
  readonly networks?: ReadonlyArray<Network> | undefined;
  readonly origins?: ReadonlyArray<string> | undefined;
  /** Unix ms (rounded down to the second); null: never (only if the issuer never expires). */
  readonly expiresAt?: number | null | undefined;
  readonly limits?: Partial<Limits> | undefined;
}

export interface DelegateOptions {
  /** The API's origin (default `https://api.mainnet.mirai.cloud`). */
  readonly audience?: string | undefined;
  /** The chain the signer holds, to delegate part of it further; absent: the signer is the account's owner. */
  readonly parent?: Chain | undefined;
}

/** Networks left out: mainnet (testnet isn't offered), or the issuer's if it can't grant mainnet. */
const defaultNetworks = (issuer: ReadonlyArray<Network>): ReadonlyArray<Network> => (issuer.includes("mainnet") ? ["mainnet"] : issuer);

const OWNER_RIGHTS: Rights = { scopes: SCOPES.filter((s) => s !== "keys:write"), networks: NETWORKS, origins: [], expiresAt: null, limits: { monthlyRequests: null, monthlyBytes: null } };

/** A fresh nonce (16 random bytes, hex). */
const nonce = (): string => hex(crypto.getRandomValues(new Uint8Array(16)));

/**
 * Sign a delegation to `request.to` with `signer` (any `@mysten/sui` signer,
 * through `fromSuiSigner`): as the account's owner, or, with
 * `options.parent`, as the agent the parent chain is for. The new chain
 * (the parent's links, then the new one), checked as the server will.
 */
export const delegate = (signer: Signer, request: DelegateRequest, options: DelegateOptions = {}): Effect.Effect<Chain, DelegationError | SignerError> =>
  Effect.gen(function* () {
    const now = Date.now();
    const fail = (message: string) => Effect.fail(new DelegationError({ message }));
    const to = parseAddress(request.to);
    if (Option.isNone(to)) return yield* fail(`not a Sui address: ${request.to}`);
    const audience = (options.audience ?? "https://api.mainnet.mirai.cloud").replace(/\/+$/, "");
    let base: { readonly account: string; readonly parent: string | null; readonly rights: Rights; readonly parentRights: Rights | null };
    if (options.parent === undefined) {
      base = { account: signer.address, parent: null, rights: OWNER_RIGHTS, parentRights: null };
    } else {
      const checked = checkChain(options.parent, { now, audience });
      if (Result.isFailure(checked)) return yield* fail(checked.failure);
      const leaf = checked.success.leaf;
      if (leaf.subject !== signer.address) return yield* fail(`the parent chain is for ${leaf.subject}, not this signer (${signer.address})`);
      if (!leaf.scopes.includes("keys:write")) return yield* fail("the parent delegation does not allow delegating further (keys:write)");
      if (options.parent.length >= MAX_CHAIN) return yield* fail(`delegations chain at most ${MAX_CHAIN} deep`);
      base = { account: checked.success.account, parent: checked.success.ancestors[0]!, rights: leaf, parentRights: leaf };
    }
    const defaultExpiry = Math.min(base.rights.expiresAt ?? Number.POSITIVE_INFINITY, now + DEFAULT_DELEGATION_LIFETIME_MS);
    const expiresAt = request.expiresAt === undefined ? defaultExpiry : request.expiresAt;
    const d: Delegation = {
      audience,
      account: base.account,
      issuer: signer.address,
      subject: to.value,
      parent: base.parent,
      scopes: request.scopes ?? base.rights.scopes,
      networks: request.networks ?? defaultNetworks(base.rights.networks),
      origins: (request.origins ?? base.rights.origins).map((o) => o.replace(/\/+$/, "")),
      expiresAt: expiresAt === null ? null : Math.floor(expiresAt / 1000) * 1000,
      limits: { monthlyRequests: request.limits?.monthlyRequests ?? base.rights.limits.monthlyRequests, monthlyBytes: request.limits?.monthlyBytes ?? base.rights.limits.monthlyBytes },
      nonce: nonce(),
    };
    if (d.scopes.length === 0 || d.networks.length === 0) return yield* fail("a delegation needs at least one scope and one network");
    if (d.expiresAt !== null && d.expiresAt <= now) return yield* fail("expiry must be in the future");
    if (base.parentRights !== null) {
      const wider = attenuationError(base.parentRights, d);
      if (wider !== undefined) return yield* fail(wider);
    }
    const message = delegationMessage(d);
    const parsed = parseDelegation(message);
    if (Result.isFailure(parsed)) return yield* fail(parsed.failure);
    const signature = yield* signer.sign(message);
    return [...(options.parent ?? []), { message, signature }];
  });

/** A chain's delegations, decoded (for display). */
export const describeChain = (chain: Chain): ReadonlyArray<Delegation & { readonly id: string }> =>
  chain.flatMap((l) => {
    const d = parseDelegation(l.message);
    return Result.isSuccess(d) ? [{ ...d.success, id: delegationId(l.message) }] : [];
  });
