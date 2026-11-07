/**
 * Accounts, keys and usage, as the API serves them.
 *
 * An account is a Sui address. Its owner signs management requests with
 * that address's key (see `signed`); there is no sign-up and no password.
 * API keys carry scopes, networks, browser origins, an expiry and limits,
 * and a key may only create keys narrower than itself, so a CI key or a
 * browser key can be cut from a broader one without asking anyone.
 *
 * Plain types and constants: importing this never loads Effect's `Schema`.
 */

/** What a key may do. */
export const SCOPES = [
  "stream:read", // Streams: the checkpoint stream and the event streams
  "rpc:read", // read-only RPC
  "rpc:write", // RPC that changes state (e.g. executing transactions)
  "keys:read", // list and inspect the account's keys
  "keys:write", // retired for API keys (no new key gets it; existing ones still revoke and rotate themselves and theirs); in a delegation: may delegate further
  "usage:read", // the account's usage
] as const;
export type Scope = (typeof SCOPES)[number];

export const NETWORKS = ["mainnet", "testnet"] as const;
export type Network = (typeof NETWORKS)[number];

export const ACCOUNT_STATUSES = ["active", "suspended"] as const;
export type AccountStatus = (typeof ACCOUNT_STATUSES)[number];

/** Monthly limits; null means unlimited. */
export interface Limits {
  readonly monthlyRequests: number | null;
  readonly monthlyBytes: number | null;
}

export const UNLIMITED: Limits = { monthlyRequests: null, monthlyBytes: null };

/**
 * An account. `id` is its Sui address (`0x` + 64 lowercase hex); the owner's
 * first signed request creates it. `label`: an operator's note; null by default.
 */
export interface Account {
  readonly id: string;
  readonly label: string | null;
  readonly status: AccountStatus;
  readonly limits: Limits;
  readonly createdAt: number;
}

/**
 * An API key. Nothing secret is stored: the token is shown once, when the
 * key is created or rotated.
 * - `origins`: browser origins allowed to use the key; empty: any (server-side keys).
 * - `parentId`: the key that created this one; null: created by the account's owner.
 * - `ancestors`: the whole parent chain, nearest first. Revoking a key revokes
 *   every key it created.
 * - `kid`, `tokenIssuedAt`: the signing key and issue time of its latest token
 *   (`tokenIssuedAt` moves on rotation).
 * Times are unix milliseconds; `expiresAt` null: never.
 */
export interface KeyInfo {
  readonly id: string;
  readonly accountId: string;
  readonly name: string;
  readonly scopes: ReadonlyArray<Scope>;
  readonly networks: ReadonlyArray<Network>;
  readonly origins: ReadonlyArray<string>;
  readonly limits: Limits;
  readonly createdAt: number;
  readonly expiresAt: number | null;
  readonly revokedAt: number | null;
  readonly parentId: string | null;
  readonly ancestors: ReadonlyArray<string>;
  readonly kid: number;
  readonly tokenIssuedAt: number;
}

/** Usage for one period (a calendar month, UTC). */
export interface Usage {
  readonly requests: number;
  readonly bytes: number;
}

/** `2026-11`: the usage period containing `ms`. */
export const periodOf = (ms: number): string => new Date(ms).toISOString().slice(0, 7);

/** Who is calling the API: the account's owner (by signature) or one of its keys. */
export type Principal = { readonly _tag: "Owner"; readonly address: string } | { readonly _tag: "Key"; readonly key: KeyInfo };

/** The account a caller acts for. */
export const accountOf = (p: Principal): string => (p._tag === "Owner" ? p.address : p.key.accountId);
