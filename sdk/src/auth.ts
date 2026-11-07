/**
 * How a client authenticates, and where it connects. Plain data (no
 * `Schema`): shared by the typed clients (`client.ts`) and the streams
 * (`stream.ts`).
 */

import { Data, type Redacted } from "effect";
import type { Scope } from "@miraicloud/core/model";
import type { Signer } from "@miraicloud/core/signer";

/** The public API (mainnet). */
export const DEFAULT_ENDPOINT = "https://api.mainnet.mirai.cloud";

/** How account calls authenticate. */
export type Auth = Data.TaggedEnum<{
  /** As the account's owner: every request signed with the account's key. */
  Owner: { readonly signer: Signer };
  /** As one of the account's API keys (`mc1_…`), within its scopes. */
  Key: { readonly token: Redacted.Redacted<string> };
  /**
   * As an agent holding a delegation chain (`mcd1_…`, see `delegation.ts`)
   * to `signer`'s address: the chain is exchanged for a short-lived API key
   * (`POST /v1/token`, signed by `signer`), refreshed before it expires;
   * streams reconnect onto the new key without a gap. Requests that need a
   * signature (revoking a delegation) are signed by `signer`.
   */
  Delegated: {
    readonly chain: string;
    readonly signer: Signer;
    /** Each key's lifetime in seconds (default 3600, at most 86400). */
    readonly lifetimeSeconds?: number | undefined;
    /** Narrower scopes than the chain's. */
    readonly scopes?: ReadonlyArray<Scope> | undefined;
  };
  /** Unauthenticated (health checks). */
  None: {};
}>;
export const Auth = Data.taggedEnum<Auth>();

export type DelegatedAuth = Extract<Auth, { readonly _tag: "Delegated" }>;
