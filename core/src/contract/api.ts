/**
 * The mirai.cloud management API: one schema-first definition, implemented
 * by the server and called through clients derived from it (the SDK's
 * `MiraiCloud`, the `miraicloud` CLI), so they cannot drift apart.
 *
 * `/v1/...`: an account's keys and usage. The caller is either the
 * account's owner, signing the request with the account's Sui address
 * (`X-Miraicloud-Signature` and friends, see `../signed.ts`), or one of its
 * API keys (`Authorization: Bearer mc1_…`), limited to its scopes; agents
 * exchange delegations, signed by their own Sui address.
 *
 * This is the public API. The server serves more (operators' endpoints),
 * defined with it, not here.
 */

import { Context, Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiMiddleware, HttpApiSecurity, OpenApi } from "effect/http-api";
import type { Principal } from "../model.js";
import { HEADERS } from "../signed.js";
import { Forbidden, Invalid, NotFound, RateLimited, Unauthorized, Unavailable } from "./errors.js";
import { Account, KeyInfo, Limits, Network, Scope, Usage } from "./schemas.js";

/** Who is calling: what `AccountAuth` gives the server's handlers. */
export class Caller extends Context.Service<Caller, Principal>()("mirai/Caller") {}

/** The Sui address that signed the request (an agent, or an owner): what `AgentAuth` gives the server's handlers. */
export class Agent extends Context.Service<Agent, { readonly address: string }>()("mirai/Agent") {}

/**
 * A request signed by any Sui address (the signed-request headers, as the
 * owner's): agents exchanging a delegation chain, and revoking delegations.
 * Signing creates no account.
 */
export class AgentAuth extends HttpApiMiddleware.Service<AgentAuth, { provides: Agent; requires: never }>()("mirai/AgentAuth", {
  requiredForClient: true,
  security: { signature: HttpApiSecurity.apiKey({ key: HEADERS.signature, in: "header" }) },
  error: [Unauthorized, Unavailable],
}) {}

export class AccountAuth extends HttpApiMiddleware.Service<AccountAuth, { provides: Caller; requires: never }>()("mirai/AccountAuth", {
  requiredForClient: true,
  // Tried in order: the owner's signature, then an API key.
  security: {
    signature: HttpApiSecurity.apiKey({ key: HEADERS.signature, in: "header" }),
    bearer: HttpApiSecurity.bearer,
  },
  error: [Unauthorized, Unavailable],
}) {}

/** A new key and its token; the token is only ever shown here. */
export const CreatedKey = Schema.Struct({ token: Schema.String, key: KeyInfo });
export type CreatedKey = typeof CreatedKey.Type;

export const CreateKey = Schema.Struct({
  name: Schema.String,
  scopes: Schema.optionalKey(Schema.Array(Scope)),
  networks: Schema.optionalKey(Schema.Array(Network)),
  origins: Schema.optionalKey(Schema.Array(Schema.String)),
  /** Unix ms; null: never (only if the creator never expires). Absent: the creator's. */
  expiresAt: Schema.optionalKey(Schema.NullOr(Schema.Number)),
  limits: Schema.optionalKey(
    Schema.Struct({
      monthlyRequests: Schema.optionalKey(Schema.NullOr(Schema.Number)),
      monthlyBytes: Schema.optionalKey(Schema.NullOr(Schema.Number)),
    }),
  ),
});
export type CreateKey = typeof CreateKey.Type;

export const Whoami = Schema.Struct({
  /** `owner`: signed by the account's address; `key`: an API key (in `key`). */
  as: Schema.Literals(["owner", "key"]),
  key: Schema.NullOr(KeyInfo),
  account: Account,
});

export const UsageReport = Schema.Struct({
  accountId: Schema.String,
  period: Schema.String,
  usage: Usage,
  limits: Limits,
  /** Per key, only the keys the caller manages. */
  keys: Schema.Record(Schema.String, Usage),
  accountOver: Schema.Boolean,
  keysOver: Schema.Array(Schema.String),
  /** When the reconciler last ran for this account; null: not yet this period. */
  updatedAt: Schema.NullOr(Schema.Number),
});
export type UsageReport = typeof UsageReport.Type;

const KeyId = { id: Schema.String };

export class KeysGroup extends HttpApiGroup.make("keys")
  .add(
    HttpApiEndpoint.get("list", "/", {
      query: { revoked: Schema.optionalKey(Schema.Literals(["true", "false"])) },
      success: Schema.Array(KeyInfo),
      error: Forbidden,
    }),
    HttpApiEndpoint.post("create", "/", { payload: CreateKey, success: CreatedKey, error: [Forbidden, Invalid, RateLimited] }),
    HttpApiEndpoint.get("get", "/:id", { params: KeyId, success: KeyInfo, error: [Forbidden, NotFound] }),
    HttpApiEndpoint.delete("revoke", "/:id", {
      params: KeyId,
      /** The revoked key (and with it every key it created, which tokens name as ancestors). */
      success: Schema.Array(KeyInfo),
      error: [Forbidden, NotFound],
    }),
    HttpApiEndpoint.post("rotate", "/:id/rotate", {
      params: KeyId,
      /** How long the old token keeps working (default an hour, at most a week). */
      payload: Schema.Struct({ graceSeconds: Schema.optionalKey(Schema.Number) }),
      success: CreatedKey,
      error: [Forbidden, NotFound, Invalid],
    }),
  )
  .middleware(AccountAuth)
  .prefix("/v1/keys")
  .annotateMerge(OpenApi.annotations({ title: "Keys", description: "An account's API keys" })) {}

export class AccountGroup extends HttpApiGroup.make("account")
  .add(
    HttpApiEndpoint.get("whoami", "/v1/whoami", { success: Whoami, error: NotFound }),
    HttpApiEndpoint.get("usage", "/v1/usage", { success: UsageReport, error: [Forbidden, NotFound] }),
  )
  .middleware(AccountAuth)
  .annotateMerge(OpenApi.annotations({ title: "Account" })) {}

/** Exchange a delegation chain (`mcd1_…`) for a short-lived API key; signed by the chain's last subject. */
export const TokenRequest = Schema.Struct({
  chain: Schema.String,
  /** The key's lifetime in seconds: default 3600, at most 86400 (and never past the chain's expiry). */
  expiresIn: Schema.optionalKey(Schema.Number),
  /** Narrower scopes than the chain's (default: the chain's, without keys:write). */
  scopes: Schema.optionalKey(Schema.Array(Scope)),
});
export type TokenRequest = typeof TokenRequest.Type;

export const ExchangedToken = Schema.Struct({
  /** An ordinary API key (`mc1_…`): its ancestors are the chain's delegation ids. */
  token: Schema.String,
  /** The key's id (not registered: listings don't show it). */
  id: Schema.String,
  expiresAt: Schema.Number,
  /** The delegations' ids, nearest first. */
  ancestors: Schema.Array(Schema.String),
});
export type ExchangedToken = typeof ExchangedToken.Type;

export const RevokeDelegation = Schema.Struct({
  /** The chain up to and including the delegation (it proves which account it belongs to, and who may revoke it). */
  chain: Schema.String,
});
export const RevokedDelegation = Schema.Struct({ id: Schema.String, revoked: Schema.Boolean });

export class DelegationGroup extends HttpApiGroup.make("delegation")
  .add(
    HttpApiEndpoint.post("token", "/v1/token", { payload: TokenRequest, success: ExchangedToken, error: [Invalid, Forbidden, RateLimited] }),
    /** Revoke a delegation: every token below it is refused within 45 s. Signed by the account's owner or by the subject of the delegation or of one above it. */
    HttpApiEndpoint.post("revoke", "/v1/delegations/:id/revoke", { params: KeyId, payload: RevokeDelegation, success: RevokedDelegation, error: [Invalid, Forbidden, RateLimited] }),
  )
  .middleware(AgentAuth)
  .annotateMerge(OpenApi.annotations({ title: "Delegations", description: "Owner-signed delegations to agents' Sui addresses, exchanged for short-lived API keys" })) {}

export class SystemGroup extends HttpApiGroup.make("system").add(HttpApiEndpoint.get("health", "/healthz", { success: Schema.String })) {}

export class Api extends HttpApi.make("mirai")
  .add(KeysGroup)
  .add(AccountGroup)
  .add(DelegationGroup)
  .add(SystemGroup)
  .annotateMerge(OpenApi.annotations({ title: "mirai.cloud API" })) {}
