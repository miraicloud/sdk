/**
 * Schemas for the records the API serves. The types are `../model.ts`'s
 * (plain types, so code that only needs them never loads `Schema`);
 * `satisfies` keeps each schema in step with its type.
 */

import { Schema } from "effect";
import * as M from "../model.js";

type Exactly<A> = Schema.Codec<A, A>;

export const Scope = Schema.Literals(M.SCOPES);
export const Network = Schema.Literals(M.NETWORKS);

/** Monthly limits; null means unlimited. */
export const Limits = Schema.Struct({
  monthlyRequests: Schema.NullOr(Schema.Number),
  monthlyBytes: Schema.NullOr(Schema.Number),
}) satisfies Exactly<M.Limits>;

/** An account: a Sui address, whose owner signs management requests. */
export const Account = Schema.Struct({
  id: Schema.String,
  /** An operator's note (e.g. the customer's name); null by default. */
  label: Schema.NullOr(Schema.String),
  status: Schema.Literals(M.ACCOUNT_STATUSES),
  limits: Limits,
  /** Concurrent checkpoint streams allowed (set by an operator); null: the default. */
  maxStreams: Schema.NullOr(Schema.Number),
  createdAt: Schema.Number,
}) satisfies Exactly<M.Account>;

/** A key as everyone may see it (nothing secret exists to hide). */
export const KeyInfo = Schema.Struct({
  id: Schema.String,
  accountId: Schema.String,
  name: Schema.String,
  scopes: Schema.Array(Scope),
  networks: Schema.Array(Network),
  /** Browser origins allowed to use the key; empty: any (server-side keys). */
  origins: Schema.Array(Schema.String),
  limits: Limits,
  createdAt: Schema.Number,
  expiresAt: Schema.NullOr(Schema.Number),
  revokedAt: Schema.NullOr(Schema.Number),
  /** The key that created this one; null: created by the account's owner. */
  parentId: Schema.NullOr(Schema.String),
  /** The whole parent chain, nearest first. */
  ancestors: Schema.Array(Schema.String),
  /** The signing key of its latest token. */
  kid: Schema.Number,
  /** When its latest token was issued (moves on rotation). */
  tokenIssuedAt: Schema.Number,
}) satisfies Exactly<M.KeyInfo>;

/** Usage for one period (a calendar month, UTC). */
export const Usage = Schema.Struct({ requests: Schema.Number, bytes: Schema.Number }) satisfies Exactly<M.Usage>;
