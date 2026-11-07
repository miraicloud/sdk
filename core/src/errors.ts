/**
 * The API's errors, as values: what a client call fails with, and what the
 * server's handlers fail with.
 *
 * Plain `Data` errors: importing them never loads Effect's `Schema` module.
 * Their wire format and HTTP statuses are in `contract`, which encodes and
 * decodes these same classes, so `Effect.catchTag("NotFound", …)` works
 * the same on both sides.
 */

import { Data } from "effect";

/** Not signed, or signed or keyed with something the API doesn't accept. */
export class Unauthorized extends Data.TaggedError("Unauthorized")<{ readonly message: string }> {}

/** Authenticated, but not allowed: a missing scope, network or origin, or a suspended account. */
export class Forbidden extends Data.TaggedError("Forbidden")<{ readonly message: string }> {}

export class NotFound extends Data.TaggedError("NotFound")<{ readonly message: string }> {}

/** The request itself is wrong (e.g. a key wider than its creator). */
export class Invalid extends Data.TaggedError("Invalid")<{ readonly message: string }> {}

/** Over the account's or the key's monthly limits. */
export class QuotaExceeded extends Data.TaggedError("QuotaExceeded")<{ readonly message: string }> {}

/** Too many requests of one kind (e.g. key mints or token exchanges per account); retry after the `Retry-After` header's seconds. */
export class RateLimited extends Data.TaggedError("RateLimited")<{ readonly message: string }> {}

/** Temporarily unavailable; worth retrying. */
export class Unavailable extends Data.TaggedError("Unavailable")<{ readonly message: string }> {}

/** Each error's HTTP status. */
export const STATUS = { Unauthorized: 401, Forbidden: 403, NotFound: 404, Invalid: 400, QuotaExceeded: 429, RateLimited: 429, Unavailable: 503 } as const;

export type ApiError = Unauthorized | Forbidden | NotFound | Invalid | QuotaExceeded | RateLimited | Unavailable;

/** The HTTP status for an error (500 for anything else). */
export const statusOf = (e: { readonly _tag: string }): number => STATUS[e._tag as keyof typeof STATUS] ?? 500;
