/**
 * Delegated authentication (`Auth.Delegated`): an agent exchanges its
 * delegation chain for a short-lived API key at `POST /v1/token`, signing the
 * request with its own Sui key, and keeps one valid key at hand: refreshed
 * when less than max(60 s, 10 % of its lifetime) remains, one exchange at a
 * time however many requests and streams share it. Rate limits and outages
 * are retried (up to three times, after `Retry-After`). While the key at hand
 * is still valid, nobody waits on its refresh, and a refresh that fails
 * leaves it in use until it expires.
 */

import { Effect, Exit } from "effect";
import { HttpClient, HttpClientRequest } from "effect/http";
import type { DelegatedAuth } from "./auth.js";
import { Forbidden, Invalid, RateLimited, Unauthorized, Unavailable } from "@miraicloud/core/errors";
import { signedHeaders } from "@miraicloud/core/signed";
import { SignerError } from "@miraicloud/core/signer";

/** A key from an exchange. */
export interface IssuedToken {
  readonly token: string;
  readonly id: string;
  /** When it expires (ms since the epoch, the server's clock). */
  readonly expiresAt: number;
  readonly ancestors: ReadonlyArray<string>;
  /**
   * When to exchange again, and when it expires, on this process's
   * monotonic clock (`monotonicNow`): its lifetime is measured on the
   * server's clock (its `Date` header), so a client clock that is off, or
   * jumps, changes nothing.
   */
  readonly refreshAt: number;
  readonly validUntil: number;
}

/** A clock that only moves forward (ms): wall-clock jumps don't touch it. */
export const monotonicNow = (): number => performance.now();

export type TokenError = Unauthorized | Forbidden | Invalid | RateLimited | Unavailable | SignerError;

export const TOKEN_PATH = "/v1/token";

/** When to refresh a key valid from `issuedAt` to `expiresAt`: max(60 s, 10 % of its lifetime) before it expires. */
export const refreshAt = (issuedAt: number, expiresAt: number): number => expiresAt - Math.min(Math.max(60_000, (expiresAt - issuedAt) / 10), (expiresAt - issuedAt) / 2);

/** One exchange in progress, shared; aborted when the last caller waiting on it leaves. */
interface Flight {
  readonly result: Promise<Exit.Exit<IssuedToken, TokenError>>;
  readonly controller: AbortController;
  waiters: number;
}

interface State {
  current?: IssuedToken;
  inflight?: Flight;
}
const states = new WeakMap<DelegatedAuth, State>();

const stateOf = (auth: DelegatedAuth): State => {
  let s = states.get(auth);
  if (s === undefined) states.set(auth, (s = {}));
  return s;
};

const errorOf = (status: number, body: { _tag?: unknown; message?: unknown }): TokenError => {
  const message = typeof body.message === "string" ? body.message : `HTTP ${status}`;
  switch (body._tag ?? status) {
    case "Unauthorized":
    case 401:
      return new Unauthorized({ message });
    case "Forbidden":
    case 403:
      return new Forbidden({ message });
    case "Invalid":
    case 400:
      return new Invalid({ message });
    case "RateLimited":
    case 429:
      return new RateLimited({ message });
    default:
      return new Unavailable({ message });
  }
};

/** One exchange, retried on rate limits and outages. */
export const exchange = (auth: DelegatedAuth, endpoint: string, client: HttpClient.HttpClient): Effect.Effect<IssuedToken, TokenError> =>
  Effect.gen(function* () {
    const base = endpoint.replace(/\/+$/, "");
    const body = JSON.stringify({ chain: auth.chain, ...(auth.lifetimeSeconds !== undefined ? { expiresIn: auth.lifetimeSeconds } : {}), ...(auth.scopes !== undefined ? { scopes: auth.scopes } : {}) });
    for (let attempt = 0; ; attempt++) {
      // Sent at (the lifetime is counted from here: conservative).
      const sentAt = monotonicNow();
      const headers = yield* signedHeaders(auth.signer.sign, { audience: new URL(base).origin, address: auth.signer.address, method: "POST", path: TOKEN_PATH, body });
      const request = HttpClientRequest.post(`${base}${TOKEN_PATH}`).pipe(HttpClientRequest.setHeaders({ ...headers, "content-type": "application/json" }), HttpClientRequest.bodyText(body, "application/json"));
      const outcome = yield* client.execute(request).pipe(
        Effect.flatMap((res) => Effect.map(res.text, (text) => ({ status: res.status, text, retryAfter: Number(res.headers["retry-after"]), date: Date.parse(res.headers["date"] ?? "") }))),
        Effect.mapError((e) => new Unavailable({ message: `cannot reach ${base}: ${e.message}` })),
        Effect.result,
      );
      let failure: TokenError;
      let waitMs = 1000 * 2 ** attempt;
      if (outcome._tag === "Success") {
        const { status, text, retryAfter, date } = outcome.success;
        let parsed: Record<string, unknown> = {};
        try {
          parsed = JSON.parse(text) as Record<string, unknown>;
        } catch {}
        if (status === 200 && typeof parsed.token === "string" && typeof parsed.expiresAt === "number") {
          // The lifetime on the server's clock: its Date header when this
          // process's clock is off by more than the header's 1 s resolution
          // (2 s, to be safe), else this clock (finer).
          const localNow = Date.now();
          const serverNow = Number.isFinite(date) && Math.abs(date - localNow) > 2_000 ? date : localNow;
          // No usable lifetime (no Date header and a clock far off, or a Date far from the clock
          // `expiresAt` is on): assume the server's shortest (60 s), a 401 renewing it if not.
          const measured = parsed.expiresAt - serverNow;
          const lifetime = measured > 0 ? measured : 60_000;
          return {
            token: parsed.token,
            id: String(parsed.id ?? ""),
            expiresAt: parsed.expiresAt,
            ancestors: Array.isArray(parsed.ancestors) ? (parsed.ancestors as string[]) : [],
            refreshAt: refreshAt(sentAt, sentAt + lifetime),
            validUntil: sentAt + lifetime,
          };
        }
        failure = errorOf(status, parsed);
        if (Number.isFinite(retryAfter) && retryAfter > 0) waitMs = Math.max(waitMs, Math.min(retryAfter, 60) * 1000);
      } else failure = outcome.failure;
      if ((failure._tag !== "RateLimited" && failure._tag !== "Unavailable") || attempt >= 3) return yield* Effect.fail(failure);
      yield* Effect.sleep(waitMs);
    }
  });

/** The exchange in progress, or a new one. */
const flightFor = (s: State, auth: DelegatedAuth, endpoint: string, client: HttpClient.HttpClient): Flight => {
  if (s.inflight !== undefined) return s.inflight;
  const controller = new AbortController();
  const flight: Flight = {
    controller,
    waiters: 0,
    result: Effect.runPromiseExit(exchange(auth, endpoint, client), { signal: controller.signal }).then((exit) => {
      if (Exit.isSuccess(exit)) s.current = exit.value;
      if (s.inflight === flight) delete s.inflight;
      return exit;
    }),
  };
  s.inflight = flight;
  return flight;
};

/** Wait for `flight`; the last waiter to leave early (interrupted) aborts it: its retries would run on for nobody. */
const join = (s: State, flight: Flight): Effect.Effect<Exit.Exit<IssuedToken, TokenError>> =>
  Effect.acquireUseRelease(
    Effect.sync(() => void flight.waiters++),
    () => Effect.promise(() => flight.result),
    () =>
      Effect.sync(() => {
        if (--flight.waiters > 0 || s.inflight !== flight) return;
        delete s.inflight;
        flight.controller.abort();
      }),
  );

/** A valid key for `auth`: the one at hand (refreshed in the background once due), or exchanged now. */
export const tokenFor = (auth: DelegatedAuth, endpoint: string, client: HttpClient.HttpClient): Effect.Effect<IssuedToken, TokenError> =>
  Effect.suspend(() => {
    const s = stateOf(auth);
    const now = monotonicNow();
    if (s.current !== undefined && now < s.current.refreshAt) return Effect.succeed(s.current);
    // Due but still valid: used while the refresh runs, so a slow or failing exchange (a 429 waits out Retry-After) holds nothing up.
    if (s.current !== undefined && now < s.current.validUntil) {
      flightFor(s, auth, endpoint, client);
      return Effect.succeed(s.current);
    }
    return Effect.flatMap(join(s, flightFor(s, auth, endpoint, client)), (exit): Effect.Effect<IssuedToken, TokenError> =>
      Exit.isSuccess(exit) ? Effect.succeed(exit.value) : Effect.failCause(exit.cause),
    );
  });

/**
 * When a connection authorized with `used` should reconnect: once a fresh
 * key exists (exchanged from its refresh time on), or, if that exchange
 * fails, when `used` expires. A connection is never dropped for a key that
 * doesn't exist yet while its own is still valid.
 */
export const renewal = (auth: DelegatedAuth, endpoint: string, client: HttpClient.HttpClient, used: IssuedToken): Effect.Effect<void> => {
  const expiry = Effect.suspend(() => Effect.sleep(Math.max(0, used.validUntil - monotonicNow())));
  const fresh = Effect.suspend(() => {
    const s = stateOf(auth);
    if (s.current !== undefined && s.current.token !== used.token && monotonicNow() < s.current.refreshAt) return Effect.void;
    return Effect.flatMap(join(s, flightFor(s, auth, endpoint, client)), (exit) => (Exit.isSuccess(exit) && exit.value.token !== used.token ? Effect.void : Effect.never));
  });
  return Effect.andThen(
    // At least a second: a renewal can never spin (a key whose lifetime is 0 renews once a second at most).
    Effect.suspend(() => Effect.sleep(Math.max(1000, used.refreshAt - monotonicNow()))),
    Effect.raceFirst(fresh, expiry),
  );
};

/** Forget the key at hand (after the API refused it): the next call exchanges again. */
export const forget = (auth: DelegatedAuth): void => {
  const s = states.get(auth);
  if (s !== undefined) delete s.current;
};

export { SignerError };
