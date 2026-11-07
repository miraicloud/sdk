/**
 * Typed clients, derived from `contract`'s definitions: `MiraiCloud` for
 * the management API (`Api`), `MiraiCloudRead` for Sui data over HTTP
 * (`ReadApi`: transactions, objects, checkpoints, validators). One layer
 * provides both, with the same endpoint and authentication.
 *
 *   const program = Effect.gen(function* () {
 *     const api = yield* MiraiCloud;
 *     const { token, key } = yield* api.keys.create({ payload: { name: "ci", scopes: ["stream:read"] } });
 *     const sui = yield* MiraiCloudRead;
 *     const tx = yield* sui.transactions.get({ params: { digest } });
 *   }).pipe(Effect.provide(Client.layerFetch({ auth: Auth.Owner({ signer }) })));
 *
 * Account calls authenticate as the owner (each request signed with the
 * account's Sui key, see `signed.ts`), as an API key, as a delegated agent,
 * or not at all.
 */

import { Context, Effect, Layer, Redacted } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest, UrlParams } from "effect/http";
import { HttpApiClient, HttpApiMiddleware } from "effect/http-api";
import { AccountAuth, AgentAuth, Api, ReadApi, ReadAuth } from "@miraicloud/core/contract";
import { Auth, DEFAULT_ENDPOINT } from "./auth.js";
import { signedHeaders } from "@miraicloud/core/signed";
import type { Signer } from "@miraicloud/core/signer";
import { forget, tokenFor } from "./token.js";

export { Auth, DEFAULT_ENDPOINT };

export class MiraiCloud extends Context.Service<MiraiCloud, HttpApiClient.ForApi<typeof Api>>()("@miraicloud/sdk/MiraiCloud") {}

/** Sui data over HTTP: transactions, objects, checkpoints, validators (needs `rpc:read`). */
export class MiraiCloudRead extends Context.Service<MiraiCloudRead, HttpApiClient.ForApi<typeof ReadApi>>()("@miraicloud/sdk/MiraiCloudRead") {}

/** `?include=` from a list of expansion names. */
export const include = (names: ReadonlyArray<string>): string => [...new Set(names)].sort().join(",");

export interface Options {
  /** Default `DEFAULT_ENDPOINT`. */
  readonly endpoint?: string | undefined;
  readonly auth: Auth;
}

/** Add the signed-request headers for `audience` (`signed.ts`). */
const signRequest = (request: HttpClientRequest.HttpClientRequest, signer: Signer, audience: string) =>
  Effect.gen(function* () {
    const body = request.body._tag === "Uint8Array" ? request.body.body : new Uint8Array();
    const query = UrlParams.toString(request.urlParams);
    // Middleware runs before the base URL is added: `url` is the path.
    const path = (request.url.startsWith("http") ? new URL(request.url).pathname : request.url) + (query === "" ? "" : `?${query}`);
    const headers = yield* signedHeaders(signer.sign, { audience, address: signer.address, method: request.method, path, body });
    return HttpClientRequest.setHeaders(request, headers);
  });

/**
 * Both clients, over any `HttpClient`. A signer that fails (a wallet that
 * refuses, a missing key) is a defect here: check the signer before use.
 */
export const layer = (options: Options): Layer.Layer<MiraiCloud | MiraiCloudRead, never, HttpClient.HttpClient> => {
  const base = (options.endpoint ?? DEFAULT_ENDPOINT).replace(/\/+$/, "");
  const audience = new URL(base).origin;
  const { auth } = options;
  const transformClient = (client: HttpClient.HttpClient) => client.pipe(HttpClient.mapRequest(HttpClientRequest.prependUrl(base)));
  type Next = { next: (r: HttpClientRequest.HttpClientRequest) => Effect.Effect<any, any>; request: HttpClientRequest.HttpClientRequest };
  /**
   * As the owner (signed), as a key (bearer), as a delegated agent (the key
   * exchanged from its chain, refreshed before it expires; once more after a
   * 401, in case the key was refused early), or anonymously.
   */
  const authenticate = ({ next, request }: Next) =>
    Auth.$match(auth, {
      Owner: ({ signer }) => Effect.flatMap(Effect.orDie(signRequest(request, signer, audience)), next),
      Key: ({ token }) => next(HttpClientRequest.bearerToken(request, Redacted.value(token))),
      Delegated: (delegated) =>
        Effect.flatMap(HttpClient.HttpClient, (client) => {
          const once = Effect.flatMap(tokenFor(delegated, base, client), (t) => next(HttpClientRequest.bearerToken(request, t.token)));
          return once.pipe(Effect.catchTag("Unauthorized", () => Effect.andThen(Effect.sync(() => forget(delegated)), once)));
        }),
      None: () => next(request),
    });
  /** Signed by any address (agents' calls): the owner's signer, or the delegated agent's. */
  const signAsAgent = ({ next, request }: Next) =>
    Auth.$match(auth, {
      Owner: ({ signer }) => Effect.flatMap(Effect.orDie(signRequest(request, signer, audience)), next),
      Delegated: ({ signer }) => Effect.flatMap(Effect.orDie(signRequest(request, signer, audience)), next),
      Key: () => next(request),
      None: () => next(request),
    });
  return Layer.mergeAll(Layer.effect(MiraiCloud, HttpApiClient.make(Api, { transformClient })), Layer.effect(MiraiCloudRead, HttpApiClient.make(ReadApi, { transformClient }))).pipe(
    Layer.provide(HttpApiMiddleware.layerClient(AccountAuth, authenticate)),
    Layer.provide(HttpApiMiddleware.layerClient(ReadAuth, authenticate)),
    Layer.provide(HttpApiMiddleware.layerClient(AgentAuth, signAsAgent)),
  );
};

/** Both clients over the platform's `fetch` (Node 18+, Bun, Deno, browsers, Workers). */
export const layerFetch = (options: Options): Layer.Layer<MiraiCloud | MiraiCloudRead> => layer(options).pipe(Layer.provide(FetchHttpClient.layer));
