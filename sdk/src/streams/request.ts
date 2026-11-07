/** Authenticating a stream request: every connection (and reconnection) is authorized afresh. */

import { Effect, Redacted } from "effect";
import { type HttpClient, HttpClientRequest } from "effect/http";
import type { Auth } from "../auth.js";
import { signedHeaders } from "@miraicloud/core/signed";
import { renewal, type TokenError, tokenFor } from "../token.js";

/** An authorized request, and (a delegated key) what completes when to reconnect with a fresh one: once it exists, well before this one expires. */
export interface Authorized {
  readonly request: HttpClientRequest.HttpClientRequest;
  readonly renew: Effect.Effect<void> | undefined;
}

/**
 * `request` as `auth`: a bearer key, a request signed by the owner for
 * `audience` (the endpoint's origin) and `target` (path and query), a
 * delegated agent's current key (exchanged from its chain at `endpoint`), or
 * anonymous. A signer that fails is a defect (check the signer before use).
 */
export const authorize = (
  request: HttpClientRequest.HttpClientRequest,
  auth: Auth,
  audience: string,
  target: string,
  connection: { readonly client: HttpClient.HttpClient; readonly endpoint: string },
): Effect.Effect<Authorized, TokenError> => {
  switch (auth._tag) {
    case "Key":
      return Effect.succeed({ request: HttpClientRequest.bearerToken(request, Redacted.value(auth.token)), renew: undefined });
    case "Owner":
      return signedHeaders(auth.signer.sign, { audience, address: auth.signer.address, method: "GET", path: target, body: "" }).pipe(
        Effect.orDie,
        Effect.map((headers) => ({ request: HttpClientRequest.setHeaders(request, headers), renew: undefined })),
      );
    case "Delegated":
      return Effect.map(tokenFor(auth, connection.endpoint, connection.client), (t) => ({
        request: HttpClientRequest.bearerToken(request, t.token),
        renew: renewal(auth, connection.endpoint, connection.client, t),
      }));
    case "None":
      return Effect.succeed({ request, renew: undefined });
  }
};
