/**
 * Signed requests: an account is a Sui address, and its owner authorizes
 * management calls by signing each request with that address's key, as a
 * Sui personal message. No account password or root key exists to leak.
 *
 * The signed message is plain text, so a wallet that shows it to a person
 * shows exactly what they authorize:
 *
 *   mirai.cloud request
 *   audience: https://api.mainnet.mirai.cloud
 *   address: 0x…
 *   method: POST
 *   path: /v1/keys
 *   body-sha256: …
 *   timestamp: 1793836000000
 *   nonce: 3f9a…
 *
 * It binds the request to one API (audience), one body, a two-minute window
 * and a nonce the server accepts once, so a captured request cannot be
 * replayed elsewhere, later, or with another body.
 *
 * Headers: `X-Miraicloud-Address`, `X-Miraicloud-Timestamp`, `X-Miraicloud-Nonce` and
 * `X-Miraicloud-Signature` (the serialized Sui signature, base64; any scheme:
 * Ed25519, Secp256k1, Secp256r1, multisig, passkey, zkLogin).
 */

import { isValidSuiAddress, normalizeSuiAddress } from "@mysten/sui/utils";
import { Clock, Effect, Option } from "effect";
import { hex, sha256Hex } from "./bytes.js";

/** Hex SHA-256 of the body (`body-sha256`). */
export { sha256Hex };

export const HEADERS = {
  address: "x-miraicloud-address",
  timestamp: "x-miraicloud-timestamp",
  nonce: "x-miraicloud-nonce",
  signature: "x-miraicloud-signature",
} as const;

/** How far a request's timestamp may be from the server's clock. */
export const MAX_SKEW_MS = 120_000;

export interface SignedFields {
  /** The API's origin, e.g. `https://api.mainnet.mirai.cloud`. */
  readonly audience: string;
  readonly address: string;
  readonly method: string;
  /** Path and query, e.g. `/v1/keys?revoked=true`. */
  readonly path: string;
  /** Hex SHA-256 of the body (of the empty string when there is none). */
  readonly bodySha256: string;
  readonly timestamp: number;
  readonly nonce: string;
}

export const message = (f: SignedFields): string =>
  [
    "mirai.cloud request",
    `audience: ${f.audience}`,
    `address: ${f.address}`,
    `method: ${f.method.toUpperCase()}`,
    `path: ${f.path}`,
    `body-sha256: ${f.bodySha256}`,
    `timestamp: ${f.timestamp}`,
    `nonce: ${f.nonce}`,
  ].join("\n");

/** The address in its canonical form (`0x` + 64 lowercase hex), if valid. */
export const parseAddress = (raw: string): Option.Option<string> => {
  const trimmed = raw.trim().toLowerCase();
  if (!/^0x[0-9a-f]{1,64}$/.test(trimmed)) return Option.none();
  const normalized = normalizeSuiAddress(trimmed);
  return isValidSuiAddress(normalized) ? Option.some(normalized) : Option.none();
};

/** A fresh single-use nonce (16 random bytes, hex). */
export const newNonce: Effect.Effect<string> = Effect.sync(() => hex(crypto.getRandomValues(new Uint8Array(16))));

/** The signed-request headers for a request, signed with `sign` (the owner's key). */
export const signedHeaders = <E>(
  sign: (message: string) => Effect.Effect<string, E>,
  request: { readonly audience: string; readonly address: string; readonly method: string; readonly path: string; readonly body: Uint8Array | string },
): Effect.Effect<Record<string, string>, E> =>
  Effect.gen(function* () {
    const timestamp = yield* Clock.currentTimeMillis;
    const nonce = yield* newNonce;
    const bodySha256 = yield* sha256Hex(request.body);
    const signature = yield* sign(message({ ...request, bodySha256, timestamp, nonce }));
    return { [HEADERS.address]: request.address, [HEADERS.timestamp]: String(timestamp), [HEADERS.nonce]: nonce, [HEADERS.signature]: signature };
  });

/** `<address>`, shortened for display: `0x1234…cdef`. */
export const shortAddress = (address: string): string => `${address.slice(0, 6)}…${address.slice(-4)}`;
