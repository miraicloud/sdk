/** The few byte helpers signed requests need (WebCrypto: Node, Bun, browsers, Workers). */

import { Effect } from "effect";

const TEXT = new TextEncoder();

/** Lowercase hex. */
export const hex = (bytes: Uint8Array): string => Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");

/** Hex SHA-256 of `data` (UTF-8 for strings). */
export const sha256Hex = (data: Uint8Array | string): Effect.Effect<string> =>
  Effect.promise(() => crypto.subtle.digest("SHA-256", typeof data === "string" ? TEXT.encode(data) : (data as Uint8Array<ArrayBuffer>))).pipe(
    Effect.map((digest) => hex(new Uint8Array(digest))),
  );
