/**
 * Who signs an account owner's requests: anything that can sign a Sui
 * personal message for the account's address. Keys never leave the signer;
 * only signatures are sent.
 */

import type { Signer as SuiSigner } from "@mysten/sui/cryptography";
import { Data, Effect } from "effect";

export class SignerError extends Data.TaggedError("SignerError")<{ readonly message: string }> {}

export interface Signer {
  /** The account: the signer's Sui address. */
  readonly address: string;
  /** The serialized Sui signature (base64) of `message` as a personal message. */
  readonly sign: (message: string) => Effect.Effect<string, SignerError>;
}

/**
 * A signer from any `@mysten/sui` signer: a keypair (Ed25519, Secp256k1,
 * Secp256r1), a multisig or passkey signer, or your own implementation.
 */
export const fromSuiSigner = (signer: SuiSigner): Signer => ({
  address: signer.toSuiAddress(),
  sign: (message) =>
    Effect.tryPromise({
      try: () => signer.signPersonalMessage(new TextEncoder().encode(message)),
      catch: (e) => new SignerError({ message: `signing failed: ${e instanceof Error ? e.message : String(e)}` }),
    }).pipe(Effect.map((s) => s.signature)),
});
