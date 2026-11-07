//! Who signs delegations and signed requests: anything that can sign a Sui
//! personal message for its address. Keys never leave the signer; only
//! signatures are sent.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// A signing failure (a wallet that refuses, a missing key, a remote signer
/// that is down).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("signing failed: {0}")]
pub struct SignerError(pub String);

/// What [`Signer::sign_personal_message`] returns.
pub type SignFuture<'a> = Pin<Box<dyn Future<Output = Result<String, SignerError>> + Send + 'a>>;

/// A Sui address's signer: a keypair ([`KeypairSigner`](super::KeypairSigner)),
/// a multisig, a passkey, a KMS or wallet bridge (implement it yourself).
pub trait Signer: Send + Sync {
    /// The signer's Sui address, canonical: `0x` and 64 lowercase hex digits.
    fn address(&self) -> String;

    /// Sign `message` as a Sui personal message (intent scope
    /// `PersonalMessage`, BCS `vector<u8>` of the message, BLAKE2b-256 of
    /// intent and BCS). Returns the serialized Sui signature
    /// (`flag || signature || public key` for simple schemes; any scheme
    /// the API accepts: multisig, passkey, zkLogin), standard base64.
    fn sign_personal_message<'a>(&'a self, message: &'a [u8]) -> SignFuture<'a>;
}

impl<S: Signer + ?Sized> Signer for Arc<S> {
    fn address(&self) -> String {
        (**self).address()
    }

    fn sign_personal_message<'a>(&'a self, message: &'a [u8]) -> SignFuture<'a> {
        (**self).sign_personal_message(message)
    }
}

impl<S: Signer + ?Sized> Signer for &S {
    fn address(&self) -> String {
        (**self).address()
    }

    fn sign_personal_message<'a>(&'a self, message: &'a [u8]) -> SignFuture<'a> {
        (**self).sign_personal_message(message)
    }
}

impl<S: Signer + ?Sized> Signer for Box<S> {
    fn address(&self) -> String {
        (**self).address()
    }

    fn sign_personal_message<'a>(&'a self, message: &'a [u8]) -> SignFuture<'a> {
        (**self).sign_personal_message(message)
    }
}

#[cfg(feature = "keypair")]
pub use keypair::KeypairSigner;

#[cfg(feature = "keypair")]
mod keypair {
    use super::{SignFuture, Signer, SignerError};
    use sui_crypto::SuiSigner;
    use sui_crypto::ed25519::Ed25519PrivateKey;
    use sui_crypto::secp256r1::Secp256r1PrivateKey;
    use sui_crypto::simple::SimpleKeypair;
    use sui_sdk_types::{MultisigMemberPublicKey, PersonalMessage};

    /// A Sui keypair held in memory: Ed25519 or Secp256r1 (`sui keytool`'s
    /// `suiprivkey1…` export of either is accepted).
    #[derive(Clone)]
    pub struct KeypairSigner {
        keypair: SimpleKeypair,
        address: String,
    }

    impl std::fmt::Debug for KeypairSigner {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("KeypairSigner")
                .field("scheme", &self.keypair.scheme())
                .field("address", &self.address)
                .finish_non_exhaustive()
        }
    }

    impl KeypairSigner {
        /// An Ed25519 key from its 32-byte seed (the private key).
        pub fn ed25519(seed: [u8; 32]) -> Self {
            Ed25519PrivateKey::new(seed).into()
        }

        /// A Secp256r1 key from its 32-byte scalar.
        pub fn secp256r1(secret: [u8; 32]) -> Self {
            Secp256r1PrivateKey::new(secret).into()
        }

        /// A fresh random Ed25519 key (keep [`to_suiprivkey`](Self::to_suiprivkey) to reuse it).
        pub fn generate_ed25519() -> Self {
            let mut seed = [0u8; 32];
            getrandom::getrandom(&mut seed).expect("the OS random number generator");
            Self::ed25519(seed)
        }

        /// A key in the Sui CLI's Bech32 format (`suiprivkey1…`, from
        /// `sui keytool export`).
        pub fn from_suiprivkey(text: &str) -> Result<Self, SignerError> {
            SimpleKeypair::from_suiprivkey(text.trim())
                .map(Self::from)
                .map_err(|e| SignerError(format!("not a usable suiprivkey: {e}")))
        }

        /// The key in the Sui CLI's Bech32 format (`suiprivkey1…`).
        pub fn to_suiprivkey(&self) -> Result<String, SignerError> {
            self.keypair
                .to_suiprivkey()
                .map_err(|e| SignerError(e.to_string()))
        }

        /// The underlying `sui-crypto` keypair.
        pub fn keypair(&self) -> &SimpleKeypair {
            &self.keypair
        }

        /// Sign synchronously (the same as [`Signer::sign_personal_message`]).
        pub fn sign_personal_message_sync(&self, message: &[u8]) -> Result<String, SignerError> {
            self.keypair
                .sign_personal_message(&PersonalMessage(message.into()))
                .map(|s| s.to_base64())
                .map_err(|e| SignerError(e.to_string()))
        }
    }

    impl From<SimpleKeypair> for KeypairSigner {
        fn from(keypair: SimpleKeypair) -> Self {
            let address = match keypair.public_key() {
                MultisigMemberPublicKey::Ed25519(pk) => pk.derive_address(),
                MultisigMemberPublicKey::Secp256k1(pk) => pk.derive_address(),
                MultisigMemberPublicKey::Secp256r1(pk) => pk.derive_address(),
                // A simple keypair is one of the three above.
                other => unreachable!("not a simple key: {other:?}"),
            };
            Self {
                keypair,
                address: address.to_string(),
            }
        }
    }

    impl From<Ed25519PrivateKey> for KeypairSigner {
        fn from(key: Ed25519PrivateKey) -> Self {
            SimpleKeypair::from(key).into()
        }
    }

    impl From<Secp256r1PrivateKey> for KeypairSigner {
        fn from(key: Secp256r1PrivateKey) -> Self {
            SimpleKeypair::from(key).into()
        }
    }

    impl Signer for KeypairSigner {
        fn address(&self) -> String {
            self.address.clone()
        }

        fn sign_personal_message<'a>(&'a self, message: &'a [u8]) -> SignFuture<'a> {
            let result = self.sign_personal_message_sync(message);
            Box::pin(std::future::ready(result))
        }
    }
}
