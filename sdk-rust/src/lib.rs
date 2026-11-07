//! The [mirai.cloud](https://mirai.cloud) SDK for Rust.
//!
//! - [`streams`]: the checkpoint stream, every Sui checkpoint in order from
//!   any starting point (history from the Sui archive), as
//!   `sui.rpc.v2.Checkpoint`.
//! - [`delegation`]: Sui-native delegation: an account's owner signs, with
//!   any Sui signer, a plain-text delegation of part of the account's rights
//!   to an agent's Sui address; agents may delegate further (narrower).
//! - [`auth`]: [`Auth`](auth::Auth): an API key, or a
//!   [`TokenSource`](auth::TokenSource) that exchanges a delegation chain for
//!   short-lived API keys and renews them.
//!
//! Every API needs a mirai.cloud API key with the right scope (for
//! [`streams`], `stream:read`): create one with the `miraicloud` CLI
//! (`miraicloud keys create --name indexer --scope stream:read`), or give an
//! agent a delegation and let it exchange the delegation for keys
//! ([`auth`]).

pub mod auth;
pub mod delegation;
pub mod streams;

/// The public API (Mainnet).
pub const DEFAULT_ENDPOINT: &str = "https://api.mainnet.mirai.cloud";
