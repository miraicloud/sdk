# miraicloud (Rust)

The [mirai.cloud](https://mirai.cloud) SDK for Rust. The TypeScript SDK is [`@miraicloud/sdk`](../sdk/README.md).

- `miraicloud::streams`: the checkpoint stream, every Sui checkpoint as `sui.rpc.v2.Checkpoint`, in order, from any starting point.
- `miraicloud::delegation`: Sui-native delegation. An account's owner signs, with any Sui signer, a plain-text grant of part of the account's rights to an agent's Sui address; the agent may grant part of that further.
- `miraicloud::auth`: `Auth`, an API key or a `TokenSource` that exchanges a delegation chain for short-lived API keys and renews them.

You need an API key with the `stream:read` scope (`miraicloud keys create --name indexer --scope stream:read`), or a delegation chain that grants it (see [Delegation](#delegation)).

Not on crates.io yet; depend on a release tag of the repository (Rust 1.90 or newer):

```toml
miraicloud = { git = "https://github.com/miraicloud/sdk", tag = "rust-v0.1.3" }
```

Checkpoints are `sui-rpc` 0.4's types (`miraicloud::streams::Checkpoint`); if you also depend on `sui-rpc`, use the same version.

## The checkpoint stream

```rust
use futures::StreamExt;
use miraicloud::streams::{Client, Event};

let mut sub = Client::builder()                       // default: the Mainnet stream and archive
    .api_key(std::env::var("MIRAICLOUD_API_KEY")?)    // sent as `authorization: Bearer …`
    .read_mask(["sequence_number", "digest"])         // default: the whole checkpoint, BCS included
    .resume_after(saved_cursor)                       // or .start_at(seq); default: the live tip
    .build()?                                         // validates the mask, URLs and headers
    .subscribe();                                     // impl Stream<Item = Result<Event, Error>>
while let Some(event) = sub.next().await {
    match event? {                                    // an Err is terminal
        Event::Checkpoint { seq, checkpoint } => {    // sui_rpc::proto::sui::rpc::v2::Checkpoint
            println!("{seq} {:?}", checkpoint.digest);
        }
        Event::Gap { from, to } => eprintln!("missed {from}..={to}"),
        _ => {}                                       // kinds added in later releases
    }
    save(sub.cursor());                               // and/or sub.last_delivered()
}
```

`subscribe_raw()` yields each checkpoint's encoded protobuf (`sui.rpc.v2.Checkpoint`) as `bytes::Bytes` instead, for callers that decode it themselves or pass it on, with every guarantee below. From the stream it is a slice of the connection's buffer, never decoded (copy it to keep only the payload alive); checkpoints read from the archive are re-encoded.

Other builder options: `url` (default `DEFAULT_STREAM_URL`), `auth` / `delegation` (see [Delegation](#delegation)), `header`, `archive(url)` (default `DEFAULT_ARCHIVE_URL`), `no_archive()`, `archive_concurrency`, `stall_timeout`. `subscribe()` needs a Tokio runtime; dropping the `Subscription` closes the connection. Checkpoint types come from crates.io `sui-rpc`.

- **Order.** Checkpoints come strictly in sequence order, without duplicates. Anything that would skip a checkpoint without a gap is treated as a broken connection and reconnected.
- **History.** The stream holds only the most recent checkpoints. A start (or resume) further back is read from the archive (`archive.mainnet.sui.io`, every checkpoint since genesis) with `ListCheckpoints`, 50 checkpoints a page, `archive_concurrency` pages in flight (default 2), up to the archive's own tip; then the stream takes over, without a hole or a duplicate. Archive checkpoints come out exactly as the stream sends them (object types in effects filled, the mask applied the same way). With `no_archive()`, that range is a gap instead. Each page in flight is held in memory: with the whole checkpoint (no read mask) ~250 MB a page at today's Mainnet sizes, so a deep backfill peaks at ~600 MB (`archive_concurrency(1)`: ~370 MB, slower); with a read mask naming the fields you need, a few MB.
- **Masks.** The stream sends every client the whole checkpoint, BCS encodings included (`BCS_PATHS`). The SDK applies the read mask (`sui.rpc.v2.Checkpoint` field paths) locally, to live and archive checkpoints alike. With no mask (or `*`) you get everything; a parent path such as `summary` keeps the whole field, its `bcs` included. Invalid paths, and fields no node renders consistently (event and object JSON, object display), are rejected by `build()`.
- **Gaps.** Checkpoints that can't be delivered are yielded once as `Event::Gap`; the stream then continues at `to + 1`.
- **Position.** `last_delivered()`: the last checkpoint yielded. `cursor()`: everything up to it has been yielded, as a checkpoint or inside a gap; persist it and pass it to `resume_after`.
- **Reconnects.** Backoff 1 s doubling to 30 s, jittered (reset on progress), at least `Retry-After`. No bytes for 45 s (keepalives come every 15 s) means a dead connection (`stall_timeout`). A slow consumer pauses reading (bounded buffers) and is never mistaken for a stall. Archive failures are retried the same way, from where the backfill stopped.
- **Errors.** A subscription ends, with `Error::Http { status, body, tag }` as its last item (`tag`: the API error's `_tag`, e.g. `Unauthorized`, `QuotaExceeded`), only when it's refused for good: HTTP 400, 401, 403, 404 (before the stream has once answered, after 6 attempts over 15–30 s; later a 404 is retried), or 429 over quota (`QuotaExceeded`); with delegated auth, also with `Error::Auth` when the token exchange refuses the chain (revoked, expired, invalid). Network errors, 5xx, other 429s, stalls, malformed frames and archive outages are retried and logged with `tracing`.
- **Compression.** The stream is zstd (concatenated zstd frames); reqwest decodes it, over rustls.
- **Compatibility.** `Error`, `Event` and `RawEvent` are `#[non_exhaustive]`: match them with a wildcard arm, so later releases can add kinds without breaking your build. A builder's `Debug` shows header names, never their values.

## Delegation

An owner delegates part of an account to an agent's Sui address, offline (nothing is registered); the agent exchanges the chain for short-lived API keys:

```rust
use miraicloud::auth::{revoke, TokenOptions, TokenSource};
use miraicloud::delegation::{delegate, Chain, DelegateRequest, KeypairSigner, Scope, Signer};
use miraicloud::streams::Client;
use std::time::Duration;

// The owner (any `Signer`: a keypair here; implement the trait for a wallet, KMS or multisig).
let owner = KeypairSigner::from_suiprivkey(&owner_key)?;     // `sui keytool export` format
let agent = KeypairSigner::generate_ed25519();               // the agent's own key; it needs no SUI
let chain = delegate(
    &owner,
    &DelegateRequest::to(agent.address())                    // anything left out: the issuer's rights
        .scopes([Scope::StreamRead])                         // owner default: every scope but keys:write
        .expires_in(Duration::from_secs(7 * 86_400))         // default: min(issuer's, 30 days)
        .monthly_requests(1_000_000),
    None,                                                    // Some(&chain): delegate further (needs keys:write)
    None,                                                    // audience: default the Mainnet API
).await?;
let text = chain.to_string();                                // mcd1_…: hand it to the agent

// The agent.
let chain: Chain = text.parse()?;
let tokens = TokenSource::new(miraicloud::DEFAULT_ENDPOINT, chain.clone(), agent, TokenOptions::default())?;
let key = tokens.current().await?;                           // mc1_…, for any API (Bearer)
let sub = Client::builder().delegation(tokens).build()?.subscribe();

// Revocation: by the owner, or by the subject of the delegation or of one above it.
revoke(miraicloud::DEFAULT_ENDPOINT, &owner, &id, &chain).await?;      // a chain that contains `id`
```

- **Format.** The delegation is a plain-text Sui personal message, which is what a wallet shows the owner; `Delegation::parse` accepts only the canonical form and `Delegation::id` is the 16-character id that tokens list as ancestors and revocation names. A chain (`Chain`, text `mcd1_…`) is the delegations root first, each with its issuer's serialized Sui signature; `Chain::check(now, audience)` applies the rules the server applies (issuers, parents, `keys:write` to delegate further, rights only narrowing, expiry). The TypeScript implementation (`core/src/delegation.ts`, shared by the TypeScript SDK and the API) is the reference; `tests/delegation.rs` checks this crate against its vectors byte for byte.
- **Exchange.** `TokenSource` calls `POST /v1/token` as a signed request by the agent (headers `x-miraicloud-address`, `-timestamp`, `-nonce`, `-signature` over the request's audience, method, path, body hash, time and nonce), caches the token, and renews it when less than max(60 s, 10 % of its lifetime) remains (at most half the lifetime). Concurrent callers share one exchange; `429`, `502`, `503`, `504` and network errors are retried with backoff (`Retry-After` honoured); a renewal that fails while the token is still valid keeps the token. `TokenOptions` sets the lifetime (`expires_in`, default 1 h, 60 s to 24 h, never past the chain's expiry), narrower `scopes`, and the renewal margins.
- **Streams.** With `.auth(…)` or `.delegation(tokens)`, each connection presents the current token, and an open stream reconnects with a fresh one shortly before its token expires. It resumes from its cursor, so nothing is skipped or repeated, and the server never has to cut it.
- **Signers.** `KeypairSigner` (cargo feature `keypair`, on by default) holds an Ed25519 or Secp256r1 key and signs through `sui-crypto`. Anything else (a multisig, a passkey, zkLogin, a remote signer) implements `Signer`: `address()` and `sign_personal_message(bytes)` returning the serialized Sui signature in base64.

## Development

```sh
cargo test && cargo clippy --all-targets --all-features -- -D warnings
MIRAICLOUD_API_KEY=mc1_… cargo test -- --ignored      # live: the Mainnet stream, archive vs fullnode
MIRAICLOUD_API_KEY=mc1_… cargo run --release --example tail -- --mask sequence_number,digest --back 5000 --secs 120
cargo bench --bench decode                            # zstd, framing and decoding cost on recorded checkpoints
```

The tests check the delegation vectors (`tests/fixtures/delegation-vectors.json`) against the TypeScript SDK's.

## License

Apache-2.0
