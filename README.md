# mirai.cloud SDKs

Sign in with a Sui wallet, manage API keys and delegations, and stream Sui checkpoints from [mirai.cloud](https://mirai.cloud).

| | |
|---|---|
| [`sdk/`](sdk) | **`@miraicloud/sdk`**, the TypeScript SDK (Node 22+, Bun). `npm install @miraicloud/sdk effect @mysten/sui` |
| [`sdk-rust/`](sdk-rust) | **`miraicloud`**, the Rust SDK. `miraicloud = { git = "https://github.com/miraicloud/sdk" }` |
| [`core/`](core) | **`@miraicloud/core`**: signed requests, delegations, errors, models and the API contract. The TypeScript SDK re-exports it; you don't need to install it yourself. |

Documentation: [docs.mirai.cloud](https://docs.mirai.cloud). The command-line tool is at [miraicloud/cli](https://github.com/miraicloud/cli).

## Development

```sh
npm install
npm run typecheck
npm test
(cd sdk-rust && cargo test)
```

## License

Apache-2.0
