# mirai.cloud SDKs

Sign in with a Sui wallet, manage API keys and delegations, and stream Sui checkpoints from [mirai.cloud](https://mirai.cloud).

| | |
|---|---|
| [`sdk/`](sdk) | **`@miraicloud/sdk`**, the TypeScript SDK (Node 22.15+, Bun). Not on npm yet: see [Installing the TypeScript SDK](#installing-the-typescript-sdk). |
| [`sdk-rust/`](sdk-rust) | **`miraicloud`**, the Rust SDK. `miraicloud = { git = "https://github.com/miraicloud/sdk" }` |
| [`core/`](core) | **`@miraicloud/core`**: signed requests, delegations, errors, models and the API contract. The TypeScript SDK re-exports it; you don't need to install it yourself. |

Documentation: [docs.mirai.cloud](https://docs.mirai.cloud). The command-line tool is at [miraicloud/cli](https://github.com/miraicloud/cli).

## Installing the TypeScript SDK

Until `@miraicloud/sdk` is on npm, build it from this repository and install the packages it makes:

```sh
git clone https://github.com/miraicloud/sdk miraicloud-sdk
cd miraicloud-sdk && npm install && npm run build
npm pack ./core/dist ./sdk/dist          # miraicloud-core-0.1.0.tgz, miraicloud-sdk-0.1.0.tgz
cd /your/project
npm install /path/to/miraicloud-sdk/miraicloud-core-0.1.0.tgz /path/to/miraicloud-sdk/miraicloud-sdk-0.1.0.tgz effect@4.0.5 @mysten/sui
```

## Development

```sh
npm install
npm run typecheck
npm test
(cd sdk-rust && cargo test)
```

## License

Apache-2.0
