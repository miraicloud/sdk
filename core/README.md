# @miraicloud/core

What the [mirai.cloud](https://mirai.cloud) TypeScript SDK and the API share, so neither duplicates the other: the `HttpApi` contract the SDK's clients are derived from and the server implements (`./contract`), the records' types (`./model`, `./sui`), the API's typed errors (`./errors`), signed requests (`./signed`), signers (`./signer`) and delegations (`./delegation`). Built on [Effect](https://effect.website).

Applications use [`@miraicloud/sdk`](https://www.npmjs.com/package/@miraicloud/sdk), which re-exports all of this; depend on this package directly only for the shared definitions without the clients.

```sh
npm install @miraicloud/core effect @mysten/sui
```

`bun test ./test` runs the tests (the delegation format against its vectors, `test/fixtures/delegation-vectors.json`, shared with the Rust SDK); `bun scripts/build.ts` builds the publishable package in `dist/`.
