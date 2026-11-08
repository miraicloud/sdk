/**
 * `@miraicloud/sdk`: the mirai.cloud API from TypeScript, built on Effect.
 *
 * Subpaths load only what they need: `@miraicloud/sdk/model`,
 * `@miraicloud/sdk/sui` and `@miraicloud/sdk/errors` are plain types,
 * constants and `Data` errors (no `Schema`); `@miraicloud/sdk/contract`
 * holds the `HttpApi` definitions; `@miraicloud/sdk/client` the typed
 * clients derived from them; `@miraicloud/sdk/streams` the checkpoint stream
 * as an Effect `Stream`.
 */

export { Auth, DEFAULT_ENDPOINT, MiraiCloud, MiraiCloudRead } from "./client.js";
export { MiraiCloudStream } from "./streams.js";
export * as Client from "./client.js";
export * as Contract from "./contract.js";
export * as Delegation from "@miraicloud/core/delegation";
export * as Errors from "@miraicloud/core/errors";
export * as Model from "@miraicloud/core/model";
export * as Signed from "@miraicloud/core/signed";
export * as Signer from "@miraicloud/core/signer";
export * as Streams from "./streams.js";
export * as Sui from "@miraicloud/core/sui";
