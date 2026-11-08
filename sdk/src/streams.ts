/**
 * Streams: live Sui data as Effect `Stream`s, with an API key that has
 * `stream:read` (or as the account's owner).
 *
 * **The checkpoint stream** (`checkpoints`, `rawCheckpoints`): every
 * checkpoint, complete (every field the node serves, BCS included), as Sui's
 * gRPC `Checkpoint` from `@mysten/sui/grpc`. Strictly in order, from any
 * checkpoint: what the stream no longer holds (it keeps the last few
 * minutes) is read from the Sui archive by this SDK, which then hands over
 * to the stream. Read masks are applied locally.
 *
 *   const program = Effect.gen(function* () {
 *     const live = yield* MiraiCloudStream;
 *     yield* live.checkpoints({ readMask: ["sequence_number", "transactions.digest"] }).pipe(
 *       Stream.runForEach((item) => (item._tag === "Checkpoint" ? Console.log(item.seq, item.checkpoint.transactions.length) : Effect.void)),
 *     );
 *   }).pipe(Effect.provide(Streams.layerFetch({ auth: Auth.Key({ token }) })));
 *
 * It reconnects by itself and never delivers a checkpoint twice or skips
 * one silently: what can't be delivered comes as a `Gap`. Refusals end the
 * stream with the API's errors (`Unauthorized`, `Forbidden`, `Invalid`,
 * `NotFound`, `QuotaExceeded`); anything temporary is retried with backoff.
 */

import { Context, Effect, Layer, type Stream } from "effect";
import { FetchHttpClient, HttpClient } from "effect/http";
import { Auth, DEFAULT_ENDPOINT } from "./auth.js";
import * as Checkpoints from "./streams/checkpoints.js";

export { BCS_PATHS, MAINNET_ARCHIVE } from "./streams/archive.js";
export {
  type ArchiveConfig,
  type Checkpoint,
  CHECKPOINT_STREAM_PATH,
  CheckpointItem,
  type CheckpointStreamError,
  type CheckpointStreamOptions,
  Diagnostic,
  RawCheckpointItem,
  type ZstdDecoder,
} from "./streams/checkpoints.js";
export { CONTENT_TYPE } from "./streams/frame.js";
export { Auth, DEFAULT_ENDPOINT };

export interface MiraiCloudStreamShape {
  /**
   * The checkpoint stream: every checkpoint from `from` (default: the live
   * tip) or after `after`, as `GrpcTypes.Checkpoint`, in strict order; a
   * `Gap` for any that can't be delivered.
   */
  readonly checkpoints: (options?: Checkpoints.CheckpointStreamOptions) => Stream.Stream<Checkpoints.CheckpointItem, Checkpoints.CheckpointStreamError>;
  /** `checkpoints`, with each checkpoint left as its encoded protobuf bytes (for storing, forwarding, or decoding selectively). */
  readonly rawCheckpoints: (options?: Checkpoints.CheckpointStreamOptions) => Stream.Stream<Checkpoints.RawCheckpointItem, Checkpoints.CheckpointStreamError>;
}

export class MiraiCloudStream extends Context.Service<MiraiCloudStream, MiraiCloudStreamShape>()("@miraicloud/sdk/MiraiCloudStream") {}

export interface Options {
  /** Default `DEFAULT_ENDPOINT`. */
  readonly endpoint?: string | undefined;
  readonly auth: Auth;
}

/** The streams over any `HttpClient`. */
export const make = (options: Options): Effect.Effect<MiraiCloudStreamShape, never, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const endpoint = (options.endpoint ?? DEFAULT_ENDPOINT).replace(/\/+$/, "");
    const connection: Checkpoints.Connection = { client, endpoint, auth: options.auth };
    return {
      checkpoints: (o) => Checkpoints.checkpoints(connection, o),
      rawCheckpoints: (o) => Checkpoints.rawCheckpoints(connection, o),
    };
  });

export const layer = (options: Options): Layer.Layer<MiraiCloudStream, never, HttpClient.HttpClient> => Layer.effect(MiraiCloudStream, make(options));

/** Over the platform's `fetch` (Node ≥ 22.15 or Bun: the stream is zstd-compressed). */
export const layerFetch = (options: Options): Layer.Layer<MiraiCloudStream> => layer(options).pipe(Layer.provide(FetchHttpClient.layer));
