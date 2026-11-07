/**
 * Read masks, applied in the SDK. The stream sends every client the same
 * checkpoint (the full canonical set, `CANONICAL`, BCS encodings included);
 * a read mask narrows what this SDK yields, live and from the archive alike.
 */

import { GrpcTypes } from "@mysten/sui/grpc";
import type { IMessageType } from "@protobuf-ts/runtime";
import { Result } from "effect";
import { Invalid } from "@miraicloud/core/errors";
import { CANONICAL, covers, effective } from "./archive.js";

/** Fields no Sui node renders consistently; never served. */
export const EXCLUDED_PATHS: readonly string[] = [
  "transactions.events.events.json",
  "objects.objects.json",
  "objects.objects.display",
];

/** Whether `path` names fields of `sui.rpc.v2.Checkpoint` (proto field names). */
const isCheckpointPath = (path: string): boolean => {
  let type = GrpcTypes.Checkpoint as unknown as IMessageType<object>;
  const parts = path.split(".");
  for (let i = 0; i < parts.length; i++) {
    const field = type.fields.find((f) => f.name === parts[i]);
    if (!field) return false;
    if (i < parts.length - 1) {
      if (field.kind !== "message") return false;
      type = field.T() as IMessageType<object>;
    }
  }
  return true;
};

/**
 * Validate a read mask (`sui.rpc.v2.Checkpoint` field paths) and resolve it
 * to the fields kept: `undefined` keeps the whole checkpoint (no mask, `*`,
 * or paths covering every served field). Fails with `Invalid` for
 * invalid paths and for fields that are not served (JSON renderings, display).
 */
export const resolveMask = (paths?: readonly string[]): Result.Result<readonly string[] | undefined, Invalid> => {
  const named = [...new Set((paths ?? []).map((p) => p.trim()).filter((p) => p.length > 0))];
  const invalid = (message: string) => Result.fail(new Invalid({ message }));
  for (const path of named) {
    if (path === "*") continue;
    if (!isCheckpointPath(path)) return invalid(`invalid readMask path ${JSON.stringify(path)}`);
    const excluded = EXCLUDED_PATHS.find((e) => covers(e, path));
    if (excluded) return invalid(`readMask path ${JSON.stringify(path)} is not served (excluded: ${excluded})`);
  }
  if (named.length === 0) return Result.succeed(undefined);
  const kept = effective(named);
  return Result.succeed(CANONICAL.every((c) => kept.some((k) => covers(k, c))) ? undefined : kept);
};
