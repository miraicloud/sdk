#!/usr/bin/env bun
/**
 * The publishable package, in `dist/`: compiled JavaScript with type
 * declarations and source maps, and a manifest whose `exports` point at
 * them (the workspace's own manifest points at the TypeScript sources, so
 * the repository needs no build step). Publish with `npm publish ./dist`.
 */

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Data, Effect, FileSystem, Path } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

class BuildError extends Data.TaggedError("BuildError")<{ readonly message: string }> {}

interface Manifest {
  readonly exports: Record<string, string>;
  readonly [field: string]: unknown;
}

/** `./src/contract/index.ts` → `{ types: ./contract/index.d.ts, default: ./contract/index.js }`. */
const compiled = (source: string) => {
  const base = source.replace(/^\.\/src\//, "./").replace(/\.ts$/, "");
  return { types: `${base}.d.ts`, default: `${base}.js` };
};

/** The manifest to publish: compiled exports, no workspace-only fields. */
export const publishManifest = ({ exports, private: _private, scripts: _scripts, devDependencies: _dev, ...rest }: Manifest) => ({
  ...rest,
  exports: Object.fromEntries(Object.entries(exports).map(([path, source]) => [path, compiled(source)])),
  publishConfig: { access: "public" },
});

const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const root = path.resolve(import.meta.dirname, "..");
  const dist = path.join(root, "dist");

  yield* fs.remove(dist, { recursive: true }).pipe(Effect.ignore);
  const code = yield* spawner.exitCode(ChildProcess.make("npx", ["tsc", "-p", "tsconfig.build.json"], { cwd: root, stdout: "inherit", stderr: "inherit" }));
  if (code !== 0) return yield* new BuildError({ message: `tsc exited with ${code}` });

  const manifest = JSON.parse(yield* fs.readFileString(path.join(root, "package.json"))) as Manifest;
  yield* fs.writeFileString(path.join(dist, "package.json"), `${JSON.stringify(publishManifest(manifest), null, 2)}\n`);
  for (const file of ["README.md", "LICENSE"]) yield* fs.copyFile(path.join(root, file), path.join(dist, file));
  yield* Console.log(`built ${manifest.name}@${manifest.version} in ${path.relative(process.cwd(), dist) || "."}`);
});

if (import.meta.main) program.pipe(Effect.provide(NodeServices.layer), NodeRuntime.runMain);
