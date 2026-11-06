import { pipeline } from "node:stream/promises";

export interface StreamPipelineOptions {
  signal?: AbortSignal;
}

export type StreamStage =
  NodeJS.ReadableStream | NodeJS.WritableStream | NodeJS.ReadWriteStream | null | undefined | false;

/**
 * Executes a Node.js stream pipeline with type-safe variadic stages,
 * filtering out falsy stages (e.g. conditional transforms) and attaching an AbortSignal.
 */
export async function runStreamPipeline(
  stages: StreamStage[],
  options?: StreamPipelineOptions,
): Promise<void> {
  const activeStages = stages.filter(
    (stage): stage is NodeJS.ReadableStream | NodeJS.WritableStream | NodeJS.ReadWriteStream =>
      Boolean(stage),
  );

  if (activeStages.length < 2) {
    throw new Error(
      `Stream pipeline requires at least 2 active stages, received ${activeStages.length}`,
    );
  }

  const runner = pipeline as unknown as (...args: unknown[]) => Promise<void>;
  if (options?.signal) {
    await runner(...activeStages, { signal: options.signal });
  } else {
    await runner(...activeStages);
  }
}
