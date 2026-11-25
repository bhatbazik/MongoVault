import type { LinkedAbortController } from "./types/abort.js";

export function linkedController(parent?: AbortSignal): LinkedAbortController {
  const controller = new AbortController();
  const forward = () => controller.abort(parent?.reason);

  if (parent?.aborted) forward();
  else parent?.addEventListener("abort", forward, { once: true });

  return {
    signal: controller.signal,
    abort: (reason?: unknown) => controller.abort(reason),
    dispose: () => parent?.removeEventListener("abort", forward),
  };
}

function isAbortLikeError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  return err.name === "AbortError" || (err as NodeJS.ErrnoException).code === "ABORT_ERR";
}

export async function settleAll<T extends readonly Promise<unknown>[]>(
  controller: AbortController | LinkedAbortController,
  tasks: [...T],
): Promise<{ [K in keyof T]: Awaited<T[K]> }> {
  let firstFailure = null as { reason: unknown } | null;
  for (const task of tasks) {
    task.catch((error) => {
      if (!firstFailure) {
        firstFailure = { reason: error };
      }
      if (!controller.signal.aborted) {
        controller.abort(error);
      }
    });
  }
  const results = await Promise.allSettled(tasks);
  const rejected = results.filter(
    (result): result is PromiseRejectedResult => result.status === "rejected",
  );
  if (rejected.length > 0) {
    const rootCause =
      firstFailure && !isAbortLikeError(firstFailure.reason)
        ? firstFailure.reason
        : (rejected.find((r) => !isAbortLikeError(r.reason))?.reason ??
          firstFailure?.reason ??
          rejected[0]!.reason);
    throw rootCause;
  }
  return results.map((result) => (result as PromiseFulfilledResult<unknown>).value) as {
    [K in keyof T]: Awaited<T[K]>;
  };
}
