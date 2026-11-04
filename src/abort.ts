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

export async function settleAll<T extends readonly Promise<unknown>[]>(
  controller: AbortController | LinkedAbortController,
  tasks: [...T],
): Promise<{ [K in keyof T]: Awaited<T[K]> }> {
  for (const task of tasks) {
    task.catch((error) => controller.abort(error));
  }
  const results = await Promise.allSettled(tasks);
  const failure = results.find((result): result is PromiseRejectedResult => result.status === "rejected");
  if (failure) throw failure.reason;
  return results.map((result) => (result as PromiseFulfilledResult<unknown>).value) as { [K in keyof T]: Awaited<T[K]> };
}
