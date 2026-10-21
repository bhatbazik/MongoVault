export function linkedController(parent) {
  const controller = new AbortController();
  const forward = () => controller.abort(parent.reason);

  if (parent?.aborted) forward();
  else parent?.addEventListener("abort", forward, { once: true });

  return {
    signal: controller.signal,
    abort: (reason) => controller.abort(reason),
    dispose: () => parent?.removeEventListener("abort", forward),
  };
}

export async function settleAll(controller, tasks) {
  for (const task of tasks) {
    task.catch((error) => controller.abort(error));
  }
  const results = await Promise.allSettled(tasks);
  const failure = results.find((result) => result.status === "rejected");
  if (failure) throw failure.reason;
  return results.map((result) => result.value);
}
