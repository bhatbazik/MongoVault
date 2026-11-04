export interface LinkedAbortController {
  signal: AbortSignal;
  abort: (reason?: unknown) => void;
  dispose: () => void;
}
