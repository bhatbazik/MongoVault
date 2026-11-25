const offsetMs = Number(process.env.MONGOVAULT_CLOCK_SKEW_MS || 0);

if (Number.isFinite(offsetMs) && offsetMs !== 0) {
  const OriginalDate = globalThis.Date;
  const origNow = OriginalDate.now;

  const SkewedDate = new Proxy(OriginalDate, {
    construct(target, args, newTarget) {
      if (args.length === 0) {
        return Reflect.construct(target, [origNow() + offsetMs], newTarget);
      }
      return Reflect.construct(target, args, newTarget);
    },
    apply() {
      return new OriginalDate(origNow() + offsetMs).toString();
    },
    get(target, prop, receiver) {
      if (prop === "now") {
        return () => origNow() + offsetMs;
      }
      return Reflect.get(target, prop, receiver);
    },
  });

  globalThis.Date = SkewedDate;
}
