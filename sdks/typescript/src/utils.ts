import { JsonValue, RetryStrategy } from "./types.js";

export function serializeError(err: unknown): JsonValue {
  if (err instanceof Error) {
    return {
      name: err.name,
      message: err.message,
      stack: err.stack || null,
    };
  }
  return { message: String(err) };
}

export function computeRetryAt(
  strategy: RetryStrategy | null,
  attempt: number,
): Date | null {
  if (!strategy || strategy.kind === "none") {
    return null;
  }

  const baseSeconds = strategy.baseSeconds ?? 5;
  let delaySeconds: number;

  if (strategy.kind === "fixed") {
    delaySeconds = baseSeconds;
  } else if (strategy.kind === "exponential") {
    const factor = strategy.factor ?? 2;
    delaySeconds = baseSeconds * Math.pow(factor, attempt - 1);
  } else {
    return null;
  }

  const maxSeconds = strategy.maxSeconds ?? Infinity;
  delaySeconds = Math.min(delaySeconds, maxSeconds);

  return new Date(Date.now() + delaySeconds * 1000);
}
