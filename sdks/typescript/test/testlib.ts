import assert from "node:assert/strict";
import {
  after,
  afterEach,
  before,
  beforeEach,
  describe,
  it,
  test,
} from "node:test";
import { isDeepStrictEqual } from "node:util";

type WaitForOptions = {
  timeout?: number;
  interval?: number;
};

type ObjectContainingMatcher = {
  __matcher: "objectContaining";
  expected: unknown;
};

type Spy = {
  mock: { calls: unknown[][] };
  mockRestore: () => void;
  mockRejectedValueOnce: (error: unknown) => Spy;
  mockRejectedValue: (error: unknown) => Spy;
};

const activeSpies = new Set<Spy>();

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function isObjectContainingMatcher(value: unknown): value is ObjectContainingMatcher {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as ObjectContainingMatcher).__matcher === "objectContaining"
  );
}

function matchesObject(actual: unknown, expected: unknown): boolean {
  if (isObjectContainingMatcher(expected)) {
    return matchesObject(actual, expected.expected);
  }
  if (typeof expected !== "object" || expected === null) {
    return isDeepStrictEqual(actual, expected);
  }
  if (typeof actual !== "object" || actual === null) {
    return false;
  }
  if (Array.isArray(expected)) {
    if (!Array.isArray(actual) || actual.length !== expected.length) {
      return false;
    }
    for (let i = 0; i < expected.length; i++) {
      if (!matchesObject(actual[i], expected[i])) {
        return false;
      }
    }
    return true;
  }

  for (const [key, expectedValue] of Object.entries(expected)) {
    const actualValue = (actual as Record<string, unknown>)[key];
    if (!matchesObject(actualValue, expectedValue)) {
      return false;
    }
  }
  return true;
}

class Expectation {
  private readonly actual: unknown;
  private readonly negate: boolean;

  constructor(actual: unknown, negate: boolean = false) {
    this.actual = actual;
    this.negate = negate;
  }

  get not(): Expectation {
    return new Expectation(this.actual, !this.negate);
  }

  private check(condition: boolean, message: string): void {
    if (this.negate ? condition : !condition) {
      throw new Error(message);
    }
  }

  toBe(expected: unknown): void {
    this.check(Object.is(this.actual, expected), `Expected ${this.actual} to be ${expected}`);
  }

  toEqual(expected: unknown): void {
    this.check(isDeepStrictEqual(this.actual, expected), "Expected values to be deeply equal");
  }

  toMatchObject(expected: unknown): void {
    this.check(matchesObject(this.actual, expected), "Expected object to match");
  }

  toContain(expected: unknown): void {
    if (typeof this.actual === "string") {
      this.check(this.actual.includes(String(expected)), `Expected string to contain ${expected}`);
      return;
    }
    if (Array.isArray(this.actual)) {
      this.check(
        this.actual.some((item) => isDeepStrictEqual(item, expected)),
        `Expected array to contain ${expected}`,
      );
      return;
    }
    throw new Error("toContain only supports strings and arrays");
  }

  toHaveLength(expected: number): void {
    const length = (this.actual as { length?: number } | null | undefined)?.length;
    this.check(length === expected, `Expected length ${expected}, got ${String(length)}`);
  }

  toBeDefined(): void {
    this.check(this.actual !== undefined, "Expected value to be defined");
  }

  toBeUndefined(): void {
    this.check(this.actual === undefined, "Expected value to be undefined");
  }

  toBeNull(): void {
    this.check(this.actual === null, "Expected value to be null");
  }

  toBeGreaterThan(expected: number): void {
    this.check(
      typeof this.actual === "number" && this.actual > expected,
      `Expected ${String(this.actual)} to be greater than ${expected}`,
    );
  }

  toHaveProperty(path: string, expected?: unknown): void {
    const keys = path.split(".");
    let current: unknown = this.actual;
    for (const key of keys) {
      if (typeof current !== "object" || current === null || !(key in (current as Record<string, unknown>))) {
        this.check(false, `Expected object to have property ${path}`);
        return;
      }
      current = (current as Record<string, unknown>)[key];
    }
    if (arguments.length > 1) {
      this.check(isDeepStrictEqual(current, expected), `Expected property ${path} to equal ${String(expected)}`);
    }
  }

  toHaveBeenCalledTimes(expected: number): void {
    const calls = (this.actual as Spy)?.mock?.calls;
    this.check(Array.isArray(calls) && calls.length === expected, `Expected spy to be called ${expected} times`);
  }

  toHaveBeenCalled(): void {
    const calls = (this.actual as Spy)?.mock?.calls;
    this.check(Array.isArray(calls) && calls.length > 0, "Expected spy to have been called");
  }

  toHaveBeenCalledWith(...expectedArgs: unknown[]): void {
    const calls = (this.actual as Spy)?.mock?.calls;
    const found =
      Array.isArray(calls) &&
      calls.some((args) => {
        if (args.length !== expectedArgs.length) {
          return false;
        }
        return args.every((arg, idx) => matchesObject(arg, expectedArgs[idx]));
      });
    this.check(Boolean(found), "Expected spy to be called with matching arguments");
  }

  get rejects() {
    const promise = this.actual as Promise<unknown>;

    const getRejection = async () => {
      try {
        await promise;
        throw new Error("Expected promise to reject, but it resolved");
      } catch (err) {
        return err;
      }
    };

    const toThrowError = async (expected?: string | RegExp) => {
      const rejected = await getRejection();
      const message = rejected instanceof Error ? rejected.message : String(rejected);
      if (expected === undefined) {
        return;
      }
      if (typeof expected === "string") {
        if (!message.includes(expected)) {
          throw new Error(`Expected rejection message to include ${expected}, got: ${message}`);
        }
        return;
      }
      if (!expected.test(message)) {
        throw new Error(`Expected rejection message to match ${expected}, got: ${message}`);
      }
    };

    return {
      toThrowError,
      toThrow: toThrowError,
      toHaveProperty: async (path: string, expected?: unknown) => {
        const rejected = await getRejection();
        new Expectation(rejected).toHaveProperty(path, expected);
      },
    };
  }
}

type ExpectFn = ((actual: unknown) => Expectation) & {
  objectContaining: (expected: unknown) => ObjectContainingMatcher;
};

const expectImpl = ((actual: unknown) => new Expectation(actual)) as ExpectFn;
expectImpl.objectContaining = (expected: unknown) => ({
  __matcher: "objectContaining",
  expected,
});

function spyOn<T extends object, K extends keyof T & string>(obj: T, method: K): Spy {
  const original = obj[method] as unknown as (...args: unknown[]) => unknown;
  if (typeof original !== "function") {
    throw new Error(`Cannot spy on ${String(method)}; not a function`);
  }

  const calls: unknown[][] = [];
  const onceQueue: Array<(...args: unknown[]) => unknown> = [];
  let defaultImpl: ((...args: unknown[]) => unknown) | null = null;

  const wrapper = function (this: unknown, ...args: unknown[]) {
    calls.push(args);
    if (onceQueue.length > 0) {
      return onceQueue.shift()!(...args);
    }
    if (defaultImpl) {
      return defaultImpl(...args);
    }
    return original.apply(this, args);
  };

  obj[method] = wrapper as T[K];

  const spy: Spy = {
    mock: { calls },
    mockRestore: () => {
      obj[method] = original as T[K];
      activeSpies.delete(spy);
    },
    mockRejectedValueOnce: (error: unknown) => {
      onceQueue.push(() => Promise.reject(error));
      return spy;
    },
    mockRejectedValue: (error: unknown) => {
      defaultImpl = () => Promise.reject(error);
      return spy;
    },
  };

  activeSpies.add(spy);
  return spy;
}

let fakeTimersEnabled = false;
let realDateNow: (() => number) | null = null;
let fakeNow = 0;

function useFakeTimers(): void {
  if (fakeTimersEnabled) {
    return;
  }
  fakeTimersEnabled = true;
  realDateNow = Date.now.bind(Date);
  fakeNow = realDateNow();
  Date.now = () => fakeNow;
}

function setSystemTime(value: Date | number): void {
  if (!fakeTimersEnabled) {
    throw new Error("setSystemTime requires useFakeTimers()");
  }
  fakeNow = value instanceof Date ? value.getTime() : value;
}

function useRealTimers(): void {
  if (!fakeTimersEnabled) {
    return;
  }
  if (realDateNow) {
    Date.now = realDateNow;
  }
  fakeTimersEnabled = false;
  realDateNow = null;
}

async function waitFor(fn: () => unknown | Promise<unknown>, options: WaitForOptions = {}): Promise<void> {
  const timeout = options.timeout ?? 1000;
  const interval = options.interval ?? 10;
  const start = Date.now();
  let lastError: unknown;

  while (Date.now() - start < timeout) {
    try {
      await fn();
      return;
    } catch (err) {
      lastError = err;
    }
    await sleep(interval);
  }

  if (lastError) {
    throw lastError;
  }
  throw new Error("waitFor timed out");
}

function restoreAllMocks(): void {
  for (const spy of [...activeSpies]) {
    spy.mockRestore();
  }
}

// Capturing log: buffers messages per-test and flushes to stderr on failure.
const _capturedLogs: { level: string; args: unknown[] }[] = [];

export const testLog = {
  log(...args: unknown[]) { _capturedLogs.push({ level: "log", args }); },
  info(...args: unknown[]) { _capturedLogs.push({ level: "info", args }); },
  warn(...args: unknown[]) { _capturedLogs.push({ level: "warn", args }); },
  error(...args: unknown[]) { _capturedLogs.push({ level: "error", args }); },
};

function clearCapturedLogs() {
  _capturedLogs.length = 0;
}

function flushCapturedLogs() {
  for (const entry of _capturedLogs) {
    const fn = (console as unknown as Record<string, Function>)[entry.level] ?? console.error;
    fn.call(console, ...entry.args);
  }
  _capturedLogs.length = 0;
}

type TestFn = (t?: unknown) => void | Promise<void>;

function wrapTestFn(
  original: (name: string, fn: TestFn) => void,
): (name: string, fn: TestFn) => void {
  return (name: string, fn: TestFn) => {
    original(name, async (t) => {
      clearCapturedLogs();
      try {
        await fn(t);
      } catch (err) {
        flushCapturedLogs();
        throw err;
      }
      clearCapturedLogs();
    });
  };
}

const wrappedIt = wrapTestFn(it as (name: string, fn: TestFn) => void);
const wrappedTest = wrapTestFn(test as (name: string, fn: TestFn) => void);

export const expect = expectImpl;
export { assert, describe, wrappedIt as it, wrappedTest as test, before as beforeAll, beforeEach, after as afterAll, afterEach };

export const vi = {
  spyOn,
  waitFor,
  restoreAllMocks,
  useFakeTimers,
  useRealTimers,
  setSystemTime,
};
