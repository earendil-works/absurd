import { describe, test, expect, beforeAll, afterEach } from "vitest";
import { AsyncLocalStorage } from "node:async_hooks";
import { pool, randomName } from "./setup.js";
import { Absurd, type SpawnOptions, type TaskContext } from "../src/index.js";

describe("Hooks", () => {
  let queueName: string;

  beforeAll(async () => {
    queueName = randomName("hooks_queue");
    const absurd = new Absurd({ db: pool, queueName });
    await absurd.createQueue(queueName);
  });

  afterEach(async () => {
    try {
      await pool.query(
        `TRUNCATE absurd.t_${queueName}, absurd.r_${queueName}, absurd.c_${queueName}, absurd.e_${queueName}, absurd.w_${queueName}`,
      );
    } catch (err: any) {
      if (!err.message?.includes("does not exist")) {
        throw err;
      }
    }
  });

  describe("beforeSpawn hook", () => {
    test("can inject headers before spawn", async () => {
      const injectedHeaders: SpawnOptions["headers"][] = [];

      const absurd = new Absurd({
        db: pool,
        queueName,
        hooks: {
          beforeSpawn: (_taskName, _params, options) => {
            return {
              ...options,
              headers: {
                ...options.headers,
                traceId: "trace-123",
                correlationId: "corr-456",
              },
            };
          },
        },
      });

      let capturedHeaders: any = null;
      absurd.registerTask({ name: "capture-headers" }, async (_params, ctx) => {
        capturedHeaders = ctx.headers;
        return "done";
      });

      await absurd.spawn("capture-headers", { test: true });
      await absurd.workBatch("worker1", 60, 1);

      expect(capturedHeaders).toEqual({
        traceId: "trace-123",
        correlationId: "corr-456",
      });
    });

    test("can use async beforeSpawn hook", async () => {
      const absurd = new Absurd({
        db: pool,
        queueName,
        hooks: {
          beforeSpawn: async (_taskName, _params, options) => {
            // Simulate async operation (e.g., fetching context)
            await new Promise((resolve) => setTimeout(resolve, 10));
            return {
              ...options,
              headers: {
                ...options.headers,
                asyncHeader: "fetched-value",
              },
            };
          },
        },
      });

      let capturedHeader: any = null;
      absurd.registerTask({ name: "async-header" }, async (_params, ctx) => {
        capturedHeader = ctx.headers["asyncHeader"];
        return "done";
      });

      await absurd.spawn("async-header", {});
      await absurd.workBatch("worker1", 60, 1);

      expect(capturedHeader).toBe("fetched-value");
    });

    test("preserves existing headers when adding new ones", async () => {
      const absurd = new Absurd({
        db: pool,
        queueName,
        hooks: {
          beforeSpawn: (_taskName, _params, options) => {
            return {
              ...options,
              headers: {
                ...options.headers,
                injected: "by-hook",
              },
            };
          },
        },
      });

      let capturedHeaders: any = null;
      absurd.registerTask({ name: "merge-headers" }, async (_params, ctx) => {
        capturedHeaders = ctx.headers;
        return "done";
      });

      await absurd.spawn(
        "merge-headers",
        {},
        {
          headers: { existing: "user-provided" },
        },
      );
      await absurd.workBatch("worker1", 60, 1);

      expect(capturedHeaders).toEqual({
        existing: "user-provided",
        injected: "by-hook",
      });
    });
  });

  describe("wrapTaskExecution hook", () => {
    test("wraps task execution", async () => {
      const executionOrder: string[] = [];

      const absurd = new Absurd({
        db: pool,
        queueName,
        hooks: {
          wrapTaskExecution: async (_ctx, execute) => {
            executionOrder.push("before");
            await execute();
            executionOrder.push("after");
          },
        },
      });

      absurd.registerTask({ name: "wrapped-task" }, async () => {
        executionOrder.push("handler");
        return "done";
      });

      await absurd.spawn("wrapped-task", {});
      await absurd.workBatch("worker1", 60, 1);

      expect(executionOrder).toEqual(["before", "handler", "after"]);
    });

    test("provides TaskContext to wrapper", async () => {
      let capturedTaskId: string | null = null;
      let capturedHeaders: any = null;

      const absurd = new Absurd({
        db: pool,
        queueName,
        hooks: {
          beforeSpawn: (_taskName, _params, options) => ({
            ...options,
            headers: { traceId: "from-spawn" },
          }),
          wrapTaskExecution: async (ctx, execute) => {
            capturedTaskId = ctx.taskID;
            capturedHeaders = ctx.headers;
            await execute();
          },
        },
      });

      absurd.registerTask({ name: "ctx-in-wrapper" }, async () => "done");

      const { taskID } = await absurd.spawn("ctx-in-wrapper", {});
      await absurd.workBatch("worker1", 60, 1);

      expect(capturedTaskId).toBe(taskID);
      expect(capturedHeaders).toEqual({ traceId: "from-spawn" });
    });
  });

  describe("AsyncLocalStorage integration", () => {
    test("full round-trip: inject context on spawn, restore on execution", async () => {
      interface TraceContext {
        traceId: string;
        spanId: string;
      }
      const als = new AsyncLocalStorage<TraceContext>();

      const absurd = new Absurd({
        db: pool,
        queueName,
        hooks: {
          beforeSpawn: (_taskName, _params, options) => {
            const store = als.getStore();
            if (store) {
              return {
                ...options,
                headers: {
                  ...options.headers,
                  traceId: store.traceId,
                  spanId: store.spanId,
                },
              };
            }
            return options;
          },
          wrapTaskExecution: async (ctx, execute) => {
            const traceId = ctx.headers["traceId"] as string | undefined;
            const spanId = ctx.headers["spanId"] as string | undefined;
            if (traceId && spanId) {
              return als.run({ traceId, spanId }, execute);
            }
            return execute();
          },
        },
      });

      let capturedInHandler: TraceContext | undefined;
      absurd.registerTask({ name: "als-test" }, async () => {
        capturedInHandler = als.getStore();
        return "done";
      });

      // Spawn within an ALS context
      await als.run({ traceId: "trace-abc", spanId: "span-xyz" }, async () => {
        await absurd.spawn("als-test", {});
      });

      // Execute the task (outside original ALS context)
      await absurd.workBatch("worker1", 60, 1);

      // Handler should have received the context via wrapTaskExecution
      expect(capturedInHandler).toEqual({
        traceId: "trace-abc",
        spanId: "span-xyz",
      });
    });

    test("child spawns inherit context from parent task", async () => {
      interface TraceContext {
        traceId: string;
      }
      const als = new AsyncLocalStorage<TraceContext>();

      const absurd = new Absurd({
        db: pool,
        queueName,
        hooks: {
          beforeSpawn: (_taskName, _params, options) => {
            const store = als.getStore();
            if (store) {
              return {
                ...options,
                headers: {
                  ...options.headers,
                  traceId: store.traceId,
                },
              };
            }
            return options;
          },
          wrapTaskExecution: async (ctx, execute) => {
            const traceId = ctx.headers["traceId"] as string | undefined;
            if (traceId) {
              return als.run({ traceId }, execute);
            }
            return execute();
          },
        },
      });

      let childTraceId: string | undefined;

      absurd.registerTask({ name: "parent-task" }, async (_params, _ctx) => {
        // Spawn child task - should inherit trace context via beforeSpawn hook
        await absurd.spawn("child-task", {});
        return "parent-done";
      });

      absurd.registerTask({ name: "child-task" }, async (_params, ctx) => {
        childTraceId = ctx.headers["traceId"] as string | undefined;
        return "child-done";
      });

      // Spawn parent within ALS context
      await als.run({ traceId: "parent-trace" }, async () => {
        await absurd.spawn("parent-task", {});
      });

      // Execute parent task
      await absurd.workBatch("worker1", 60, 1);
      // Execute child task
      await absurd.workBatch("worker1", 60, 1);

      expect(childTraceId).toBe("parent-trace");
    });
  });

  describe("TaskContext header accessors", () => {
    test("headers returns undefined for missing key", async () => {
      const absurd = new Absurd({ db: pool, queueName });

      let result: any;
      absurd.registerTask({ name: "no-headers" }, async (_params, ctx) => {
        result = ctx.headers["nonexistent"];
        return "done";
      });

      await absurd.spawn("no-headers", {});
      await absurd.workBatch("worker1", 60, 1);

      expect(result).toBeUndefined();
    });

    test("headers getter returns empty object when no headers set", async () => {
      const absurd = new Absurd({ db: pool, queueName });

      let result: any;
      absurd.registerTask({ name: "empty-headers" }, async (_params, ctx) => {
        result = ctx.headers;
        return "done";
      });

      await absurd.spawn("empty-headers", {});
      await absurd.workBatch("worker1", 60, 1);

      expect(result).toEqual({});
    });

    test("headers getter returns all headers", async () => {
      const absurd = new Absurd({ db: pool, queueName });

      let result: any;
      absurd.registerTask({ name: "all-headers" }, async (_params, ctx) => {
        result = ctx.headers;
        return "done";
      });

      await absurd.spawn(
        "all-headers",
        {},
        {
          headers: { a: 1, b: "two", c: true },
        },
      );
      await absurd.workBatch("worker1", 60, 1);

      expect(result).toEqual({ a: 1, b: "two", c: true });
    });
  });
});
