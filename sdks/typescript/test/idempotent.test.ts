import { describe, test, expect, beforeAll, afterEach } from "vitest";
import { createTestAbsurd, randomName, type TestContext } from "./setup.js";
import type { Absurd } from "../src/index.js";

describe("Idempotent Task Spawning", () => {
  let ctx: TestContext;
  let absurd: Absurd;

  beforeAll(async () => {
    ctx = await createTestAbsurd(randomName("idempotent"));
    absurd = ctx.absurd;
  });

  afterEach(async () => {
    await ctx.cleanupTasks();
    await ctx.setFakeNow(null);
  });

  describe("spawn with idempotencyKey", () => {
    test("creates task when idempotencyKey is new", async () => {
      absurd.registerTask({ name: "idempotent-task" }, async () => {
        return { done: true };
      });

      const result = await absurd.spawn(
        "idempotent-task",
        { value: 42 },
        { idempotencyKey: "unique-key-1" },
      );

      expect(result.taskID).toBeDefined();
      expect(result.runID).toBeDefined();
      expect(result.attempt).toBe(1);
      expect(result.created).toBe(true);

      const task = await ctx.getTask(result.taskID);
      expect(task).toBeDefined();
      expect(task?.state).toBe("pending");
    });

    test("returns existing task when idempotencyKey already exists", async () => {
      absurd.registerTask({ name: "dup-task" }, async () => {
        return { done: true };
      });

      const first = await absurd.spawn(
        "dup-task",
        { value: 1 },
        { idempotencyKey: "dup-key" },
      );

      expect(first.taskID).toBeDefined();
      expect(first.runID).toBeDefined();
      expect(first.attempt).toBe(1);
      expect(first.created).toBe(true);

      const second = await absurd.spawn(
        "dup-task",
        { value: 2 },
        { idempotencyKey: "dup-key" },
      );

      expect(second.taskID).toBe(first.taskID);
      expect(second.runID).toBe(first.runID);
      expect(second.attempt).toBe(first.attempt);
      expect(second.created).toBe(false);
    });

    test("spawn without idempotencyKey always creates new task", async () => {
      absurd.registerTask({ name: "no-idem-task" }, async () => {
        return { done: true };
      });

      const first = await absurd.spawn("no-idem-task", { value: 1 });
      const second = await absurd.spawn("no-idem-task", { value: 2 });

      expect(first.taskID).not.toBe(second.taskID);
      expect(first.runID).toBeDefined();
      expect(second.runID).toBeDefined();
      expect(first.created).toBe(true);
      expect(second.created).toBe(true);
    });

    test("different idempotencyKeys create separate tasks", async () => {
      absurd.registerTask({ name: "diff-keys-task" }, async () => {
        return { done: true };
      });

      const first = await absurd.spawn(
        "diff-keys-task",
        { value: 1 },
        { idempotencyKey: "key-a" },
      );
      const second = await absurd.spawn(
        "diff-keys-task",
        { value: 2 },
        { idempotencyKey: "key-b" },
      );

      expect(first.taskID).not.toBe(second.taskID);
      expect(first.runID).toBeDefined();
      expect(second.runID).toBeDefined();
      expect(first.created).toBe(true);
      expect(second.created).toBe(true);
    });

    test("idempotencyKey persists after task completes", async () => {
      absurd.registerTask({ name: "complete-idem-task" }, async () => {
        return { result: "done" };
      });

      const first = await absurd.spawn(
        "complete-idem-task",
        { value: 1 },
        { idempotencyKey: "complete-key" },
      );

      await absurd.workBatch("test-worker", 60, 1);

      const task = await ctx.getTask(first.taskID);
      expect(task?.state).toBe("completed");

      const second = await absurd.spawn(
        "complete-idem-task",
        { value: 2 },
        { idempotencyKey: "complete-key" },
      );

      expect(second.taskID).toBe(first.taskID);
      expect(second.runID).toBe(first.runID);
      expect(second.attempt).toBe(first.attempt);
      expect(second.created).toBe(false);
    });

    test("idempotencyKey is scoped to queue", async () => {
      const otherQueueName = randomName("other_queue");
      await absurd.createQueue(otherQueueName);

      absurd.registerTask(
        { name: "queue-scoped-task", queue: ctx.queueName },
        async () => ({ done: true }),
      );
      absurd.registerTask(
        { name: "queue-scoped-task-other", queue: otherQueueName },
        async () => ({ done: true }),
      );

      const first = await absurd.spawn(
        "queue-scoped-task",
        { value: 1 },
        { idempotencyKey: "scoped-key" },
      );

      const second = await absurd.spawn(
        "queue-scoped-task-other",
        { value: 2 },
        { idempotencyKey: "scoped-key" },
      );

      expect(first.taskID).not.toBe(second.taskID);
      expect(first.created).toBe(true);
      expect(second.created).toBe(true);

      await absurd.dropQueue(otherQueueName);
    });

    test("idempotent spawn can be used for fire-and-forget patterns", async () => {
      let execCount = 0;
      absurd.registerTask({ name: "fire-forget-task" }, async () => {
        execCount++;
        return { done: true };
      });

      await absurd.spawn(
        "fire-forget-task",
        {},
        { idempotencyKey: "daily-report:2025-01-15" },
      );
      await absurd.spawn(
        "fire-forget-task",
        {},
        { idempotencyKey: "daily-report:2025-01-15" },
      );
      await absurd.spawn(
        "fire-forget-task",
        {},
        { idempotencyKey: "daily-report:2025-01-15" },
      );

      await absurd.workBatch("test-worker", 60, 10);

      expect(execCount).toBe(1);
    });
  });
});
