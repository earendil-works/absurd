import { describe, test, assert, expect, beforeAll } from "vitest";
import { createTestAbsurd, randomName, type TestContext } from "./setup.js";
import type { Absurd } from "../src/index.js";

describe("Basic SDK Operations", () => {
  let thelper: TestContext;
  let absurd: Absurd;

  beforeAll(async () => {
    thelper = await createTestAbsurd(randomName("test_queue"));
    absurd = thelper.absurd;
  });

  describe("Queue management", () => {
    test("create, list, and drop queue", async () => {
      const queueName = randomName("test_queue");
      await absurd.createQueue(queueName);

      let queues = await absurd.listQueues();
      expect(queues).toContain(queueName);

      const tables = await thelper.pool.query(`
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'absurd'
          AND tablename LIKE '%_${queueName}'
        ORDER BY tablename
      `);
      expect(tables.rows.length).toBe(5);
      expect(tables.rows.map((r) => r.tablename).sort()).toEqual([
        `c_${queueName}`,
        `e_${queueName}`,
        `r_${queueName}`,
        `t_${queueName}`,
        `w_${queueName}`,
      ]);

      await absurd.dropQueue(queueName);

      queues = await absurd.listQueues();
      expect(queues).not.toContain(queueName);

      expect(
        await thelper.pool.query(
          "SELECT tablename FROM pg_tables WHERE schemaname = 'absurd' AND tablename LIKE $1",
          [`%_${queueName}`],
        ),
      ).toMatchObject({
        rows: [],
      });
    });
  });

  describe("Task spawning", () => {
    test("spawn with maxAttempts override", async () => {
      absurd.registerTask<{ shouldFail: boolean }>(
        { name: "test-max-attempts", defaultMaxAttempts: 5 },
        async () => {
          throw new Error("Always fails");
        },
      );

      const { taskID } = await absurd.spawn("test-max-attempts", undefined, {
        maxAttempts: 2,
      });

      await absurd.workBatch("test-worker-attempts", 60, 1);
      await absurd.workBatch("test-worker-attempts", 60, 1);

      expect(await thelper.getTask(taskID)).toMatchObject({
        state: "failed",
        attempts: 2,
      });
    });

    test("rejects spawning unregistered task without queue override", async () => {
      await expect(
        absurd.spawn("unregistered-task", { value: 1 }),
      ).rejects.toThrowError(
        'Task "unregistered-task" is not registered. Provide options.queue when spawning unregistered tasks.',
      );
    });

    test("rejects spawning registered task on mismatched queue", async () => {
      const taskName = "registered-queue-task";
      const otherQueue = randomName("other_queue");

      absurd.registerTask(
        { name: taskName, queue: thelper.queueName },
        async () => ({ success: true }),
      );

      await expect(
        absurd.spawn(taskName, undefined, { queue: otherQueue }),
      ).rejects.toThrowError(
        `Task "${taskName}" is registered for queue "${thelper.queueName}" but spawn requested queue "${otherQueue}".`,
      );
    });
  });

  describe("Task claiming", () => {
    test("claim tasks with various batch sizes", async () => {
      await thelper.cleanupTasks();

      absurd.registerTask<{ id: number }>(
        { name: "test-claim" },
        async (params) => {
          return params;
        },
      );

      const spawned = await Promise.all(
        [1, 2, 3].map((id) => absurd.spawn("test-claim", { id })),
      );

      // Test batch claim
      const claimed = await absurd.claimTasks({
        batchSize: 3,
        claimTimeout: 60,
        workerId: "test-worker",
      });

      expect(claimed.length).toBe(3);
      expect(claimed.map((c) => c.task_id).sort()).toEqual(
        spawned.map((s) => s.taskID).sort(),
      );

      // Should now be "running"
      expect((await thelper.getTask(spawned[0].taskID))?.state).toBe("running");

      // There should be none to claim
      expect(
        await absurd.claimTasks({
          batchSize: 10,
          claimTimeout: 60,
          workerId: "test-worker-empty",
        }),
      ).toEqual([]);
    });
  });

  describe("Task state transitions", () => {
    test("task transitions through all states: pending -> running -> completed", async () => {
      absurd.registerTask<{ value: number }>(
        { name: "test-task-complete" },
        async (params, ctx) => {
          const doubled = await ctx.step("double", async () => {
            return params.value * 2;
          });
          return { doubled };
        },
      );

      // Spawn: transitions to pending
      const { taskID } = await absurd.spawn("test-task-complete", {
        value: 21,
      });
      expect((await thelper.getTask(taskID))?.state).toBe("pending");

      // Process with workBatch: transitions pending -> running -> completed
      await absurd.workBatch("test-worker-complete", 60, 1);

      expect(await thelper.getTask(taskID)).toMatchObject({
        state: "completed",
        attempts: 1,
        completed_payload: { doubled: 42 },
      });
    });

    test("task transitions to sleeping state when suspended (waiting for event)", async () => {
      const eventName = randomName("suspend_event");
      absurd.registerTask(
        { name: "test-task-suspend" },
        async (params, ctx) => {
          return { received: await ctx.awaitEvent(eventName) };
        },
      );

      const { taskID } = await absurd.spawn("test-task-suspend", undefined);

      // Process task (suspends waiting for event)
      await absurd.workBatch("test-worker-suspend", 60, 1);
      expect((await thelper.getTask(taskID))?.state).toBe("sleeping");

      // Emit event and resume
      await absurd.emitEvent(eventName, { data: "wakeup" });
      await absurd.workBatch("test-worker-suspend", 60, 1);

      expect(await thelper.getTask(taskID)).toMatchObject({
        state: "completed",
        completed_payload: { received: { data: "wakeup" } },
      });
    });

    test("task transitions to failed state after all retries exhausted", async () => {
      absurd.registerTask(
        { name: "test-task-fail", defaultMaxAttempts: 2 },
        async () => {
          throw new Error("Task intentionally failed");
        },
      );

      const { taskID, runID: firstRunID } = await absurd.spawn(
        "test-task-fail",
        undefined,
      );

      // First attempt fails (task: pending, run: failed)
      await absurd.workBatch("test-worker-fail", 60, 1);
      expect((await thelper.getRun(firstRunID))?.state).toBe("failed");
      expect((await thelper.getTask(taskID))?.state).toBe("pending");
      // Second attempt fails (task: failed, run: failed)
      await absurd.workBatch("test-worker-fail", 60, 1);
      expect((await thelper.getTask(taskID))?.state).toBe("failed");
      expect(await thelper.getRun(firstRunID)).toMatchObject({
        state: "failed",
        attempt: 1,
        failure_reason: expect.objectContaining({
          message: "Task intentionally failed",
        }),
      });
    });
  });

  describe("Event system", () => {
    test("task receives event emitted before task was spawned", async () => {
      absurd.registerTask<{ eventName: string }, { received: any }>(
        { name: "test-cached-event" },
        async (params, ctx) => {
          const payload = await ctx.awaitEvent(params.eventName);
          return { received: payload };
        },
      );

      const eventName = randomName("test_event");

      await absurd.emitEvent(eventName, { data: "cached-payload" });

      const { taskID } = await absurd.spawn("test-cached-event", { eventName });

      await absurd.workBatch("test-worker-cached", 60, 1);

      const taskInfo = await thelper.getTask(taskID);
      assert(taskInfo);
      expect(taskInfo).toMatchObject({
        state: "completed",
        completed_payload: { received: { data: "cached-payload" } },
      });
    });
  });

  describe("Batch processing", () => {
    test("workBatch processes multiple tasks", async () => {
      absurd.registerTask<{ id: number }>(
        { name: "test-work-batch" },
        async (params) => {
          return { result: `task-${params.id}` };
        },
      );

      const tasks = await Promise.all(
        [1, 2, 3].map((id) => absurd.spawn("test-work-batch", { id })),
      );

      await absurd.workBatch("test-worker-batch", 60, 5);

      for (let i = 0; i < tasks.length; i++) {
        const task = tasks[i];
        expect(await thelper.getTask(task.taskID)).toMatchObject({
          state: "completed",
          completed_payload: { result: `task-${i + 1}` },
        });
      }
    });

    test("workBatch handles mixed success and failure", async () => {
      absurd.registerTask<{ fail: boolean }>(
        { name: "mixed", defaultMaxAttempts: 1 },
        async (params) => {
          if (params.fail) {
            throw new Error("Task failed in batch");
          }
          return { result: "success" };
        },
      );

      const bad = await absurd.spawn("mixed", {
        fail: true,
      });
      const ok = await absurd.spawn("mixed", {
        fail: false,
      });

      await absurd.workBatch("mixed", 60, 2);

      expect((await thelper.getTask(bad.taskID))?.state).toBe("failed");
      expect((await thelper.getTask(ok.taskID))?.state).toBe("completed");
    });
  });
});
