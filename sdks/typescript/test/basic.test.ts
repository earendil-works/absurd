import { describe, test, assert, expect, beforeAll, afterEach } from "vitest";
import { createTestAbsurd, randomName, type TestContext } from "./setup.js";
import type { Absurd } from "../src/index.js";

describe("Basic SDK Operations", () => {
  let ctx: TestContext;
  let absurd: Absurd;

  beforeAll(async () => {
    ctx = await createTestAbsurd(randomName("test_queue"));
    absurd = ctx.absurd;
  });

  afterEach(async () => {
    await ctx.cleanupTasks();
    await ctx.setFakeNow(null);
  });

  describe("Queue management", () => {
    test("create, list, and drop queue", async () => {
      const queueName = randomName("test_queue");
      await absurd.createQueue(queueName);

      let queues = await absurd.listQueues();
      expect(queues).toContain(queueName);

      const tables = await ctx.pool.query(`
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
        await ctx.pool.query(
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

      expect(await ctx.getTask(taskID)).toMatchObject({
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
        { name: taskName, queue: ctx.queueName },
        async () => ({ success: true }),
      );

      await expect(
        absurd.spawn(taskName, undefined, { queue: otherQueue }),
      ).rejects.toThrowError(
        `Task "${taskName}" is registered for queue "${ctx.queueName}" but spawn requested queue "${otherQueue}".`,
      );
    });
  });

  describe("Task claiming", () => {
    test("claim tasks with various batch sizes", async () => {
      await ctx.cleanupTasks();

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
      expect((await ctx.getTask(spawned[0].taskID))?.state).toBe("running");

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

  describe("State transitions", () => {
    test("scheduleRun moves run between running and sleeping", async () => {
      await ctx.cleanupTasks();
      const baseTime = new Date("2024-04-01T10:00:00Z");
      await ctx.setFakeNow(baseTime);

      absurd.registerTask<{ step: string }>(
        { name: "schedule-task" },
        async () => {
          return { done: true };
        },
      );

      const { runID } = await absurd.spawn("schedule-task", { step: "start" });
      const [claim] = await absurd.claimTasks({
        workerId: "worker-1",
        claimTimeout: 120,
      });
      expect(claim.run_id).toBe(runID);

      const wakeAt = new Date(baseTime.getTime() + 5 * 60 * 1000);
      await ctx.pool.query(`SELECT absurd.schedule_run($1, $2, $3)`, [
        ctx.queueName,
        runID,
        wakeAt,
      ]);

      const scheduledRun = await ctx.getRun(runID);
      expect(scheduledRun).toMatchObject({
        state: "sleeping",
        available_at: wakeAt,
        wake_event: null,
      });

      const scheduledTask = await ctx.getTask(claim.task_id);
      expect(scheduledTask?.state).toBe("sleeping");

      await ctx.setFakeNow(wakeAt);
      const [resumed] = await absurd.claimTasks({
        workerId: "worker-1",
        claimTimeout: 120,
      });
      expect(resumed.run_id).toBe(runID);
      expect(resumed.attempt).toBe(1);

      const resumedRun = await ctx.getRun(runID);
      expect(resumedRun).toMatchObject({
        state: "running",
        started_at: wakeAt,
      });
    });

    test("claim timeout releases run to a new worker", async () => {
      await ctx.cleanupTasks();
      const baseTime = new Date("2024-04-02T09:00:00Z");
      await ctx.setFakeNow(baseTime);

      absurd.registerTask<{ step: string }>(
        { name: "lease-task" },
        async () => {
          return { ok: true };
        },
      );

      const { taskID } = await absurd.spawn("lease-task", { step: "attempt" });
      const [claim] = await absurd.claimTasks({
        workerId: "worker-a",
        claimTimeout: 30,
      });
      expect(claim.task_id).toBe(taskID);

      const running = await ctx.getRun(claim.run_id);
      expect(running).toMatchObject({
        state: "running",
        claimed_by: "worker-a",
        claim_expires_at: new Date(baseTime.getTime() + 30 * 1000),
      });

      await ctx.setFakeNow(new Date(baseTime.getTime() + 5 * 60 * 1000));
      const [reclaim] = await absurd.claimTasks({
        workerId: "worker-b",
        claimTimeout: 45,
      });
      expect(reclaim.run_id).not.toBe(claim.run_id);
      expect(reclaim.attempt).toBe(2);

      const expiredRun = await ctx.getRun(claim.run_id);
      expect(expiredRun?.state).toBe("failed");
      expect(expiredRun?.failure_reason).toMatchObject({
        name: "$ClaimTimeout",
        workerId: "worker-a",
        attempt: 1,
      });

      const newRun = await ctx.getRun(reclaim.run_id);
      expect(newRun).toMatchObject({
        state: "running",
        claimed_by: "worker-b",
      });

      const taskRow = await ctx.getTask(taskID);
      expect(taskRow).toMatchObject({
        state: "running",
        attempts: 2,
      });
    });
  });

  describe("Cleanup maintenance", () => {
    test("cleanup tasks and events respect TTLs", async () => {
      await ctx.cleanupTasks();
      const base = new Date("2024-03-01T08:00:00Z");
      await ctx.setFakeNow(base);

      absurd.registerTask<{ step: string }>({ name: "cleanup" }, async () => {
        return { status: "done" };
      });

      const { runID } = await absurd.spawn("cleanup", { step: "start" });
      const [claim] = await absurd.claimTasks({
        workerId: "worker-clean",
        claimTimeout: 60,
      });
      expect(claim.run_id).toBe(runID);

      const finishTime = new Date(base.getTime() + 10 * 60 * 1000);
      await ctx.setFakeNow(finishTime);
      await ctx.pool.query(`SELECT absurd.complete_run($1, $2, $3)`, [
        ctx.queueName,
        runID,
        JSON.stringify({ status: "done" }),
      ]);

      await absurd.emitEvent("cleanup-event", { kind: "notify" });

      const runRow = await ctx.getRun(runID);
      expect(runRow).toMatchObject({
        claimed_by: "worker-clean",
        claim_expires_at: new Date(base.getTime() + 60 * 1000),
      });

      const beforeTTL = new Date(finishTime.getTime() + 30 * 60 * 1000);
      await ctx.setFakeNow(beforeTTL);
      const beforeTasks = await ctx.pool.query<{ count: string }>(
        `SELECT absurd.cleanup_tasks($1, $2, $3) AS count`,
        [ctx.queueName, 3600, 10],
      );
      expect(Number(beforeTasks.rows[0].count)).toBe(0);
      const beforeEvents = await ctx.pool.query<{ count: string }>(
        `SELECT absurd.cleanup_events($1, $2, $3) AS count`,
        [ctx.queueName, 3600, 10],
      );
      expect(Number(beforeEvents.rows[0].count)).toBe(0);

      const later = new Date(finishTime.getTime() + 26 * 60 * 60 * 1000);
      await ctx.setFakeNow(later);
      const deletedTasks = await ctx.pool.query<{ count: string }>(
        `SELECT absurd.cleanup_tasks($1, $2, $3) AS count`,
        [ctx.queueName, 3600, 10],
      );
      expect(Number(deletedTasks.rows[0].count)).toBe(1);
      const deletedEvents = await ctx.pool.query<{ count: string }>(
        `SELECT absurd.cleanup_events($1, $2, $3) AS count`,
        [ctx.queueName, 3600, 10],
      );
      expect(Number(deletedEvents.rows[0].count)).toBe(1);

      const remainingTasks = await ctx.pool.query<{ count: string }>(
        `SELECT COUNT(*)::text AS count FROM absurd.t_${ctx.queueName}`,
      );
      expect(Number(remainingTasks.rows[0].count)).toBe(0);
      const remainingEvents = await ctx.pool.query<{ count: string }>(
        `SELECT COUNT(*)::text AS count FROM absurd.e_${ctx.queueName}`,
      );
      expect(Number(remainingEvents.rows[0].count)).toBe(0);
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
      expect((await ctx.getTask(taskID))?.state).toBe("pending");

      // Process with workBatch: transitions pending -> running -> completed
      await absurd.workBatch("test-worker-complete", 60, 1);

      expect(await ctx.getTask(taskID)).toMatchObject({
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
      expect((await ctx.getTask(taskID))?.state).toBe("sleeping");

      // Emit event and resume
      await absurd.emitEvent(eventName, { data: "wakeup" });
      await absurd.workBatch("test-worker-suspend", 60, 1);

      expect(await ctx.getTask(taskID)).toMatchObject({
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
      expect((await ctx.getRun(firstRunID))?.state).toBe("failed");
      expect((await ctx.getTask(taskID))?.state).toBe("pending");
      // Second attempt fails (task: failed, run: failed)
      await absurd.workBatch("test-worker-fail", 60, 1);
      expect((await ctx.getTask(taskID))?.state).toBe("failed");
      expect(await ctx.getRun(firstRunID)).toMatchObject({
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

      const taskInfo = await ctx.getTask(taskID);
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
        expect(await ctx.getTask(task.taskID)).toMatchObject({
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

      expect((await ctx.getTask(bad.taskID))?.state).toBe("failed");
      expect((await ctx.getTask(ok.taskID))?.state).toBe("completed");
    });
  });
});
