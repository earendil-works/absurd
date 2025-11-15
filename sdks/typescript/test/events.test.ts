import { describe, test, expect, beforeAll, afterEach } from "vitest";
import { createTestAbsurd, randomName, type TestContext } from "./setup.js";
import type { Absurd } from "../src/index.js";
import { TimeoutError } from "../src/index.js";

describe("Event system", () => {
  let thelper: TestContext;
  let absurd: Absurd;

  beforeAll(async () => {
    thelper = await createTestAbsurd(randomName("event_queue"));
    absurd = thelper.absurd;
  });

  afterEach(async () => {
    await thelper.cleanupTasks();
    await thelper.setFakeNow(null);
  });

  test("await and emit event flow", async () => {
    const eventName = randomName("test_event");

    absurd.registerTask({ name: "waiter" }, async (params, ctx) => {
      const payload = await ctx.awaitEvent(eventName, { timeout: 60 });
      return { received: payload };
    });

    const { taskID, runID } = await absurd.spawn("waiter", { step: 1 });

    // Start processing, task should suspend waiting for event
    await absurd.workBatch("worker1", 60, 1);

    const sleepingRun = await thelper.getRun(runID);
    expect(sleepingRun).toMatchObject({
      state: "sleeping",
      wake_event: eventName,
    });

    // Emit event
    const payload = { value: 42 };
    await absurd.emitEvent(eventName, payload);

    // Task should now be pending
    const pendingRun = await thelper.getRun(runID);
    expect(pendingRun?.state).toBe("pending");

    // Resume and complete
    await absurd.workBatch("worker1", 60, 1);

    expect(await thelper.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { received: payload },
    });
  });

  test("event emitted before await is cached and retrieved", async () => {
    const eventName = randomName("cached_event");
    const payload = { data: "pre-emitted" };

    // Emit event before task even exists
    await absurd.emitEvent(eventName, payload);

    absurd.registerTask({ name: "late-waiter" }, async (params, ctx) => {
      const received = await ctx.awaitEvent(eventName);
      return { received };
    });

    const { taskID } = await absurd.spawn("late-waiter", undefined);

    // Should complete immediately with cached event
    await absurd.workBatch("worker1", 60, 1);

    expect(await thelper.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { received: payload },
    });
  });

  test("awaitEvent with timeout expires and wakes task", async () => {
    const eventName = randomName("timeout_event");
    const baseTime = new Date("2024-05-01T10:00:00Z");
    const timeoutSeconds = 600;

    await thelper.setFakeNow(baseTime);

    absurd.registerTask({ name: "timeout-waiter" }, async (_params, ctx) => {
      try {
        const payload = await ctx.awaitEvent(eventName, {
          timeout: timeoutSeconds,
        });
        return { timedOut: false, result: payload };
      } catch (err) {
        if (err instanceof TimeoutError) {
          return { timedOut: true, result: null };
        }
        throw err;
      }
    });

    const { taskID, runID } = await absurd.spawn("timeout-waiter", undefined);
    await absurd.workBatch("worker1", 120, 1);

    const waitCountBefore = await thelper.pool.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM absurd.w_${thelper.queueName}`,
    );
    expect(Number(waitCountBefore.rows[0].count)).toBe(1);

    const sleepingRun = await thelper.getRun(runID);
    expect(sleepingRun).toMatchObject({
      state: "sleeping",
      wake_event: eventName,
    });
    const expectedWake = new Date(baseTime.getTime() + timeoutSeconds * 1000);
    expect(sleepingRun?.available_at?.getTime()).toBe(expectedWake.getTime());

    await thelper.setFakeNow(new Date(expectedWake.getTime() + 1000));
    await absurd.workBatch("worker1", 120, 1);

    expect(await thelper.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { timedOut: true, result: null },
    });

    const waitCountAfter = await thelper.pool.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM absurd.w_${thelper.queueName}`,
    );
    expect(Number(waitCountAfter.rows[0].count)).toBe(0);
  });

  test("multiple tasks can await the same event", async () => {
    const eventName = randomName("broadcast_event");

    absurd.registerTask<{ taskNum: number }>(
      { name: "multi-waiter" },
      async (params, ctx) => {
        const payload = await ctx.awaitEvent(eventName);
        return { taskNum: params.taskNum, received: payload };
      },
    );

    const tasks = await Promise.all([
      absurd.spawn("multi-waiter", { taskNum: 1 }),
      absurd.spawn("multi-waiter", { taskNum: 2 }),
      absurd.spawn("multi-waiter", { taskNum: 3 }),
    ]);

    // All tasks suspend waiting for event
    await absurd.workBatch("worker1", 60, 10);

    for (const task of tasks) {
      expect((await thelper.getTask(task.taskID))?.state).toBe("sleeping");
    }

    // Emit event once
    const payload = { data: "broadcast" };
    await absurd.emitEvent(eventName, payload);

    // All tasks should resume and complete
    await absurd.workBatch("worker1", 60, 10);

    for (let i = 0; i < tasks.length; i++) {
      const task = tasks[i];
      expect(await thelper.getTask(task.taskID)).toMatchObject({
        state: "completed",
        completed_payload: { taskNum: i + 1, received: payload },
      });
    }
  });

  test("awaitEvent timeout does not recreate wait on resume", async () => {
    const eventName = randomName("timeout_no_loop");
    const baseTime = new Date("2024-05-02T11:00:00Z");
    await thelper.setFakeNow(baseTime);

    absurd.registerTask({ name: "timeout-no-loop" }, async (_params, ctx) => {
      try {
        await ctx.awaitEvent(eventName, { stepName: "wait", timeout: 10 });
        return { stage: "unexpected" };
      } catch (err) {
        if (err instanceof TimeoutError) {
          const payload = await ctx.awaitEvent(eventName, {
            stepName: "wait",
            timeout: 10,
          });
          return { stage: "resumed", payload };
        }
        throw err;
      }
    });

    const { taskID, runID } = await absurd.spawn("timeout-no-loop", undefined);
    await absurd.workBatch("worker-timeout", 60, 1);

    const waitCount = await thelper.pool.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM absurd.w_${thelper.queueName}`,
    );
    expect(Number(waitCount.rows[0].count)).toBe(1);

    await thelper.setFakeNow(new Date(baseTime.getTime() + 15 * 1000));
    await absurd.workBatch("worker-timeout", 60, 1);

    const waitCountAfter = await thelper.pool.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM absurd.w_${thelper.queueName}`,
    );
    expect(Number(waitCountAfter.rows[0].count)).toBe(0);

    const run = await thelper.getRun(runID);
    expect(run?.state).toBe("completed");

    expect(await thelper.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { stage: "resumed", payload: null },
    });
  });
});
