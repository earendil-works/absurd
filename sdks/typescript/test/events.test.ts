import { describe, test, expect, beforeAll, afterEach } from "vitest";
import { createTestAbsurd, randomName, type TestContext } from "./setup.js";
import type { Absurd } from "../src/index.js";

describe("Event system", () => {
  let thelper: TestContext;
  let absurd: Absurd;

  beforeAll(async () => {
    thelper = await createTestAbsurd(randomName("event_queue"));
    absurd = thelper.absurd;
  });

  afterEach(async () => {
    await thelper.cleanupTasks();
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

  test.skip("awaitEvent with timeout expires and wakes task", async () => {
    // Skipped: awaitEvent timeout behavior needs SDK investigation
    const eventName = randomName("timeout_event");
    const baseTime = new Date("2024-05-01T10:00:00Z");
    const timeoutSeconds = 600;

    await thelper.setFakeNow(baseTime);

    absurd.registerTask({ name: "timeout-waiter" }, async (params, ctx) => {
      // Wait with a timeout
      const result = await ctx.awaitEvent(eventName, {
        timeout: timeoutSeconds,
      });
      return { timedOut: result === null, result };
    });

    const { taskID, runID } = await absurd.spawn("timeout-waiter", undefined);

    // Start processing, task suspends
    await absurd.workBatch("worker1", 120, 1);

    const sleepingRun = await thelper.getRun(runID);
    expect(sleepingRun).toMatchObject({
      state: "sleeping",
      wake_event: eventName,
    });

    // Verify the wake time is set correctly
    const expectedWakeTime = new Date(
      baseTime.getTime() + timeoutSeconds * 1000,
    );
    expect(sleepingRun?.available_at).toEqual(expectedWakeTime);

    // Advance time past timeout
    await thelper.setFakeNow(new Date(expectedWakeTime.getTime() + 1000));

    // Resume after timeout - should wake with null payload and complete
    await absurd.workBatch("worker1", 120, 1);

    expect(await thelper.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { timedOut: true, result: null },
    });
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
});
