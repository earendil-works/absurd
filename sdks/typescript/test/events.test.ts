import { afterEach, beforeAll, describe, expect, test } from "./testlib.ts";
import { createTestAbsurd, randomName, type TestContext } from "./setup.ts";
import type { Absurd } from "../src/index.ts";
import { TimeoutError } from "../src/index.ts";

describe("Event system", () => {
  let ctx: TestContext;
  let absurd: Absurd;

  beforeAll(async () => {
    ctx = await createTestAbsurd(randomName("event_queue"));
    absurd = ctx.absurd;
  });

  afterEach(async () => {
    await ctx.cleanupTasks();
    await ctx.setFakeNow(null);
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

    const sleepingRun = await ctx.getRun(runID);
    expect(sleepingRun).toMatchObject({
      state: "sleeping",
      wake_event: eventName,
    });

    // Emit event
    const payload = { value: 42 };
    await absurd.emitEvent(eventName, payload);

    // Task should now be pending
    const pendingRun = await ctx.getRun(runID);
    expect(pendingRun?.state).toBe("pending");

    // Resume and complete
    await absurd.workBatch("worker1", 60, 1);

    expect(await ctx.getTask(taskID)).toMatchObject({
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

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { received: payload },
    });
  });

  test("emitEvent is first-write-wins", async () => {
    const eventName = randomName("stable_event");
    const firstPayload = { value: 1 };
    const secondPayload = { value: 2 };
    const firstEmitAt = new Date("2024-05-01T09:00:00Z");

    await ctx.setFakeNow(firstEmitAt);
    await absurd.emitEvent(eventName, firstPayload);

    await ctx.setFakeNow(new Date(firstEmitAt.getTime() + 30 * 1000));
    await absurd.emitEvent(eventName, secondPayload);

    const eventRows = await ctx.pool.query<{
      payload: unknown;
      emitted_at: Date;
    }>(
      `SELECT payload, emitted_at FROM absurd.e_${ctx.queueName} WHERE event_name = $1`,
      [eventName],
    );
    expect(eventRows.rows).toHaveLength(1);
    expect(eventRows.rows[0].payload).toEqual(firstPayload);
    expect(new Date(eventRows.rows[0].emitted_at).getTime()).toBe(
      firstEmitAt.getTime(),
    );

    absurd.registerTask(
      { name: "late-first-write-waiter" },
      async (_params, ctx) => {
        const received = await ctx.awaitEvent(eventName);
        return { received };
      },
    );

    const { taskID } = await absurd.spawn("late-first-write-waiter", undefined);
    await absurd.workBatch("worker1", 60, 1);

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { received: firstPayload },
    });
  });

  test("awaitEvent with timeout expires and wakes task", async () => {
    const eventName = randomName("timeout_event");
    const baseTime = new Date("2024-05-01T10:00:00Z");
    const timeoutSeconds = 600;

    await ctx.setFakeNow(baseTime);

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

    const waitCountBefore = await ctx.pool.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM absurd.w_${ctx.queueName}`,
    );
    expect(Number(waitCountBefore.rows[0].count)).toBe(1);

    const sleepingRun = await ctx.getRun(runID);
    expect(sleepingRun).toMatchObject({
      state: "sleeping",
      wake_event: eventName,
    });
    const expectedWake = new Date(baseTime.getTime() + timeoutSeconds * 1000);
    expect(sleepingRun?.available_at?.getTime()).toBe(expectedWake.getTime());

    await ctx.setFakeNow(new Date(expectedWake.getTime() + 1000));
    await absurd.workBatch("worker1", 120, 1);

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { timedOut: true, result: null },
    });

    const waitCountAfter = await ctx.pool.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM absurd.w_${ctx.queueName}`,
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
      expect((await ctx.getTask(task.taskID))?.state).toBe("sleeping");
    }

    // Emit event once
    const payload = { data: "broadcast" };
    await absurd.emitEvent(eventName, payload);

    // All tasks should resume and complete
    await absurd.workBatch("worker1", 60, 10);

    for (let i = 0; i < tasks.length; i++) {
      const task = tasks[i];
      expect(await ctx.getTask(task.taskID)).toMatchObject({
        state: "completed",
        completed_payload: { taskNum: i + 1, received: payload },
      });
    }
  });

  test("awaitEvent timeout does not recreate wait on resume", async () => {
    const eventName = randomName("timeout_no_loop");
    const baseTime = new Date("2024-05-02T11:00:00Z");
    await ctx.setFakeNow(baseTime);

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

    const waitCount = await ctx.pool.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM absurd.w_${ctx.queueName}`,
    );
    expect(Number(waitCount.rows[0].count)).toBe(1);

    await ctx.setFakeNow(new Date(baseTime.getTime() + 15 * 1000));
    await absurd.workBatch("worker-timeout", 60, 1);

    const waitCountAfter = await ctx.pool.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM absurd.w_${ctx.queueName}`,
    );
    expect(Number(waitCountAfter.rows[0].count)).toBe(0);

    const run = await ctx.getRun(runID);
    expect(run?.state).toBe("completed");

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { stage: "resumed", payload: null },
    });
  });

  test("wakes on first matching event", async () => {
    const accepted = randomName("order_accepted");
    const rejected = randomName("order_rejected");

    absurd.registerTask({ name: "await-any-basic" }, async (_params, ctx) => {
      return await ctx.awaitAnyEvent([accepted, rejected], { timeout: 60 });
    });

    const { taskID, runID } = await absurd.spawn("await-any-basic", undefined);
    await absurd.workBatch("worker1", 60, 1);

    const sleepingRun = await ctx.getRun(runID);
    expect(sleepingRun?.state).toBe("sleeping");

    await absurd.emitEvent(accepted, { status: "ok" });
    await absurd.workBatch("worker1", 60, 1);

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: {
        event_name: accepted,
        payload: { status: "ok" },
        remaining: [rejected],
      },
    });
  });

  test("wakes on second event if first did not fire", async () => {
    const accepted = randomName("order_accepted2");
    const rejected = randomName("order_rejected2");

    absurd.registerTask({ name: "await-any-second" }, async (_params, ctx) => {
      return await ctx.awaitAnyEvent([accepted, rejected], { timeout: 60 });
    });

    const { taskID } = await absurd.spawn("await-any-second", undefined);
    await absurd.workBatch("worker1", 60, 1);

    await absurd.emitEvent(rejected, { reason: "too busy" });
    await absurd.workBatch("worker1", 60, 1);

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: {
        event_name: rejected,
        payload: { reason: "too busy" },
        remaining: [accepted],
      },
    });
  });

  test("both events emitted before await — returns earliest emitted", async () => {
    const first = randomName("first_event");
    const second = randomName("second_event");
    const baseTime = new Date("2024-05-01T10:00:00Z");

    await ctx.setFakeNow(baseTime);
    await absurd.emitEvent(first, { order: 1 });

    await ctx.setFakeNow(new Date(baseTime.getTime() + 5000));
    await absurd.emitEvent(second, { order: 2 });

    absurd.registerTask(
      { name: "await-any-pre-emitted" },
      async (_params, ctx) => {
        return await ctx.awaitAnyEvent([first, second]);
      },
    );

    const { taskID } = await absurd.spawn("await-any-pre-emitted", undefined);
    await absurd.workBatch("worker1", 60, 1);

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: {
        event_name: first,
        payload: { order: 1 },
        remaining: [second],
      },
    });
  });

  test("sibling wait rows cleaned up after one event fires", async () => {
    const e1 = randomName("cleanup_e1");
    const e2 = randomName("cleanup_e2");
    const e3 = randomName("cleanup_e3");

    absurd.registerTask({ name: "await-any-cleanup" }, async (_params, ctx) => {
      return await ctx.awaitAnyEvent([e1, e2, e3], { timeout: 60 });
    });

    await absurd.spawn("await-any-cleanup", undefined);
    await absurd.workBatch("worker1", 60, 1);

    const waitsBefore = await ctx.pool.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM absurd.w_${ctx.queueName}`,
    );
    expect(Number(waitsBefore.rows[0].count)).toBe(3);

    await absurd.emitEvent(e2, { fired: true });

    const waitsAfter = await ctx.pool.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM absurd.w_${ctx.queueName}`,
    );
    expect(Number(waitsAfter.rows[0].count)).toBe(0);
  });

  test("timeout fires if no event emitted", async () => {
    const e1 = randomName("timeout_e1");
    const e2 = randomName("timeout_e2");
    const baseTime = new Date("2024-05-01T12:00:00Z");
    const timeoutSeconds = 300;

    await ctx.setFakeNow(baseTime);

    absurd.registerTask({ name: "await-any-timeout" }, async (_params, ctx) => {
      try {
        return await ctx.awaitAnyEvent([e1, e2], { timeout: timeoutSeconds });
      } catch (err) {
        if (err instanceof TimeoutError) {
          return { timedOut: true };
        }
        throw err;
      }
    });

    const { taskID } = await absurd.spawn("await-any-timeout", undefined);
    await absurd.workBatch("worker1", 120, 1);

    await ctx.setFakeNow(
      new Date(baseTime.getTime() + (timeoutSeconds + 1) * 1000),
    );
    await absurd.workBatch("worker1", 120, 1);

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { timedOut: true },
    });
  });

  test("result is stable on replay regardless of later events", async () => {
    const e1 = randomName("replay_e1");
    const e2 = randomName("replay_e2");

    absurd.registerTask({ name: "await-any-replay" }, async (_params, ctx) => {
      const result = await ctx.awaitAnyEvent([e1, e2], { timeout: 60 });
      // second step to force a retry/replay scenario
      return await ctx.step("confirm", async () => result);
    });

    const { taskID } = await absurd.spawn("await-any-replay", undefined);
    await absurd.workBatch("worker1", 60, 1);

    await absurd.emitEvent(e1, { winner: true });
    // emit e2 as well — should be ignored
    await absurd.emitEvent(e2, { winner: false });

    await absurd.workBatch("worker1", 60, 1);

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: {
        event_name: e1,
        payload: { winner: true },
        remaining: [e2],
      },
    });
  });
});
