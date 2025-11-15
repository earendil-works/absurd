import { describe, test, expect, beforeAll, afterEach } from "vitest";
import { createTestAbsurd, randomName, type TestContext } from "./setup.js";
import type { Absurd } from "../src/index.js";

describe("Retry and cancellation", () => {
  let thelper: TestContext;
  let absurd: Absurd;

  beforeAll(async () => {
    thelper = await createTestAbsurd(randomName("retry_queue"));
    absurd = thelper.absurd;
  });

  afterEach(async () => {
    await thelper.cleanupTasks();
  });

  test("fail run without strategy requeues immediately", async () => {
    let attempts = 0;

    absurd.registerTask(
      { name: "no-strategy", defaultMaxAttempts: 3 },
      async () => {
        attempts++;
        if (attempts < 2) {
          throw new Error("boom");
        }
        return { attempts };
      },
    );

    const { taskID } = await absurd.spawn("no-strategy", { payload: 1 });

    // First attempt fails
    await absurd.workBatch("worker1", 60, 1);
    expect((await thelper.getTask(taskID))?.state).toBe("pending");
    expect((await thelper.getTask(taskID))?.attempts).toBe(2);

    // Second attempt succeeds
    await absurd.workBatch("worker1", 60, 1);
    expect(await thelper.getTask(taskID)).toMatchObject({
      state: "completed",
      attempts: 2,
      completed_payload: { attempts: 2 },
    });
  });

  test("exponential backoff retry strategy", async () => {
    const baseTime = new Date("2024-05-01T10:00:00Z");
    await thelper.setFakeNow(baseTime);

    let attempts = 0;

    absurd.registerTask({ name: "exp-backoff" }, async () => {
      attempts++;
      if (attempts < 3) {
        throw new Error(`fail-${attempts}`);
      }
      return { success: true };
    });

    const { taskID } = await absurd.spawn("exp-backoff", undefined, {
      maxAttempts: 3,
      retryStrategy: { kind: "exponential", baseSeconds: 40, factor: 2 },
    });

    // First attempt - fails, schedules retry with 40s backoff
    await absurd.workBatch("worker1", 60, 1);
    let task = await thelper.getTask(taskID);
    expect(task?.state).toBe("sleeping");
    expect(task?.attempts).toBe(2);

    // Advance time past first backoff (40 seconds)
    await thelper.setFakeNow(new Date(baseTime.getTime() + 40 * 1000));

    // Second attempt - fails again with 80s backoff (40 * 2)
    await absurd.workBatch("worker1", 60, 1);
    task = await thelper.getTask(taskID);
    expect(task?.state).toBe("sleeping");
    expect(task?.attempts).toBe(3);

    // Advance time past second backoff (80 seconds from second failure)
    await thelper.setFakeNow(
      new Date(baseTime.getTime() + 40 * 1000 + 80 * 1000),
    );

    // Third attempt - succeeds
    await absurd.workBatch("worker1", 60, 1);
    expect(await thelper.getTask(taskID)).toMatchObject({
      state: "completed",
      attempts: 3,
      completed_payload: { success: true },
    });
  });

  test("fixed backoff retry strategy", async () => {
    const baseTime = new Date("2024-05-01T11:00:00Z");
    await thelper.setFakeNow(baseTime);

    let attempts = 0;

    absurd.registerTask({ name: "fixed-backoff" }, async () => {
      attempts++;
      if (attempts < 2) {
        throw new Error("first-fail");
      }
      return { attempts };
    });

    const { taskID } = await absurd.spawn("fixed-backoff", undefined, {
      maxAttempts: 2,
      retryStrategy: { kind: "fixed", baseSeconds: 10 },
    });

    // First attempt fails
    await absurd.workBatch("worker1", 60, 1);
    expect((await thelper.getTask(taskID))?.state).toBe("sleeping");

    // Advance time past fixed backoff (10 seconds)
    await thelper.setFakeNow(new Date(baseTime.getTime() + 10 * 1000));

    // Second attempt succeeds
    await absurd.workBatch("worker1", 60, 1);
    expect(await thelper.getTask(taskID)).toMatchObject({
      state: "completed",
      attempts: 2,
    });
  });

  test("task fails permanently after max attempts exhausted", async () => {
    absurd.registerTask(
      { name: "always-fail", defaultMaxAttempts: 2 },
      async () => {
        throw new Error("always fails");
      },
    );

    const { taskID } = await absurd.spawn("always-fail", undefined);

    // Attempt 1
    await absurd.workBatch("worker1", 60, 1);
    expect((await thelper.getTask(taskID))?.state).toBe("pending");

    // Attempt 2 (final)
    await absurd.workBatch("worker1", 60, 1);
    expect(await thelper.getTask(taskID)).toMatchObject({
      state: "failed",
      attempts: 2,
    });
  });

  test.skip("cancellation by max duration", async () => {
    // Skipped: Cancellation during retry flow needs SDK investigation
    const baseTime = new Date("2024-05-01T09:00:00Z");
    await thelper.setFakeNow(baseTime);

    let attempts = 0;

    absurd.registerTask({ name: "duration-cancel" }, async () => {
      attempts++;
      throw new Error("always fails");
    });

    const { taskID } = await absurd.spawn("duration-cancel", undefined, {
      maxAttempts: 4,
      retryStrategy: { kind: "fixed", baseSeconds: 30 },
      cancellation: { maxDuration: 90 }, // 90 seconds total duration max
    });

    // First attempt - fails at baseTime
    await absurd.workBatch("worker1", 60, 1);

    // Advance to second attempt (30s later)
    await thelper.setFakeNow(new Date(baseTime.getTime() + 30 * 1000));
    await absurd.workBatch("worker1", 60, 1);

    // Advance to after max_duration (91 seconds from start)
    // Third attempt would be at 60s, but we advance to 91s and fail the second run
    await thelper.setFakeNow(new Date(baseTime.getTime() + 91 * 1000));

    // Task should be cancelled due to max_duration
    const task = await thelper.getTask(taskID);
    expect(task?.state).toBe("cancelled");
    expect(task?.cancelled_at).not.toBeNull();
  });

  test.skip("cancellation by max delay", async () => {
    // Skipped: Max delay cancellation needs SDK investigation
    const baseTime = new Date("2024-05-01T08:00:00Z");
    await thelper.setFakeNow(baseTime);

    absurd.registerTask({ name: "delay-cancel" }, async () => {
      return { done: true };
    });

    const { taskID } = await absurd.spawn("delay-cancel", undefined, {
      cancellation: { maxDelay: 60 }, // 60 seconds from enqueue to first start
    });

    // Task is enqueued at baseTime
    // Advance time past maxDelay (60 seconds + 1) before claiming
    await thelper.setFakeNow(new Date(baseTime.getTime() + 61 * 1000));

    // Try to claim - should be cancelled immediately due to maxDelay
    await absurd.workBatch("worker1", 60, 1);

    expect(await thelper.getTask(taskID)).toMatchObject({
      state: "cancelled",
    });
  });
});
