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
    await thelper.setFakeNow(null);
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

  test("cancellation by max duration", async () => {
    const baseTime = new Date("2024-05-01T09:00:00Z");
    await thelper.setFakeNow(baseTime);

    absurd.registerTask({ name: "duration-cancel" }, async () => {
      throw new Error("always fails");
    });

    const { taskID } = await absurd.spawn("duration-cancel", undefined, {
      maxAttempts: 4,
      retryStrategy: { kind: "fixed", baseSeconds: 30 },
      cancellation: { maxDuration: 90 },
    });

    await absurd.workBatch("worker1", 60, 1);

    await thelper.setFakeNow(new Date(baseTime.getTime() + 91 * 1000));
    await absurd.workBatch("worker1", 60, 1);

    const task = await thelper.getTask(taskID);
    expect(task?.state).toBe("cancelled");
    expect(task?.cancelled_at).not.toBeNull();

    const runs = await thelper.getRuns(taskID);
    expect(runs.length).toBe(2);
    expect(runs[1].state).toBe("cancelled");
  });

  test("cancellation by max delay", async () => {
    const baseTime = new Date("2024-05-01T08:00:00Z");
    await thelper.setFakeNow(baseTime);

    absurd.registerTask({ name: "delay-cancel" }, async () => {
      return { done: true };
    });

    const { taskID } = await absurd.spawn("delay-cancel", undefined, {
      cancellation: { maxDelay: 60 },
    });

    await thelper.setFakeNow(new Date(baseTime.getTime() + 61 * 1000));
    await absurd.workBatch("worker1", 60, 1);

    const task = await thelper.getTask(taskID);
    expect(task?.state).toBe("cancelled");
    expect(task?.cancelled_at).not.toBeNull();
  });

  test("manual cancel pending task", async () => {
    absurd.registerTask({ name: "pending-cancel" }, async () => {
      return { ok: true };
    });

    const { taskID } = await absurd.spawn("pending-cancel", { data: 1 });
    expect((await thelper.getTask(taskID))?.state).toBe("pending");

    await absurd.cancelTask(taskID);

    const task = await thelper.getTask(taskID);
    expect(task?.state).toBe("cancelled");
    expect(task?.cancelled_at).not.toBeNull();

    const claims = await absurd.claimTasks({
      workerId: "worker-1",
      claimTimeout: 60,
    });
    expect(claims).toHaveLength(0);
  });

  test("manual cancel running task", async () => {
    absurd.registerTask({ name: "running-cancel" }, async () => {
      return { ok: true };
    });

    const { taskID } = await absurd.spawn("running-cancel", { data: 1 });
    const [claim] = await absurd.claimTasks({
      workerId: "worker-1",
      claimTimeout: 60,
    });
    expect(claim.task_id).toBe(taskID);

    await absurd.cancelTask(taskID);

    const task = await thelper.getTask(taskID);
    expect(task?.state).toBe("cancelled");
    expect(task?.cancelled_at).not.toBeNull();
  });

  test("cancel blocks checkpoint writes", async () => {
    absurd.registerTask({ name: "checkpoint-cancel" }, async () => {
      return { ok: true };
    });

    const { taskID } = await absurd.spawn("checkpoint-cancel", { data: 1 });
    const [claim] = await absurd.claimTasks({
      workerId: "worker-1",
      claimTimeout: 60,
    });

    await absurd.cancelTask(taskID);

    await expect(
      thelper.pool.query(
        `SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)`,
        [
          thelper.queueName,
          taskID,
          "step-1",
          JSON.stringify({ result: "value" }),
          claim.run_id,
          60,
        ],
      ),
    ).rejects.toHaveProperty("code", "AB001");
  });

  test("cancel blocks awaitEvent registrations", async () => {
    absurd.registerTask({ name: "await-cancel" }, async () => {
      return { ok: true };
    });

    const { taskID } = await absurd.spawn("await-cancel", { data: 1 });
    const [claim] = await absurd.claimTasks({
      workerId: "worker-1",
      claimTimeout: 60,
    });

    await absurd.cancelTask(taskID);

    await expect(
      thelper.pool.query(`SELECT absurd.await_event($1, $2, $3, $4, $5, $6)`, [
        thelper.queueName,
        taskID,
        claim.run_id,
        "wait-step",
        "some-event",
        null,
      ]),
    ).rejects.toHaveProperty("code", "AB001");
  });

  test("cancel blocks extendClaim", async () => {
    absurd.registerTask({ name: "extend-cancel" }, async () => {
      return { ok: true };
    });

    const { taskID } = await absurd.spawn("extend-cancel", { data: 1 });
    const [claim] = await absurd.claimTasks({
      workerId: "worker-1",
      claimTimeout: 60,
    });

    await absurd.cancelTask(taskID);

    await expect(
      thelper.pool.query(`SELECT absurd.extend_claim($1, $2, $3)`, [
        thelper.queueName,
        claim.run_id,
        30,
      ]),
    ).rejects.toHaveProperty("code", "AB001");
  });

  test("cancel is idempotent", async () => {
    absurd.registerTask({ name: "idempotent-cancel" }, async () => {
      return { ok: true };
    });

    const { taskID } = await absurd.spawn("idempotent-cancel", { data: 1 });
    await absurd.cancelTask(taskID);
    const first = await thelper.getTask(taskID);
    expect(first?.cancelled_at).not.toBeNull();

    await absurd.cancelTask(taskID);
    const second = await thelper.getTask(taskID);
    expect(second?.cancelled_at?.getTime()).toBe(
      first?.cancelled_at?.getTime(),
    );
  });

  test("cancelling completed task is a no-op", async () => {
    absurd.registerTask({ name: "complete-cancel" }, async () => {
      return { status: "done" };
    });

    const { taskID } = await absurd.spawn("complete-cancel", { data: 1 });
    await absurd.workBatch("worker-1", 60, 1);

    await absurd.cancelTask(taskID);

    const task = await thelper.getTask(taskID);
    expect(task?.state).toBe("completed");
    expect(task?.cancelled_at).toBeNull();
  });

  test("cancelling failed task is a no-op", async () => {
    absurd.registerTask(
      { name: "failed-cancel", defaultMaxAttempts: 1 },
      async () => {
        throw new Error("boom");
      },
    );

    const { taskID } = await absurd.spawn("failed-cancel", { data: 1 });
    await absurd.workBatch("worker-1", 60, 1);

    await absurd.cancelTask(taskID);

    const task = await thelper.getTask(taskID);
    expect(task?.state).toBe("failed");
    expect(task?.cancelled_at).toBeNull();
  });

  test("cancel sleeping task transitions run to cancelled", async () => {
    const eventName = randomName("sleep-event");
    absurd.registerTask({ name: "sleep-cancel" }, async () => {
      return { ok: true };
    });

    const { taskID } = await absurd.spawn("sleep-cancel", { data: 1 });
    const [claim] = await absurd.claimTasks({
      workerId: "worker-1",
      claimTimeout: 60,
    });

    await thelper.pool.query(
      `SELECT absurd.await_event($1, $2, $3, $4, $5, $6)`,
      [thelper.queueName, taskID, claim.run_id, "wait-step", eventName, 300],
    );

    const sleepingTask = await thelper.getTask(taskID);
    expect(sleepingTask?.state).toBe("sleeping");

    await absurd.cancelTask(taskID);

    const cancelledTask = await thelper.getTask(taskID);
    expect(cancelledTask?.state).toBe("cancelled");
    const run = await thelper.getRun(claim.run_id);
    expect(run?.state).toBe("cancelled");
  });

  test("cancel non-existent task errors", async () => {
    await expect(
      absurd.cancelTask("019a32d3-8425-7ae2-a5af-2f17a6707666"),
    ).rejects.toThrow(/not found/i);
  });
});
