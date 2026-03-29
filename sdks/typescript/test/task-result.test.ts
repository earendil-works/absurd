import { beforeAll, afterEach, describe, expect, test, vi } from "./testlib.ts";
import { createTestAbsurd, randomName, type TestContext } from "./setup.ts";
import { Absurd } from "../src/index.ts";

describe("Task result APIs", () => {
  let ctx: TestContext;
  let absurd: Absurd;

  beforeAll(async () => {
    ctx = await createTestAbsurd(randomName("task_result_queue"));
    absurd = ctx.absurd;
  });

  afterEach(async () => {
    await ctx.cleanupTasks();
    await ctx.setFakeNow(null);
  });

  test("fetchTaskResult returns lifecycle snapshots", async () => {
    absurd.registerTask({ name: "fetch-target" }, async () => ({ ok: true }));

    const spawned = await absurd.spawn("fetch-target", {});

    expect(await absurd.fetchTaskResult(spawned.taskID)).toMatchObject({
      state: "pending",
    });

    const [claim] = await absurd.claimTasks({
      workerId: "fetch-worker",
      claimTimeout: 60,
    });
    expect(claim.run_id).toBe(spawned.runID);

    expect(await absurd.fetchTaskResult(spawned.taskID)).toMatchObject({
      state: "running",
    });

    await ctx.pool.query(`SELECT absurd.complete_run($1, $2, $3)`, [
      ctx.queueName,
      claim.run_id,
      JSON.stringify({ ok: true }),
    ]);

    expect(await absurd.fetchTaskResult(spawned.taskID)).toEqual({
      state: "completed",
      result: { ok: true },
    });
  });

  test("awaitTaskResult times out for non-terminal tasks", async () => {
    absurd.registerTask({ name: "await-timeout" }, async () => ({ ok: true }));
    const spawned = await absurd.spawn("await-timeout", {});

    await expect(
      absurd.awaitTaskResult(spawned.taskID, {
        timeout: 0.05,
      }),
    ).rejects.toHaveProperty("name", "TimeoutError");
  });

  test("awaitTaskResult rejects invalid timeout values", async () => {
    absurd.registerTask({ name: "await-invalid-timeout" }, async () => ({
      ok: true,
    }));
    const spawned = await absurd.spawn("await-invalid-timeout", {});

    await expect(
      absurd.awaitTaskResult(spawned.taskID, {
        timeout: Number.NaN,
      }),
    ).rejects.toThrow("timeout must be a finite non-negative number");

    await expect(
      absurd.awaitTaskResult(spawned.taskID, {
        timeout: -1,
      }),
    ).rejects.toThrow("timeout must be a finite non-negative number");
  });

  test("awaitTaskResult resolves once task completes", async () => {
    absurd.registerTask({ name: "await-complete" }, async () => ({
      done: true,
    }));

    const worker = await absurd.startWorker({
      concurrency: 1,
      pollInterval: 0.01,
    });
    try {
      const spawned = await absurd.spawn("await-complete", {});

      const snapshot = await absurd.awaitTaskResult(spawned.taskID, {
        timeout: 5,
      });

      expect(snapshot).toEqual({
        state: "completed",
        result: { done: true },
      });
    } finally {
      await worker.close();
    }
  });

  test("task context awaitTaskResult polls child task to completion in another queue", async () => {
    const childQueue = randomName("task_result_child_queue");
    const childAbsurd = new Absurd({ db: ctx.pool, queueName: childQueue });
    await childAbsurd.createQueue(childQueue);

    childAbsurd.registerTask({ name: "child-task" }, async () => ({
      child: "ok",
    }));

    absurd.registerTask({ name: "parent-task" }, async (_params, taskCtx) => {
      const child = await childAbsurd.spawn("child-task", {});
      const childResult = await taskCtx.awaitTaskResult(child.taskID, {
        queue: childQueue,
        timeout: 5,
      });

      if (childResult.state !== "completed") {
        throw new Error(`unexpected child state: ${childResult.state}`);
      }

      return { child: childResult.result };
    });

    const parentWorker = await absurd.startWorker({
      concurrency: 1,
      pollInterval: 0.01,
      claimTimeout: 120,
    });
    const childWorker = await childAbsurd.startWorker({
      concurrency: 1,
      pollInterval: 0.01,
      claimTimeout: 120,
    });

    try {
      const spawned = await absurd.spawn("parent-task", {});

      await vi.waitFor(
        async () => {
          const task = await ctx.getTask(spawned.taskID);
          expect(task?.state).toBe("completed");
        },
        { timeout: 5000, interval: 25 },
      );

      const task = await ctx.getTask(spawned.taskID);
      expect(task?.completed_payload).toEqual({ child: { child: "ok" } });
    } finally {
      await childWorker.close();
      await parentWorker.close();
    }
  });

  test("task context awaitTaskResult fails fast on same-queue waits", async () => {
    absurd.registerTask({ name: "same-queue-child" }, async () => ({
      child: "ok",
    }));

    absurd.registerTask({ name: "same-queue-parent" }, async (_params, taskCtx) => {
      const child = await absurd.spawn("same-queue-child", {});
      await taskCtx.awaitTaskResult(child.taskID, {
        timeout: 120,
      });
      return { ok: true };
    });

    const worker = await absurd.startWorker({
      concurrency: 1,
      pollInterval: 0.01,
      claimTimeout: 120,
    });

    try {
      const spawned = await absurd.spawn("same-queue-parent", {});

      await vi.waitFor(
        async () => {
          const task = await ctx.getTask(spawned.taskID);
          expect(task?.state).toBe("failed");
        },
        { timeout: 2000, interval: 25 },
      );

      const runs = await ctx.getRuns(spawned.taskID);
      expect((runs[0]?.failure_reason as any)?.message).toContain(
        "cannot wait on tasks in the same queue",
      );
    } finally {
      await worker.close();
    }
  });
});
