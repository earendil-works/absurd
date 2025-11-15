import { describe, test, expect, beforeAll, afterEach, vi } from "vitest";
import { createTestAbsurd, randomName, type TestContext } from "./setup.js";
import type { Absurd } from "../src/index.js";

describe("Step functionality", () => {
  let ctx: TestContext;
  let absurd: Absurd;

  beforeAll(async () => {
    ctx = await createTestAbsurd(randomName("step_queue"));
    absurd = ctx.absurd;
  });

  afterEach(async () => {
    await ctx.cleanupTasks();
    await ctx.setFakeNow(null);
    vi.useRealTimers();
  });

  test("step executes and returns value", async () => {
    absurd.registerTask<{ value: number }, { result: string }>(
      { name: "basic" },
      async (params, ctx) => {
        const result = await ctx.step("process", async () => {
          return `processed-${params.value}`;
        });
        return { result };
      },
    );

    const { taskID } = await absurd.spawn("basic", { value: 42 });
    await absurd.workBatch(randomName("w"), 60, 1);

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { result: "processed-42" },
    });
  });

  test("step result is cached and not re-executed on retry", async () => {
    let executionCount = 0;
    let attemptCount = 0;

    absurd.registerTask<void, { random: number; count: number }>(
      { name: "cache", defaultMaxAttempts: 2 },
      async (params, ctx) => {
        attemptCount++;

        const cached = await ctx.step("generate-random", async () => {
          executionCount++;
          return Math.random();
        });

        if (attemptCount === 1) {
          throw new Error("Intentional failure");
        }

        return { random: cached, count: executionCount };
      },
    );

    const { taskID } = await absurd.spawn("cache", undefined);

    const workerID = randomName("w");
    await absurd.workBatch(workerID, 60, 1);
    expect(executionCount).toBe(1);

    await absurd.workBatch(workerID, 60, 1);
    expect(executionCount).toBe(1);
    expect(attemptCount).toBe(2);

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { count: 1 },
      attempts: 2,
    });
  });

  test("task with multiple steps only re-executes uncompleted steps on retry", async () => {
    const executed: string[] = [];
    let attemptCount = 0;

    absurd.registerTask<void, { steps: string[]; attemptNum: number }>(
      { name: "multistep-retry", defaultMaxAttempts: 2 },
      async (params, ctx) => {
        attemptCount++;

        const step1 = await ctx.step("step1", async () => {
          executed.push("step1");
          return "result1";
        });

        const step2 = await ctx.step("step2", async () => {
          executed.push("step2");
          return "result2";
        });

        if (attemptCount === 1) {
          throw new Error("Fail before step3");
        }

        const step3 = await ctx.step("step3", async () => {
          executed.push("step3");
          return "result3";
        });

        return { steps: [step1, step2, step3], attemptNum: attemptCount };
      },
    );

    const { taskID } = await absurd.spawn("multistep-retry", undefined);

    const workerID = randomName("w");
    await absurd.workBatch(workerID, 60, 1);
    expect(executed).toEqual(["step1", "step2"]);

    await absurd.workBatch(workerID, 60, 1);
    expect(executed).toEqual(["step1", "step2", "step3"]);

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: {
        steps: ["result1", "result2", "result3"],
        attemptNum: 2,
      },
      attempts: 2,
    });
  });

  test("repeated step names work correctly", async () => {
    absurd.registerTask<void, { results: number[] }>(
      { name: "deduplicate" },
      async (params, ctx) => {
        const results: number[] = [];
        for (let i = 0; i < 3; i++) {
          const result = await ctx.step("loop-step", async () => {
            return i * 10;
          });
          results.push(result);
        }
        return { results };
      },
    );

    const { taskID } = await absurd.spawn("deduplicate", undefined);
    await absurd.workBatch(randomName("w"), 60, 1);

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { results: [0, 10, 20] },
    });
  });

  test("failed step does not save checkpoint and re-executes on retry", async () => {
    let attemptCount = 0;

    absurd.registerTask<void, { result: string }>(
      { name: "fail", defaultMaxAttempts: 2 },
      async (_, ctx) => {
        const result = await ctx.step("fail", async () => {
          attemptCount++;
          if (attemptCount === 1) {
            throw new Error("Step fails on first attempt");
          }
          return "success";
        });

        return { result };
      },
    );

    const { taskID } = await absurd.spawn("fail", undefined);

    const workerID = randomName("w");
    await absurd.workBatch(workerID, 60, 1);
    expect(attemptCount).toBe(1);

    await absurd.workBatch(workerID, 60, 1);
    expect(attemptCount).toBe(2);

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { result: "success" },
      attempts: 2,
    });
  });

  test("sleepFor suspends until duration elapses", async () => {
    vi.useFakeTimers();
    const base = new Date("2024-05-05T10:00:00Z");
    vi.setSystemTime(base);
    await ctx.setFakeNow(base);

    const durationSeconds = 60;
    absurd.registerTask({ name: "sleep-for" }, async (_params, ctx) => {
      await ctx.sleepFor("wait-for", durationSeconds);
      return { resumed: true };
    });

    const { taskID, runID } = await absurd.spawn("sleep-for", undefined);
    await absurd.workBatch("worker-sleep", 120, 1);

    const sleepingRun = await ctx.getRun(runID);
    expect(sleepingRun).toMatchObject({
      state: "sleeping",
    });
    const wakeTime = new Date(base.getTime() + durationSeconds * 1000);
    expect(sleepingRun?.available_at?.getTime()).toBe(wakeTime.getTime());

    const resumeTime = new Date(wakeTime.getTime() + 5 * 1000);
    vi.setSystemTime(resumeTime);
    await ctx.setFakeNow(resumeTime);
    await absurd.workBatch("worker-sleep", 120, 1);

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { resumed: true },
    });
  });

  test("sleepUntil checkpoint prevents re-scheduling when wake time passed", async () => {
    vi.useFakeTimers();
    const base = new Date("2024-05-06T09:00:00Z");
    vi.setSystemTime(base);
    await ctx.setFakeNow(base);

    const wakeTime = new Date(base.getTime() + 5 * 60 * 1000);
    let executions = 0;

    absurd.registerTask({ name: "sleep-until" }, async (_params, ctx) => {
      executions++;
      await ctx.sleepUntil("sleep-step", wakeTime);
      return { executions };
    });

    const { taskID, runID } = await absurd.spawn("sleep-until", undefined);
    await absurd.workBatch("worker-sleep", 120, 1);

    const checkpointRow = await ctx.pool.query<{
      checkpoint_name: string;
      state: string;
      owner_run_id: string;
    }>(
      `SELECT checkpoint_name, state, owner_run_id FROM absurd.c_${ctx.queueName} WHERE task_id = $1`,
      [taskID],
    );
    expect(checkpointRow.rows[0]).toMatchObject({
      checkpoint_name: "sleep-step",
      owner_run_id: runID,
      state: wakeTime.toISOString(),
    });

    const sleepingRun = await ctx.getRun(runID);
    expect(sleepingRun?.state).toBe("sleeping");

    vi.setSystemTime(wakeTime);
    await ctx.setFakeNow(wakeTime);
    await absurd.workBatch("worker-sleep", 120, 1);

    expect(await ctx.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { executions: 2 },
    });
    expect(executions).toBe(2);
  });
});
