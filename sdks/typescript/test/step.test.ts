import { describe, test, expect, beforeAll, afterEach } from "vitest";
import { createTestAbsurd, randomName, type TestContext } from "./setup.js";
import type { Absurd } from "../src/index.js";

describe("Step functionality", () => {
  let thelper: TestContext;
  let absurd: Absurd;

  beforeAll(async () => {
    thelper = await createTestAbsurd(randomName("step_queue"));
    absurd = thelper.absurd;
  });

  afterEach(async () => {
    await thelper.cleanupTasks();
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

    expect(await thelper.getTask(taskID)).toMatchObject({
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

    expect(await thelper.getTask(taskID)).toMatchObject({
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

    expect(await thelper.getTask(taskID)).toMatchObject({
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

    expect(await thelper.getTask(taskID)).toMatchObject({
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

    expect(await thelper.getTask(taskID)).toMatchObject({
      state: "completed",
      completed_payload: { result: "success" },
      attempts: 2,
    });
  });
});
