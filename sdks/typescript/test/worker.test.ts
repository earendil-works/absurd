import { describe, it, expect, beforeAll, afterEach, vi } from "vitest";
import { EventEmitter, once } from "events";
import type { TestContext } from "./setup";
import { createTestAbsurd, randomName } from "./setup";
import type { Absurd } from "../src/index";

describe("Worker", () => {
  let ctx: TestContext;
  let absurd: Absurd;

  beforeAll(async () => {
    ctx = await createTestAbsurd(randomName("worker"));
    absurd = ctx.absurd;
  });

  afterEach(async () => {
    await ctx.cleanupTasks();
    vi.restoreAllMocks();
  });

  it("respects concurrency limit and skips claims when at capacity", async () => {
    const gate = new EventEmitter();
    const atGate = new Set<number>();

    gate.on("arrived", (id: number) => atGate.add(id));
    gate.on("left", (id: number) => atGate.delete(id));

    absurd.registerTask<{ id: number }, void>(
      { name: "gated-task" },
      async (params) => {
        gate.emit("arrived", params.id);
        await once(gate, "release");
        gate.emit("left", params.id);
      },
    );

    for (let i = 1; i <= 5; i++) {
      await absurd.spawn("gated-task", { id: i });
    }

    const claimSpy = vi.spyOn(absurd, "claimTasks");

    const concurrency = 2;
    const worker = await absurd.startWorker({
      concurrency,
      pollInterval: 0.01,
    });

    // Wait until we are at capacity
    await vi.waitFor(() => expect(atGate.size).toBe(concurrency), {
      timeout: 100,
    });

    expect(claimSpy).toHaveBeenCalledTimes(1);
    expect(claimSpy).toHaveBeenCalledWith(
      expect.objectContaining({ batchSize: concurrency }),
    );

    // Wait for ~5 poll cycles
    await ctx.sleep(50);

    // Things should still be the same, no more claims
    expect(atGate.size).toBe(concurrency);
    expect(claimSpy).toHaveBeenCalledTimes(1);

    gate.emit("release");
    await worker.close();
    expect(atGate.size).toBe(0);
  });

  it("polls again immediately when capacity frees up", async () => {
    const gate = new EventEmitter();
    const atGate = new Set<number>();

    gate.on("arrived", (id: number) => atGate.add(id));
    gate.on("left", (id: number) => atGate.delete(id));

    absurd.registerTask<{ id: number }, void>(
      { name: "responsive-task" },
      async (params) => {
        gate.emit("arrived", params.id);
        await once(gate, "release");
        gate.emit("left", params.id);
      },
    );

    for (let i = 1; i <= 3; i++) {
      await absurd.spawn("responsive-task", { id: i });
    }

    const claimSpy = vi.spyOn(absurd, "claimTasks");

    const worker = await absurd.startWorker({
      concurrency: 2,
      pollInterval: 3600, // once every hour
    });

    await vi.waitFor(() => expect(atGate.size).toBe(2), {
      timeout: 100,
    });

    expect(claimSpy).toHaveBeenCalledTimes(1);
    const initialCallCount = claimSpy.mock.calls.length;

    // Free current tasks so the worker can immediately claim again
    gate.emit("release");

    // Polls again without waiting for pollInterval
    await vi.waitFor(() => {
      expect(claimSpy.mock.calls.length).toBeGreaterThan(initialCallCount);
    });

    // Release any remaining tasks so the worker can shut down
    gate.emit("release");
    await worker.close();
  });

  it("shuts down gracefully", async () => {
    const gate = new EventEmitter();
    const started: number[] = [];

    gate.on("started", (id: number) => started.push(id));

    absurd.registerTask<{ id: number }, void>(
      { name: "shutdown-task" },
      async (params) => {
        gate.emit("started", params.id);
        await once(gate, "release");
      },
    );

    await absurd.spawn("shutdown-task", { id: 1 });
    await absurd.spawn("shutdown-task", { id: 2 });

    const worker = await absurd.startWorker({ concurrency: 1 });
    await once(gate, "started");

    const closePromise = worker.close();
    const claimSpy = vi.spyOn(absurd, "claimTasks");

    let resolved = false;
    closePromise.then(() => {
      resolved = true;
    });

    // Does not resolve until the task finishes
    await ctx.sleep(50);
    expect(resolved).toBe(false);

    gate.emit("release");

    await vi.waitFor(() => expect(resolved).toBe(true), {
      timeout: 200,
    });

    expect(started).toEqual([1]);
    expect(claimSpy).not.toHaveBeenCalled();
  });

  it("calls onError for unexpected failures", async () => {
    const errors: string[] = [];

    absurd.registerTask<void, void>({ name: "test-task" }, async () => {});
    await absurd.spawn("test-task", undefined);

    vi.spyOn(absurd, "executeTask").mockRejectedValueOnce(
      new Error("Execute failed, PG error"),
    );

    const worker = await absurd.startWorker({
      pollInterval: 0.01,
      onError: (err) => errors.push(err.message),
    });

    await vi.waitFor(
      () => expect(errors).toContain("Execute failed, PG error"),
      {
        timeout: 200,
      },
    );

    vi.spyOn(absurd, "claimTasks").mockRejectedValue(new Error("Claim failed"));

    await vi.waitFor(() => expect(errors).toContain("Claim failed"), {
      timeout: 200,
    });

    await worker.close();

    expect(errors).toContain("Execute failed, PG error");
    expect(errors).toContain("Claim failed");
  });
});
