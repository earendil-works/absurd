import { describe, test, expect, assert } from "./testlib.ts";
import { Absurd } from "../src/index.ts";

describe("Queue name validation", () => {
  test("rejects empty default queue in constructor", () => {
    const fakeCon = { query: async () => ({ rows: [] }) } as any;

    assert.throws(
      () => new Absurd({ db: fakeCon, queueName: "   " }),
      /Queue name must be provided/,
    );
  });

  test("accepts permissive queue override in createQueue", async () => {
    let called = 0;
    const fakeCon = {
      query: async () => {
        called += 1;
        return { rows: [] };
      },
    } as any;
    const absurd = new Absurd({ db: fakeCon, queueName: "default" });

    await absurd.createQueue("Queue Name-1");
    expect(called).toBe(1);
  });

  test("rejects overlong queue names", async () => {
    const fakeCon = { query: async () => ({ rows: [] }) } as any;
    const absurd = new Absurd({ db: fakeCon, queueName: "default" });
    const tooLong = "q".repeat(58);

    await expect(absurd.dropQueue(tooLong)).rejects.toThrowError(
      'Queue name "' + tooLong + '" is too long (max 57 bytes).',
    );
  });

  test("rejects empty queue for unregistered spawn", async () => {
    let called = 0;
    const fakeCon = {
      query: async () => {
        called += 1;
        return { rows: [] };
      },
    } as any;
    const absurd = new Absurd({ db: fakeCon, queueName: "default" });

    await expect(
      absurd.spawn("task", { value: 1 }, { queue: "\t" }),
    ).rejects.toThrowError("Queue name must be provided");
    expect(called).toBe(0);
  });
});
