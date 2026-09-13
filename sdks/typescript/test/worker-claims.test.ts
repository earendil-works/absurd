import assert from "node:assert/strict";
import { test } from "node:test";
import { Absurd, LostClaimError, type ClaimedTask } from "../src/index.ts";

interface QueryCall {
  text: string;
  values: unknown[];
}

const silentLog = {
  log() {},
  info() {},
  warn() {},
  error() {},
};

function createClaim(overrides: Partial<ClaimedTask> = {}): ClaimedTask {
  return {
    run_id: "00000000-0000-0000-0000-000000000001",
    task_id: "00000000-0000-0000-0000-000000000002",
    task_name: "claimed-task",
    attempt: 1,
    params: null,
    retry_strategy: null,
    max_attempts: 1,
    headers: null,
    wake_event: null,
    event_payload: null,
    worker_id: "claim-worker",
    ...overrides,
  };
}

function createConnection(
  respond?: (text: string, values: unknown[]) => unknown,
): { con: any; calls: QueryCall[] } {
  const calls: QueryCall[] = [];
  const con = {
    async query(text: string, values: unknown[] = []) {
      calls.push({ text, values });
      const response = respond?.(text, values);
      if (response !== undefined) {
        return response;
      }
      if (text.includes("absurd.await_event")) {
        return {
          rows: [{ should_suspend: false, payload: { emitted: true } }],
        };
      }
      return { rows: [] };
    },
  };
  return { con, calls };
}

function findCall(calls: QueryCall[], functionName: string): QueryCall {
  const call = calls.find(({ text }) =>
    text.includes(`absurd.${functionName}(`),
  );
  assert(call, `Expected a call to absurd.${functionName}`);
  return call;
}

test("claimTasks retains the exact worker identity used to claim", async () => {
  const claim = createClaim({ worker_id: undefined });
  const { con, calls } = createConnection((text) => {
    if (text.includes("absurd.claim_task")) {
      return { rows: [claim] };
    }
  });
  const absurd = new Absurd({ db: con, log: silentLog });

  const [claimed] = await absurd.claimTasks({ workerId: "worker-42" });

  assert.equal(claimed.worker_id, "worker-42");
  assert.equal(findCall(calls, "claim_task").values[1], "worker-42");
});

test("claimTasks generates a unique worker identity by default", async () => {
  const { con, calls } = createConnection();
  const absurd = new Absurd({ db: con, log: silentLog });

  await absurd.claimTasks();
  await absurd.claimTasks();

  const workerIDs = calls
    .filter(({ text }) => text.includes("absurd.claim_task"))
    .map(({ values }) => values[1]);
  assert.equal(workerIDs.length, 2);
  assert.notEqual(workerIDs[0], workerIDs[1]);
});

test("task context uses guarded worker-owned mutations", async () => {
  const { con, calls } = createConnection();
  const absurd = new Absurd({ db: con, log: silentLog });
  absurd.registerTask({ name: "claimed-task" }, async (_params, ctx) => {
    await ctx.step("checkpoint", async () => ({ saved: true }));
    await ctx.heartbeat(45);
    await ctx.awaitEvent("ready");
    return { done: true };
  });

  await absurd.executeTask(createClaim(), 60);

  const mutations = [
    ["set_task_checkpoint_state", 7],
    ["extend_claim", 4],
    ["await_event", 7],
    ["complete_run", 4],
  ] as const;
  for (const [functionName, arity] of mutations) {
    const call = findCall(calls, functionName);
    assert.equal(call.values.length, arity);
    assert.equal(call.values.at(-1), "claim-worker");
  }
});

test("sleep and unknown-task deferral use guarded schedule mutations", async () => {
  const sleeping = createConnection();
  const sleepingAbsurd = new Absurd({ db: sleeping.con, log: silentLog });
  sleepingAbsurd.registerTask(
    { name: "claimed-task" },
    async (_params, ctx) => {
      await ctx.sleepFor("pause", 30);
    },
  );

  await sleepingAbsurd.executeTask(createClaim(), 60);

  const sleepSchedule = findCall(sleeping.calls, "schedule_run");
  assert.equal(sleepSchedule.values.length, 4);
  assert.equal(sleepSchedule.values.at(-1), "claim-worker");

  const unknown = createConnection();
  const unknownAbsurd = new Absurd({ db: unknown.con, log: silentLog });
  await unknownAbsurd.executeTask(
    createClaim({ task_name: "unknown-task" }),
    60,
  );

  const deferSchedule = findCall(unknown.calls, "schedule_run");
  assert.equal(deferSchedule.values.length, 4);
  assert.equal(deferSchedule.values.at(-1), "claim-worker");
});

test("task failures use guarded fail mutations", async () => {
  const { con, calls } = createConnection();
  const absurd = new Absurd({ db: con, log: silentLog });
  absurd.registerTask({ name: "claimed-task" }, async () => {
    throw new Error("boom");
  });

  await absurd.executeTask(createClaim(), 60);

  const fail = findCall(calls, "fail_run");
  assert.equal(fail.values.length, 5);
  assert.equal(fail.values.at(-1), "claim-worker");
});

test("AB004 becomes a terminal LostClaimError", async () => {
  const { con, calls } = createConnection((text) => {
    if (text.includes("absurd.extend_claim")) {
      throw Object.assign(new Error("The worker claim has been lost"), {
        code: "AB004",
      });
    }
  });
  const absurd = new Absurd({ db: con, log: silentLog });
  let caught: unknown;
  let continued = false;
  absurd.registerTask({ name: "claimed-task" }, async (_params, ctx) => {
    try {
      await ctx.heartbeat();
      continued = true;
    } catch (error) {
      caught = error;
      throw error;
    }
  });

  await absurd.executeTask(createClaim(), 60);

  assert(caught instanceof LostClaimError);
  assert.equal(caught.name, "LostClaimError");
  assert.equal(caught.message, "Worker claim has been lost");
  assert.equal(continued, false);
  assert.equal(
    calls.some(({ text }) => text.includes("absurd.fail_run")),
    false,
  );
  assert.equal(
    calls.some(({ text }) => text.includes("absurd.complete_run")),
    false,
  );
});

test("executeTask rejects missing worker claim identity before user code", async () => {
  const { con } = createConnection();
  const absurd = new Absurd({ db: con, log: silentLog });
  let executed = false;
  absurd.registerTask({ name: "claimed-task" }, async () => {
    executed = true;
  });

  await assert.rejects(
    absurd.executeTask(createClaim({ worker_id: undefined }), 60),
    LostClaimError,
  );
  assert.equal(executed, false);
});
