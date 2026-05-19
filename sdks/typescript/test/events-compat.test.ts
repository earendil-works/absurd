import { describe, test, expect } from "./testlib.ts";
import { TaskContext, type ClaimedTask } from "../src/index.ts";

describe("awaitEvent legacy compatibility", () => {
  test("treats legacy row shape timeout as TimeoutError", async () => {
    const eventName = "legacy-timeout-event";
    const task: ClaimedTask = {
      run_id: "run-1",
      task_id: "task-1",
      task_name: "test",
      attempt: 1,
      params: null,
      retry_strategy: null,
      max_attempts: null,
      headers: null,
      wake_event: eventName,
      event_payload: null,
    };

    const con = {
      query: async (sql: string) => {
        if (sql.includes("get_task_checkpoint_states")) {
          return { rows: [] };
        }
        if (sql.includes("get_task_checkpoint_state")) {
          return { rows: [] };
        }
        if (sql.includes("await_event")) {
          return {
            rows: [
              {
                should_suspend: false,
                payload: null,
              },
            ],
          };
        }
        throw new Error(`unexpected query: ${sql}`);
      },
    } as any;

    const ctx = await TaskContext.create({
      log: console,
      taskID: task.task_id,
      con,
      queueName: "default",
      task,
      claimTimeout: 60,
      onLeaseExtended: () => {},
    });

    await expect(ctx.awaitEvent(eventName)).rejects.toThrow(
      `Timed out waiting for event "${eventName}"`,
    );
  });
});
