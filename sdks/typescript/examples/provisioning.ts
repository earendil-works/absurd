/**
 * Example workflow that provisions a customer account using the Absurd TypeScript SDK.
 *
 * node --experimental-transform-types examples/provisioning.ts
 *
 * Environment:
 *   ABSURD_WORKERS    - Number of worker loops to start (default: 3)
 *   ABSURD_RUNTIME    - Worker lifetime before shutdown in seconds (default: 20)
 *   SPAWN_CONCURRENCY - Max concurrent task spawns (default: 3)
 */
import crypto from "node:crypto";
import process from "node:process";

import pg from "pg";

import { Absurd, TaskContext, TimeoutError } from "../src/index.ts";

const DEFAULT_CANCELLATION = {
  maxDelay: 60,
  maxDuration: 120,
};
const ACTIVATION_TIMEOUT = 5;
const ON_TIME_DELAY_RANGE = { min: 1, max: 4 } as const;
const LATE_DELAY_RANGE = {
  min: ACTIVATION_TIMEOUT + 2,
  max: ACTIVATION_TIMEOUT + 3,
} as const;

type ProvisionCustomerParams = {
  customerId: string;
  email: string;
  plan: "basic" | "pro";
};

type ActivationSimulatorParams = {
  customerId: string;
  minDelay: number;
  maxDelay: number;
  timeout: number;
  shouldTimeout: boolean;
};

type ActivationAuditParams = {
  customerId: string;
  activatedAt: string;
};

type ActivationEventPayload = {
  customerId: string;
  activatedAt: string;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function log(scope: string, message: string, detail?: Record<string, unknown>) {
  const context = detail ? ` ${JSON.stringify(detail)}` : "";
  console.log(`[${new Date().toISOString()}] [${scope}] ${message}${context}`);
}

function registerTasks(absurd: Absurd) {
  absurd.registerTask<ProvisionCustomerParams, unknown>(
    { name: "provision-customer", defaultCancellation: DEFAULT_CANCELLATION },
    async (params, ctx: TaskContext) => {
      const customer = await ctx.step("create-customer-record", async () => {
        const createdAt = new Date().toISOString();
        log("provision", "customer record created", {
          customerId: params.customerId,
          email: params.email,
          plan: params.plan,
          createdAt,
        });
        return { id: params.customerId, createdAt };
      });

      await ctx.step("queue-activation-email", async () => {
        log("provision", "queueing activation email task", {
          customerId: customer.id,
        });
        await absurd.spawn(
          "send-activation-email",
          {
            customerId: customer.id,
            email: params.email,
          },
          { maxAttempts: 4 },
        );
        return true;
      });

      const shouldTimeout = Math.random() < 0.5;
      const delayRange = shouldTimeout ? LATE_DELAY_RANGE : ON_TIME_DELAY_RANGE;
      const activationTimeout = ACTIVATION_TIMEOUT;

      await ctx.step("start-activation-simulator", async () => {
        log("provision", "starting activation simulator", {
          customerId: customer.id,
          shouldTimeout,
          delayRange,
          timeout: activationTimeout,
        });

        await absurd.spawn<ActivationSimulatorParams>(
          "activation-simulator",
          {
            customerId: customer.id,
            minDelay: delayRange.min,
            maxDelay: delayRange.max,
            timeout: activationTimeout,
            shouldTimeout,
          },
          { maxAttempts: 1 },
        );

        return true;
      });

      const eventName = `customer:${customer.id}:activated`;
      log("provision", "waiting for activation event", {
        customerId: customer.id,
        eventName,
        timeout: activationTimeout,
      });

      let activationPayload: ActivationEventPayload;
      try {
        const payload = await ctx.awaitEvent(eventName, {
          timeout: activationTimeout,
        });
        activationPayload = payload as ActivationEventPayload;
      } catch (err) {
        if (err instanceof TimeoutError) {
          const timedOutAt = new Date().toISOString();
          log("provision", "activation wait timed out", {
            customerId: customer.id,
            timedOutAt,
            timeout: activationTimeout,
            expectedTimeout: shouldTimeout,
          });

          log("provision", "customer provisioning stalled", {
            customerId: customer.id,
            status: "activation-timeout",
            timedOutAt,
            timeout: activationTimeout,
          });

          return {
            customerId: customer.id,
            status: "activation-timeout",
            timedOutAt,
          };
        }
        throw err;
      }

      if (
        !activationPayload ||
        typeof activationPayload !== "object" ||
        typeof activationPayload.activatedAt !== "string"
      ) {
        throw new Error("Activation payload missing activatedAt");
      }

      const activatedAt = await ctx.step("finalize-activation", async () => {
        const activationTime = activationPayload.activatedAt;
        log("provision", "activation confirmed", {
          customerId: customer.id,
          activatedAt: activationTime,
        });
        return activationTime;
      });

      await ctx.step("audit-log-task", async () => {
        log("provision", "queueing audit trail task", {
          customerId: customer.id,
        });
        await absurd.spawn<ActivationAuditParams>(
          "post-activation-audit",
          {
            customerId: customer.id,
            activatedAt,
          },
          { maxAttempts: 1 },
        );
        return true;
      });

      log("provision", "customer provisioning complete", {
        customerId: customer.id,
        status: "activated",
      });

      return {
        customerId: customer.id,
        status: "activated",
        activatedAt,
      };
    },
  );

  const simulatedFailureTracker = new Map<string, number>();

  absurd.registerTask(
    {
      name: "send-activation-email",
      defaultCancellation: DEFAULT_CANCELLATION,
    },
    async (
      params: { customerId: string; email: string },
      ctx: TaskContext,
    ): Promise<{ messageId: string }> => {
      const attempt = (simulatedFailureTracker.get(params.customerId) ?? 0) + 1;
      simulatedFailureTracker.set(params.customerId, attempt);

      log("send-email", "attempting to send activation email", {
        customerId: params.customerId,
        email: params.email,
        attempt,
      });

      const failChance = await ctx.step("failure-roll", async () => {
        return Math.random();
      });

      if (failChance < 0.45 && attempt <= 2) {
        log("send-email", "simulated transient failure", {
          customerId: params.customerId,
          attempt,
          failChance,
        });
        throw new Error("Simulated email provider outage");
      }

      const messageId = await ctx.step("message-id", async () => {
        const id = `msg_${crypto.randomBytes(3).toString("hex")}`;
        log("send-email", "rendered activation email", {
          customerId: params.customerId,
          messageId: id,
        });
        return id;
      });

      log("send-email", "activation email delivered", {
        customerId: params.customerId,
        messageId,
      });

      return { messageId };
    },
  );

  absurd.registerTask<ActivationSimulatorParams>(
    { name: "activation-simulator", defaultCancellation: DEFAULT_CANCELLATION },
    async (params, ctx: TaskContext) => {
      const delay = await ctx.step("activation-delay", async () => {
        const range = Math.max(params.maxDelay - params.minDelay, 0);
        const delay =
          params.minDelay +
          (range > 0 ? Math.floor(Math.random() * (range + 1)) : 0);
        log("activation", "user will eventually activate", {
          customerId: params.customerId,
          delay,
          shouldTimeout: params.shouldTimeout,
          timeout: params.timeout,
        });
        return delay;
      });

      const wakeAtIso = await ctx.step("wake-at", async () => {
        const wake = new Date(Date.now() + delay * 1000).toISOString();
        log("activation", "scheduled wake for activation", {
          customerId: params.customerId,
          wakeAt: wake,
        });
        return wake;
      });

      const wakeAt = new Date(wakeAtIso);
      const remaining = (wakeAt.getTime() - Date.now()) / 1000;

      if (remaining > 0) {
        log("activation", "waiting for simulated user activation", {
          customerId: params.customerId,
          remaining,
        });
        await ctx.sleepFor("wait-for-activation", remaining);
      }

      await ctx.step("emit-activation-event", async () => {
        log("activation", "emitting activation event", {
          customerId: params.customerId,
          shouldTimeout: params.shouldTimeout,
        });
        await ctx.emitEvent(`customer:${params.customerId}:activated`, {
          customerId: params.customerId,
          activatedAt: new Date().toISOString(),
        });
        return true;
      });

      log("activation", "activation event dispatched", {
        customerId: params.customerId,
      });
    },
  );

  absurd.registerTask<ActivationAuditParams>(
    {
      name: "post-activation-audit",
      defaultCancellation: DEFAULT_CANCELLATION,
    },
    async (params, ctx: TaskContext) => {
      const auditEntry = await ctx.step("record-audit", async () => {
        const recordedAt = new Date().toISOString();
        log("audit", "recorded activation trail", {
          customerId: params.customerId,
          activatedAt: params.activatedAt,
          recordedAt,
        });
        return { recordedAt };
      });

      log("audit", "audit complete", {
        customerId: params.customerId,
        recordedAt: auditEntry.recordedAt,
      });

      return auditEntry;
    },
  );
}

async function main() {
  const workerCount = Number(process.env.ABSURD_WORKERS ?? "6");
  const runtime = Number(process.env.ABSURD_RUNTIME ?? "15");
  const spawnConcurrency = Number(process.env.SPAWN_CONCURRENCY ?? "3");

  log("main", "connecting to absurd queue", {
    workers: workerCount,
    spawnConcurrency,
  });

  const absurd = new Absurd();

  log("main", "creating queue if not exists");
  await absurd.createQueue();

  registerTasks(absurd);

  const worker = await absurd.startWorker({
    workerId: "demo-worker",
    concurrency: workerCount,
    onError: (error) => {
      log("worker", "worker loop error", {
        workerId: "demo-worker",
        error: error.message,
      });
    },
  });

  // Generate all task parameters upfront
  const taskParams: ProvisionCustomerParams[] = Array.from(
    { length: 12 },
    () => ({
      customerId: crypto.randomUUID(),
      email: `${crypto.randomUUID()}@example.com`,
      plan: Math.random() > 0.5 ? "pro" : "basic",
    }),
  );

  // Spawn tasks with controlled concurrency using SDK pattern
  const executing = new Set<Promise<void>>();
  for (const params of taskParams) {
    const promise = (async () => {
      const { taskID, runID } = await absurd.spawn(
        "provision-customer",
        params,
        {
          maxAttempts: 3,
        },
      );
      log("main", "spawned provisioning workflow", {
        taskId: taskID,
        runId: runID,
        customerId: params.customerId,
      });
    })();

    const trackedPromise = promise.finally(() =>
      executing.delete(trackedPromise),
    );
    executing.add(trackedPromise);

    // Wait for one to finish if we've reached the concurrency limit
    if (executing.size >= spawnConcurrency) {
      await Promise.race(executing);
    }
  }

  // Wait for all remaining spawns to complete
  await Promise.all(executing);

  log("main", "worker running", { runtime, concurrency: workerCount });
  await sleep(runtime * 1000);

  log("main", "shutting down worker");
  await worker.close();

  await absurd.close();
  log("main", "example finished");
}

main().catch((err) => {
  log("main", "example failed", { error: err.message });
  console.log(err);
  process.exitCode = 1;
});
