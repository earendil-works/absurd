/**
 * Example workflow that provisions a customer account using the Absurd TypeScript SDK.
 *
 * node --experimental-transform-types examples/provisioning.ts
 *
 * Environment:
 *   ABSURD_DB_URL      - PostgreSQL connection string (default: postgresql://localhost/absurd)
 *   ABSURD_WORKERS     - Number of worker loops to start (default: 3)
 *   ABSURD_RUNTIME_MS  - Worker lifetime before shutdown (default: 20000)
 */
import crypto from "node:crypto";
import process from "node:process";

import pg from "pg";

import { Absurd, TaskContext, TimeoutError } from "../src/index.ts";

const DEFAULT_DB_URL = "postgresql://localhost/absurd";
const DEFAULT_QUEUE = "provisioning_demo";
const DEFAULT_CANCELLATION = {
  maxDelayMs: 60000,
  maxDurationMs: 120000,
};
const ACTIVATION_TIMEOUT_MS = 5000;
const ON_TIME_DELAY_RANGE = { min: 400, max: 1800 } as const;
const LATE_DELAY_RANGE = {
  min: ACTIVATION_TIMEOUT_MS + 1200,
  max: ACTIVATION_TIMEOUT_MS + 1800,
} as const;

type ProvisionCustomerParams = {
  customerId: string;
  email: string;
  plan: "basic" | "pro";
};

type ActivationSimulatorParams = {
  customerId: string;
  minDelayMs: number;
  maxDelayMs: number;
  timeoutMs: number;
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
      const activationTimeoutMs = ACTIVATION_TIMEOUT_MS;

      await ctx.step("start-activation-simulator", async () => {
        log("provision", "starting activation simulator", {
          customerId: customer.id,
          shouldTimeout,
          delayRange,
          timeoutMs: activationTimeoutMs,
        });

        await absurd.spawn<ActivationSimulatorParams>(
          "activation-simulator",
          {
            customerId: customer.id,
            minDelayMs: delayRange.min,
            maxDelayMs: delayRange.max,
            timeoutMs: activationTimeoutMs,
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
        timeoutMs: activationTimeoutMs,
      });

      let activationPayload: ActivationEventPayload;
      try {
        const payload = await ctx.awaitEvent(eventName, {
          timeout: activationTimeoutMs,
        });
        activationPayload = payload as ActivationEventPayload;
      } catch (err) {
        if (err instanceof TimeoutError) {
          const timedOutAt = new Date().toISOString();
          log("provision", "activation wait timed out", {
            customerId: customer.id,
            timedOutAt,
            timeoutMs: activationTimeoutMs,
            expectedTimeout: shouldTimeout,
          });

          log("provision", "customer provisioning stalled", {
            customerId: customer.id,
            status: "activation-timeout",
            timedOutAt,
            timeoutMs: activationTimeoutMs,
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
      const delayMs = await ctx.step("activation-delay", async () => {
        const range = Math.max(params.maxDelayMs - params.minDelayMs, 0);
        const delay =
          params.minDelayMs +
          (range > 0 ? Math.floor(Math.random() * (range + 1)) : 0);
        log("activation", "user will eventually activate", {
          customerId: params.customerId,
          delayMs: delay,
          shouldTimeout: params.shouldTimeout,
          timeoutMs: params.timeoutMs,
        });
        return delay;
      });

      const wakeAtIso = await ctx.step("wake-at", async () => {
        const wake = new Date(Date.now() + delayMs).toISOString();
        log("activation", "scheduled wake for activation", {
          customerId: params.customerId,
          wakeAt: wake,
        });
        return wake;
      });

      const wakeAt = new Date(wakeAtIso);
      const remainingMs = wakeAt.getTime() - Date.now();

      if (remainingMs > 25) {
        log("activation", "waiting for simulated user activation", {
          customerId: params.customerId,
          remainingMs,
        });
        await ctx.sleepFor("wait-for-activation", remainingMs);
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
  const connectionString = process.env.ABSURD_DB_URL ?? DEFAULT_DB_URL;
  const workerCount = Number(process.env.ABSURD_WORKERS ?? "6");
  const runtimeMs = Number(process.env.ABSURD_RUNTIME_MS ?? "15000");

  log("main", "connecting to absurd queue", {
    connectionString,
    queue: DEFAULT_QUEUE,
    workers: workerCount,
  });

  const pool = new pg.Pool({ connectionString });
  const absurd = new Absurd(pool, DEFAULT_QUEUE);

  log("main", "creating queue if not exists", { queue: DEFAULT_QUEUE });
  await absurd.createQueue();

  registerTasks(absurd);

  const workerShutdowns = await Promise.all(
    Array.from({ length: workerCount }, (_, idx) =>
      absurd.startWorker({
        workerId: `demo-worker-${idx + 1}`,
        batchSize: 1,
        pollInterval: 250,
        onError: (error) => {
          log("worker", "worker loop error", {
            workerId: `demo-worker-${idx + 1}`,
            error: error.message,
          });
        },
      }),
    ),
  );

  const pendingTasks = [];
  for (let i = 0; i < 12; i++) {
    const params: ProvisionCustomerParams = {
      customerId: crypto.randomUUID(),
      email: `${crypto.randomUUID()}@example.com`,
      plan: Math.random() > 0.5 ? "pro" : "basic",
    };

    pendingTasks.push(
      (async () => {
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
      })(),
    );
  }

  await Promise.all(pendingTasks);

  log("main", "workers running", { runtimeMs });
  await sleep(runtimeMs);

  log("main", "shutting down workers");
  for (const shutdown of workerShutdowns) {
    await shutdown();
  }

  await absurd.close();
  await pool.end();
  log("main", "example finished");
}

main().catch((err) => {
  log("main", "example failed", { error: err.message });
  console.log(err);
  process.exitCode = 1;
});
