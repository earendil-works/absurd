/**
 * Example workflow that provisions a customer account using the Absurd TypeScript SDK.
 *
 * To run from repository root:
 *   1. Prepare the database:
 *        node sdks/typescript/examples/load_absurd_db.ts
 *   2. Execute the workflow demo:
 *        node sdks/typescript/examples/motivating_provisioning.ts
 *
 * Environment:
 *   ABSURD_DB_URL      - PostgreSQL connection string (default: postgresql://localhost/absurd)
 *   ABSURD_WORKERS     - Number of worker loops to start (default: 3)
 *   ABSURD_RUNTIME_MS  - Worker lifetime before shutdown (default: 20000)
 */
import crypto from "node:crypto";
import process from "node:process";

import pg from "pg";

import { Absurd, TaskContext } from "../src/index.ts";

const DEFAULT_DB_URL = "postgresql://localhost/absurd";
const DEFAULT_QUEUE = "provisioning_demo";

type ProvisionCustomerParams = {
  customerId: string;
  email: string;
  plan: "basic" | "pro";
};

type ActivationSimulatorParams = {
  customerId: string;
  minDelayMs: number;
  maxDelayMs: number;
};

type ActivationAuditParams = {
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
    "provision-customer",
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

      await ctx.step("start-activation-simulator", async () => {
        log("provision", "starting activation simulator", {
          customerId: customer.id,
        });
        await absurd.spawn<ActivationSimulatorParams>(
          "activation-simulator",
          {
            customerId: customer.id,
            minDelayMs: 1500,
            maxDelayMs: 4500,
          },
          { maxAttempts: 1 },
        );
        return true;
      });

      log("provision", "waiting for activation event", {
        customerId: customer.id,
      });
      await ctx.awaitEvent(
        "await-activation",
        `customer:${customer.id}:activated`,
      );

      const activation = await ctx.step("finalize-activation", async () => {
        const activatedAt = new Date().toISOString();
        log("provision", "activation confirmed", {
          customerId: customer.id,
          activatedAt,
        });
        return { activatedAt };
      });

      await ctx.step("audit-log-task", async () => {
        log("provision", "queueing audit trail task", {
          customerId: customer.id,
        });
        await absurd.spawn<ActivationAuditParams>(
          "post-activation-audit",
          {
            customerId: customer.id,
            activatedAt: activation.activatedAt,
          },
          { maxAttempts: 1 },
        );
        return true;
      });

      log("provision", "customer provisioning complete", {
        customerId: customer.id,
      });

      return {
        customerId: customer.id,
        activatedAt: activation.activatedAt,
      };
    },
  );

  const simulatedFailureTracker = new Map<string, number>();

  absurd.registerTask(
    "send-activation-email",
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
    "activation-simulator",
    async (params, ctx: TaskContext) => {
      const delayMs = await ctx.step("activation-delay", async () => {
        const range = params.maxDelayMs - params.minDelayMs;
        const delay = params.minDelayMs + Math.floor(Math.random() * range);
        log("activation", "user will eventually activate", {
          customerId: params.customerId,
          delayMs: delay,
        });
        return delay;
      });

      const wakeAtIso = await ctx.step("wake-at", async () => {
        const wake = new Date(Date.now() + delayMs).toISOString();
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
    "post-activation-audit",
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
  const workerCount = Number(process.env.ABSURD_WORKERS ?? "3");
  const runtimeMs = Number(process.env.ABSURD_RUNTIME_MS ?? "20000");

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
  for (let i = 0; i < 10; i++) {
    const params: ProvisionCustomerParams = {
      customerId: `cust_${Date.now().toString(36)}`,
      email: `demo+${Date.now().toString(36)}@example.com`,
      plan: Math.random() > 0.5 ? "pro" : "basic",
    };
  
    pendingTasks.push((async () => {
      const { task_id, run_id } = await absurd.spawn("provision-customer", params, {
        maxAttempts: 3,
      });
      log("main", "spawned provisioning workflow", {
        taskId: task_id,
        runId: run_id,
        customerId: params.customerId,
      });
    })());
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
