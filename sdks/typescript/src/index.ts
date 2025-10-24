import * as pg from "pg";

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

export interface RetryStrategy {
  kind: "fixed" | "exponential" | "none";
  baseSeconds?: number;
  factor?: number;
  maxSeconds?: number;
}

export interface SpawnOptions {
  maxAttempts?: number;
  retryStrategy?: RetryStrategy;
  headers?: JsonObject;
  queue?: string;
}

export interface ClaimedMessage {
  run_id: string;
  task_id: string;
  task_name: string;
  attempt: number;
  params: JsonValue;
  retry_strategy: JsonValue;
  max_attempts: number | null;
  headers: JsonObject | null;
  lease_expires_at: Date;
  wake_event: string | null;
  event_payload: JsonValue | null;
}

export interface WorkerOptions {
  workerId?: string;
  claimTimeout?: number;
  batchSize?: number;
  pollInterval?: number;
  onError?: (error: Error) => void;
}

interface CheckpointRow {
  checkpoint_name: string;
  state: JsonValue;
  status: string;
  owner_run_id: string;
  updated_at: Date;
}

interface SpawnResult {
  taskID: string;
  runID: string;
  attempt: number;
}

export type TaskHandler<P = any, R = any> = (
  params: P,
  ctx: TaskContext,
) => Promise<R>;

export class SuspendTask extends Error {
  constructor() {
    super("Task suspended");
    this.name = "SuspendTask";
  }
}

export interface TaskRegistrationOptions {
  name: string;
  queue?: string;
  defaultMaxAttempts?: number;
}

interface RegisteredTask {
  name: string;
  queue: string;
  defaultMaxAttempts?: number;
  handler: TaskHandler<any, any>;
}

export class TaskContext {
  private stepNameCounter: Map<string, number> = new Map();

  private constructor(
    private readonly pool: pg.Pool,
    private readonly queueName: string,
    private readonly message: ClaimedMessage,
    private readonly checkpointCache: Map<string, JsonValue>,
  ) {}

  static async create(args: {
    pool: pg.Pool;
    queueName: string;
    message: ClaimedMessage;
  }): Promise<TaskContext> {
    const { pool, queueName, message } = args;
    const result = await pool.query<CheckpointRow>(
      `SELECT checkpoint_name, state, status, owner_run_id, updated_at
       FROM absurd.get_task_checkpoint_states($1, $2, $3)`,
      [queueName, message.task_id, message.run_id],
    );
    const cache = new Map<string, JsonValue>();
    for (const row of result.rows) {
      cache.set(row.checkpoint_name, row.state);
    }
    return new TaskContext(pool, queueName, message, cache);
  }

  async step<T>(name: string, fn: () => Promise<T>): Promise<T> {
    const checkpointName = this.getCheckpointName(name);
    const state = await this.lookupCheckpoint(checkpointName);
    if (state !== undefined) {
      return state as T;
    }

    const rv = await fn();
    await this.persistCheckpoint(checkpointName, rv as JsonValue);
    return rv;
  }

  async sleepFor(stepName: string, durationMs: number): Promise<void> {
    return await this.sleepUntil(stepName, new Date(Date.now() + durationMs));
  }

  async sleepUntil(stepName: string, wakeAt: Date): Promise<void> {
    const checkpointName = this.getCheckpointName(stepName);
    const state = await this.lookupCheckpoint(checkpointName);
    let actualWakeAt = (typeof state === "string") ? new Date(state) : wakeAt;
    if (!state) {
      await this.persistCheckpoint(checkpointName, wakeAt.toISOString());
    }

    if (Date.now() < actualWakeAt.getTime()) {
      await this.scheduleRun(actualWakeAt);
      throw new SuspendTask();
    }
  }

  private getCheckpointName(name: string): string {
    const count = (this.stepNameCounter.get(name) ?? 0) + 1;
    this.stepNameCounter.set(name, count);
    const actualStepName = count === 1 ? name : `${name}#${count}`;
    return actualStepName;
  }

  private async lookupCheckpoint(checkpointName: string): Promise<JsonValue | undefined> {
    const cached = this.checkpointCache.get(checkpointName);
    if (cached !== undefined) {
      return cached;
    }

    const result = await this.pool.query<CheckpointRow>(
      `SELECT checkpoint_name, state, status, owner_run_id, updated_at
       FROM absurd.get_task_checkpoint_state($1, $2, $3)`,
      [this.queueName, this.message.task_id, checkpointName],
    );
    if (result.rows.length > 0) {
      const state = result.rows[0].state;
      this.checkpointCache.set(checkpointName, state);
      return state;
    }
    return undefined;
  }

  private async persistCheckpoint(checkpointName: string, value: JsonValue) : Promise<void> {
    await this.pool.query(
      `SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5)`,
      [
        this.queueName,
        this.message.task_id,
        checkpointName,
        JSON.stringify(value),
        this.message.run_id,
      ],
    );
    this.checkpointCache.set(checkpointName, value);
  }

  private async scheduleRun(wakeAt: Date): Promise<void> {
    await this.pool.query(`SELECT absurd.schedule_run($1, $2, $3, $4)`, [
      this.queueName,
      this.message.run_id,
      wakeAt,
      true,
    ]);
  }

  async awaitEvent(
    stepName: string,
    eventName: string,
  ): Promise<JsonValue | null> {
    if (!eventName) {
      throw new Error("eventName must be a non-empty string");
    }
    const checkpointName = this.getCheckpointName(stepName);
    const result = await this.pool.query<{
      should_suspend: boolean;
      payload: JsonValue | null;
    }>(
      `SELECT should_suspend, payload
       FROM absurd.await_event($1, $2, $3, $4, $5)`,
      [
        this.queueName,
        this.message.task_id,
        this.message.run_id,
        checkpointName,
        eventName,
      ],
    );

    if (result.rows.length === 0) {
      throw new Error("Failed to await event");
    }

    const { should_suspend, payload } = result.rows[0];

    if (!should_suspend) {
      this.persistCheckpoint(checkpointName, payload);
      return payload;
    }

    throw new SuspendTask();
  }

  async emitEvent(eventName: string, payload?: JsonValue): Promise<void> {
    if (!eventName) {
      throw new Error("eventName must be a non-empty string");
    }
    await this.pool.query(`SELECT absurd.emit_event($1, $2, $3)`, [
      this.queueName,
      eventName,
      JSON.stringify(payload ?? null),
    ]);
  }

  async complete(result?: any): Promise<void> {
    await this.pool.query(`SELECT absurd.complete_run($1, $2, $3, $4)`, [
      this.queueName,
      this.message.run_id,
      JSON.stringify(result ?? null),
      true,
    ]);
  }

  async fail(err: unknown): Promise<void> {
    console.error("[absurd] task execution failed:", err);
    await this.pool.query(`SELECT absurd.fail_run($1, $2, $3, $4)`, [
      this.queueName,
      this.message.run_id,
      JSON.stringify(serializeError(err)),
      null,
    ]);
  }
}

export class Absurd {
  private readonly pool: pg.Pool;
  private readonly ownedPool: boolean;
  private readonly queueName: string;
  private readonly registry = new Map<string, RegisteredTask>();
  private workerShutdown: (() => void) | null = null;

  constructor(
    poolOrUrl?: pg.Pool | string | null,
    queueName: string = "default",
  ) {
    if (!poolOrUrl) {
      poolOrUrl =
        process.env.ABSURD_DATABASE_URL || "postgresql://localhost/absurd";
    }
    if (typeof poolOrUrl === "string") {
      this.pool = new pg.Pool({ connectionString: poolOrUrl });
      this.ownedPool = true;
    } else {
      this.pool = poolOrUrl;
      this.ownedPool = false;
    }
    this.queueName = queueName;
  }

  registerTask<P = any, R = any>(
    options: TaskRegistrationOptions,
    handler: TaskHandler<P, R>,
  ): void {
    if (!options?.name) {
      throw new Error("Task registration requires a name");
    }
    if (
      options.defaultMaxAttempts !== undefined &&
      options.defaultMaxAttempts < 1
    ) {
      throw new Error("defaultMaxAttempts must be at least 1");
    }
    const queue = options.queue ?? this.queueName;
    if (!queue) {
      throw new Error(
        `Task "${options.name}" must specify a queue or use a client with a default queue`,
      );
    }
    this.registry.set(options.name, {
      name: options.name,
      queue,
      defaultMaxAttempts: options.defaultMaxAttempts,
      handler: handler as TaskHandler<any, any>,
    });
  }

  async createQueue(queueName?: string): Promise<void> {
    const queue = queueName ?? this.queueName;
    await this.pool.query(`SELECT absurd.create_queue($1)`, [queue]);
  }

  async dropQueue(queueName?: string): Promise<void> {
    const queue = queueName ?? this.queueName;
    await this.pool.query(`SELECT absurd.drop_queue($1)`, [queue]);
  }

  async listQueues(): Promise<Array<string>> {
    const result = await this.pool.query(`SELECT * FROM absurd.list_queues()`);
    const rv = [];
    console.log(result);
    for (const row of result.rows) {
      rv.push(row.queue_name);
    }
    return rv;
  }

  async spawn<P = any>(
    taskName: string,
    params: P,
    options: SpawnOptions = {},
  ): Promise<SpawnResult> {
    const registration = this.registry.get(taskName);
    let queue: string | undefined;
    if (registration) {
      queue = registration.queue;
      if (options.queue !== undefined && options.queue !== registration.queue) {
        throw new Error(
          `Task "${taskName}" is registered for queue "${registration.queue}" but spawn requested queue "${options.queue}".`,
        );
      }
    } else if (!options.queue) {
      throw new Error(
        `Task "${taskName}" is not registered. Provide options.queue when spawning unregistered tasks.`,
      );
    }
    const effectiveMaxAttempts =
      options.maxAttempts !== undefined
        ? options.maxAttempts
        : registration?.defaultMaxAttempts;
    const normalizedOptions = normalizeSpawnOptions({
      ...options,
      maxAttempts: effectiveMaxAttempts,
    });

    const result = await this.pool.query<{
      task_id: string;
      run_id: string;
      attempt: number;
    }>(
      `SELECT task_id, run_id, attempt
       FROM absurd.spawn_task($1, $2, $3, $4)`,
      [
        queue,
        taskName,
        JSON.stringify(params),
        JSON.stringify(normalizedOptions),
      ],
    );

    if (result.rows.length === 0) {
      throw new Error("Failed to spawn task");
    }

    const row = result.rows[0];
    return {
      taskID: row.task_id,
      runID: row.run_id,
      attempt: row.attempt,
    };
  }

  async workOnce(
    workerId: string = "worker",
    claimTimeout: number = 30,
    batchSize: number = 1,
  ): Promise<void> {
    const result = await this.pool.query<ClaimedMessage>(
      `SELECT run_id, task_id, attempt, task_name, params, retry_strategy, max_attempts,
              headers, lease_expires_at, wake_event, event_payload
       FROM absurd.claim_task($1, $2, $3, $4)`,
      [this.queueName, workerId, claimTimeout, batchSize],
    );

    for (const msg of result.rows) {
      await this.executeMessage(msg);
    }
  }

  async startWorker(options: WorkerOptions = {}): Promise<() => Promise<void>> {
    const {
      workerId = "worker",
      claimTimeout = 30,
      batchSize = 1,
      pollInterval = 1000,
      onError = (err) => console.error("Worker error:", err),
    } = options;

    let running = true;

    const shutdown = async () => {
      running = false;
    };

    this.workerShutdown = shutdown;

    // Start worker loop
    (async () => {
      while (running) {
        try {
          await this.workOnce(workerId, claimTimeout, batchSize);
        } catch (err) {
          onError(err as Error);
        }
        await sleep(pollInterval);
      }
    })();

    return shutdown;
  }

  async close(): Promise<void> {
    if (this.workerShutdown) {
      await this.workerShutdown();
    }

    if (this.ownedPool) {
      await this.pool.end();
    }
  }

  private async executeMessage(msg: ClaimedMessage): Promise<void> {
    const registration = this.registry.get(msg.task_name);
    const ctx = await TaskContext.create({
      pool: this.pool,
      queueName: registration?.queue ?? "unknown",
      message: msg,
    });

    try {
      if (!registration) {
        throw new Error("Unknown task");
      } else if (registration.queue !== this.queueName) {
        throw new Error("Misconfigured task (queue mismatch)");
      }
      const result = await registration.handler(msg.params, ctx);
      await ctx.complete(result);
    } catch (err) {
      if (err instanceof SuspendTask) {
        // Task suspended (sleep or await), don't complete or fail
        return;
      }
      await ctx.fail(err);
    }
  }
}

function serializeError(err: unknown): JsonValue {
  if (err instanceof Error) {
    return {
      name: err.name,
      message: err.message,
      stack: err.stack || null,
    };
  }
  return { message: String(err) };
}

function normalizeSpawnOptions(options: SpawnOptions): JsonObject {
  const normalized: JsonObject = {};
  if (options.headers !== undefined) {
    normalized.headers = options.headers;
  }
  if (options.maxAttempts !== undefined) {
    normalized.max_attempts = options.maxAttempts;
  }
  if (options.retryStrategy) {
    normalized.retry_strategy = serializeRetryStrategy(options.retryStrategy);
  }
  return normalized;
}

function serializeRetryStrategy(strategy: RetryStrategy): JsonObject {
  const serialized: JsonObject = {
    kind: strategy.kind,
  };
  if (strategy.baseSeconds !== undefined) {
    serialized.base_seconds = strategy.baseSeconds;
  }
  if (strategy.factor !== undefined) {
    serialized.factor = strategy.factor;
  }
  if (strategy.maxSeconds !== undefined) {
    serialized.max_seconds = strategy.maxSeconds;
  }
  return serialized;
}

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
