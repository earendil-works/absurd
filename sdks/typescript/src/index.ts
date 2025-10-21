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

export interface NormalizedClaimedMessage {
  run_id: string;
  task_id: string;
  task_name: string;
  attempt: number;
  params: JsonValue;
  retry_strategy: RetryStrategy | null;
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
  ephemeral: boolean;
  expires_at: Date | null;
  updated_at: Date;
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

export class TaskContext {
  private constructor(
    private readonly pool: pg.Pool,
    private readonly queueName: string,
    private readonly message: NormalizedClaimedMessage,
    private readonly checkpointCache: Map<string, JsonValue>,
  ) {}

  static async create(args: {
    pool: pg.Pool;
    queueName: string;
    message: NormalizedClaimedMessage;
  }): Promise<TaskContext> {
    const { pool, queueName, message } = args;

    // Prefetch all checkpoints for this task run
    const result = await pool.query<CheckpointRow>(
      `SELECT checkpoint_name, state, status, owner_run_id, ephemeral, expires_at, updated_at
       FROM absurd.get_task_checkpoint_states($1, $2)`,
      [message.task_id, message.run_id],
    );

    const cache = new Map<string, JsonValue>();
    for (const row of result.rows) {
      cache.set(row.checkpoint_name, row.state);
    }

    return new TaskContext(pool, queueName, message, cache);
  }

  async step<T>(
    name: string,
    fn: () => Promise<T>,
    options: { ephemeral?: boolean; ttlSeconds?: number } = {},
  ): Promise<T> {
    // Check in-memory cache first
    if (this.checkpointCache.has(name)) {
      return this.checkpointCache.get(name) as T;
    }

    // Check database for existing checkpoint
    const result = await this.pool.query<CheckpointRow>(
      `SELECT checkpoint_name, state, status, owner_run_id, ephemeral, expires_at, updated_at
       FROM absurd.get_task_checkpoint_state($1, $2)`,
      [this.message.task_id, name],
    );

    if (result.rows.length > 0) {
      const state = result.rows[0].state;
      this.checkpointCache.set(name, state);
      return state as T;
    }

    // Execute the function and save checkpoint
    const value = await fn();

    await this.pool.query(
      `SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)`,
      [
        this.message.task_id,
        name,
        JSON.stringify(value),
        this.message.run_id,
        options.ephemeral ?? false,
        options.ttlSeconds ?? null,
      ],
    );

    this.checkpointCache.set(name, value as JsonValue);
    return value;
  }

  async sleepFor(stepName: string, durationMs: number): Promise<never> {
    const wakeAt = new Date(Date.now() + durationMs);

    await this.pool.query(`SELECT absurd.schedule_run($1, $2, $3)`, [
      this.message.run_id,
      wakeAt,
      true,
    ]);

    await this.pool.query(
      `SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)`,
      [
        this.message.task_id,
        stepName,
        JSON.stringify(wakeAt.toISOString()),
        this.message.run_id,
        true,
        Math.floor(durationMs / 1000),
      ],
    );

    throw new SuspendTask();
  }

  async awaitEvent(
    stepName: string,
    eventName: string,
    payload?: JsonValue,
  ): Promise<JsonValue | null> {
    if (!eventName) {
      throw new Error("eventName must be a non-empty string");
    }
    if (this.checkpointCache.has(stepName)) {
      return this.checkpointCache.get(stepName) ?? null;
    }

    const result = await this.pool.query<{
      should_suspend: boolean;
      payload: JsonValue | null;
    }>(
      `SELECT should_suspend, payload
       FROM absurd.await_event($1, $2, $3, $4)`,
      [
        this.message.run_id,
        stepName,
        eventName,
        JSON.stringify(payload ?? null),
      ],
    );

    if (result.rows.length === 0) {
      throw new Error("Failed to await event");
    }

    const { should_suspend, payload: eventPayload } = result.rows[0];

    if (!should_suspend) {
      await this.pool.query(
        `SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)`,
        [
          this.message.task_id,
          stepName,
          JSON.stringify(eventPayload ?? null),
          this.message.run_id,
          true,
          null,
        ],
      );

      this.checkpointCache.set(stepName, (eventPayload ?? null) as JsonValue);
      return eventPayload ?? null;
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
    const retryAt = computeRetryAt(
      this.message.retry_strategy,
      this.message.attempt,
    );

    await this.pool.query(`SELECT absurd.fail_run($1, $2, $3, $4)`, [
      this.queueName,
      this.message.run_id,
      JSON.stringify(serializeError(err)),
      retryAt,
    ]);
  }
}

export class Absurd {
  private readonly pool: pg.Pool;
  private readonly ownedPool: boolean;
  private readonly queueName: string;
  private readonly registry = new Map<string, TaskHandler>();
  private workerShutdown: (() => void) | null = null;

  constructor(poolOrUrl?: pg.Pool | string | null, queueName: string = "default") {
    if (!poolOrUrl) {
      poolOrUrl = process.env.ABSURD_DATABASE_URL || "postgresql://localhost/absurd";
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
    name: string,
    handler: TaskHandler<P, R>,
  ): void {
    this.registry.set(name, handler as TaskHandler);
  }

  async createQueue(queueName?: string): Promise<void> {
    const queue = queueName ?? this.queueName;
    await this.pool.query(`SELECT absurd.create($1)`, [queue]);
  }

  async spawn<P = any>(
    taskName: string,
    params: P,
    options: SpawnOptions = {},
  ): Promise<{ task_id: string; run_id: string; attempt: number }> {
    const normalizedOptions = normalizeSpawnOptions(options);
    const result = await this.pool.query<{
      task_id: string;
      run_id: string;
      attempt: number;
    }>(
      `SELECT task_id, run_id, attempt
       FROM absurd.spawn_task($1, $2, $3, $4)`,
      [
        this.queueName,
        taskName,
        JSON.stringify(params),
        JSON.stringify(normalizedOptions),
      ],
    );

    if (result.rows.length === 0) {
      throw new Error("Failed to spawn task");
    }

    return result.rows[0];
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
      const deserialized = deserializeClaimedMessage(msg);
      await this.executeMessage(deserialized);
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

          // If no tasks were claimed, wait before polling again
          await new Promise((resolve) => setTimeout(resolve, pollInterval));
        } catch (err) {
          onError(err as Error);
          // Wait a bit before retrying after an error
          await new Promise((resolve) => setTimeout(resolve, pollInterval * 2));
        }
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

  private async executeMessage(msg: NormalizedClaimedMessage): Promise<void> {
    const handler = this.registry.get(msg.task_name);

    if (!handler) {
      await this.pool.query(`SELECT absurd.fail_run($1, $2, $3, $4)`, [
        this.queueName,
        msg.run_id,
        JSON.stringify({ error: "unknown-task" }),
        null,
      ]);
      return;
    }

    const ctx = await TaskContext.create({
      pool: this.pool,
      queueName: this.queueName,
      message: msg,
    });

    try {
      const result = await handler(msg.params, ctx);
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

function computeRetryAt(
  strategy: RetryStrategy | null,
  attempt: number,
): Date | null {
  if (!strategy || strategy.kind === "none") {
    return null;
  }

  const baseSeconds = strategy.baseSeconds ?? 5;
  let delaySeconds: number;

  if (strategy.kind === "fixed") {
    delaySeconds = baseSeconds;
  } else if (strategy.kind === "exponential") {
    const factor = strategy.factor ?? 2;
    delaySeconds = baseSeconds * Math.pow(factor, attempt - 1);
  } else {
    return null;
  }

  const maxSeconds = strategy.maxSeconds ?? Infinity;
  delaySeconds = Math.min(delaySeconds, maxSeconds);

  return new Date(Date.now() + delaySeconds * 1000);
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

function deserializeClaimedMessage(msg: ClaimedMessage): NormalizedClaimedMessage {
  return {
    ...msg,
    retry_strategy: msg.retry_strategy
      ? deserializeRetryStrategy(msg.retry_strategy)
      : null,
  };
}

function deserializeRetryStrategy(strategy: JsonValue): RetryStrategy | null {
  if (!strategy || typeof strategy !== "object" || Array.isArray(strategy)) {
    return null;
  }

  const obj = strategy as JsonObject;
  const kind = obj.kind;

  if (
    typeof kind !== "string" ||
    (kind !== "fixed" && kind !== "exponential" && kind !== "none")
  ) {
    return null;
  }

  const result: RetryStrategy = { kind };

  const baseSeconds = obj.base_seconds;
  if (typeof baseSeconds === "number") {
    result.baseSeconds = baseSeconds;
  }

  const factor = obj.factor;
  if (typeof factor === "number") {
    result.factor = factor;
  }

  const maxSeconds = obj.max_seconds;
  if (typeof maxSeconds === "number") {
    result.maxSeconds = maxSeconds;
  }

  return result;
}
