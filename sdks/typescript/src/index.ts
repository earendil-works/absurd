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
    // Generate unique step name with counter for duplicates to allow the use
    // of steps in loops.
    const count = (this.stepNameCounter.get(name) ?? 0) + 1;
    this.stepNameCounter.set(name, count);
    const actualStepName = count === 1 ? name : `${name}#${count}`;

    if (this.checkpointCache.has(actualStepName)) {
      return this.checkpointCache.get(actualStepName) as T;
    }

    const result = await this.pool.query<CheckpointRow>(
      `SELECT checkpoint_name, state, status, owner_run_id, updated_at
       FROM absurd.get_task_checkpoint_state($1, $2, $3)`,
      [this.queueName, this.message.task_id, actualStepName],
    );
    if (result.rows.length > 0) {
      const state = result.rows[0].state;
      this.checkpointCache.set(actualStepName, state);
      return state as T;
    }

    const rv = await fn();
    await this.pool.query(
      `SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5)`,
      [
        this.queueName,
        this.message.task_id,
        actualStepName,
        JSON.stringify(rv),
        this.message.run_id,
      ],
    );
    this.checkpointCache.set(actualStepName, rv as JsonValue);
    return rv;
  }

  async sleepFor(stepName: string, durationMs: number): Promise<void> {
    return await this.sleepUntil(stepName, new Date(Date.now() + durationMs));
  }

  async sleepUntil(stepName: string, wakeAt: Date): Promise<void> {
    const cachedWakeAt = this.getCachedWakeAt(stepName);
    const targetWakeAt = cachedWakeAt ?? wakeAt;
    if (!cachedWakeAt) {
      await this.persistWakeAt(stepName, targetWakeAt);
    }

    if (Date.now() >= targetWakeAt.getTime()) {
      return;
    }

    await this.scheduleRun(targetWakeAt);
    throw new SuspendTask();
  }

  private getCachedWakeAt(stepName: string): Date | null {
    const rawValue = this.checkpointCache.get(stepName);
    return typeof rawValue === "string" ? new Date(rawValue) : null;
  }

  private async persistWakeAt(stepName: string, wakeAt: Date): Promise<void> {
    const iso = wakeAt.toISOString();
    await this.pool.query(
      `SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5)`,
      [
        this.queueName,
        this.message.task_id,
        stepName,
        JSON.stringify(iso),
        this.message.run_id,
      ],
    );
    this.checkpointCache.set(stepName, iso);
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
    if (this.checkpointCache.has(stepName)) {
      return this.checkpointCache.get(stepName) ?? null;
    }

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
        stepName,
        eventName,
      ],
    );

    if (result.rows.length === 0) {
      throw new Error("Failed to await event");
    }

    const { should_suspend, payload } = result.rows[0];

    if (!should_suspend) {
      await this.pool.query(
        `SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5)`,
        [
          this.queueName,
          this.message.task_id,
          stepName,
          JSON.stringify(payload ?? null),
          this.message.run_id,
        ],
      );

      this.checkpointCache.set(stepName, payload ?? null);
      return payload ?? null;
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
  ): Promise<{ task_id: string; run_id: string; attempt: number }> {
    const registration = this.registry.get(taskName);
    let queue: string | undefined;
    if (registration) {
      queue = registration.queue;
      if (options.queue !== undefined && options.queue !== registration.queue) {
        throw new Error(
          `Task "${taskName}" is registered for queue "${registration.queue}" but spawn requested queue "${options.queue}".`,
        );
      }
    } else {
      queue = options.queue;
      if (!queue) {
        throw new Error(
          `Task "${taskName}" is not registered. Provide options.queue when spawning unregistered tasks.`,
        );
      }
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

  private async executeMessage(msg: ClaimedMessage): Promise<void> {
    const registration = this.registry.get(msg.task_name);
    if (!registration) {
      await this.pool.query(`SELECT absurd.fail_run($1, $2, $3, $4)`, [
        this.queueName,
        msg.run_id,
        JSON.stringify({ error: "unknown-task" }),
        null,
      ]);
      return;
    }

    if (registration.queue !== this.queueName) {
      await this.pool.query(`SELECT absurd.fail_run($1, $2, $3, $4)`, [
        this.queueName,
        msg.run_id,
        JSON.stringify({
          error: "misconfigured-task",
          expected_queue: registration.queue,
        }),
        null,
      ]);
      return;
    }

    const ctx = await TaskContext.create({
      pool: this.pool,
      queueName: registration.queue,
      message: msg,
    });

    try {
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
