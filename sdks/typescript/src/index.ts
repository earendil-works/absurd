/**
 * Absurd SDK for TypeScript and JavaScript
 */
import * as pg from "pg";
import * as os from "os";

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

export interface CancellationPolicy {
  maxDuration?: number;
  maxDelay?: number;
}

export interface SpawnOptions {
  maxAttempts?: number;
  retryStrategy?: RetryStrategy;
  headers?: JsonObject;
  queue?: string;
  cancellation?: CancellationPolicy;
}

export interface ClaimedTask {
  run_id: string;
  task_id: string;
  task_name: string;
  attempt: number;
  params: JsonValue;
  retry_strategy: JsonValue;
  max_attempts: number | null;
  headers: JsonObject | null;
  wake_event: string | null;
  event_payload: JsonValue | null;
}

export interface WorkerOptions {
  workerId?: string;
  claimTimeout?: number;
  batchSize?: number;
  concurrency?: number;
  pollInterval?: number;
  onError?: (error: Error) => void;
  fatalOnLeaseTimeout?: boolean;
}

export interface Worker {
  close(): Promise<void>;
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

/**
 * Internal exception that is thrown to suspend a run.  As a user
 * you should never see this exception.
 */
export class SuspendTask extends Error {
  constructor() {
    super("Task suspended");
    this.name = "SuspendTask";
  }
}

/**
 * This error is thrown when awaiting an event ran into a timeout.
 */
export class TimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TimeoutError";
  }
}

export interface TaskRegistrationOptions {
  name: string;
  queue?: string;
  defaultMaxAttempts?: number;
  defaultCancellation?: CancellationPolicy;
}

interface Log {
  log(...args: any[]): void;
  info(...args: any[]): void;
  warn(...args: any[]): void;
  error(...args: any[]): void;
}

export interface AbsurdOptions {
  db?: pg.Pool | string;
  queueName?: string;
  defaultMaxAttempts?: number;
  log?: Log;
}

interface RegisteredTask {
  name: string;
  queue: string;
  defaultMaxAttempts?: number;
  defaultCancellation?: CancellationPolicy;
  handler: TaskHandler<any, any>;
}

export class TaskContext {
  private stepNameCounter: Map<string, number> = new Map();

  private constructor(
    private readonly log: Log,
    readonly taskID: string,
    private readonly pool: pg.Pool,
    private readonly queueName: string,
    private readonly task: ClaimedTask,
    private readonly checkpointCache: Map<string, JsonValue>,
    private readonly claimTimeout: number,
  ) {}

  static async create(args: {
    log: Log;
    taskID: string;
    pool: pg.Pool;
    queueName: string;
    task: ClaimedTask;
    claimTimeout: number;
  }): Promise<TaskContext> {
    const { log, taskID, pool, queueName, task, claimTimeout } = args;
    const result = await pool.query<CheckpointRow>(
      `SELECT checkpoint_name, state, status, owner_run_id, updated_at
       FROM absurd.get_task_checkpoint_states($1, $2, $3)`,
      [queueName, task.task_id, task.run_id],
    );
    const cache = new Map<string, JsonValue>();
    for (const row of result.rows) {
      cache.set(row.checkpoint_name, row.state);
    }
    return new TaskContext(
      log,
      taskID,
      pool,
      queueName,
      task,
      cache,
      claimTimeout,
    );
  }

  /**
   * Defines a step in the task execution.  Steps are idempotent in
   * that they are executed exactly once (unless they fail) and their
   * results are cached.  As a result the return value of this function
   * must support `JSON.stringify`.
   */
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

  /**
   * Sleeps for a given number of seconds.  Note that this
   * *always* suspends the task, even if you only wait for a very
   * short period of time.
   */
  async sleepFor(stepName: string, duration: number): Promise<void> {
    return await this.sleepUntil(
      stepName,
      new Date(Date.now() + duration * 1000),
    );
  }

  /**
   * Like `sleepFor` but with an absolute time when the task should be
   * awoken again.
   */
  async sleepUntil(stepName: string, wakeAt: Date): Promise<void> {
    const checkpointName = this.getCheckpointName(stepName);
    const state = await this.lookupCheckpoint(checkpointName);
    let actualWakeAt = typeof state === "string" ? new Date(state) : wakeAt;
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

  private async lookupCheckpoint(
    checkpointName: string,
  ): Promise<JsonValue | undefined> {
    const cached = this.checkpointCache.get(checkpointName);
    if (cached !== undefined) {
      return cached;
    }

    const result = await this.pool.query<CheckpointRow>(
      `SELECT checkpoint_name, state, status, owner_run_id, updated_at
       FROM absurd.get_task_checkpoint_state($1, $2, $3)`,
      [this.queueName, this.task.task_id, checkpointName],
    );
    if (result.rows.length > 0) {
      const state = result.rows[0].state;
      this.checkpointCache.set(checkpointName, state);
      return state;
    }
    return undefined;
  }

  private async persistCheckpoint(
    checkpointName: string,
    value: JsonValue,
  ): Promise<void> {
    await this.pool.query(
      `SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)`,
      [
        this.queueName,
        this.task.task_id,
        checkpointName,
        JSON.stringify(value),
        this.task.run_id,
        this.claimTimeout,
      ],
    );
    this.checkpointCache.set(checkpointName, value);
  }

  private async scheduleRun(wakeAt: Date): Promise<void> {
    await this.pool.query(`SELECT absurd.schedule_run($1, $2, $3)`, [
      this.queueName,
      this.task.run_id,
      wakeAt,
    ]);
  }

  /**
   * Awaits the arrival of an event.  Events need to be uniquely
   * named so fold in the necessary parameters into the name (eg: customer id).
   */
  async awaitEvent(
    eventName: string,
    options?: { stepName?: string; timeout?: number },
  ): Promise<JsonValue> {
    // the default step name is derived from the event name.
    const stepName = options?.stepName || `$awaitEvent:${eventName}`;
    let timeout: number | null = null;
    if (
      options?.timeout !== undefined &&
      Number.isFinite(options?.timeout) &&
      options?.timeout >= 0
    ) {
      timeout = Math.floor(options?.timeout);
    }
    const checkpointName = this.getCheckpointName(stepName);
    const cached = await this.lookupCheckpoint(checkpointName);
    if (cached !== undefined) {
      return cached as JsonValue;
    }
    if (
      this.task.wake_event === eventName &&
      (this.task.event_payload === null ||
        this.task.event_payload === undefined)
    ) {
      this.task.wake_event = null;
      this.task.event_payload = null;
      throw new TimeoutError(`Timed out waiting for event "${eventName}"`);
    }
    const result = await this.pool.query<{
      should_suspend: boolean;
      payload: JsonValue;
    }>(
      `SELECT should_suspend, payload
       FROM absurd.await_event($1, $2, $3, $4, $5, $6)`,
      [
        this.queueName,
        this.task.task_id,
        this.task.run_id,
        checkpointName,
        eventName,
        timeout,
      ],
    );

    if (result.rows.length === 0) {
      throw new Error("Failed to await event");
    }

    const { should_suspend, payload } = result.rows[0];

    if (!should_suspend) {
      this.checkpointCache.set(checkpointName, payload);
      this.task.event_payload = null;
      return payload;
    }

    throw new SuspendTask();
  }

  /**
   * Emits an event that can be awaited.
   */
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
    await this.pool.query(`SELECT absurd.complete_run($1, $2, $3)`, [
      this.queueName,
      this.task.run_id,
      JSON.stringify(result ?? null),
    ]);
  }

  async fail(err: unknown): Promise<void> {
    this.log.error("[absurd] task execution failed:", err);
    await this.pool.query(`SELECT absurd.fail_run($1, $2, $3, $4)`, [
      this.queueName,
      this.task.run_id,
      JSON.stringify(serializeError(err)),
      null,
    ]);
  }
}

/**
 * The Absurd SDK Client.
 *
 * Instanciate this class and keep it around to interact with Absurd.
 */
export class Absurd {
  private readonly pool: pg.Pool;
  private readonly ownedPool: boolean;
  private readonly queueName: string;
  private readonly defaultMaxAttempts: number;
  private readonly registry = new Map<string, RegisteredTask>();
  private readonly log: Log;
  private worker: Worker | null = null;

  constructor(options: AbsurdOptions | string | pg.Pool = {}) {
    if (typeof options === "string" || options instanceof pg.Pool) {
      options = { db: options };
    }
    let poolOrUrl = options.db;
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
    this.queueName = options?.queueName ?? "default";
    this.defaultMaxAttempts = options?.defaultMaxAttempts ?? 5;
    this.log = options?.log ?? console;
  }

  /**
   * This registers a given function as task.
   */
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
    if (options.defaultCancellation) {
      normalizeCancellation(options.defaultCancellation);
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
      defaultCancellation: options.defaultCancellation,
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
    for (const row of result.rows) {
      rv.push(row.queue_name);
    }
    return rv;
  }

  /**
   * Spawns a specific task.
   */
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
    } else {
      queue = options.queue;
    }
    const effectiveMaxAttempts =
      options.maxAttempts !== undefined
        ? options.maxAttempts
        : (registration?.defaultMaxAttempts ?? this.defaultMaxAttempts);
    const effectiveCancellation =
      options.cancellation !== undefined
        ? options.cancellation
        : registration?.defaultCancellation;
    const normalizedOptions = normalizeSpawnOptions({
      ...options,
      maxAttempts: effectiveMaxAttempts,
      cancellation: effectiveCancellation,
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

  /**
   * Emits an event from outside of a task.
   */
  async emitEvent(
    eventName: string,
    payload?: JsonValue,
    queueName?: string,
  ): Promise<void> {
    if (!eventName) {
      throw new Error("eventName must be a non-empty string");
    }
    await this.pool.query(`SELECT absurd.emit_event($1, $2, $3)`, [
      queueName || this.queueName,
      eventName,
      JSON.stringify(payload ?? null),
    ]);
  }

  async claimTasks(options?: {
    batchSize?: number;
    claimTimeout?: number;
    workerId?: string;
  }): Promise<ClaimedTask[]> {
    const {
      batchSize: count = 1,
      claimTimeout = 120,
      workerId = "worker",
    } = options ?? {};

    const result = await this.pool.query<ClaimedTask>(
      `SELECT run_id, task_id, attempt, task_name, params, retry_strategy, max_attempts,
              headers, wake_event, event_payload
       FROM absurd.claim_task($1, $2, $3, $4)`,
      [this.queueName, workerId, claimTimeout, count],
    );

    return result.rows;
  }

  /**
   * Polls and processes a batch of messages sequentially.
   * For parallel processing, use startWorker with concurrency option.
   */
  async workBatch(
    workerId: string = "worker",
    claimTimeout: number = 120,
    batchSize: number = 1,
  ): Promise<void> {
    const tasks = await this.claimTasks({ batchSize, claimTimeout, workerId });

    for (const task of tasks) {
      await this.executeTask(task, claimTimeout);
    }
  }

  /**
   * Starts a worker that continuously polls for tasks and processes them.
   * Returns a Worker instance with a close() method for graceful shutdown.
   */
  async startWorker(options: WorkerOptions = {}): Promise<Worker> {
    const {
      workerId = `${os.hostname?.() || "host"}:${process.pid}`,
      claimTimeout = 120,
      concurrency = 1,
      batchSize,
      pollInterval = 0.25,
      onError = (err) => console.error("Worker error:", err),
      fatalOnLeaseTimeout = true,
    } = options;
    const effectiveBatchSize = batchSize ?? concurrency;
    let running = true;
    let workerLoopPromise: Promise<void>;
    const executing = new Set<Promise<void>>();
    let availabilityPromise: Promise<void> | null = null;
    let availabilityResolve: (() => void) | null = null;
    let sleepTimer: NodeJS.Timeout | null = null;

    const notifyAvailability = () => {
      if (sleepTimer) {
        clearTimeout(sleepTimer);
        sleepTimer = null;
      }
      if (availabilityResolve) {
        availabilityResolve();
        availabilityResolve = null;
        availabilityPromise = null;
      }
    };

    const waitForAvailability = async () => {
      if (!availabilityPromise) {
        availabilityPromise = new Promise<void>((resolve) => {
          availabilityResolve = resolve;
          sleepTimer = setTimeout(() => {
            sleepTimer = null;
            availabilityResolve = null;
            availabilityPromise = null;
            resolve();
          }, pollInterval * 1000);
        });
      }
      await availabilityPromise;
    };

    const worker: Worker = {
      close: async () => {
        running = false;
        await workerLoopPromise;
      },
    };

    this.worker = worker;
    workerLoopPromise = (async () => {
      while (running) {
        try {
          if (executing.size >= concurrency) {
            await waitForAvailability();
            continue;
          }

          const availableCapacity = Math.max(concurrency - executing.size, 0);
          const toClaim = Math.min(effectiveBatchSize, availableCapacity);

          if (toClaim <= 0) {
            await waitForAvailability();
            continue;
          }

          const messages = await this.claimTasks({
            batchSize: toClaim,
            claimTimeout: claimTimeout,
            workerId,
          });

          if (messages.length === 0) {
            await waitForAvailability();
            continue;
          }

          for (const task of messages) {
            const promise = this.executeTask(task, claimTimeout, {
              fatalOnLeaseTimeout,
            })
              .catch((err) => onError(err as Error))
              .finally(() => {
                executing.delete(promise);
                notifyAvailability();
              });
            executing.add(promise);
          }
        } catch (err) {
          onError(err as Error);
          await waitForAvailability();
        }
      }
      await Promise.allSettled(executing);
    })();

    return worker;
  }

  async close(): Promise<void> {
    if (this.worker) {
      await this.worker.close();
    }

    if (this.ownedPool) {
      await this.pool.end();
    }
  }

  private async executeTask(
    task: ClaimedTask,
    claimTimeout: number,
    options?: { fatalOnLeaseTimeout?: boolean },
  ): Promise<void> {
    let warnTimer: any;
    let fatalTimer: any;

    const registration = this.registry.get(task.task_name);
    const ctx = await TaskContext.create({
      log: this.log,
      taskID: task.task_id,
      pool: this.pool,
      queueName: registration?.queue ?? "unknown",
      task: task,
      claimTimeout,
    });

    try {
      if (claimTimeout > 0) {
        const taskLabel = `${task.task_name} (${task.task_id})`;
        warnTimer = setTimeout(() => {
          this.log.warn(
            `task ${taskLabel} exceeded claim timeout of ${claimTimeout}s`,
          );
        }, claimTimeout * 1000);
        if (options?.fatalOnLeaseTimeout) {
          fatalTimer = setTimeout(
            () => {
              this.log.error(
                `task ${taskLabel} exceeded claim timeout of ${claimTimeout}s by more than 100%; terminating process`,
              );
              process.exit(1);
            },
            claimTimeout * 1000 * 2,
          );
        }
      }

      if (!registration) {
        throw new Error("Unknown task");
      } else if (registration.queue !== this.queueName) {
        throw new Error("Misconfigured task (queue mismatch)");
      }
      const result = await registration.handler(task.params, ctx);
      await ctx.complete(result);
    } catch (err) {
      if (err instanceof SuspendTask) {
        // Task suspended (sleep or await), don't complete or fail
        return;
      }
      await ctx.fail(err);
    } finally {
      if (warnTimer) {
        clearTimeout(warnTimer);
      }
      if (fatalTimer) {
        clearTimeout(fatalTimer);
      }
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
  const cancellation = normalizeCancellation(options.cancellation);
  if (cancellation) {
    normalized.cancellation = cancellation;
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

function normalizeCancellation(
  policy?: CancellationPolicy,
): JsonObject | undefined {
  if (!policy) {
    return undefined;
  }
  const normalized: JsonObject = {};
  if (policy.maxDuration !== undefined) {
    normalized.max_duration = policy.maxDuration;
  }
  if (policy.maxDelay !== undefined) {
    normalized.max_delay = policy.maxDelay;
  }
  return Object.keys(normalized).length > 0 ? normalized : undefined;
}
