import { afterAll, beforeAll } from "vitest";
import {
  PostgreSqlContainer,
  StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import { Pool } from "pg";
import { readFileSync } from "fs";
import { join } from "path";
import { Absurd, type JsonValue } from "../src/index.js";

// Database row types matching the PostgreSQL schema
export interface TaskRow {
  task_id: string;
  task_name: string;
  params: JsonValue;
  headers: JsonValue | null;
  retry_strategy: JsonValue | null;
  max_attempts: number | null;
  cancellation: JsonValue | null;
  enqueue_at: Date;
  first_started_at: Date | null;
  state:
    | "pending"
    | "running"
    | "sleeping"
    | "completed"
    | "failed"
    | "cancelled";
  attempts: number;
  last_attempt_run: string | null;
  completed_payload: JsonValue | null;
  cancelled_at: Date | null;
}

export interface RunRow {
  run_id: string;
  task_id: string;
  attempt: number;
  state:
    | "pending"
    | "running"
    | "sleeping"
    | "completed"
    | "failed"
    | "cancelled";
  claimed_by: string | null;
  claim_expires_at: Date | null;
  available_at: Date;
  wake_event: string | null;
  event_payload: JsonValue | null;
  started_at: Date | null;
  completed_at: Date | null;
  failed_at: Date | null;
  result: JsonValue | null;
  failure_reason: JsonValue | null;
  created_at: Date;
}

export let container: StartedPostgreSqlContainer;
export let pool: Pool;

beforeAll(async () => {
  console.time("Test container startup");
  container = await new PostgreSqlContainer("postgres:16-alpine")
    .withExposedPorts(5432)
    .start();
  console.timeEnd("Test container startup");

  pool = new Pool({
    host: container.getHost(),
    port: container.getPort(),
    database: container.getDatabase(),
    user: container.getUsername(),
    password: container.getPassword(),
    max: 1,
  });

  const schemaPath = join(__dirname, "../../../sql/absurd.sql");
  const schema = readFileSync(schemaPath, "utf-8");
  await pool.query(schema);

  console.log("✓ Test container started and schema loaded");
}, 60000);

afterAll(async () => {
  console.time("Test container cleanup");
  if (pool) {
    await pool.end();
  }
  if (container) {
    await container.stop();
  }
  console.timeEnd("Test container cleanup");
});

export interface TestContext {
  absurd: Absurd;
  pool: typeof pool;
  queueName: string;
  cleanupTasks(): Promise<void>;
  getTask(taskID: string): Promise<TaskRow | null>;
  getRun(runID: string): Promise<RunRow | null>;
  setFakeNow(ts: Date | null): Promise<void>;
}

export function randomName(prefix = "test"): string {
  return `${prefix}_${Math.random().toString(36).substring(7)}`;
}

export async function createTestAbsurd(
  queueName: string = "default",
): Promise<TestContext> {
  const absurd = new Absurd({
    db: pool,
    queueName,
  });

  await absurd.createQueue(queueName);

  return {
    absurd,
    pool,
    queueName,
    cleanupTasks: () => cleanupTasks(queueName),
    getTask: (taskID: string) => getTask(taskID, queueName),
    getRun: (runID: string) => getRun(runID, queueName),
    setFakeNow: (ts: Date | null) => setFakeNow(ts),
  };
}

async function setFakeNow(ts: Date | null): Promise<void> {
  if (ts === null) {
    await pool.query("SELECT set_config('absurd.fake_now', NULL, false)");
  } else {
    await pool.query("SELECT set_config('absurd.fake_now', $1, false)", [
      ts.toISOString(),
    ]);
  }
}

async function cleanupTasks(queue: string): Promise<void> {
  try {
    await pool.query(
      `TRUNCATE absurd.t_${queue}, absurd.r_${queue}, absurd.c_${queue}, absurd.e_${queue}, absurd.w_${queue}`,
    );
  } catch (err: any) {
    if (!err.message?.includes("does not exist")) {
      throw err;
    }
  }
}

// Internal helpers for querying task and run state
async function getTask(taskID: string, queue: string): Promise<TaskRow | null> {
  const { rows } = await pool.query<TaskRow>(
    `SELECT * FROM absurd.t_${queue} WHERE task_id = $1`,
    [taskID],
  );
  return rows.length > 0 ? rows[0] : null;
}

async function getRun(runID: string, queue: string): Promise<RunRow | null> {
  const { rows } = await pool.query<RunRow>(
    `SELECT * FROM absurd.r_${queue} WHERE run_id = $1`,
    [runID],
  );
  return rows.length > 0 ? rows[0] : null;
}
