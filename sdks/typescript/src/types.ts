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

export interface CheckpointRow {
  checkpoint_name: string;
  state: JsonValue;
  status: string;
  owner_run_id: string;
  ephemeral: boolean;
  expires_at: Date | null;
  updated_at: Date;
}
