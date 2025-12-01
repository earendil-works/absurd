export interface QueueMetrics {
  queueName: string;
  queueLength: number;
  queueVisibleLength: number;
  newestMsgAgeSec?: number | null;
  oldestMsgAgeSec?: number | null;
  totalMessages: number;
  scrapeTime: string;
}

export class APIError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

const defaultHeaders = {
  Accept: "application/json",
};

async function handleResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const message = await extractErrorMessage(response);
    throw new APIError(message, response.status);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return (await response.json()) as T;
}

async function extractErrorMessage(response: Response): Promise<string> {
  try {
    const payload = await response.clone().json();
    if (payload && typeof payload === "object" && "error" in payload) {
      const value = (payload as { error?: string }).error;
      if (typeof value === "string" && value.trim() !== "") {
        return value;
      }
    }
  } catch {
    // ignore json parsing failure
  }

  try {
    const text = await response.text();
    if (text.trim() !== "") {
      return text;
    }
  } catch {
    // ignore
  }

  return `request failed with status ${response.status}`;
}

export async function fetchMetrics(): Promise<QueueMetrics[]> {
  const result = await handleResponse<{ queues: QueueMetrics[] }>(
    await fetch("/api/metrics", {
      headers: defaultHeaders,
    }),
  );

  return result.queues ?? [];
}

export interface TaskSummary {
  taskId: string;
  runId: string;
  queueName: string;
  taskName: string;
  status: string;
  attempt: number;
  maxAttempts?: number | null;
  createdAt: string;
  updatedAt: string;
  completedAt?: string | null;
  workerId?: string;
  params?: string;
}

export interface CheckpointState {
  stepName: string;
  state: any; // JSON object
  status: string;
  ownerRunId?: string | null;
  expiresAt?: string | null;
  updatedAt: string;
}

export interface WaitState {
  waitType: string;
  wakeAt?: string | null;
  wakeEvent?: string | null;
  stepName?: string | null;
  payload?: any;
  eventPayload?: any;
  emittedAt?: string | null;
  updatedAt: string;
}

export interface TaskDetail extends TaskSummary {
  params?: any; // JSON object
  retryStrategy?: any | null;
  headers?: any | null;
  state?: any | null;
  checkpoints: CheckpointState[];
  waits: WaitState[];
}

export interface QueueSummary {
  queueName: string;
  createdAt?: string | null;
  pendingCount: number;
  runningCount: number;
  sleepingCount: number;
  completedCount: number;
  failedCount: number;
  cancelledCount: number;
}

export interface QueueEvent {
  queueName: string;
  eventName: string;
  payload?: any;
  emittedAt?: string | null;
  createdAt: string;
}

export interface TaskListResponse {
  items: TaskSummary[];
  total: number;
  page: number;
  perPage: number;
  availableStatuses: string[];
  availableQueues: string[];
  availableTaskNames: string[];
}

export interface TaskListQuery {
  search?: string;
  status?: string | null;
  queue?: string | null;
  taskName?: string | null;
  taskId?: string | null;
  page?: number;
  perPage?: number;
}

export async function fetchTasks(
  filters: TaskListQuery = {},
): Promise<TaskListResponse> {
  const params = new URLSearchParams();
  const search = filters.search?.trim();
  if (search) {
    params.set("q", search);
  }
  if (filters.status) {
    params.set("status", filters.status);
  }
  if (filters.queue) {
    params.set("queue", filters.queue);
  }
  if (filters.taskName) {
    params.set("taskName", filters.taskName);
  }
  if (filters.taskId) {
    params.set("taskId", filters.taskId);
  }
  if (typeof filters.page === "number" && Number.isFinite(filters.page)) {
    params.set("page", String(filters.page));
  }
  if (typeof filters.perPage === "number" && Number.isFinite(filters.perPage)) {
    params.set("perPage", String(filters.perPage));
  }

  const query = params.toString();
  const url = query ? `/api/tasks?${query}` : "/api/tasks";

  return handleResponse<TaskListResponse>(
    await fetch(url, {
      headers: defaultHeaders,
    }),
  );
}

export async function fetchTask(runId: string): Promise<TaskDetail> {
  return handleResponse<TaskDetail>(
    await fetch(`/api/tasks/${runId}`, {
      headers: defaultHeaders,
    }),
  );
}

export async function fetchQueues(): Promise<QueueSummary[]> {
  return handleResponse<QueueSummary[]>(
    await fetch("/api/queues", {
      headers: defaultHeaders,
    }),
  );
}

export async function fetchQueueTasks(
  queueName: string,
): Promise<TaskSummary[]> {
  return handleResponse<TaskSummary[]>(
    await fetch(`/api/queues/${queueName}/tasks`, {
      headers: defaultHeaders,
    }),
  );
}

export interface QueueEventFilters {
  eventName?: string;
  limit?: number;
}

export async function fetchQueueEvents(
  queueName: string,
  filters: QueueEventFilters = {},
): Promise<QueueEvent[]> {
  const params = new URLSearchParams();
  if (filters.eventName) {
    params.set("eventName", filters.eventName);
  }
  if (typeof filters.limit === "number" && Number.isFinite(filters.limit)) {
    params.set("limit", String(filters.limit));
  }
  const query = params.toString();
  const url = query
    ? `/api/queues/${queueName}/events?${query}`
    : `/api/queues/${queueName}/events`;

  return handleResponse<QueueEvent[]>(
    await fetch(url, {
      headers: defaultHeaders,
    }),
  );
}

export interface EventLogFilters {
  queue?: string | null;
  eventName?: string | null;
  limit?: number;
}

export async function fetchEvents(
  filters: EventLogFilters = {},
): Promise<QueueEvent[]> {
  const params = new URLSearchParams();
  if (filters.queue) {
    params.set("queue", filters.queue);
  }
  if (filters.eventName) {
    params.set("eventName", filters.eventName);
  }
  if (typeof filters.limit === "number" && Number.isFinite(filters.limit)) {
    params.set("limit", String(filters.limit));
  }

  const query = params.toString();
  const url = query ? `/api/events?${query}` : "/api/events";

  return handleResponse<QueueEvent[]>(
    await fetch(url, {
      headers: defaultHeaders,
    }),
  );
}
