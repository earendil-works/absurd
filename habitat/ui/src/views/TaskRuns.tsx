import { A, useParams } from "@solidjs/router";
import {
  For,
  Show,
  createEffect,
  createMemo,
  createResource,
  createSignal,
  onCleanup,
} from "solid-js";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { TaskDetailView } from "@/components/TaskDetailView";
import { TaskStatusBadge } from "@/components/TaskStatusBadge";
import { IdDisplay } from "@/components/IdDisplay";
import {
  type TaskDetail,
  type TaskSummary,
  fetchTask,
  fetchTasks,
} from "@/lib/api";

export default function TaskRuns() {
  const params = useParams<{ taskId: string }>();
  const taskId = () => params.taskId;

  const [runsError, setRunsError] = createSignal<string | null>(null);
  const [detailError, setDetailError] = createSignal<string | null>(null);
  const [runDetails, setRunDetails] = createSignal<Record<string, TaskDetail>>(
    {},
  );

  const RUNS_PAGE_SIZE = 200;

  const fetchRunsForTask = async (id: string): Promise<TaskSummary[]> => {
    if (!id) {
      return [];
    }

    const firstPage = await fetchTasks({
      taskId: id,
      page: 1,
      perPage: RUNS_PAGE_SIZE,
    });
    let results = firstPage.items;

    const total = firstPage.total;
    const perPage = firstPage.perPage || RUNS_PAGE_SIZE;
    const totalPages = perPage > 0 ? Math.ceil(total / perPage) : 1;

    if (totalPages <= 1) {
      return results;
    }

    for (let currentPage = 2; currentPage <= totalPages; currentPage += 1) {
      const pageResult = await fetchTasks({
        taskId: id,
        page: currentPage,
        perPage,
      });
      results = results.concat(pageResult.items);
    }

    return results;
  };

  const [runs, { refetch: refetchRuns }] = createResource<
    TaskSummary[],
    string
  >(taskId, fetchRunsForTask);

  createEffect(() => {
    const error = runs.error;
    if (!error) {
      setRunsError(null);
      return;
    }

    setRunsError(error.message ?? "Failed to load task runs.");
  });

  createEffect(() => {
    // Reset cached details when the task changes
    taskId();
    setRunDetails({});
    setDetailError(null);
  });

  createEffect(() => {
    const summaries = runs();
    if (!summaries || summaries.length === 0) {
      return;
    }

    const cached = runDetails();
    const missing = summaries.filter((run) => !cached[run.runId]);
    if (missing.length === 0) {
      return;
    }

    let cancelled = false;

    (async () => {
      try {
        const results = await Promise.all(
          missing.map(async (run) => {
            const detail = await fetchTask(run.runId);
            return [run.runId, detail] as const;
          }),
        );
        if (cancelled) {
          return;
        }
        setRunDetails((current) => {
          const next = { ...current };
          for (const [runId, detail] of results) {
            next[runId] = detail;
          }
          return next;
        });
        setDetailError(null);
      } catch (error) {
        if (cancelled) {
          return;
        }
        console.error("failed to load run details", error);
        setDetailError("Failed to load run details.");
      }
    })();

    onCleanup(() => {
      cancelled = true;
    });
  });

  const taskName = createMemo(() => runs()?.[0]?.taskName ?? null);
  const queueNames = createMemo(() => {
    const items = runs();
    if (!items) return [];
    return Array.from(new Set(items.map((run) => run.queueName)));
  });

  const orderedRuns = createMemo(() => {
    const items = runs();
    if (!items) return [];
    return [...items].sort((a, b) => {
      if (a.attempt !== b.attempt) {
        return a.attempt - b.attempt;
      }
      const aCreated = Date.parse(a.createdAt);
      const bCreated = Date.parse(b.createdAt);
      if (Number.isNaN(aCreated) || Number.isNaN(bCreated)) {
        return 0;
      }
      return aCreated - bCreated;
    });
  });

  const totalDurationMs = createMemo(() => {
    const items = orderedRuns();
    if (items.length === 0) {
      return null;
    }

    let earliest = Number.POSITIVE_INFINITY;
    let latest = Number.NEGATIVE_INFINITY;

    for (const run of items) {
      const created = Date.parse(run.createdAt);
      if (!Number.isNaN(created)) {
        if (created < earliest) {
          earliest = created;
        }
        if (created > latest) {
          latest = created;
        }
      }

      const updated = Date.parse(run.updatedAt);
      if (!Number.isNaN(updated) && updated > latest) {
        latest = updated;
      }

      if (run.completedAt) {
        const completed = Date.parse(run.completedAt);
        if (!Number.isNaN(completed) && completed > latest) {
          latest = completed;
        }
      }
    }

    if (
      !Number.isFinite(earliest) ||
      !Number.isFinite(latest) ||
      latest < earliest
    ) {
      return null;
    }

    return Math.max(0, latest - earliest);
  });

  const completionAttempts = createMemo(() => {
    const items = orderedRuns();
    if (items.length === 0) {
      return null;
    }

    const completedIndex = items.findIndex(
      (run) => run.status.toLowerCase() === "completed",
    );
    if (completedIndex === -1) {
      return null;
    }

    const attemptNumber = items[completedIndex]?.attempt;
    const derivedAttempts =
      typeof attemptNumber === "number" && attemptNumber > 0
        ? attemptNumber
        : completedIndex + 1;
    return Math.max(derivedAttempts, completedIndex + 1);
  });

  const checkpointsInfo = createMemo(() => {
    const items = orderedRuns();
    if (items.length === 0) {
      return { total: 0, pending: false };
    }

    const detailMap = runDetails();
    let total = 0;
    let missing = 0;

    for (const run of items) {
      const detail = detailMap[run.runId];
      if (!detail) {
        missing += 1;
        continue;
      }
      total += detail.checkpoints?.length ?? 0;
    }

    return { total, pending: missing > 0 };
  });

  const queueSummary = createMemo(() => {
    const names = queueNames();
    if (!names || names.length === 0) {
      return { label: "Queue", value: null as string | null };
    }
    return {
      label: names.length > 1 ? "Queues" : "Queue",
      value: names.join(", "),
    };
  });

  const handleRefresh = async () => {
    setDetailError(null);
    setRunsError(null);
    setRunDetails({});

    try {
      await refetchRuns();
    } catch (error) {
      console.error("failed to refresh task runs", error);
      setRunsError("Failed to refresh task runs.");
    }
  };

  const formatRelativeAge = (timestamp: string) => {
    const date = new Date(timestamp);
    if (Number.isNaN(date.getTime())) {
      return "—";
    }

    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffSec = Math.floor(diffMs / 1000);

    if (diffSec < 60) return `${diffSec}s`;
    const diffMin = Math.floor(diffSec / 60);
    if (diffMin < 60) return `${diffMin}m`;
    const diffHour = Math.floor(diffMin / 60);
    if (diffHour < 24) return `${diffHour}h`;
    const diffDay = Math.floor(diffHour / 24);
    return `${diffDay}d`;
  };

  const formatAbsolute = (timestamp: string) => {
    const date = new Date(timestamp);
    if (Number.isNaN(date.getTime())) {
      return "—";
    }
    try {
      return new Intl.DateTimeFormat(undefined, {
        dateStyle: "medium",
        timeStyle: "medium",
      }).format(date);
    } catch {
      return date.toISOString();
    }
  };

  const formatDuration = (ms: number | null) => {
    if (ms === null || ms === undefined) {
      return "—";
    }
    if (!Number.isFinite(ms)) {
      return "—";
    }
    if (ms < 1000) {
      return "<1s";
    }

    const totalSeconds = Math.floor(ms / 1000);
    const units = [
      { label: "d", size: 86400 },
      { label: "h", size: 3600 },
      { label: "m", size: 60 },
      { label: "s", size: 1 },
    ] as const;

    const parts: string[] = [];
    let remainder = totalSeconds;

    for (const unit of units) {
      if (unit.size > remainder && parts.length === 0 && unit.label !== "s") {
        continue;
      }
      const value = Math.floor(remainder / unit.size);
      if (value > 0) {
        parts.push(`${value}${unit.label}`);
        remainder -= value * unit.size;
      }
    }

    if (parts.length === 0) {
      return "0s";
    }

    return parts.join(" ");
  };

  return (
    <>
      <header class="flex flex-col gap-4 border-b bg-background px-6 py-6 sm:flex-row sm:items-center sm:justify-between">
        <div class="space-y-2">
          <div>
            <h1 class="text-2xl font-semibold tracking-tight">
              Task {taskName() ? `“${taskName()}”` : ""}
            </h1>
            <p class="text-xs text-muted-foreground flex flex-wrap items-center gap-1">
              <span>Task ID:</span>
              <IdDisplay value={taskId()} />
            </p>
          </div>
          <Show when={queueNames().length}>
            <p class="text-xs text-muted-foreground">
              Queues: {queueNames().join(", ")}
            </p>
          </Show>
        </div>
        <div class="flex flex-col-reverse gap-3 sm:flex-row sm:items-center">
          <A
            href="/tasks"
            class={`${buttonVariants({ variant: "ghost", size: "sm" })} text-xs text-muted-foreground`}
          >
            ← Back to runs
          </A>
          <Button
            variant="outline"
            onClick={handleRefresh}
            class="min-w-[96px]"
            disabled={runs.loading}
          >
            {runs.loading ? "Refreshing…" : "Refresh"}
          </Button>
        </div>
      </header>

      <section class="flex-1 space-y-6 px-6 py-6">
        <Card class="border-dashed bg-muted/20">
          <Show
            when={orderedRuns().length > 0}
            fallback={
              <CardContent class="p-4 text-xs text-muted-foreground sm:text-sm">
                No runs have been recorded for this task yet.
              </CardContent>
            }
          >
            <CardContent class="flex flex-wrap items-center gap-x-6 gap-y-2 p-4 text-xs sm:flex-nowrap sm:text-sm">
              <div class="flex items-center gap-2">
                <span class="text-muted-foreground">Runs</span>
                <span class="font-medium text-foreground">
                  {runs()?.length ?? 0}
                </span>
              </div>
              <div class="flex items-center gap-2">
                <span class="text-muted-foreground">Statuses</span>
                <div class="flex flex-wrap items-center gap-1">
                  <For each={orderedRuns()}>
                    {(run) => (
                      <span title={`Attempt ${run.attempt}`}>
                        <TaskStatusBadge status={run.status} />
                      </span>
                    )}
                  </For>
                </div>
              </div>
              <div class="flex items-center gap-1">
                <span class="text-muted-foreground">Duration</span>
                <span class="font-medium text-foreground">
                  {formatDuration(totalDurationMs())}
                </span>
              </div>
              <div class="flex items-center gap-1">
                <span class="text-muted-foreground">Completion</span>
                <span class="font-medium text-foreground">
                  {(() => {
                    const attempts = completionAttempts();
                    if (attempts === null) {
                      return "Not completed";
                    }
                    return `Completed in ${attempts} attempt${
                      attempts === 1 ? "" : "s"
                    }`;
                  })()}
                </span>
              </div>
              <div class="flex items-center gap-1">
                <span class="text-muted-foreground">
                  {queueSummary().label}
                </span>
                <span class="font-medium text-foreground">
                  {queueSummary().value ?? "—"}
                </span>
              </div>
              <div class="flex items-center gap-1">
                <span class="text-muted-foreground">Checkpoints</span>
                <span class="font-medium text-foreground">
                  {(() => {
                    const info = checkpointsInfo();
                    if (info.pending) {
                      return "Loading…";
                    }
                    return info.total.toString();
                  })()}
                </span>
              </div>
              <div class="flex items-center gap-1">
                <span class="text-muted-foreground">Updated</span>
                <span class="font-medium text-foreground">
                  {(() => {
                    const items = orderedRuns();
                    if (items.length === 0) {
                      return "—";
                    }
                    const latest = items.reduce((acc, run) => {
                      const updated = Date.parse(run.updatedAt);
                      return Number.isNaN(updated)
                        ? acc
                        : Math.max(acc, updated);
                    }, Number.NEGATIVE_INFINITY);
                    if (!Number.isFinite(latest)) {
                      return "—";
                    }
                    return `${formatRelativeAge(new Date(latest).toISOString())} ago`;
                  })()}
                </span>
              </div>
            </CardContent>
          </Show>
        </Card>

        <Show
          when={!runs.loading || (runs()?.length ?? 0) > 0}
          fallback={<p class="text-sm text-muted-foreground">Loading runs…</p>}
        >
          <Show
            when={(runs()?.length ?? 0) > 0}
            fallback={
              <p class="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
                No runs found for this task.
              </p>
            }
          >
            <div class="space-y-6">
              <For each={runs()}>
                {(run) => (
                  <Card>
                    <CardHeader>
                      <CardTitle class="flex flex-wrap items-center gap-2 text-base">
                        <span class="text-sm font-semibold">Run ID:</span>
                        <IdDisplay value={run.runId} />
                        <TaskStatusBadge status={run.status} />
                      </CardTitle>
                      <CardDescription>
                        <div class="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
                          <span class="inline-flex items-center gap-1">
                            <span class="text-muted-foreground opacity-80">
                              Queue:
                            </span>
                            <span class="font-medium text-foreground">
                              {run.queueName}
                            </span>
                          </span>
                          <span class="inline-flex items-center gap-1">
                            <span class="text-muted-foreground opacity-80">
                              Attempt:
                            </span>
                            <span class="font-medium text-foreground">
                              {run.attempt}
                            </span>
                          </span>
                          <Show
                            when={
                              run.maxAttempts !== null &&
                              run.maxAttempts !== undefined
                            }
                          >
                            <span class="inline-flex items-center gap-1">
                              <span class="text-muted-foreground opacity-80">
                                Max attempts:
                              </span>
                              <span class="font-medium text-foreground">
                                {run.maxAttempts}
                              </span>
                            </span>
                          </Show>
                          <span class="inline-flex items-center gap-1">
                            <span class="text-muted-foreground opacity-80">
                              Created:
                            </span>
                            <span
                              class="font-medium text-foreground"
                              title={formatAbsolute(run.createdAt)}
                            >
                              {formatRelativeAge(run.createdAt)} ago
                            </span>
                          </span>
                          <span class="inline-flex items-center gap-1">
                            <span class="text-muted-foreground opacity-80">
                              Updated:
                            </span>
                            <span
                              class="font-medium text-foreground"
                              title={formatAbsolute(run.updatedAt)}
                            >
                              {formatRelativeAge(run.updatedAt)} ago
                            </span>
                          </span>
                          <Show when={run.completedAt}>
                            {(completedAt) => (
                              <span class="inline-flex items-center gap-1">
                                <span class="text-muted-foreground opacity-80">
                                  Completed:
                                </span>
                                <span
                                  class="font-medium text-foreground"
                                  title={formatAbsolute(completedAt())}
                                >
                                  {formatRelativeAge(completedAt())} ago
                                </span>
                              </span>
                            )}
                          </Show>
                        </div>
                      </CardDescription>
                    </CardHeader>
                    <CardContent class="p-0">
                      <TaskDetailView
                        task={run}
                        detail={runDetails()[run.runId]}
                        variant="compact"
                      />
                    </CardContent>
                  </Card>
                )}
              </For>
            </div>
          </Show>
        </Show>

        <Show when={runsError()}>
          {(message) => (
            <p class="rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
              {message()}
            </p>
          )}
        </Show>
        <Show when={detailError()}>
          {(message) => (
            <p class="rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
              {message()}
            </p>
          )}
        </Show>
      </section>
    </>
  );
}
