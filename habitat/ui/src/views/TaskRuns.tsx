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

  const stats = createMemo(() => {
    const items = runs();
    const totals = new Map<string, number>();
    if (items) {
      for (const item of items) {
        const status = item.status.toLowerCase();
        totals.set(status, (totals.get(status) ?? 0) + 1);
      }
    }
    return totals;
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
        <Card>
          <CardHeader>
            <CardTitle>Run Summary</CardTitle>
            <CardDescription>
              This task has {runs()?.length ?? 0} recorded run
              {runs()?.length === 1 ? "" : "s"}.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Show
              when={stats().size > 0}
              fallback={
                <p class="text-sm text-muted-foreground">
                  No runs have been recorded for this task yet.
                </p>
              }
            >
              <div class="flex flex-wrap gap-4 text-sm">
                <For each={Array.from(stats().entries())}>
                  {([status, count]) => (
                    <div class="flex items-center gap-2 rounded border px-3 py-1.5">
                      <span class="capitalize">{status}</span>
                      <span class="font-mono text-xs">{count.toString()}</span>
                    </div>
                  )}
                </For>
              </div>
            </Show>
          </CardContent>
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
