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
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { TaskDetailView } from "@/components/TaskDetailView";
import { TaskStatusBadge } from "@/components/TaskStatusBadge";
import { IdDisplay } from "@/components/IdDisplay";
import { RelativeTimestamp } from "@/components/Timestamp";
import {
  APIError,
  type TaskDetail,
  type TaskSummary,
  fetchTask,
  fetchTasks,
  retryTask,
} from "@/lib/api";

export default function TaskRuns() {
  const params = useParams<{ taskId: string }>();
  const taskId = () => params.taskId;

  const [runsError, setRunsError] = createSignal<string | null>(null);
  const [detailError, setDetailError] = createSignal<string | null>(null);
  const [retryError, setRetryError] = createSignal<string | null>(null);
  const [retrySuccess, setRetrySuccess] = createSignal<string | null>(null);
  const [retryInFlight, setRetryInFlight] = createSignal<
    "inplace" | "spawn" | null
  >(null);
  const [retryDialogOpen, setRetryDialogOpen] = createSignal(false);
  const [retrySpawnNewTask, setRetrySpawnNewTask] = createSignal(false);
  const [retryMaxAttemptsInput, setRetryMaxAttemptsInput] = createSignal("");
  const [retryDialogError, setRetryDialogError] = createSignal<string | null>(
    null,
  );
  const [runDetails, setRunDetails] = createSignal<Record<string, TaskDetail>>(
    {},
  );

  const RUNS_PAGE_SIZE = 200;
  const MAX_RUNS_PAGE_FETCH = 1000;

  const fetchRunsForTask = async (id: string): Promise<TaskSummary[]> => {
    if (!id) {
      return [];
    }

    let currentPage = 1;
    const perPage = RUNS_PAGE_SIZE;
    let hasMore = true;
    let results: TaskSummary[] = [];

    while (hasMore) {
      if (currentPage > MAX_RUNS_PAGE_FETCH) {
        throw new Error(
          `Task run history exceeds ${MAX_RUNS_PAGE_FETCH} pages (${MAX_RUNS_PAGE_FETCH * perPage} runs). Refine filters or use direct database queries for full history.`,
        );
      }

      const pageResult = await fetchTasks({
        taskId: id,
        page: currentPage,
        perPage,
      });
      results = results.concat(pageResult.items);

      hasMore = pageResult.hasMore;
      if (hasMore && pageResult.items.length === 0) {
        throw new Error(
          "Task run pagination stalled: backend reported more pages but returned no rows.",
        );
      }

      currentPage += 1;
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
    setRetryError(null);
    setRetrySuccess(null);
    setRetryInFlight(null);
    setRetryDialogOpen(false);
    setRetrySpawnNewTask(false);
    setRetryMaxAttemptsInput("");
    setRetryDialogError(null);
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

  const latestRun = createMemo(() => {
    const items = runs();
    if (!items || items.length === 0) {
      return null;
    }
    return items[0];
  });

  const canRetry = createMemo(() => {
    const latest = latestRun();
    if (!latest) {
      return false;
    }
    return latest.status.toLowerCase() === "failed";
  });

  const retryFieldLabel = createMemo(() =>
    retrySpawnNewTask() ? "Max attempts" : "Extra attempts",
  );

  const retryDefaultValue = createMemo(() => {
    const latest = latestRun();
    if (!latest) {
      return 1;
    }
    if (retrySpawnNewTask()) {
      return latest.maxAttempts ?? latest.attempt;
    }
    return 1;
  });

  const retryFieldHelper = createMemo(() => {
    const latest = latestRun();
    const currentAttempt = latest?.attempt ?? 1;
    if (retrySpawnNewTask()) {
      return `Total max attempts for the new task. Defaults to ${retryDefaultValue()}.`;
    }
    return `Added to current attempts (${currentAttempt}). Defaults to 1.`;
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

  const latestUpdatedAt = createMemo(() => {
    const items = orderedRuns();
    if (items.length === 0) {
      return null;
    }

    let latest = Number.NEGATIVE_INFINITY;
    for (const run of items) {
      const updated = Date.parse(run.updatedAt);
      if (!Number.isNaN(updated) && updated > latest) {
        latest = updated;
      }
    }

    return Number.isFinite(latest) ? new Date(latest).toISOString() : null;
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

  const openRetryDialog = () => {
    setRetryError(null);
    setRetrySuccess(null);
    setRetryDialogError(null);
    setRetrySpawnNewTask(false);
    setRetryMaxAttemptsInput("");
    setRetryDialogOpen(true);
  };

  const handleRetry = async () => {
    const latest = latestRun();
    if (!latest) {
      setRetryError("Task metadata is not loaded yet.");
      return;
    }
    if (!canRetry()) {
      setRetryError("Only failed tasks can be retried.");
      return;
    }

    const rawAttempts = retryMaxAttemptsInput().trim();
    let parsedAttempts: number | undefined;
    if (rawAttempts !== "") {
      if (!/^[1-9]\d*$/.test(rawAttempts)) {
        setRetryDialogError(`${retryFieldLabel()} must be an integer >= 1.`);
        return;
      }
      parsedAttempts = Number(rawAttempts);
    }

    setRetryError(null);
    setRetrySuccess(null);
    setRetryDialogError(null);
    setRetryInFlight(retrySpawnNewTask() ? "spawn" : "inplace");

    try {
      const payload = {
        taskId: latest.taskId,
        queueName: latest.queueName,
        spawnNewTask: retrySpawnNewTask(),
        ...(retrySpawnNewTask()
          ? parsedAttempts !== undefined
            ? { maxAttempts: parsedAttempts }
            : {}
          : parsedAttempts !== undefined
            ? { extraAttempts: parsedAttempts }
            : {}),
      };

      const result = await retryTask(payload);

      if (result.created) {
        setRetrySuccess(
          `Spawned new task ${result.taskId} (attempt ${result.attempt}).`,
        );
      } else {
        setRetrySuccess(
          `Retried task ${result.taskId} on attempt ${result.attempt}.`,
        );
      }

      setRetryDialogOpen(false);
      await handleRefresh();
    } catch (error) {
      if (error instanceof APIError) {
        setRetryError(error.message);
      } else {
        setRetryError("Failed to retry task.");
      }
    } finally {
      setRetryInFlight(null);
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
          <Show when={canRetry()}>
            <Button
              variant="secondary"
              onClick={openRetryDialog}
              class="min-w-[100px]"
              disabled={runs.loading || retryInFlight() !== null}
            >
              Retry
            </Button>
          </Show>
          <Button
            variant="outline"
            onClick={handleRefresh}
            class="min-w-[96px]"
            disabled={runs.loading || retryInFlight() !== null}
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
                <RelativeTimestamp
                  class="font-medium text-foreground"
                  value={latestUpdatedAt()}
                  variant="long"
                />
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
                            <RelativeTimestamp
                              class="font-medium text-foreground"
                              value={run.createdAt}
                              variant="long"
                            />
                          </span>
                          <span class="inline-flex items-center gap-1">
                            <span class="text-muted-foreground opacity-80">
                              Updated:
                            </span>
                            <RelativeTimestamp
                              class="font-medium text-foreground"
                              value={run.updatedAt}
                              variant="long"
                            />
                          </span>
                          <Show when={run.completedAt}>
                            {(completedAt) => (
                              <span class="inline-flex items-center gap-1">
                                <span class="text-muted-foreground opacity-80">
                                  Completed:
                                </span>
                                <RelativeTimestamp
                                  class="font-medium text-foreground"
                                  value={completedAt()}
                                  variant="long"
                                />
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
        <Show when={retryError()}>
          {(message) => (
            <p class="rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
              {message()}
            </p>
          )}
        </Show>
        <Show when={retrySuccess()}>
          {(message) => (
            <p class="rounded-md border border-emerald-500/30 bg-emerald-500/10 p-3 text-sm text-emerald-700 dark:text-emerald-300">
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

      <Dialog
        open={retryDialogOpen()}
        onOpenChange={(open) => {
          setRetryDialogOpen(open);
          if (!open) {
            setRetryDialogError(null);
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Retry failed task</DialogTitle>
            <DialogDescription>
              Choose how you want to retry this task. You can retry in place or
              spawn a new task using the original inputs.
            </DialogDescription>
          </DialogHeader>

          <div class="space-y-4">
            <label class="flex items-start gap-2 text-sm">
              <input
                type="checkbox"
                class="mt-0.5"
                checked={retrySpawnNewTask()}
                onInput={(event) => {
                  const checked = event.currentTarget.checked;
                  setRetrySpawnNewTask(checked);
                  setRetryMaxAttemptsInput("");
                }}
              />
              <span>
                <span class="font-medium">Spawn as new task</span>
                <span class="block text-xs text-muted-foreground">
                  Creates a brand new task with copied inputs instead of
                  extending the existing failed task.
                </span>
              </span>
            </label>

            <div class="space-y-1">
              <label class="text-sm font-medium" for="retry-max-attempts">
                {retryFieldLabel()}
              </label>
              <input
                id="retry-max-attempts"
                type="number"
                min="1"
                step="1"
                value={retryMaxAttemptsInput()}
                onInput={(event) =>
                  setRetryMaxAttemptsInput(event.currentTarget.value)
                }
                placeholder={String(retryDefaultValue())}
                class="w-full rounded-md border bg-background px-3 py-2 text-sm"
              />
              <p class="text-xs text-muted-foreground">{retryFieldHelper()}</p>
            </div>

            <Show when={retryDialogError()}>
              {(message) => (
                <p class="rounded-md border border-destructive/30 bg-destructive/10 p-2 text-sm text-destructive">
                  {message()}
                </p>
              )}
            </Show>
          </div>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setRetryDialogOpen(false)}
              disabled={retryInFlight() !== null}
            >
              Cancel
            </Button>
            <Button
              variant="secondary"
              onClick={handleRetry}
              disabled={retryInFlight() !== null}
            >
              {retryInFlight() !== null ? "Retrying…" : "Retry"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
