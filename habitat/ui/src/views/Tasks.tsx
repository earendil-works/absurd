import {
  createMemo,
  createResource,
  createSignal,
  For,
  Show,
  createEffect,
  onCleanup,
} from "solid-js";
import { createStore, reconcile } from "solid-js/store";
import { useSearchParams, type NavigateOptions } from "@solidjs/router";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import {
  type TaskDetail,
  type TaskSummary,
  fetchTasks,
  fetchTask,
} from "@/lib/api";
import { TaskStatusBadge } from "@/components/TaskStatusBadge";
import { IdDisplay } from "@/components/IdDisplay";
import { AutoRefreshToggle } from "@/components/AutoRefreshToggle";
import { TaskDetailView } from "@/components/TaskDetailView";
import { Highlight } from "@/components/Highlight";
import {
  TextField,
  TextFieldLabel,
  TextFieldRoot,
} from "@/components/ui/textfield";
import {
  Combobox,
  ComboboxContent,
  ComboboxItem,
  ComboboxInput,
  ComboboxTrigger,
} from "@/components/ui/combobox";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Pagination,
  PaginationEllipsis,
  PaginationItem,
  PaginationItems,
  PaginationNext,
  PaginationPrevious,
} from "@/components/ui/pagination";

const REFRESH_INTERVAL_MS = 15_000;
const PAGE_SIZE = 25;

interface FilterOption {
  label: string;
  value: string;
}

function buildFilterOptions(
  values: string[],
  allLabel: string,
): FilterOption[] {
  const uniqueValues = Array.from(new Set(values)).filter(
    (value) => value.trim() !== "",
  );
  return [
    { label: allLabel, value: "" },
    ...uniqueValues.map((value) => ({ label: value, value })),
  ];
}

function resolveSelectedOption(
  options: FilterOption[],
  value: string | null,
): FilterOption {
  if (!options.length) {
    return { label: "", value: "" };
  }

  if (!value || value.trim() === "") {
    return options[0];
  }

  return options.find((option) => option.value === value) ?? options[0];
}

function findParamsMatch(params: any, search: string): string | null {
  console.log("[DEBUG] findParamsMatch called:", { params, search });
  if (!search || !params) {
    console.log(
      "[DEBUG] findParamsMatch: early return, search or params missing",
    );
    return null;
  }
  const paramsStr = JSON.stringify(params);
  console.log("[DEBUG] findParamsMatch: paramsStr=", paramsStr);
  const lowerParams = paramsStr.toLowerCase();
  const lowerSearch = search.toLowerCase();
  const index = lowerParams.indexOf(lowerSearch);
  console.log("[DEBUG] findParamsMatch: index=", index);
  if (index === -1) return null;

  // Extract a snippet around the match (30 chars before and after)
  const contextSize = 30;
  const start = Math.max(0, index - contextSize);
  const end = Math.min(paramsStr.length, index + search.length + contextSize);
  let snippet = paramsStr.slice(start, end);
  if (start > 0) snippet = "..." + snippet;
  if (end < paramsStr.length) snippet = snippet + "...";
  console.log("[DEBUG] findParamsMatch: returning snippet=", snippet);
  return snippet;
}

export default function Tasks() {
  const [searchParams, setSearchParams] = useSearchParams();

  const getParam = (key: string) => searchParams[key] as string | undefined;

  const normalizeNullableParam = (value: string | undefined): string | null => {
    if (value === undefined) {
      return null;
    }
    return value.trim().length === 0 ? null : value;
  };

  const parsePageParam = (value: string | undefined): number => {
    const parsed = Number.parseInt(value ?? "", 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 1;
  };

  const [searchTerm, setSearchTerm] = createSignal(getParam("search") ?? "");
  const [searchInput, setSearchInput] = createSignal(getParam("search") ?? "");
  const [queueFilter, setQueueFilter] = createSignal<string | null>(
    normalizeNullableParam(getParam("queue")),
  );
  const [statusFilter, setStatusFilter] = createSignal<string | null>(
    normalizeNullableParam(getParam("status")),
  );
  const [taskNameFilter, setTaskNameFilter] = createSignal<string | null>(
    normalizeNullableParam(getParam("taskName")),
  );
  const [page, setPage] = createSignal(parsePageParam(getParam("page")));

  const toParamValue = (value: string | null | undefined) => {
    if (value == null) {
      return undefined;
    }
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : undefined;
  };

  const syncSearchParams = (
    updates: Partial<{
      search: string | null;
      queue: string | null;
      status: string | null;
      taskName: string | null;
      page: number | null;
    }>,
    options?: Partial<NavigateOptions>,
  ) => {
    const payload: Record<string, string | undefined> = {};

    if ("search" in updates) {
      payload.search = toParamValue(updates.search);
    }
    if ("queue" in updates) {
      payload.queue = toParamValue(updates.queue);
    }
    if ("status" in updates) {
      payload.status = toParamValue(updates.status);
    }
    if ("taskName" in updates) {
      payload.taskName = toParamValue(updates.taskName);
    }
    if ("page" in updates) {
      const value = updates.page;
      payload.page =
        value != null && value > 1 ? String(Math.floor(value)) : undefined;
    }

    if (Object.keys(payload).length > 0) {
      setSearchParams(payload, options);
    }
  };

  createEffect(() => {
    const nextSearch = getParam("search") ?? "";
    if (nextSearch !== searchTerm()) {
      setSearchTerm(nextSearch);
      setSearchInput(nextSearch);
    }

    const nextQueue = normalizeNullableParam(getParam("queue"));
    if (nextQueue !== queueFilter()) {
      setQueueFilter(nextQueue);
    }

    const nextStatus = normalizeNullableParam(getParam("status"));
    if (nextStatus !== statusFilter()) {
      setStatusFilter(nextStatus);
    }

    const nextTaskName = normalizeNullableParam(getParam("taskName"));
    if (nextTaskName !== taskNameFilter()) {
      setTaskNameFilter(nextTaskName);
    }

    const nextPage = parsePageParam(getParam("page"));
    if (nextPage !== page()) {
      setPage(nextPage);
    }
  });

  const filters = createMemo(() => ({
    search: searchTerm(),
    queue: queueFilter(),
    status: statusFilter(),
    taskName: taskNameFilter(),
    page: page(),
    perPage: PAGE_SIZE,
  }));

  const [taskList, { refetch: refetchTasks }] = createResource(
    filters,
    fetchTasks,
  );
  const [tasksError, setTasksError] = createSignal<string | null>(null);
  const [expandedRunId, setExpandedRunId] = createSignal<string | null>(null);
  const [autoRefreshEnabled, setAutoRefreshEnabled] = createSignal(true);
  const [taskDetails, setTaskDetails] = createSignal<
    Record<string, TaskDetail>
  >({});

  // Use a store with reconcile for fine-grained updates - only changed items re-render
  const [tasks, setTasks] = createStore<{ items: TaskSummary[] }>({
    items: [],
  });

  // Reconcile tasks when taskList changes - this diffs by runId
  createEffect(() => {
    const newItems = taskList()?.items ?? [];
    setTasks("items", reconcile(newItems, { key: "runId" }));
  });

  const allTasks = () => tasks.items;
  const totalTasks = createMemo(() => taskList()?.total ?? 0);
  const totalPages = createMemo(() => {
    const total = totalTasks();
    if (!total) {
      return 1;
    }
    return Math.max(1, Math.ceil(total / PAGE_SIZE));
  });
  const showPagination = createMemo(() => totalTasks() > PAGE_SIZE);
  const queueOptions = createMemo(() =>
    buildFilterOptions(taskList()?.availableQueues ?? [], "All queues"),
  );
  const statusOptions = createMemo(() =>
    buildFilterOptions(taskList()?.availableStatuses ?? [], "All statuses"),
  );
  const taskNameOptions = createMemo(() =>
    buildFilterOptions(taskList()?.availableTaskNames ?? [], "All task names"),
  );
  const selectedQueueOption = createMemo(() =>
    resolveSelectedOption(queueOptions(), queueFilter()),
  );
  const selectedStatusOption = createMemo(() =>
    resolveSelectedOption(statusOptions(), statusFilter()),
  );
  const selectedTaskNameOption = createMemo(() =>
    resolveSelectedOption(taskNameOptions(), taskNameFilter()),
  );
  const pageStart = createMemo(() => {
    if (!totalTasks()) {
      return 0;
    }
    return (page() - 1) * PAGE_SIZE + 1;
  });
  const pageEnd = createMemo(() => {
    if (!totalTasks()) {
      return 0;
    }
    return Math.min(totalTasks(), pageStart() + allTasks().length - 1);
  });

  const renderQueueOption = (props: { item: any }) => {
    const option = () => props.item.rawValue as FilterOption;
    return <ComboboxItem item={props.item}>{option().label}</ComboboxItem>;
  };

  const renderSelectOption = (props: { item: any }) => {
    const option = () => props.item.rawValue as FilterOption;
    return <SelectItem item={props.item}>{option().label}</SelectItem>;
  };

  createEffect(() => {
    const maxPage = totalPages();
    if (page() > maxPage) {
      const normalized = Math.max(1, maxPage);
      setPage(normalized);
      syncSearchParams({ page: normalized }, { replace: true });
    }
  });

  createEffect(() => {
    const error = taskList.error;
    if (!error) {
      setTasksError(null);
      return;
    }

    setTasksError(error.message);
  });

  createEffect(() => {
    if (!autoRefreshEnabled()) {
      return;
    }

    const timer = setInterval(() => {
      refetchTasks();
    }, REFRESH_INTERVAL_MS);

    onCleanup(() => clearInterval(timer));
  });

  const handleRefresh = async () => {
    try {
      await refetchTasks();
    } catch (error) {
      console.error("refresh failed", error);
    }
  };

  const handleRowClick = async (runId: string) => {
    if (expandedRunId() === runId) {
      setExpandedRunId(null);
      return;
    }

    setExpandedRunId(runId);

    // Fetch task details if not already loaded
    if (!taskDetails()[runId]) {
      try {
        const detail = await fetchTask(runId);
        setTaskDetails({ ...taskDetails(), [runId]: detail });
      } catch (error) {
        console.error("Failed to fetch task details:", error);
      }
    }
  };

  const formatAge = (timestamp: string): string => {
    const date = new Date(timestamp);
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

  return (
    <>
      <header class="flex flex-col gap-4 border-b bg-background px-6 py-6 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 class="text-2xl font-semibold tracking-tight">Tasks</h1>
          <p class="text-sm text-muted-foreground">
            Monitor and manage durable tasks across all queues.
          </p>
        </div>
        <div class="flex flex-col-reverse gap-3 sm:flex-row sm:items-center">
          <div class="flex items-center gap-2">
            <AutoRefreshToggle onToggle={setAutoRefreshEnabled} />
            <Button
              variant="outline"
              class="min-w-[96px]"
              onClick={handleRefresh}
              disabled={taskList.loading}
            >
              {taskList.loading ? "Refreshing…" : "Refresh"}
            </Button>
          </div>
        </div>
      </header>

      <section class="flex-1 space-y-6 px-6 py-6">
        <Card>
          <CardHeader>
            <CardTitle>Task Runs</CardTitle>
            <CardDescription>
              Each row represents a single run. Click a run to view details or
              open the full task history.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div class="mb-6 space-y-4">
              <div class="grid grid-cols-2 gap-3 lg:grid-cols-4">
                <TextFieldRoot>
                  <TextFieldLabel>Search</TextFieldLabel>
                  <TextField
                    value={searchInput()}
                    onInput={(event) => {
                      setSearchInput(event.currentTarget.value);
                    }}
                    onKeyDown={(event) => {
                      if (event.key === "Enter") {
                        const value = searchInput();
                        setSearchTerm(value);
                        if (page() !== 1) {
                          setPage(1);
                        }
                        syncSearchParams(
                          { search: value, page: 1 },
                          { replace: true },
                        );
                      }
                    }}
                    placeholder="Search IDs, names, queue, or params... (Enter to search)"
                  />
                </TextFieldRoot>
                <div class="space-y-1">
                  <span class="text-sm font-medium text-foreground">Queue</span>
                  <Combobox
                    multiple={false}
                    options={queueOptions()}
                    optionLabel={(option: FilterOption) => option.label}
                    optionValue={(option: FilterOption) => option.value}
                    optionTextValue={(option: FilterOption) => option.label}
                    value={selectedQueueOption()}
                    onChange={(option) => {
                      const nextValue = option?.value ? option.value : null;
                      if (nextValue === queueFilter()) {
                        return;
                      }

                      setQueueFilter(nextValue);
                      if (page() !== 1) {
                        setPage(1);
                      }
                      syncSearchParams({ queue: nextValue, page: 1 });
                    }}
                    itemComponent={renderQueueOption}
                    defaultFilter="contains"
                    disallowEmptySelection={false}
                    placeholder="All queues"
                    aria-label="Queue filter"
                  >
                    <ComboboxTrigger>
                      <ComboboxInput placeholder="All queues" />
                    </ComboboxTrigger>
                    <ComboboxContent />
                  </Combobox>
                </div>
                <div class="space-y-1">
                  <span class="text-sm font-medium text-foreground">
                    Status
                  </span>
                  <Select
                    multiple={false}
                    options={statusOptions()}
                    optionValue={(option: FilterOption) => option.value}
                    optionTextValue={(option: FilterOption) => option.label}
                    value={selectedStatusOption()}
                    onChange={(option) => {
                      const nextValue = option?.value ? option.value : null;
                      if (nextValue === statusFilter()) {
                        return;
                      }

                      setStatusFilter(nextValue);
                      if (page() !== 1) {
                        setPage(1);
                      }
                      syncSearchParams({ status: nextValue, page: 1 });
                    }}
                    itemComponent={renderSelectOption}
                    placeholder="All statuses"
                    aria-label="Status filter"
                  >
                    <SelectTrigger>
                      <SelectValue>
                        {(state) => {
                          const option = state.selectedOption() as
                            | FilterOption
                            | undefined;
                          return (
                            option?.label ??
                            selectedStatusOption()?.label ??
                            "All statuses"
                          );
                        }}
                      </SelectValue>
                    </SelectTrigger>
                    <SelectContent />
                  </Select>
                </div>
                <div class="space-y-1">
                  <span class="text-sm font-medium text-foreground">
                    Task name
                  </span>
                  <Select
                    multiple={false}
                    options={taskNameOptions()}
                    optionValue={(option: FilterOption) => option.value}
                    optionTextValue={(option: FilterOption) => option.label}
                    value={selectedTaskNameOption()}
                    onChange={(option) => {
                      const nextValue = option?.value ? option.value : null;
                      if (nextValue === taskNameFilter()) {
                        return;
                      }

                      setTaskNameFilter(nextValue);
                      if (page() !== 1) {
                        setPage(1);
                      }
                      syncSearchParams({ taskName: nextValue, page: 1 });
                    }}
                    itemComponent={renderSelectOption}
                    placeholder="All task names"
                    aria-label="Task name filter"
                  >
                    <SelectTrigger>
                      <SelectValue>
                        {(state) => {
                          const option = state.selectedOption() as
                            | FilterOption
                            | undefined;
                          return (
                            option?.label ??
                            selectedTaskNameOption()?.label ??
                            "All task names"
                          );
                        }}
                      </SelectValue>
                    </SelectTrigger>
                    <SelectContent />
                  </Select>
                </div>
              </div>
              <div class="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
                <Show when={totalTasks() > 0}>
                  <span>
                    Showing {pageStart()}–{pageEnd()} of {totalTasks()} task
                    {totalTasks() === 1 ? "" : "s"}
                  </span>
                </Show>
                <Show when={totalTasks() === 0 && !taskList.loading}>
                  <span>No tasks match the current filters.</span>
                </Show>
              </div>
            </div>
            <Show
              when={!taskList.loading || allTasks().length}
              fallback={<LoadingPlaceholder />}
            >
              <Show
                when={allTasks().length > 0}
                fallback={
                  <p class="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
                    No tasks found. Adjust your filters or check back once
                    workers enqueue tasks.
                  </p>
                }
              >
                <div class="overflow-x-auto">
                  <table class="min-w-full divide-y divide-border text-sm">
                    <thead>
                      <tr class="text-left text-xs uppercase text-muted-foreground">
                        <th class="px-3 py-2 font-medium">Task ID</th>
                        <th class="px-3 py-2 font-medium">Task Name</th>
                        <th class="px-3 py-2 font-medium">Queue</th>
                        <th class="px-3 py-2 font-medium">Status</th>
                        <th class="px-3 py-2 font-medium">Attempt</th>
                        <th class="px-3 py-2 font-medium">Run ID</th>
                        <th class="px-3 py-2 font-medium">Age</th>
                        <th class="px-3 py-2 font-medium w-10"></th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-border">
                      <For each={allTasks()}>
                        {(task) => (
                          <>
                            <tr
                              class="hover:bg-muted/40 cursor-pointer"
                              onClick={() => handleRowClick(task.runId)}
                            >
                              <td class="px-3 py-2">
                                <IdDisplay value={task.taskId} />
                              </td>
                              <td class="px-3 py-2 font-medium">
                                <Highlight
                                  text={task.taskName}
                                  search={searchTerm()}
                                />
                              </td>
                              <td class="px-3 py-2">
                                <Highlight
                                  text={task.queueName}
                                  search={searchTerm()}
                                />
                              </td>
                              <td class="px-3 py-2">
                                <TaskStatusBadge status={task.status} />
                              </td>
                              <td class="px-3 py-2 tabular-nums">
                                {task.attempt}
                                {task.maxAttempts
                                  ? ` / ${task.maxAttempts}`
                                  : " / ∞"}
                              </td>
                              <td class="px-3 py-2">
                                <IdDisplay value={task.runId} />
                              </td>
                              <td class="px-3 py-2">
                                {formatAge(task.createdAt)}
                              </td>
                              <td class="px-3 py-2 text-center">
                                <span class="text-muted-foreground">
                                  {expandedRunId() === task.runId ? "▲" : "▼"}
                                </span>
                              </td>
                            </tr>
                            <Show
                              when={findParamsMatch(task.params, searchTerm())}
                            >
                              {(match) => (
                                <tr class="bg-yellow-50 dark:bg-yellow-900/20">
                                  <td colspan="8" class="px-3 py-1">
                                    <span class="text-xs text-muted-foreground">
                                      Match in params:{" "}
                                    </span>
                                    <code class="text-xs font-mono">
                                      <Highlight
                                        text={match()}
                                        search={searchTerm()}
                                      />
                                    </code>
                                  </td>
                                </tr>
                              )}
                            </Show>
                            <Show when={expandedRunId() === task.runId}>
                              <tr>
                                <td colspan="8" class="bg-muted/20 p-0">
                                  <div class="animate-slide-down">
                                    <TaskDetailView
                                      task={task}
                                      detail={taskDetails()[task.runId]}
                                      taskLink={`/tasks/${task.taskId}`}
                                    />
                                  </div>
                                </td>
                              </tr>
                            </Show>
                          </>
                        )}
                      </For>
                    </tbody>
                  </table>
                </div>
              </Show>
            </Show>
            <Show when={showPagination()}>
              <Pagination
                class="mt-4"
                count={totalPages()}
                page={page()}
                disabled={taskList.loading}
                onPageChange={(nextPage) => {
                  if (nextPage !== page()) {
                    setPage(nextPage);
                  }
                  syncSearchParams({ page: nextPage });
                }}
                itemComponent={PaginationItem}
                ellipsisComponent={() => <PaginationEllipsis />}
              >
                <PaginationPrevious aria-label="Previous page" />
                <PaginationItems />
                <PaginationNext aria-label="Next page" />
              </Pagination>
            </Show>
            <Show when={tasksError()}>
              {(error) => (
                <p class="mt-4 rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
                  {error()}
                </p>
              )}
            </Show>
          </CardContent>
        </Card>
      </section>
    </>
  );
}

function LoadingPlaceholder() {
  return (
    <div class="space-y-3">
      <div class="h-4 w-1/3 rounded bg-muted animate-pulse" />
      <div class="h-10 rounded bg-muted animate-pulse" />
      <div class="h-10 rounded bg-muted animate-pulse" />
      <div class="h-10 rounded bg-muted animate-pulse" />
    </div>
  );
}
