import { A, useSearchParams } from "@solidjs/router";
import {
  For,
  Show,
  createEffect,
  createMemo,
  createResource,
  createSignal,
} from "solid-js";
import {
  type ScheduleSummary,
  fetchQueueSchedules,
  fetchQueues,
} from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";

export default function Schedules() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [error, setError] = createSignal<string | null>(null);

  const [queues] = createResource(fetchQueues);

  const queueFilter = createMemo(
    () => (searchParams.queue as string | undefined) ?? null,
  );

  const fetcherArgs = createMemo(() => ({
    queue: queueFilter(),
    queueNames: queues()?.map((q) => q.queueName) ?? [],
  }));

  const [schedules, { refetch }] = createResource(fetcherArgs, async (args) => {
    if (args.queueNames.length === 0) return [];

    const queuesToFetch = args.queue ? [args.queue] : args.queueNames;

    const results: ScheduleSummary[] = [];
    for (const q of queuesToFetch) {
      try {
        const items = await fetchQueueSchedules(q);
        results.push(...items);
      } catch {
        // skip queues with errors
      }
    }
    return results;
  });

  createEffect(() => {
    const err = schedules.error;
    if (!err) {
      setError(null);
      return;
    }
    setError(
      err instanceof Error
        ? err.message
        : String(err ?? "Failed to load schedules."),
    );
  });

  const allSchedules = createMemo(() => {
    const items = schedules() ?? [];
    return items.length > 0 ? items : undefined;
  });

  const availableQueues = createMemo(
    () => queues()?.map((q) => q.queueName) ?? [],
  );

  return (
    <>
      <header class="flex flex-col gap-4 border-b bg-background px-6 py-6 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 class="text-2xl font-semibold tracking-tight">Schedules</h1>
          <p class="text-sm text-muted-foreground">
            Recurring task definitions and their next fire times.
          </p>
        </div>
        <div class="flex flex-col-reverse gap-3 sm:flex-row sm:items-center">
          <Show when={availableQueues().length > 1}>
            <select
              class="rounded-md border border-input bg-background px-3 py-2 text-sm"
              value={queueFilter() ?? ""}
              onChange={(e) =>
                setSearchParams({
                  queue: e.currentTarget.value || undefined,
                })
              }
            >
              <option value="">All queues</option>
              <For each={availableQueues()}>
                {(q) => <option value={q}>{q}</option>}
              </For>
            </select>
          </Show>
          <Button
            variant="outline"
            class="min-w-[96px]"
            onClick={() => refetch()}
            disabled={schedules.loading}
          >
            {schedules.loading ? "Refreshing..." : "Refresh"}
          </Button>
        </div>
      </header>

      <section class="flex-1 space-y-6 px-6 py-6">
        <Show
          when={allSchedules()}
          fallback={
            <Card>
              <CardContent>
                <p class="min-h-[160px] py-8 text-center text-sm text-muted-foreground">
                  {schedules.loading
                    ? "Loading schedules..."
                    : "No schedules have been created yet."}
                </p>
              </CardContent>
            </Card>
          }
        >
          {(items) => (
            <div class="overflow-x-auto">
              <table class="w-full text-sm">
                <thead>
                  <tr class="border-b text-left text-xs text-muted-foreground">
                    <th class="px-3 py-2 font-medium">Name</th>
                    <th class="px-3 py-2 font-medium">Task</th>
                    <th class="px-3 py-2 font-medium">Expression</th>
                    <th class="px-3 py-2 font-medium">Queue</th>
                    <th class="px-3 py-2 font-medium">Status</th>
                    <th class="px-3 py-2 font-medium">Catchup</th>
                    <th class="px-3 py-2 font-medium">Last Triggered</th>
                    <th class="px-3 py-2 font-medium">Next Run</th>
                  </tr>
                </thead>
                <tbody>
                  <For each={items()}>
                    {(schedule) => (
                      <tr class="border-b border-border/50 hover:bg-muted/50">
                        <td class="px-3 py-2.5 font-medium">
                          {schedule.scheduleName}
                        </td>
                        <td class="px-3 py-2.5">
                          <A
                            href={`/tasks?taskName=${encodeURIComponent(schedule.taskName)}&queue=${encodeURIComponent(schedule.queueName)}`}
                            class="text-primary hover:underline"
                          >
                            {schedule.taskName}
                          </A>
                        </td>
                        <td class="px-3 py-2.5">
                          <code class="rounded bg-muted px-1.5 py-0.5 text-xs">
                            {schedule.scheduleExpr}
                          </code>
                        </td>
                        <td class="px-3 py-2.5 text-muted-foreground">
                          {schedule.queueName}
                        </td>
                        <td class="px-3 py-2.5">
                          <StatusBadge enabled={schedule.enabled} />
                        </td>
                        <td class="px-3 py-2.5 text-muted-foreground">
                          {schedule.catchupPolicy}
                        </td>
                        <td class="px-3 py-2.5 text-muted-foreground">
                          {formatRelativeTime(schedule.lastTriggeredAt)}
                        </td>
                        <td class="px-3 py-2.5">
                          {formatRelativeTime(schedule.nextRunAt)}
                          <span class="ml-1 text-xs text-muted-foreground">
                            ({formatTimestamp(schedule.nextRunAt)})
                          </span>
                        </td>
                      </tr>
                    )}
                  </For>
                </tbody>
              </table>
            </div>
          )}
        </Show>
        <Show when={error()}>
          {(err) => (
            <p class="rounded-md border border-destructive/30 bg-destructive/10 p-2 text-xs text-destructive">
              {err()}
            </p>
          )}
        </Show>
      </section>
    </>
  );
}

function StatusBadge(props: { enabled: boolean }) {
  return (
    <span
      class={`inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium ${
        props.enabled
          ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-600"
          : "border-muted-foreground/30 bg-muted text-muted-foreground"
      }`}
    >
      {props.enabled ? "enabled" : "disabled"}
    </span>
  );
}

function formatTimestamp(value?: string | null): string {
  if (!value) return "\u2014";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "\u2014";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

function formatRelativeTime(value?: string | null): string {
  if (!value) return "\u2014";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "\u2014";
  const now = Date.now();
  const diff = now - date.getTime();
  if (!Number.isFinite(diff)) return "\u2014";

  const absDiff = Math.abs(diff);
  const isFuture = diff < 0;
  const minutes = Math.round(absDiff / 60000);

  if (minutes <= 0) return "just now";
  const suffix = isFuture ? "" : " ago";
  const prefix = isFuture ? "in " : "";

  if (minutes < 60) return `${prefix}${minutes}m${suffix}`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${prefix}${hours}h${suffix}`;
  const days = Math.round(hours / 24);
  return `${prefix}${days}d${suffix}`;
}
