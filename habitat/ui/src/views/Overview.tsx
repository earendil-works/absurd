import {
  createMemo,
  createResource,
  createSignal,
  For,
  Show,
  createEffect,
  onCleanup,
} from "solid-js";
import { A } from "@solidjs/router";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { type QueueMetrics, fetchMetrics } from "@/lib/api";

const REFRESH_INTERVAL_MS = 15_000;

export default function Overview() {
  const [metrics, { refetch: refetchMetrics }] =
    createResource<QueueMetrics[]>(fetchMetrics);
  const [metricsError, setMetricsError] = createSignal<string | null>(null);

  const queues = createMemo(() => metrics() ?? []);

  const stats = createMemo(() => {
    const items = queues();
    let totalMessages = 0;
    let totalPending = 0;
    let totalVisible = 0;

    for (const item of items) {
      totalMessages += item.totalMessages;
      totalPending += item.queueLength;
      totalVisible += item.queueVisibleLength;
    }

    return {
      queueCount: items.length,
      totalMessages,
      totalPending,
      totalVisible,
    };
  });

  createEffect(() => {
    const error = metrics.error;
    if (!error) {
      setMetricsError(null);
      return;
    }
    setMetricsError(error.message);
  });

  createEffect(() => {
    const timer = setInterval(() => {
      refetchMetrics();
    }, REFRESH_INTERVAL_MS);

    onCleanup(() => clearInterval(timer));
  });

  const handleRefresh = async () => {
    try {
      await refetchMetrics();
    } catch (error) {
      console.error("refresh failed", error);
    }
  };

  return (
    <>
      <header class="flex flex-col gap-4 border-b bg-background px-6 py-6 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 class="text-2xl font-semibold tracking-tight">Overview</h1>
          <p class="text-sm text-muted-foreground">
            Current queue health across your Absurd installation.
          </p>
        </div>
        <div class="flex flex-col-reverse gap-3 sm:flex-row sm:items-center">
          <div class="flex items-center gap-2">
            <Button
              variant="outline"
              class="min-w-[96px]"
              onClick={handleRefresh}
              disabled={metrics.loading}
            >
              {metrics.loading ? "Refreshing…" : "Refresh"}
            </Button>
          </div>
        </div>
      </header>

      <section class="flex-1 space-y-6 px-6 py-6">
        <div class="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <StatCard
            title="Active queues"
            value={stats().queueCount.toLocaleString()}
            description="Queues with registered metrics"
          />
          <StatCard
            title="Messages processed"
            value={stats().totalMessages.toLocaleString()}
            description="Total messages the queues have seen"
          />
          <StatCard
            title="Messages in queue"
            value={stats().totalPending.toLocaleString()}
            description="Unclaimed messages in queue storage"
          />
          <StatCard
            title="Visible right now"
            value={stats().totalVisible.toLocaleString()}
            description="Ready for immediate consumption"
          />
        </div>

        <Card>
          <CardHeader class="flex flex-row items-start justify-between gap-4">
            <div>
              <CardTitle>Queue metrics</CardTitle>
              <CardDescription>
                Visibility into backlog depth and message freshness.
              </CardDescription>
            </div>
          </CardHeader>
          <CardContent>
            <Show
              when={!metrics.loading || queues().length}
              fallback={<LoadingPlaceholder />}
            >
              <Show
                when={queues().length > 0}
                fallback={
                  <p class="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
                    No queues discovered yet. Once workers enqueue tasks,
                    metrics will appear automatically.
                  </p>
                }
              >
                <div class="overflow-x-auto">
                  <table class="min-w-full divide-y divide-border text-sm">
                    <thead>
                      <tr class="text-left text-xs uppercase text-muted-foreground">
                        <th class="px-3 py-2 font-medium">Queue</th>
                        <th class="px-3 py-2 font-medium">In Queue</th>
                        <th class="px-3 py-2 font-medium">Visible</th>
                        <th class="px-3 py-2 font-medium">Newest age</th>
                        <th class="px-3 py-2 font-medium">Oldest age</th>
                        <th class="px-3 py-2 font-medium">Total seen</th>
                        <th class="px-3 py-2 font-medium">Scraped</th>
                        <th class="px-3 py-2 font-medium text-right">
                          Actions
                        </th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-border">
                      <For each={queues()}>
                        {(queue) => (
                          <tr class="hover:bg-muted/40">
                            <td class="px-3 py-2 font-medium">
                              {queue.queueName}
                            </td>
                            <td class="px-3 py-2 tabular-nums">
                              {queue.queueLength.toLocaleString()}
                            </td>
                            <td class="px-3 py-2 tabular-nums">
                              {queue.queueVisibleLength.toLocaleString()}
                            </td>
                            <td class="px-3 py-2">
                              {formatDuration(queue.newestMsgAgeSec)}
                            </td>
                            <td class="px-3 py-2">
                              {formatDuration(queue.oldestMsgAgeSec)}
                            </td>
                            <td class="px-3 py-2 tabular-nums">
                              {queue.totalMessages.toLocaleString()}
                            </td>
                            <td class="px-3 py-2 text-xs text-muted-foreground">
                              {formatTimestamp(new Date(queue.scrapeTime))}
                            </td>
                            <td class="px-3 py-2">
                              <div class="flex justify-end gap-3 text-xs font-medium">
                                <A
                                  class="text-primary hover:underline"
                                  href={`/tasks?queue=${encodeURIComponent(queue.queueName)}`}
                                >
                                  Tasks →
                                </A>
                                <A
                                  class="text-primary hover:underline"
                                  href={`/events?queue=${encodeURIComponent(queue.queueName)}`}
                                >
                                  Events →
                                </A>
                              </div>
                            </td>
                          </tr>
                        )}
                      </For>
                    </tbody>
                  </table>
                </div>
              </Show>
            </Show>
            <Show when={metricsError()}>
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

function StatCard(props: {
  title: string;
  value: string;
  description: string;
}) {
  return (
    <Card>
      <CardHeader class="pb-2">
        <CardDescription>{props.title}</CardDescription>
        <CardTitle class="text-2xl">{props.value}</CardTitle>
      </CardHeader>
      <CardContent>
        <p class="text-xs text-muted-foreground">{props.description}</p>
      </CardContent>
    </Card>
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

function formatDuration(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value) || value < 0) {
    return "—";
  }

  const seconds = Math.floor(value);
  if (seconds < 60) {
    return `${seconds}s`;
  }

  const minutes = Math.floor(seconds / 60);
  const remainderSeconds = seconds % 60;
  if (minutes < 60) {
    return remainderSeconds
      ? `${minutes}m ${remainderSeconds}s`
      : `${minutes}m`;
  }

  const hours = Math.floor(minutes / 60);
  const remainderMinutes = minutes % 60;
  return remainderMinutes ? `${hours}h ${remainderMinutes}m` : `${hours}h`;
}

const timeFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "medium",
});

function formatTimestamp(value: Date | null): string {
  if (!value || Number.isNaN(value.getTime())) {
    return "—";
  }

  return timeFormatter.format(value);
}
