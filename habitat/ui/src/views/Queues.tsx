import { A } from "@solidjs/router";
import {
  For,
  Show,
  createEffect,
  createMemo,
  createResource,
  createSignal,
} from "solid-js";
import { type QueueSummary, fetchQueues } from "@/lib/api";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  CardFooter,
} from "@/components/ui/card";

export default function Queues() {
  const [queuesError, setQueuesError] = createSignal<string | null>(null);

  const [queues, { refetch: refetchQueues }] =
    createResource<QueueSummary[]>(fetchQueues);

  createEffect(() => {
    const error = queues.error;
    if (!error) {
      setQueuesError(null);
      return;
    }
    const message =
      error instanceof Error
        ? error.message
        : String(error ?? "Failed to load queues.");
    setQueuesError(message);
  });

  const allQueues = createMemo(() => {
    const items = queues() ?? [];
    return items.length > 0 ? items : undefined;
  });

  const handleRefresh = async () => {
    await refetchQueues();
  };

  return (
    <>
      <header class="flex flex-col gap-4 border-b bg-background px-6 py-6 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 class="text-2xl font-semibold tracking-tight">Queues</h1>
          <p class="text-sm text-muted-foreground">
            Inspect queue health, backlog contents, and recent events.
          </p>
        </div>
        <div class="flex flex-col-reverse gap-3 sm:flex-row sm:items-center">
          <div class="flex items-center gap-2">
            <Button
              variant="outline"
              class="min-w-[96px]"
              onClick={handleRefresh}
              disabled={queues.loading}
            >
              {queues.loading ? "Refreshing…" : "Refresh"}
            </Button>
          </div>
        </div>
      </header>

      <section class="flex-1 space-y-6 px-6 py-6">
        <Show
          when={allQueues()}
          fallback={
            <Card>
              <CardContent>
                <p class="min-h-[160px] py-8 text-center text-sm text-muted-foreground">
                  {queues.loading
                    ? "Loading queues…"
                    : "No queues have been registered yet."}
                </p>
              </CardContent>
            </Card>
          }
        >
          {(items) => (
            <div class="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
              <For each={items()}>
                {(queue) => (
                  <Card class="border-border/70">
                    <CardHeader class="space-y-2 pb-3 pt-4">
                      <div class="flex items-start justify-between gap-2">
                        <CardTitle class="text-base">
                          {queue.queueName}
                        </CardTitle>
                        <span class="text-xs text-muted-foreground">
                          {formatRelativeTime(queue.createdAt)}
                        </span>
                      </div>
                      <CardDescription class="text-xs">
                        Created {formatTimestamp(queue.createdAt)}
                      </CardDescription>
                    </CardHeader>
                    <CardContent class="space-y-3 pb-4">
                      <div class="grid grid-cols-2 gap-2 text-xs">
                        <StatBlock label="Pending" value={queue.pendingCount} />
                        <StatBlock label="Running" value={queue.runningCount} />
                        <StatBlock
                          label="Sleeping"
                          value={queue.sleepingCount}
                        />
                        <StatBlock
                          label="Completed"
                          value={queue.completedCount}
                        />
                        <StatBlock label="Failed" value={queue.failedCount} />
                        <StatBlock
                          label="Cancelled"
                          value={queue.cancelledCount}
                        />
                      </div>
                    </CardContent>
                    <CardFooter class="gap-3 pb-4 pt-0">
                      <A
                        class="text-xs font-medium text-primary hover:underline"
                        href={`/tasks?queue=${encodeURIComponent(queue.queueName)}`}
                      >
                        Tasks →
                      </A>
                      <A
                        class="text-xs font-medium text-primary hover:underline"
                        href={`/events?queue=${encodeURIComponent(queue.queueName)}`}
                      >
                        Events →
                      </A>
                    </CardFooter>
                  </Card>
                )}
              </For>
            </div>
          )}
        </Show>
        <Show when={queuesError()}>
          {(error) => (
            <p class="rounded-md border border-destructive/30 bg-destructive/10 p-2 text-xs text-destructive">
              {error()}
            </p>
          )}
        </Show>
      </section>
    </>
  );
}

function StatBlock(props: { label: string; value: number }) {
  return (
    <div class="rounded-md border border-border/60 p-2">
      <div class="text-[10px] uppercase text-muted-foreground">
        {props.label}
      </div>
      <div class="text-base font-semibold tabular-nums">
        {props.value.toLocaleString()}
      </div>
    </div>
  );
}

function formatTimestamp(value?: string | null): string {
  if (!value) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "—";
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

function formatRelativeTime(value?: string | null): string {
  if (!value) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "—";
  }
  const now = Date.now();
  const diff = now - date.getTime();
  if (!Number.isFinite(diff)) {
    return "—";
  }
  const minutes = Math.round(diff / 60000);
  if (minutes <= 0) {
    return "just now";
  }
  if (minutes < 60) {
    return `${minutes}m ago`;
  }
  const hours = Math.round(minutes / 60);
  if (hours < 24) {
    return `${hours}h ago`;
  }
  const days = Math.round(hours / 24);
  return `${days}d ago`;
}
