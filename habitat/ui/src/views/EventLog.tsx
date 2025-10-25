import {
  For,
  Show,
  createEffect,
  createMemo,
  createResource,
  createSignal,
} from "solid-js";
import {
  type QueueSummary,
  type QueueEvent,
  fetchQueues,
  fetchEvents,
} from "@/lib/api";
import { useSearchParams, type NavigateOptions } from "@solidjs/router";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  TextField,
  TextFieldLabel,
  TextFieldRoot,
} from "@/components/ui/textfield";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { JSONViewer } from "@/components/JSONViewer";

export default function EventLog() {
  const [searchParams, setSearchParams] = useSearchParams();

  const getParam = (key: string) => searchParams[key] as string | undefined;

  const normalizeNullableParam = (value: string | undefined): string | null => {
    if (value === undefined) {
      return null;
    }
    const trimmed = value.trim();
    return trimmed.length === 0 ? null : trimmed;
  };

  const [queueFilter, setQueueFilter] = createSignal<string | null>(
    normalizeNullableParam(getParam("queue")),
  );
  const [eventNameFilter, setEventNameFilter] = createSignal<string>(
    getParam("eventName") ?? "",
  );

  const [queues, { refetch: refetchQueues }] =
    createResource<QueueSummary[]>(fetchQueues);
  const [queuesError, setQueuesError] = createSignal<string | null>(null);
  const [eventsError, setEventsError] = createSignal<string | null>(null);

  const toParamValue = (value: string | null | undefined) => {
    if (value == null) {
      return undefined;
    }
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : undefined;
  };

  const syncSearchParams = (
    updates: Partial<{ queue: string | null; eventName: string | null }>,
    options?: Partial<NavigateOptions>,
  ) => {
    const payload: Record<string, string | undefined> = {};

    if ("queue" in updates) {
      payload.queue = toParamValue(updates.queue ?? null);
    }
    if ("eventName" in updates) {
      payload.eventName = toParamValue(updates.eventName ?? null);
    }

    if (Object.keys(payload).length > 0) {
      setSearchParams(payload, options);
    }
  };

  createEffect(() => {
    const nextQueue = normalizeNullableParam(getParam("queue"));
    if (nextQueue !== queueFilter()) {
      setQueueFilter(nextQueue);
    }

    const nextEventName = getParam("eventName") ?? "";
    if (nextEventName !== eventNameFilter()) {
      setEventNameFilter(nextEventName);
    }
  });

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

  const filters = createMemo(() => {
    const queue = queueFilter();
    const eventName = eventNameFilter().trim();
    return {
      queue: queue && queue.length > 0 ? queue : null,
      eventName: eventName.length > 0 ? eventName : null,
    } as const;
  });

  const [events, { refetch: refetchEvents }] = createResource<
    QueueEvent[],
    ReturnType<typeof filters>
  >(filters, async (input) => {
    return fetchEvents({
      queue: input.queue,
      eventName: input.eventName,
    });
  });

  createEffect(() => {
    const error = events.error;
    if (!error) {
      setEventsError(null);
      return;
    }
    const message =
      error instanceof Error
        ? error.message
        : String(error ?? "Failed to load events.");
    setEventsError(message);
  });

  const handleRefresh = async () => {
    await refetchQueues();
    await refetchEvents();
  };

  const queueOptions = createMemo(() =>
    (queues() ?? [])
      .map((queue) => queue.queueName)
      .sort((a, b) => a.localeCompare(b)),
  );

  type FilterOption = { label: string; value: string };

  const queueFilterOptions = createMemo<FilterOption[]>(() => {
    const names = queueOptions();
    return [
      { label: "All queues", value: "" },
      ...names.map((name) => ({ label: name, value: name })),
    ];
  });

  const selectedQueueOption = createMemo<FilterOption>(() => {
    const options = queueFilterOptions();
    const value = queueFilter() ?? "";
    return options.find((option) => option.value === value) ?? options[0];
  });

  const renderSelectOption = (props: { item: any }) => {
    const option = () => props.item.rawValue as FilterOption;
    return <SelectItem item={props.item}>{option().label}</SelectItem>;
  };

  return (
    <>
      <header class="flex flex-col gap-4 border-b bg-background px-6 py-6 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 class="text-2xl font-semibold tracking-tight">Event log</h1>
          <p class="text-sm text-muted-foreground">
            Inspect emitted events across queues with filterable payloads.
          </p>
        </div>
        <div class="flex flex-col-reverse gap-3 sm:flex-row sm:items-center">
          <div class="flex items-center gap-2">
            <Button
              variant="outline"
              class="min-w-[96px]"
              onClick={handleRefresh}
              disabled={events.loading || queues.loading}
            >
              {events.loading || queues.loading ? "Refreshing…" : "Refresh"}
            </Button>
          </div>
        </div>
      </header>

      <section class="flex-1 space-y-6 px-6 py-6">
        <Card>
          <CardHeader class="pb-2">
            <CardTitle>Filters</CardTitle>
            <CardDescription>
              Narrow the event list by queue and event name.
            </CardDescription>
          </CardHeader>
          <CardContent class="space-y-4">
            <div class="grid gap-4 md:grid-cols-2">
              <div class="space-y-2">
                <TextFieldRoot>
                  <TextFieldLabel>Event name</TextFieldLabel>
                  <TextField
                    type="text"
                    placeholder="payment.completed"
                    value={eventNameFilter()}
                    onInput={(event) => {
                      const value = event.currentTarget.value;
                      if (value === eventNameFilter()) {
                        return;
                      }
                      setEventNameFilter(value);
                      syncSearchParams({ eventName: value }, { replace: true });
                    }}
                  />
                </TextFieldRoot>
              </div>
              <div class="space-y-2">
                <p class="text-xs font-medium uppercase text-muted-foreground">
                  Queue
                </p>
                <Select
                  multiple={false}
                  options={queueFilterOptions()}
                  optionValue={(option: FilterOption) => option.value}
                  optionTextValue={(option: FilterOption) => option.label}
                  value={selectedQueueOption()}
                  onChange={(option) => {
                    const nextValue = option?.value ?? "";
                    const normalized =
                      nextValue.trim().length > 0 ? nextValue : null;
                    if (normalized === queueFilter()) {
                      return;
                    }
                    setQueueFilter(normalized);
                    syncSearchParams({ queue: normalized }, { replace: true });
                  }}
                  itemComponent={renderSelectOption}
                  placeholder="All queues"
                  aria-label="Queue filter"
                >
                  <SelectTrigger>
                    <SelectValue>
                      {(state) => {
                        const option = state.selectedOption() as
                          | FilterOption
                          | undefined;
                        return option?.label ?? selectedQueueOption().label;
                      }}
                    </SelectValue>
                  </SelectTrigger>
                  <SelectContent />
                </Select>
              </div>
            </div>
            <Show when={queuesError()}>
              {(error) => (
                <p class="rounded-md border border-destructive/30 bg-destructive/10 p-2 text-xs text-destructive">
                  {error()}
                </p>
              )}
            </Show>
          </CardContent>
        </Card>

        <Card>
          <CardHeader class="pb-2">
            <CardTitle>Event timeline</CardTitle>
            <CardDescription>
              Showing the most recent events matching your filters.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Show
              when={(events() ?? []).length > 0}
              fallback={
                <p class="rounded-md border border-dashed p-4 text-center text-sm text-muted-foreground">
                  {events.loading
                    ? "Loading events…"
                    : "No events matched the selected filters."}
                </p>
              }
            >
              <div class="space-y-3">
                <For each={events() ?? []}>
                  {(event) => (
                    <div class="rounded-md border p-3 text-sm">
                      <div class="flex flex-wrap items-center justify-between gap-2">
                        <span class="font-medium">{event.eventName}</span>
                        <span class="text-xs text-muted-foreground">
                          {formatEventTiming(event.emittedAt, event.createdAt)}{" "}
                          • Queue {event.queueName}
                        </span>
                      </div>
                      <div class="mt-1 text-xs text-muted-foreground">
                        Created {formatTimestamp(event.createdAt)}
                      </div>
                      <Show when={event.payload}>
                        <div class="mt-2">
                          <JSONViewer data={event.payload} label="Payload" />
                        </div>
                      </Show>
                    </div>
                  )}
                </For>
              </div>
            </Show>
            <Show when={eventsError()}>
              {(error) => (
                <p class="mt-3 rounded-md border border-destructive/30 bg-destructive/10 p-2 text-xs text-destructive">
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

function formatEventTiming(
  emittedAt?: string | null,
  createdAt?: string,
): string {
  if (emittedAt) {
    return `Emitted ${formatTimestamp(emittedAt)}`;
  }
  return `Created ${formatTimestamp(createdAt)}`;
}
