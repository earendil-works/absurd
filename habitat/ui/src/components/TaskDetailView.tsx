import { A } from "@solidjs/router";
import { For, Show } from "solid-js";
import { JSONViewer } from "@/components/JSONViewer";
import { TaskStatusBadge } from "@/components/TaskStatusBadge";
import { IdDisplay } from "@/components/IdDisplay";
import type { TaskDetail, TaskSummary } from "@/lib/api";
import { buttonVariants } from "@/components/ui/button";

interface TaskDetailViewProps {
  task: TaskSummary;
  detail: TaskDetail | undefined;
  taskLink?: string;
}

export function TaskDetailView(props: TaskDetailViewProps) {
  return (
    <div class="p-6 space-y-4">
      <Show
        when={props.detail}
        fallback={
          <div class="text-sm text-muted-foreground">Loading details...</div>
        }
      >
        {(detail) => (
          <DetailContent detail={detail()} taskLink={props.taskLink} />
        )}
      </Show>
    </div>
  );
}

function DetailContent(props: { detail: TaskDetail; taskLink?: string }) {
  const stateLabel = () =>
    props.detail.status?.toLowerCase() === "failed" ? "Failure" : "Final State";

  return (
    <>
      <Show when={props.taskLink}>
        {(link) => (
          <A
            href={link()}
            class={`${buttonVariants({ variant: "secondary", size: "sm" })} float-right items-center gap-1`}
          >
            View task history
          </A>
        )}
      </Show>

      <div class="grid gap-4 md:grid-cols-2">
        <div>
          <h3 class="text-sm font-semibold mb-2">Basic Information</h3>
          <dl class="space-y-1 text-sm">
            <div class="flex gap-2">
              <dt class="text-muted-foreground w-32">Current status:</dt>
              <dd>
                <TaskStatusBadge status={props.detail.status} />
              </dd>
            </div>
            <div class="flex gap-2">
              <dt class="text-muted-foreground w-32">Final status:</dt>
              <dd>
                {props.detail.finalStatus ? (
                  <TaskStatusBadge status={props.detail.finalStatus} />
                ) : (
                  "—"
                )}
              </dd>
            </div>
            <div class="flex gap-2">
              <dt class="text-muted-foreground w-32">Task Name:</dt>
              <dd class="font-medium">
                <Show when={props.taskLink} fallback={props.detail.taskName}>
                  {(link) => (
                    <A href={link()} class="text-primary hover:underline">
                      {props.detail.taskName}
                    </A>
                  )}
                </Show>
              </dd>
            </div>
            <div class="flex gap-2">
              <dt class="text-muted-foreground w-32">Queue:</dt>
              <dd>{props.detail.queueName}</dd>
            </div>
            <div class="flex gap-2">
              <dt class="text-muted-foreground w-32">Task ID:</dt>
              <dd>
                <Show
                  when={props.taskLink}
                  fallback={<IdDisplay value={props.detail.taskId} />}
                >
                  {(link) => (
                    <A
                      href={link()}
                      class="inline-flex items-center gap-1 hover:underline"
                    >
                      <IdDisplay value={props.detail.taskId} />
                    </A>
                  )}
                </Show>
              </dd>
            </div>
            <div class="flex gap-2">
              <dt class="text-muted-foreground w-32">Run ID:</dt>
              <dd>
                <IdDisplay value={props.detail.runId} />
              </dd>
            </div>
          </dl>
        </div>

        <div>
          <h3 class="text-sm font-semibold mb-2">Timing</h3>
          <dl class="space-y-1 text-sm">
            <div class="flex gap-2">
              <dt class="text-muted-foreground w-32">Created:</dt>
              <dd>{formatTimestamp(props.detail.createdAt)}</dd>
            </div>
            <div class="flex gap-2">
              <dt class="text-muted-foreground w-32">Updated:</dt>
              <dd>{formatTimestamp(props.detail.updatedAt)}</dd>
            </div>
            <Show when={props.detail.completedAt}>
              <div class="flex gap-2">
                <dt class="text-muted-foreground w-32">Completed:</dt>
                <dd>{formatTimestamp(props.detail.completedAt!)}</dd>
              </div>
            </Show>
          </dl>
        </div>
      </div>

      <Show
        when={
          props.detail.claimedBy ||
          props.detail.leaseExpiresAt ||
          props.detail.lastClaimedAt
        }
      >
        <div>
          <h3 class="text-sm font-semibold mb-2">Worker Information</h3>
          <dl class="space-y-1 text-sm">
            <Show when={props.detail.claimedBy}>
              <div class="flex gap-2">
                <dt class="text-muted-foreground w-32">Claimed By:</dt>
                <dd>{props.detail.claimedBy}</dd>
              </div>
            </Show>
            <Show when={props.detail.lastClaimedAt}>
              <div class="flex gap-2">
                <dt class="text-muted-foreground w-32">Last Claimed:</dt>
                <dd>{formatTimestamp(props.detail.lastClaimedAt!)}</dd>
              </div>
            </Show>
            <Show when={props.detail.leaseExpiresAt}>
              <div class="flex gap-2">
                <dt class="text-muted-foreground w-32">Lease Expires:</dt>
                <dd>{formatTimestamp(props.detail.leaseExpiresAt!)}</dd>
              </div>
            </Show>
          </dl>
        </div>
      </Show>

      <Show when={props.detail.nextWakeAt || props.detail.wakeEvent}>
        <div>
          <h3 class="text-sm font-semibold mb-2">Wake Information</h3>
          <dl class="space-y-1 text-sm">
            <Show when={props.detail.nextWakeAt}>
              <div class="flex gap-2">
                <dt class="text-muted-foreground w-32">Next Wake:</dt>
                <dd>{formatTimestamp(props.detail.nextWakeAt!)}</dd>
              </div>
            </Show>
            <Show when={props.detail.wakeEvent}>
              <div class="flex gap-2">
                <dt class="text-muted-foreground w-32">Wake Event:</dt>
                <dd>{props.detail.wakeEvent}</dd>
              </div>
            </Show>
          </dl>
        </div>
      </Show>

      <Show when={props.detail.waits.length > 0}>
        <div>
          <h3 class="text-sm font-semibold mb-2">Wait States</h3>
          <div class="space-y-3">
            <For each={props.detail.waits}>
              {(wait) => (
                <div class="rounded border p-3 space-y-2">
                  <div class="flex flex-wrap items-center gap-2">
                    <span class="font-medium text-sm">
                      {formatWaitType(wait.waitType)}
                    </span>
                    <Show when={wait.stepName}>
                      {(step) => (
                        <code class="rounded bg-muted px-1 text-xs">
                          {step()}
                        </code>
                      )}
                    </Show>
                  </div>
                  <dl class="space-y-1 text-xs">
                    <div class="flex gap-2">
                      <dt class="text-muted-foreground w-32">Wake at:</dt>
                      <dd>{formatTimestamp(wait.wakeAt)}</dd>
                    </div>
                    <Show when={wait.wakeEvent}>
                      {(eventName) => (
                        <div class="flex gap-2">
                          <dt class="text-muted-foreground w-32">
                            Wake event:
                          </dt>
                          <dd class="break-all">{eventName()}</dd>
                        </div>
                      )}
                    </Show>
                    <div class="flex gap-2">
                      <dt class="text-muted-foreground w-32">Updated:</dt>
                      <dd>{formatTimestamp(wait.updatedAt)}</dd>
                    </div>
                    <Show when={wait.emittedAt}>
                      {(emitted) => (
                        <div class="flex gap-2">
                          <dt class="text-muted-foreground w-32">Last emit:</dt>
                          <dd>{formatTimestamp(emitted())}</dd>
                        </div>
                      )}
                    </Show>
                  </dl>
                  <Show when={typeof wait.payload !== "undefined"}>
                    <div>
                      <JSONViewer data={wait.payload} label="Wait payload" />
                    </div>
                  </Show>
                  <Show when={typeof wait.eventPayload !== "undefined"}>
                    <div>
                      <JSONViewer
                        data={wait.eventPayload}
                        label="Event payload"
                      />
                    </div>
                  </Show>
                </div>
              )}
            </For>
          </div>
        </div>
      </Show>

      <div>
        <h3 class="text-sm font-semibold mb-2">Retry Information</h3>
        <dl class="space-y-1 text-sm">
          <div class="flex gap-2">
            <dt class="text-muted-foreground w-32">Attempt:</dt>
            <dd>
              {props.detail.attempt}
              {props.detail.maxAttempts
                ? ` of ${props.detail.maxAttempts}`
                : " (unlimited)"}
            </dd>
          </div>
        </dl>
        <Show when={props.detail.retryStrategy}>
          <div class="mt-2">
            <JSONViewer
              data={props.detail.retryStrategy}
              label="Retry Strategy"
            />
          </div>
        </Show>
      </div>

      <Show when={props.detail.params}>
        <div>
          <JSONViewer data={props.detail.params} label="Parameters" />
        </div>
      </Show>

      <Show when={props.detail.headers}>
        <div>
          <JSONViewer data={props.detail.headers} label="Headers" />
        </div>
      </Show>

      <Show when={props.detail.state !== undefined}>
        <div>
          <JSONViewer data={props.detail.state} label={stateLabel()} />
        </div>
      </Show>

      <Show when={props.detail.checkpoints.length > 0}>
        <div>
          <h3 class="text-sm font-semibold mb-2">Checkpoints</h3>
          <div class="space-y-2">
            <For each={props.detail.checkpoints}>
              {(checkpoint) => (
                <div class="border rounded p-3">
                  <div class="flex items-center gap-2 mb-2">
                    <span class="font-medium text-sm">
                      {checkpoint.stepName}
                    </span>
                    <TaskStatusBadge status={checkpoint.status} />
                  </div>
                  <div class="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground mb-2">
                    <span>Updated {formatTimestamp(checkpoint.updatedAt)}</span>
                    <Show when={checkpoint.expiresAt}>
                      {(expires) => (
                        <span>Expires {formatTimestamp(expires())}</span>
                      )}
                    </Show>
                  </div>
                  <JSONViewer data={checkpoint.state} />
                </div>
              )}
            </For>
          </div>
        </div>
      </Show>
    </>
  );
}

function formatWaitType(value: string | null | undefined): string {
  if (!value) return "Wait";
  const normalized = value.toLowerCase();
  if (normalized === "sleep") {
    return "Sleep wait";
  }
  if (normalized === "event") {
    return "Event wait";
  }
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function formatTimestamp(value: string | Date | null | undefined): string {
  if (!value) return "—";

  try {
    const date = typeof value === "string" ? new Date(value) : value;
    if (Number.isNaN(date.getTime())) {
      return "—";
    }

    return new Intl.DateTimeFormat(undefined, {
      dateStyle: "medium",
      timeStyle: "medium",
    }).format(date);
  } catch {
    return "—";
  }
}
