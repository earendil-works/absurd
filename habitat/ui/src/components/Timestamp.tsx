import { Tooltip } from "@kobalte/core/tooltip";
import { For, Show, createMemo } from "solid-js";
import { cn } from "@/lib/cn";

type TimestampInput = string | Date | null | undefined;

type RelativeVariant = "compact" | "long";

interface BaseTimestampProps {
  value: TimestampInput;
  class?: string;
  fallback?: string;
}

interface RelativeTimestampProps extends BaseTimestampProps {
  variant?: RelativeVariant;
  label?: string;
}

export function RelativeTimestamp(props: RelativeTimestampProps) {
  const date = createMemo(() => parseTimestamp(props.value));
  const renderedLabel = createMemo(() => {
    const parsed = date();
    if (!parsed) {
      return props.fallback ?? "—";
    }
    return (
      props.label ?? formatRelativeTimestamp(parsed, props.variant ?? "long")
    );
  });

  return (
    <Show
      when={date()}
      fallback={<span class={props.class}>{props.fallback ?? "—"}</span>}
    >
      {(parsed) => (
        <TimestampTooltip
          class={props.class}
          lines={[formatUtcTimestamp(parsed()), formatLocalTimestamp(parsed())]}
        >
          {renderedLabel()}
        </TimestampTooltip>
      )}
    </Show>
  );
}

export function AbsoluteUtcTimestamp(props: BaseTimestampProps) {
  const date = createMemo(() => parseTimestamp(props.value));

  return (
    <Show
      when={date()}
      fallback={<span class={props.class}>{props.fallback ?? "—"}</span>}
    >
      {(parsed) => (
        <TimestampTooltip
          class={props.class}
          lines={[
            `Relative: ${formatRelativeTimestamp(parsed(), "long")}`,
            formatLocalTimestamp(parsed()),
          ]}
        >
          {formatUtcTimestamp(parsed())}
        </TimestampTooltip>
      )}
    </Show>
  );
}

function TimestampTooltip(props: {
  children: string;
  lines: string[];
  class?: string;
}) {
  return (
    <Tooltip openDelay={120}>
      <Tooltip.Trigger
        as="span"
        class={cn("inline-flex cursor-help", props.class)}
      >
        {props.children}
      </Tooltip.Trigger>
      <Tooltip.Portal>
        <Tooltip.Content class="z-50 max-w-xs rounded-md border border-border bg-popover px-2.5 py-2 text-xs text-popover-foreground shadow-md">
          <div class="space-y-1 font-mono tabular-nums">
            <For each={props.lines}>
              {(line) => <div class="whitespace-nowrap">{line}</div>}
            </For>
          </div>
        </Tooltip.Content>
      </Tooltip.Portal>
    </Tooltip>
  );
}

export function parseTimestamp(value: TimestampInput): Date | null {
  if (!value) {
    return null;
  }

  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date;
}

export function formatRelativeTimestamp(
  value: TimestampInput,
  variant: RelativeVariant = "long",
): string {
  const date = parseTimestamp(value);
  if (!date) {
    return "—";
  }

  const diffMs = date.getTime() - Date.now();
  const isFuture = diffMs > 0;
  const absSeconds = Math.floor(Math.abs(diffMs) / 1000);

  if (absSeconds < 5) {
    return variant === "long" ? "just now" : "now";
  }

  const units = [
    { label: "d", size: 86_400 },
    { label: "h", size: 3_600 },
    { label: "m", size: 60 },
    { label: "s", size: 1 },
  ] as const;

  for (const unit of units) {
    if (absSeconds >= unit.size) {
      const valuePart = Math.floor(absSeconds / unit.size);
      const token = `${valuePart}${unit.label}`;

      if (isFuture) {
        return `in ${token}`;
      }

      return variant === "long" ? `${token} ago` : token;
    }
  }

  return variant === "long" ? "just now" : "now";
}

export function formatUtcTimestamp(value: TimestampInput): string {
  const date = parseTimestamp(value);
  if (!date) {
    return "—";
  }

  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hours = String(date.getUTCHours()).padStart(2, "0");
  const minutes = String(date.getUTCMinutes()).padStart(2, "0");
  const seconds = String(date.getUTCSeconds()).padStart(2, "0");

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds} (UTC)`;
}

export function formatLocalTimestamp(value: TimestampInput): string {
  const date = parseTimestamp(value);
  if (!date) {
    return "—";
  }

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  const seconds = String(date.getSeconds()).padStart(2, "0");
  const gmtOffset = formatGmtOffset(date);

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds} (${gmtOffset})`;
}

function formatGmtOffset(date: Date): string {
  const offsetMinutes = -date.getTimezoneOffset();
  const sign = offsetMinutes >= 0 ? "+" : "-";
  const absMinutes = Math.abs(offsetMinutes);
  const hours = Math.floor(absMinutes / 60);
  const minutes = absMinutes % 60;

  if (minutes === 0) {
    return `GMT${sign}${hours}`;
  }

  return `GMT${sign}${hours}:${String(minutes).padStart(2, "0")}`;
}
