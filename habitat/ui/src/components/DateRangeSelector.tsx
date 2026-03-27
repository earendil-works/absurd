import {
  createSignal,
  createEffect,
  createMemo,
  onMount,
  onCleanup,
  untrack,
} from "solid-js";
import { Popover } from "@kobalte/core/popover";
import { cn } from "@/lib/cn";

/* ------------------------------------------------------------------ */
/*  Public interface                                                    */
/* ------------------------------------------------------------------ */

/** Computed time window for API queries. */
export interface TimeRange {
  after?: string;
  before?: string;
}

/**
 * URL-safe representation of the user's selection.
 *
 * `time`        – "15m" | "30m" | "1h" | "12h" | "24h" | "around" | "range" | undefined (= all)
 * `timeCenter`  – datetime-local value (only for "around")
 * `timeRadius`  – seconds string (only for "around")
 * `after`       – ISO string (only for "range")
 * `before`      – ISO string (only for "range")
 */
export interface TimeSelectionParams {
  time?: string;
  timeCenter?: string;
  timeRadius?: string;
  after?: string;
  before?: string;
}

export interface DateRangeSelectorProps {
  /** Called with the computed API time range whenever the selection changes. */
  onChange: (range: TimeRange) => void;
  /** Called with URL-safe params whenever the selection changes (for persistence). */
  onParamsChange: (params: TimeSelectionParams) => void;
  /** Restore selection from URL params on mount. */
  params?: TimeSelectionParams;
}

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

interface RelativePreset {
  label: string;
  key: string;
  seconds: number;
}

const RELATIVE_PRESETS: RelativePreset[] = [
  { label: "Last 15 minutes", key: "15m", seconds: 15 * 60 },
  { label: "Last 30 minutes", key: "30m", seconds: 30 * 60 },
  { label: "Last hour", key: "1h", seconds: 60 * 60 },
  { label: "Last 12 hours", key: "12h", seconds: 12 * 60 * 60 },
  { label: "Last 24 hours", key: "24h", seconds: 24 * 60 * 60 },
];

const PRESET_BY_KEY = new Map(RELATIVE_PRESETS.map((p) => [p.key, p]));

interface RadiusOption {
  shortLabel: string;
  seconds: number;
}

const RADIUS_OPTIONS: RadiusOption[] = [
  { shortLabel: "± 15m", seconds: 15 * 60 },
  { shortLabel: "± 30m", seconds: 30 * 60 },
  { shortLabel: "± 1h", seconds: 60 * 60 },
];

type SelectionKind = "all" | "relative" | "around" | "absolute";

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function toLocalInputValue(date: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function fmtShortDate(v: string): string {
  const d = new Date(v);
  if (isNaN(d.getTime())) return "?";
  return d.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatTriggerLabel(
  kind: SelectionKind,
  relKey: string | null,
  aroundCenter: string,
  aroundRadius: number,
  absStart: string,
  absEnd: string,
): string {
  if (kind === "all") return "All time";
  if (kind === "relative") {
    const preset = relKey ? PRESET_BY_KEY.get(relKey) : null;
    return preset?.label ?? "All time";
  }
  if (kind === "around") {
    const d = new Date(aroundCenter);
    if (isNaN(d.getTime())) return "Around…";
    const radius = RADIUS_OPTIONS.find((r) => r.seconds === aroundRadius);
    return `${fmtShortDate(aroundCenter)} ${radius?.shortLabel ?? ""}`.trim();
  }
  if (kind === "absolute") {
    const start = absStart ? fmtShortDate(absStart) : "…";
    const end = absEnd ? fmtShortDate(absEnd) : "now";
    return `${start} → ${end}`;
  }
  return "All time";
}

/* ------------------------------------------------------------------ */
/*  Component                                                          */
/* ------------------------------------------------------------------ */

export function DateRangeSelector(props: DateRangeSelectorProps) {
  /* --- Restore state from URL params --- */
  const restoreState = () => {
    const p = props.params;
    if (!p?.time)
      return { kind: "all" as const, relKey: null as string | null };

    const preset = PRESET_BY_KEY.get(p.time);
    if (preset) return { kind: "relative" as const, relKey: p.time };
    if (p.time === "around") return { kind: "around" as const, relKey: null };
    if (p.time === "range") return { kind: "absolute" as const, relKey: null };
    return { kind: "all" as const, relKey: null as string | null };
  };

  const initial = restoreState();

  const [kind, setKind] = createSignal<SelectionKind>(initial.kind);
  const [relKey, setRelKey] = createSignal<string | null>(initial.relKey);
  const [aroundCenter, setAroundCenter] = createSignal(
    props.params?.timeCenter || toLocalInputValue(new Date()),
  );
  const [aroundRadius, setAroundRadius] = createSignal(
    Number(props.params?.timeRadius) || RADIUS_OPTIONS[0].seconds,
  );
  const [absStart, setAbsStart] = createSignal(
    initial.kind === "absolute" && props.params?.after
      ? toLocalInputValue(new Date(props.params.after))
      : "",
  );
  const [absEnd, setAbsEnd] = createSignal(
    initial.kind === "absolute" && props.params?.before
      ? toLocalInputValue(new Date(props.params.before))
      : "",
  );
  const [open, setOpen] = createSignal(false);
  const [relativeTick, setRelativeTick] = createSignal(0);

  /* --- Compute API range --- */
  const range = createMemo((): TimeRange => {
    const k = kind();
    if (k === "all") return {};
    if (k === "relative") {
      const key = relKey();
      const preset = key ? PRESET_BY_KEY.get(key) : null;
      if (!preset) return {};
      void relativeTick();
      return {
        after: new Date(Date.now() - preset.seconds * 1000).toISOString(),
      };
    }
    if (k === "around") {
      const center = new Date(aroundCenter());
      if (isNaN(center.getTime())) return {};
      const r = aroundRadius();
      return {
        after: new Date(center.getTime() - r * 1000).toISOString(),
        before: new Date(center.getTime() + r * 1000).toISOString(),
      };
    }
    if (k === "absolute") {
      const rv: TimeRange = {};
      const s = absStart();
      const e = absEnd();
      if (s) {
        const d = new Date(s);
        if (!isNaN(d.getTime())) rv.after = d.toISOString();
      }
      if (e) {
        const d = new Date(e);
        if (!isNaN(d.getTime())) rv.before = d.toISOString();
      }
      return rv;
    }
    return {};
  });

  /* --- Compute URL params --- */
  const computeParams = (): TimeSelectionParams => {
    const k = kind();
    if (k === "all") return {};
    if (k === "relative") {
      const key = relKey();
      return key ? { time: key } : {};
    }
    if (k === "around") {
      return {
        time: "around",
        timeCenter: aroundCenter(),
        timeRadius: String(aroundRadius()),
      };
    }
    if (k === "absolute") {
      const r = range();
      return { time: "range", after: r.after, before: r.before };
    }
    return {};
  };

  /* --- Tick for relative mode freshness --- */
  onMount(() => {
    const timer = setInterval(() => {
      if (kind() === "relative") setRelativeTick((t) => t + 1);
    }, 30_000);
    onCleanup(() => clearInterval(timer));
  });

  /* --- Emit changes --- */
  // Track a serializable "version" of the selection so we only emit on real changes.
  // (We don't want the relative tick to trigger onParamsChange.)
  const selectionVersion = () => {
    const k = kind();
    if (k === "all") return "all";
    if (k === "relative") return `rel:${relKey()}`;
    if (k === "around") return `around:${aroundCenter()}:${aroundRadius()}`;
    if (k === "absolute") return `abs:${absStart()}:${absEnd()}`;
    return "all";
  };

  createEffect(() => {
    const r = range();
    untrack(() => props.onChange(r));
  });

  createEffect(() => {
    void selectionVersion(); // track
    const params = computeParams();
    untrack(() => props.onParamsChange(params));
  });

  /* --- Actions --- */
  const selectRelative = (key: string) => {
    setKind("relative");
    setRelKey(key);
    setOpen(false);
  };

  const selectAll = () => {
    setKind("all");
    setRelKey(null);
    setOpen(false);
  };

  const label = () =>
    formatTriggerLabel(
      kind(),
      relKey(),
      aroundCenter(),
      aroundRadius(),
      absStart(),
      absEnd(),
    );

  const isAroundActive = () => kind() === "around";
  const isAbsoluteActive = () => kind() === "absolute";

  return (
    <Popover
      open={open()}
      onOpenChange={setOpen}
      gutter={4}
      placement="bottom-end"
    >
      <Popover.Trigger
        class={cn(
          "inline-flex items-center gap-2 rounded-md border border-input bg-background",
          "min-w-[200px] px-3 py-1.5 text-sm shadow-sm transition-colors cursor-pointer",
          "hover:bg-accent hover:text-accent-foreground",
          "focus-visible:outline-none focus-visible:ring-[1.5px] focus-visible:ring-ring",
        )}
      >
        <ClockIcon />
        <span class="flex-1 truncate text-left">{label()}</span>
        <ChevronIcon />
      </Popover.Trigger>

      <Popover.Portal>
        <Popover.Content
          class={cn(
            "z-50 w-[480px] rounded-lg border bg-popover text-popover-foreground shadow-lg",
            "data-[expanded]:animate-in data-[closed]:animate-out",
            "data-[closed]:fade-out-0 data-[expanded]:fade-in-0",
            "data-[closed]:zoom-out-95 data-[expanded]:zoom-in-95",
            "origin-[var(--kb-popover-content-transform-origin)]",
          )}
        >
          <div class="flex min-h-[320px]">
            {/* Left: Custom range — always fully rendered, inactive parts greyed */}
            <div class="flex-1 border-r p-4 flex flex-col gap-4">
              <p class="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                Custom range
              </p>

              {/* Around a time */}
              <div class="space-y-2">
                <button
                  class={cn(
                    "w-full text-left text-sm rounded-md px-2 py-1.5 cursor-pointer transition-colors",
                    isAroundActive()
                      ? "bg-accent text-accent-foreground font-medium"
                      : "text-foreground hover:bg-accent/60",
                  )}
                  onClick={() => {
                    if (!isAroundActive()) {
                      setAroundCenter(toLocalInputValue(new Date()));
                    }
                    setKind("around");
                  }}
                >
                  Around a time
                </button>
                <div
                  class={cn(
                    "space-y-2 pl-2 transition-opacity",
                    isAroundActive()
                      ? "opacity-100"
                      : "opacity-40 pointer-events-none",
                  )}
                >
                  <input
                    type="datetime-local"
                    tabIndex={isAroundActive() ? 0 : -1}
                    class="w-full h-8 rounded-md border border-input bg-transparent px-2 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-[1.5px] focus-visible:ring-ring"
                    value={aroundCenter()}
                    onInput={(e) => setAroundCenter(e.currentTarget.value)}
                  />
                  <div class="flex gap-1.5">
                    {RADIUS_OPTIONS.map((opt) => (
                      <button
                        tabIndex={isAroundActive() ? 0 : -1}
                        class={cn(
                          "flex-1 rounded-md border px-2 py-1 text-xs font-medium tabular-nums transition-colors cursor-pointer",
                          aroundRadius() === opt.seconds
                            ? "bg-primary text-primary-foreground border-primary"
                            : "border-input bg-background text-muted-foreground hover:bg-accent hover:text-accent-foreground",
                        )}
                        onClick={() => setAroundRadius(opt.seconds)}
                      >
                        {opt.shortLabel}
                      </button>
                    ))}
                  </div>
                </div>
              </div>

              {/* Absolute range */}
              <div class="space-y-2">
                <button
                  class={cn(
                    "w-full text-left text-sm rounded-md px-2 py-1.5 cursor-pointer transition-colors",
                    isAbsoluteActive()
                      ? "bg-accent text-accent-foreground font-medium"
                      : "text-foreground hover:bg-accent/60",
                  )}
                  onClick={() => setKind("absolute")}
                >
                  Absolute range
                </button>
                <div
                  class={cn(
                    "space-y-2 pl-2 transition-opacity",
                    isAbsoluteActive()
                      ? "opacity-100"
                      : "opacity-40 pointer-events-none",
                  )}
                >
                  <div class="space-y-1">
                    <label class="text-xs text-muted-foreground">Start</label>
                    <input
                      type="datetime-local"
                      tabIndex={isAbsoluteActive() ? 0 : -1}
                      class="w-full h-8 rounded-md border border-input bg-transparent px-2 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-[1.5px] focus-visible:ring-ring"
                      value={absStart()}
                      onInput={(e) => setAbsStart(e.currentTarget.value)}
                    />
                  </div>
                  <div class="space-y-1">
                    <label class="text-xs text-muted-foreground">End</label>
                    <input
                      type="datetime-local"
                      tabIndex={isAbsoluteActive() ? 0 : -1}
                      class="w-full h-8 rounded-md border border-input bg-transparent px-2 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-[1.5px] focus-visible:ring-ring"
                      value={absEnd()}
                      onInput={(e) => setAbsEnd(e.currentTarget.value)}
                    />
                  </div>
                </div>
              </div>
            </div>

            {/* Right: Presets */}
            <div class="w-[180px] p-4 space-y-1">
              <p class="text-xs font-semibold uppercase tracking-wide text-muted-foreground mb-2">
                Presets
              </p>
              <button
                class={cn(
                  "w-full text-left text-sm rounded-md px-2 py-1.5 cursor-pointer transition-colors",
                  kind() === "all"
                    ? "bg-accent text-accent-foreground font-medium"
                    : "text-foreground hover:bg-accent/60",
                )}
                onClick={selectAll}
              >
                All time
              </button>
              {RELATIVE_PRESETS.map((preset) => (
                <button
                  class={cn(
                    "w-full text-left text-sm rounded-md px-2 py-1.5 cursor-pointer transition-colors",
                    kind() === "relative" && relKey() === preset.key
                      ? "bg-accent text-accent-foreground font-medium"
                      : "text-foreground hover:bg-accent/60",
                  )}
                  onClick={() => selectRelative(preset.key)}
                >
                  {preset.label}
                </button>
              ))}
            </div>
          </div>
        </Popover.Content>
      </Popover.Portal>
    </Popover>
  );
}

/* ------------------------------------------------------------------ */
/*  Icons                                                              */
/* ------------------------------------------------------------------ */

function ClockIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="14"
      height="14"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      stroke-width="2"
      stroke-linecap="round"
      stroke-linejoin="round"
      class="shrink-0 text-muted-foreground"
    >
      <circle cx="12" cy="12" r="10" />
      <polyline points="12 6 12 12 16 14" />
    </svg>
  );
}

function ChevronIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="12"
      height="12"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      stroke-width="2"
      stroke-linecap="round"
      stroke-linejoin="round"
      class="shrink-0 text-muted-foreground"
    >
      <path d="m6 9 6 6 6-6" />
    </svg>
  );
}
