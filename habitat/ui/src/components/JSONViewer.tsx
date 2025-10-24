import { createSignal, Show } from "solid-js";

interface JSONViewerProps {
  data: any;
  label?: string;
}

export function JSONViewer(props: JSONViewerProps) {
  const [copied, setCopied] = createSignal(false);

  const errorInfo = () => extractErrorLike(props.data);

  const jsonString = () => {
    try {
      return JSON.stringify(props.data, null, 2);
    } catch {
      return String(props.data);
    }
  };

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(jsonString());
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (error) {
      console.error("Failed to copy:", error);
    }
  };

  return (
    <div class="rounded-md border bg-muted/30">
      <Show when={props.label}>
        <div class="flex items-center justify-between border-b bg-muted/50 px-3 py-2">
          <span class="text-sm font-medium">{props.label}</span>
          <button
            type="button"
            onClick={handleCopy}
            class="text-xs text-muted-foreground hover:text-foreground"
          >
            {copied() ? "Copied!" : "Copy"}
          </button>
        </div>
      </Show>
      <Show
        when={errorInfo()}
        fallback={
          <pre class="overflow-x-auto p-3 text-xs whitespace-pre-wrap">
            <code>{jsonString()}</code>
          </pre>
        }
      >
        {(info) => (
          <div class="space-y-3 p-3 text-xs">
            <div class="text-sm font-semibold">{info().name}</div>
            <div>
              <div class="mb-1 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
                Message
              </div>
              <pre class="max-h-48 overflow-auto rounded border bg-background px-2 py-1 text-xs whitespace-pre-wrap">
                {info().message}
              </pre>
            </div>
            <Show when={info().stack}>
              <div>
                <div class="mb-1 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
                  Stack trace
                </div>
                <pre class="max-h-64 overflow-auto rounded border bg-background px-2 py-1 text-xs whitespace-pre-wrap">
                  {info().stack}
                </pre>
              </div>
            </Show>
          </div>
        )}
      </Show>
    </div>
  );
}

type ErrorLikeInfo = {
  name: string;
  message: string;
  stack: string;
};

function extractErrorLike(value: unknown): ErrorLikeInfo | null {
  if (!value) {
    return null;
  }

  if (value instanceof Error) {
    if (typeof value.stack !== "string") {
      return null;
    }

    return {
      name: value.name ?? "Error",
      message: String(value.message ?? ""),
      stack: value.stack,
    };
  }

  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    if (
      typeof record.name === "string" &&
      typeof record.message === "string" &&
      typeof record.stack === "string"
    ) {
      return {
        name: record.name,
        message: record.message,
        stack: record.stack,
      };
    }
  }

  return null;
}
