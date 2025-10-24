import { createSignal, Show } from "solid-js";

interface JSONViewerProps {
  data: any;
  label?: string;
}

export function JSONViewer(props: JSONViewerProps) {
  const [copied, setCopied] = createSignal(false);

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
      <pre class="overflow-x-auto p-3 text-xs whitespace-pre-wrap">
        <code>{jsonString()}</code>
      </pre>
    </div>
  );
}
