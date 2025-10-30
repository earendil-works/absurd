import { createSignal, Show, For, createMemo } from "solid-js";

interface JSONViewerProps {
  data: any;
  label?: string;
}

type JSONToken =
  | { type: "key"; value: string; path: string }
  | { type: "string"; value: string; path: string }
  | { type: "number"; value: string; path: string }
  | { type: "boolean"; value: string; path: string }
  | { type: "null"; value: string; path: string }
  | { type: "punctuation"; value: string }
  | { type: "whitespace"; value: string }
  | {
      type: "foldable-start";
      value: string;
      path: string;
      foldType: "object" | "array";
    }
  | { type: "foldable-end"; value: string };

export function JSONViewer(props: JSONViewerProps) {
  const [copied, setCopied] = createSignal(false);
  const [toggledStrings, setToggledStrings] = createSignal<Set<string>>(
    new Set(),
  );
  const [foldedPaths, setFoldedPaths] = createSignal<Set<string>>(new Set());

  const errorInfo = () => extractErrorLike(props.data);

  const jsonString = () => {
    try {
      return JSON.stringify(props.data, null, 2);
    } catch {
      return String(props.data);
    }
  };

  const tokens = createMemo(() => {
    try {
      return tokenizeJSON(props.data, toggledStrings(), foldedPaths());
    } catch {
      return [];
    }
  });

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(jsonString());
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (error) {
      console.error("Failed to copy:", error);
    }
  };

  const toggleString = (path: string) => {
    const current = toggledStrings();
    const next = new Set(current);
    if (next.has(path)) {
      next.delete(path);
    } else {
      next.add(path);
    }
    setToggledStrings(next);
  };

  const toggleFold = (path: string) => {
    const current = foldedPaths();
    const next = new Set(current);
    if (next.has(path)) {
      next.delete(path);
    } else {
      next.add(path);
    }
    setFoldedPaths(next);
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
          <pre class="p-3 text-xs font-mono overflow-x-auto whitespace-pre-wrap break-all">
            <code>
              <For each={tokens()}>
                {(token) => {
                  if (token.type === "key") {
                    return <span class="text-emerald-600">{token.value}</span>;
                  }
                  if (token.type === "string") {
                    return (
                      <span
                        class="text-sky-600 cursor-pointer hover:text-sky-800"
                        onClick={() => toggleString(token.path)}
                      >
                        {token.value}
                      </span>
                    );
                  }
                  if (token.type === "number") {
                    return <span class="text-amber-700">{token.value}</span>;
                  }
                  if (token.type === "boolean" || token.type === "null") {
                    return <span class="text-violet-600">{token.value}</span>;
                  }
                  if (token.type === "foldable-start") {
                    const isFolded = foldedPaths().has(token.path);
                    return (
                      <span>
                        <span
                          class="text-muted-foreground cursor-pointer hover:text-foreground select-none inline-block w-4 text-center"
                          onClick={() => toggleFold(token.path)}
                        >
                          {isFolded ? "▶" : "▼"}
                        </span>
                        <span class="text-muted-foreground">{token.value}</span>
                      </span>
                    );
                  }
                  if (token.type === "punctuation") {
                    return (
                      <span class="text-muted-foreground">{token.value}</span>
                    );
                  }
                  if (token.type === "whitespace") {
                    return <span>{token.value}</span>;
                  }
                  if (token.type === "foldable-end") {
                    return (
                      <span class="text-muted-foreground">{token.value}</span>
                    );
                  }
                  return null;
                }}
              </For>
            </code>
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
              <pre class="max-h-48 overflow-auto rounded border bg-background px-2 py-1 text-xs whitespace-pre-wrap break-all">
                {info().message}
              </pre>
            </div>
            <Show when={info().stack}>
              <div>
                <div class="mb-1 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
                  Stack trace
                </div>
                <pre class="max-h-64 overflow-auto rounded border bg-background px-2 py-1 text-xs whitespace-pre-wrap break-all">
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

function tokenizeJSON(
  data: any,
  toggledStrings: Set<string>,
  foldedPaths: Set<string>,
  path: string = "$",
  indent: number = 0,
): JSONToken[] {
  const tokens: JSONToken[] = [];
  const indentStr = "  ".repeat(indent);

  if (data === null) {
    tokens.push({ type: "null", value: "null", path });
  } else if (typeof data === "boolean") {
    tokens.push({ type: "boolean", value: String(data), path });
  } else if (typeof data === "number") {
    tokens.push({ type: "number", value: String(data), path });
  } else if (typeof data === "string") {
    const isToggled = toggledStrings.has(path);
    if (isToggled) {
      // Show the unescaped string (raw rendering)
      tokens.push({ type: "string", value: `"${data}"`, path });
    } else {
      // Show the escaped string (JSON.stringify handles escaping)
      tokens.push({ type: "string", value: JSON.stringify(data), path });
    }
  } else if (Array.isArray(data)) {
    if (data.length === 0) {
      tokens.push({ type: "punctuation", value: "[]" });
    } else {
      const isFolded = foldedPaths.has(path);
      tokens.push({
        type: "foldable-start",
        value: "[",
        path,
        foldType: "array",
      });

      if (isFolded) {
        tokens.push({ type: "punctuation", value: " ... " });
      } else {
        tokens.push({ type: "whitespace", value: "\n" });
        data.forEach((item, index) => {
          tokens.push({ type: "whitespace", value: indentStr + "  " });
          tokens.push(
            ...tokenizeJSON(
              item,
              toggledStrings,
              foldedPaths,
              `${path}[${index}]`,
              indent + 1,
            ),
          );
          if (index < data.length - 1) {
            tokens.push({ type: "punctuation", value: "," });
          }
          tokens.push({ type: "whitespace", value: "\n" });
        });
        tokens.push({ type: "whitespace", value: indentStr });
      }

      tokens.push({ type: "foldable-end", value: "]" });
    }
  } else if (typeof data === "object") {
    const entries = Object.entries(data);
    if (entries.length === 0) {
      tokens.push({ type: "punctuation", value: "{}" });
    } else {
      const isFolded = foldedPaths.has(path);
      tokens.push({
        type: "foldable-start",
        value: "{",
        path,
        foldType: "object",
      });

      if (isFolded) {
        tokens.push({ type: "punctuation", value: " ... " });
      } else {
        tokens.push({ type: "whitespace", value: "\n" });
        entries.forEach(([key, value], index) => {
          const keyPath = `${path}.${key}`;
          tokens.push({ type: "whitespace", value: indentStr + "  " });
          tokens.push({
            type: "key",
            value: JSON.stringify(key),
            path: keyPath,
          });
          tokens.push({ type: "punctuation", value: ": " });
          tokens.push(
            ...tokenizeJSON(
              value,
              toggledStrings,
              foldedPaths,
              keyPath,
              indent + 1,
            ),
          );
          if (index < entries.length - 1) {
            tokens.push({ type: "punctuation", value: "," });
          }
          tokens.push({ type: "whitespace", value: "\n" });
        });
        tokens.push({ type: "whitespace", value: indentStr });
      }

      tokens.push({ type: "foldable-end", value: "}" });
    }
  } else {
    tokens.push({ type: "string", value: JSON.stringify(String(data)), path });
  }

  return tokens;
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
