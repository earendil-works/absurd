import { createSignal, onMount } from "solid-js";

const STORAGE_KEY = "autoRefreshEnabled";

interface AutoRefreshToggleProps {
  onToggle?: (enabled: boolean) => void;
}

export function AutoRefreshToggle(props: AutoRefreshToggleProps) {
  const [enabled, setEnabled] = createSignal(true);

  onMount(() => {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored !== null) {
      const value = stored === "true";
      setEnabled(value);
      props.onToggle?.(value);
    }
  });

  const handleToggle = () => {
    const newValue = !enabled();
    setEnabled(newValue);
    localStorage.setItem(STORAGE_KEY, String(newValue));
    props.onToggle?.(newValue);
  };

  return (
    <label class="flex items-center gap-2 cursor-pointer">
      <span class="text-sm text-muted-foreground">Auto-refresh (15s)</span>
      <button
        type="button"
        role="switch"
        aria-checked={enabled()}
        onClick={handleToggle}
        class={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
          enabled() ? "bg-primary" : "bg-muted"
        }`}
      >
        <span
          class={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
            enabled() ? "translate-x-6" : "translate-x-1"
          }`}
        />
      </button>
    </label>
  );
}

export function useAutoRefresh(): boolean {
  const [enabled, setEnabled] = createSignal(true);

  onMount(() => {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored !== null) {
      setEnabled(stored === "true");
    }
  });

  return enabled();
}
