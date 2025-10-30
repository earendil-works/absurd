import {
  createContext,
  createMemo,
  createSignal,
  splitProps,
  useContext,
  type JSX,
  type ParentComponent,
} from "solid-js";
import { cn } from "@/lib/cn";

const TOTAL_SHADES = 17; // must be prime
const HUE_STEP = 360 / TOTAL_SHADES;

interface IdDisplayProps extends JSX.HTMLAttributes<HTMLSpanElement> {
  value: string;
}

function getColor(s: string): string {
  const index = hueIndexForId(s);
  const hue = Math.round(index * HUE_STEP);
  return `oklch(0.52 0.18 ${hue})`;
}

type IdDisplayContextValue = {
  hoveredId: () => string | null;
  setHoveredId: (id: string | null) => void;
};

const IdDisplayContext = createContext<IdDisplayContextValue>();

export const IdDisplayProvider: ParentComponent = (props) => {
  const [hoveredId, setHoveredId] = createSignal<string | null>(null);

  return (
    <IdDisplayContext.Provider value={{ hoveredId, setHoveredId }}>
      {props.children}
    </IdDisplayContext.Provider>
  );
};

export function IdDisplay(props: IdDisplayProps) {
  const [local, rest] = splitProps(props, ["value", "class", "style"]);
  const context = useContext(IdDisplayContext);

  const isHighlighted = createMemo(() => context?.hoveredId() === local.value);

  const style = createMemo(() => {
    const color = getColor(local.value);
    return {
      color,
    };
  });

  return (
    <span
      class={cn(
        "inline-flex items-center rounded border px-1.5 py-0.5 font-mono text-xs leading-tight whitespace-nowrap transition-all",
        isHighlighted() && "ring-2 ring-current ring-offset-2 scale-105",
        local.class,
      )}
      style={style()}
      onMouseEnter={() => context?.setHoveredId(local.value)}
      onMouseLeave={() => context?.setHoveredId(null)}
      {...rest}
    >
      {local.value}
    </span>
  );
}

function fnv1a32(s: string) {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

function hueIndexForId(value: string | undefined): number {
  return fnv1a32(value || "") % TOTAL_SHADES;
}
