import { createMemo, splitProps, type JSX } from "solid-js";
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

export function IdDisplay(props: IdDisplayProps) {
  const [local, rest] = splitProps(props, ["value", "class", "style"]);

  const style = createMemo(() => {
    const color = getColor(local.value);
    return {
      color,
    };
  });

  return (
    <span
      class={cn(
        "inline-flex items-center rounded border px-1.5 py-0.5 font-mono text-xs leading-tight break-all",
        local.class,
      )}
      style={style()}
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
