import { For, Show, type JSX } from "solid-js";

interface HighlightProps extends JSX.HTMLAttributes<HTMLSpanElement> {
  text: string;
  search: string;
}

interface HighlightPart {
  text: string;
  isMatch: boolean;
}

function splitBySearch(text: string, search: string): HighlightPart[] {
  if (!search || search.length === 0) {
    return [{ text, isMatch: false }];
  }

  const lowerText = text.toLowerCase();
  const lowerSearch = search.toLowerCase();
  const parts: HighlightPart[] = [];
  let lastIndex = 0;

  let index = lowerText.indexOf(lowerSearch);
  while (index !== -1) {
    if (index > lastIndex) {
      parts.push({ text: text.slice(lastIndex, index), isMatch: false });
    }
    parts.push({
      text: text.slice(index, index + search.length),
      isMatch: true,
    });
    lastIndex = index + search.length;
    index = lowerText.indexOf(lowerSearch, lastIndex);
  }

  if (lastIndex < text.length) {
    parts.push({ text: text.slice(lastIndex), isMatch: false });
  }

  return parts.length > 0 ? parts : [{ text, isMatch: false }];
}

export function Highlight(props: HighlightProps) {
  const parts = () => splitBySearch(props.text, props.search);

  return (
    <span class={props.class}>
      <For each={parts()}>
        {(part) => (
          <Show when={part.isMatch} fallback={<>{part.text}</>}>
            <mark class="bg-yellow-300 dark:bg-yellow-600 rounded px-0.5">
              {part.text}
            </mark>
          </Show>
        )}
      </For>
    </span>
  );
}
