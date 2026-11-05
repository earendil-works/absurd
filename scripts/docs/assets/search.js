// Search: search.json (one entry per section, built by scripts/docs/build.py) is loaded
// on the first use of the box and searched here. Every word of the query must occur in a section;
// matches in headings and page titles rank first. `/` focuses the box.
const box = document.getElementById("search");
const list = document.getElementById("search-results");
let index;
let selected = -1;

async function load() {
  index ??= fetch(new URL("../search.json", import.meta.url))
    .then((r) => r.json())
    .then((rows) => rows.map(([file, page, anchor, heading, text]) => ({
      file, page, anchor, heading, text,
      hay: `${heading}\n${text}`.toLowerCase(),
      head: heading.toLowerCase(),
      title: page.toLowerCase(),
    })));
  return index;
}

function esc(s) {
  return s.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
}

function snippet(text, words) {
  const lower = text.toLowerCase();
  const at = Math.max(0, Math.min(...words.map((w) => lower.indexOf(w)).filter((i) => i >= 0)));
  const start = Math.max(0, at - 60);
  let s = esc((start > 0 ? "…" : "") + text.slice(start, start + 180) + (start + 180 < text.length ? "…" : ""));
  for (const w of words) {
    s = s.replace(new RegExp(esc(w).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi"), (m) => `<mark>${m}</mark>`);
  }
  return s;
}

function score(e, words) {
  let n = 0;
  for (const w of words) {
    if (!e.hay.includes(w)) return 0;
    if (e.head.includes(w)) n += 10;
    if (e.title.includes(w)) n += 3;
    let k = 0;
    for (let i = e.hay.indexOf(w); i >= 0 && k < 5; i = e.hay.indexOf(w, i + w.length)) k++;
    n += k;
  }
  return n;
}

async function search() {
  const words = box.value.toLowerCase().split(/\s+/).filter((w) => w.length > 1);
  if (words.length === 0) {
    list.hidden = true;
    return;
  }
  const rows = await load();
  const hits = rows
    .map((e) => [score(e, words), e])
    .filter(([s]) => s > 0)
    .sort((a, b) => b[0] - a[0])
    .slice(0, 12);
  selected = -1;
  list.innerHTML = hits.length
    ? hits.map(([, e]) => {
        const where = e.heading && e.heading !== e.page ? `${esc(e.page)} › ${esc(e.heading)}` : esc(e.page);
        // Files are relative to the site's root, which is this script's parent directory.
        const url = new URL(`../${e.file}`, import.meta.url);
        const href = e.anchor ? `${url.href.split("#")[0]}#${e.anchor}` : url.href;
        return `<li><a href="${href}"><b>${where}</b><span>${snippet(e.text, words)}</span></a></li>`;
      }).join("")
    : `<li class="none">No section contains all of these words.</li>`;
  list.hidden = false;
}

function move(d) {
  const links = [...list.querySelectorAll("a")];
  if (!links.length) return;
  selected = (selected + d + links.length) % links.length;
  links.forEach((a, i) => a.classList.toggle("sel", i === selected));
  links[selected].scrollIntoView({ block: "nearest" });
}

if (box && list) {
  box.addEventListener("focus", load, { once: true });
  box.addEventListener("input", search);
  box.addEventListener("keydown", (ev) => {
    if (ev.key === "ArrowDown") { move(1); ev.preventDefault(); }
    else if (ev.key === "ArrowUp") { move(-1); ev.preventDefault(); }
    else if (ev.key === "Enter") {
      const a = list.querySelectorAll("a")[Math.max(selected, 0)];
      if (a) location.href = a.href;
    } else if (ev.key === "Escape") { list.hidden = true; box.blur(); }
  });
  document.addEventListener("keydown", (ev) => {
    if (ev.key === "/" && document.activeElement !== box && !/^(INPUT|TEXTAREA)$/.test(document.activeElement?.tagName ?? "")
        && !document.activeElement?.isContentEditable) {
      ev.preventDefault();
      box.focus();
    }
  });
  document.addEventListener("click", (ev) => {
    if (!list.contains(ev.target) && ev.target !== box) list.hidden = true;
  });
  // A link to `page/#search=words` opens with the results for those words.
  const m = location.hash.match(/^#search=(.*)$/);
  if (m) {
    box.value = decodeURIComponent(m[1]);
    search();
  }
}
