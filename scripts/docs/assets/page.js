// The page's own behavior: the light/dark toggle, the chapter menu on narrow screens, the
// table of contents' current section, linked language tabs and the copy buttons on code
// blocks. The chosen color scheme is set on <html> before the first paint by a line in
// the template's <head>.
const root = document.documentElement;
const KEY = "absurd-color-scheme";
const TAB_KEY = "absurd-tab";

document.querySelector(".scheme")?.addEventListener("click", () => {
  const dark = root.dataset.colorScheme
    ? root.dataset.colorScheme === "dark"
    : matchMedia("(prefers-color-scheme: dark)").matches;
  const next = dark ? "light" : "dark";
  // Back to following the system when the choice matches it.
  if (matchMedia(`(prefers-color-scheme: ${next})`).matches) {
    delete root.dataset.colorScheme;
    try { localStorage.removeItem(KEY); } catch {}
  } else {
    root.dataset.colorScheme = next;
    try { localStorage.setItem(KEY, next); } catch {}
  }
});

const menu = document.querySelector(".menu-button");
menu?.addEventListener("click", () => {
  const open = document.body.classList.toggle("nav-open");
  menu.setAttribute("aria-expanded", String(open));
});

// The current section: the last heading above the top quarter of the window.
const links = [...document.querySelectorAll("aside.toc a")];
const heads = links.map((a) => document.getElementById(decodeURIComponent(a.hash.slice(1))));
let current = null;
function spy() {
  const line = innerHeight / 4;
  let i = heads.findIndex((h) => h && h.getBoundingClientRect().top > line);
  i = i === -1 ? heads.length - 1 : i - 1;
  const a = links[Math.max(0, i)];
  if (a === current) return;
  current?.removeAttribute("aria-current");
  a?.setAttribute("aria-current", "true");
  current = a;
}
if (links.length) {
  addEventListener("scroll", spy, { passive: true });
  spy();
}

// Tabs: choosing a language selects it in every set that has it, here and on the next
// page. The clicked set stays where it is on the screen while the others change height.
const label = (input) =>
  input.parentElement.querySelector(`.tabbed-labels > label[for="${input.id}"]`)?.textContent.trim();
const inputs = [...document.querySelectorAll(".tabbed-set > input[type=radio]")];
function selectTab(name, except) {
  for (const input of inputs) {
    if (input !== except && label(input) === name) input.checked = true;
  }
}
try {
  const saved = localStorage.getItem(TAB_KEY);
  if (saved) selectTab(saved);
} catch {}
for (const input of inputs) {
  input.addEventListener("change", () => {
    if (!input.checked) return;
    const name = label(input);
    const set = input.parentElement;
    const top = set.getBoundingClientRect().top;
    selectTab(name, input);
    scrollBy(0, set.getBoundingClientRect().top - top);
    try { localStorage.setItem(TAB_KEY, name); } catch {}
  });
}

// Copy buttons.
for (const wrap of document.querySelectorAll(".code-wrap")) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "copy";
  button.textContent = "Copy";
  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(wrap.querySelector("code").textContent);
      button.textContent = "Copied";
    } catch {
      button.textContent = "Failed";
    }
    button.classList.add("done");
    setTimeout(() => {
      button.textContent = "Copy";
      button.classList.remove("done");
    }, 1500);
  });
  wrap.append(button);
}
