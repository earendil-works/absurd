#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "markdown>=3.8",
#     "pymdown-extensions>=10.14",
#     "pygments>=2.19",
# ]
# ///
"""Builds the documentation site (docs/ -> site/).

The site uses the same layout and styles as Earendil's other documentation (the djs
guide): Elanor's design tokens and fonts (the npm packages pinned in
scripts/docs/elanor.json, downloaded and cached on first use), a side navigation from
docs/SUMMARY.md (mdBook's format), a table of contents, a client-side search and a
light/dark toggle. Elanor's icons and logos are not on npm yet; the few the template uses
are vendored in scripts/docs/assets/elanor.

    scripts/docs/build.py build [--out site]
    scripts/docs/build.py serve [--port 8000]
    scripts/docs/build.py update-elanor     # pin the latest Elanor packages

Every page `foo/bar.md` is written to `foo/bar/index.html` (`index.md` to `index.html`),
so the URLs are the ones the previous MkDocs-style site had.
"""

from __future__ import annotations

import argparse
import base64
import fnmatch
import hashlib
import html
import json
import os
import posixpath
import re
import shutil
import sys
import tarfile
import tempfile
import threading
import time
from dataclasses import dataclass, field
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import urlopen

import markdown
from markdown.extensions import Extension
from markdown.treeprocessors import Treeprocessor
from pygments.lexers import get_lexer_by_name
from pygments.token import Token
from pygments.util import ClassNotFound

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
HERE = Path(__file__).resolve().parent
ASSETS = HERE / "assets"
ELANOR = ASSETS / "elanor"
ELANOR_LOCK = HERE / "elanor.json"
NPM_REGISTRY = "https://registry.npmjs.org/"

SITE_NAME = "Absurd"
SITE_TAGLINE = "Documentation"
SITE_URL = "https://earendil-works.github.io/absurd/"
SITE_DESCRIPTION = (
    "Absurd is a Postgres-native durable workflow system built around durable "
    "execution in PostgreSQL."
)
REPO_URL = "https://github.com/earendil-works/absurd"
EDIT_URL = REPO_URL + "/blob/main/docs/"

# Copied from docs/ as they are, except for these (the Markdown pages are rendered).
SKIP_COPY = {"SUMMARY.md"}


def esc(s: str) -> str:
    return html.escape(s, quote=True)


# --- Navigation ------------------------------------------------------------------------


@dataclass
class Entry:
    """One line of SUMMARY.md: `# Part`, `- [Title](page.md)`, or a draft `- [Title]()`."""

    depth: int
    title: str
    path: str | None
    part: bool = False


def parse_summary(text: str) -> list[Entry]:
    entries = []
    for line in text.splitlines():
        if line.startswith("# "):
            entries.append(Entry(0, line[2:].strip(), None, part=True))
            continue
        m = re.match(r"^(\s*)[-*] \[(.*?)\]\((.*?)\)\s*$", line)
        if m:
            indent, title, path = m.groups()
            entries.append(Entry(len(indent) // 2, title, path or None))
    return entries


def page_url(md_path: str) -> str:
    """The site-relative URL of a page: `sdks/go.md` -> `sdks/go/`."""
    stem = md_path[: -len(".md")]
    if stem == "index":
        return ""
    if stem.endswith("/index"):
        return stem[: -len("index")]
    return stem + "/"


def page_file(md_path: str) -> str:
    return page_url(md_path) + "index.html"


def rel_url(target: str, from_dir: str) -> str:
    """`target` (site-relative, `""` is the root) as seen from the page in `from_dir`."""
    trailing = target == "" or target.endswith("/")
    rel = posixpath.relpath(target or ".", from_dir or ".")
    if rel == ".":
        return "./"
    return rel + "/" if trailing and not rel.endswith("/") else rel


# --- Markdown --------------------------------------------------------------------------

# Pygments token types to the roles of Elanor's Shiki theme, as djs-docs' highlighter
# marks them (style.css colors the `tok-*` classes).
ROLE_OF = [
    (Token.Comment, "comment"),
    (Token.Keyword.Constant, "number"),
    (Token.Keyword.Type, "type"),
    (Token.Keyword, "keyword"),
    (Token.Operator.Word, "keyword"),
    (Token.Name.Builtin.Pseudo, "keyword"),
    (Token.Name.Class, "type"),
    (Token.Name.Namespace, "type"),
    (Token.Name.Exception, "type"),
    (Token.Name.Function, "function"),
    (Token.Name.Decorator, "function"),
    (Token.Name.Builtin, "function"),
    (Token.Name.Tag, "keyword"),
    (Token.Name.Attribute, "variable"),
    (Token.Name, "variable"),
    (Token.Literal.String, "string"),
    (Token.Literal.Number, "number"),
    (Token.Operator, "punctuation"),
    (Token.Punctuation, "punctuation"),
    (Token.Generic.Prompt, "comment"),
    (Token.Generic.Output, "comment"),
    (Token.Generic.Heading, "keyword"),
    (Token.Generic.Subheading, "keyword"),
    (Token.Generic.Deleted, "string"),
    (Token.Generic.Inserted, "function"),
]

LANG_ALIASES = {"cron": "text", "pycon": "pycon"}


def role(ttype) -> str | None:
    for t, r in ROLE_OF:
        if ttype in t:
            return r
    return None


def highlight(code: str, lang: str) -> str:
    try:
        lexer = get_lexer_by_name(LANG_ALIASES.get(lang, lang) or "text", stripnl=False)
    except ClassNotFound:
        return esc(code)
    tokens = [(t, v) for t, v in lexer.get_tokens(code)]
    out = []
    for i, (ttype, value) in enumerate(tokens):
        r = role(ttype)
        if r == "variable":
            # Like djs-docs: a capitalized name is a type, a called name a function.
            nxt = next((v for _, v in tokens[i + 1 :] if v.strip()), "")
            if value[:1].isupper() and not value.isupper():
                r = "type"
            elif nxt.startswith("("):
                r = "function"
        if r is None or not value.strip():
            out.append(esc(value))
        else:
            # A token spanning lines is split so every line's markup stays balanced.
            out.append(
                "\n".join(
                    f'<span class="tok-{r}">{esc(part)}</span>' if part else ""
                    for part in value.split("\n")
                )
            )
    return "".join(out).rstrip("\n")


def fence(src, language, class_name, options, md, **kwargs):
    lang = language or ""
    code = highlight(src, lang)
    return f'<div class="code-wrap"><pre class="code"><code class="lang-{esc(lang)}">{code}</code></pre></div>'


class LinkRewriter(Treeprocessor):
    """Points links to other pages (`../concepts.md#steps`) at their URLs and keeps links
    to files of docs/ (images) working from the page's own directory."""

    def __init__(self, md, builder: "Builder"):
        super().__init__(md)
        self.builder = builder

    def run(self, root):
        page = self.builder.current
        for el in root.iter():
            attr = {"a": "href", "img": "src"}.get(el.tag)
            if attr and el.get(attr):
                el.set(attr, self.builder.resolve(page, el.get(attr)))
        return None


class SiteExtension(Extension):
    def __init__(self, builder: "Builder"):
        super().__init__()
        self.builder = builder

    def extendMarkdown(self, md):
        md.treeprocessors.register(LinkRewriter(md, self.builder), "absurd-links", 1)


# --- The site --------------------------------------------------------------------------


@dataclass
class Page:
    path: str
    title: str
    html: str
    toc: list[tuple[int, str, str]] = field(default_factory=list)


class Builder:
    def __init__(self, out: Path, base: str | None = None):
        self.out = out
        # The site's absolute path on its server, for the 404 page.
        self.base = base if base is not None else urlsplit(SITE_URL).path
        self.entries = parse_summary((DOCS / "SUMMARY.md").read_text())
        self.order = [e for e in self.entries if e.path]
        self.pages = {e.path for e in self.order}
        self.current = ""
        self.warnings: list[str] = []
        self.md = markdown.Markdown(
            extensions=[
                "tables",
                "attr_list",
                "md_in_html",
                "footnotes",
                "def_list",
                "toc",
                "pymdownx.superfences",
                "pymdownx.tabbed",
                SiteExtension(self),
            ],
            extension_configs={
                "toc": {"anchorlink": True, "anchorlink_class": "anchor"},
                "pymdownx.superfences": {
                    "custom_fences": [{"name": "*", "class": "code", "format": fence}],
                },
                "pymdownx.tabbed": {"alternate_style": True},
            },
        )

    def resolve(self, page: str, url: str) -> str:
        parts = urlsplit(url)
        if parts.scheme or parts.netloc or url.startswith(("#", "/", "mailto:")):
            return url
        here = posixpath.dirname(page)
        target = posixpath.normpath(posixpath.join(here, parts.path))
        frag = "#" + parts.fragment if parts.fragment else ""
        from_dir = page_url(page)
        if target.endswith(".md"):
            if target not in self.pages:
                self.warnings.append(f"{page}: link to unknown page {url}")
                return url
            return rel_url(page_url(target), from_dir) + frag
        if not (DOCS / target).exists():
            self.warnings.append(f"{page}: link to missing file {url}")
        return posixpath.relpath(target, from_dir or ".") + frag

    def render(self, path: str) -> Page:
        self.current = path
        self.md.reset()
        body = self.md.convert((DOCS / path).read_text())
        flat: list[tuple[int, str, str]] = []

        def walk(tokens):
            for t in tokens:
                flat.append((t["level"], t["id"], html.unescape(t["name"])))
                walk(t["children"])

        walk(self.md.toc_tokens)
        title = next((n for lvl, _, n in flat if lvl == 1), None)
        if title is None:
            title = next(e.title for e in self.order if e.path == path)
        # Wide tables scroll inside their wrapper instead of widening the column.
        body = body.replace("<table>", '<div class="table-scroll"><table>').replace(
            "</table>", "</table></div>"
        )
        toc = [t for t in flat if t[0] in (2, 3)]
        return Page(path, title, body, toc)

    def build(self) -> None:
        start = time.time()
        if self.out.exists():
            shutil.rmtree(self.out)
        self.out.mkdir(parents=True)
        self.copy_static()
        rendered = [self.render(e.path) for e in self.order]
        for i, page in enumerate(rendered):
            dest = self.out / page_file(page.path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(self.template(page, i))
        (self.out / "404.html").write_text(self.not_found())
        (self.out / "search.json").write_text(search_index(rendered))
        (self.out / "sitemap.xml").write_text(self.sitemap())
        (self.out / ".nojekyll").write_text("")
        for w in self.warnings:
            print(f"warning: {w}", file=sys.stderr)
        took = time.time() - start
        print(f"docs: {len(rendered)} pages in {self.out} ({took:.2f}s)", flush=True)

    def copy_static(self) -> None:
        for src in DOCS.rglob("*"):
            rel = src.relative_to(DOCS)
            if src.is_dir() or src.suffix == ".md" or str(rel) in SKIP_COPY:
                continue
            if src.name == ".DS_Store":
                continue
            dest = self.out / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
        theme = self.out / "_theme"
        shutil.copytree(ASSETS, theme, ignore=shutil.ignore_patterns(".DS_Store"))
        install_elanor(theme / "elanor")

    def nav(self, current: str, root: str) -> str:
        out = ["<ol>"]
        for e in self.entries:
            if e.part:
                out.append(f'<li class="part">{esc(e.title)}</li>')
            elif e.path:
                cur = ' aria-current="page"' if e.path == current else ""
                href = rel_url(page_url(e.path), page_url(current))
                out.append(
                    f'<li class="d{e.depth}"><a href="{href}"{cur}>{esc(e.title)}</a></li>'
                )
            else:
                out.append(f'<li class="d{e.depth} draft">{esc(e.title)}</li>')
        out.append("</ol>")
        return "".join(out)

    def template(self, page: Page, i: int) -> str:
        here = page_url(page.path)
        root = rel_url("", here)
        toc = ""
        if page.toc:
            items = "".join(
                f'<li class="h{lvl}"><a href="#{esc(id)}">{esc(text)}</a></li>'
                for lvl, id, text in page.toc[:80]
            )
            toc = f'<p class="toc-title">On this page</p><ol>{items}</ol>'
        pager = ['<nav class="pager">']
        for cls, label, j in (("prev", "Previous", i - 1), ("next", "Next", i + 1)):
            if 0 <= j < len(self.order):
                e = self.order[j]
                href = rel_url(page_url(e.path), here)
                pager.append(
                    f'<a class="{cls}" href="{href}"><small>{label}</small>{esc(e.title)}</a>'
                )
        pager.append("</nav>")
        title = (
            SITE_NAME
            if page.path == "index.md"
            else f"{page.title} · {SITE_NAME}"
        )
        return render_template(
            root=root,
            title=title,
            canonical=SITE_URL + here,
            nav=self.nav(page.path, root),
            body=page.html,
            toc=toc,
            pager="".join(pager),
            edit=EDIT_URL + page.path,
        )

    def not_found(self) -> str:
        # Served for any missing path, so its links are absolute.
        root = self.base
        return render_template(
            root=root,
            title=f"Page not found · {SITE_NAME}",
            canonical=SITE_URL,
            nav=self.nav("", root).replace('href="', f'href="{root}').replace(
                f'href="{root}./', f'href="{root}'
            ),
            body=(
                '<h1 id="not-found">Page not found</h1>'
                f'<p>There is no page at this address. Try the search, or go to the '
                f'<a href="{root}">start page</a>.</p>'
            ),
            toc="",
            pager="",
            edit=REPO_URL + "/tree/main/docs",
        )

    def sitemap(self) -> str:
        urls = "".join(
            f"<url><loc>{esc(SITE_URL + page_url(e.path))}</loc></url>" for e in self.order
        )
        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{urls}</urlset>\n'
        )


def icon(name: str, cls: str) -> str:
    """An Elanor icon inline, so that it takes the text color (`.icon` in style.css)."""
    svg = (ELANOR / "icons" / f"{name}.svg").read_text()
    svg = svg[svg.find("<svg") :].strip()
    return svg.replace(
        "<svg ", f'<svg class="icon {cls}" aria-hidden="true" focusable="false" ', 1
    )


def elanor_lock() -> dict:
    return json.loads(ELANOR_LOCK.read_text())


def npm_tarball(name: str, version: str, integrity: str) -> Path:
    """The package's tarball from npm, checked against its pinned integrity and cached in
    ~/.cache/absurd-docs."""
    algo, digest = integrity.split("-", 1)

    def ok(data: bytes) -> bool:
        return hashlib.new(algo, data).digest() == base64.b64decode(digest)

    cache = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache") / "absurd-docs"
    path = cache / f"{name.lstrip('@').replace('/', '-')}-{version}.tgz"
    if path.exists() and ok(path.read_bytes()):
        return path
    url = f"{NPM_REGISTRY}{name}/-/{name.split('/')[-1]}-{version}.tgz"
    print(f"docs: downloading {url}", flush=True)
    with urlopen(url) as r:
        data = r.read()
    if not ok(data):
        raise SystemExit(f"docs: {name}@{version} does not match its integrity in {ELANOR_LOCK}")
    cache.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)
    return path


def install_elanor(dest: Path) -> None:
    """Copies the files the site uses out of the pinned Elanor packages: one brand's
    tokens, and Mallos Serif and Commit Mono with their licenses."""
    lock = elanor_lock()
    brand = lock["brand"]
    wanted = {
        "@earendil-works/design-tokens": {
            f"dist/{brand}/tokens.css": "tokens.css",
            "LICENSE": "tokens.LICENSE",
        },
        "@earendil-works/elanor-fonts": {
            "mallos-serif.css": "fonts/mallos-serif.css",
            "commit-mono.css": "fonts/commit-mono.css",
            "files/MallosSerif-*.woff2": "fonts/files/",
            "files/CommitMono-*.woff2": "fonts/files/",
            "licenses/mallos-serif.txt": "fonts/licenses/mallos-serif.txt",
            "licenses/commit-mono.txt": "fonts/licenses/commit-mono.txt",
        },
    }
    for name, files in wanted.items():
        pkg = lock["packages"][name]
        with tarfile.open(npm_tarball(name, pkg["version"], pkg["integrity"])) as tar:
            members = {
                m.name.removeprefix("package/"): m for m in tar.getmembers() if m.isfile()
            }
            for pattern, target in files.items():
                hits = [n for n in members if fnmatch.fnmatchcase(n, pattern)]
                if not hits:
                    raise SystemExit(f"docs: {name}@{pkg['version']} has no {pattern}")
                for n in hits:
                    out = dest / (target + Path(n).name if target.endswith("/") else target)
                    out.parent.mkdir(parents=True, exist_ok=True)
                    out.write_bytes(tar.extractfile(members[n]).read())


def update_elanor() -> None:
    """Pins the latest published version of every Elanor package in elanor.json."""
    lock = elanor_lock()
    for name, pkg in lock["packages"].items():
        with urlopen(f"{NPM_REGISTRY}{name}/latest") as r:
            meta = json.load(r)
        new = {"version": meta["version"], "integrity": meta["dist"]["integrity"]}
        if new != pkg:
            print(f"{name}: {pkg['version']} -> {new['version']}")
        lock["packages"][name] = new
    ELANOR_LOCK.write_text(json.dumps(lock, indent=2) + "\n")
    # Fail now, not on the next build, if a file the site uses went away.
    with tempfile.TemporaryDirectory() as tmp:
        install_elanor(Path(tmp))


def render_template(*, root, title, canonical, nav, body, toc, pager, edit) -> str:
    lock = elanor_lock()
    brand = lock["brand"]
    tokens = lock["packages"]["@earendil-works/design-tokens"]["version"]
    t = f"{root}_theme/"
    social = SITE_URL + "assets/social-preview-1200x630.jpg"
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)}</title>
<meta name="description" content="{esc(SITE_DESCRIPTION)}">
<link rel="canonical" href="{esc(canonical)}">
<link rel="icon" type="image/png" sizes="64x64" href="{root}assets/favicon-64.png">
<link rel="icon" type="image/png" sizes="128x128" href="{root}assets/favicon-128.png">
<link rel="icon" type="image/png" sizes="512x512" href="{root}assets/favicon-512.png">
<link rel="apple-touch-icon" href="{root}assets/favicon-512.png">
<meta property="og:type" content="website">
<meta property="og:title" content="{esc(title)}">
<meta property="og:description" content="{esc(SITE_DESCRIPTION)}">
<meta property="og:url" content="{esc(canonical)}">
<meta property="og:image" content="{social}">
<meta property="og:image:type" content="image/jpeg">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta property="og:image:alt" content="Absurd elephant logo artwork">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{esc(title)}">
<meta name="twitter:description" content="{esc(SITE_DESCRIPTION)}">
<meta name="twitter:image" content="{social}">
<meta name="twitter:image:alt" content="Absurd elephant logo artwork">
<link rel="stylesheet" href="{t}elanor/tokens.css">
<link rel="stylesheet" href="{t}elanor/fonts/mallos-serif.css">
<link rel="stylesheet" href="{t}elanor/fonts/commit-mono.css">
<link rel="stylesheet" href="{t}style.css">
<script>try{{const s=localStorage.getItem("absurd-color-scheme");if(s)document.documentElement.dataset.colorScheme=s}}catch{{}}</script>
</head>
<body>
<a class="skip" href="#main">Skip to content</a>
<header class="top">
<button class="icon-button menu-button" type="button" aria-label="Chapters" aria-expanded="false">{icon("menu", "menu-icon")}</button>
<div class="identity"><a class="mark" href="https://earendil.com" title="Earendil"><img class="on-light" src="{t}elanor/logos/earendil-logo-on-light.svg" alt="Earendil"><img class="on-dark" src="{t}elanor/logos/earendil-logo-on-dark.svg" alt="Earendil"></a><a class="brand" href="{root}"><b>{esc(SITE_NAME)}</b><span>{esc(SITE_TAGLINE)}</span></a></div>
<span class="spacer"></span>
<div class="search">{icon("search", "search-icon")}<input type="search" id="search" placeholder="Search  /" aria-label="Search the documentation" autocomplete="off"><ol id="search-results" hidden></ol></div>
<div class="actions"><a class="icon-button src" href="{esc(edit)}" title="This page's source">Source</a><button class="icon-button scheme" type="button" aria-label="Switch between light and dark">{icon("dark-mode", "to-dark")}{icon("light-mode", "to-light")}</button></div>
</header>
<div class="layout">
<nav class="book" aria-label="Chapters">{nav}</nav>
<main id="main" tabindex="-1"><div class="page"><article data-text-context="prose">
{body}
</article>{pager}
<footer class="foot"><p>Absurd is built by <a href="https://earendil.com">Earendil</a>. <a href="{REPO_URL}">Source on GitHub</a>.</p><p>Designed with <a href="https://github.com/earendil-works/elanor">Elanor</a>, Earendil's design system (design tokens {esc(tokens)}, {esc(brand)} brand, <a href="{t}elanor/tokens.LICENSE">ISC License</a>). Fonts: Mallos Serif and Commit Mono, SIL Open Font License 1.1 (<a href="{t}elanor/fonts/licenses/mallos-serif.txt">Mallos Serif</a>, <a href="{t}elanor/fonts/licenses/commit-mono.txt">Commit Mono</a>). Icons: Material Symbols, <a href="{t}elanor/icons/LICENSE">Apache License 2.0</a>.</p></footer></div>
<aside class="toc">{toc}</aside></main>
</div>
<script type="module" src="{t}page.js"></script>
<script type="module" src="{t}search.js"></script>
</body>
</html>
"""


# --- Search ----------------------------------------------------------------------------

SECTION_RE = re.compile(r'<h([1-3]) id="([^"]+)"[^>]*>(.*?)</h\1>', re.S)
TAG_RE = re.compile(r"<[^>]+>")
INLINE_TAG_RE = re.compile(r"</?(?:span|code|a|strong|em|b|i|mark|kbd|sup|sub)\b[^>]*>")
SKIP_RE = re.compile(r'<div class="tabbed-labels">.*?</div>', re.S)


def text_of(fragment: str) -> str:
    fragment = INLINE_TAG_RE.sub("", SKIP_RE.sub(" ", fragment))
    return re.sub(r"\s+", " ", html.unescape(TAG_RE.sub(" ", fragment))).strip()


def search_index(pages: list[Page]) -> str:
    """One row per section: [file, page title, anchor, heading, text], as search.js
    expects (the file relative to the site's root)."""
    rows = []
    for page in pages:
        url = page_url(page.path) or "./"
        matches = list(SECTION_RE.finditer(page.html))
        starts = [(0, "", page.title)] + [
            (m.end(), m.group(2), text_of(m.group(3))) for m in matches
        ]
        ends = [m.start() for m in matches] + [len(page.html)]
        for (start, anchor, heading), end in zip(starts, ends):
            text = text_of(page.html[start:end])
            if not text and not anchor:
                continue
            rows.append([url, page.title, anchor, heading, text])
    return json.dumps(rows, ensure_ascii=False, separators=(",", ":"))


# --- Commands --------------------------------------------------------------------------


def snapshot() -> dict[Path, float]:
    files = {}
    for base in (DOCS, HERE):
        for p in base.rglob("*"):
            if p.is_file():
                files[p] = p.stat().st_mtime
    return files


def serve(out: Path, port: int) -> None:
    Builder(out, base="/").build()

    def watch():
        seen = snapshot()
        while True:
            time.sleep(0.5)
            now = snapshot()
            if now != seen:
                seen = now
                try:
                    Builder(out, base="/").build()
                except Exception as e:  # keep serving while the docs are being edited
                    print(f"docs: build failed: {e}", file=sys.stderr)

    threading.Thread(target=watch, daemon=True).start()

    class Handler(SimpleHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def send_error(self, code, message=None, explain=None):
            if code == 404 and (out / "404.html").exists():
                body = (out / "404.html").read_bytes()
                self.send_response(404)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            super().send_error(code, message, explain)

    server = ThreadingHTTPServer(("127.0.0.1", port), partial(Handler, directory=str(out)))
    print(f"docs: serving on http://127.0.0.1:{port}/ (rebuilds on change)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="build the site")
    b.add_argument("--out", type=Path, default=ROOT / "site")
    s = sub.add_parser("serve", help="build, serve and rebuild on change")
    s.add_argument("--out", type=Path, default=ROOT / "site")
    s.add_argument("--port", type=int, default=8000)
    sub.add_parser("update-elanor", help="pin the latest Elanor packages in elanor.json")
    args = p.parse_args()
    if args.cmd == "build":
        builder = Builder(args.out)
        builder.build()
        if builder.warnings and os.environ.get("DOCS_STRICT"):
            sys.exit(1)
    elif args.cmd == "update-elanor":
        update_elanor()
    else:
        serve(args.out, args.port)


if __name__ == "__main__":
    main()
