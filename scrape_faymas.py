#!/usr/bin/env python3
"""
Scrape EVERY prompt from https://faymas.in and write CSV files per faymas.in category:

    output/photography/photography_1-100.csv
    output/photography/photography_101-200.csv
    output/photography/photography_201-300.csv ...   (last file may be shorter, e.g. _301-342)

How it avoids missing prompts
  1. It reads the site's category list, then scrolls EVERY category page to the very end
     (and tries ?page=N pagination as a second check).
  2. It also scrolls the home page and compares: any prompt found there but in no category is reported.
  3. Every prompt page is scraped; pages that fail are retried on the next run and listed in
     data/remaining_urls.txt, so you can see exactly what (if anything) is still missing.
  4. data/discovery_report.json shows the per-category counts so you can compare with the site.

Large runs are resumable: stop any time, run again, it continues (the GitHub workflow does this
automatically).

Usage:
    python scrape_faymas.py --explore                # diagnostics first (categories, scrolling, parsing)
    python scrape_faymas.py --render --limit 20 --max-scrolls 5 --force   # small test
    python scrape_faymas.py --render                 # full run
    python scrape_faymas.py --render --rediscover    # look for newly published prompts
    python scrape_faymas.py --build-only             # rebuild CSVs from saved data
    python scrape_faymas.py --inspect URL --render   # debug one prompt page
"""
import argparse
import csv
import gzip
import json
import multiprocessing as mp
import random
import re
import shutil
import sys
import time
import urllib.robotparser
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE = "https://faymas.in"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; faymas-prompt-archiver/1.0)"}
CHUNK = 100
DATA_DIR = Path("data")
OUT_DIR = Path("output")
CACHE = DATA_DIR / "prompts.jsonl"
URLS = DATA_DIR / "urls.txt"
CATS = DATA_DIR / "categories.json"
STATUS = DATA_DIR / "status.json"
REMAINING = DATA_DIR / "remaining_urls.txt"
REPORT = DATA_DIR / "discovery_report.json"
SITEMAP_URLS = DATA_DIR / "sitemap_urls.txt"
COMPLETENESS = DATA_DIR / "completeness.json"
GONE = DATA_DIR / "gone_urls.txt"
DELAY_FILE = DATA_DIR / "delay.txt"
SUSPECT = DATA_DIR / "suspect_prompts.txt"
EXPLORE_DIR = DATA_DIR / "explore"
SEED_CATEGORIES = ["photography", "product-brand", "social-media", "wallpapers"]
CSV_FIELDS = ["id", "category", "title", "prompt", "ai_model", "url"]


# ----------------------------------------------------------------- helpers
def clean(s):
    return re.sub(r"\s+", " ", s or "").strip()


def slugify(s):
    s = re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")
    return s or "uncategorized"


def norm_url(u):
    return u.split("#")[0].split("?")[0].rstrip("/")


def http_get(session, url, retries=3):
    for i in range(retries):
        try:
            r = session.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                return r
            if r.status_code in (403, 404, 410):
                return None
        except requests.RequestException:
            pass
        time.sleep(2 * (i + 1))
    return None


class BlockedError(Exception):
    """The site refused the request (rate limit, bot check, access denied)."""

    def __init__(self, reason, retry_after=0):
        super().__init__(reason)
        self.reason = reason
        self.retry_after = retry_after


class GoneError(Exception):
    """The page answered 404/410."""


BLOCK_MARKERS = re.compile(
    r"just a moment|checking your browser|verify you are human|cf-chl|cf-browser-verification|"
    r"attention required|access denied|request blocked|unusual traffic from your|are you a robot|"
    r"enable javascript and cookies to continue|complete the security check|too many requests",
    re.I,
)


def detect_block(status, html=""):
    """Return a short reason if this response looks like a block / bot check, else ''."""
    if status == 429:
        return "HTTP 429 (too many requests)"
    if status in (401, 403):
        return f"HTTP {status} (access refused)"
    if status == 503:
        return "HTTP 503 (busy or bot-check page)"
    # bot-check pages are small; real prompt pages are much larger, so this avoids false alarms
    if html and len(html) < 30000 and BLOCK_MARKERS.search(html):
        return "bot-check / challenge page"
    return ""


def _retry_after(headers):
    try:
        return min(int((headers or {}).get("retry-after", "0")), 900)
    except (ValueError, TypeError):
        return 0


class Fetcher:
    """Fetches HTML with requests, or with headless Chromium when render=True.
    With strict=True it raises BlockedError / GoneError instead of returning None."""

    def __init__(self, render=False, delay=1.0, block_assets=False, storage_state=None, headed=False):
        self.session = requests.Session()
        self.render = False
        self.delay = delay
        self.block_assets = block_assets
        self.storage_state = storage_state if storage_state and Path(storage_state).exists() else None
        self.headed = headed
        self.page = None
        if render:
            self.enable_render()

    def enable_render(self):
        if self.render:
            return
        from playwright.sync_api import sync_playwright

        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=not self.headed)
        kw = dict(user_agent=HEADERS["User-Agent"], viewport={"width": 1400, "height": 900})
        if self.storage_state:  # a session you saved yourself with --warmup
            kw["storage_state"] = self.storage_state
        self._ctx = self._browser.new_context(**kw)
        if self.block_assets:  # images/fonts/video are not needed and slow everything down
            self._ctx.route(
                "**/*",
                lambda route: route.abort()
                if route.request.resource_type in ("image", "media", "font")
                else route.continue_(),
            )
        self.page = self._ctx.new_page()
        self.render = True

    def _http(self, url, strict):
        for i in range(3):
            try:
                r = self.session.get(url, headers=HEADERS, timeout=30)
            except requests.RequestException:
                time.sleep(2 * (i + 1))
                continue
            if r.status_code == 200:
                reason = detect_block(200, r.text)
                if reason:
                    if strict:
                        raise BlockedError(reason)
                    return None
                return r.text
            if r.status_code in (404, 410):
                if strict:
                    raise GoneError(f"HTTP {r.status_code}")
                return None
            reason = detect_block(r.status_code)
            if reason:
                if strict:
                    raise BlockedError(reason, _retry_after(r.headers))
                return None
            time.sleep(2 * (i + 1))
        return None

    def html(self, url, strict=False, force_http=False):
        time.sleep(self.delay)
        if self.render and not force_http:
            page = None
            try:
                # a fresh tab per page avoids a stuck/broken tab breaking every later page
                page = self._ctx.new_page()
                resp = page.goto(url, wait_until="domcontentloaded", timeout=45000)
                status = resp.status if resp is not None else 200
                if status in (404, 410):
                    if strict:
                        raise GoneError(f"HTTP {status}")
                    print(f"    HTTP {status} for {url}")
                    return None
                if status >= 400:
                    reason = detect_block(status)
                    if reason and strict:
                        raise BlockedError(reason, _retry_after(resp.headers))
                    print(f"    HTTP {status} for {url}")
                    return None
                try:
                    page.wait_for_selector("h1", timeout=15000)
                except Exception:
                    pass
                page.wait_for_timeout(500)
                content = page.content()
                reason = detect_block(200, content)
                if reason:
                    if strict:
                        raise BlockedError(reason)
                    print(f"    blocked ({reason}) for {url}")
                    return None
                return content
            except (BlockedError, GoneError):
                raise
            except Exception as e:
                print(f"    browser error: {str(e).splitlines()[0][:200]}")
                return None
            finally:
                if page:
                    page.close()
        return self._http(url, strict)

    def close(self):
        if self.render:
            try:
                self._browser.close()
                self._pw.stop()
            except Exception:
                pass
            self.render = False


def check_robots():
    rp = urllib.robotparser.RobotFileParser()
    try:
        rp.set_url(BASE + "/robots.txt")
        rp.read()
        if not rp.can_fetch(HEADERS["User-Agent"], BASE + "/prompt/x"):
            sys.exit("robots.txt disallows fetching /prompt/ pages. Stopping.")
    except SystemExit:
        raise
    except Exception:
        pass  # robots.txt unreachable: carry on


# ------------------------------------------------------- saved URL / category state
def read_urls():
    if not URLS.exists():
        return []
    return [l.strip() for l in URLS.read_text(encoding="utf-8").splitlines() if l.strip()]


def save_urls(urls):
    """Merge into data/urls.txt, keeping the original order and appending new ones."""
    DATA_DIR.mkdir(exist_ok=True)
    old = read_urls()
    seen = set(old)
    merged = old + [u for u in dict.fromkeys(urls) if u not in seen]
    URLS.write_text("\n".join(merged) + "\n", encoding="utf-8")


def load_cats():
    if CATS.exists():
        try:
            return json.loads(CATS.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"categories": {}, "home": {"prompt_urls": [], "complete": False}, "complete": False}


def save_cats(state):
    DATA_DIR.mkdir(exist_ok=True)
    CATS.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")



# ---------------------------------------------------------------- sitemap
def _canon(u):
    """Same site, same form: https://faymas.in/prompt/slug (no www, query or trailing slash)."""
    u = re.sub(r"^https?://(www\.)?faymas\.in", BASE, u.strip(), flags=re.I)
    return norm_url(u)


def _fetch_bytes(session, u, fetcher):
    """Plain HTTP first; if the site refuses it (not a plain 404), ask through the browser session instead."""
    r = None
    for i in range(2):
        try:
            r = session.get(u, headers=HEADERS, timeout=30)
            break
        except requests.RequestException:
            time.sleep(2)
    if r is not None and r.status_code == 200:
        return r.content
    if r is not None and r.status_code in (404, 410):
        return None
    if fetcher is not None:
        try:
            fetcher.enable_render()
            resp = fetcher._ctx.request.get(u, timeout=45000)
            if resp.status == 200:
                return resp.body()
        except Exception:
            pass
    return None


def sitemap_prompt_urls(fetcher=None, max_files=500):
    """Read robots.txt + sitemap.xml (and nested sitemap indexes) and return every /prompt/ URL listed.
    This is independent of scrolling, so it is the strongest check that nothing is missed."""
    session = requests.Session()
    queue = [BASE + "/sitemap.xml", BASE + "/sitemap_index.xml", BASE + "/sitemap-index.xml", BASE + "/sitemap-0.xml"]
    data = _fetch_bytes(session, BASE + "/robots.txt", fetcher)
    if data:
        for line in data.decode("utf-8", errors="replace").splitlines():
            if line.lower().startswith("sitemap:"):
                queue.insert(0, line.split(":", 1)[1].strip())
    seen, found, files_ok = set(), [], 0
    while queue and len(seen) < max_files:
        u = queue.pop(0)
        if u in seen:
            continue
        seen.add(u)
        data = _fetch_bytes(session, u, fetcher)
        if not data:
            continue
        if data[:2] == b"\x1f\x8b":
            try:
                data = gzip.decompress(data)
            except Exception:
                continue
        text = data.decode("utf-8", errors="replace")
        if "<loc" not in text:
            continue
        files_ok += 1
        for loc in re.findall(r"<loc>\s*(?:<!\[CDATA\[)?\s*([^<\]\s]+)", text):
            if re.search(r"\.xml(\.gz)?(\?|$)", loc):
                queue.append(loc)
            elif "/prompt/" in loc:
                found.append(_canon(loc))
    found = list(dict.fromkeys(found))
    print(f"Sitemap: read {files_ok} sitemap file(s), {len(found)} prompt URLs listed")
    return found


# ---------------------------------------------------------------- discovery
# Only look at links in the main content, not the sidebar ("Recent updates" etc.)
GRAB_JS = """els => els.filter(e => !e.closest('aside, nav, header, footer, [data-sidebar], [data-slot="sidebar"]'))
                      .map(e => e.href)"""
# Scroll the last prompt into view and push every scrollable parent to its bottom.
SCROLL_JS = """() => {
  const links = document.querySelectorAll("a[href*='/prompt/']");
  const last = links.length ? links[links.length - 1] : null;
  if (last) last.scrollIntoView({block: 'end'});
  let el = last;
  while (el) {
    if (el.scrollHeight > el.clientHeight + 5) {
      const oy = getComputedStyle(el).overflowY;
      if (oy === 'auto' || oy === 'scroll') el.scrollTop = el.scrollHeight;
    }
    el = el.parentElement;
  }
  window.scrollTo(0, document.documentElement.scrollHeight);
}"""
LOAD_MORE = re.compile(r"load more|view more|see more|more prompts|next", re.I)
CAT_RE = re.compile(r"^https?://[^/]+/category/([^/?#]+)/?$")


def find_category_links(page):
    """Open the sidebar 'Categories' section and collect every /category/<slug> link."""
    cats = {}

    def grab():
        for h in page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)"):
            m = CAT_RE.match(h.split("#")[0].split("?")[0])
            if m:
                cats.setdefault(m.group(1), f"{BASE}/category/{m.group(1)}")

    def open_categories():
        try:
            page.get_by_text(re.compile(r"^\s*Categories\s*$")).first.click(timeout=2500)
            page.wait_for_timeout(1200)
        except Exception:
            pass

    grab()
    open_categories()
    grab()
    if not cats:  # sidebar may be collapsed
        try:
            page.get_by_role("button", name=re.compile("toggle sidebar", re.I)).first.click(timeout=1500)
            page.wait_for_timeout(1000)
        except Exception:
            pass
        open_categories()
        grab()
    return cats


def collect_listing(page, url, max_scrolls, deadline, seed=(), verbose=False):
    """Scroll a listing page (home or a category) to its end and return all prompt links.
    Returns {"links": [...], "ended": bool, "title": str}. ended=True means the end was reached."""
    found = {u: i for i, u in enumerate(seed)}

    def grab():
        for h in page.eval_on_selector_all("a[href*='/prompt/']", GRAB_JS):
            h = norm_url(h)
            if "/prompt/" in h:
                found.setdefault(h, len(found))

    title = ""
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)
        why = detect_block(resp.status if resp is not None else 200, page.content())
        if why:
            print(f"    BLOCKED while opening {url}: {why}")
            return {"links": list(found), "ended": False, "title": title, "blocked": why}
        try:
            title = clean(page.locator("h1").first.inner_text(timeout=3000))
        except Exception:
            pass
    except Exception as e:
        print(f"    could not open {url}: {e}")
        return {"links": list(found), "ended": False, "title": title}

    last, stable, ended = len(found), 0, False
    for n in range(1, max_scrolls + 1):
        grab()
        page.evaluate(SCROLL_JS)
        try:
            page.mouse.move(700, 450)
            page.mouse.wheel(0, 4000)
            page.keyboard.press("End")
        except Exception:
            pass
        try:
            btn = page.locator("button", has_text=LOAD_MORE)
            if btn.count():
                btn.first.click(timeout=1000)
        except Exception:
            pass
        page.wait_for_timeout(2000)
        stable = stable + 1 if len(found) == last else 0
        last = len(found)
        if verbose or n == 1 or n % 10 == 0:
            print(f"    scroll {n}/{max_scrolls}: {len(found)} prompt links")
        if stable in (4, 7, 10, 13):  # nudge: scroll up a little, then down again
            try:
                page.mouse.wheel(0, -1500)
                page.wait_for_timeout(600)
                page.evaluate(SCROLL_JS)
            except Exception:
                pass
        if stable >= 16:
            ended = True
            break
        if time.time() > deadline:
            print("    time budget reached while scrolling")
            break
    grab()

    # Second check: does ?page=N reveal prompts that scrolling did not?
    if ended or max_scrolls <= 10:
        for n in range(2, 5000):
            if time.time() > deadline:
                ended = False
                break
            try:
                page.goto(f"{url}?page={n}", wait_until="domcontentloaded", timeout=45000)
                page.wait_for_timeout(2000)
            except Exception:
                break
            before = len(found)
            grab()
            if len(found) == before:
                break
            ended = True
            if n % 10 == 0:
                print(f"    ?page={n}: {len(found)} prompt links")
    return {"links": list(found), "ended": ended, "title": title}


def discover(fetcher, args):
    """Find every prompt URL (and which category each is in). Saved so later runs skip this step."""
    state = load_cats()
    if state.get("complete") and URLS.exists() and not args.rediscover:
        urls = read_urls()
        print(f"Using {len(urls)} saved prompt URLs from {len(state['categories'])} categories "
              f"(add --rediscover to look for new prompts)")
        return urls, state

    # 1) the sitemap: every prompt the site itself says exists (no scrolling needed)
    sm = sitemap_prompt_urls(fetcher)
    if sm:
        DATA_DIR.mkdir(exist_ok=True)
        SITEMAP_URLS.write_text("\n".join(sm) + "\n", encoding="utf-8")
        save_urls(sm)
    else:
        print("WARNING: no sitemap found. Relying on scrolling the listing pages only.")

    if args.skip_listing and sm:
        print("--skip-listing: using the sitemap only; categories come from each prompt page.")
        state["complete"] = True
        state["sitemap_only"] = True
        save_cats(state)
        return read_urls(), state

    # 2) the listing pages (scrolling) give the category of each prompt and a second opinion on the count
    fetcher.enable_render()
    page = fetcher.page
    deadline = time.time() + args.crawl_minutes * 60

    page.goto(BASE, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3000)
    found = find_category_links(page)
    for slug in SEED_CATEGORIES + (args.category or []):
        found.setdefault(slug, f"{BASE}/category/{slug}")
    cats = state["categories"]
    for slug, url in found.items():
        cats.setdefault(slug, {"name": "", "url": url, "prompt_urls": [], "complete": False})
    print(f"Categories ({len(cats)}): {', '.join(cats)}")
    if args.rediscover:
        for info in cats.values():
            info["complete"] = False

    listing_blocked = False
    for slug, info in cats.items():
        if info.get("complete"):
            continue
        if time.time() > deadline:
            print("Discovery time budget reached; run again to continue.")
            break
        print(f"  listing category '{slug}'")
        res = collect_listing(page, info["url"], args.max_scrolls, deadline, seed=info["prompt_urls"])
        info["prompt_urls"] = res["links"]
        info["complete"] = res["ended"]
        info["name"] = res["title"] or info.get("name") or slug.replace("-", " ").title()
        save_cats(state)
        save_urls(res["links"])
        print(f"    -> {slug} ({info['name']}): {len(res['links'])} prompts "
              f"({'complete' if res['ended'] else 'INCOMPLETE'})")
        if res.get("blocked"):
            print("    The listing pages are being blocked. Not hammering the site: relying on the sitemap "
                  "and each prompt page for category info.")
            listing_blocked = True
            break

    home = state["home"]
    if (not args.skip_home and not listing_blocked and (not home.get("complete") or args.rediscover)
            and time.time() < deadline):
        print("  listing the home page (cross-check)")
        res = collect_listing(page, BASE, args.max_scrolls, deadline, seed=home.get("prompt_urls", []))
        home.update(prompt_urls=res["links"], complete=res["ended"])
        save_urls(res["links"])
        print(f"    -> home: {len(res['links'])} prompts ({'complete' if res['ended'] else 'INCOMPLETE'})")

    state["complete"] = bool(cats) and all(i.get("complete") for i in cats.values())
    save_cats(state)
    write_report(state)
    if not state["complete"]:
        print("WARNING: not every category listing reached its end. Run again, or raise "
              "--max-scrolls / --crawl-minutes, to continue.")
    return read_urls(), state


def write_report(state):
    in_cat = {}
    for slug, info in state["categories"].items():
        for u in info["prompt_urls"]:
            in_cat.setdefault(u, []).append(slug)
    home = set(state["home"].get("prompt_urls", []))
    report = {
        "per_category": {s: len(i["prompt_urls"]) for s, i in state["categories"].items()},
        "unique_prompts_in_categories": len(in_cat),
        "prompts_in_more_than_one_category": sum(1 for v in in_cat.values() if len(v) > 1),
        "home_listing_count": len(home),
        "in_home_but_no_category": sorted(home - set(in_cat))[:200],
        "in_home_but_no_category_total": len(home - set(in_cat)),
    }
    DATA_DIR.mkdir(exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("---- discovery report ----")
    for s, n in report["per_category"].items():
        print(f"  {s}: {n}")
    print(f"  unique prompts across categories: {report['unique_prompts_in_categories']}")
    print(f"  home listing: {report['home_listing_count']} "
          f"({report['in_home_but_no_category_total']} not in any category)")


# ------------------------------------------------------------------ parsing
H2 = "\x00H2\x00"
STOP = re.compile(r"^(Show more|Show less|Prompt tags|Images in this prompt.*|Related .*|Sponsored|Ai Model.*|Total Likes.*)$", re.I)
BLOCK_TAGS = ["p", "div", "li", "ul", "ol", "section", "article", "pre", "blockquote", "tr", "table", "h1", "h2",
              "h3", "h4", "h5", "h6", "button", "header", "footer", "main", "aside", "nav"]
KNOWN_CATEGORY_NAMES = []


def set_known_categories(names):
    global KNOWN_CATEGORY_NAMES
    KNOWN_CATEGORY_NAMES = sorted({n for n in names if n}, key=len, reverse=True)


def _squash(s):
    return re.sub(r"\s+", "", s or "")


def linearize(soup):
    """Turn the page into a list of text lines, one per block element (inline tags stay together)."""
    s = BeautifulSoup(str(soup), "lxml")
    for t in s(["script", "style", "noscript", "svg", "head"]):
        t.decompose()
    for br in s.find_all("br"):
        br.replace_with("\n")
    for h in s.find_all("h2"):
        h.insert(0, H2 + " ")
    for t in s.find_all(BLOCK_TAGS):
        t.insert_before("\n")
        t.insert_after("\n")
    lines = [re.sub(r"[ \t\r\f\v\u00a0]+", " ", l).strip() for l in s.get_text("").split("\n")]
    return [l for l in lines if l]


def find_title(soup):
    h1 = soup.find("h1")
    if h1 and clean(h1.get_text(" ")):
        return clean(h1.get_text(" "))
    t = soup.title.string if soup.title and soup.title.string else ""
    return clean(re.sub(r"\s*[-|]\s*(AI prompt for .*|Faymas)$", "", t, flags=re.I))


def locate_title(lines, title):
    t = _squash(title)
    if not t:
        return -1
    for i, l in enumerate(lines):
        if _squash(l.replace(H2, "")) == t:
            return i
    return -1


def extract_struct(lines, idx):
    """The prompt is the block of text right after the title, up to 'Show more' / 'Prompt tags' / related cards."""
    body = []
    for l in lines[idx + 1:]:
        if l.startswith(H2) or STOP.match(l):
            break
        body.append(l)
    return "\n".join(body).strip()


def find_site_category(lines, idx):
    """The category name is shown in the header just above the title (next to the AI model)."""
    if idx < 0:
        return ""
    header = " \n ".join(lines[max(0, idx - 12): idx])
    for name in KNOWN_CATEGORY_NAMES:  # plain substring: the model and category names can run together
        if name.lower() in header.lower():
            return name
    last = lines[idx - 1] if idx > 0 else ""
    if last and len(last) < 40 and not re.search(r"\bago\b|published|ai model", last, re.I):
        return last
    return ""


def find_model(soup):
    h1 = soup.find("h1")
    if h1:
        for img in h1.find_all_previous("img"):
            m = re.search(r"/icons/([^/?.]+)\.svg", img.get("src", ""))
            if m:
                return m.group(1)
    m = re.search(r"AI prompt for ([\w.\- ]+)", (soup.title.string if soup.title and soup.title.string else ""), re.I)
    return clean(m.group(1)) if m else ""


JSON_KEYS = {"prompt", "prompttext", "prompt_text", "promptcontent"}


def _walk_json(o):
    if isinstance(o, dict):
        for k, v in o.items():
            if isinstance(v, str):
                if k.lower() in JSON_KEYS and len(v) > 40:
                    yield v
            else:
                yield from _walk_json(v)
    elif isinstance(o, list):
        for i in o:
            yield from _walk_json(i)


def _meta(soup, *names):
    for n in names:
        tag = soup.find("meta", attrs={"property": n}) or soup.find("meta", attrs={"name": n})
        if tag and tag.get("content"):
            return clean(tag["content"])
    return ""


def fallback_prompt_text(soup):
    for script in soup.find_all("script"):
        if script.get("type") == "application/ld+json" or script.get("id") == "__NEXT_DATA__":
            try:
                data = json.loads(script.string or "")
            except Exception:
                continue
            best = max(_walk_json(data), key=len, default="")
            if best:
                return best.strip(), "json"
    cands = [el.get_text("\n", strip=True) for el in soup.find_all(["textarea", "pre", "code"])]
    cands = [c for c in cands if len(c) > 80]
    if cands:
        return max(cands, key=len), "dom"
    return _meta(soup, "og:description", "description"), "meta"


def parse_prompt_page(html, url):
    soup = BeautifulSoup(html, "lxml")
    lines = linearize(soup)
    title = find_title(soup)
    idx = locate_title(lines, title)
    text = extract_struct(lines, idx) if idx >= 0 else ""
    source = "struct"
    if len(text) < 40:
        text, source = fallback_prompt_text(soup)
    return {
        "url": url,
        "title": title,
        "category": find_site_category(lines, idx),
        "ai_model": find_model(soup),
        "prompt": text,
        "source": source,
    }


# ----------------------------------------------------------------- pipeline
def load_cache():
    rows = []
    if CACHE.exists():
        with CACHE.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass  # half-written last line from an interrupted run
    return rows


_worker_fetcher = None
_shared_delay = None
_try_http_first = False


def _init_worker(render, delay, category_names, shared_delay=None, try_http_first=False, storage_state=None):
    global _worker_fetcher, _shared_delay, _try_http_first
    set_known_categories(category_names)
    _shared_delay = shared_delay
    _try_http_first = try_http_first
    _worker_fetcher = Fetcher(render=render, delay=delay, block_assets=True, storage_state=storage_state)


def _scrape_one(url):
    try:
        if _shared_delay is not None:  # the controller slows everybody down when the site pushes back
            _worker_fetcher.delay = _shared_delay.value * random.uniform(0.7, 1.5)
        if _try_http_first and _worker_fetcher.render:
            h = _worker_fetcher.html(url, strict=True, force_http=True)
            if h:
                r = parse_prompt_page(h, url)
                if r["source"] in ("struct", "json", "dom") and len(r["prompt"]) >= 40:
                    return url, r, ""
        html = _worker_fetcher.html(url, strict=True)
        if not html:
            return url, None, "fetch failed"
        rec = parse_prompt_page(html, url)
        if not rec["prompt"]:
            return url, None, "no prompt text found"
        return url, rec, ""
    except BlockedError as e:
        return url, None, f"BLOCKED|{e.retry_after}|{e.reason}"
    except GoneError:
        try:  # double-check with a plain request before calling a page gone
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code in (404, 410):
                return url, None, f"GONE|{r.status_code}"
        except Exception:
            pass
        return url, None, "fetch failed (404 in browser only)"
    except Exception as e:  # keep the pool alive no matter what one page does
        return url, None, (str(e).splitlines()[0][:150] if str(e) else "error")


def read_gone():
    return set(GONE.read_text(encoding="utf-8").split()) if GONE.exists() else set()


def scrape(args):
    check_robots()
    DATA_DIR.mkdir(exist_ok=True)
    STATUS.unlink(missing_ok=True)  # so a crashed run can never trigger another chained run
    t0 = time.time()
    budget = args.time_budget * 60

    cached = load_cache()
    have = {r["url"] for r in cached}
    seq = max((r["seq"] for r in cached), default=0)

    urls_before = len(read_urls())
    fetcher = Fetcher(render=args.render, delay=args.delay, storage_state=args.storage_state, headed=args.headed)
    try:
        urls, state = discover(fetcher, args)
    finally:
        fetcher.close()  # workers start their own browsers

    if len(urls) < 40 and not args.force:
        sys.exit(
            f"Only {len(urls)} prompt URLs were found, far fewer than expected. The listing pages are not "
            f"loading more prompts when scrolled. Run the workflow with mode=explore and send me the log. "
            f"(Use --force to continue anyway.)"
        )

    names = [i.get("name") for i in state["categories"].values()]
    gone = read_gone()  # pages confirmed 404/410: nothing to collect, so they do not count as missing
    todo = [u for u in urls if u not in have and u not in gone]
    if args.limit:
        todo = todo[: args.limit]
    print(f"{len(urls)} prompt URLs known, {len(have)} already scraped, {len(todo)} to scrape this run "
          f"({args.workers} parallel browsers)")

    scraped, failures = 0, {}
    blocked_stop, block_streak, block_events, final_delay = False, 0, 0, None
    new_gone = set()
    if todo:
        batch_size = max(args.workers * 25, 50)
        ctx = mp.get_context("spawn")
        start_delay = float(args.delay)
        if DELAY_FILE.exists():  # keep the slower pace learned in an earlier run
            try:
                start_delay = max(start_delay, float(DELAY_FILE.read_text().strip()))
            except ValueError:
                pass
        shared_delay = ctx.Value("d", start_delay)
        with CACHE.open("a", encoding="utf-8") as out, ProcessPoolExecutor(
            max_workers=args.workers, mp_context=ctx, initializer=_init_worker,
            initargs=(args.render, args.delay, names, shared_delay, args.try_http_first, args.storage_state),
        ) as ex:
            for b in range(0, len(todo), batch_size):
                if time.time() - t0 > budget:
                    print(f"Time budget of {args.time_budget} minutes reached, stopping cleanly. Run again to continue.")
                    break
                batch = todo[b: b + batch_size]
                n_blocked, retry_hint = 0, 0
                for url, rec, err in ex.map(_scrape_one, batch):
                    if rec is None:
                        if err.startswith("BLOCKED|"):
                            _, ra, why = err.split("|", 2)
                            n_blocked += 1
                            retry_hint = max(retry_hint, int(ra or 0))
                            err = f"blocked: {why}"
                        elif err.startswith("GONE|"):
                            new_gone.add(url)
                            err = f"page no longer exists (HTTP {err.split('|')[1]})"
                        failures[url] = err
                        print(f"  skipped ({err}): {url}")
                        continue
                    seq += 1
                    rec["seq"] = seq
                    rec["scraped_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
                    out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    out.flush()
                    have.add(url)
                    scraped += 1
                    if scraped % 25 == 0:
                        elapsed = (time.time() - t0) / 60
                        print(f"[{scraped}/{len(todo)}] {elapsed:.0f} min elapsed | {rec['title'][:50]} ({rec['source']})")

                # ---- react to the site pushing back: slow down, pause, and if it persists stop politely
                if n_blocked / len(batch) >= 0.3:
                    block_streak += 1
                    block_events += 1
                    shared_delay.value = min(max(shared_delay.value * 2, args.slow_delay), 30.0)
                    wait = max(retry_hint, min(args.cooldown * 2 ** (block_streak - 1), 900))
                    print(f"  The site is refusing requests ({n_blocked} of {len(batch)} in this batch). "
                          f"Slowing to ~{shared_delay.value:.1f}s between pages per browser; pausing {wait:.0f}s.")
                    if block_streak >= args.max_block_streak or time.time() - t0 + wait > budget:
                        blocked_stop = True
                        print("  Still blocked after repeated pauses. Stopping this run so the site is not hammered.")
                        break
                    time.sleep(wait)
                else:
                    if block_streak:
                        print("  Requests are going through again.")
                    block_streak = 0
                    shared_delay.value = max(float(args.delay), shared_delay.value * 0.85)
            final_delay = shared_delay.value
    if block_streak > 0:  # the run ended while the site was still refusing requests
        blocked_stop = True
    if final_delay is not None:
        DELAY_FILE.write_text(f"{final_delay:.2f}\n", encoding="utf-8")
    if new_gone:
        gone |= new_gone
        GONE.write_text("\n".join(sorted(gone)) + "\n", encoding="utf-8")

    left = [u for u in urls if u not in have and u not in gone]
    if left:
        REMAINING.write_text("\n".join(f"{u}\t{failures.get(u, 'not tried yet')}" for u in left) + "\n", encoding="utf-8")
    else:
        REMAINING.unlink(missing_ok=True)
    disc_complete = bool(state.get("complete"))
    new_urls = len(urls) - urls_before
    sm_set = set(SITEMAP_URLS.read_text(encoding="utf-8").split()) if SITEMAP_URLS.exists() else set()
    not_in_sitemap = len(set(urls) - sm_set) if sm_set else None
    sitemap_missing = len(sm_set - set(urls))
    enough = (not args.expected) or len(urls) >= args.expected
    # with a sitemap, completeness no longer depends on the scrolling having reached the end
    verified = (not left) and (disc_complete or bool(sm_set)) and enough and sitemap_missing == 0
    COMPLETENESS.write_text(json.dumps({
        "verified_complete": verified,
        "expected_minimum": args.expected,
        "urls_known": len(urls),
        "urls_scraped": len(have),
        "urls_still_missing": len(left),
        "urls_gone_404": len(gone),
        "stopped_because_blocked": blocked_stop,
        "block_events_this_run": block_events,
        "listing_scroll_complete": disc_complete,
        "sitemap_urls": len(sm_set),
        "sitemap_urls_not_yet_known": sitemap_missing,
        "known_urls_not_in_sitemap": not_in_sitemap,
        "enough_urls_found": enough,
        "updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }, indent=2), encoding="utf-8")
    STATUS.write_text(
        json.dumps({"total_urls": len(urls), "scraped_total": len(have), "scraped_this_run": scraped,
                    "failed_this_run": len(failures), "remaining": len(left),
                    "discovery_complete": disc_complete, "new_urls_found": new_urls,
                    "verified_complete": verified, "blocked": blocked_stop}),
        encoding="utf-8",
    )
    print("==== COMPLETENESS CHECK ====")
    print(f"  URLs known: {len(urls)}   scraped: {len(have)}   still missing: {len(left)}")
    if args.expected:
        print(f"  expected at least {args.expected}: {'OK' if enough else 'NOT REACHED, some prompts are still undiscovered'}")
    print(f"  listing scroll complete: {disc_complete}   sitemap URLs: {len(sm_set)}")
    if gone:
        print(f"  {len(gone)} pages no longer exist (404/410 confirmed twice); listed in {GONE}")
    if blocked_stop:
        print("  The site is refusing automated requests right now. Progress is saved. Options:\n"
              "   1) wait a few hours and run again (it resumes and keeps the slower pace);\n"
              "   2) run on your own PC with  --workers 1 --delay 3 ;\n"
              "   3) python scrape_faymas.py --warmup   (pass any check by hand in a real window, then reuse that session locally);\n"
              "   4) save pages yourself and load them with  --import-html <folder> ;\n"
              "   5) ask faymas.in for a data export or written permission.")
    print("  RESULT: " + ("COMPLETE, every known prompt is scraped and the counts agree" if verified
                          else "NOT YET COMPLETE, see data/completeness.json (the next run will continue)"))
    print(f"Run finished: {scraped} scraped, {len(failures)} failed, {len(left)} still to do "
          f"({len(have)} of {len(urls)} done overall)")
    if left:
        print(f"Missing prompts are listed in {REMAINING}")


def build_csvs(args):
    rows = load_cache()
    if not rows:
        print("Nothing in the cache yet, no CSVs written.")
        return
    state = load_cats()
    name_to_slug = {i.get("name", "").lower(): s for s, i in state["categories"].items() if i.get("name")}
    cat_name = {s: (i.get("name") or s) for s, i in state["categories"].items()}
    membership = {}
    for slug, info in state["categories"].items():
        for u in info["prompt_urls"]:
            membership.setdefault(u, []).append(slug)

    groups = {}  # slug -> (display name, rows)
    for r in sorted(rows, key=lambda r: r["seq"]):
        slugs = membership.get(r["url"])
        if not slugs:
            guess = (r.get("category") or "").strip()
            slugs = [name_to_slug.get(guess.lower()) or (slugify(guess) if guess else "uncategorized")]
        for s in slugs:
            name = cat_name.get(s) or ("Uncategorized" if s == "uncategorized" else s.replace("-", " ").title())
            groups.setdefault(s, (name, []))[1].append(r)

    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    files = total_rows = 0
    for slug, (name, items) in sorted(groups.items()):
        folder = OUT_DIR / slug
        folder.mkdir(parents=True, exist_ok=True)
        for start in range(0, len(items), CHUNK):
            chunk = items[start: start + CHUNK]
            lo, hi = start + 1, start + len(chunk)
            with (folder / f"{slug}_{lo}-{hi}.csv").open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                w.writeheader()
                for n, r in enumerate(chunk, lo):
                    prompt = clean(r["prompt"]) if args.flatten else r["prompt"]
                    w.writerow({"id": n, "category": name, "title": r["title"], "prompt": prompt,
                                "ai_model": r["ai_model"], "url": r["url"]})
            files += 1
        total_rows += len(items)
        print(f"{name}: {len(items)} prompts")
    unique = len({r["url"] for r in rows})
    print(f"Wrote {files} CSV files to {OUT_DIR}/ ({unique} unique prompts, {total_rows} rows; "
          f"a prompt listed in two categories appears in both)")
    if "uncategorized" in groups:
        print(f"NOTE: {len(groups['uncategorized'][1])} prompts were not found in any category listing.")
    bad = [r for r in rows if r.get("source") == "meta" or len((r.get("prompt") or "").strip()) < 60]
    if bad:
        DATA_DIR.mkdir(exist_ok=True)
        SUSPECT.write_text("\n".join(f"{r['url']}\t{r.get('source')}\t{len(r.get('prompt') or '')} chars" for r in bad) + "\n",
                           encoding="utf-8")
        print(f"NOTE: {len(bad)} prompts look short or came from fallback text; listed in {SUSPECT} for checking.")
    else:
        SUSPECT.unlink(missing_ok=True)
    short = sum(1 for r in rows if r.get("source") == "meta")
    if short:
        print(f"WARNING: {short} prompts came from the meta description and may be truncated.")


# ----------------------------------------------------------- diagnostics
def summarize(rec):
    p = rec["prompt"]
    print(f"    title    : {rec['title']}")
    print(f"    category : {rec['category'] or '(none found)'}")
    print(f"    ai_model : {rec['ai_model'] or '(none found)'}")
    print(f"    source   : {rec['source']}   prompt length: {len(p)} chars")
    print(f"    starts   : {p[:160]!r}")
    print(f"    ends     : {p[-120:]!r}")


def inspect(args):
    state = load_cats()
    set_known_categories([i.get("name") for i in state["categories"].values()] + ["Photography"])
    fetcher = Fetcher(render=args.render, delay=0)
    try:
        html = fetcher.html(args.inspect)
    finally:
        fetcher.close()
    if not html:
        sys.exit("Could not fetch that URL.")
    Path("debug_page.html").write_text(html, encoding="utf-8")
    summarize(parse_prompt_page(html, args.inspect))
    print("\nFull HTML saved to debug_page.html")


def explore(args):
    """Diagnostics to send back if something does not work: categories, scrolling, API calls, parsing."""
    EXPLORE_DIR.mkdir(parents=True, exist_ok=True)
    f = Fetcher(render=True, delay=0)
    page = f.page
    api, saved_api = [], []

    def on_response(r):
        if r.request.resource_type not in ("xhr", "fetch"):
            return
        api.append(f"{r.status} {r.request.resource_type} {r.url[:160]}")
        try:  # keep a few JSON replies that carry prompt text: they show how to read the site's own data feed
            if "json" in r.headers.get("content-type", "") and len(saved_api) < 8:
                body = r.text()
                if re.search(r'"(prompt|prompt_text|promptText|title|slug)"', body):
                    saved_api.append(r.url)
                    (EXPLORE_DIR / f"api_{len(saved_api)}.json").write_text(
                        json.dumps({"url": r.url, "body": body[:300000]}, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    page.on("response", on_response)
    try:
        print("== 1. Home page ==")
        page.goto(BASE, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(4000)
        (EXPLORE_DIR / "home.html").write_text(page.content(), encoding="utf-8")
        n = len(page.eval_on_selector_all("a[href*='/prompt/']", GRAB_JS))
        print(f"prompt links visible on first load: {n}")

        print("== 2. Category list from the sidebar ==")
        before = set(page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)"))
        cats = find_category_links(page)
        after = set(page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)"))
        print(f"categories found: {list(cats) or 'NONE'}")
        print("links that appeared after opening the sidebar section:")
        for h in sorted(after - before)[:60]:
            print("   ", h)

        slug = (list(cats) or SEED_CATEGORIES)[0]
        url = f"{BASE}/category/{slug}"
        print(f"== 3. Scrolling test on {url} (30 steps) ==")
        res = collect_listing(page, url, 30, time.time() + 600, verbose=True)
        (EXPLORE_DIR / "category.html").write_text(page.content(), encoding="utf-8")
        print(f"h1 title: {res['title']!r}; links after 30 scrolls: {len(res['links'])}; reached end: {res['ended']}")
        print("== 4. Pagination test ==")
        for n in (2, 3):
            page.goto(f"{url}?page={n}", wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(2500)
            links = {norm_url(h) for h in page.eval_on_selector_all("a[href*='/prompt/']", GRAB_JS)}
            print(f"?page={n}: {len(links)} links, {len(links - set(res['links'][:20]))} not in the first 20")
        print("== 5. Background requests seen (possible API) ==")
        if saved_api:
            print(f"   JSON replies saved to {EXPLORE_DIR}/api_*.json: " + ", ".join(u[:100] for u in saved_api))
        for line in list(dict.fromkeys(api))[:40]:
            print("   ", line)

        print("== 6. Parsing three prompt pages ==")
        set_known_categories([slug.replace("-", " ").title()] + ["Photography", "Product & Brand", "Social Media", "Wallpapers"])
        for i, purl in enumerate(res["links"][:3]):
            html = f.html(purl)
            print(f"  [{i + 1}] {purl}")
            if not html:
                print("    could not load")
                continue
            if i == 0:
                (EXPLORE_DIR / "prompt.html").write_text(html, encoding="utf-8")
            summarize(parse_prompt_page(html, purl))
    finally:
        f.close()
    print("\nDone. Please send this whole log back.")


# ------------------------------------------------- other ways to get data
def warmup(args):
    """Open a real browser window so YOU can pass any bot check / log in by hand, then save that session
    so later local runs reuse it (--storage-state). Cookies are tied to your PC/IP and may expire."""
    from playwright.sync_api import sync_playwright

    path = args.storage_state or str(DATA_DIR / "browser_state.json")
    DATA_DIR.mkdir(exist_ok=True)
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        ctx = browser.new_context(user_agent=HEADERS["User-Agent"], viewport={"width": 1400, "height": 900})
        page = ctx.new_page()
        page.goto(BASE, wait_until="domcontentloaded", timeout=60000)
        input("In the browser window: complete any check or login by hand, open a prompt page to be sure it loads, "
              "then press Enter here... ")
        ctx.storage_state(path=path)
        browser.close()
    print(f"Session saved to {path}. Use it with:  python scrape_faymas.py --render --storage-state {path}")
    print("Do NOT commit this file to GitHub: it contains your cookies.")


def import_html(args):
    """Read prompt pages you saved yourself (browser 'Save page as' / SingleFile) from a folder."""
    folder = Path(args.import_html)
    files = [f for f in folder.rglob("*") if f.suffix.lower() in (".html", ".htm")]
    if not files:
        sys.exit(f"No .html files found in {folder}")
    DATA_DIR.mkdir(exist_ok=True)
    cached = load_cache()
    have = {r["url"] for r in cached}
    seq = max((r["seq"] for r in cached), default=0)
    state = load_cats()
    set_known_categories([i.get("name") for i in state["categories"].values()] + ["Photography"])
    added = skipped = 0
    with CACHE.open("a", encoding="utf-8") as out:
        for f in sorted(files):
            html = f.read_text(encoding="utf-8", errors="replace")
            soup = BeautifulSoup(html, "lxml")
            link = soup.find("link", rel="canonical")
            u = (link.get("href") if link else "") or _meta(soup, "og:url")
            if "/prompt/" not in (u or ""):
                print(f"  skipped (no /prompt/ address found inside): {f.name}")
                skipped += 1
                continue
            url = _canon(u)
            if url in have:
                continue
            rec = parse_prompt_page(html, url)
            if not rec["prompt"]:
                print(f"  skipped (no prompt text found): {f.name}")
                skipped += 1
                continue
            seq += 1
            rec["seq"] = seq
            rec["scraped_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            have.add(url)
            save_urls([url])
            added += 1
    print(f"Imported {added} prompts from {folder} ({skipped} skipped). Rebuilding CSVs...")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--render", action="store_true", help="use headless Chromium for prompt pages")
    ap.add_argument("--explore", action="store_true", help="print diagnostics (categories, scrolling, parsing)")
    ap.add_argument("--rediscover", action="store_true", help="scroll every listing again to find new prompts")
    ap.add_argument("--category", action="append", help="extra category slug to include (repeatable)")
    ap.add_argument("--expected", type=int, default=0, help="minimum number of prompts the site should have (e.g. 22000)")
    ap.add_argument("--skip-listing", action="store_true", help="discover from the sitemap only (faster) if it lists every prompt")
    ap.add_argument("--storage-state", metavar="FILE", help="browser session saved with --warmup")
    ap.add_argument("--headed", action="store_true", help="show the browser window (local use)")
    ap.add_argument("--warmup", action="store_true", help="open a real window so you can pass a bot check by hand, save the session")
    ap.add_argument("--import-html", metavar="DIR", help="read prompt pages you saved yourself from a folder")
    ap.add_argument("--try-http-first", action="store_true", help="try a cheap plain request before the browser")
    ap.add_argument("--cooldown", type=float, default=60, help="first pause (seconds) when the site blocks; doubles each time")
    ap.add_argument("--slow-delay", type=float, default=2.0, help="minimum per-browser delay after a block")
    ap.add_argument("--max-block-streak", type=int, default=4, help="stop the run after this many blocked batches in a row")
    ap.add_argument("--skip-home", action="store_true", help="skip the home-page cross-check")
    ap.add_argument("--force", action="store_true", help="continue even if very few prompts were found")
    ap.add_argument("--workers", type=int, default=4, help="parallel browsers (default 4)")
    ap.add_argument("--delay", type=float, default=0.5, help="seconds each browser waits between pages")
    ap.add_argument("--max-scrolls", type=int, default=4000, help="max scroll steps per listing page")
    ap.add_argument("--crawl-minutes", type=int, default=150, help="time limit for finding all prompt links")
    ap.add_argument("--time-budget", type=int, default=320, help="stop scraping after this many minutes")
    ap.add_argument("--limit", type=int, default=0, help="max new prompts to scrape this run")
    ap.add_argument("--build-only", action="store_true", help="only rebuild CSVs from saved data")
    ap.add_argument("--flatten", action="store_true", help="collapse line breaks in prompts to single spaces")
    ap.add_argument("--inspect", metavar="URL", help="parse one prompt page and save its HTML")
    args = ap.parse_args()
    sys.stdout.reconfigure(line_buffering=True)

    if args.warmup:
        warmup(args)
        return
    if args.import_html:
        import_html(args)
        build_csvs(args)
        return
    if args.explore:
        explore(args)
        return
    if args.inspect:
        inspect(args)
        return
    if not args.build_only:
        scrape(args)
    build_csvs(args)


if __name__ == "__main__":
    main()
