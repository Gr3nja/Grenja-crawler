"""
crawler.py - シンプルWebクローラー
実行すると index.csv に結果を書き込みます

使い方:
  python crawler.py           （標準ライブラリのみ・pip不要）
  python crawler.py keyword   （キーワード付きクロール）
"""

import json
import csv
import time
import ssl
import sys
import urllib.request
import xml.etree.ElementTree as ET
from queue import Queue, Empty
from urllib.parse import urljoin, urlparse, urlunparse, quote
from html.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# =============================================
#                    設定
# =============================================
SEED_URLS = [
    # 日本語サイト
    "https://ja.wikipedia.org/",
    "https://en.wikipedia.org/",

]
MAX_PAGES   = 1000
MAX_DEPTH   = 3
DELAY       = 0.0     # robots.txt の Crawl-delay を優先するため0に
TIMEOUT     = 5       # 遅いサイトを早く諦める（秒）
OUTPUT_FILE = "index.csv"
MAX_WORKERS = 64      # 並列スレッド数（増やすほど速い、増やしすぎるとBANリスク）
# =============================================


# ── HTMLパーサー ──────────────────────────────────────────

class ContentParser(HTMLParser):
    def __init__(self, base_url):
        super().__init__()
        self.base_url = base_url
        self.title = ""
        self.links = []
        self._in_title = False
        self._skip = False

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "title":
            self._in_title = True
        elif tag in ("script", "style"):
            self._skip = True
        elif tag == "a" and "href" in attrs_dict:
            abs_url = urljoin(self.base_url, attrs_dict["href"])
            self.links.append(abs_url)

    def handle_endtag(self, tag):
        if tag == "title":
            self._in_title = False
        elif tag in ("script", "style"):
            self._skip = False

    def handle_data(self, data):
        if self._skip:
            return
        if self._in_title:
            self.title += data


# ── URL ユーティリティ ────────────────────────────────────

def normalize_url(url):
    p = urlparse(url)
    return urlunparse((p.scheme.lower(), p.netloc.lower(), p.path, p.params, p.query, ""))

def is_valid_url(url):
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return False
        if not p.netloc:
            return False
        bad_exts = (".pdf", ".jpg", ".jpeg", ".png", ".gif", ".zip", ".exe", ".mp4", ".mp3")
        if any(p.path.lower().endswith(e) for e in bad_exts):
            return False
        return True
    except Exception:
        return False


def load_existing_urls(filename):
    """既存のCSVファイルからURLを読み込む"""
    visited = set()
    try:
        with open(filename, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("url"):
                    visited.add(normalize_url(row["url"]))
        print(f"[INFO] 既存の{len(visited)}件を読み込みました")
    except FileNotFoundError:
        print(f"[INFO] {filename}はまだ存在しません。新規作成します")
    return visited

def generate_search_urls(keyword):
    """キーワードに基づいた検索URLを生成"""
    search_urls = [
        f"https://www.google.com/search?q={quote(keyword)}",
        f"https://www.bing.com/search?q={quote(keyword)}",
        f"https://duckduckgo.com/?q={quote(keyword)}",
        f"https://github.com/search?q={quote(keyword)}",
        f"https://stackoverflow.com/search?q={quote(keyword)}",
        f"https://zenn.dev/search?q={quote(keyword)}",
        f"https://qiita.com/search?q={quote(keyword)}",
    ]
    print(f"[INFO] キーワード'{keyword}'に基づいた検索URLを追加")
    return search_urls


# ── robots.txt チェック ───────────────────────────────────

class RobotsTxtParser:
    def __init__(self):
        self._disallowed = {}
        self._delay = {}
        self._lock = threading.Lock()  # スレッドセーフのために追加

    def fetch(self, domain):
        with self._lock:
            if domain in self._disallowed:
                return
            # プレースホルダーをセットして二重取得を防ぐ
            self._disallowed[domain] = []

        try:
            url = f"https://{domain}/robots.txt"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=5, context=_ssl_ctx) as res:
                text = res.read().decode("utf-8", errors="ignore")
            current_agent = None
            disallowed = []
            delay = None
            for line in text.splitlines():
                line = line.split("#")[0].strip()
                if not line:
                    continue
                parts = line.split(":", 1)
                if len(parts) != 2:
                    continue
                directive, value = parts[0].strip().lower(), parts[1].strip()
                if directive == "user-agent":
                    current_agent = value
                elif directive == "disallow" and current_agent == "*" and value:
                    disallowed.append(value)
                elif directive == "crawl-delay" and current_agent == "*":
                    try:
                        delay = float(value)
                    except ValueError:
                        pass
            with self._lock:
                self._disallowed[domain] = disallowed
                if delay is not None:
                    self._delay[domain] = delay
        except Exception as e:
            print(f"  [robots.txt] {domain} → {e}")

    def can_fetch(self, url):
        p = urlparse(url)
        for blocked in self._disallowed.get(p.netloc, []):
            if p.path.startswith(blocked):
                return False
        return True

    def crawl_delay(self, domain):
        return self._delay.get(domain, DELAY)


# ── SSL設定 ───────────────────────────────────────────────

_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "ja,en;q=0.9",
}

def _open(url, timeout=None):
    req = urllib.request.Request(url, headers=HEADERS)
    return urllib.request.urlopen(req, timeout=timeout or TIMEOUT, context=_ssl_ctx)


# ── sitemap / RSS ─────────────────────────────────────────

def fetch_sitemap(domain):
    urls = []
    for path in ("/sitemap.xml", "/sitemap_index.xml"):
        try:
            with _open(f"https://{domain}{path}") as res:
                root = ET.fromstring(res.read())
            for elem in root.iter():
                if elem.tag.endswith("loc") and elem.text:
                    u = elem.text.strip()
                    if is_valid_url(u):
                        urls.append(normalize_url(u))
        except Exception:
            pass
    return urls

def fetch_rss(domain):
    urls = []
    for path in ("/feed", "/rss", "/atom.xml", "/feed.xml"):
        try:
            with _open(f"https://{domain}{path}") as res:
                root = ET.fromstring(res.read())
            for elem in root.iter():
                if elem.tag.endswith("link") and elem.text:
                    u = elem.text.strip()
                    if is_valid_url(u):
                        urls.append(normalize_url(u))
        except Exception:
            pass
    return urls


# ── ページ取得 ────────────────────────────────────────────

def fetch_page(url):
    try:
        with _open(url) as res:
            ct = res.headers.get("Content-Type", "")
            if "text/html" not in ct:
                return None
            encoding = res.headers.get_content_charset("utf-8")
            return res.read().decode(encoding, errors="ignore")
    except Exception as e:
        print(f"  [SKIP] 取得失敗: {e}  ({url})")
        return None


# ── 共有状態 ─────────────────────────────────────────────

class SharedState:
    def __init__(self):
        self.visited = set()
        self.results = []
        self.seen_content = set()
        self.lock = threading.Lock()

    def add_result(self, item):
        with self.lock:
            # URLだけのキー（既存CSV由来）でも、URL+titleキーでも重複扱い
            url_key = f"{item['url']}|"
            content_key = f"{item['url']}|{item['title']}"
            if url_key in self.seen_content or content_key in self.seen_content:
                return False
            self.seen_content.add(url_key)
            self.seen_content.add(content_key)
            self.results.append(item)
            return True

    def mark_visited(self, url):
        with self.lock:
            self.visited.add(url)

    def is_visited(self, url):
        with self.lock:
            return url in self.visited

    def count(self):
        with self.lock:
            return len(self.results)


# ── メインクローラー ──────────────────────────────────────

def crawl(keyword=None):
    robots = RobotsTxtParser()
    shared_state = SharedState()

    # 既存URLを重複チェック用に読み込む（visitedには入れない＝子リンク探索を妨げない）
    existing_urls = load_existing_urls(OUTPUT_FILE)
    with shared_state.lock:
        # seen_content に登録しておくことで add_result での重複追加を防ぐ
        for u in existing_urls:
            shared_state.seen_content.add(f"{u}|")

    # URL単位の統合キュー（url, depth）
    url_queue = Queue()

    seed_list = SEED_URLS.copy()
    if keyword:
        seed_list.extend(generate_search_urls(keyword))

    # robots.txt 並列取得
    domains = list({urlparse(normalize_url(s)).netloc for s in seed_list})
    print(f"robots.txt を {len(domains)} ドメイン分取得中...")
    with ThreadPoolExecutor(max_workers=min(32, len(domains))) as ex:
        ex.map(robots.fetch, domains)

    # シードをキューへ投入
    for seed in seed_list:
        seed = normalize_url(seed)
        url_queue.put((seed, 0))
        # サイトマップ・RSS からも追加（シードドメインのみ）
        domain = urlparse(seed).netloc
        for u in fetch_sitemap(domain) + fetch_rss(domain):
            if urlparse(u).netloc == domain:
                url_queue.put((u, 1))

    print(f"\nクロール開始  最大{MAX_PAGES}ページ / 深さ{MAX_DEPTH} / スレッド{MAX_WORKERS}")
    if keyword:
        print(f"🔍 キーワード: '{keyword}'")
    print()

    start_time = time.time()

    def worker():
        while True:
            # 上限チェック
            if shared_state.count() >= MAX_PAGES:
                return

            try:
                url, depth = url_queue.get(timeout=3)
            except Empty:
                # キューが空 → 終了
                return

            try:
                url = normalize_url(url)

                if shared_state.is_visited(url) or not is_valid_url(url):
                    continue  # ← return だとスレッドが死ぬ
                if not robots.can_fetch(url):
                    continue  # ← 同上

                shared_state.mark_visited(url)

                if shared_state.count() >= MAX_PAGES:
                    continue

                current = shared_state.count() + 1
                print(f"[{current}/{MAX_PAGES}] {url}")

                html = fetch_page(url)
                if not html:
                    continue  # ← 同上

                parser = ContentParser(url)
                try:
                    parser.feed(html)
                except Exception as e:
                    print(f"  [SKIP] パース失敗: {e}")
                    continue  # ← 同上

                title = parser.title.strip()

                if shared_state.add_result({"url": url, "title": title[:200]}):
                    marker = "[KEY]" if keyword and (
                        keyword.lower() in title.lower() or keyword.lower() in url.lower()
                    ) else "  ✓"
                    elapsed = time.time() - start_time
                    pps = shared_state.count() / elapsed if elapsed > 0 else 0
                    print(f"  {marker} {title[:60] or '(no title)'}  [{pps:.1f} p/s]")
                else:
                    print(f"  [DUP] {title[:60] or '(no title)'}")

                # 子リンクをキューへ追加
                if depth < MAX_DEPTH:
                    for link in parser.links[:30]:
                        nl = normalize_url(link)
                        if not shared_state.is_visited(nl) and is_valid_url(nl):
                            new_domain = urlparse(nl).netloc
                            robots.fetch(new_domain)
                            url_queue.put((nl, depth + 1))

            finally:
                url_queue.task_done()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(worker) for _ in range(MAX_WORKERS)]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                print(f"スレッドエラー: {e}")

    elapsed = time.time() - start_time
    count = shared_state.count()
    print(f"\n⏱  {elapsed:.1f}秒  平均 {count/elapsed:.1f} pages/sec")

    return shared_state.results


# ── エントリポイント ──────────────────────────────────────

if __name__ == "__main__":
    keyword = sys.argv[1] if len(sys.argv) > 1 else None

    pages = crawl(keyword=keyword)

    # CSV書き出し（既存ファイルがあれば追記）
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing_pages = list(reader)
    except FileNotFoundError:
        existing_pages = []

    existing_urls = {p["url"] for p in existing_pages}
    new_pages = [p for p in pages if p["url"] not in existing_urls]
    all_pages = existing_pages + new_pages

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "title"])
        writer.writeheader()
        writer.writerows(all_pages)

    print(f"✅ 完了！  追加: {len(new_pages)} / 合計: {len(all_pages)} ページ → {OUTPUT_FILE}")