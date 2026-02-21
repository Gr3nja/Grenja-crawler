"""
crawler.py - シンプルWebクローラー
実行すると index.csv に結果を書き込みます

使い方:
  python crawler.py   （標準ライブラリのみ・pip不要）
"""

import json
import csv
import time
import ssl
import sys
import urllib.request
import xml.etree.ElementTree as ET
from collections import deque
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
    "https://zenn.dev/",
    "https://qiita.com/",
    "https://note.com/",
    # 海外サイト - 百科事典
    "https://en.wikipedia.org/",
    # 海外サイト - ニュース
    "https://www.bbc.com/",
    "https://www.cnn.com/",
    "https://news.google.com/",
    # 技術系
    "https://github.com/",
    "https://dev.to/",
    "https://www.techcrunch.com/",
    "https://www.wired.com/",
    "https://slashdot.org/",
    "https://news.ycombinator.com/",
    "https://arxiv.org/",
    # その他
    "https://www.quora.com/",
    "https://www.tumblr.com/",
    "https://wordpress.com/",
    "https://www.blogger.com/",
    "https://readthedocs.org/",
    "https://github.com/Gr3nja/",
    "https://ja.wikipedia.org/wiki/",
    "https://www.yahoo.co.jp/",
    "https://www.facebook.com/",
    "https://www.cloudflare.com/ja-jp/",
    "https://claude.ai/",


]
MAX_PAGES   = 1000
MAX_DEPTH   = 3
DELAY       = 0.01
OUTPUT_FILE = "index.csv"
MAX_WORKERS = 10  # 並列スレッド数
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

    def fetch(self, domain):
        if domain in self._disallowed:
            return
        self._disallowed[domain] = []
        try:
            url = f"https://{domain}/robots.txt"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=5, context=_ssl_ctx) as res:
                text = res.read().decode("utf-8", errors="ignore")
            current_agent = None
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
                    self._disallowed[domain].append(value)
                elif directive == "crawl-delay" and current_agent == "*":
                    try:
                        self._delay[domain] = float(value)
                    except ValueError:
                        pass
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

# ブラウザに近いUser-Agentで弾かれにくくする
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "ja,en;q=0.9",
}

def _open(url, timeout=10):
    req = urllib.request.Request(url, headers=HEADERS)
    return urllib.request.urlopen(req, timeout=timeout, context=_ssl_ctx)


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
                print(f"  [SKIP] Content-Typeが対象外: {ct[:50]}  ({url})")
                return None
            encoding = res.headers.get_content_charset("utf-8")
            return res.read().decode(encoding, errors="ignore")
    except Exception as e:
        print(f"  [SKIP] 取得失敗: {e}  ({url})")
        return None


# ── メインクローラー ──────────────────────────────────────

class SharedState:
    """複数スレッド間で共有する状態"""
    def __init__(self):
        self.visited = set()
        self.results = []
        self.seen_content = set()  # URL+タイトル組み合わせで重複排除
        self.lock = threading.Lock()
    
    def add_result(self, item):
        with self.lock:
            # URL+タイトルのハッシュで重複排除
            content_key = f"{item['url']}|{item['title']}"
            if content_key not in self.seen_content:
                self.seen_content.add(content_key)
                self.results.append(item)
                return True
            return False
    
    def mark_visited(self, url):
        with self.lock:
            self.visited.add(url)
    
    def is_visited(self, url):
        with self.lock:
            return url in self.visited

def crawl_domain(domain, urls, shared_state, robots, keyword=None):
    """特定ドメインをクロール"""
    domain_last = 0
    local_visited = set()
    
    while urls:
        if len(shared_state.results) >= MAX_PAGES:
            break
        
        url = urls.pop(0)
        url = normalize_url(url)
        
        if url in local_visited or shared_state.is_visited(url):
            continue
        if not is_valid_url(url):
            continue
        if not robots.can_fetch(url):
            continue
        
        local_visited.add(url)
        shared_state.mark_visited(url)
        
        # クロール遅延
        delay = robots.crawl_delay(domain)
        wait = delay - (time.time() - domain_last)
        if wait > 0:
            time.sleep(wait)
        domain_last = time.time()
        
        with shared_state.lock:
            page_num = len(shared_state.results) + 1
            if page_num > MAX_PAGES:
                break
            print(f"[{page_num}/{MAX_PAGES}] {url}")
        
        html = fetch_page(url)
        if not html:
            continue
        
        parser = ContentParser(url)
        try:
            parser.feed(html)
        except Exception as e:
            print(f"  [SKIP] パース失敗: {e}")
            continue
        
        title = parser.title.strip()  # タイトルが空の場合は空文字列を使う
        
        if shared_state.add_result({
            "url":   url,
            "title": title[:200],
        }):
            # 実際に追加された場合のみ表示
            is_keyword_match = keyword and (keyword.lower() in title.lower() or keyword.lower() in url.lower())
            marker = "[KEY]" if is_keyword_match else "  ✓"
            print(f"  {marker} {title[:60] if title else '(no title)'}")
        else:
            # 重複の場合
            print(f"  [DUP] {title[:60] if title else '(no title)'}")
        
        # 新しいリンクをキューに追加
        # キーワードを含むリンクを優先して処理
        keyword_links = []
        other_links = []
        for link in parser.links[:30]:
            nl = normalize_url(link)
            new_domain = urlparse(nl).netloc
            if not shared_state.is_visited(nl) and is_valid_url(nl):
                if new_domain == domain:
                    if keyword and keyword.lower() in nl.lower():
                        keyword_links.append(nl)
                    else:
                        other_links.append(nl)
        
        # キーワードリンクを先粗に処理
        if keyword:
            urls = keyword_links + other_links + urls
        else:
            urls = other_links + urls

def crawl(keyword=None):
    robots = RobotsTxtParser()
    shared_state = SharedState()
    domain_queues = {}
    
    # 既存のCSVから訪問済みURLを読み込む
    existing_urls = load_existing_urls(OUTPUT_FILE)
    shared_state.visited.update(existing_urls)
    
    # 各ドメインのキューを初期化
    seed_list = SEED_URLS.copy()
    if keyword:
        seed_list.extend(generate_search_urls(keyword))
    
    for seed in seed_list:
        seed = normalize_url(seed)
        domain = urlparse(seed).netloc
        print(f"robots.txt 取得中: {domain}")
        robots.fetch(domain)
        
        if domain not in domain_queues:
            domain_queues[domain] = []
        domain_queues[domain].append(seed)
        
        # サイトマップとRSSを取得
        for u in fetch_sitemap(domain) + fetch_rss(domain):
            if urlparse(u).netloc == domain:
                domain_queues[domain].append(u)
    
    print(f"\nクロール開始  最大{MAX_PAGES}ページ / 深さ{MAX_DEPTH}")
    if keyword:
        print(f"🔍 キーワード: '{keyword}' \n")
    else:
        print()
    
    # ThreadPoolExecutorで並列処理
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for domain, urls in domain_queues.items():
            future = executor.submit(crawl_domain, domain, urls, shared_state, robots, keyword)
            futures.append(future)
        
        # すべてのスレッドが完了するのを待つ
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"スレッドエラー: {e}")
    
    return shared_state.results


# ── エントリポイント ──────────────────────────────────────

if __name__ == "__main__":
    # コマンドライン引数でキーワードを取得
    keyword = sys.argv[1] if len(sys.argv) > 1 else None
    
    pages = crawl(keyword=keyword)

    # CSVフォーマットで出力（既存ファイルがあれば追加）
    try:
        existing_pages = []
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing_pages = list(reader) if reader else []
    except FileNotFoundError:
        existing_pages = []
    
    # 既存と新規を設合
    existing_urls = {p["url"] for p in existing_pages}
    new_pages = [p for p in pages if p["url"] not in existing_urls]
    
    all_pages = existing_pages + new_pages
    
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "title"])
        writer.writeheader()
        writer.writerows(all_pages)

    print(f"\n✅ 完了！  追加: {len(new_pages)} / 合計: {len(all_pages)} ページ → {OUTPUT_FILE}")