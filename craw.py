"""
crawler.py - 高速Webクローラー（asyncio + aiohttp）
実行すると index.csv に結果を書き込みます

使い方:
  pip install aiohttp
  python crawler.py           （キーワードなし）
  python crawler.py keyword   （キーワード付きクロール）
"""

import asyncio
import csv
import sys
import time
import ssl
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse, urlunparse, quote
from html.parser import HTMLParser

import aiohttp

# Windows対応
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# =============================================
#                    設定
# =============================================
SEED_URLS = [
    "https://zenn.dev/",
    "https://qiita.com/",
    "https://ja.wikipedia.org/",
    "https://note.com/",
    "https://github.com/",
    "https://dev.to/",
    "https://news.ycombinator.com/",
    "https://www.techcrunch.com/",
]
MAX_PAGES       = 500
MAX_DEPTH       = 3
TIMEOUT         = 5
OUTPUT_FILE     = "index.csv"
MAX_CONCURRENT  = 20
MAX_LINKS       = 30
# =============================================


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "ja,en;q=0.9",
}


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

def normalize_url(url: str) -> str:
    p = urlparse(url)
    return urlunparse((p.scheme.lower(), p.netloc.lower(), p.path, p.params, p.query, ""))

def is_valid_url(url: str) -> bool:
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

def load_existing_urls(filename: str) -> set:
    urls = set()
    try:
        with open(filename, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("url"):
                    urls.add(normalize_url(row["url"]))
        print(f"[INFO] 既存の{len(urls)}件を読み込みました")
    except FileNotFoundError:
        print(f"[INFO] {filename}はまだ存在しません。新規作成します")
    return urls

def generate_search_urls(keywords: str | list) -> list:
    """キーワード（複数可）に基づいた検索URLを生成"""
    if isinstance(keywords, str):
        keywords = [keywords]
    
    urls = []
    for keyword in keywords:
        q = quote(keyword)
        urls.extend([
            f"https://www.google.com/search?q={q}",
            f"https://www.bing.com/search?q={q}",
            f"https://duckduckgo.com/?q={q}",
            f"https://github.com/search?q={q}",
            f"https://stackoverflow.com/search?q={q}",
            f"https://zenn.dev/search?q={q}",
            f"https://qiita.com/search?q={q}",
        ])
    print(f"[INFO] {len(keywords)}個のキーワードに基づいた検索URL({len(urls)}件)を追加")
    return urls


def calc_keyword_score(url: str, title: str, keywords: list) -> float:
    """キーワードに対する関連度スコア(0.0-1.0)を計算
    
    キーワード単語との完全一致のみ対象
    例: "apple"は"pineapple"には一致しない（完全一致のみ）
    """
    if not keywords:
        return 0.0
    
    score = 0.0
    title_lower = title.lower()
    url_lower = url.lower()
    
    for kw in keywords:
        kw_lower = kw.lower()
        
        # タイトルで完全一致する単語があるか（スペース区切り）
        title_words = title_lower.split()
        if kw_lower in title_words:
            score += 1.0
            continue
        
        # URLで完全一致する単語があるか（スラッシュ・ハイフン・ドット区切り）
        url_words = url_lower.replace('-', ' ').replace('/', ' ').replace('.', ' ').split()
        if kw_lower in url_words:
            score += 0.5
    
    return min(score / len(keywords), 1.0)


# ── robots.txt（async版） ─────────────────────────────────

class RobotsTxtParser:
    def __init__(self):
        self._disallowed: dict = {}
        self._delay: dict = {}
        self._fetching: set = set()
        self._lock = asyncio.Lock()

    async def fetch(self, session: aiohttp.ClientSession, domain: str):
        async with self._lock:
            if domain in self._disallowed or domain in self._fetching:
                return
            self._fetching.add(domain)

        disallowed = []
        delay = None
        try:
            url = f"https://{domain}/robots.txt"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as res:
                text = await res.text(errors="ignore")
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
                    disallowed.append(value)
                elif directive == "crawl-delay" and current_agent == "*":
                    try:
                        delay = float(value)
                    except ValueError:
                        pass
        except Exception as e:
            print(f"  [robots.txt] {domain} → {e}")
        finally:
            async with self._lock:
                self._disallowed[domain] = disallowed
                if delay is not None:
                    self._delay[domain] = delay
                self._fetching.discard(domain)

    def can_fetch(self, url: str) -> bool:
        p = urlparse(url)
        for blocked in self._disallowed.get(p.netloc, []):
            if p.path.startswith(blocked):
                return False
        return True

    def is_fetched(self, domain: str) -> bool:
        return domain in self._disallowed


# ── sitemap / RSS（async版） ──────────────────────────────

async def fetch_sitemap(session: aiohttp.ClientSession, domain: str) -> list:
    urls = []
    for path in ("/sitemap.xml", "/sitemap_index.xml"):
        try:
            async with session.get(
                f"https://{domain}{path}",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as res:
                text = await res.text(errors="ignore")
            root = ET.fromstring(text)
            for elem in root.iter():
                if elem.tag.endswith("loc") and elem.text:
                    u = elem.text.strip()
                    if is_valid_url(u):
                        urls.append(normalize_url(u))
        except Exception:
            pass
    return urls

async def fetch_rss(session: aiohttp.ClientSession, domain: str) -> list:
    urls = []
    for path in ("/feed", "/rss", "/atom.xml", "/feed.xml"):
        try:
            async with session.get(
                f"https://{domain}{path}",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as res:
                text = await res.text(errors="ignore")
            root = ET.fromstring(text)
            for elem in root.iter():
                if elem.tag.endswith("link") and elem.text:
                    u = elem.text.strip()
                    if is_valid_url(u):
                        urls.append(normalize_url(u))
        except Exception:
            pass
    return urls


# ── メインクローラー ──────────────────────────────────────

async def crawl_async(keywords: str | list | None = None) -> list:
    """キーワード対応のクローラー
    
    Args:
        keywords: 単一キーワード(str)、複数キーワード(list)、またはNone
    """
    robots = RobotsTxtParser()
    visited: set = set()
    seen_urls: set = set()
    results: list = []
    start_time = time.time()
    
    # キーワード正規化
    if isinstance(keywords, str):
        keywords = [keywords] if keywords else []
    elif keywords is None:
        keywords = []
    
    keywords = [kw.strip() for kw in keywords if kw.strip()]

    existing_urls = load_existing_urls(OUTPUT_FILE)

    queue: asyncio.Queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    seed_list = SEED_URLS.copy()
    if keywords:
        seed_list.extend(generate_search_urls(keywords))

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit=MAX_CONCURRENT)

    async with aiohttp.ClientSession(
        connector=connector,
        headers=HEADERS,
        timeout=aiohttp.ClientTimeout(total=TIMEOUT),
    ) as session:

        domains = list({urlparse(normalize_url(s)).netloc for s in seed_list})
        print(f"robots.txt を {len(domains)} ドメイン分取得中...")
        await asyncio.gather(*[robots.fetch(session, d) for d in domains])

        for seed in seed_list:
            await queue.put((normalize_url(seed), 0))

        print("サイトマップ・RSS 取得中...")
        sitemap_results = await asyncio.gather(*[fetch_sitemap(session, d) for d in domains])
        rss_results     = await asyncio.gather(*[fetch_rss(session, d)     for d in domains])
        for domain, sm_urls, rss_urls in zip(domains, sitemap_results, rss_results):
            for u in sm_urls + rss_urls:
                if urlparse(u).netloc == domain:
                    await queue.put((u, 1))

        print(f"\nクロール開始  最大{MAX_PAGES}ページ / 深さ{MAX_DEPTH} / 同時{MAX_CONCURRENT}")
        if keywords:
            print(f"🔍 キーワード: {', '.join(keywords)}")
        print()

        # ── ワーカー ──────────────────────────────────────
        stop = False

        async def worker():
            nonlocal stop

            while True:
                try:
                    url, depth = await queue.get()  # 空なら新アイテムが来るまでブロック
                except asyncio.CancelledError:
                    return

                try:
                    if stop:
                        continue  # stop済みでも task_done() は呼ぶ

                    url = normalize_url(url)

                    if url in visited or not is_valid_url(url):
                        continue
                    if url in seen_urls:
                        visited.add(url)
                        continue

                    domain = urlparse(url).netloc
                    if not robots.is_fetched(domain):
                        await robots.fetch(session, domain)
                    if not robots.can_fetch(url):
                        continue

                    visited.add(url)

                    async with semaphore:
                        try:
                            async with session.get(url) as res:
                                ct = res.headers.get("Content-Type", "")
                                if "text/html" not in ct:
                                    continue
                                html = await res.text(errors="ignore")
                        except asyncio.CancelledError:
                            return
                        except Exception as e:
                            print(f"  [SKIP] {e}  ({url})")
                            continue

                    # Cloudflare / bot検知ページをスキップ
                    if "<title>Just a moment" in html or "cf-browser-verification" in html or "Checking your browser" in html:
                        print(f"  [CF]  Cloudflare検知、スキップ  ({url})")
                        continue

                    parser = ContentParser(url)
                    try:
                        parser.feed(html)
                    except Exception as e:
                        print(f"  [SKIP] パース失敗: {e}")
                        continue

                    title = parser.title.strip()
                    seen_urls.add(url)
                    
                    # キーワードスコア計算
                    kw_score = calc_keyword_score(url, title, keywords)
                    results.append({
                        "url": url,
                        "title": title[:200],
                        "_score": kw_score
                    })

                    elapsed = time.time() - start_time
                    pps = len(results) / elapsed if elapsed > 0 else 0
                    
                    # 表示: キーワード関連度を可視化
                    if keywords and kw_score > 0:
                        bar = "█" * int(kw_score * 5)
                        marker = f"[{bar:<5}]"
                    else:
                        marker = "  ✓  "
                    
                    print(f"[{len(results)}/{MAX_PAGES}] {marker} {title[:60] or '(no title)'}  [{pps:.1f} p/s]")

                    if len(results) >= MAX_PAGES:
                        stop = True

                    # 子リンクをキューへ（stop済みなら追加しない）
                    if not stop and depth < MAX_DEPTH:
                        for link in parser.links[:MAX_LINKS]:
                            nl = normalize_url(link)
                            if nl not in visited and nl not in seen_urls and is_valid_url(nl):
                                await queue.put((nl, depth + 1))

                except asyncio.CancelledError:
                    return
                finally:
                    queue.task_done()  # 必ずここで通知

        # ワーカー起動
        tasks = [asyncio.create_task(worker()) for _ in range(MAX_CONCURRENT)]

        # queue.join() = キューのアイテムが全て task_done() されるまで待つ
        await queue.join()

        # 全ワーカーを停止
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.time() - start_time
    count = len(results)
    print(f"\n⏱  {elapsed:.1f}秒  平均 {count / elapsed:.1f} pages/sec")
    return results


# ── エントリポイント ──────────────────────────────────────

if __name__ == "__main__":
    # コマンドライン引数でキーワードを取得（複数対応: -k kw1 kw2 kw3）
    keywords = None
    if len(sys.argv) > 1:
        if sys.argv[1] == "-k" and len(sys.argv) > 2:
            keywords = sys.argv[2:]
        else:
            keywords = [sys.argv[1]]
    
    pages = asyncio.run(crawl_async(keywords=keywords))
    
    # キーワード指定時: スコアでソート
    if keywords:
        pages.sort(key=lambda p: p.get("_score", 0), reverse=True)
    
    # 常に _score フィールドを削除（CSV出力に不要）
    for p in pages:
        p.pop("_score", None)

    # CSV書き出し（既存ファイルがあれば追記）
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            existing_pages = list(csv.DictReader(f))
    except FileNotFoundError:
        existing_pages = []

    existing_urls = {p["url"] for p in existing_pages}
    new_pages  = [p for p in pages if p["url"] not in existing_urls]
    all_pages  = existing_pages + new_pages

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "title"])
        writer.writeheader()
        writer.writerows(all_pages)

    print(f"✅ 完了！  追加: {len(new_pages)} / 合計: {len(all_pages)} ページ → {OUTPUT_FILE}")
    if keywords:
        print(f"   キーワード: {', '.join(keywords)} (関連度スコア順にソート)")