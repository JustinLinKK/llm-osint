"""
Download: fetch each URL, optionally save raw HTML, and extract main text.
"""
from __future__ import annotations

import re
import time
from pathlib import Path
from dataclasses import dataclass
from urllib.parse import urlparse

import requests
import trafilatura


@dataclass
class PageResult:
    """One result: URL plus extracted info about the person."""
    url: str
    title: str
    snippet: str
    main_text: str | None
    error: str | None = None
    html_path: str | None = None
    skipped: bool = False  # True if URL was skipped (already seen in a previous run)


# Browser-like headers to reduce 403 from sites that block minimal/bot requests
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

# Wikimedia (Wikipedia) requires a descriptive bot User-Agent with contact info.
# Using a browser UA for bot requests is against policy and can trigger 403.
# https://foundation.wikimedia.org/wiki/Policy:User-Agent_policy
WIKIMEDIA_BOT_HEADERS = {
    "User-Agent": "PersonSearch/1.0 (CSE227 course project; contact via course)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _headers_for_url(url: str) -> dict[str, str]:
    """Use bot-style headers for Wikimedia sites, browser-like for others."""
    if "wikipedia.org" in url or "wikimedia.org" in url:
        return WIKIMEDIA_BOT_HEADERS.copy()
    return DEFAULT_HEADERS.copy()


def fetch_and_extract(url: str, timeout: float = 10.0) -> tuple[str | None, str | None, str | None]:
    """
    Fetch URL and extract main text using trafilatura.
    Returns (main_text, error_message, raw_html). On success error is None; on failure main_text and raw_html are None.
    Uses requests for all fetches: its TLS fingerprint is less often blocked than httpx by CDNs (e.g. factually.co, Wikipedia).
    """
    headers = _headers_for_url(url)
    try:
        resp = requests.get(
            url,
            timeout=timeout,
            allow_redirects=True,
            headers=headers,
        )
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        return None, str(e), None

    main = trafilatura.extract(html, include_comments=False, include_tables=False)
    return main, None, html


def normalize_url(url: str) -> str:
    """Simple normalization to avoid obvious duplicates (same path)."""
    try:
        p = urlparse(url)
        return (p.netloc or "") + (p.path or "") or url
    except Exception:
        return url


def _url_to_safe_filename(url: str, index: int) -> str:
    """Build a safe filesystem name from URL and index."""
    try:
        p = urlparse(url)
        name = (p.netloc or "").replace(".", "_") + "_" + (p.path.strip("/").replace("/", "_") or "index")
    except Exception:
        name = f"page_{index}"
    name = re.sub(r'[\\/:*?"<>|]', "_", name)[:120]
    return f"{index:02d}_{name}.html"


def _load_seen_urls(path: Path) -> set[str]:
    """Load normalized URLs from a text file (one per line)."""
    if not path.exists():
        return set()
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").strip().splitlines()
        return {normalize_url(line.strip()) for line in lines if line.strip()}
    except Exception:
        return set()


def _record_seen_url(path: Path, url: str) -> None:
    """Append one normalized URL to the seen file."""
    key = normalize_url(url)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(key + "\n")
    except Exception:
        pass


def download_pages(
    items: list[dict],
    fetch_delay_seconds: float = 1.0,
    request_timeout: float = 10.0,
    download_dir: str | Path | None = None,
    seen_urls_file: str | Path | None = None,
    use_seen_cache: bool = True,
) -> list[PageResult]:
    """
    Download pages from a list of search-style items (each with 'href' or 'url', optional 'title', 'body').
    Fetches each URL, optionally saves raw HTML under download_dir, extracts main text.
    If use_seen_cache is True, URLs in seen_urls_file are skipped (no refetch).
    """
    if not items:
        return []

    out_dir = Path(download_dir) if download_dir else None
    if out_dir is not None:
        out_dir.mkdir(parents=True, exist_ok=True)

    seen_path = Path(seen_urls_file) if seen_urls_file else Path.cwd() / "person_search_seen_urls.txt"
    seen = _load_seen_urls(seen_path) if use_seen_cache else set()

    out: list[PageResult] = []
    saved_index = 0
    for i, item in enumerate(items):
        url = item.get("href") or item.get("url", "")
        if not url or not url.startswith("http"):
            continue
        title = item.get("title", "")
        snippet = item.get("body", "")
        url_key = normalize_url(url)

        if use_seen_cache and url_key in seen:
            out.append(
                PageResult(
                    url=url,
                    title=title,
                    snippet=snippet,
                    main_text=None,
                    error="skipped (already fetched)",
                    html_path=None,
                    skipped=True,
                )
            )
            continue

        if i > 0 and fetch_delay_seconds > 0:
            time.sleep(fetch_delay_seconds)

        main_text, err, raw_html = fetch_and_extract(url, timeout=request_timeout)
        if use_seen_cache:
            _record_seen_url(seen_path, url)
            seen.add(url_key)

        html_path = None
        if out_dir is not None and raw_html is not None:
            saved_index += 1
            fname = _url_to_safe_filename(url, saved_index)
            path = out_dir / fname
            path.write_text(raw_html, encoding="utf-8", errors="replace")
            html_path = str(path)

        out.append(
            PageResult(
                url=url,
                title=title,
                snippet=snippet,
                main_text=main_text,
                error=err,
                html_path=html_path,
                skipped=False,
            )
        )

    return out
