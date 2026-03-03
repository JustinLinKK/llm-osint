"""
Composed workflow: search (DuckDuckGo) then download (fetch + save HTML).

Call run_workflow() for both steps, or use search.search_pages() and
download.download_pages() separately.
"""
from __future__ import annotations

from pathlib import Path

from .download import PageResult, download_pages
from .search import search_pages


def run_workflow(
    name: str,
    max_search_results: int = 10,
    fetch_delay_seconds: float = 1.0,
    request_timeout: float = 10.0,
    download_dir: str | Path | None = None,
    seen_urls_file: str | Path | None = None,
    use_seen_cache: bool = True,
) -> list[PageResult]:
    """
    Full workflow: search for pages about the person (DuckDuckGo), then download each and extract content.
    If download_dir is set, save each page's raw HTML there (filename derived from URL).
    If use_seen_cache is True, URLs already recorded in seen_urls_file are skipped (no refetch).
    """
    search_results = search_pages(name, max_results=max_search_results)
    return download_pages(
        search_results,
        fetch_delay_seconds=fetch_delay_seconds,
        request_timeout=request_timeout,
        download_dir=download_dir,
        seen_urls_file=seen_urls_file,
        use_seen_cache=use_seen_cache,
    )
