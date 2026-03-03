"""
Search: use DuckDuckGo to find URLs for a query (e.g. a person's name).
"""
from __future__ import annotations

from ddgs import DDGS


def search_pages(query: str, max_results: int = 10) -> list[dict]:
    """
    Search for web pages containing the query (e.g. a person's name).
    Returns list of dicts with keys: title, href, body (snippet).
    """
    results = []
    try:
        raw = DDGS().text(query, max_results=max_results)
        for r in raw:
            results.append({
                "title": r.get("title", ""),
                "href": r.get("href", r.get("url", "")),
                "body": r.get("body", ""),
            })
    except Exception as e:
        raise RuntimeError(f"Search failed: {e}") from e
    return results
