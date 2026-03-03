from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

from archive.common import html_to_text
from technical.common import clean_text, extract_emails, extract_social_links, http_request


ARTICLE_REGEX = re.compile(r"https://([a-z0-9-]+)\.substack\.com/p/[A-Za-z0-9_-]+", re.IGNORECASE)
AUTHOR_REGEX = re.compile(r"\b(?:by|author)\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,3})\b")


def _parse_inputs(input_data: Dict[str, Any]) -> Tuple[str, str]:
    url = str(input_data.get("url") or input_data.get("profile_url") or "").strip()
    subdomain = str(input_data.get("subdomain") or input_data.get("username") or "").strip().lower()
    if url.startswith(("http://", "https://")):
        host = (urlparse(url).hostname or "").lower()
        if host.endswith(".substack.com"):
            subdomain = host.split(".substack.com", 1)[0]
    if not subdomain:
        raise RuntimeError("Missing required input: subdomain, username, or url")
    if not url:
        url = f"https://{subdomain}.substack.com"
    return subdomain, url


def _fetch_page(url: str) -> Tuple[str, str, str]:
    _, _, body, final_url = http_request(url, timeout=20)
    return body, html_to_text(body, max_len=6000), final_url


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    subdomain, base_url = _parse_inputs(input_data)
    home_html, home_text, final_url = _fetch_page(base_url)
    about_url = final_url.rstrip("/") + "/about"
    try:
        about_html, about_text, about_final_url = _fetch_page(about_url)
    except Exception:
        about_html, about_text, about_final_url = "", "", about_url

    combined_html = " ".join(part for part in (home_html, about_html) if part)
    combined_text = " ".join(part for part in (home_text, about_text) if part)
    author_match = AUTHOR_REGEX.search(combined_text)
    article_urls: List[str] = []
    for match in ARTICLE_REGEX.finditer(combined_html):
        article_urls.append(match.group(0))

    return {
        "tool": "substack_author_search",
        "subdomain": subdomain,
        "author_name": clean_text(author_match.group(1) if author_match else "", max_len=120),
        "bio": clean_text(about_text or home_text, max_len=500),
        "social_links": extract_social_links(combined_html, max_items=10),
        "emails": extract_emails(combined_text, max_items=5),
        "articles": list(dict.fromkeys(article_urls))[:10],
        "profile_url": final_url,
        "about_url": about_final_url,
        "confidence": 0.7 if combined_text else 0.0,
    }

