from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

from archive.common import html_to_text
from technical.common import clean_text, extract_social_links, http_request


ARTICLE_REGEX = re.compile(r"https://medium\.com/@([A-Za-z0-9_.-]+)/([A-Za-z0-9-]+)", re.IGNORECASE)


def _parse_inputs(input_data: Dict[str, Any]) -> Tuple[str, str]:
    profile_url = str(input_data.get("profile_url") or input_data.get("url") or "").strip()
    username = str(input_data.get("username") or "").strip().lstrip("@")
    if profile_url.startswith(("http://", "https://")):
        parsed = urlparse(profile_url)
        path = parsed.path.strip("/")
        if path.startswith("@"):
            username = path[1:].split("/", 1)[0]
    if not username:
        raise RuntimeError("Missing required input: username or profile_url")
    if not profile_url:
        profile_url = f"https://medium.com/@{username}"
    return username, profile_url


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    username, profile_url = _parse_inputs(input_data)
    _, _, body, final_url = http_request(profile_url, timeout=20)
    text = html_to_text(body, max_len=6000)
    article_urls: List[str] = []
    for match in ARTICLE_REGEX.finditer(body):
        article_username = match.group(1)
        if article_username.lower() != username.lower():
            continue
        article_urls.append(match.group(0))

    return {
        "tool": "medium_author_search",
        "username": username,
        "bio": clean_text(text, max_len=500),
        "articles": list(dict.fromkeys(article_urls))[:10],
        "linked_accounts": extract_social_links(body, max_items=10),
        "profile_url": final_url,
        "confidence": 0.7 if text else 0.0,
    }

