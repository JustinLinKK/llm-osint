from __future__ import annotations

import re
from typing import Any, Dict, List
from urllib.parse import urljoin

from archive.common import fetch_text
from technical.common import clean_text


STAFF_REGEX = re.compile(
    r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s*(?:-|,|\|)\s*([A-Za-z][A-Za-z/& ,.-]{2,80})$"
)


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    org_url = str(input_data.get("org_url") or input_data.get("url") or "").strip()
    if not org_url.startswith(("http://", "https://")):
        raise RuntimeError("Missing required input: org_url")

    candidate_urls = [
        urljoin(org_url.rstrip("/") + "/", path)
        for path in ("team", "about", "people", "staff")
    ]
    staff: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for url in candidate_urls:
        try:
            text, final_url = fetch_text(url, timeout=15, max_len=5000)
        except Exception:
            continue
        segments = [segment.strip() for segment in re.split(r"(?:\.\s+|[\n\r;]+)", text) if segment.strip()]
        for segment in segments:
            match = STAFF_REGEX.search(segment)
            if not match:
                continue
            name = clean_text(match.group(1), max_len=120)
            title = clean_text(match.group(2), max_len=120)
            dedupe_key = (name.lower(), title.lower())
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            staff.append({"name": name, "title": title, "source_url": final_url})
            if len(staff) >= 20:
                break
        if len(staff) >= 20:
            break

    return {
        "tool": "org_staff_page_search",
        "org_url": org_url,
        "staff": staff,
    }
