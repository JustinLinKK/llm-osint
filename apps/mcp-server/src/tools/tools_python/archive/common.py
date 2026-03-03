from __future__ import annotations

import re
from typing import Any

from technical.common import clean_text, http_request


TITLE_PATTERNS = [
    re.compile(r"\b(?:software engineer|staff engineer|principal engineer|engineer|research scientist|scientist|founder|cto|ceo|director|manager|developer advocate)\b", re.IGNORECASE),
]
LOCATION_PATTERNS = [
    re.compile(r"\b(?:based in|location[:\s]+|from )([A-Z][A-Za-z]+(?:,\s*[A-Z][A-Za-z]+)?)"),
]
EMPLOYMENT_PATTERNS = [
    re.compile(r"\b(?:at|works at|working at|joined|joining)\s+([A-Z][A-Za-z0-9& .-]{1,80})"),
]


def html_to_text(html: str, max_len: int = 6000) -> str:
    text = re.sub(r"(?is)<script\b[^>]*>.*?</script>", " ", html or "")
    text = re.sub(r"(?is)<style\b[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?is)<!--.*?-->", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    return clean_text(text, max_len=max_len)


def fetch_text(url: str, timeout: int = 20, max_len: int = 6000) -> tuple[str, str]:
    _, _, body, final_url = http_request(url, timeout=timeout)
    return html_to_text(body, max_len=max_len), final_url


def extract_bio_fields(text: Any) -> dict[str, str]:
    source = str(text or "")
    employment = ""
    title = ""
    location = ""

    for pattern in EMPLOYMENT_PATTERNS:
        match = pattern.search(source)
        if match:
            employment = clean_text(match.group(1), max_len=120)
            break

    for pattern in TITLE_PATTERNS:
        match = pattern.search(source)
        if match:
            title = clean_text(match.group(0), max_len=120)
            break

    for pattern in LOCATION_PATTERNS:
        match = pattern.search(source)
        if match:
            location = clean_text(match.group(1), max_len=120)
            break

    return {"employment": employment, "title": title, "location": location}
