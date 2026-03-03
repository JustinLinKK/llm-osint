from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urljoin

from archive.common import fetch_text
from technical.common import extract_emails


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    site_url = str(input_data.get("site_url") or input_data.get("url") or "").strip()
    if not site_url.startswith(("http://", "https://")):
        raise RuntimeError("Missing required input: site_url")
    candidates = [urljoin(site_url.rstrip("/") + "/", path) for path in ("contact", "about", "team")]
    pages: List[Dict[str, Any]] = []
    all_emails: List[str] = []
    for url in candidates:
        try:
            text, final_url = fetch_text(url, timeout=15, max_len=4000)
        except Exception:
            continue
        emails = extract_emails(text, max_items=10)
        all_emails.extend(emails)
        pages.append({"url": final_url, "emails": emails, "extracted_text": text[:500]})
    deduped_emails = list(dict.fromkeys(all_emails))
    return {
        "tool": "contact_page_extractor",
        "pages": pages,
        "emails": deduped_emails,
        "office_address": "",
        "confidence": 0.7 if pages else 0.0,
    }
