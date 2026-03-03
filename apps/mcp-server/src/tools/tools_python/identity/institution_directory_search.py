from __future__ import annotations

from typing import Any, Dict
from urllib.parse import quote_plus

from technical.common import extract_emails
from archive.common import fetch_text


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    institution_domain = str(input_data.get("institution_domain") or input_data.get("domain") or "").strip().lower()
    person_name = str(input_data.get("person_name") or input_data.get("name") or "").strip()
    if not institution_domain or not person_name:
        raise RuntimeError("Missing required input: institution_domain and person_name")
    query = f'site:{institution_domain} "{person_name}"'
    query_url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
    text, final_url = fetch_text(query_url, timeout=20, max_len=4000)
    emails = extract_emails(text, max_items=5)
    return {
        "tool": "institution_directory_search",
        "institution": institution_domain,
        "title": "",
        "department": "",
        "email": emails[0] if emails else "",
        "profile_url": final_url,
        "confidence": 0.6 if person_name.lower() in text.lower() else 0.2,
    }
