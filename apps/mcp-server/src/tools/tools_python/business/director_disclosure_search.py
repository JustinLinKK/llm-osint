from __future__ import annotations

import re
from typing import Any, Dict, List

from business.common import build_evidence, clean_text, http_request, normalize_query


COMMITTEE_RE = re.compile(r"committee[s]?:?\s*([^<\n]{1,200})", re.IGNORECASE)
COMP_RE = re.compile(r"compensation[^$\n]{0,80}(\$[\d,]+(?:\.\d{2})?)", re.IGNORECASE)
TENURE_RE = re.compile(r"(?:since|tenure|director since)\s+((?:19|20)\d{2})", re.IGNORECASE)


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    if not query["filing_url"]:
        raise RuntimeError("Missing required input: filing_url")
    _, _, html, final_url = http_request(query["filing_url"], timeout=20)
    text = re.sub(r"(?is)<script\b[^>]*>.*?</script>", " ", html)
    text = re.sub(r"(?is)<style\b[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)

    committee_roles = []
    committee_match = COMMITTEE_RE.search(text)
    if committee_match:
        committee_roles = [item.strip() for item in re.split(r",|;| and ", committee_match.group(1)) if item.strip()]
    tenure_start = ""
    tenure_match = TENURE_RE.search(text)
    if tenure_match:
        tenure_start = tenure_match.group(1)
    compensation = ""
    comp_match = COMP_RE.search(text)
    if comp_match:
        compensation = comp_match.group(1)

    company = query["company_name"] or clean_text(final_url, max_len=120)
    return {
        "tool": "director_disclosure_search",
        "directorships": [
            {
                "company": company,
                "committee_roles": committee_roles,
                "tenure_start": tenure_start,
                "tenure_end": "",
                "compensation": compensation,
            }
        ],
        "source_url": final_url,
        "evidence": [build_evidence(final_url, clean_text(text, max_len=220), ["filing_url"])],
        "confidence": 0.7 if (committee_roles or tenure_start or compensation) else 0.4,
    }
