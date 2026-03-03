from __future__ import annotations

import os
from typing import Any, Dict

from academic.common import normalize_query, unsupported_result


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    if os.getenv("FEATURE_SCHOLAR_SCRAPE", "false").strip().lower() in {"1", "true", "yes", "on"}:
        return unsupported_result(
            "google_scholar_profile_search",
            query,
            "Google Scholar scraping is enabled by flag but not implemented yet.",
        )
    return unsupported_result(
        "google_scholar_profile_search",
        query,
        "Requires a custom scraper or paid SERP provider; disabled by default.",
    )

