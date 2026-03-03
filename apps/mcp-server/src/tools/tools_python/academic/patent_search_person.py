from __future__ import annotations

import os
from typing import Any, Dict, List

from academic.common import build_base_result, build_evidence, clean_text, http_json_request, normalize_query, unsupported_result, validate_result_shape


PATENTSVIEW_URL = "https://api.patentsview.org/patents/query"


def _build_query(name: str) -> Dict[str, Any]:
    parts = [part for part in name.split() if part]
    if len(parts) >= 2:
        return {
            "_and": [
                {"_text_any": {"inventor_first_name": " ".join(parts[:-1])}},
                {"_text_any": {"inventor_last_name": parts[-1]}},
            ]
        }
    return {"_text_any": {"inventor_last_name": name}}


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    result = build_base_result("patent_search_person", query)
    patentsearch_api_key = os.getenv("PATENTSVIEW_API_KEY", "").strip()
    if not patentsearch_api_key:
        return unsupported_result(
            "patent_search_person",
            query,
            "PatentsView legacy API was discontinued on May 1, 2025. PatentSearch API now requires an API key; set PATENTSVIEW_API_KEY to enable this tool.",
        )
    raw = http_json_request(
        PATENTSVIEW_URL,
        method="POST",
        data={
            "q": _build_query(query["person_name"]),
            "f": [
                "patent_number",
                "patent_title",
                "patent_date",
                "patent_type",
                "patent_kind",
                "patent_num_cited_by_us_patents",
            ],
            "o": {"page": 1, "per_page": query["max_results"]},
        },
    )
    patents = raw.get("patents", [])
    if not isinstance(patents, list):
        patents = []
    records: List[Dict[str, Any]] = []
    for item in patents[: query["max_results"]]:
        if not isinstance(item, dict):
            continue
        patent_number = str(item.get("patent_number") or "").strip()
        records.append(
            {
                "patent_id": patent_number,
                "title": clean_text(item.get("patent_title") or "", max_len=240),
                "filing_date": item.get("patent_date"),
                "patent_type": item.get("patent_type"),
                "kind": item.get("patent_kind"),
                "citation_count": item.get("patent_num_cited_by_us_patents"),
                "url": f"https://patents.google.com/patent/US{patent_number}" if patent_number else "",
            }
        )
    result["records"] = records
    if records:
        result["candidates"].append(
            {
                "canonical_name": query["person_name"],
                "source": "patentsview",
                "source_id": query["person_name"],
                "confidence": 0.65,
                "match_features": {
                    "reasons": ["inventor name query"],
                    "weights": {"name_match": 0.35, "inventor_signal": 0.3},
                    "score_breakdown": {"name_match": 0.35, "inventor_signal": 0.3},
                },
                "affiliations": query["affiliations"],
                "topics": query["field_keywords"],
                "external_ids": {},
                "works_summary": {"patent_count": len(records)},
                "evidence": [
                    build_evidence("https://api.patentsview.org/", f"Found {len(records)} patent candidates", ["person_name"])
                ],
            }
        )
    return validate_result_shape(result)
