from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List

from academic.common import build_base_result, build_evidence, normalize_query, validate_result_shape
from academic.dblp_author_search import run as run_dblp_search


CS_VENUE_TOKENS = {
    "acl",
    "neurips",
    "icml",
    "ieee",
    "cvpr",
    "emnlp",
    "naacl",
    "kdd",
    "sigir",
}


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    result = build_base_result("conference_profile_search", query)
    dblp_input = dict(query)
    if not dblp_input.get("fetch_publications"):
        dblp_input["fetch_publications"] = True
    dblp_input["max_results"] = max(50, query["max_results"] * 20)
    if not dblp_input.get("dblp_pid"):
        search_result = run_dblp_search(query)
        if search_result["candidates"]:
            top = sorted(search_result["candidates"], key=lambda item: item.get("confidence", 0), reverse=True)[0]
            dblp_input["dblp_pid"] = top.get("source_id")
    if not dblp_input.get("dblp_pid"):
        result["status"] = "no_candidate"
        result["message"] = "No DBLP candidate was strong enough to fetch publications."
        return validate_result_shape(result)

    fetched = run_dblp_search(dblp_input)
    records = fetched.get("records", [])
    if not isinstance(records, list):
        records = []
    venue_counter: Counter[str] = Counter()
    appearance_records: List[Dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        venue = str(item.get("venue") or "").strip()
        kind = str(item.get("kind") or "").strip().lower()
        if not venue or not kind:
            continue
        venue_lower = venue.lower()
        if kind not in {"inproceedings", "proceedings"} and not any(token in venue_lower for token in CS_VENUE_TOKENS):
            continue
        venue_counter[venue] += 1
        appearance_records.append(item)
    result["records"] = appearance_records[: query["max_results"]]
    if appearance_records:
        evidence_url = str(dblp_input["dblp_pid"])
        if not evidence_url.startswith("http://") and not evidence_url.startswith("https://"):
            evidence_url = f"https://dblp.org/pid/{evidence_url}"
        result["candidates"].append(
            {
                "canonical_name": query["person_name"],
                "source": "conference_aggregator",
                "source_id": str(dblp_input["dblp_pid"]),
                "confidence": 0.8,
                "match_features": {
                    "reasons": ["dblp publication fetch", "conference venue aggregation"],
                    "weights": {"dblp_pid": 0.5, "venue_signal": 0.3},
                    "score_breakdown": {"dblp_pid": 0.5, "venue_signal": 0.3},
                },
                "affiliations": query["affiliations"],
                "topics": list(venue_counter.keys())[:10],
                "external_ids": {"dblp_pid": dblp_input["dblp_pid"]},
                "works_summary": {
                    "conference_appearance_count": len(appearance_records),
                    "top_venues": dict(venue_counter.most_common(10)),
                },
                "evidence": [
                    build_evidence(evidence_url, f"Conference appearances: {dict(venue_counter.most_common(5))}", ["dblp_pid"])
                ],
            }
        )
    return validate_result_shape(result)
