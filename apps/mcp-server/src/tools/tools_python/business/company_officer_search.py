from __future__ import annotations

from typing import Any, Dict, List

from business.common import build_evidence, http_json_request, normalize_person_name, normalize_query, score_business_confidence


OFFICER_SEARCH_URL = "https://api.opencorporates.com/v0.4/officers/search"


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    if not query["person_name"]:
        raise RuntimeError("Missing required input: person_name")
    params = {"q": query["person_name"]}
    if query["jurisdiction_code"]:
        params["jurisdiction_code"] = query["jurisdiction_code"]
    raw = http_json_request(OFFICER_SEARCH_URL, params=params, timeout=20)
    officers = (((raw.get("results") if isinstance(raw, dict) else {}) or {}).get("officers")) or []
    roles = []
    for row in officers[: query["max_results"]]:
        officer = row.get("officer") if isinstance(row, dict) and isinstance(row.get("officer"), dict) else {}
        company = officer.get("company") if isinstance(officer.get("company"), dict) else {}
        name_match = normalize_person_name(officer.get("name")) == normalize_person_name(query["person_name"])
        jurisdiction_match = bool(query["jurisdiction_code"] and str(company.get("jurisdiction_code") or "").lower() == query["jurisdiction_code"])
        roles.append(
            {
                "company_name": str(company.get("name") or "").strip(),
                "company_number": str(company.get("company_number") or "").strip(),
                "jurisdiction": str(company.get("jurisdiction_code") or "").strip(),
                "role": str(officer.get("position") or "").strip(),
                "start_date": officer.get("start_date"),
                "end_date": officer.get("end_date"),
                "source_url": str(company.get("opencorporates_url") or "").strip(),
                "confidence": score_business_confidence(exact_name_match=name_match, jurisdiction_match=jurisdiction_match),
            }
        )
    confidence = max((float(item.get("confidence") or 0.0) for item in roles), default=0.0)
    return {
        "tool": "company_officer_search",
        "roles": roles,
        "source_url": f"https://opencorporates.com/officers?q={query['person_name']}",
        "evidence": [build_evidence(f"https://opencorporates.com/officers?q={query['person_name']}", query["person_name"], ["person_name"])],
        "confidence": confidence,
    }
