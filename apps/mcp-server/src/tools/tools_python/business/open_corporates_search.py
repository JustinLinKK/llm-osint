from __future__ import annotations

from typing import Any, Dict, List

from business.common import (
    build_evidence,
    clean_text,
    http_json_request,
    normalize_company_name,
    normalize_query,
    score_business_confidence,
)


SEARCH_URL = "https://api.opencorporates.com/v0.4/companies/search"
DETAIL_URL = "https://api.opencorporates.com/v0.4/companies/{jurisdiction}/{company_number}"
OFFICERS_URL = "https://api.opencorporates.com/v0.4/companies/{jurisdiction}/{company_number}/officers"


def _pick_company(results: List[Dict[str, Any]], query: Dict[str, Any]) -> Dict[str, Any]:
    normalized_query = normalize_company_name(query["company_name"])
    jurisdiction_hint = query.get("jurisdiction_code")
    best: tuple[float, Dict[str, Any]] = (0.0, {})
    for item in results:
        company = item.get("company") if isinstance(item.get("company"), dict) else {}
        name = str(company.get("name") or "").strip()
        jurisdiction = str(company.get("jurisdiction_code") or "").strip().lower()
        exact_name = normalize_company_name(name) == normalized_query and bool(normalized_query)
        jurisdiction_match = bool(jurisdiction_hint and jurisdiction == jurisdiction_hint)
        confidence = score_business_confidence(exact_name_match=exact_name, jurisdiction_match=jurisdiction_match)
        if confidence > best[0]:
            best = (confidence, company)
    return best[1]


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    if not query["company_name"] and not (query["company_number"] and query["jurisdiction_code"]):
        raise RuntimeError("Missing required input: company_name or company_number+jurisdiction_code")

    company_data: Dict[str, Any]
    source_url: str
    if query["company_number"] and query["jurisdiction_code"]:
        detail_raw = http_json_request(
            DETAIL_URL.format(jurisdiction=query["jurisdiction_code"], company_number=query["company_number"]),
            timeout=20,
        )
        company_data = (
            (((detail_raw.get("results") if isinstance(detail_raw, dict) else {}) or {}).get("company"))
            if isinstance(detail_raw, dict)
            else {}
        ) or {}
        source_url = f"https://opencorporates.com/companies/{query['jurisdiction_code']}/{query['company_number']}"
    else:
        params = {"q": query["company_name"]}
        if query["jurisdiction_code"]:
            params["jurisdiction_code"] = query["jurisdiction_code"]
        search_raw = http_json_request(SEARCH_URL, params=params, timeout=20)
        companies = (((search_raw.get("results") if isinstance(search_raw, dict) else {}) or {}).get("companies")) or []
        company_data = _pick_company([item for item in companies if isinstance(item, dict)], query)
        jurisdiction = str(company_data.get("jurisdiction_code") or "").strip()
        company_number = str(company_data.get("company_number") or "").strip()
        if not jurisdiction or not company_number:
            return {
                "tool": "open_corporates_search",
                "company_name": query["company_name"],
                "company_number": "",
                "jurisdiction": query["jurisdiction_code"],
                "incorporation_date": "",
                "status": "",
                "registered_address": "",
                "officers": [],
                "source_url": "",
                "confidence": 0.0,
            }
        detail_raw = http_json_request(DETAIL_URL.format(jurisdiction=jurisdiction, company_number=company_number), timeout=20)
        company_data = (
            (((detail_raw.get("results") if isinstance(detail_raw, dict) else {}) or {}).get("company"))
            if isinstance(detail_raw, dict)
            else {}
        ) or company_data
        source_url = f"https://opencorporates.com/companies/{jurisdiction}/{company_number}"

    jurisdiction = str(company_data.get("jurisdiction_code") or "").strip()
    company_number = str(company_data.get("company_number") or "").strip()
    officers_raw = http_json_request(OFFICERS_URL.format(jurisdiction=jurisdiction, company_number=company_number), timeout=20)
    officer_rows = (((officers_raw.get("results") if isinstance(officers_raw, dict) else {}) or {}).get("officers")) or []
    officers = []
    for row in officer_rows[:20]:
        officer = row.get("officer") if isinstance(row, dict) and isinstance(row.get("officer"), dict) else {}
        officers.append(
            {
                "name": str(officer.get("name") or "").strip(),
                "position": str(officer.get("position") or "").strip(),
                "start_date": officer.get("start_date"),
                "end_date": officer.get("end_date"),
            }
        )

    registered_address = clean_text(company_data.get("registered_address_in_full"), max_len=220)
    confidence = score_business_confidence(
        exact_name_match=normalize_company_name(company_data.get("name")) == normalize_company_name(query["company_name"]),
        jurisdiction_match=bool(query["jurisdiction_code"] and jurisdiction == query["jurisdiction_code"]),
        address_match=bool(registered_address),
        timeline_consistency=bool(company_data.get("incorporation_date")),
    )
    return {
        "tool": "open_corporates_search",
        "company_name": str(company_data.get("name") or query["company_name"]).strip(),
        "company_number": company_number,
        "jurisdiction": jurisdiction,
        "incorporation_date": company_data.get("incorporation_date"),
        "status": company_data.get("current_status"),
        "registered_address": registered_address,
        "officers": officers,
        "source_url": source_url,
        "evidence": [build_evidence(source_url, str(company_data.get("name") or query["company_name"]), ["company_name"])],
        "confidence": confidence,
    }
