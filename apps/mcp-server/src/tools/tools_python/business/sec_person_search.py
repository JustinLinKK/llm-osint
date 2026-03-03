from __future__ import annotations

from typing import Any, Dict, List

from business.common import build_evidence, clean_text, http_json_request, normalize_company_name, normalize_person_name, normalize_query, score_business_confidence


CIK_LOOKUP_URL = "https://www.sec.gov/files/company_tickers.json"
FULLTEXT_URL = "https://efts.sec.gov/LATEST/search-index"


def _sec_headers() -> Dict[str, str]:
    return {"User-Agent": "llm-osint/1.0 sec person search"}


def _resolve_cik(company_name: str) -> tuple[str, str]:
    raw = http_json_request(CIK_LOOKUP_URL, headers=_sec_headers(), timeout=20)
    normalized = normalize_company_name(company_name)
    if not isinstance(raw, dict):
        return "", ""
    for item in raw.values():
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        if normalize_company_name(title) == normalized and normalized:
            cik = str(item.get("cik_str") or "").strip()
            return cik.zfill(10), title
    return "", ""


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    params = {"q": query["person_name"] or query["company_name"], "category": "custom", "forms": "DEF 14A,10-K,3,4,5"}
    raw = http_json_request(FULLTEXT_URL, params=params, headers=_sec_headers(), timeout=20)
    hits = ((((raw.get("hits") if isinstance(raw, dict) else {}) or {}).get("hits"))) or []
    companies: List[str] = []
    roles: List[Dict[str, Any]] = []
    insider_filings: List[Dict[str, Any]] = []
    for item in hits[: query["max_results"]]:
        source = item.get("_source") if isinstance(item, dict) and isinstance(item.get("_source"), dict) else {}
        display_names = source.get("display_names") if isinstance(source.get("display_names"), list) else []
        company = str(display_names[0] if display_names else source.get("entityName") or "").strip()
        form = str(source.get("form") or "").strip()
        filing_date = source.get("file_date")
        adsh = str(source.get("adsh") or "").strip()
        filing_url = f"https://www.sec.gov/Archives/edgar/data/{adsh.replace('-', '')}" if adsh else ""
        if company:
            companies.append(company)
        if form in {"3", "4", "5"}:
            insider_filings.append({"form": form, "filing_date": filing_date, "company": company, "source_url": filing_url})
        else:
            roles.append({"company": company, "form": form, "filing_date": filing_date, "source_url": filing_url})

    cik = query["cik"]
    resolved_company = ""
    if not cik and query["company_name"]:
        cik, resolved_company = _resolve_cik(query["company_name"])
        if resolved_company:
            companies.append(resolved_company)

    confidence = score_business_confidence(
        exact_name_match=bool(query["person_name"] or query["company_name"]),
        timeline_consistency=bool(roles or insider_filings),
    )
    return {
        "tool": "sec_person_search",
        "cik": cik,
        "companies": list(dict.fromkeys([item for item in companies if item])),
        "roles": roles,
        "insider_filings": insider_filings,
        "source_url": f"https://efts.sec.gov/LATEST/search-index?q={query['person_name'] or query['company_name']}",
        "evidence": [
            build_evidence(
                f"https://efts.sec.gov/LATEST/search-index?q={query['person_name'] or query['company_name']}",
                clean_text(query["person_name"] or query["company_name"]),
                ["person_name", "company_name"],
            )
        ],
        "confidence": confidence,
    }
