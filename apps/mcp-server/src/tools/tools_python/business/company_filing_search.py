from __future__ import annotations

from typing import Any, Dict, List

from business.common import build_evidence, http_json_request, normalize_query


OPEN_CORPORATES_FILINGS_URL = "https://api.opencorporates.com/v0.4/companies/{jurisdiction}/{company_number}/filings"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"


def _sec_headers() -> Dict[str, str]:
    return {"User-Agent": "llm-osint/1.0 business filing search"}


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    filings: List[Dict[str, Any]] = []
    source_url = ""

    if query["company_number"] and query["jurisdiction_code"]:
        raw = http_json_request(
            OPEN_CORPORATES_FILINGS_URL.format(jurisdiction=query["jurisdiction_code"], company_number=query["company_number"]),
            timeout=20,
        )
        rows = (((raw.get("results") if isinstance(raw, dict) else {}) or {}).get("filings")) or []
        for row in rows[: query["max_results"]]:
            filing = row.get("filing") if isinstance(row, dict) and isinstance(row.get("filing"), dict) else {}
            filings.append(
                {
                    "filing_type": str(filing.get("title") or filing.get("description") or "").strip(),
                    "filing_date": filing.get("date"),
                    "description": str(filing.get("description") or "").strip(),
                    "document_url": str(filing.get("opencorporates_url") or "").strip(),
                }
            )
        source_url = f"https://opencorporates.com/companies/{query['jurisdiction_code']}/{query['company_number']}/filings"

    elif query["cik"]:
        cik = query["cik"].zfill(10)
        raw = http_json_request(SEC_SUBMISSIONS_URL.format(cik=cik), headers=_sec_headers(), timeout=20)
        recent = ((raw.get("filings") if isinstance(raw, dict) else {}) or {}).get("recent") or {}
        forms = recent.get("form") if isinstance(recent.get("form"), list) else []
        dates = recent.get("filingDate") if isinstance(recent.get("filingDate"), list) else []
        accessions = recent.get("accessionNumber") if isinstance(recent.get("accessionNumber"), list) else []
        for idx, form in enumerate(forms[: query["max_results"]]):
            accession = str(accessions[idx] if idx < len(accessions) else "").replace("-", "")
            filings.append(
                {
                    "filing_type": str(form),
                    "filing_date": dates[idx] if idx < len(dates) else None,
                    "description": str(form),
                    "document_url": f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/" if accession else "",
                }
            )
        source_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    else:
        raise RuntimeError("Missing required input: company_number+jurisdiction_code or cik")

    return {
        "tool": "company_filing_search",
        "company_number": query["company_number"] or query["cik"],
        "filings": filings,
        "source_url": source_url,
        "evidence": [build_evidence(source_url, "filing search", ["company_number", "cik"])] if source_url else [],
    }
