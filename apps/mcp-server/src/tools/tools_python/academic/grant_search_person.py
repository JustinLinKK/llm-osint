from __future__ import annotations

from typing import Any, Dict, List

from academic.common import affiliation_aliases, build_base_result, build_evidence, clean_text, http_json_request, normalize_query, validate_result_shape
from matching.confidence import normalize_name


NIH_URL = "https://api.reporter.nih.gov/v2/projects/search"
NSF_URL = "https://api.nsf.gov/services/v1/awards.json"


def _person_name_matches(query_name: str, candidate_name: str) -> bool:
    query_tokens = normalize_name(query_name).split()
    candidate_tokens = normalize_name(candidate_name).split()
    if len(query_tokens) < 2 or len(candidate_tokens) < 2:
        return normalize_name(query_name) == normalize_name(candidate_name)
    query_first = query_tokens[0]
    query_last = query_tokens[-1]
    candidate_first = candidate_tokens[0]
    candidate_last = candidate_tokens[-1]
    if query_last != candidate_last:
        return False
    if query_first == candidate_first:
        return True
    if len(query_first) == 1 and candidate_first.startswith(query_first):
        return True
    if len(candidate_first) == 1 and query_first.startswith(candidate_first):
        return True
    return False


def _token_overlap(left: str, right: str) -> float:
    left_tokens = set(normalize_name(left).split())
    right_tokens = set(normalize_name(right).split())
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _institution_matches(affiliations: List[str], institution: str) -> bool:
    if not affiliations:
        return True
    normalized_institution = clean_text(institution or "", max_len=200)
    return any(_token_overlap(affiliation, normalized_institution) >= 0.25 for affiliation in affiliations if affiliation.strip())


def _search_nih(query: Dict[str, Any]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    org_variants = affiliation_aliases(query["affiliations"], max_items=6)
    query_variants = [None, *org_variants]
    years = []
    if query.get("time_range"):
        start = query["time_range"].get("start_year")
        end = query["time_range"].get("end_year")
        if start and end:
            years = list(range(int(start), int(end) + 1))
    for org_name in query_variants:
        payload: Dict[str, Any] = {
            "criteria": {
                "pi_names": [{"any_name": query["person_name"]}],
            },
            "limit": max(query["max_results"] * 3, 10),
            "offset": 0,
        }
        if org_name:
            payload["criteria"]["org_names"] = [org_name]
        if years:
            payload["criteria"]["fiscal_years"] = years[:25]
        raw = http_json_request(NIH_URL, method="POST", data=payload)
        for item in raw.get("results", []):
            if not isinstance(item, dict):
                continue
            institution = clean_text(item.get("organization", {}).get("org_name") if isinstance(item.get("organization"), dict) else "", max_len=160)
            if org_name and not _institution_matches(query["affiliations"], institution):
                continue
            grant_id = str(item.get("core_project_num") or item.get("appl_id") or "")
            if not grant_id or grant_id in seen_ids:
                continue
            seen_ids.add(grant_id)
            output.append(
                {
                    "source": "nih_reporter",
                    "grant_id": grant_id,
                    "title": clean_text(item.get("project_title") or "", max_len=240),
                    "pi": clean_text(item.get("contact_pi_name") or "", max_len=120),
                    "institution": institution,
                    "fiscal_year": item.get("fiscal_year"),
                    "amount": item.get("award_amount"),
                    "agency": "NIH",
                    "url": f"https://reporter.nih.gov/project-details/{item.get('appl_id')}" if item.get("appl_id") else "",
                }
            )
            if len(output) >= query["max_results"]:
                return output
    return output


def _search_nsf(query: Dict[str, Any]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    keywords = [query["person_name"]]
    keywords.extend(f"{query['person_name']} {affiliation}" for affiliation in affiliation_aliases(query["affiliations"], max_items=4))
    seen_ids: set[str] = set()
    for keyword in keywords:
        raw = http_json_request(
            NSF_URL,
            params={
                "keyword": keyword,
                "rpp": max(query["max_results"] * 5, 10),
                "offset": 1,
            },
        )
        awards = raw.get("response", {}).get("award", [])
        if isinstance(awards, dict):
            awards = [awards]
        for item in awards:
            if not isinstance(item, dict):
                continue
            pi_name = " ".join(part for part in [item.get("piFirstName"), item.get("piLastName")] if isinstance(part, str) and part.strip()).strip()
            institution = clean_text(item.get("awardeeName") or "", max_len=160)
            grant_id = str(item.get("id") or "")
            if not grant_id or grant_id in seen_ids:
                continue
            if not _person_name_matches(query["person_name"], pi_name):
                continue
            if not _institution_matches(query["affiliations"], institution):
                continue
            seen_ids.add(grant_id)
            output.append(
                {
                    "source": "nsf_awards",
                    "grant_id": grant_id,
                    "title": clean_text(item.get("title") or "", max_len=240),
                    "pi": clean_text(pi_name, max_len=120),
                    "institution": institution,
                    "fiscal_year": item.get("date"),
                    "amount": item.get("fundsObligatedAmt"),
                    "agency": "NSF",
                    "url": f"https://www.nsf.gov/awardsearch/showAward?AWD_ID={item.get('id')}" if item.get("id") else "",
                }
            )
            if len(output) >= query["max_results"]:
                return output
    return output


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    result = build_base_result("grant_search_person", query)
    nih_records = _search_nih(query)
    nsf_records = _search_nsf(query)
    records = nih_records + nsf_records
    result["records"] = records[: query["max_results"] * 2]
    if records:
        result["candidates"].append(
            {
                "canonical_name": query["person_name"],
                "source": "grant_aggregator",
                "source_id": query["person_name"],
                "confidence": 0.75 if query["affiliations"] else 0.6,
                "match_features": {
                    "reasons": ["grant PI search", "affiliation narrowing" if query["affiliations"] else "name-only match"],
                    "weights": {"name_match": 0.35, "affiliation_overlap": 0.25 if query["affiliations"] else 0.0},
                    "score_breakdown": {"name_match": 0.35, "affiliation_overlap": 0.25 if query["affiliations"] else 0.0},
                },
                "affiliations": query["affiliations"],
                "topics": query["field_keywords"],
                "external_ids": {},
                "works_summary": {
                    "grant_count": len(records),
                    "nih_count": len(nih_records),
                    "nsf_count": len(nsf_records),
                },
                "evidence": [
                    build_evidence("https://api.reporter.nih.gov/", f"NIH={len(nih_records)} NSF={len(nsf_records)}", ["person_name", "affiliations"])
                ],
            }
        )
    return validate_result_shape(result)
