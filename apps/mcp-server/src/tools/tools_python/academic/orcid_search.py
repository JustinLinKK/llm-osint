from __future__ import annotations

from typing import Any, Dict

from academic.common import add_candidate, affiliation_aliases, build_base_result, build_evidence, coerce_multi_string, domain_from_url, http_json_request, normalize_query, validate_result_shape
from matching.confidence import score_candidate_match


ORCID_SEARCH_URL = "https://pub.orcid.org/v3.0/expanded-search/"
ORCID_RECORD_URL = "https://pub.orcid.org/v3.0/{orcid_id}/record"


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    result = build_base_result("orcid_search", query)

    if query.get("orcid_id") and query.get("fetch_record"):
        orcid_id = str(query["orcid_id"]).strip()
        record = http_json_request(
            ORCID_RECORD_URL.format(orcid_id=orcid_id),
            headers={"Accept": "application/json"},
        )
        person = record.get("person") if isinstance(record, dict) else {}
        activities = record.get("activities-summary") if isinstance(record, dict) else {}
        name_data = person.get("name") if isinstance(person, dict) else {}
        credit_name = ""
        if isinstance(name_data, dict):
            credit_name_value = name_data.get("credit-name")
            if isinstance(credit_name_value, dict):
                credit_name = str(credit_name_value.get("value") or "").strip()
            if not credit_name:
                given_names = name_data.get("given-names") if isinstance(name_data.get("given-names"), dict) else {}
                family_name = name_data.get("family-name") if isinstance(name_data.get("family-name"), dict) else {}
                credit_name = f"{given_names.get('value', '')} {family_name.get('value', '')}".strip()
        employments = []
        groups = activities.get("employments", {}).get("affiliation-group", []) if isinstance(activities, dict) else []
        for group in groups[:10]:
            summaries = group.get("summaries", []) if isinstance(group, dict) else []
            for summary in summaries[:1]:
                org = summary.get("employment-summary", {}).get("organization", {}) if isinstance(summary, dict) else {}
                name = org.get("name")
                if isinstance(name, str) and name.strip():
                    employments.append(name.strip())
        match = score_candidate_match(
            query_name=query["person_name"],
            candidate_name=credit_name or query["person_name"],
            query_affiliations=query["affiliations"],
            candidate_affiliations=employments,
            query_topics=query["field_keywords"],
            candidate_topics=[],
            known_ids=query.get("known_ids", {}),
            candidate_ids={"orcid": orcid_id},
            homepage_domain_match=False,
        )
        add_candidate(
            result,
            canonical_name=credit_name or query["person_name"],
            source="orcid",
            source_id=orcid_id,
            confidence=match["confidence"],
            match_features=match["match_features"],
            affiliations=employments,
            topics=[],
            external_ids={"orcid": orcid_id},
            works_summary={"record_fetched": True},
            evidence=[build_evidence(f"https://orcid.org/{orcid_id}", f"ORCID record for {credit_name or query['person_name']}", ["orcid"])],
        )
        return validate_result_shape(result)

    raw = http_json_request(
        ORCID_SEARCH_URL,
        params={
            "q": query["person_name"],
            "rows": max(query["max_results"] * 5, 15),
            "start": 0,
        },
        headers={"Accept": "application/json"},
    )
    for item in raw.get("expanded-result", []):
        if not isinstance(item, dict):
            continue
        orcid_id = str(item.get("orcid-id") or "").strip()
        if not orcid_id:
            continue
        candidate_name = str(item.get("given-names") or "").strip()
        family_name = str(item.get("family-names") or "").strip()
        full_name = " ".join(part for part in [candidate_name, family_name] if part).strip() or query["person_name"]
        affiliations = coerce_multi_string(item.get("institution-name"), max_items=10)
        topics = coerce_multi_string(item.get("keywords"), max_items=10)
        external_ids = {"orcid": orcid_id}
        candidate_domains = []
        for url in coerce_multi_string(item.get("researcher-url-urls"), max_items=5):
            domain = domain_from_url(url)
            if domain:
                candidate_domains.append(domain)
        query_domains = [domain_from_url(value) for value in query.get("constraints", {}).get("domain_allowlist", [])]
        match = score_candidate_match(
            query_name=query["person_name"],
            candidate_name=full_name,
            query_affiliations=query["affiliations"],
            candidate_affiliations=affiliations,
            query_topics=query["field_keywords"],
            candidate_topics=topics,
            known_ids=query.get("known_ids", {}),
            candidate_ids=external_ids,
            homepage_domain_match=bool(set(query_domains) & set(candidate_domains)),
        )
        add_candidate(
            result,
            canonical_name=full_name,
            source="orcid",
            source_id=orcid_id,
            confidence=match["confidence"],
            match_features=match["match_features"],
            affiliations=affiliations,
            topics=topics,
            external_ids=external_ids,
            works_summary={
                "works_count": item.get("works-count"),
            },
            evidence=[
                build_evidence(
                    f"https://orcid.org/{orcid_id}",
                    " ".join(
                        part
                        for part in [
                            full_name,
                            "; ".join(affiliations),
                            "; ".join(topics),
                        ]
                        if part
                    ),
                    ["person_name", "affiliations", "field_keywords"],
                )
            ],
        )
    result["candidates"] = result["candidates"][: query["max_results"]]
    return validate_result_shape(result)
