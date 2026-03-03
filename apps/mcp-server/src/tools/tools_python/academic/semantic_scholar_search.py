from __future__ import annotations

from typing import Any, Dict

from academic.common import add_candidate, build_base_result, build_evidence, domain_from_url, http_json_request, normalize_query, validate_result_shape
from matching.confidence import score_candidate_match


SEARCH_URL = "https://api.semanticscholar.org/graph/v1/author/search"
AUTHOR_URL = "https://api.semanticscholar.org/graph/v1/author/{author_id}"
SEARCH_FIELDS = "name,affiliations,homepage,paperCount,citationCount,hIndex,externalIds,url"
AUTHOR_FIELDS = "name,affiliations,homepage,paperCount,citationCount,hIndex,externalIds,url"


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    result = build_base_result("semantic_scholar_search", query)

    if query.get("author_id") and query.get("fetch_author"):
        author_id = str(query["author_id"]).strip()
        raw = http_json_request(
            AUTHOR_URL.format(author_id=author_id),
            params={"fields": AUTHOR_FIELDS},
        )
        candidate_name = str(raw.get("name") or query["person_name"]).strip()
        affiliations = raw.get("affiliations") if isinstance(raw.get("affiliations"), list) else []
        topics = []
        homepage = raw.get("homepage")
        homepage_domain = domain_from_url(homepage) if isinstance(homepage, str) else ""
        query_domains = [domain_from_url(value) for value in query.get("constraints", {}).get("domain_allowlist", [])]
        match = score_candidate_match(
            query_name=query["person_name"],
            candidate_name=candidate_name,
            query_affiliations=query["affiliations"],
            candidate_affiliations=affiliations,
            query_topics=query["field_keywords"],
            candidate_topics=topics,
            known_ids=query.get("known_ids", {}),
            candidate_ids={"semantic_scholar": author_id},
            homepage_domain_match=bool(homepage_domain and homepage_domain in query_domains),
        )
        add_candidate(
            result,
            canonical_name=candidate_name,
            source="semanticscholar",
            source_id=author_id,
            confidence=match["confidence"],
            match_features=match["match_features"],
            affiliations=affiliations,
            topics=topics,
            external_ids=raw.get("externalIds") if isinstance(raw.get("externalIds"), dict) else {"semantic_scholar": author_id},
            works_summary={
                "paper_count": raw.get("paperCount"),
                "citation_count": raw.get("citationCount"),
                "h_index": raw.get("hIndex"),
            },
            evidence=[build_evidence(str(raw.get("url") or f"https://www.semanticscholar.org/author/{author_id}"), candidate_name, ["person_name"])],
            extra={"homepage": homepage, "homepage_domain": homepage_domain, "profile_url": raw.get("url")},
        )
        return validate_result_shape(result)

    raw = http_json_request(
        SEARCH_URL,
        params={
            "query": query["person_name"],
            "limit": max(query["max_results"], 10),
            "fields": SEARCH_FIELDS,
        },
    )
    for item in raw.get("data", []):
        if not isinstance(item, dict):
            continue
        author_id = str(item.get("authorId") or "").strip()
        if not author_id:
            continue
        candidate_name = str(item.get("name") or query["person_name"]).strip()
        affiliations = item.get("affiliations") if isinstance(item.get("affiliations"), list) else []
        topics = []
        homepage = item.get("homepage")
        homepage_domain = domain_from_url(homepage) if isinstance(homepage, str) else ""
        query_domains = [domain_from_url(value) for value in query.get("constraints", {}).get("domain_allowlist", [])]
        match = score_candidate_match(
            query_name=query["person_name"],
            candidate_name=candidate_name,
            query_affiliations=query["affiliations"],
            candidate_affiliations=affiliations,
            query_topics=query["field_keywords"],
            candidate_topics=topics,
            known_ids=query.get("known_ids", {}),
            candidate_ids={"semantic_scholar": author_id, **(item.get("externalIds") if isinstance(item.get("externalIds"), dict) else {})},
            homepage_domain_match=bool(homepage_domain and homepage_domain in query_domains),
        )
        add_candidate(
            result,
            canonical_name=candidate_name,
            source="semanticscholar",
            source_id=author_id,
            confidence=match["confidence"],
            match_features=match["match_features"],
            affiliations=affiliations,
            topics=topics,
            external_ids=item.get("externalIds") if isinstance(item.get("externalIds"), dict) else {"semantic_scholar": author_id},
            works_summary={
                "paper_count": item.get("paperCount"),
                "citation_count": item.get("citationCount"),
                "h_index": item.get("hIndex"),
            },
            evidence=[build_evidence(str(item.get("url") or f"https://www.semanticscholar.org/author/{author_id}"), candidate_name, ["person_name"])],
            extra={"homepage": homepage, "homepage_domain": homepage_domain, "profile_url": item.get("url")},
        )
    result["candidates"] = result["candidates"][: query["max_results"]]
    return validate_result_shape(result)
