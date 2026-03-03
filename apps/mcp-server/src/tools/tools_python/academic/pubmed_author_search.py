from __future__ import annotations

import json
from collections import Counter
from typing import Any, Dict, List

from academic.common import build_base_result, build_evidence, clean_text, http_json_request, normalize_query, validate_result_shape
from matching.confidence import normalize_name


ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"


def _author_query(name: str, affiliations: List[str]) -> str:
    normalized = normalize_name(name)
    parts = normalized.split()
    if len(parts) >= 2:
        author_term = f"{parts[-1]} {' '.join(token[:1] for token in parts[:-1])}[Author]"
    else:
        author_term = f"{normalized}[Author]"
    if affiliations:
        affl = " OR ".join(f'"{item}"[Affiliation]' for item in affiliations[:3])
        return f"({author_term}) AND ({affl})"
    return author_term


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    result = build_base_result("pubmed_author_search", query)
    term = _author_query(query["person_name"], query["affiliations"])
    if query.get("time_range"):
        time_range = query["time_range"]
        start = time_range.get("start_year")
        end = time_range.get("end_year")
        if start and end:
            term = f"{term} AND ({start}:{end}[pdat])"

    esearch = http_json_request(
        ESEARCH_URL,
        params={"db": "pubmed", "retmode": "json", "retmax": query["max_results"], "term": term},
    )
    id_list = esearch.get("esearchresult", {}).get("idlist", [])
    if not isinstance(id_list, list):
        id_list = []
    summaries = {}
    if id_list:
        esummary = http_json_request(
            ESUMMARY_URL,
            params={"db": "pubmed", "retmode": "json", "id": ",".join(id_list)},
        )
        summaries = esummary.get("result", {}) if isinstance(esummary, dict) else {}

    journal_counter: Counter[str] = Counter()
    year_counter: Counter[str] = Counter()
    records: List[Dict[str, Any]] = []
    for pmid in id_list:
        item = summaries.get(str(pmid), {}) if isinstance(summaries, dict) else {}
        if not isinstance(item, dict):
            continue
        journal = clean_text(item.get("fulljournalname") or item.get("source") or "", max_len=140)
        year = str(item.get("pubdate") or "")[:4]
        if journal:
            journal_counter[journal] += 1
        if year.isdigit():
            year_counter[year] += 1
        records.append(
            {
                "pmid": str(pmid),
                "title": clean_text(item.get("title") or "", max_len=300),
                "journal": journal,
                "year": year,
                "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
            }
        )

    if id_list:
        result["records"] = records
        result["candidates"].append(
            {
                "canonical_name": query["person_name"],
                "source": "pubmed",
                "source_id": ",".join(id_list[:5]),
                "confidence": 0.7 if query["affiliations"] else 0.55,
                "match_features": {
                    "reasons": ["pubmed author query", "affiliation narrowing" if query["affiliations"] else "name-only author query"],
                    "weights": {"pubmed_query": 0.55, "affiliation_filter": 0.15 if query["affiliations"] else 0.0},
                    "score_breakdown": {"pubmed_query": 0.55, "affiliation_filter": 0.15 if query["affiliations"] else 0.0},
                },
                "affiliations": query["affiliations"],
                "topics": query["field_keywords"],
                "external_ids": {"pmid_hint": id_list[0]},
                "works_summary": {
                    "pubmed_count": len(id_list),
                    "top_journals": dict(journal_counter.most_common(5)),
                    "counts_by_year": dict(year_counter.most_common(10)),
                },
                "evidence": [
                    build_evidence(
                        f"https://pubmed.ncbi.nlm.nih.gov/?term={term}",
                        json.dumps(records[:3], ensure_ascii=True),
                        ["person_name", "affiliations"],
                    )
                ],
            }
        )
    return validate_result_shape(result)

