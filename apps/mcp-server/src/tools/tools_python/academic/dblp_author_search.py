from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, List

from academic.common import add_candidate, build_base_result, build_evidence, clean_text, http_json_request, http_text_request, normalize_query, validate_result_shape
from matching.confidence import score_candidate_match


SEARCH_URL = "https://dblp.org/search/author/api"


def _extract_hits(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = raw.get("result") if isinstance(raw, dict) else {}
    hits = result.get("hits") if isinstance(result, dict) else {}
    hit_rows = hits.get("hit") if isinstance(hits, dict) else []
    if isinstance(hit_rows, dict):
        return [hit_rows]
    return hit_rows if isinstance(hit_rows, list) else []


def _extract_affiliations_from_info(info: Dict[str, Any]) -> List[str]:
    output: List[str] = []
    notes = info.get("notes")
    if isinstance(notes, dict):
        note_items = notes.get("note")
        if isinstance(note_items, dict):
            note_items = [note_items]
        if isinstance(note_items, list):
            for item in note_items:
                if not isinstance(item, dict):
                    continue
                if str(item.get("@type") or "").strip().lower() != "affiliation":
                    continue
                text = clean_text(item.get("text") or "", max_len=180)
                if text:
                    output.append(text)
    for key in ("note", "affiliation"):
        value = info.get(key)
        text = clean_text(value or "", max_len=180)
        if text:
            output.append(text)
    seen: set[str] = set()
    deduped: List[str] = []
    for item in output:
        lowered = item.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(item)
    return deduped


def _fetch_publications(pid_or_url: str, max_results: int) -> List[Dict[str, Any]]:
    xml_url = pid_or_url
    if "/pid/" in pid_or_url and not pid_or_url.endswith(".xml"):
        xml_url = f"{pid_or_url}.xml"
    elif pid_or_url and "dblp.org/pid/" not in pid_or_url:
        xml_url = f"https://dblp.org/pid/{pid_or_url}.xml"
    raw_xml = http_text_request(xml_url)
    root = ET.fromstring(raw_xml)
    records: List[Dict[str, Any]] = []
    for child in root:
        if child.tag not in {"r", "dblpperson"}:
            continue
        if child.tag == "dblpperson":
            for entry in child.findall("./r/*"):
                record = _entry_to_record(entry)
                if record:
                    records.append(record)
        else:
            for entry in child:
                record = _entry_to_record(entry)
                if record:
                    records.append(record)
        if len(records) >= max_results:
            break
    return records[:max_results]


def _entry_to_record(entry: ET.Element) -> Dict[str, Any] | None:
    kind = entry.tag
    title = clean_text(entry.findtext("title") or "", max_len=300)
    if not title:
        return None
    url = entry.findtext("ee") or entry.findtext("url") or ""
    venue = entry.findtext("booktitle") or entry.findtext("journal") or ""
    year = entry.findtext("year") or ""
    return {
        "kind": kind,
        "title": title,
        "venue": clean_text(venue, max_len=160),
        "year": year,
        "url": url,
    }


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    result = build_base_result("dblp_author_search", query)

    if query.get("dblp_pid") and query.get("fetch_publications"):
        pid = str(query["dblp_pid"]).strip()
        records = _fetch_publications(pid, query["max_results"])
        result["records"] = records
        evidence_url = pid if pid.startswith("http://") or pid.startswith("https://") else f"https://dblp.org/pid/{pid}"
        add_candidate(
            result,
            canonical_name=query["person_name"],
            source="dblp",
            source_id=pid,
            confidence=1.0 if query.get("known_ids", {}).get("dblp_pid") == pid else 0.85,
            match_features={"reasons": ["dblp_pid fetch"], "weights": {"dblp_pid": 0.85}, "score_breakdown": {"dblp_pid": 0.85}},
            affiliations=[],
            topics=[],
            external_ids={"dblp_pid": pid},
            works_summary={"publication_count_fetched": len(records)},
            evidence=[build_evidence(evidence_url, f"Fetched {len(records)} DBLP publications", ["dblp_pid"])],
        )
        return validate_result_shape(result)

    raw = http_json_request(
        SEARCH_URL,
        params={"q": query["person_name"], "format": "json", "h": query["max_results"]},
    )
    for item in _extract_hits(raw)[: query["max_results"]]:
        info = item.get("info") if isinstance(item, dict) else {}
        if not isinstance(info, dict):
            continue
        author = str(info.get("author") or query["person_name"]).strip()
        pid = str(info.get("@pid") or info.get("pid") or info.get("url") or "").strip()
        if not pid:
            continue
        affiliations = _extract_affiliations_from_info(info)
        evidence_url = str(info.get("url") or (f"https://dblp.org/pid/{pid}" if "/" not in pid else pid))
        match = score_candidate_match(
            query_name=query["person_name"],
            candidate_name=author,
            query_affiliations=query["affiliations"],
            candidate_affiliations=affiliations,
            query_topics=query["field_keywords"],
            candidate_topics=[],
            known_ids=query.get("known_ids", {}),
            candidate_ids={"dblp_pid": pid},
        )
        add_candidate(
            result,
            canonical_name=author,
            source="dblp",
            source_id=pid,
            confidence=match["confidence"],
            match_features=match["match_features"],
            affiliations=affiliations,
            topics=[],
            external_ids={"dblp_pid": pid},
            works_summary={"hit_score": item.get("@score")},
            evidence=[build_evidence(evidence_url, json.dumps(info, ensure_ascii=True), ["person_name", "affiliations"])],
            extra={"profile_url": evidence_url},
        )
    return validate_result_shape(result)
