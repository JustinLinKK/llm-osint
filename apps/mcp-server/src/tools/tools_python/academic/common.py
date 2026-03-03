from __future__ import annotations

import json
import os
import re
import ast
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_MAX_RESULTS = 10
USER_AGENT = os.getenv("ACADEMIC_TOOL_USER_AGENT", "llm-osint academic tools/1.0")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[6]


def load_schema(name: str) -> Dict[str, Any]:
    path = repo_root() / "schemas" / name
    return json.loads(path.read_text(encoding="utf-8"))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clean_text(value: Any, max_len: int = 400) -> str:
    if not isinstance(value, str):
        return ""
    compact = re.sub(r"\s+", " ", value).strip()
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 1].rstrip() + "…"


def as_string_list(value: Any, max_items: int | None = None) -> List[str]:
    if isinstance(value, str):
        items = [value]
    elif isinstance(value, list):
        items = [item for item in value if isinstance(item, str)]
    else:
        items = []
    deduped: List[str] = []
    seen: set[str] = set()
    for item in items:
        text = item.strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(text)
        if max_items is not None and len(deduped) >= max_items:
            break
    return deduped


def coerce_multi_string(value: Any, max_items: int | None = None) -> List[str]:
    if isinstance(value, list):
        return as_string_list(value, max_items=max_items)
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = ast.literal_eval(text)
            except Exception:
                parsed = None
            if isinstance(parsed, list):
                return as_string_list(parsed, max_items=max_items)
        if ";" in text:
            return as_string_list([item.strip() for item in text.split(";")], max_items=max_items)
        return as_string_list([text], max_items=max_items)
    return []


def affiliation_aliases(values: Iterable[str], max_items: int = 8) -> List[str]:
    aliases: List[str] = []
    seen: set[str] = set()
    stop_phrases = {
        "department of",
        "dept of",
        "faculty of",
        "school of",
        "college of",
        "institute of",
        "laboratory",
        "lab",
        "centre for",
        "center for",
    }
    for raw_value in values:
        if not isinstance(raw_value, str):
            continue
        base = clean_text(raw_value, max_len=200)
        if not base:
            continue
        parts = [part.strip() for part in base.split(",") if part.strip()]
        candidates = [base]
        if parts:
            candidates.extend(parts)
            if len(parts) >= 2:
                candidates.append(", ".join(parts[:2]))
        for candidate in candidates:
            lowered = candidate.lower()
            if any(phrase in lowered for phrase in stop_phrases) and "," in candidate:
                candidate = candidate.split(",", 1)[0].strip()
                lowered = candidate.lower()
            if len(candidate) < 4 or lowered in seen:
                continue
            seen.add(lowered)
            aliases.append(candidate)
            if len(aliases) >= max_items:
                return aliases
    return aliases


def normalize_query(input_data: Dict[str, Any]) -> Dict[str, Any]:
    person_name = str(input_data.get("person_name") or "").strip()
    if not person_name:
        raise RuntimeError("Missing required input: person_name")

    time_range = input_data.get("time_range") if isinstance(input_data.get("time_range"), dict) else None
    constraints = input_data.get("constraints") if isinstance(input_data.get("constraints"), dict) else None
    max_results = int(input_data.get("max_results", DEFAULT_MAX_RESULTS))
    max_results = max(1, min(max_results, 50))

    query = {
        "person_name": person_name,
        "affiliations": as_string_list(input_data.get("affiliations"), max_items=10),
        "field_keywords": as_string_list(input_data.get("field_keywords"), max_items=10),
        "known_ids": input_data.get("known_ids") if isinstance(input_data.get("known_ids"), dict) else {},
        "time_range": time_range,
        "max_results": max_results,
        "constraints": {
            "country": str(constraints.get("country")).strip() if constraints and constraints.get("country") else "",
            "domain_allowlist": as_string_list(constraints.get("domain_allowlist") if constraints else []),
            "venue_allowlist": as_string_list(constraints.get("venue_allowlist") if constraints else []),
        },
    }
    for key in ("orcid_id", "author_id", "dblp_pid"):
        if input_data.get(key):
            query[key] = str(input_data.get(key)).strip()
    for key in ("fetch_record", "fetch_author", "fetch_publications"):
        if key in input_data:
            query[key] = bool(input_data.get(key))
    return query


def build_base_result(tool_name: str, query: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "tool": tool_name,
        "query": query,
        "candidates": [],
        "records": [],
    }


def build_evidence(url: str, snippet: str, fields_matched: Iterable[str]) -> Dict[str, Any]:
    return {
        "url": url,
        "snippet": clean_text(snippet, max_len=280),
        "retrieved_at": utc_now_iso(),
        "fields_matched": as_string_list(list(fields_matched)),
    }


def add_candidate(
    result: Dict[str, Any],
    *,
    canonical_name: str,
    source: str,
    source_id: str,
    confidence: float,
    match_features: Dict[str, Any],
    affiliations: Iterable[str] = (),
    topics: Iterable[str] = (),
    external_ids: Dict[str, Any] | None = None,
    works_summary: Dict[str, Any] | None = None,
    evidence: Iterable[Dict[str, Any]] = (),
    extra: Dict[str, Any] | None = None,
) -> None:
    candidate = {
        "canonical_name": canonical_name,
        "source": source,
        "source_id": source_id,
        "confidence": max(0.0, min(1.0, float(confidence))),
        "match_features": match_features,
        "affiliations": as_string_list(list(affiliations), max_items=10),
        "topics": as_string_list(list(topics), max_items=10),
        "external_ids": external_ids or {},
        "works_summary": works_summary or {},
        "evidence": list(evidence),
    }
    if extra:
        candidate.update(extra)
    result["candidates"].append(candidate)


def validate_result_shape(result: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result.get("tool"), str) or not result["tool"].strip():
        raise RuntimeError("Academic tool result missing tool name")
    if not isinstance(result.get("query"), dict):
        raise RuntimeError("Academic tool result missing query echo")
    if not isinstance(result.get("candidates"), list):
        raise RuntimeError("Academic tool result missing candidates list")
    if "records" in result and not isinstance(result["records"], list):
        raise RuntimeError("Academic tool result records must be a list")
    result["candidates"] = sorted(
        result["candidates"],
        key=lambda item: (
            float(item.get("confidence") or 0.0),
            float((item.get("works_summary") or {}).get("citation_count") or 0.0),
            float((item.get("works_summary") or {}).get("paper_count") or 0.0),
            float((item.get("works_summary") or {}).get("works_count") or 0.0),
        ),
        reverse=True,
    )
    return result


def unsupported_result(tool_name: str, query: Dict[str, Any], message: str) -> Dict[str, Any]:
    result = build_base_result(tool_name, query)
    result["status"] = "unsupported"
    result["message"] = message
    return result


def http_json_request(
    url: str,
    *,
    method: str = "GET",
    params: Dict[str, Any] | None = None,
    data: Dict[str, Any] | None = None,
    headers: Dict[str, str] | None = None,
    timeout: int = 20,
) -> Dict[str, Any]:
    final_url = url
    if params:
        final_url = f"{url}?{urlencode(params, doseq=True)}"
    encoded_body = None
    if data is not None:
        encoded_body = json.dumps(data).encode("utf-8")
    req = Request(final_url, data=encoded_body, method=method.upper())
    req.add_header("User-Agent", USER_AGENT)
    req.add_header("Accept", "application/json")
    if encoded_body is not None:
        req.add_header("Content-Type", "application/json")
    if headers:
        for key, value in headers.items():
            req.add_header(key, value)
    try:
        with urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code} for {final_url}") from exc
    except URLError as exc:
        raise RuntimeError(f"Request failed for {final_url}: {exc.reason}") from exc
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from {final_url}") from exc


def http_text_request(
    url: str,
    *,
    params: Dict[str, Any] | None = None,
    headers: Dict[str, str] | None = None,
    timeout: int = 20,
) -> str:
    final_url = url
    if params:
        final_url = f"{url}?{urlencode(params, doseq=True)}"
    req = Request(final_url, method="GET")
    req.add_header("User-Agent", USER_AGENT)
    if headers:
        for key, value in headers.items():
            req.add_header(key, value)
    try:
        with urlopen(req, timeout=timeout) as response:
            return response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code} for {final_url}") from exc
    except URLError as exc:
        raise RuntimeError(f"Request failed for {final_url}: {exc.reason}") from exc


def domain_from_url(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    match = re.match(r"^[a-z]+://([^/]+)", text, flags=re.IGNORECASE)
    return match.group(1).lower() if match else ""
