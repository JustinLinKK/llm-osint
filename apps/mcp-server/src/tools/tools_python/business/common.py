from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


USER_AGENT = os.getenv("BUSINESS_TOOL_USER_AGENT", "llm-osint business tools/1.0")
COMPANY_SUFFIX_RE = re.compile(r"\b(inc|incorporated|llc|ltd|limited|corp|corporation|co|company|plc)\b\.?", re.IGNORECASE)
PUNCT_RE = re.compile(r"[^a-z0-9\s]")
DATE_RE = re.compile(r"\b(19|20)\d{2}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12]\d|3[01])\b")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clean_text(value: Any, max_len: int = 400) -> str:
    if not isinstance(value, str):
        return ""
    compact = re.sub(r"\s+", " ", value).strip()
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 1].rstrip() + "..."


def as_string_list(value: Any, max_items: int | None = None) -> List[str]:
    if isinstance(value, str):
        items = [value]
    elif isinstance(value, list):
        items = [item for item in value if isinstance(item, str)]
    else:
        items = []
    seen: set[str] = set()
    output: List[str] = []
    for item in items:
        text = item.strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        output.append(text)
        if max_items is not None and len(output) >= max_items:
            break
    return output


def build_evidence(url: str, snippet: str, fields_matched: Iterable[str]) -> Dict[str, Any]:
    return {
        "url": url,
        "snippet": clean_text(snippet, max_len=280),
        "retrieved_at": utc_now_iso(),
        "fields_matched": as_string_list(list(fields_matched), max_items=10),
    }


def normalize_company_name(value: str) -> str:
    text = COMPANY_SUFFIX_RE.sub(" ", str(value or ""))
    text = PUNCT_RE.sub(" ", text.lower())
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_person_name(value: str) -> str:
    text = PUNCT_RE.sub(" ", str(value or "").lower())
    return re.sub(r"\s+", " ", text).strip()


def normalize_query(input_data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "company_name": str(input_data.get("company_name") or "").strip(),
        "person_name": str(input_data.get("person_name") or input_data.get("name") or "").strip(),
        "jurisdiction_code": str(input_data.get("jurisdiction_code") or input_data.get("jurisdiction") or "").strip().lower(),
        "company_number": str(input_data.get("company_number") or "").strip(),
        "cik": str(input_data.get("cik") or "").strip(),
        "domain": str(input_data.get("domain") or "").strip().lower(),
        "filing_url": str(input_data.get("filing_url") or input_data.get("url") or "").strip(),
        "max_results": max(1, min(int(input_data.get("max_results", 5)), 20)),
    }


def http_request(
    url: str,
    *,
    method: str = "GET",
    params: Dict[str, Any] | None = None,
    headers: Dict[str, str] | None = None,
    timeout: int = 20,
) -> Tuple[int, Dict[str, str], str, str]:
    final_url = url
    if params:
        final_url = f"{url}?{urlencode(params, doseq=True)}"
    request = Request(final_url, method=method.upper())
    request.add_header("User-Agent", USER_AGENT)
    request.add_header("Accept", "application/json, text/html;q=0.9, */*;q=0.8")
    if headers:
        for key, value in headers.items():
            request.add_header(key, value)
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            response_headers = {key.lower(): value for key, value in response.headers.items()}
            return int(response.status), response_headers, body, response.geturl()
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        response_headers = {key.lower(): value for key, value in exc.headers.items()}
        return int(exc.code), response_headers, body, exc.geturl()
    except URLError as exc:
        raise RuntimeError(f"HTTP request failed for {url}: {exc}") from exc


def http_json_request(
    url: str,
    *,
    method: str = "GET",
    params: Dict[str, Any] | None = None,
    headers: Dict[str, str] | None = None,
    timeout: int = 20,
) -> Any:
    status, _, body, _ = http_request(url, method=method, params=params, headers=headers, timeout=timeout)
    if status >= 400:
        raise RuntimeError(f"HTTP {status} for {url}")
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Expected JSON from {url}") from exc


def score_business_confidence(
    *,
    exact_name_match: bool = False,
    jurisdiction_match: bool = False,
    address_match: bool = False,
    timeline_consistency: bool = False,
) -> float:
    score = 0.0
    if exact_name_match:
        score += 0.4
    if jurisdiction_match:
        score += 0.3
    if address_match:
        score += 0.2
    if timeline_consistency:
        score += 0.1
    return round(min(score, 1.0), 4)


def extract_dates(text: str, max_items: int = 5) -> List[str]:
    return as_string_list(DATE_RE.findall(text or ""), max_items=max_items)
