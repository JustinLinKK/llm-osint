from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


USER_AGENT = os.getenv("TECHNICAL_TOOL_USER_AGENT", "llm-osint technical tools/1.0")
EMAIL_REGEX = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
SOCIAL_LINK_PATTERNS = {
    "github": re.compile(r"https?://(?:www\.)?github\.com/[^/?#\"'>\s]+", re.IGNORECASE),
    "gitlab": re.compile(r"https?://(?:www\.)?gitlab\.com/[^/?#\"'>\s]+", re.IGNORECASE),
    "bitbucket": re.compile(r"https?://(?:www\.)?bitbucket\.org/[^/?#\"'>\s]+", re.IGNORECASE),
    "huggingface": re.compile(r"https?://(?:www\.)?huggingface\.co/[^/?#\"'>\s]+", re.IGNORECASE),
    "linkedin": re.compile(r"https?://(?:www\.)?linkedin\.com/in/[^?#\"'>\s]+", re.IGNORECASE),
    "x": re.compile(r"https?://(?:www\.)?(?:x\.com|twitter\.com)/[^/?#\"'>\s]+", re.IGNORECASE),
}


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


def dedupe_objects(items: Iterable[Dict[str, Any]], *, key: str) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    output: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        value = item.get(key)
        if not isinstance(value, str) or not value.strip():
            continue
        normalized = value.strip().lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        output.append(item)
    return output


def domain_from_url(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        return ""
    parsed = urlparse(value.strip())
    host = (parsed.hostname or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def domain_from_email(value: Any) -> str:
    if not isinstance(value, str) or "@" not in value:
        return ""
    return value.rsplit("@", 1)[-1].strip().lower()


def normalize_query(input_data: Dict[str, Any]) -> Dict[str, Any]:
    person_name = str(input_data.get("person_name") or input_data.get("name") or "").strip()
    username = str(input_data.get("username") or "").strip().lstrip("@")
    email = str(input_data.get("email") or "").strip().lower()
    profile_url = str(input_data.get("profile_url") or input_data.get("url") or "").strip()
    blog = str(input_data.get("blog") or "").strip()
    domain = str(input_data.get("domain") or "").strip().lower()
    repo_url = str(input_data.get("repo_url") or "").strip()
    max_results = max(1, min(int(input_data.get("max_results", 5)), 10))
    return {
        "person_name": person_name,
        "username": username,
        "email": email,
        "profile_url": profile_url,
        "blog": blog,
        "domain": domain,
        "repo_url": repo_url,
        "max_results": max_results,
    }


def build_base_result(tool_name: str, platform: str, query: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "tool": tool_name,
        "query": query,
        "stable_id": "",
        "platform": platform,
        "profile_url": "",
        "created_at": None,
        "last_active": None,
        "organizations": [],
        "repositories": [],
        "publications": [],
        "contact_signals": [],
        "external_links": [],
        "evidence": [],
        "confidence": 0.0,
        "match_features": {"reasons": [], "score_breakdown": {}},
    }


def build_evidence(url: str, snippet: str, fields_matched: Iterable[str]) -> Dict[str, Any]:
    return {
        "url": url,
        "snippet": clean_text(snippet, max_len=280),
        "retrieved_at": utc_now_iso(),
        "fields_matched": as_string_list(list(fields_matched), max_items=10),
    }


def validate_result_shape(result: Dict[str, Any]) -> Dict[str, Any]:
    required = (
        "tool",
        "stable_id",
        "platform",
        "profile_url",
        "created_at",
        "last_active",
        "organizations",
        "repositories",
        "publications",
        "contact_signals",
        "external_links",
        "evidence",
        "confidence",
        "match_features",
    )
    for key in required:
        if key not in result:
            raise RuntimeError(f"Technical tool result missing key: {key}")
    for key in ("organizations", "repositories", "publications", "contact_signals", "external_links", "evidence"):
        if not isinstance(result.get(key), list):
            raise RuntimeError(f"Technical tool result key must be a list: {key}")
    if not isinstance(result.get("match_features"), dict):
        raise RuntimeError("Technical tool result match_features must be a dict")
    result["contact_signals"] = dedupe_objects(result.get("contact_signals", []), key="value")
    result["external_links"] = dedupe_objects(result.get("external_links", []), key="url")
    result["organizations"] = dedupe_objects(result.get("organizations", []), key="url")
    result["repositories"] = dedupe_objects(result.get("repositories", []), key="url")
    result["evidence"] = dedupe_objects(result.get("evidence", []), key="url")
    result["confidence"] = max(0.0, min(1.0, float(result.get("confidence") or 0.0)))
    return result


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
) -> Dict[str, Any]:
    status, _, body, _ = http_request(url, method=method, params=params, headers=headers, timeout=timeout)
    if status >= 400:
        raise RuntimeError(f"HTTP {status} for {url}")
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Expected JSON from {url}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError(f"Expected JSON object from {url}")
    return parsed


def http_json_or_list_request(
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


def extract_emails(text: str, max_items: int = 10) -> List[str]:
    return as_string_list(EMAIL_REGEX.findall(text or ""), max_items=max_items)


def extract_social_links(text: str, max_items: int = 20) -> List[Dict[str, str]]:
    links: List[Dict[str, str]] = []
    for link_type, pattern in SOCIAL_LINK_PATTERNS.items():
        for match in pattern.findall(text or ""):
            links.append({"type": link_type, "url": match.rstrip('.,)"\'')})
    return dedupe_objects(links, key="url")[:max_items]
