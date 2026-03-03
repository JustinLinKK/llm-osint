from __future__ import annotations

import re
from typing import Any, Dict, List

from technical.common import (
    as_string_list,
    build_base_result,
    build_evidence,
    clean_text,
    dedupe_objects,
    domain_from_email,
    domain_from_url,
    extract_emails,
    extract_social_links,
    http_request,
    normalize_query,
    validate_result_shape,
)


TITLE_REGEX = re.compile(r"(?is)<title[^>]*>(.*?)</title>")
GENERATOR_REGEX = re.compile(
    r'(?is)<meta[^>]+name=["\']generator["\'][^>]+content=["\'](.*?)["\']'
)


def _candidate_urls(query: Dict[str, Any]) -> List[str]:
    candidates: List[str] = []
    for key in ("profile_url", "blog"):
        value = str(query.get(key) or "").strip()
        if value.startswith(("http://", "https://")):
            candidates.append(value)

    domain = str(query.get("domain") or "").strip().lower()
    if not domain:
        domain = domain_from_email(query.get("email"))
    if not domain:
        domain = domain_from_url(query.get("blog"))
    if domain:
        candidates.append(f"https://{domain}")
        candidates.append(f"http://{domain}")

    person_name = str(query.get("person_name") or "").strip().lower()
    tokens = [token for token in re.split(r"[^a-z0-9]+", person_name) if token]
    if len(tokens) >= 2 and not candidates:
        base = f"{tokens[0]}-{tokens[-1]}"
        for tld in ("dev", "com", "io"):
            candidates.append(f"https://{base}.{tld}")
    return as_string_list(candidates, max_items=6)


def _extract_title(html: str) -> str:
    match = TITLE_REGEX.search(html or "")
    if not match:
        return ""
    return clean_text(match.group(1), max_len=180)


def _detect_technologies(html: str, headers: Dict[str, str], final_url: str) -> List[str]:
    technologies: List[str] = []
    generator = GENERATOR_REGEX.search(html or "")
    if generator:
        technologies.append(clean_text(generator.group(1), max_len=80))
    server = str(headers.get("server") or "").strip()
    if server:
        technologies.append(server)
    powered_by = str(headers.get("x-powered-by") or "").strip()
    if powered_by:
        technologies.append(powered_by)
    html_lower = (html or "").lower()
    if "wp-content" in html_lower:
        technologies.append("WordPress")
    if "/_next/" in html_lower:
        technologies.append("Next.js")
    if "astro-island" in html_lower:
        technologies.append("Astro")
    if "gatsby" in html_lower:
        technologies.append("Gatsby")
    if domain_from_url(final_url).endswith("github.io"):
        technologies.append("GitHub Pages")
    return as_string_list(technologies, max_items=8)


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    urls = _candidate_urls(query)
    if not urls:
        raise RuntimeError("Missing required input: url, blog, domain, email, or person_name")

    selected: Dict[str, Any] | None = None
    attempted: List[str] = []
    for url in urls:
        attempted.append(url)
        status, headers, body, final_url = http_request(url, timeout=15)
        content_type = str(headers.get("content-type") or "").lower()
        if status >= 400:
            continue
        if "text/html" not in content_type and "<html" not in body.lower():
            continue
        selected = {
            "status": status,
            "headers": headers,
            "body": body,
            "final_url": final_url,
            "source_url": url,
        }
        break

    result = build_base_result("personal_site_search", "website", query)
    if selected is None:
        result["match_features"] = {"reasons": ["no reachable HTML page found"], "attempted_urls": attempted}
        result["external_links"] = [{"type": "attempted", "url": url} for url in attempted]
        return validate_result_shape(result)

    body = str(selected["body"])
    final_url = str(selected["final_url"])
    title = _extract_title(body)
    technologies = _detect_technologies(body, selected["headers"], final_url)
    emails = extract_emails(body, max_items=10)
    social_links = extract_social_links(body, max_items=20)
    confidence = 0.55
    reasons: List[str] = ["reachable HTML page"]
    if str(query.get("profile_url") or "").strip().rstrip("/") == final_url.rstrip("/"):
        confidence = max(confidence, 0.92)
        reasons.append("direct URL matched")
    elif query.get("blog") and domain_from_url(query.get("blog")) == domain_from_url(final_url):
        confidence = max(confidence, 0.88)
        reasons.append("blog domain matched")
    elif query.get("domain") and str(query.get("domain")).strip().lower() == domain_from_url(final_url):
        confidence = max(confidence, 0.84)
        reasons.append("direct domain matched")
    elif query.get("email") and domain_from_email(query.get("email")) == domain_from_url(final_url):
        confidence = max(confidence, 0.78)
        reasons.append("email domain matched")
    elif query.get("person_name"):
        normalized_name = str(query["person_name"]).strip().lower()
        if normalized_name and normalized_name in body.lower():
            confidence = max(confidence, 0.72)
            reasons.append("person name present on page")

    external_links = [{"type": "canonical", "url": final_url}]
    external_links.extend(social_links)
    contact_signals = [{"type": "email", "value": email, "source": final_url} for email in emails]

    result.update(
        {
            "stable_id": f"site:{domain_from_url(final_url) or final_url}",
            "profile_url": final_url,
            "last_active": None,
            "contact_signals": contact_signals,
            "external_links": dedupe_objects(external_links, key="url"),
            "evidence": [
                build_evidence(
                    final_url,
                    " | ".join(part for part in [title, clean_text(body, max_len=180)] if part),
                    ["url", "domain", "email", "person_name"],
                )
            ],
            "confidence": confidence,
            "match_features": {
                "reasons": as_string_list(reasons, max_items=10),
                "attempted_urls": attempted,
                "http_status": selected["status"],
            },
            "canonical_url": final_url,
            "site_title": title,
            "detected_technologies": technologies,
        }
    )
    return validate_result_shape(result)
