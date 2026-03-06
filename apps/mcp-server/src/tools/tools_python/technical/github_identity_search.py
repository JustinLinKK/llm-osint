from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse

_TOOLS_PYTHON_ROOT = Path(__file__).resolve().parents[1]
if str(_TOOLS_PYTHON_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_PYTHON_ROOT))

from matching.confidence import score_candidate_match
from technical.common import (
    as_string_list,
    build_base_result,
    build_evidence,
    clean_text,
    domain_from_email,
    domain_from_url,
    http_json_request,
    normalize_query,
    validate_result_shape,
)


SEARCH_USERS_URL = "https://api.github.com/search/users"
USER_URL = "https://api.github.com/users/{username}"
USER_ORGS_URL = "https://api.github.com/users/{username}/orgs"
USER_REPOS_URL = "https://api.github.com/users/{username}/repos"


def _github_headers() -> Dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _extract_username(query: Dict[str, Any]) -> str:
    username = str(query.get("username") or "").strip().lstrip("@")
    if username:
        return username
    profile_url = str(query.get("profile_url") or "").strip()
    if profile_url:
        parsed = urlparse(profile_url)
        if (parsed.hostname or "").lower() in {"github.com", "www.github.com"}:
            parts = [part for part in parsed.path.split("/") if part]
            if parts:
                return parts[0]
    repo_url = str(query.get("repo_url") or "").strip()
    if repo_url:
        parsed = urlparse(repo_url)
        if (parsed.hostname or "").lower() in {"github.com", "www.github.com"}:
            parts = [part for part in parsed.path.split("/") if part]
            if parts:
                return parts[0]
    return ""


def _search_query(query: Dict[str, Any]) -> str:
    pieces: List[str] = []
    if query.get("person_name"):
        pieces.append(str(query["person_name"]))
        pieces.append(f"{query['person_name']} in:fullname")
    if query.get("email"):
        pieces.append(str(query["email"]))
    if query.get("username"):
        pieces.append(str(query["username"]))
    if query.get("blog"):
        domain = domain_from_url(query["blog"])
        if domain:
            pieces.append(domain)
    if query.get("domain"):
        pieces.append(str(query["domain"]))
    return " ".join(piece for piece in pieces if piece).strip()


def _dedupe_strings(values: List[str]) -> List[str]:
    seen: set[str] = set()
    output: List[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        lowered = text.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        output.append(text)
    return output


def _person_name_variants(person_name: str) -> List[str]:
    normalized = " ".join(person_name.split()).strip()
    if not normalized:
        return []
    variants: List[str] = [normalized]
    tokens = [token for token in re.split(r"\s+", normalized) if token]
    if len(tokens) >= 2:
        variants.append(f"{tokens[0]} {tokens[-1]}")
    if len(tokens) >= 3:
        variants.append(f"{tokens[1]} {tokens[-1]}")
        for idx in range(len(tokens) - 1):
            variants.append(f"{tokens[idx]} {tokens[idx + 1]}")
    return _dedupe_strings(variants)


def _search_query_variants(query: Dict[str, Any]) -> List[str]:
    person_name = str(query.get("person_name") or "").strip()
    email = str(query.get("email") or "").strip()
    domain = str(query.get("domain") or "").strip()
    blog_domain = domain_from_url(query.get("blog"))

    variants: List[str] = []
    for name_variant in _person_name_variants(person_name):
        variants.append(name_variant)
        variants.append(f"{name_variant} in:fullname")

    if email:
        variants.append(email)
    if blog_domain:
        variants.append(blog_domain)
    if domain:
        variants.append(domain)

    fallback = _search_query(query)
    if fallback:
        variants.append(fallback)

    return _dedupe_strings(variants)


def _search_usernames(search_query: str, max_results: int) -> List[str]:
    from technical.common import http_request

    status, _, body, _ = http_request(
        SEARCH_USERS_URL,
        params={"q": search_query, "per_page": max(max_results, 5)},
        headers=_github_headers(),
    )
    if status == 422:
        return []
    if status >= 400:
        raise RuntimeError(f"GitHub search failed with HTTP {status}")

    import json

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return []
    items = parsed.get("items") if isinstance(parsed, dict) else []
    if not isinstance(items, list):
        return []
    return [
        str(item.get("login") or "").strip()
        for item in items[: max(max_results, 5)]
        if isinstance(item, dict) and str(item.get("login") or "").strip()
    ]


def _fetch_user(username: str) -> Dict[str, Any]:
    return http_json_request(USER_URL.format(username=username), headers=_github_headers())


def _fetch_orgs_list(username: str) -> List[Dict[str, Any]]:
    from technical.common import http_request
    status, _, body, _ = http_request(USER_ORGS_URL.format(username=username), headers=_github_headers())
    if status >= 400:
        return []
    import json
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return []
    return [item for item in parsed if isinstance(item, dict)] if isinstance(parsed, list) else []


def _fetch_repos(username: str) -> List[Dict[str, Any]]:
    from technical.common import http_request
    status, _, body, _ = http_request(
        USER_REPOS_URL.format(username=username),
        params={"per_page": 30, "sort": "updated"},
        headers=_github_headers(),
    )
    if status >= 400:
        return []
    import json
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return []
    return [item for item in parsed if isinstance(item, dict)] if isinstance(parsed, list) else []


def _candidate_result(query: Dict[str, Any], user: Dict[str, Any], orgs: List[Dict[str, Any]], repos: List[Dict[str, Any]]) -> Dict[str, Any]:
    username = str(user.get("login") or "").strip()
    display_name = str(user.get("name") or username).strip()
    public_email = str(user.get("email") or "").strip().lower()
    blog = str(user.get("blog") or "").strip()
    company = str(user.get("company") or "").strip()
    location = str(user.get("location") or "").strip()
    bio = clean_text(user.get("bio"), max_len=280)

    known_ids: Dict[str, Any] = {}
    if query.get("username"):
        known_ids["github"] = str(query["username"]).strip().lower()
    if query.get("email"):
        known_ids["email"] = str(query["email"]).strip().lower()
    if query.get("blog"):
        known_ids["blog_domain"] = domain_from_url(query["blog"])

    candidate_ids: Dict[str, Any] = {
        "github": username.lower(),
    }
    if public_email:
        candidate_ids["email"] = public_email
    blog_domain = domain_from_url(blog)
    if blog_domain:
        candidate_ids["blog_domain"] = blog_domain

    match = score_candidate_match(
        query_name=str(query.get("person_name") or display_name),
        candidate_name=display_name,
        query_affiliations=[],
        candidate_affiliations=[company] if company else [],
        query_topics=[],
        candidate_topics=[bio] if bio else [],
        known_ids=known_ids,
        candidate_ids=candidate_ids,
        homepage_domain_match=bool(blog_domain and blog_domain in {domain_from_url(query.get("blog")), query.get("domain"), domain_from_email(query.get("email"))}),
    )

    score = float(match["confidence"])
    reasons = list(match.get("match_features", {}).get("reasons", []))
    if query.get("username") and str(query["username"]).strip().lower() == username.lower():
        score = max(score, 0.95)
        reasons.append("direct username lookup")
    elif query.get("profile_url") and str(query["profile_url"]).strip().rstrip("/") == str(user.get("html_url") or "").strip().rstrip("/"):
        score = max(score, 0.96)
        reasons.append("direct profile URL lookup")
    elif public_email and query.get("email") and public_email == str(query["email"]).strip().lower():
        score = max(score, 0.92)
        reasons.append("public email matched query")

    repo_items: List[Dict[str, Any]] = []
    last_active = str(user.get("updated_at") or "").strip() or None
    latest_repo_push = ""
    for repo in repos[:10]:
        repo_url = str(repo.get("html_url") or "").strip()
        repo_name = str(repo.get("full_name") or repo.get("name") or "").strip()
        if not repo_url or not repo_name:
            continue
        pushed_at = str(repo.get("pushed_at") or "").strip()
        if pushed_at and pushed_at > latest_repo_push:
            latest_repo_push = pushed_at
        repo_items.append(
            {
                "name": repo_name,
                "url": repo_url,
                "description": clean_text(repo.get("description"), max_len=160),
                "stars": repo.get("stargazers_count"),
                "fork": bool(repo.get("fork")),
                "updated_at": pushed_at or repo.get("updated_at"),
            }
        )
    if latest_repo_push:
        last_active = latest_repo_push

    organizations = [
        {
            "name": str(org.get("login") or "").strip(),
            "url": str(org.get("html_url") or "").strip(),
            "relation": "member",
        }
        for org in orgs[:10]
        if isinstance(org, dict) and str(org.get("html_url") or "").strip()
    ]

    contact_signals: List[Dict[str, Any]] = []
    if public_email:
        contact_signals.append({"type": "email", "value": public_email, "source": "github_public_profile"})
    if company:
        contact_signals.append({"type": "company", "value": company, "source": "github_public_profile"})
    if location:
        contact_signals.append({"type": "location", "value": location, "source": "github_public_profile"})

    external_links = [{"type": "profile", "url": str(user.get("html_url") or "").strip()}]
    if blog:
        external_links.append({"type": "blog", "url": blog})

    result = build_base_result("github_identity_search", "github", query)
    result.update(
        {
            "stable_id": f"github:{user.get('id') or username.lower()}",
            "profile_url": str(user.get("html_url") or "").strip(),
            "created_at": user.get("created_at"),
            "last_active": last_active,
            "organizations": organizations,
            "repositories": repo_items,
            "contact_signals": contact_signals,
            "external_links": external_links,
            "evidence": [
                build_evidence(
                    str(user.get("html_url") or ""),
                    " | ".join(part for part in [display_name, username, bio, company, location] if part),
                    ["person_name", "username", "email", "blog"],
                ),
                build_evidence(
                    f"https://api.github.com/users/{username}",
                    f"GitHub public profile API response for {username}",
                    ["username"],
                ),
            ],
            "confidence": score,
            "match_features": {
                **(match.get("match_features") if isinstance(match.get("match_features"), dict) else {}),
                "reasons": as_string_list(reasons, max_items=10),
                "github_id": user.get("id"),
            },
            "username": username,
            "id": user.get("id"),
            "display_name": display_name,
            "bio": bio,
            "followers": user.get("followers"),
            "public_repos": user.get("public_repos"),
            "repo_count": len(repo_items),
        }
    )
    return validate_result_shape(result)


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    username = _extract_username(query)
    if username:
        user = _fetch_user(username)
        return _candidate_result(query, user, _fetch_orgs_list(username), _fetch_repos(username))

    search_queries = _search_query_variants(query)
    if not search_queries:
        raise RuntimeError("Missing required input: person_name, username, profile_url, email, or repo_url")

    candidate_usernames: List[str] = []
    target_candidate_count = max(query["max_results"], 5)
    candidate_username_limit = min(20, max(target_candidate_count * 3, 10))
    for search_query in search_queries[:6]:
        for candidate_username in _search_usernames(search_query, max_results=target_candidate_count):
            if candidate_username not in candidate_usernames:
                candidate_usernames.append(candidate_username)
            if len(candidate_usernames) >= candidate_username_limit:
                break
        if len(candidate_usernames) >= candidate_username_limit:
            break

    candidates: List[Dict[str, Any]] = []
    for candidate_username in candidate_usernames[:candidate_username_limit]:
        user = _fetch_user(candidate_username)
        candidates.append(_candidate_result(query, user, _fetch_orgs_list(candidate_username), _fetch_repos(candidate_username)))

    if not candidates:
        return validate_result_shape(build_base_result("github_identity_search", "github", query))
    candidates.sort(key=lambda item: float(item.get("confidence") or 0.0), reverse=True)
    top = candidates[0]
    top["match_features"]["candidate_count"] = len(candidates)
    top["match_features"]["alternate_usernames"] = [item.get("username") for item in candidates[1:4] if item.get("username")]
    return validate_result_shape(top)
