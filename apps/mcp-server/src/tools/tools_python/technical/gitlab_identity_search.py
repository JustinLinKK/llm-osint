from __future__ import annotations

import sys
from collections import Counter
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
    domain_from_url,
    http_json_or_list_request,
    normalize_query,
    validate_result_shape,
)


USERS_URL = "https://gitlab.com/api/v4/users"
USER_PROJECTS_URL = "https://gitlab.com/api/v4/users/{user_id}/projects"


def _extract_username(query: Dict[str, Any]) -> str:
    username = str(query.get("username") or "").strip().lstrip("@")
    if username:
        return username
    profile_url = str(query.get("profile_url") or "").strip()
    if profile_url:
        parsed = urlparse(profile_url)
        if (parsed.hostname or "").lower() in {"gitlab.com", "www.gitlab.com"}:
            parts = [part for part in parsed.path.split("/") if part]
            if parts:
                return parts[0]
    return ""


def _fetch_candidates(query: Dict[str, Any]) -> List[Dict[str, Any]]:
    username = _extract_username(query)
    if username:
        raw = http_json_or_list_request(USERS_URL, params={"username": username, "per_page": 5}, timeout=20)
        return [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []

    search_query = str(query.get("person_name") or query.get("username") or "").strip()
    if not search_query:
        raise RuntimeError("Missing required input: person_name, username, or profile_url")
    raw = http_json_or_list_request(USERS_URL, params={"search": search_query, "per_page": query["max_results"]}, timeout=20)
    return [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []


def _fetch_projects(user_id: Any) -> List[Dict[str, Any]]:
    raw = http_json_or_list_request(
        USER_PROJECTS_URL.format(user_id=user_id),
        params={"simple": True, "per_page": 20, "order_by": "last_activity_at"},
        timeout=20,
    )
    return [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []


def _candidate_result(query: Dict[str, Any], user: Dict[str, Any], projects: List[Dict[str, Any]]) -> Dict[str, Any]:
    username = str(user.get("username") or "").strip()
    display_name = str(user.get("name") or username).strip()
    profile_url = str(user.get("web_url") or "").strip()
    bio = clean_text(user.get("bio"), max_len=240)
    location = clean_text(user.get("location"), max_len=120)
    company = clean_text(user.get("organization"), max_len=120)

    match = score_candidate_match(
        query_name=str(query.get("person_name") or display_name),
        candidate_name=display_name,
        query_affiliations=[],
        candidate_affiliations=[company] if company else [],
        query_topics=[],
        candidate_topics=[bio] if bio else [],
        known_ids={"gitlab": str(query.get("username") or "").lower()} if query.get("username") else {},
        candidate_ids={"gitlab": username.lower()},
        homepage_domain_match=bool(query.get("profile_url") and str(query["profile_url"]).rstrip("/") == profile_url.rstrip("/")),
    )
    confidence = float(match["confidence"])
    reasons = list(match.get("match_features", {}).get("reasons", []))
    if query.get("username") and str(query["username"]).strip().lower() == username.lower():
        confidence = max(confidence, 0.94)
        reasons.append("direct username lookup")

    repo_items: List[Dict[str, Any]] = []
    org_items: List[Dict[str, Any]] = []
    language_counter: Counter[str] = Counter()
    for project in projects[:10]:
        project_url = str(project.get("web_url") or "").strip()
        path_with_namespace = str(project.get("path_with_namespace") or "").strip()
        if not project_url or not path_with_namespace:
            continue
        namespace = project.get("namespace") if isinstance(project.get("namespace"), dict) else {}
        namespace_name = str(namespace.get("full_path") or namespace.get("path") or "").strip()
        if namespace_name and namespace_name.lower() != username.lower():
            org_items.append(
                {
                    "name": namespace_name,
                    "url": f"https://gitlab.com/{namespace_name}",
                    "relation": "namespace_member",
                }
            )
        language = str(project.get("language") or "").strip()
        if language:
            language_counter[language] += 1
        repo_items.append(
            {
                "name": path_with_namespace,
                "url": project_url,
                "language": language or None,
                "description": clean_text(project.get("description"), max_len=160),
                "stars": project.get("star_count"),
                "updated_at": project.get("last_activity_at"),
            }
        )

    result = build_base_result("gitlab_identity_search", "gitlab", query)
    result.update(
        {
            "stable_id": f"gitlab:{user.get('id') or username.lower()}",
            "profile_url": profile_url,
            "created_at": user.get("created_at"),
            "last_active": repo_items[0].get("updated_at") if repo_items else user.get("created_at"),
            "organizations": org_items,
            "repositories": repo_items,
            "contact_signals": [{"type": "location", "value": location, "source": "gitlab_public_profile"}] if location else [],
            "external_links": [{"type": "profile", "url": profile_url}],
            "evidence": [
                build_evidence(
                    profile_url,
                    " | ".join(part for part in [display_name, username, bio, location, company] if part),
                    ["person_name", "username"],
                )
            ],
            "confidence": confidence,
            "match_features": {
                **(match.get("match_features") if isinstance(match.get("match_features"), dict) else {}),
                "reasons": as_string_list(reasons, max_items=10),
            },
            "username": username,
            "display_name": display_name,
            "bio": bio,
            "repo_count": len(repo_items),
            "top_languages": [name for name, _ in language_counter.most_common(5)],
        }
    )
    return validate_result_shape(result)


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    candidates = _fetch_candidates(query)
    if not candidates:
        return validate_result_shape(build_base_result("gitlab_identity_search", "gitlab", query))
    results = []
    for user in candidates[: query["max_results"]]:
        results.append(_candidate_result(query, user, _fetch_projects(user.get("id"))))
    results.sort(key=lambda item: float(item.get("confidence") or 0.0), reverse=True)
    top = results[0]
    top["match_features"]["candidate_count"] = len(results)
    return validate_result_shape(top)
