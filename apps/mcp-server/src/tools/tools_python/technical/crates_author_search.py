from __future__ import annotations

from typing import Any, Dict, List

from technical.common import build_base_result, build_evidence, clean_text, http_json_request, normalize_query, validate_result_shape


USER_SEARCH_URL = "https://crates.io/api/v1/users"
USER_CRATES_URL = "https://crates.io/api/v1/crates"


def _search_term(query: Dict[str, Any]) -> str:
    if query.get("username"):
        return str(query["username"])
    if query.get("person_name"):
        return str(query["person_name"])
    raise RuntimeError("Missing required input: person_name or username")


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    users_raw = http_json_request(USER_SEARCH_URL, params={"q": _search_term(query), "per_page": query["max_results"]}, timeout=20)
    users = users_raw.get("users") if isinstance(users_raw.get("users"), list) else []
    if not users:
        return validate_result_shape(build_base_result("crates_author_search", "crates.io", query))

    user = next((item for item in users if isinstance(item, dict)), {})
    user_id = user.get("id")
    if user_id is None:
        return validate_result_shape(build_base_result("crates_author_search", "crates.io", query))

    crates_raw = http_json_request(USER_CRATES_URL, params={"user_id": user_id, "per_page": 20}, timeout=20)
    crates = crates_raw.get("crates") if isinstance(crates_raw.get("crates"), list) else []

    publications: List[Dict[str, Any]] = []
    repositories: List[Dict[str, Any]] = []
    evidence: List[Dict[str, Any]] = []
    for crate in crates[: query["max_results"]]:
        if not isinstance(crate, dict):
            continue
        crate_name = str(crate.get("id") or crate.get("name") or "").strip()
        if not crate_name:
            continue
        crate_url = f"https://crates.io/crates/{crate_name}"
        repo_url = str(crate.get("repository") or "").strip()
        publication = {
            "name": crate_name,
            "url": crate_url,
            "version": crate.get("newest_version"),
            "license": crate.get("max_stable_version"),
            "publish_date": crate.get("updated_at"),
            "repository_url": repo_url or None,
            "downloads": crate.get("downloads"),
            "description": clean_text(crate.get("description"), max_len=180),
        }
        publications.append(publication)
        if repo_url:
            repositories.append(
                {"name": crate_name, "url": repo_url, "language": "Rust", "updated_at": crate.get("updated_at")}
            )
        evidence.append(
            build_evidence(
                crate_url,
                " | ".join(part for part in [crate_name, publication["description"], repo_url] if isinstance(part, str) and part),
                ["person_name", "username"],
            )
        )

    username = str(user.get("login") or user.get("name") or "").strip()
    result = build_base_result("crates_author_search", "crates.io", query)
    result.update(
        {
            "stable_id": f"crates:{user_id}",
            "profile_url": f"https://crates.io/users/{user_id}",
            "organizations": [],
            "repositories": repositories,
            "publications": publications,
            "contact_signals": [{"type": "crates_username", "value": username, "source": "crates.io"}] if username else [],
            "external_links": [{"type": "crates_profile", "url": f"https://crates.io/users/{user_id}"}],
            "evidence": evidence,
            "confidence": 0.76 if publications else 0.0,
            "match_features": {
                "reasons": ["public crates.io author search match"] if publications else [],
                "crate_count": len(publications),
            },
            "username": username,
        }
    )
    return validate_result_shape(result)
