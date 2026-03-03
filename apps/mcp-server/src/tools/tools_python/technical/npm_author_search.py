from __future__ import annotations

from typing import Any, Dict, List

from technical.common import (
    build_base_result,
    build_evidence,
    clean_text,
    domain_from_url,
    http_json_request,
    normalize_query,
    validate_result_shape,
)


SEARCH_URL = "https://registry.npmjs.org/-/v1/search"


def _query_text(query: Dict[str, Any]) -> str:
    if query.get("username"):
        return f"maintainer:{query['username']}"
    if query.get("email"):
        return str(query["email"])
    if query.get("person_name"):
        return f"author:{query['person_name']}"
    raise RuntimeError("Missing required input: person_name, username, or email")


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_query(input_data)
    raw = http_json_request(
        SEARCH_URL,
        params={"text": _query_text(query), "size": query["max_results"]},
        timeout=20,
    )
    objects = raw.get("objects") if isinstance(raw.get("objects"), list) else []

    publications: List[Dict[str, Any]] = []
    repositories: List[Dict[str, Any]] = []
    contact_signals: List[Dict[str, Any]] = []
    external_links: List[Dict[str, Any]] = []
    evidence: List[Dict[str, Any]] = []
    namespaces: Dict[str, int] = {}

    for item in objects[: query["max_results"]]:
        if not isinstance(item, dict):
            continue
        package = item.get("package") if isinstance(item.get("package"), dict) else {}
        score = item.get("score") if isinstance(item.get("score"), dict) else {}
        package_name = str(package.get("name") or "").strip()
        if not package_name:
            continue
        repo_url = ""
        links = package.get("links") if isinstance(package.get("links"), dict) else {}
        if isinstance(links.get("repository"), str):
            repo_url = str(links["repository"]).strip()
        if package_name.startswith("@") and "/" in package_name:
            namespace = package_name.split("/", 1)[0]
            namespaces[namespace] = namespaces.get(namespace, 0) + 1
        maintainers = package.get("maintainers") if isinstance(package.get("maintainers"), list) else []
        maintainer_names = []
        for maintainer in maintainers[:5]:
            if not isinstance(maintainer, dict):
                continue
            username = str(maintainer.get("username") or "").strip()
            email = str(maintainer.get("email") or "").strip().lower()
            if username:
                maintainer_names.append(username)
                contact_signals.append({"type": "npm_username", "value": username, "source": package_name})
            if email:
                contact_signals.append({"type": "email", "value": email, "source": package_name})
        publication = {
            "name": package_name,
            "url": str(links.get("npm") or f"https://www.npmjs.com/package/{package_name}"),
            "version": package.get("version"),
            "license": package.get("license"),
            "publish_date": package.get("date"),
            "maintainers": maintainer_names,
            "repository_url": repo_url or None,
            "description": clean_text(package.get("description"), max_len=180),
            "score": score.get("final"),
        }
        publications.append(publication)
        external_links.append({"type": "npm_package", "url": publication["url"]})
        if repo_url:
            repositories.append({"name": package_name, "url": repo_url, "language": None, "updated_at": package.get("date")})
            external_links.append({"type": "repository", "url": repo_url})
        evidence.append(
            build_evidence(
                publication["url"],
                " | ".join(part for part in [package_name, publication["description"], repo_url] if isinstance(part, str) and part),
                ["person_name", "username", "email"],
            )
        )

    result = build_base_result("npm_author_search", "npm", query)
    result.update(
        {
            "stable_id": f"npm:{query.get('username') or query.get('email') or query.get('person_name') or 'search'}",
            "profile_url": f"https://www.npmjs.com/~{query['username']}" if query.get("username") else "",
            "organizations": [
                {"name": namespace, "url": f"https://www.npmjs.com/org/{namespace.lstrip('@')}", "relation": "owns_namespace"}
                for namespace in namespaces
            ],
            "repositories": repositories,
            "publications": publications,
            "contact_signals": contact_signals,
            "external_links": external_links,
            "evidence": evidence,
            "confidence": 0.78 if publications else 0.0,
            "match_features": {
                "reasons": ["public npm package search match"] if publications else [],
                "package_count": len(publications),
                "scoped_namespace_count": len(namespaces),
            },
        }
    )
    return validate_result_shape(result)
