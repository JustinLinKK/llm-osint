from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urlparse


def build_technical_graph_entities(tool_name: str, arguments: Dict[str, Any], result: Dict[str, Any]) -> List[Dict[str, Any]]:
    if tool_name not in {
        "github_identity_search",
        "gitlab_identity_search",
        "wayback_fetch_url",
        "package_registry_search",
        "npm_author_search",
        "crates_author_search",
    }:
        return []

    if tool_name == "wayback_fetch_url":
        return _build_wayback_graph_entities(result)

    stable_id = str(result.get("stable_id") or "").strip()
    profile_url = str(result.get("profile_url") or "").strip()
    entity_type = "Person" if tool_name in {"github_identity_search", "gitlab_identity_search"} else "RegistryAccount"
    entity_id = stable_id or profile_url or _fallback_registry_entity_id(tool_name, arguments)
    if not entity_id:
        return []

    properties: Dict[str, Any] = {"platform": result.get("platform")}
    for key in ("username", "display_name", "bio", "repo_count", "profile_url"):
        value = result.get(key)
        if value not in (None, "", []):
            properties[key] = value
    if profile_url:
        properties["profile_url"] = profile_url

    relations: List[Dict[str, Any]] = []
    for organization in _as_dict_list(result.get("organizations"))[:20]:
        org_name = str(organization.get("name") or "").strip()
        org_url = str(organization.get("url") or "").strip()
        if not org_name and not org_url:
            continue
        relation = {
            "type": "MEMBER_OF",
            "targetType": "CodeOrganization",
            "targetId": org_url or f"codeorg:{org_name.lower()}",
            "targetProperties": {
                "name": org_name or org_url,
                "url": org_url or None,
                "platform": result.get("platform"),
            },
        }
        relations.append(relation)

    for repository in _as_dict_list(result.get("repositories"))[:20]:
        repo_name = str(repository.get("name") or "").strip()
        repo_url = str(repository.get("url") or "").strip()
        if not repo_name and not repo_url:
            continue
        relation = {
            "type": "MAINTAINS",
            "targetType": "Repository",
            "targetId": repo_url or f"repo:{repo_name.lower()}",
            "targetProperties": {
                "name": repo_name or repo_url,
                "url": repo_url or None,
                "updated_at": repository.get("updated_at"),
                "platform": _repo_platform(repo_url, result.get("platform")),
            },
        }
        relations.append(relation)

    publication_rel = "PUBLISHED_PACKAGE"
    for publication in _as_dict_list(result.get("publications"))[:20]:
        package_name = str(publication.get("name") or "").strip()
        package_url = str(publication.get("url") or "").strip()
        if not package_name and not package_url:
            continue
        relation = {
            "type": publication_rel,
            "targetType": "Package",
            "targetId": package_url or f"package:{package_name.lower()}",
            "targetProperties": {
                "name": package_name or package_url,
                "url": package_url or None,
                "version": publication.get("version"),
                "license": publication.get("license"),
                "publish_date": publication.get("publish_date"),
                "platform": result.get("platform"),
            },
        }
        relations.append(relation)
        repo_url = str(publication.get("repository_url") or "").strip()
        if repo_url:
            relations.append(
                {
                    "type": "HOSTED_ON",
                    "targetType": "Repository",
                    "targetId": repo_url,
                    "targetProperties": {
                        "url": repo_url,
                        "platform": _repo_platform(repo_url, result.get("platform")),
                    },
                }
            )

    entity: Dict[str, Any] = {
        "entityType": entity_type,
        "entityId": entity_id,
        "properties": properties,
        "relations": relations,
    }
    return [entity]


def _as_dict_list(value: Any) -> List[Dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _repo_platform(url: str, default_platform: Any) -> str | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    if host == "github.com":
        return "github"
    if host == "gitlab.com":
        return "gitlab"
    return str(default_platform) if default_platform else None


def _fallback_registry_entity_id(tool_name: str, arguments: Dict[str, Any]) -> str:
    username = str(arguments.get("username") or "").strip()
    email = str(arguments.get("email") or "").strip().lower()
    person_name = str(arguments.get("person_name") or arguments.get("name") or "").strip().lower().replace(" ", "_")
    if username:
        return f"{tool_name}:{username}"
    if email:
        return f"{tool_name}:{email}"
    if person_name:
        return f"{tool_name}:{person_name}"
    return ""


def _build_wayback_graph_entities(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    original_url = str(result.get("original_url") or "").strip()
    snapshots = _as_dict_list(result.get("snapshots"))
    if not original_url or not snapshots:
        return []

    relations: List[Dict[str, Any]] = []
    for snapshot in snapshots[:10]:
        archived_url = str(snapshot.get("archived_url") or "").strip()
        timestamp = str(snapshot.get("timestamp") or "").strip()
        if not archived_url:
            continue
        relations.append(
            {
                "type": "APPEARS_IN_ARCHIVE",
                "targetType": "ArchivedPage",
                "targetId": archived_url,
                "targetProperties": {
                    "url": archived_url,
                    "original_url": original_url,
                    "timestamp": timestamp or None,
                    "mime_type": snapshot.get("mime_type"),
                },
            }
        )

    return [
        {
            "entityType": "Article",
            "entityId": original_url,
            "properties": {"uri": original_url, "url": original_url},
            "relations": relations,
        }
    ]
