from __future__ import annotations

from typing import Any, Dict, List

from technical.common import build_base_result, validate_result_shape
from technical.crates_author_search import run as run_crates_author_search
from technical.npm_author_search import run as run_npm_author_search


def _merge_list_dicts(items: List[List[Dict[str, Any]]], key: str) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    output: List[Dict[str, Any]] = []
    for collection in items:
        for item in collection:
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


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    errors: List[str] = []
    for handler in (run_npm_author_search, run_crates_author_search):
        try:
            results.append(handler(input_data))
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))

    query = dict(input_data)
    result = build_base_result("package_registry_search", "package_registries", query)
    if not results:
        result["match_features"] = {"reasons": ["all package registry searches failed"], "errors": errors}
        return validate_result_shape(result)

    publications = _merge_list_dicts([item.get("publications", []) for item in results], key="url")
    repositories = _merge_list_dicts([item.get("repositories", []) for item in results], key="url")
    organizations = _merge_list_dicts([item.get("organizations", []) for item in results], key="url")
    contact_signals = _merge_list_dicts([item.get("contact_signals", []) for item in results], key="value")
    external_links = _merge_list_dicts([item.get("external_links", []) for item in results], key="url")
    evidence = _merge_list_dicts([item.get("evidence", []) for item in results], key="url")

    result.update(
        {
            "stable_id": "package_registry:aggregate",
            "profile_url": "",
            "organizations": organizations,
            "repositories": repositories,
            "publications": publications,
            "contact_signals": contact_signals,
            "external_links": external_links,
            "evidence": evidence,
            "confidence": max(float(item.get("confidence") or 0.0) for item in results),
            "match_features": {
                "reasons": ["aggregated registry search"],
                "registries_queried": [item.get("platform") for item in results if item.get("platform")],
                "errors": errors,
            },
        }
    )
    return validate_result_shape(result)
