from __future__ import annotations

from typing import Any, Dict, List


def _normalized_roles(input_data: Dict[str, Any]) -> List[Dict[str, str]]:
    roles: List[Dict[str, str]] = []
    for key in ("roles", "officers", "directorships"):
        value = input_data.get(key)
        if not isinstance(value, list):
            continue
        for item in value:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or item.get("person_name") or item.get("director_name") or "").strip()
            company = str(item.get("company_name") or item.get("company") or "").strip()
            role = str(item.get("role") or item.get("position") or "").strip()
            if name and company:
                roles.append({"name": name, "company": company, "role": role})
    return roles


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    roles = _normalized_roles(input_data)
    by_person: Dict[str, Dict[str, Any]] = {}
    for item in roles:
        key = item["name"].lower()
        record = by_person.setdefault(item["name"], {"companies": [], "roles": []})
        if item["company"] not in record["companies"]:
            record["companies"].append(item["company"])
        if item["role"] and item["role"] not in record["roles"]:
            record["roles"].append(item["role"])

    overlaps = [
        {"name": name, "companies": record["companies"], "roles": record["roles"]}
        for name, record in by_person.items()
        if len(record["companies"]) > 1
    ]
    overlaps.sort(key=lambda item: (-len(item["companies"]), item["name"].lower()))
    return {"tool": "board_member_overlap_search", "overlaps": overlaps[:20]}

