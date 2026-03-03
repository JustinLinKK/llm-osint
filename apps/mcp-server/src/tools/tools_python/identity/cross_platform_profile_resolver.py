from __future__ import annotations

from typing import Any, Dict, List


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _profile_name(item: Dict[str, Any]) -> str:
    for key in ("canonical_name", "full_name", "display_name", "name"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _name_parts(value: str) -> tuple[str, str]:
    parts = [part for part in _normalize_text(value).replace(",", " ").split() if part]
    if len(parts) < 2:
        return ("", "")
    return (parts[0], parts[-1])


def _collect_strings(item: Dict[str, Any], *keys: str) -> set[str]:
    values: set[str] = set()
    for key in keys:
        raw = item.get(key)
        if isinstance(raw, str) and raw.strip():
            values.add(_normalize_text(raw))
        elif isinstance(raw, list):
            for value in raw:
                if isinstance(value, str) and value.strip():
                    values.add(_normalize_text(value))
                elif isinstance(value, dict):
                    name = value.get("name")
                    if isinstance(name, str) and name.strip():
                        values.add(_normalize_text(name))
    return values


def _publication_titles(item: Dict[str, Any]) -> set[str]:
    publications = item.get("publications")
    if not isinstance(publications, list):
        return set()
    titles: set[str] = set()
    for publication in publications:
        if isinstance(publication, str) and publication.strip():
            titles.add(_normalize_text(publication))
        elif isinstance(publication, dict):
            for key in ("title", "name", "paper_title"):
                value = publication.get(key)
                if isinstance(value, str) and value.strip():
                    titles.add(_normalize_text(value))
                    break
    return titles


def _best_canonical_name(profiles: List[Dict[str, Any]]) -> str:
    named = [_profile_name(item) for item in profiles if _profile_name(item)]
    if not named:
        return ""
    named.sort(key=lambda value: (-len(value.split()), -len(value), value.lower()))
    return named[0]


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    profiles = input_data.get("profiles")
    if not isinstance(profiles, list):
        raise RuntimeError("Missing required input: profiles")
    normalized = [item for item in profiles if isinstance(item, dict)]
    if not normalized:
        return {"tool": "cross_platform_profile_resolver", "resolved_identity_id": "", "confidence": 0.0, "matched_profiles": []}

    usernames = {str(item.get("username") or "").strip().lower() for item in normalized if str(item.get("username") or "").strip()}
    bios = {str(item.get("bio") or "").strip().lower() for item in normalized if str(item.get("bio") or "").strip()}
    sites = {str(item.get("site") or item.get("profile_url") or "").strip().lower() for item in normalized if str(item.get("site") or item.get("profile_url") or "").strip()}
    names = [_profile_name(item) for item in normalized if _profile_name(item)]
    affiliations = [value for item in normalized for value in _collect_strings(item, "organization", "institution", "affiliation", "affiliations")]
    advisors = [value for item in normalized for value in _collect_strings(item, "advisor", "advisors")]
    publication_titles = [value for item in normalized for value in _publication_titles(item)]

    score = 0.0
    reasons: List[str] = []
    disambiguation_evidence: List[Dict[str, Any]] = []
    if len(usernames) == 1 and usernames:
        score += 0.5
        reasons.append("same username")
        disambiguation_evidence.append({"type": "username", "value": next(iter(usernames)), "strength": "strong"})
    if len(bios) == 1 and bios:
        score += 0.25
        reasons.append("same bio string")
        disambiguation_evidence.append({"type": "bio", "value": next(iter(bios)), "strength": "moderate"})
    if len(sites) == 1 and sites:
        score += 0.25
        reasons.append("same personal site link")
        disambiguation_evidence.append({"type": "site", "value": next(iter(sites)), "strength": "strong"})

    first_names = set()
    last_names = set()
    for name in names:
        first_name, last_name = _name_parts(name)
        if first_name:
            first_names.add(first_name)
        if last_name:
            last_names.add(last_name)

    if len(set(_normalize_text(name) for name in names)) == 1 and names:
        score += 0.35
        reasons.append("same display name")
        disambiguation_evidence.append({"type": "name_exact", "value": names[0], "strength": "strong"})
    elif len(last_names) == 1 and last_names:
        score += 0.1
        reasons.append("same family name")
        disambiguation_evidence.append({"type": "family_name", "value": next(iter(last_names)), "strength": "weak"})

    affiliation_overlap = sorted({value for value in affiliations if value})
    if affiliation_overlap:
        score += 0.15
        reasons.append("shared affiliation signal")
        disambiguation_evidence.append({"type": "affiliation", "value": affiliation_overlap[0], "strength": "moderate"})

    advisor_overlap = sorted({value for value in advisors if value})
    if advisor_overlap:
        score += 0.15
        reasons.append("shared advisor signal")
        disambiguation_evidence.append({"type": "advisor", "value": advisor_overlap[0], "strength": "moderate"})

    publication_overlap = sorted({value for value in publication_titles if value})
    if publication_overlap:
        score += 0.2
        reasons.append("shared publication signal")
        disambiguation_evidence.append({"type": "publication", "value": publication_overlap[0], "strength": "strong"})

    independent_signal_types = {item["type"] for item in disambiguation_evidence}
    if len(last_names) == 1 and len(independent_signal_types - {"family_name"}) >= 2:
        score = max(score, 0.8)

    canonical_name = _best_canonical_name(normalized)
    aliases = sorted({name for name in names if name and name != canonical_name})
    resolved_identity_id = next(iter(sites), "") or next(iter(usernames), "") or canonical_name
    return {
        "tool": "cross_platform_profile_resolver",
        "resolved_identity_id": resolved_identity_id,
        "confidence": round(min(score, 1.0), 4),
        "matched_profiles": normalized,
        "canonical_identity": {
            "canonical_name": canonical_name,
            "aliases": aliases,
            "profile_count": len(normalized),
        },
        "disambiguation_evidence": disambiguation_evidence[:8],
        "match_features": {"reasons": reasons, "independent_signal_count": len(independent_signal_types)},
    }
