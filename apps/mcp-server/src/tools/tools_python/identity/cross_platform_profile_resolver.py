from __future__ import annotations

import re
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

GENERIC_MULTI_TENANT_DOMAINS = {
    "github.com",
    "gitlab.com",
    "bitbucket.org",
    "linkedin.com",
    "www.linkedin.com",
    "x.com",
    "twitter.com",
    "researchgate.net",
    "scholar.google.com",
    "semanticscholar.org",
}


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


def _name_signature(value: str) -> str:
    return " ".join(part for part in _normalize_text(value).replace(",", " ").split() if part)


def _domain_from_profile(item: Dict[str, Any]) -> str:
    for key in ("site", "profile_url", "url"):
        value = item.get(key)
        if not isinstance(value, str) or not value.strip():
            continue
        host = (urlparse(value).hostname or "").lower().strip()
        if host.startswith("www."):
            host = host[4:]
        if host:
            return host
    return ""


def _compact_alpha_signature(value: Any) -> str:
    return "".join(ch for ch in _normalize_text(value) if ch.isalpha())


def _profile_path_identifiers(item: Dict[str, Any]) -> set[str]:
    identifiers: set[str] = set()
    username = str(item.get("username") or "").strip().lower().lstrip("@")
    if username:
        identifiers.add(username)
    for key in ("site", "profile_url", "url"):
        value = item.get(key)
        if not isinstance(value, str) or not value.strip():
            continue
        parsed = urlparse(value)
        host = (parsed.hostname or "").lower().strip()
        if host.startswith("www."):
            host = host[4:]
        path_parts = [part.strip().lstrip("@") for part in parsed.path.split("/") if part.strip()]
        if host in {"github.com", "gitlab.com", "bitbucket.org", "x.com", "twitter.com"}:
            if path_parts:
                identifiers.add(path_parts[0].lower())
        elif host == "linkedin.com":
            if len(path_parts) >= 2 and path_parts[0].casefold() in {"in", "pub"}:
                identifiers.add(path_parts[1].lower())
        elif host == "researchgate.net":
            if len(path_parts) >= 2 and path_parts[0].casefold() == "profile":
                identifiers.add(path_parts[1].lower())
        elif host == "scholar.google.com":
            user_id = parse_qs(parsed.query).get("user", [])
            if user_id and str(user_id[0]).strip():
                identifiers.add(str(user_id[0]).strip().lower())
        elif host == "semanticscholar.org":
            lowered_parts = [part.casefold() for part in path_parts]
            if "author" in lowered_parts:
                index = lowered_parts.index("author")
                if index + 1 < len(path_parts):
                    identifiers.add(path_parts[index + 1].lower())
                if path_parts:
                    identifiers.add(path_parts[-1].lower())
    return {item for item in identifiers if item}


def _profile_path_matches_contract(
    profile: Dict[str, Any],
    aliases: List[str],
    approved_handles: set[str],
) -> tuple[bool, str]:
    alias_signatures = {_name_signature(alias) for alias in aliases if _name_signature(alias)}
    alias_compacts = {_compact_alpha_signature(alias) for alias in aliases if _compact_alpha_signature(alias)}
    for identifier in _profile_path_identifiers(profile):
        normalized = identifier.strip().lower().lstrip("@")
        if not normalized:
            continue
        if normalized in approved_handles:
            return (True, "path_handle_match")
        separator_tokens = [token for token in re.split(r"[^a-z0-9]+", normalized) if token]
        alpha_tokens = [token for token in separator_tokens if token.isalpha()]
        if alpha_tokens:
            alpha_signature = " ".join(alpha_tokens)
            if alpha_signature in alias_signatures or "".join(alpha_tokens) in alias_compacts:
                return (True, "path_name_match")
        compact = "".join(ch for ch in normalized if ch.isalpha())
        if compact and compact in alias_compacts:
            return (True, "path_name_match")
    return (False, "profile path did not match target contract handles or aliases")


def _contract_aliases(contract: Dict[str, Any]) -> List[str]:
    aliases: List[str] = []
    for key in ("canonical_name",):
        value = contract.get(key)
        if isinstance(value, str) and value.strip():
            aliases.append(value.strip())
    for key in ("prompt_targets", "approved_aliases"):
        value = contract.get(key)
        if isinstance(value, list):
            aliases.extend([str(item).strip() for item in value if str(item).strip()])
    deduped: List[str] = []
    seen: set[str] = set()
    for alias in aliases:
        signature = _name_signature(alias)
        if not signature or signature in seen:
            continue
        seen.add(signature)
        deduped.append(alias)
    return deduped


def _profile_matches_target_contract(profile: Dict[str, Any], contract: Dict[str, Any]) -> tuple[bool, str]:
    if not isinstance(contract, dict) or not contract:
        return (True, "")
    aliases = _contract_aliases(contract)
    approved_handles = {
        str(item).strip().lower().lstrip("@")
        for item in contract.get("approved_handles", [])
        if str(item).strip()
    }
    approved_domains = {
        str(item).strip().lower()
        for item in contract.get("approved_domains", [])
        if str(item).strip()
    }
    profile_name = _profile_name(profile)
    if profile_name:
        profile_signature = _name_signature(profile_name)
        for alias in aliases:
            alias_signature = _name_signature(alias)
            if profile_signature and alias_signature and (
                profile_signature == alias_signature
                or set(profile_signature.split()) == set(alias_signature.split())
                or set(profile_signature.split()).issubset(set(alias_signature.split()))
                or set(alias_signature.split()).issubset(set(profile_signature.split()))
            ):
                return (True, "name_match")
    username = str(profile.get("username") or "").strip().lower().lstrip("@")
    if username and username in approved_handles:
        return (True, "handle_match")
    domain = _domain_from_profile(profile)
    if domain in GENERIC_MULTI_TENANT_DOMAINS:
        matched, reason = _profile_path_matches_contract(profile, aliases, approved_handles)
        if matched:
            return (True, reason)
    if domain and domain in approved_domains and domain not in GENERIC_MULTI_TENANT_DOMAINS:
        return (True, "domain_match")
    return (False, "profile did not match target contract aliases, handles, or approved domains")


def _best_canonical_name_for_contract(profiles: List[Dict[str, Any]], contract: Dict[str, Any]) -> str:
    contract_name = str(contract.get("canonical_name") or "").strip() if isinstance(contract, dict) else ""
    if contract_name:
        contract_signature = _name_signature(contract_name)
        for profile in profiles:
            profile_name = _profile_name(profile)
            if profile_name and _name_signature(profile_name) == contract_signature:
                return profile_name
        if any(_profile_matches_target_contract(profile, contract)[0] for profile in profiles):
            return contract_name
    return _best_canonical_name(profiles)


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    profiles = input_data.get("profiles")
    if not isinstance(profiles, list):
        raise RuntimeError("Missing required input: profiles")
    normalized = [item for item in profiles if isinstance(item, dict)]
    if not normalized:
        return {"tool": "cross_platform_profile_resolver", "resolved_identity_id": "", "confidence": 0.0, "matched_profiles": []}
    target_contract = input_data.get("target_contract") if isinstance(input_data.get("target_contract"), dict) else {}
    accepted_profiles: List[Dict[str, Any]] = []
    rejected_profiles: List[Dict[str, Any]] = []
    rejected_reasons: List[Dict[str, Any]] = []
    for profile in normalized:
        accepted, reason = _profile_matches_target_contract(profile, target_contract)
        if accepted:
            accepted_profiles.append(profile)
        else:
            rejected_profiles.append(profile)
            rejected_reasons.append(
                {
                    "profile_name": _profile_name(profile),
                    "platform": str(profile.get("platform") or "").strip(),
                    "reason": reason,
                }
            )
    normalized = accepted_profiles if target_contract else (accepted_profiles if accepted_profiles else normalized)

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

    canonical_name = _best_canonical_name_for_contract(normalized, target_contract)
    aliases = sorted({name for name in names if name and name != canonical_name})
    resolved_identity_id = next(iter(sites), "") or next(iter(usernames), "") or canonical_name
    return {
        "tool": "cross_platform_profile_resolver",
        "resolved_identity_id": resolved_identity_id,
        "confidence": round(min(score, 1.0), 4),
        "matched_profiles": normalized,
        "rejected_profiles": rejected_profiles[:8],
        "rejected_profile_reasons": rejected_reasons[:8],
        "canonical_identity": {
            "canonical_name": canonical_name,
            "aliases": aliases,
            "profile_count": len(normalized),
        },
        "disambiguation_evidence": disambiguation_evidence[:8],
        "match_features": {"reasons": reasons, "independent_signal_count": len(independent_signal_types)},
    }
