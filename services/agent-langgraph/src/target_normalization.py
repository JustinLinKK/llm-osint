from __future__ import annotations

import re
from typing import Any, Dict, List

URL_REGEX = re.compile(r"https?://[^\s\]]+")
EMAIL_REGEX = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
DOMAIN_REGEX = re.compile(
    r"\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}\b", re.IGNORECASE
)
USERNAME_REGEX = re.compile(r"(?<!\w)@([A-Za-z0-9](?:[A-Za-z0-9_.-]{1,62}[A-Za-z0-9])?)")
CAPITALIZED_NAME_REGEX = re.compile(r"\b[A-Z][a-z]+(?:[\s-]+[A-Z][a-z]+){0,3}\b")
PARENTHETICAL_ALIAS_NAME_REGEX = re.compile(
    r"\b([A-Za-z][A-Za-z'-]{1,31})\s*\(\s*([A-Za-z][A-Za-z'-]{1,31})\s*\)\s*([A-Za-z][A-Za-z'-]{1,31})\b"
)
PERSON_HINT_REGEX = re.compile(
    r"(?i)\b(?:investigate|investigation(?:\s+into)?|profile|research|look\s+into|find\s+info\s+on|osint(?:\s+on)?)\b[:\s-]*([A-Za-z][A-Za-z'\s-]{1,79})"
)

PERSON_CANDIDATE_STOPWORDS = {
    "please",
    "map",
    "mapping",
    "investigate",
    "investigation",
    "profile",
    "research",
    "look",
    "into",
    "find",
    "info",
    "osint",
    "person",
    "target",
    "public",
    "records",
    "social",
    "domain",
    "email",
    "phone",
    "website",
    "company",
    "organization",
    "account",
    "accounts",
    "repository",
    "repositories",
    "github",
    "gitlab",
    "linkedin",
}
PERSON_CANDIDATE_BREAKWORDS = {
    "and",
    "or",
    "of",
    "the",
    "with",
    "for",
    "about",
    "gather",
    "collect",
    "find",
    "look",
    "into",
}

PERSON_CANDIDATE_REJECT_TOKENS = {
    "none",
    "null",
    "unknown",
    "na",
    "n/a",
    "search",
    "through",
    "internet",
    "confidence",
    "score",
    "google",
    "serp",
    "queried",
    "query",
    "ran",
    "searched",
    "fetched",
    "discovered",
    "reviewed",
    "found",
    "result",
    "results",
    "source",
    "sources",
    "types",
    "public",
    "web",
    "profile",
    "profiles",
    "person",
    "people",
    "target",
    "targets",
    "evidence",
    "summary",
    "summaries",
    "pivots",
    "pivot",
    "archive",
    "archived",
    "downloaded",
    "download",
    "history",
    "contact",
    "relationship",
    "relationships",
    "account",
    "accounts",
    "biography",
    "biographic",
    "women",
    "woman",
    "men",
    "man",
    "her",
    "him",
    "you",
    "your",
    "our",
    "their",
    "them",
    "us",
    "engineer",
    "engineers",
    "engineering",
    "biomedical",
    "mechanical",
    "electrical",
    "software",
    "technology",
    "technologies",
    "science",
    "sciences",
    "department",
    "guide",
    "overview",
    "article",
    "articles",
    "occupation",
    "occupations",
    "specialization",
    "specializations",
    "career",
    "careers",
    "tech",
    "wikipedia",
    "society",
    "publication",
    "publications",
    "record",
    "records",
    "candidate",
    "candidates",
    "github",
    "gitlab",
    "linkedin",
    "repository",
    "repositories",
    "map",
    "mapping",
}

PERSON_CANDIDATE_REJECT_PHRASES = (
    "search through the internet",
    "confidence score",
    "ran google",
    "ran google serp",
    "searched public web",
    "public web sources",
    "queried arxiv",
    "source types include",
)

PERSON_CANDIDATE_REJECT_SUFFIXES = {
    "article",
    "account",
    "accounts",
    "articles",
    "career",
    "careers",
    "contact",
    "department",
    "details",
    "engineer",
    "engineers",
    "engineering",
    "guide",
    "history",
    "occupation",
    "occupations",
    "overview",
    "page",
    "pages",
    "profile",
    "profiles",
    "publication",
    "publications",
    "record",
    "records",
    "result",
    "results",
    "research",
    "repository",
    "repositories",
    "science",
    "sciences",
    "source",
    "sources",
    "specialization",
    "specializations",
    "tech",
    "technology",
    "university",
}


def _dedupe(items: List[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for item in items:
        normalized = item.strip().rstrip(".,)")
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def normalize_person_candidate(value: str) -> str | None:
    normalized = " ".join(value.strip(" \t\r\n:;,.!?-").split())
    if not normalized:
        return None

    words = normalized.split()
    collected: List[str] = []
    for word in words:
        cleaned_word = word.strip("()[]{}\"'.,;:!?")
        if not cleaned_word:
            continue
        lower_word = cleaned_word.lower()
        if lower_word in PERSON_CANDIDATE_STOPWORDS:
            if collected:
                break
            continue
        if lower_word in PERSON_CANDIDATE_BREAKWORDS:
            if collected:
                break
            continue
        if not re.fullmatch(r"[A-Za-z][A-Za-z'-]*", cleaned_word):
            if collected:
                break
            continue
        collected.append(cleaned_word)
        if len(collected) >= 4:
            break

    if not collected:
        return None

    if len(collected) < 2:
        return None

    if all(word.islower() for word in collected):
        collected = [word.capitalize() for word in collected]

    candidate = " ".join(collected)
    if not _is_valid_person_candidate(candidate):
        return None
    return candidate


def _extract_parenthetical_alias_variants(text: str) -> List[str]:
    candidates: List[str] = []
    for first, alias, last in PARENTHETICAL_ALIAS_NAME_REGEX.findall(text or ""):
        variants = (
            f"{first} {last}",
            f"{alias} {last}",
            f"{first} {alias} {last}",
        )
        for item in variants:
            normalized = normalize_person_candidate(item)
            if normalized:
                candidates.append(normalized)
    return _dedupe(candidates)


def _is_valid_person_candidate(value: str) -> bool:
    normalized = " ".join(value.strip().split())
    if not normalized:
        return False

    lowered = normalized.casefold()
    if any(phrase in lowered for phrase in PERSON_CANDIDATE_REJECT_PHRASES):
        return False

    words = normalized.split()
    if len(words) < 2 or len(words) > 4:
        return False

    reject_hits = sum(1 for word in words if word.casefold() in PERSON_CANDIDATE_REJECT_TOKENS)
    if reject_hits:
        return False

    generic_hits = sum(
        1
        for word in words
        if word.casefold() in PERSON_CANDIDATE_REJECT_TOKENS or word.casefold() in PERSON_CANDIDATE_REJECT_SUFFIXES
    )
    if generic_hits >= 2:
        return False

    if words[-1].casefold() in PERSON_CANDIDATE_REJECT_SUFFIXES:
        return False

    if any(len(word) == 1 for word in words):
        return False

    return True


def extract_person_targets(text: str) -> List[str]:
    scrubbed = text or ""
    scrubbed = URL_REGEX.sub(" ", scrubbed)
    scrubbed = EMAIL_REGEX.sub(" ", scrubbed)
    scrubbed = DOMAIN_REGEX.sub(" ", scrubbed)
    scrubbed = USERNAME_REGEX.sub(" ", scrubbed)

    candidates: List[str] = _extract_parenthetical_alias_variants(scrubbed)
    for match in PERSON_HINT_REGEX.findall(scrubbed):
        normalized = normalize_person_candidate(match)
        if normalized:
            candidates.append(normalized)

    for match in CAPITALIZED_NAME_REGEX.findall(scrubbed):
        normalized = normalize_person_candidate(match)
        if normalized:
            candidates.append(normalized)

    # Only treat the full string as a candidate when the source text is already short.
    # Longer prompt-style strings ("Map the public profile of ...") are better handled
    # by the targeted regex passes above.
    if len(scrubbed.split()) <= 8:
        direct = normalize_person_candidate(scrubbed)
        if direct:
            candidates.append(direct)

    return _dedupe(candidates)


def sanitize_search_tool_arguments(
    tool_name: str,
    arguments: Dict[str, Any],
    fallback_person_targets: List[str] | None = None,
) -> Dict[str, Any]:
    normalized = dict(arguments)
    fallback = next((item for item in (fallback_person_targets or []) if isinstance(item, str) and item.strip()), None)

    def normalized_text(value: Any) -> str:
        return " ".join(str(value or "").split()).strip()

    def looks_like_natural_language_query(value: str) -> bool:
        text = normalized_text(value)
        if not text:
            return False
        lowered = text.casefold()
        if text.startswith(("http://", "https://")):
            return False
        if len(text.split()) >= 6:
            return True
        return lowered.startswith(
            (
                "find ",
                "search ",
                "who is ",
                "look up ",
                "look for ",
                "research ",
                "identify ",
                "discover ",
                "check ",
                "show ",
            )
        )

    def tavily_research_query(target: str) -> str:
        cleaned = normalized_text(target)
        if not cleaned:
            return (
                "Find public information about the target, including biography, affiliations, publications, "
                "employment history, and online presence."
            )
        return (
            f"Find public information about {cleaned}, including biography, affiliations, publications, "
            "employment history, and online presence."
        )

    def tavily_person_query(target: str) -> str:
        cleaned = normalized_text(target)
        if not cleaned:
            return "Find public profiles, biographies, affiliations, and contact-relevant web results for the target."
        return f"Find public profiles, biographies, affiliations, and contact-relevant web results for {cleaned}."

    def coerce_target(*keys: str) -> str | None:
        for key in keys:
            value = normalized.get(key)
            if isinstance(value, str) and value.strip():
                candidates = extract_person_targets(value)
                if candidates:
                    return candidates[0]
        return fallback

    if tool_name == "person_search":
        target = coerce_target("name", "query")
        if target:
            normalized["name"] = target
            if "query" in normalized:
                normalized["query"] = target

    if tool_name == "google_serp_person_search":
        target = coerce_target("target_name", "query")
        if target:
            normalized["target_name"] = target
            if "query" in normalized:
                normalized["query"] = target

    if tool_name == "tavily_research":
        explicit_input = normalized_text(normalized.get("input"))
        target = coerce_target("input", "query", "target_name", "name")
        if target:
            normalized["input"] = tavily_research_query(target)
        elif explicit_input and not looks_like_natural_language_query(explicit_input):
            normalized["input"] = tavily_research_query(explicit_input)

    if tool_name == "tavily_person_search":
        target = coerce_target("target_name", "query", "name")
        if target:
            normalized["target_name"] = target
            explicit_query = normalized.get("query")
            if isinstance(explicit_query, str) and explicit_query.strip():
                normalized["query"] = (
                    explicit_query.strip()
                    if looks_like_natural_language_query(explicit_query)
                    else tavily_person_query(target)
                )
            else:
                normalized["query"] = tavily_person_query(target)
            if "name" in normalized:
                normalized["name"] = target

    return normalized
