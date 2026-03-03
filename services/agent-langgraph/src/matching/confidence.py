from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, Iterable, List


PUNCTUATION_RE = re.compile(r"[^a-z0-9\s]")


def normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().strip()
    text = PUNCTUATION_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _initials(tokens: List[str]) -> str:
    return "".join(token[:1] for token in tokens if token)


def _simplify_name_tokens(tokens: List[str]) -> List[str]:
    if len(tokens) <= 2:
        return tokens
    simplified: List[str] = []
    for idx, token in enumerate(tokens):
        if 0 < idx < len(tokens) - 1 and len(token) == 1:
            continue
        simplified.append(token)
    return simplified


def _token_overlap(left: Iterable[str], right: Iterable[str]) -> float:
    left_set = {item for item in left if item}
    right_set = {item for item in right if item}
    if not left_set or not right_set:
        return 0.0
    intersection = left_set & right_set
    union = left_set | right_set
    return len(intersection) / len(union)


def _names_equivalent_ignoring_middle_initials(query_tokens: List[str], candidate_tokens: List[str]) -> bool:
    return _simplify_name_tokens(query_tokens) == _simplify_name_tokens(candidate_tokens)


def score_candidate_match(
    *,
    query_name: str,
    candidate_name: str,
    query_affiliations: Iterable[str],
    candidate_affiliations: Iterable[str],
    query_topics: Iterable[str],
    candidate_topics: Iterable[str],
    known_ids: Dict[str, Any] | None = None,
    candidate_ids: Dict[str, Any] | None = None,
    coauthor_overlap: float = 0.0,
    homepage_domain_match: bool = False,
) -> Dict[str, Any]:
    known_ids = known_ids or {}
    candidate_ids = candidate_ids or {}

    query_norm = normalize_name(query_name)
    candidate_norm = normalize_name(candidate_name)
    query_tokens = query_norm.split()
    candidate_tokens = candidate_norm.split()

    score_breakdown: Dict[str, float] = {
        "name_match": 0.0,
        "affiliation_overlap": 0.0,
        "id_consistency": 0.0,
        "coauthor_overlap": 0.0,
        "topic_overlap": 0.0,
    }
    reasons: List[str] = []

    if query_norm and candidate_norm and query_norm == candidate_norm:
        score_breakdown["name_match"] = 0.35
        reasons.append("exact normalized name match")
    elif query_tokens and candidate_tokens and _names_equivalent_ignoring_middle_initials(query_tokens, candidate_tokens):
        score_breakdown["name_match"] = 0.33
        reasons.append("name match ignoring middle initials")
    elif query_tokens and candidate_tokens and query_tokens[-1] == candidate_tokens[-1] and _initials(query_tokens[:-1]) == _initials(candidate_tokens[:-1]):
        score_breakdown["name_match"] = 0.25
        reasons.append("initials-compatible surname match")
    else:
        partial_overlap = _token_overlap(query_tokens, candidate_tokens)
        if partial_overlap >= 0.5:
            score_breakdown["name_match"] = round(0.2 * partial_overlap, 4)
            reasons.append("partial normalized name overlap")

    aff_overlap = _token_overlap(
        normalize_name(" ".join(query_affiliations)).split(),
        normalize_name(" ".join(candidate_affiliations)).split(),
    )
    if aff_overlap > 0:
        score_breakdown["affiliation_overlap"] = round(0.25 * min(1.0, aff_overlap * 1.5), 4)
        reasons.append("affiliation token overlap")

    id_consistency = 0.0
    for key, value in known_ids.items():
        if not value or key not in candidate_ids:
            continue
        if str(candidate_ids.get(key)).strip().lower() == str(value).strip().lower():
            id_consistency = 0.2
            break
    if id_consistency == 0.0 and homepage_domain_match:
        id_consistency = 0.12
    if id_consistency > 0:
        score_breakdown["id_consistency"] = id_consistency
        reasons.append("cross-source identifier consistency")

    if coauthor_overlap > 0:
        score_breakdown["coauthor_overlap"] = round(0.1 * min(1.0, coauthor_overlap), 4)
        reasons.append("coauthor overlap")

    topic_overlap = _token_overlap(
        normalize_name(" ".join(query_topics)).split(),
        normalize_name(" ".join(candidate_topics)).split(),
    )
    if topic_overlap > 0:
        score_breakdown["topic_overlap"] = round(0.1 * min(1.0, topic_overlap * 1.5), 4)
        reasons.append("topic overlap")

    confidence = round(min(1.0, sum(score_breakdown.values())), 4)
    return {
        "confidence": confidence,
        "match_features": {
            "reasons": reasons,
            "weights": {
                "exact_name": 0.35,
                "affiliation_overlap": 0.25,
                "external_id_overlap": 0.2,
                "coauthor_overlap": 0.1,
                "topic_overlap": 0.1,
            },
            "score_breakdown": score_breakdown,
        },
    }
