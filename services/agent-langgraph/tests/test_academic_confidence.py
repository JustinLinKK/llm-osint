from __future__ import annotations

from matching.confidence import normalize_name, score_candidate_match


def test_normalize_name_handles_punctuation_and_accents() -> None:
    assert normalize_name("J. García-Lin") == "j garcia lin"


def test_score_candidate_match_is_deterministic() -> None:
    left = score_candidate_match(
        query_name="J. Lin",
        candidate_name="Jing Lin",
        query_affiliations=["Stanford University"],
        candidate_affiliations=["Stanford University"],
        query_topics=["machine learning"],
        candidate_topics=["machine learning", "natural language processing"],
        known_ids={"orcid": "0000-0001"},
        candidate_ids={"orcid": "0000-0001"},
    )
    right = score_candidate_match(
        query_name="J. Lin",
        candidate_name="Jing Lin",
        query_affiliations=["Stanford University"],
        candidate_affiliations=["Stanford University"],
        query_topics=["machine learning"],
        candidate_topics=["machine learning", "natural language processing"],
        known_ids={"orcid": "0000-0001"},
        candidate_ids={"orcid": "0000-0001"},
    )
    assert left == right
    assert left["confidence"] > 0.6


def test_score_candidate_match_handles_middle_initial_variants() -> None:
    scored = score_candidate_match(
        query_name="Geoffrey Hinton",
        candidate_name="Geoffrey E. Hinton",
        query_affiliations=[],
        candidate_affiliations=[],
        query_topics=[],
        candidate_topics=[],
        known_ids={},
        candidate_ids={},
    )
    assert scored["confidence"] >= 0.33
    assert "name match ignoring middle initials" in scored["match_features"]["reasons"]
