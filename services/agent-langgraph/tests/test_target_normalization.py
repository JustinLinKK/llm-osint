from __future__ import annotations

from target_normalization import extract_person_targets, sanitize_search_tool_arguments


def test_extract_person_targets_rejects_generic_search_phrases() -> None:
    text = (
        "The search for 'Biomedical Engineering Her' identified 5 sources: "
        "a Wikipedia overview, Michigan Tech's department page, and an SWE article."
    )

    assert extract_person_targets(text) == []


def test_extract_person_targets_preserves_real_person_names() -> None:
    assert extract_person_targets("The search for 'Xinyu Frederick Pi' identified 5 sources") == [
        "Xinyu Frederick Pi"
    ]


def test_extract_person_targets_supports_parenthetical_alias_variants() -> None:
    assert extract_person_targets("Perform a comprehensive search for Xinyu (Frederick) Pi.") == [
        "Xinyu Pi",
        "Frederick Pi",
        "Xinyu Frederick Pi",
    ]


def test_extract_person_targets_rejects_synthetic_none_publications() -> None:
    assert extract_person_targets("None Publications") == []


def test_extract_person_targets_drops_leading_breakwords() -> None:
    assert extract_person_targets("Ran Tavily research for Xinyu Pi and found 5 results") == [
        "Xinyu Pi"
    ]


def test_sanitize_search_tool_arguments_uses_fallback_when_query_is_not_a_person() -> None:
    normalized = sanitize_search_tool_arguments(
        "tavily_person_search",
        {
            "runId": "run-1",
            "query": "Find the public GitHub profile or account for Biomedical Engineering Her.",
        },
        fallback_person_targets=["Ada Lovelace"],
    )

    assert normalized["target_name"] == "Ada Lovelace"
    assert normalized["query"] == "Find the public GitHub profile or account for Biomedical Engineering Her."
