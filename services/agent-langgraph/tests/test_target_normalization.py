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


def test_extract_person_targets_ignores_prompt_leadin_and_keeps_real_name() -> None:
    assert extract_person_targets(
        "Map the public profile of Frederick Xinyu Pi, including academic career, affiliations, collaborators, publications, code presence, and public online profiles."
    ) == [
        "Frederick Xinyu Pi"
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


def test_sanitize_search_tool_arguments_rewrites_bare_tavily_research_input_as_natural_language() -> None:
    normalized = sanitize_search_tool_arguments(
        "tavily_research",
        {
            "runId": "run-1",
            "input": "Frederick Pi",
        },
    )

    assert normalized["input"] == (
        "Find public information about Frederick Pi, including biography, affiliations, publications, "
        "employment history, and online presence."
    )


def test_sanitize_search_tool_arguments_rewrites_prompt_style_tavily_research_input_to_real_target() -> None:
    normalized = sanitize_search_tool_arguments(
        "tavily_research",
        {
            "runId": "run-1",
            "input": (
                "Map the public profile of Frederick Xinyu Pi, including academic career, affiliations, "
                "collaborators, publications, code presence, and public online profiles."
            ),
        },
    )

    assert normalized["input"] == (
        "Find public information about Frederick Xinyu Pi, including biography, affiliations, publications, "
        "employment history, and online presence."
    )


def test_sanitize_search_tool_arguments_rewrites_bare_tavily_person_query_as_natural_language() -> None:
    normalized = sanitize_search_tool_arguments(
        "tavily_person_search",
        {
            "runId": "run-1",
            "target_name": "Frederick Pi",
            "query": "Frederick Pi",
        },
    )

    assert normalized["target_name"] == "Frederick Pi"
    assert normalized["query"] == (
        "Find public profiles, biographies, affiliations, and contact-relevant web results for Frederick Pi."
    )
