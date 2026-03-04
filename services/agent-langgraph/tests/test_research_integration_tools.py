from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
RESEARCH_ROOT = REPO_ROOT / "apps" / "mcp-server" / "src" / "tools" / "tools_python" / "research_integration"
if str(RESEARCH_ROOT) not in sys.path:
    sys.path.insert(0, str(RESEARCH_ROOT))

from arxiv_paper_ingest import build_author_contacts, extract_emails, infer_topics  # noqa: E402


def test_extract_emails_expands_grouped_locals() -> None:
    text = "Contact: {ada,alan.turing}@research.edu and grace@lab.org"
    emails = extract_emails(text)

    assert emails == [
        "ada@research.edu",
        "alan.turing@research.edu",
        "grace@lab.org",
    ]


def test_build_author_contacts_matches_common_email_local_parts() -> None:
    contacts = build_author_contacts(
        ["Ada Lovelace", "Alan Turing", "Grace Hopper"],
        ["ada.lovelace@example.edu", "aturing@example.edu"],
    )

    assert contacts[0]["email"] == "ada.lovelace@example.edu"
    assert contacts[1]["email"] == "aturing@example.edu"
    assert contacts[2]["email"] is None


def test_infer_topics_preserves_categories_and_title_segments() -> None:
    topics = infer_topics(
        "Reasoning Like Program Executors: Symbolic Traces for LLMs",
        "This paper studies symbolic execution traces for large language models.",
        ["cs.AI", "cs.CL"],
        topic_hint="reasoning",
    )

    assert "reasoning" in topics
    assert "cs.AI" in topics
    assert "cs.CL" in topics
    assert any("Program Executors" in topic or "Symbolic Traces" in topic for topic in topics)
