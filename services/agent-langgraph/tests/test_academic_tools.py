from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
ACADEMIC_ROOT = REPO_ROOT / "apps" / "mcp-server" / "src" / "tools" / "tools_python"
if str(ACADEMIC_ROOT) not in sys.path:
    sys.path.insert(0, str(ACADEMIC_ROOT))
AGENT_SRC = REPO_ROOT / "services" / "agent-langgraph" / "src"
if str(AGENT_SRC) not in sys.path:
    sys.path.insert(0, str(AGENT_SRC))

from academic.common import validate_result_shape  # noqa: E402
from academic.common import coerce_multi_string  # noqa: E402
from academic.dblp_author_search import run as run_dblp_search  # noqa: E402
from academic.dblp_author_search import _extract_affiliations_from_info  # noqa: E402
from academic.grant_search_person import run as run_grant_search  # noqa: E402
from academic.grant_search_person import _person_name_matches  # noqa: E402
from academic.orcid_search import run as run_orcid_search  # noqa: E402
from academic.patent_search_person import run as run_patent_search  # noqa: E402
from academic.pubmed_author_search import run as run_pubmed_search  # noqa: E402
from academic.semantic_scholar_search import run as run_semantic_search  # noqa: E402


def test_orcid_search_flow(monkeypatch) -> None:
    def fake_request(*args, **kwargs):
        return {
            "expanded-result": [
                {
                    "orcid-id": "0000-0001",
                    "given-names": "Ada",
                    "family-names": "Lovelace",
                    "institution-name": "Analytical Engine Institute",
                    "keywords": "mathematics",
                    "works-count": 3,
                }
            ]
        }

    monkeypatch.setattr("academic.orcid_search.http_json_request", fake_request)
    result = run_orcid_search({"person_name": "Ada Lovelace", "max_results": 5})
    validate_result_shape(result)
    assert result["candidates"][0]["external_ids"]["orcid"] == "0000-0001"


def test_semantic_scholar_search_flow(monkeypatch) -> None:
    monkeypatch.setattr(
        "academic.semantic_scholar_search.http_json_request",
        lambda *args, **kwargs: {
            "data": [
                {
                    "authorId": "123",
                    "name": "Ada Lovelace",
                    "affiliations": ["Analytical Engine Institute"],
                    "paperCount": 10,
                    "citationCount": 100,
                    "hIndex": 5,
                    "externalIds": {"ORCID": "0000-0001"},
                    "url": "https://www.semanticscholar.org/author/123",
                }
            ]
        },
    )
    result = run_semantic_search({"person_name": "Ada Lovelace"})
    assert result["candidates"][0]["source_id"] == "123"


def test_dblp_author_search_flow(monkeypatch) -> None:
    monkeypatch.setattr(
        "academic.dblp_author_search.http_json_request",
        lambda *args, **kwargs: {
            "result": {
                "hits": {
                    "hit": [
                        {
                            "info": {
                                "author": "Ada Lovelace",
                                "url": "https://dblp.org/pid/01/1234",
                                "@pid": "01/1234",
                            }
                        }
                    ]
                }
            }
        },
    )
    result = run_dblp_search({"person_name": "Ada Lovelace"})
    assert result["candidates"][0]["external_ids"]["dblp_pid"] == "01/1234"


def test_pubmed_author_search_flow(monkeypatch) -> None:
    responses = [
        {"esearchresult": {"idlist": ["1", "2"]}},
        {
            "result": {
                "1": {"title": "Paper One", "fulljournalname": "Nature", "pubdate": "2024 Jan"},
                "2": {"title": "Paper Two", "fulljournalname": "Cell", "pubdate": "2023 Feb"},
            }
        },
    ]

    monkeypatch.setattr("academic.pubmed_author_search.http_json_request", lambda *args, **kwargs: responses.pop(0))
    result = run_pubmed_search({"person_name": "Ada Lovelace", "affiliations": ["Nature Institute"]})
    assert result["candidates"][0]["works_summary"]["pubmed_count"] == 2


def test_grant_search_person_flow(monkeypatch) -> None:
    responses = [
        {
            "results": [
                {
                    "core_project_num": "R01-1",
                    "appl_id": "111",
                    "project_title": "Biomedical Engines",
                    "contact_pi_name": "Ada Lovelace",
                    "organization": {"org_name": "NIH Lab"},
                    "fiscal_year": 2025,
                    "award_amount": 1000,
                }
            ]
        },
        {
            "response": {
                "award": [
                    {
                        "id": "NSF-1",
                        "title": "Analytical Research",
                        "piFirstName": "Ada",
                        "piLastName": "Lovelace",
                        "awardeeName": "NSF Lab",
                        "date": "2024",
                        "fundsObligatedAmt": 500,
                    }
                ]
            }
        },
    ]
    monkeypatch.setattr("academic.grant_search_person.http_json_request", lambda *args, **kwargs: responses.pop(0))
    result = run_grant_search({"person_name": "Ada Lovelace"})
    assert len(result["records"]) == 2


def test_patent_search_person_flow(monkeypatch) -> None:
    monkeypatch.setenv("PATENTSVIEW_API_KEY", "test-key")
    monkeypatch.setattr(
        "academic.patent_search_person.http_json_request",
        lambda *args, **kwargs: {
            "patents": [
                {
                    "patent_number": "1234567",
                    "patent_title": "Computing Machine",
                    "patent_date": "2020-01-01",
                }
            ]
        },
    )
    result = run_patent_search({"person_name": "Ada Lovelace"})
    assert result["records"][0]["patent_id"] == "1234567"


def test_coerce_multi_string_handles_list_like_strings() -> None:
    assert coerce_multi_string("['University of Toronto', 'Google DeepMind']") == [
        "University of Toronto",
        "Google DeepMind",
    ]


def test_nsf_person_name_filter_rejects_geoffrey_fox() -> None:
    assert _person_name_matches("Geoffrey Hinton", "Geoffrey Hinton")
    assert not _person_name_matches("Geoffrey Hinton", "Geoffrey Fox")


def test_extract_affiliations_from_dblp_notes() -> None:
    affiliations = _extract_affiliations_from_info(
        {
            "notes": {
                "note": [
                    {"@type": "affiliation", "text": "Google DeepMind, London, UK"},
                    {"@type": "award", "text": "Turing Award"},
                    {"@type": "affiliation", "text": "University of Toronto, Department of Computer Science, ON, Canada"},
                ]
            }
        }
    )
    assert affiliations == [
        "Google DeepMind, London, UK",
        "University of Toronto, Department of Computer Science, ON, Canada",
    ]
