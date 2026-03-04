from report_helpers import (
    assemble_final_report,
    build_coverage_ledger,
    build_limits,
    build_report_memory,
    coverage_is_complete,
    pack_evidence,
    run_consistency_validator,
)
from report_models import ClaimModel, EvidenceRefModel, SectionDraftModel
from tool_worker_graph import ToolReceipt


def test_coverage_ledger_marks_complete_when_baselines_exist() -> None:
    claims = [
        ClaimModel(
            claim_id="c1",
            section_id="identity_profile",
            text="Ada Lovelace is identified by name in profile evidence.",
            confidence=0.9,
            evidence_keys=["ID_1"],
        ),
        ClaimModel(
            claim_id="c2",
            section_id="biography_history",
            text="Ada Lovelace studied mathematics in 1830 and worked with Charles Babbage.",
            confidence=0.8,
            evidence_keys=["BIO_1"],
        ),
        ClaimModel(
            claim_id="c3",
            section_id="academic_research",
            text="Publication and coauthor evidence exists via scholarly sources.",
            confidence=0.7,
            evidence_keys=["AC_1"],
        ),
        ClaimModel(
            claim_id="c4",
            section_id="relationships_and_associates",
            text="Charles Babbage is a collaborator.",
            confidence=0.7,
            evidence_keys=["REL_1"],
        ),
        ClaimModel(
            claim_id="c5",
            section_id="social_accounts_and_interests",
            text="Public profile handle username ada was identified.",
            confidence=0.6,
            evidence_keys=["SOC_1"],
        ),
        ClaimModel(
            claim_id="c6",
            section_id="public_contact_methods",
            text="A public contact email ada@example.com is listed on the profile.",
            confidence=0.7,
            evidence_keys=["CON_1"],
        ),
    ]
    evidence = [
        EvidenceRefModel(citation_key="ID_1", section_id="identity_profile", snippet="Profile confirms Ada Lovelace.", source_url="https://example.com/ada"),
        EvidenceRefModel(citation_key="BIO_1", section_id="biography_history", snippet="University study in 1830; joined company in 1835.", source_url="https://university.example/ada"),
        EvidenceRefModel(citation_key="AC_1", section_id="academic_research", snippet="Paper with coauthor listed in DBLP.", source_url="https://dblp.org/pid/ada"),
        EvidenceRefModel(citation_key="REL_1", section_id="relationships_and_associates", snippet="Collaborator Charles Babbage listed on lab page.", source_url="https://lab.example/team"),
        EvidenceRefModel(citation_key="SOC_1", section_id="social_accounts_and_interests", snippet="GitHub profile github.com/ada.", source_url="https://github.com/ada"),
        EvidenceRefModel(citation_key="CON_1", section_id="public_contact_methods", snippet="Contact: ada@example.com", source_url="https://example.com/contact"),
    ]
    drafts = [SectionDraftModel(section_id=claim.section_id, title=claim.section_id, content=claim.text) for claim in claims]

    coverage = build_coverage_ledger("person", claims, evidence, drafts, [])

    assert coverage.identity_resolved.resolved is True
    assert coverage.affiliations_resolved.resolved is True
    assert coverage.education_resolved.resolved is True
    assert coverage.publications_resolved.resolved is True
    assert coverage.relationships_resolved.resolved is True
    assert coverage.handles_resolved.resolved is True
    assert coverage.contacts_resolved.resolved is True
    assert coverage.timeline_resolved.resolved is True
    assert coverage_is_complete(coverage) is True


def test_consistency_validator_detects_publication_conflict() -> None:
    drafts = [
        SectionDraftModel(
            section_id="academic_research",
            title="Academic / Research",
            content="No publications found during this pass.",
        )
    ]
    claims = [
        ClaimModel(
            claim_id="c1",
            section_id="academic_research",
            text="No publications found.",
            confidence=0.4,
            evidence_keys=["AC_1"],
        )
    ]
    evidence = [
        EvidenceRefModel(
            citation_key="AC_1",
            section_id="academic_research",
            snippet="Coauthor cluster and paper title listed in Semantic Scholar.",
            source_url="https://www.semanticscholar.org/author/123",
        )
    ]

    issues = run_consistency_validator(drafts, claims, evidence)
    limits = build_limits([], build_coverage_ledger("person", claims, evidence, drafts, []), issues)

    assert any(issue.issue_id == "publication_conflict" for issue in issues)
    assert any("targeted follow-up" in item for item in limits)


def test_coverage_ledger_does_not_mark_complete_from_keyword_only_evidence() -> None:
    evidence = [
        EvidenceRefModel(
            citation_key="E1",
            section_id="biography_history",
            snippet="University of Somewhere and publication keywords appear in an unverified snippet.",
        )
    ]
    drafts = [
        SectionDraftModel(
            section_id="biography_history",
            title="Biography and history",
            content="Unverified draft text mentions a university and publications.",
        )
    ]

    coverage = build_coverage_ledger("person", [], evidence, drafts, [])

    assert coverage.affiliations_resolved.resolved is False
    assert coverage.education_resolved.resolved is False
    assert coverage.publications_resolved.resolved is False
    assert coverage.timeline_resolved.resolved is False


def test_build_report_memory_recovers_profile_and_publication_inventory_from_receipts() -> None:
    receipts = [
        ToolReceipt(
            run_id="run-1",
            tool_name="github_identity_search",
            arguments={},
            argument_signature="sig-1",
            ok=True,
            summary="Resolved GitHub profile.",
            key_facts=[
                {"profileUrl": "https://github.com/FrederickPi"},
                {"repositories": ["cs225-potd"]},
                {"publications": [{"title": "Reasoning Like Program Executors", "year": 2024, "url": "https://arxiv.org/abs/2402.04333"}]},
            ],
            artifact_ids=[],
            document_ids=[],
        ),
        ToolReceipt(
            run_id="run-1",
            tool_name="semantic_scholar_search",
            arguments={},
            argument_signature="sig-2",
            ok=True,
            summary="Resolved academic candidate.",
            key_facts=[
                {
                    "candidates": [
                        {
                            "canonical_name": "Xinyu Pi",
                            "evidence": [
                                {
                                    "title": "Bridging Human Interpretation and Machine Representation",
                                    "url": "https://www.semanticscholar.org/paper/abc",
                                    "year": 2025,
                                    "venue": "Semantic Scholar",
                                }
                            ],
                        }
                    ]
                }
            ],
            artifact_ids=[],
            document_ids=[],
        ),
    ]

    memory = build_report_memory(
        question="profile Xinyu Pi",
        report_type="person",
        primary_entities=["Xinyu Frederick Pi"],
        noteboard=[],
        stage1_receipts=receipts,
        claims=[],
        evidence=[],
        section_issues=[],
        section_drafts=[],
        latest_observation="",
    )

    assert any(item.url == "https://github.com/FrederickPi" for item in memory.profile_index)
    titles = {item.title for item in memory.publication_inventory}
    assert "Reasoning Like Program Executors" in titles
    assert "Bridging Human Interpretation and Machine Representation" in titles


def test_assemble_final_report_accepts_long_form_llm_output() -> None:
    class _FakeLLM:
        def complete_json(self, prompt: str, payload: dict, temperature: float, timeout: int) -> dict:
            return {
                "report_text": "# Xinyu Pi\n\n## Identity\nA long-form narrative report with citations [ID_1].\n\n## Research\nAdditional detail.\n"
                * 8
            }

    state = {
        "prompt": "profile Xinyu Pi",
        "report_type": "person",
        "primary_entities": ["Xinyu Pi"],
        "section_drafts": [SectionDraftModel(section_id="identity_profile", title="Identity", content="Identity draft [ID_1]")],
        "claim_ledger": [],
        "evidence_refs": [],
        "section_issues": [],
        "stage1_receipts": [],
        "report_memory": None,
    }

    report = assemble_final_report(state, _FakeLLM())

    assert report.startswith("# Xinyu Pi")


def test_assemble_final_report_rejects_legacy_ledger_format() -> None:
    class _LegacyLLM:
        def complete_json(self, prompt: str, payload: dict, temperature: float, timeout: int, **kwargs: object) -> dict:
            return {
                "report_text": "Findings\n- item\n\nCanonical Identity\n- item\n\nCoverage Ledger\n- item\n\nEvidence Index\n1. item"
            }

    state = {
        "prompt": "profile Xinyu Pi",
        "report_type": "person",
        "primary_entities": ["Xinyu Pi"],
        "section_drafts": [
            SectionDraftModel(
                section_id="identity_profile",
                title="Core Identity and Professional Branding",
                content="Xinyu Pi appears publicly as both Xinyu Pi and Frederick Pi [ID_1].",
            )
        ],
        "claim_ledger": [],
        "evidence_refs": [],
        "section_issues": ["identity evidence remains partially ambiguous"],
        "stage1_receipts": [],
        "noteboard": [],
        "report_memory": None,
    }

    report = assemble_final_report(state, _LegacyLLM())

    assert report.startswith("Qwen Deep Research")
    assert "## Core Identity and Professional Branding" in report
    assert "Coverage Ledger" not in report
    assert "Evidence Index" not in report


def test_pack_evidence_accepts_graph_backed_rows_and_rejects_unbacked_rows() -> None:
    rows = [
        {
            "graph_entity_id": "ent_123",
            "graph_ref": {"entityId": "ent_123", "labels": ["Entity"]},
            "snippet": "Graph entity Ada Lovelace. type=Person. attributes=mathematician.",
            "title": "Ada Lovelace",
            "score": 0.9,
            "db_source": "graph",
        },
        {
            "snippet": "This row was never retrieved from a database and should be ignored.",
            "score": 1.0,
        },
    ]

    packed = pack_evidence("identity_profile", rows, k=5)

    assert len(packed) == 1
    assert packed[0].db_source == "graph"
    assert packed[0].graph_ref["entityId"] == "ent_123"
