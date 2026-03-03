from report_helpers import build_coverage_ledger, build_limits, coverage_is_complete, run_consistency_validator
from report_models import ClaimModel, EvidenceRefModel, SectionDraftModel


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
            text="Public profile handles were identified.",
            confidence=0.6,
            evidence_keys=["SOC_1"],
        ),
    ]
    evidence = [
        EvidenceRefModel(citation_key="ID_1", section_id="identity_profile", snippet="Profile confirms Ada Lovelace.", source_url="https://example.com/ada"),
        EvidenceRefModel(citation_key="BIO_1", section_id="biography_history", snippet="University study in 1830; joined company in 1835.", source_url="https://university.example/ada"),
        EvidenceRefModel(citation_key="AC_1", section_id="academic_research", snippet="Paper with coauthor listed in DBLP.", source_url="https://dblp.org/pid/ada"),
        EvidenceRefModel(citation_key="REL_1", section_id="relationships_and_associates", snippet="Collaborator Charles Babbage listed on lab page.", source_url="https://lab.example/team"),
        EvidenceRefModel(citation_key="SOC_1", section_id="social_accounts_and_interests", snippet="GitHub profile github.com/ada.", source_url="https://github.com/ada"),
    ]
    drafts = [SectionDraftModel(section_id=claim.section_id, title=claim.section_id, content=claim.text) for claim in claims]

    coverage = build_coverage_ledger("person", claims, evidence, drafts, [])

    assert coverage.identity_resolved.resolved is True
    assert coverage.affiliations_resolved.resolved is True
    assert coverage.education_resolved.resolved is True
    assert coverage.publications_resolved.resolved is True
    assert coverage.relationships_resolved.resolved is True
    assert coverage.handles_resolved.resolved is True
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
