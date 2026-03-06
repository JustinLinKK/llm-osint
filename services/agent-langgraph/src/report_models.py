from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, TypedDict

from pydantic import BaseModel, Field

from tool_worker_graph import ToolReceipt


class SectionTaskModel(BaseModel):
    section_id: str
    title: str
    objective: str
    required: bool = True
    section_group: str = ""
    graph_chain: List[str] = Field(default_factory=list)
    entity_ids: List[str] = Field(default_factory=list)
    query_hints: List[str] = Field(default_factory=list)
    current_content: str = ""
    revision_focus: str = ""
    next_step_suggestion: str = ""


class SectionReflectionModel(BaseModel):
    section_id: str
    status: Literal["ok", "needs_revision", "missing"] = "ok"
    critique: str = ""
    current_content: str = ""
    next_step_suggestion: str = ""
    query_hints: List[str] = Field(default_factory=list)


class EvidenceRefModel(BaseModel):
    citation_key: str
    section_id: str
    document_id: str | None = None
    snippet: str
    source_url: str | None = None
    source_url_unavailable_reason: str | None = None
    title: str | None = None
    domain: str | None = None
    retrieved_at: str | None = None
    content_hash: str | None = None
    relevance_score: float | None = None
    evidence_object_key: str | None = None
    source_type: str = "web"
    score: float | None = None
    db_source: str = "vector"
    object_ref: Dict[str, Any] = Field(default_factory=dict)
    graph_ref: Dict[str, Any] = Field(default_factory=dict)


class ClaimModel(BaseModel):
    claim_id: str
    section_id: str
    text: str
    subject_entity_id: str | None = None
    predicate: str = "observed"
    object: str | None = None
    confidence: float = 0.0
    impact: str = "medium"
    evidence_keys: List[str] = Field(default_factory=list)
    conflict_flags: List[str] = Field(default_factory=list)
    source_url: str | None = None
    source_type: str | None = None
    retrieved_at: str | None = None
    quote_span: str | None = None


class SectionDraftModel(BaseModel):
    section_id: str
    title: str
    content: str
    citation_keys: List[str] = Field(default_factory=list)


class EntityModel(BaseModel):
    entity_id: str
    entity_type: Literal["Person", "Organization", "Paper", "WebProfile", "EmailPattern", "Handle", "Location"] = "Person"
    name: str
    aliases: List[str] = Field(default_factory=list)
    attributes: Dict[str, Any] = Field(default_factory=dict)
    confidence: float = 0.0


class CoverageItemModel(BaseModel):
    resolved: bool = False
    confidence: float = 0.0
    notes: List[str] = Field(default_factory=list)


class CoverageLedgerModel(BaseModel):
    identity_resolved: CoverageItemModel = Field(default_factory=CoverageItemModel)
    aliases_resolved: CoverageItemModel = Field(default_factory=CoverageItemModel)
    affiliations_resolved: CoverageItemModel = Field(default_factory=CoverageItemModel)
    education_resolved: CoverageItemModel = Field(default_factory=CoverageItemModel)
    publications_resolved: CoverageItemModel = Field(default_factory=CoverageItemModel)
    relationships_resolved: CoverageItemModel = Field(default_factory=CoverageItemModel)
    contacts_resolved: CoverageItemModel = Field(default_factory=CoverageItemModel)
    handles_resolved: CoverageItemModel = Field(default_factory=CoverageItemModel)
    code_presence_resolved: CoverageItemModel = Field(default_factory=CoverageItemModel)
    business_roles_resolved: CoverageItemModel = Field(default_factory=CoverageItemModel)
    archived_history_resolved: CoverageItemModel = Field(default_factory=CoverageItemModel)
    timeline_resolved: CoverageItemModel = Field(default_factory=CoverageItemModel)
    limits_explained: CoverageItemModel = Field(default_factory=CoverageItemModel)


class ConsistencyIssueModel(BaseModel):
    issue_id: str
    severity: Literal["low", "medium", "high"] = "medium"
    description: str
    conflicting_sections: List[str] = Field(default_factory=list)
    targeted_queries: List[str] = Field(default_factory=list)


class DisambiguationEvidenceModel(BaseModel):
    evidence_type: str
    value: str
    strength: Literal["weak", "moderate", "strong"] = "moderate"
    source_url: str | None = None


class CanonicalIdentityModel(BaseModel):
    canonical_name: str = ""
    aliases: List[str] = Field(default_factory=list)
    justification: List[str] = Field(default_factory=list)
    low_social_footprint: bool = False
    high_academic_footprint: bool = False


class AttemptLogEntryModel(BaseModel):
    tool_name: str
    query: str
    top_sources: List[str] = Field(default_factory=list)
    outcome: str = ""
    source_type: str = "web"


class NotFoundReasonModel(BaseModel):
    category: Literal["private", "ambiguous_identity", "not_searched", "not_publicly_found", "auth_blocked"] = "not_publicly_found"
    detail: str
    related_query: str | None = None


class TimelineEventModel(BaseModel):
    date_label: str
    event: str
    citation_keys: List[str] = Field(default_factory=list)
    source_url: str | None = None


class PublicationInventoryItemModel(BaseModel):
    title: str
    year: str | None = None
    venue: str | None = None
    coauthors: List[str] = Field(default_factory=list)
    links: List[str] = Field(default_factory=list)


class ThesisInventoryItemModel(BaseModel):
    title: str
    pdf_url: str | None = None
    advisor: str | None = None
    committee: List[str] = Field(default_factory=list)
    abstract_keywords: List[str] = Field(default_factory=list)


class ResearchThemeModel(BaseModel):
    theme: str
    evidence_count: int = 0


class CollaborationGraphNodeModel(BaseModel):
    node_id: str
    node_type: Literal["Person", "Institution", "Paper", "Venue"] = "Person"
    label: str


class CollaborationGraphEdgeModel(BaseModel):
    src: str
    rel: Literal["COAUTHOR_OF", "ADVISED_BY", "AFFILIATED_WITH", "MEMBER_OF_LAB", "PUBLISHED_IN"] = "COAUTHOR_OF"
    dst: str
    evidence_count: int = 0


class CoauthorClusterModel(BaseModel):
    label: str
    members: List[str] = Field(default_factory=list)
    representative_works: List[str] = Field(default_factory=list)


class ProfileIndexItemModel(BaseModel):
    platform: str
    url: str
    last_active: str | None = None
    title: str | None = None
    affiliation: str | None = None
    projects: List[str] = Field(default_factory=list)
    pinned_items: List[str] = Field(default_factory=list)


class DocDeepDiveModel(BaseModel):
    source_url: str
    affiliations: List[str] = Field(default_factory=list)
    advisors: List[str] = Field(default_factory=list)
    methods_keywords: List[str] = Field(default_factory=list)
    acknowledgements: List[str] = Field(default_factory=list)


class ReportMemoryModel(BaseModel):
    question: str
    entities: List[EntityModel] = Field(default_factory=list)
    claims: List[ClaimModel] = Field(default_factory=list)
    evidence: List[EvidenceRefModel] = Field(default_factory=list)
    coverage: CoverageLedgerModel = Field(default_factory=CoverageLedgerModel)
    consistency_issues: List[ConsistencyIssueModel] = Field(default_factory=list)
    open_questions: List[str] = Field(default_factory=list)
    limits: List[str] = Field(default_factory=list)
    latest_observation: str = ""
    step_count: int = 0
    canonical_identity: CanonicalIdentityModel = Field(default_factory=CanonicalIdentityModel)
    disambiguation_evidence: List[DisambiguationEvidenceModel] = Field(default_factory=list)
    attempt_log: List[AttemptLogEntryModel] = Field(default_factory=list)
    not_found_reasons: List[NotFoundReasonModel] = Field(default_factory=list)
    timeline: List[TimelineEventModel] = Field(default_factory=list)
    publication_inventory: List[PublicationInventoryItemModel] = Field(default_factory=list)
    thesis_inventory: List[ThesisInventoryItemModel] = Field(default_factory=list)
    research_themes: List[ResearchThemeModel] = Field(default_factory=list)
    collaboration_graph_nodes: List[CollaborationGraphNodeModel] = Field(default_factory=list)
    collaboration_graph_edges: List[CollaborationGraphEdgeModel] = Field(default_factory=list)
    coauthor_clusters: List[CoauthorClusterModel] = Field(default_factory=list)
    profile_index: List[ProfileIndexItemModel] = Field(default_factory=list)
    doc_deep_dives: List[DocDeepDiveModel] = Field(default_factory=list)


class ReportState(TypedDict):
    run_id: str
    prompt: str
    noteboard: List[str]
    stage1_receipts: List[ToolReceipt]
    report_type: str
    primary_entities: List[str]
    outline: List[SectionTaskModel]
    section_tasks: List[SectionTaskModel]
    pending_section_tasks: List[SectionTaskModel]
    active_task: SectionTaskModel | None
    query_hints: List[str]
    section_hits: List[Dict[str, Any]]
    section_evidence_buffer: List[EvidenceRefModel]
    section_claims_buffer: List[ClaimModel]
    section_issues_buffer: List[str]
    section_drafts: List[SectionDraftModel]
    claim_ledger: List[ClaimModel]
    evidence_refs: List[EvidenceRefModel]
    section_issues: List[str]
    section_reflections: List[SectionReflectionModel]
    missing_section_ids: List[str]
    refine_round: int
    max_refine_rounds: int
    quality_ok: bool
    done: bool
    final_report: str
    evidence_appendix: str
    report_memory: ReportMemoryModel
    consistency_issues: List[ConsistencyIssueModel]
    contradiction_query_hints: List[str]


@dataclass
class ReportResult:
    run_id: str
    report_type: str
    final_report: str
    evidence_appendix: str
    section_drafts: List[SectionDraftModel]
    claim_ledger: List[ClaimModel]
    evidence_refs: List[EvidenceRefModel]
    quality_ok: bool
    refine_round: int
    report_memory: ReportMemoryModel


def make_initial_report_state(
    run_id: str,
    prompt: str,
    noteboard: List[str],
    stage1_receipts: List[ToolReceipt],
    max_refine_rounds: int,
) -> ReportState:
    """Create the canonical initial state used by the report subgraph."""
    return {
        "run_id": run_id,
        "prompt": prompt,
        "noteboard": list(noteboard),
        "stage1_receipts": list(stage1_receipts),
        "report_type": "person",
        "primary_entities": [],
        "outline": [],
        "section_tasks": [],
        "pending_section_tasks": [],
        "active_task": None,
        "query_hints": [],
        "section_hits": [],
        "section_evidence_buffer": [],
        "section_claims_buffer": [],
        "section_issues_buffer": [],
        "section_drafts": [],
        "claim_ledger": [],
        "evidence_refs": [],
        "section_issues": [],
        "section_reflections": [],
        "missing_section_ids": [],
        "refine_round": 0,
        "max_refine_rounds": max_refine_rounds,
        "quality_ok": False,
        "done": False,
        "final_report": "",
        "evidence_appendix": "",
        "report_memory": ReportMemoryModel(question=prompt),
        "consistency_issues": [],
        "contradiction_query_hints": [],
    }
