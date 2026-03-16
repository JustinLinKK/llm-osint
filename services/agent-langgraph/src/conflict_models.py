from __future__ import annotations

from typing import Any, Dict, List, Literal

from pydantic import BaseModel, Field


class ConflictEvidenceRefModel(BaseModel):
    evidence_id: str = ""
    case_id: str = ""
    candidate_value: str = ""
    polarity: Literal["supporting", "contradicting", "neutral"] = "supporting"
    document_id: str | None = None
    chunk_id: str | None = None
    object_ref: Dict[str, Any] = Field(default_factory=dict)
    source_url: str | None = None
    source_domain: str | None = None
    snippet: str = ""
    source_tool: str = ""
    retrieved_at: str | None = None
    score: float = 0.0


class ConflictCandidateValueModel(BaseModel):
    value: str
    normalized_value: str
    score: float = 0.0
    source_count: int = 0
    direct_source_count: int = 0
    official_source_count: int = 0
    source_tools: List[str] = Field(default_factory=list)
    source_domains: List[str] = Field(default_factory=list)
    receipt_keys: List[str] = Field(default_factory=list)
    evidence_ids: List[str] = Field(default_factory=list)


class GraphConflictCaseModel(BaseModel):
    case_id: str
    run_id: str = ""
    target_type: Literal["entity", "relation", "synthetic"] = "entity"
    target_id: str = ""
    field_name: str
    relation_type: str = ""
    scope: Literal["primary", "first_hop", "secondary"] = "primary"
    status: Literal["detected", "resolved", "unresolved", "applied"] = "detected"
    blocking: bool = True
    chosen_value: str | None = None
    deterministic_winner: str | None = None
    confidence: float = 0.0
    rationale: str = ""
    candidate_values: List[ConflictCandidateValueModel] = Field(default_factory=list)
    evidence: List[ConflictEvidenceRefModel] = Field(default_factory=list)
    source_tools: List[str] = Field(default_factory=list)
    source_domains: List[str] = Field(default_factory=list)
    graph_entity_ids: List[str] = Field(default_factory=list)
    graph_relation_ids: List[str] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)

