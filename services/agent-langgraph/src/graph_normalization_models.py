from __future__ import annotations

from typing import List, Literal

from pydantic import BaseModel, Field


GraphNormalizationActionType = Literal[
    "ensure_root_entity",
    "merge_entity_into",
    "ensure_relation",
    "suppress_entity",
    "suppress_relation",
    "keep_separate",
    "merge_into_root",
    "merge_into_entity",
    "suppress_noise",
]


class GraphNormalizationActionModel(BaseModel):
    action_id: str = ""
    action_type: GraphNormalizationActionType = "keep_separate"
    source_entity_id: str = ""
    target_entity_id: str = ""
    relation_id: str = ""
    src_entity_id: str = ""
    dst_entity_id: str = ""
    rel_type: str = ""
    canonical_name: str = ""
    entity_type: str = ""
    aliases: List[str] = Field(default_factory=list)
    rationale: str = ""
    confidence: float = 0.0
    deterministic: bool = True
    applyable: bool = False


class GraphNormalizationCaseModel(BaseModel):
    case_id: str
    case_type: Literal["root_resolution", "duplicate_cluster", "noise_cluster"] = "duplicate_cluster"
    status: Literal["detected", "suggested", "applied", "skipped"] = "detected"
    target_entity_id: str = ""
    candidate_entity_ids: List[str] = Field(default_factory=list)
    candidate_relation_ids: List[str] = Field(default_factory=list)
    summary: str = ""
    notes: List[str] = Field(default_factory=list)
    deterministic_actions: List[GraphNormalizationActionModel] = Field(default_factory=list)
    suggested_actions: List[GraphNormalizationActionModel] = Field(default_factory=list)
