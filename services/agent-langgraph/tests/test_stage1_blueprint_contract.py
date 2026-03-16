from __future__ import annotations

import json
from pathlib import Path


def test_stage1_blueprint_contract_allows_award_grant_and_patent_entities() -> None:
    contract_path = Path(__file__).resolve().parents[3] / "schemas" / "stage1_graph_blueprint_contract.v1.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))

    entity_types = set(contract.get("entity_types") or [])
    relation_types = set(contract.get("relation_types") or [])

    assert {"Award", "Grant", "Patent"}.issubset(entity_types)
    assert {"RECEIVED_AWARD", "HAS_GRANT", "HAS_PATENT"}.issubset(relation_types)
