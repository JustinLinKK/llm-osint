# Stage 1 Blueprint Runtime Mapping

## Runtime Source Of Truth
- Runtime contract file: `schemas/stage1_graph_blueprint_contract.v1.json`
- Visual artifacts (`graph_blueprint_sample.json/png`) are design references only.
- Stage 1 planner reads the runtime contract and uses it for:
  - graph-slot coverage computation
  - planner hint generation
  - Stage 1 stop-gate enforcement (`balanced` by default)

## Blueprint Slot Mapping

| Contract Slot | Runtime Signal (labels/relations) |
|---|---|
| `primary_anchor_node` | resolved anchor IDs + `Person`/`Organization` focus labels |
| `identity_surface` | identity labels (`Person`, `ContactPoint`, `Domain`, etc.) or identity relations (`HAS_PROFILE`, `HAS_CONTACT`, `IDENTIFIED_AS`) |
| `related_identity_surface` | secondary-person presence plus related identity relations (`HAS_PROFILE`, `HAS_CONTACT_POINT`, `HAS_EMAIL`, `HAS_PHONE`, `HAS_HANDLE`, etc.) |
| `relationship_surface` | typed interpersonal/org relations (`COAUTHORED_WITH`, `COLLEAGUE_OF`, `WORKS_AT`, `OFFICER_OF`, etc.) |
| `timeline_surface` | timeline entities (`TimelineEvent`, dated context entities) or timeline relations (`HAS_TIMELINE_EVENT`, `FILED`, `APPEARS_IN_ARCHIVE`) |
| `timeline_mention_surface` | `MENTIONS_TIMELINE_EVENT` edges from profile/contact surfaces |
| `time_node_surface` | `TimeNode` labels and/or `IN_TIME_NODE`/`NEXT_TIME_NODE` edges |
| `topic_surface` | `Topic` labels and topic relations (`RESEARCHES`, `FOCUSES_ON`, `HAS_TOPIC`, `HAS_SKILL_TOPIC`, `HAS_HOBBY_TOPIC`, `HAS_INTEREST_TOPIC`) |
| `evidence_surface` | evidence entities (`Document`, `ArchivedPage`, `CorporateFiling`) or evidence relations (`FILED`, `APPEARS_IN_ARCHIVE`, `HAS_EVIDENCE`) |

## Unified Topic Model
- Topic node type: `Topic`
- Required topic class attribute:
  - `topic_kind: <skill|hobby|interest|research|industry|language|domain|community>`
- Preferred person-to-topic relations:
  - `HAS_SKILL_TOPIC`
  - `HAS_HOBBY_TOPIC`
  - `HAS_INTEREST_TOPIC`
  - `RESEARCHES` / `FOCUSES_ON` / `HAS_TOPIC` for non-person or general links

## Time Model
- Explicit time entity: `TimeNode`
- Required timeline link relations:
  - `IN_TIME_NODE` from `TimelineEvent`/`Experience`/`EducationalCredential`/`Affiliation`/`Publication`
  - `NEXT_TIME_NODE` for sortable chronological chains
- Social timeline mentions:
  - `MENTIONS_TIMELINE_EVENT` from LinkedIn/X profile/contact surfaces to `TimelineEvent`

## Canonical Runtime Names
- New standardized entity type:
  - `TimeNode`
- New standardized relation types:
  - `HAS_SKILL_TOPIC`
  - `HAS_HOBBY_TOPIC`
  - `HAS_INTEREST_TOPIC`
  - `MENTIONS_TIMELINE_EVENT`
  - `IN_TIME_NODE`
  - `NEXT_TIME_NODE`

## Balanced Gate Definition
- Controlled by:
  - `STAGE1_BLUEPRINT_ENABLED`
  - `STAGE1_BLUEPRINT_CONTRACT_PATH`
  - `STAGE1_BLUEPRINT_ENFORCEMENT`
- In `balanced` mode, Stage 1 stop is blocked when any contract `required_slots_balanced` slot is missing.
- Safety fallback behavior:
  - If graph tools are unavailable (`graph_search_entities`/`graph_neighbors` etc.), planner falls back to receipt-driven logic and does not hard-fail.
