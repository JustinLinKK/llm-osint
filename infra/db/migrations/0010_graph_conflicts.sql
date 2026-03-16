-- 0010_graph_conflicts.sql
-- Stage 1 conflict adjudication persistence and evidence lineage.

CREATE TABLE IF NOT EXISTS graph_conflict_cases (
  case_id               TEXT PRIMARY KEY,
  run_id                UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  target_type           TEXT NOT NULL DEFAULT 'entity',
  target_id             TEXT,
  field_name            TEXT NOT NULL,
  relation_type         TEXT,
  scope                 TEXT NOT NULL DEFAULT 'primary',
  status                TEXT NOT NULL DEFAULT 'detected',
  blocking              BOOLEAN NOT NULL DEFAULT TRUE,
  chosen_value          TEXT,
  deterministic_winner  TEXT,
  confidence            REAL NOT NULL DEFAULT 0.0,
  rationale             TEXT NOT NULL DEFAULT '',
  candidate_values      JSONB NOT NULL DEFAULT '[]'::jsonb,
  source_tools          JSONB NOT NULL DEFAULT '[]'::jsonb,
  source_domains        JSONB NOT NULL DEFAULT '[]'::jsonb,
  graph_entity_ids      JSONB NOT NULL DEFAULT '[]'::jsonb,
  graph_relation_ids    JSONB NOT NULL DEFAULT '[]'::jsonb,
  notes                 JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_graph_conflict_cases_run_id ON graph_conflict_cases(run_id);
CREATE INDEX IF NOT EXISTS idx_graph_conflict_cases_status ON graph_conflict_cases(run_id, status);
CREATE INDEX IF NOT EXISTS idx_graph_conflict_cases_blocking ON graph_conflict_cases(run_id, blocking);

CREATE TABLE IF NOT EXISTS graph_conflict_evidence (
  evidence_id           TEXT PRIMARY KEY,
  run_id                UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  case_id               TEXT NOT NULL REFERENCES graph_conflict_cases(case_id) ON DELETE CASCADE,
  candidate_value       TEXT,
  polarity              TEXT NOT NULL DEFAULT 'supporting',
  document_id           UUID REFERENCES documents(document_id) ON DELETE SET NULL,
  chunk_id              UUID REFERENCES chunks(chunk_id) ON DELETE SET NULL,
  object_ref            JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_url            TEXT,
  source_domain         TEXT,
  snippet               TEXT NOT NULL DEFAULT '',
  source_tool           TEXT,
  retrieved_at          TIMESTAMPTZ,
  score                 REAL NOT NULL DEFAULT 0.0,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_graph_conflict_evidence_run_id ON graph_conflict_evidence(run_id);
CREATE INDEX IF NOT EXISTS idx_graph_conflict_evidence_case_id ON graph_conflict_evidence(case_id);
CREATE INDEX IF NOT EXISTS idx_graph_conflict_evidence_document_id ON graph_conflict_evidence(document_id);
