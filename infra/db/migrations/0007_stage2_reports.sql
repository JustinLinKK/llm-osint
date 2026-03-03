-- 0007_stage2_reports.sql
-- Stage 2 report graph persistence (queryable outside LangGraph)

CREATE TABLE IF NOT EXISTS report_runs (
  run_id              UUID PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
  report_type         TEXT NOT NULL DEFAULT 'person', -- person|org
  status              TEXT NOT NULL DEFAULT 'draft',  -- draft|ready|failed
  refine_round        INT NOT NULL DEFAULT 0,
  quality_ok          BOOLEAN NOT NULL DEFAULT FALSE,
  final_report        TEXT NOT NULL DEFAULT '',
  evidence_appendix   TEXT NOT NULL DEFAULT '',
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_report_runs_status ON report_runs(status);
CREATE INDEX IF NOT EXISTS idx_report_runs_updated_at ON report_runs(updated_at);

CREATE TABLE IF NOT EXISTS section_drafts (
  run_id              UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  section_id          TEXT NOT NULL,
  section_order       INT NOT NULL DEFAULT 0,
  title               TEXT NOT NULL,
  content             TEXT NOT NULL,
  citation_keys       JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, section_id)
);

CREATE INDEX IF NOT EXISTS idx_section_drafts_run_order ON section_drafts(run_id, section_order);

CREATE TABLE IF NOT EXISTS claim_ledger (
  run_id              UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  claim_id            TEXT NOT NULL,
  section_id          TEXT NOT NULL,
  claim_text          TEXT NOT NULL,
  confidence          REAL NOT NULL DEFAULT 0.0,
  impact              TEXT NOT NULL DEFAULT 'medium',
  evidence_keys       JSONB NOT NULL DEFAULT '[]'::jsonb,
  conflict_flags      JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, claim_id)
);

CREATE INDEX IF NOT EXISTS idx_claim_ledger_run_section ON claim_ledger(run_id, section_id);
CREATE INDEX IF NOT EXISTS idx_claim_ledger_impact ON claim_ledger(impact);

CREATE TABLE IF NOT EXISTS evidence_refs (
  run_id              UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  citation_key        TEXT NOT NULL,
  section_id          TEXT NOT NULL,
  document_id         UUID REFERENCES documents(document_id) ON DELETE SET NULL,
  snippet             TEXT NOT NULL,
  source_url          TEXT,
  score               REAL,
  object_ref          JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, citation_key)
);

CREATE INDEX IF NOT EXISTS idx_evidence_refs_run_section ON evidence_refs(run_id, section_id);
CREATE INDEX IF NOT EXISTS idx_evidence_refs_document ON evidence_refs(document_id);
