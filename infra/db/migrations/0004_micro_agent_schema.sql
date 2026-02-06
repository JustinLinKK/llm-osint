-- 0004_micro_agent_schema.sql
-- Micro-agent artifacts + receipts + notes

CREATE TABLE IF NOT EXISTS artifacts (
  artifact_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id          UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  document_id     UUID REFERENCES documents(document_id) ON DELETE SET NULL,
  tool_name       TEXT NOT NULL,
  kind            TEXT NOT NULL,
  bucket          TEXT,
  object_key      TEXT,
  version_id      TEXT,
  etag            TEXT,
  size_bytes      BIGINT,
  content_type    TEXT,
  sha256          TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_artifacts_run_id ON artifacts(run_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_document_id ON artifacts(document_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_tool_name ON artifacts(tool_name);

CREATE TABLE IF NOT EXISTS artifact_summaries (
  summary_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  artifact_id     UUID NOT NULL REFERENCES artifacts(artifact_id) ON DELETE CASCADE,
  summary         TEXT NOT NULL,
  key_facts       JSONB NOT NULL DEFAULT '[]'::jsonb,
  confidence      REAL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_artifact_summaries_artifact_id ON artifact_summaries(artifact_id);

CREATE TABLE IF NOT EXISTS tool_call_receipts (
  receipt_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id          UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  tool_name       TEXT NOT NULL,
  ok              BOOLEAN NOT NULL,
  arguments       JSONB NOT NULL DEFAULT '{}'::jsonb,
  summary_id      UUID REFERENCES artifact_summaries(summary_id) ON DELETE SET NULL,
  artifact_ids    JSONB NOT NULL DEFAULT '[]'::jsonb,
  vector_upserts  JSONB NOT NULL DEFAULT '{}'::jsonb,
  graph_upserts   JSONB NOT NULL DEFAULT '{}'::jsonb,
  next_hints      JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tool_call_receipts_run_id ON tool_call_receipts(run_id);
CREATE INDEX IF NOT EXISTS idx_tool_call_receipts_tool_name ON tool_call_receipts(tool_name);

CREATE TABLE IF NOT EXISTS run_notes (
  note_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id          UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  note            TEXT NOT NULL,
  citations       JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_run_notes_run_id ON run_notes(run_id);
