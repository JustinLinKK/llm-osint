-- 0001_init.sql
-- Core metadata + provenance schema for OSINT pipeline

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) Runs (one investigation session)
CREATE TABLE IF NOT EXISTS runs (
  run_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_by       TEXT,
  status           TEXT NOT NULL DEFAULT 'created',  -- created|collecting|extracting|mining|reporting|done|failed
  prompt           TEXT NOT NULL,
  seeds            JSONB NOT NULL DEFAULT '[]'::jsonb,  -- [{type:"handle", value:"..."}, ...]
  constraints      JSONB NOT NULL DEFAULT '{}'::jsonb,   -- allowlist, max_depth, time_window, etc.
  notes            TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at);

-- 2) Documents (logical artifact)
CREATE TABLE IF NOT EXISTS documents (
  document_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id           UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,

  source_url       TEXT,                      -- can be null for file drops
  source_domain    TEXT,
  source_type      TEXT NOT NULL,             -- html|pdf|image|audio|video|text|json
  retrieved_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at     TIMESTAMPTZ,

  content_type     TEXT,                      -- HTTP Content-Type or inferred
  language         TEXT,                      -- ISO code if detected (optional)

  sha256           TEXT NOT NULL,             -- hash of normalized bytes (raw object) OR raw payload
  trust_tier       SMALLINT NOT NULL DEFAULT 3, -- 1=official,2=reputable,3=ugc,4=unknown
  license_flag     BOOLEAN NOT NULL DEFAULT FALSE,

  title            TEXT,
  summary          TEXT,                      -- optional quick summary (non-LLM or LLM later)
  extraction_state TEXT NOT NULL DEFAULT 'pending', -- pending|parsed|extracted|failed

  UNIQUE (run_id, sha256)
);

CREATE INDEX IF NOT EXISTS idx_documents_run_id ON documents(run_id);
CREATE INDEX IF NOT EXISTS idx_documents_source_domain ON documents(source_domain);
CREATE INDEX IF NOT EXISTS idx_documents_retrieved_at ON documents(retrieved_at);

-- 3) Document storage pointers (MinIO object reference)
-- Keep this separate so you can store multiple representations per document
-- e.g. raw bytes, normalized text, OCR text, ASR transcript
CREATE TABLE IF NOT EXISTS document_objects (
  object_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id      UUID NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,

  kind             TEXT NOT NULL,             -- raw|normalized_text|ocr_text|asr_text|thumbnail|metadata_json
  bucket           TEXT NOT NULL,
  object_key       TEXT NOT NULL,             -- path/key in MinIO
  version_id       TEXT,                      -- MinIO version id (nullable if versioning disabled)
  etag             TEXT,                      -- MinIO etag (useful for integrity)
  size_bytes       BIGINT NOT NULL,
  content_type     TEXT,

  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (document_id, kind),
  UNIQUE (bucket, object_key, version_id)
);

CREATE INDEX IF NOT EXISTS idx_doc_objects_document_id ON document_objects(document_id);

-- 4) Chunks (for vector DB linking + evidence snippets)
CREATE TABLE IF NOT EXISTS chunks (
  chunk_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id      UUID NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,

  kind             TEXT NOT NULL DEFAULT 'body', -- body|title|caption|ocr|asr
  chunk_index      INT NOT NULL,
  char_start       INT,
  char_end         INT,
  text             TEXT NOT NULL,

  -- links to vector DB record
  vector_id        TEXT,                        -- Qdrant point id (string/uuid)
  embedding_model  TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (document_id, kind, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_chunks_document_id ON chunks(document_id);
CREATE INDEX IF NOT EXISTS idx_chunks_vector_id ON chunks(vector_id);

-- 5) Tool call audit log (agentic collection observability)
CREATE TABLE IF NOT EXISTS tool_calls (
  tool_call_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id           UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,

  tool_name        TEXT NOT NULL,              -- fetch_url|crawl_domain|parse_pdf|ocr|asr|...
  requested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at      TIMESTAMPTZ,

  input            JSONB NOT NULL DEFAULT '{}'::jsonb,
  output           JSONB NOT NULL DEFAULT '{}'::jsonb,

  status           TEXT NOT NULL DEFAULT 'ok', -- ok|error|blocked
  error_message    TEXT
);

CREATE INDEX IF NOT EXISTS idx_tool_calls_run_id ON tool_calls(run_id);
CREATE INDEX IF NOT EXISTS idx_tool_calls_tool_name ON tool_calls(tool_name);
CREATE INDEX IF NOT EXISTS idx_tool_calls_requested_at ON tool_calls(requested_at);
