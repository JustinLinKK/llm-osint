-- 0005_evidence_links.sql
-- Evidence links for vector chunks (MinIO pointers)

ALTER TABLE chunks
  ADD COLUMN IF NOT EXISTS evidence_bucket TEXT,
  ADD COLUMN IF NOT EXISTS evidence_object_key TEXT,
  ADD COLUMN IF NOT EXISTS evidence_version_id TEXT,
  ADD COLUMN IF NOT EXISTS evidence_etag TEXT,
  ADD COLUMN IF NOT EXISTS evidence_document_id UUID;
