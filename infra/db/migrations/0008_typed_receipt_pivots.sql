-- 0008_typed_receipt_pivots.sql
-- Add typed receipt pivots so planners do not rely on untyped free-text next_hints.

ALTER TABLE tool_call_receipts
ADD COLUMN IF NOT EXISTS next_urls JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE tool_call_receipts
ADD COLUMN IF NOT EXISTS next_people JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE tool_call_receipts
ADD COLUMN IF NOT EXISTS next_orgs JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE tool_call_receipts
ADD COLUMN IF NOT EXISTS next_topics JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE tool_call_receipts
ADD COLUMN IF NOT EXISTS next_handles JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE tool_call_receipts
ADD COLUMN IF NOT EXISTS next_queries JSONB NOT NULL DEFAULT '[]'::jsonb;
