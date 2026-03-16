-- 0009_primary_target_contract.sql
-- Persist the Stage 1 anchor lock so Stage 2 reruns do not need to infer the target again.

ALTER TABLE runs
ADD COLUMN IF NOT EXISTS primary_target_contract JSONB NOT NULL DEFAULT '{}'::jsonb;
