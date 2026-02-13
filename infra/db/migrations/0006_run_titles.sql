-- 0006_run_titles.sql
-- Add run titles for frontend discoverability

ALTER TABLE runs
ADD COLUMN IF NOT EXISTS title TEXT;

-- Backfill existing rows with prompt-based fallback so legacy runs remain usable in UI.
UPDATE runs
SET title = left(regexp_replace(trim(prompt), '\\s+', ' ', 'g'), 120)
WHERE (title IS NULL OR trim(title) = '')
  AND prompt IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_runs_title ON runs(title);
