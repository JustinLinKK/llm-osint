-- 0003_reports.sql
-- Report pointers + status

CREATE TABLE IF NOT EXISTS reports (
  report_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id                UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  status                TEXT NOT NULL DEFAULT 'draft', -- draft|ready|failed

  markdown_bucket       TEXT,
  markdown_object_key   TEXT,
  markdown_version_id   TEXT,

  json_bucket           TEXT,
  json_object_key       TEXT,
  json_version_id       TEXT
);

CREATE INDEX IF NOT EXISTS idx_reports_run_id ON reports(run_id);
