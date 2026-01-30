-- 0002_run_events.sql
-- Run events for observability / UI streaming

CREATE TABLE IF NOT EXISTS run_events (
  event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id          UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  type            TEXT NOT NULL,
  ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload         JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_run_events_run_id_ts ON run_events(run_id, ts);
