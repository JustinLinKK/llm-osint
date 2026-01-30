import { pool } from "../clients/pg.js";

export type RunEventRow = {
  event_id: string;
  run_id: string;
  type: string;
  ts: string;
  payload: Record<string, unknown>;
};

export async function emitEvent(
  runId: string,
  type: string,
  payload: Record<string, unknown> = {}
) {
  await pool.query(
    `INSERT INTO run_events(run_id, type, ts, payload)
     VALUES ($1, $2, now(), $3::jsonb)`,
    [runId, type, JSON.stringify(payload)]
  );
}

export async function listRunEvents(runId: string, sinceTs?: Date | null, limit = 200) {
  if (sinceTs) {
    const { rows } = await pool.query<RunEventRow>(
      `SELECT event_id, run_id, type, ts, payload
       FROM run_events
       WHERE run_id = $1 AND ts > $2
       ORDER BY ts ASC
       LIMIT $3`,
      [runId, sinceTs.toISOString(), limit]
    );
    return rows;
  }

  const { rows } = await pool.query<RunEventRow>(
    `SELECT event_id, run_id, type, ts, payload
     FROM run_events
     WHERE run_id = $1
     ORDER BY ts ASC
     LIMIT $2`,
    [runId, limit]
  );
  return rows;
}
