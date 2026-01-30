import { pool } from "../clients/pg.js";

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
