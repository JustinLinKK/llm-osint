import { v4 as uuidv4 } from "uuid";
import { pool } from "../clients/pg.js";

export async function emitRunEvent(
  runId: string,
  type: string,
  payload: Record<string, unknown>
): Promise<void> {
  try {
    await pool.query(
      `INSERT INTO run_events(run_id, type, ts, payload)
       VALUES ($1, $2, now(), $3::jsonb)`,
      [runId, type, JSON.stringify(payload)]
    );
  } catch (error) {
    console.error("Error emitting run event:", error);
  }
}

export async function logToolCall(
  runId: string,
  toolName: string,
  input: Record<string, unknown>,
  output: Record<string, unknown>,
  status: "ok" | "error",
  errorMessage?: string
): Promise<void> {
  try {
    await pool.query(
      `INSERT INTO tool_calls(tool_call_id, run_id, tool_name, requested_at, finished_at, input, output, status, error_message)
       VALUES ($1, $2, $3, now(), now(), $4::jsonb, $5::jsonb, $6, $7)`,
      [uuidv4(), runId, toolName, JSON.stringify(input), JSON.stringify(output), status, errorMessage ?? null]
    );
  } catch (error) {
    console.error("Error logging tool call:", error);
  }
}
