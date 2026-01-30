import { emitEvent } from "./services/events.js";

export async function plan(runId: string) {
  await emitEvent(runId, "PLANNER_STARTED", {});
  await emitEvent(runId, "TOOLS_SELECTED", { tools: [] });
}

export async function collect(runId: string) {
  await emitEvent(runId, "TOOL_CALL_STARTED", { tool: "mcp" });
  await emitEvent(runId, "TOOL_CALL_FINISHED", { tool: "mcp", ok: true });
}

export async function process(runId: string) {
  await emitEvent(runId, "PROCESSING_STARTED", {});
  await emitEvent(runId, "CHUNKING_FINISHED", {});
  await emitEvent(runId, "EMBEDDING_FINISHED", {});
  await emitEvent(runId, "GRAPH_FINISHED", {});
}

export async function synthesize(runId: string) {
  await emitEvent(runId, "SYNTHESIS_STARTED", {});
  await emitEvent(runId, "REPORT_READY", {});
}

export type Activities = typeof import("./activities.js");
