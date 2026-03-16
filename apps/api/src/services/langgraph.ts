import { existsSync } from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { pool } from "../clients/pg.js";
import { cfg } from "../config.js";
import { emitEvent } from "./events.js";

type LaunchLangGraphParams = {
  runId: string;
  prompt: string;
};

type Stage2ModelOverrides = Record<string, unknown>;

type ChildFailureInfo = {
  phase: "spawn" | "exit";
  error: string;
  stderrTail?: string;
  exitCode?: number | null;
  signal?: NodeJS.Signals | null;
};

function resolvePlannerScriptPath(): string | null {
  const candidates = [
    cfg.langgraph.scriptPath,
    path.resolve(process.cwd(), "services/agent-langgraph/src/run_planner.py"),
    path.resolve(process.cwd(), "../services/agent-langgraph/src/run_planner.py"),
    path.resolve(process.cwd(), "../../services/agent-langgraph/src/run_planner.py"),
    path.resolve(process.cwd(), "../../../services/agent-langgraph/src/run_planner.py"),
    path.resolve(process.cwd(), "../../../../services/agent-langgraph/src/run_planner.py"),
    "/workspaces/llm-osint/services/agent-langgraph/src/run_planner.py"
  ].filter((item): item is string => Boolean(item && item.trim()));

  for (const candidate of candidates) {
    const resolved = path.resolve(candidate);
    if (existsSync(resolved)) return resolved;
  }
  return null;
}

async function markRunStatus(runId: string, status: string) {
  await pool.query(`UPDATE runs SET status = $2 WHERE run_id = $1`, [runId, status]);
}

async function emitLaunchFailure(runId: string, info: ChildFailureInfo) {
  await markRunStatus(runId, "failed");
  await emitEvent(runId, "RUN_FAILED", info);
}

async function loadStage2ModelOverrides(runId: string): Promise<Stage2ModelOverrides> {
  const { rows } = await pool.query(`SELECT constraints FROM runs WHERE run_id = $1`, [runId]);
  const constraints = rows[0]?.constraints;
  if (!constraints || typeof constraints !== "object") return {};
  const stage2 = (constraints as Record<string, unknown>).stage2;
  if (!stage2 || typeof stage2 !== "object") return {};
  const models = (stage2 as Record<string, unknown>).models;
  if (!models || typeof models !== "object") return {};
  return models as Stage2ModelOverrides;
}

export async function launchLangGraphRun(params: LaunchLangGraphParams): Promise<void> {
  if (!cfg.langgraph.autostart) return;

  const scriptPath = resolvePlannerScriptPath();
  if (!scriptPath) {
    await emitLaunchFailure(params.runId, {
      phase: "spawn",
      error: "LangGraph planner script not found. Set LANGGRAPH_SCRIPT_PATH or disable LANGGRAPH_AUTOSTART."
    });
    return;
  }

  await markRunStatus(params.runId, "running");
  await emitEvent(params.runId, "RUN_STARTED", {
    engine: "langgraph",
    scriptPath,
    maxIterations: cfg.langgraph.maxIterations
  });

  const args = [
    scriptPath,
    "--run-id",
    params.runId,
    "--prompt",
    params.prompt,
    "--run-stage2",
    "--max-iterations",
    String(cfg.langgraph.maxIterations)
  ];
  const stage2ModelOverrides = await loadStage2ModelOverrides(params.runId);
  if (Object.keys(stage2ModelOverrides).length > 0) {
    args.push("--stage2-model-config-json", JSON.stringify(stage2ModelOverrides));
  }

  const child = spawn(cfg.langgraph.pythonBin, args, {
    cwd: cfg.langgraph.workdir || process.cwd(),
    env: process.env,
    stdio: ["ignore", "ignore", "pipe"]
  });

  let stderrTail = "";
  let finalized = false;
  child.stderr.on("data", (chunk: Buffer | string) => {
    const message = String(chunk);
    stderrTail = (stderrTail + message).slice(-8000);
  });

  child.once("error", async (error) => {
    if (finalized) return;
    finalized = true;
    await emitLaunchFailure(params.runId, {
      phase: "spawn",
      error: error.message
    });
  });

  child.once("close", async (code, signal) => {
    if (finalized) return;
    finalized = true;
    if (code === 0) {
      await markRunStatus(params.runId, "done");
      await emitEvent(params.runId, "RUN_FINISHED", {
        engine: "langgraph",
        exitCode: code
      });
      return;
    }

    await emitLaunchFailure(params.runId, {
      phase: "exit",
      error: "LangGraph process exited with a non-zero status",
      stderrTail: stderrTail || undefined,
      exitCode: code,
      signal
    });
  });
}
