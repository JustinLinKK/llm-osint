import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { resolve, relative } from "node:path";
import crypto from "node:crypto";
import { v4 as uuidv4 } from "uuid";
import { cfg } from "../config.js";
import { pool } from "../clients/pg.js";
import { minio, ensureBucket } from "../clients/minio.js";
import { emitRunEvent, logToolCall } from "./helpers.js";
import { runPythonTool } from "./python_bridge.js";
import { logger } from "../utils/logger.js";

type PythonToolConfig = {
  name: string;
  description: string;
  scriptPath: string;
  timeoutMs?: number;
};

type StoredResult = {
  documentId: string;
  bucket: string;
  objectKey: string;
  versionId: string | null;
  etag: string | null;
  sizeBytes: number;
  contentType: string;
  sha256: string;
};

const configSchema = z.array(
  z.object({
    name: z.string().min(1),
    description: z.string().min(1),
    scriptPath: z.string().min(1),
    timeoutMs: z.number().int().positive().optional(),
  })
);

function parsePythonToolConfig(): PythonToolConfig[] {
  if (!cfg.python.toolsJson) return [];

  try {
    const parsed = JSON.parse(cfg.python.toolsJson) as unknown;
    return configSchema.parse(parsed);
  } catch (error) {
    console.error("Invalid MCP_PYTHON_TOOLS JSON:", error);
    return [];
  }
}

function resolveToolPath(scriptPath: string): string {
  const resolved = resolve(cfg.paths.repoRoot, scriptPath);
  const rel = relative(cfg.paths.repoRoot, resolved);
  if (rel.startsWith("..")) {
    throw new Error(`Python tool path must be inside repo: ${scriptPath}`);
  }
  return resolved;
}

async function storePythonResult(runId: string, toolName: string, payload: unknown): Promise<StoredResult> {
  const bytes = Buffer.from(JSON.stringify(payload));
  const sha256 = crypto.createHash("sha256").update(bytes).digest("hex");
  const objectKey = `runs/${runId}/raw/python/${toolName}/${sha256}.json`;
  const contentType = "application/json";

  await ensureBucket(cfg.minio.bucket);

  const putRes = await minio.putObject(
    cfg.minio.bucket,
    objectKey,
    bytes,
    bytes.length,
    { "Content-Type": contentType }
  );

  const etag = (putRes as any).etag ?? null;
  const versionId = (putRes as any).versionId ?? null;

  let documentId: string | null = null;
  const existing = await pool.query(
    "SELECT document_id FROM documents WHERE run_id = $1 AND sha256 = $2",
    [runId, sha256]
  );
  if (existing.rows[0]?.document_id) {
    documentId = existing.rows[0].document_id;
  } else {
    documentId = uuidv4();
    await pool.query(
      `INSERT INTO documents(
        document_id, run_id, source_url, source_domain, source_type,
        content_type, sha256, trust_tier, extraction_state, title
      ) VALUES ($1, $2, $3, $4, 'json', $5, $6, 3, 'parsed', $7)`,
      [documentId, runId, null, null, contentType, sha256, `python:${toolName}`]
    );
  }

  await pool.query(
    `INSERT INTO document_objects(
      object_id, document_id, kind, bucket, object_key, version_id, etag, size_bytes, content_type
    ) VALUES ($1, $2, 'raw', $3, $4, $5, $6, $7, $8)
    ON CONFLICT (document_id, kind) DO NOTHING`,
    [uuidv4(), documentId, cfg.minio.bucket, objectKey, versionId, etag, bytes.length, contentType]
  );

  return {
    documentId,
    bucket: cfg.minio.bucket,
    objectKey,
    versionId,
    etag,
    sizeBytes: bytes.length,
    contentType,
    sha256,
  };
}

export function registerPythonTools(server: McpServer) {
  const tools = parsePythonToolConfig();
  if (!tools.length) return;

  for (const tool of tools) {
    const resolvedPath = resolveToolPath(tool.scriptPath);

    server.registerTool(
      tool.name,
      {
        description: tool.description,
        inputSchema: z.object({ runId: z.string().uuid() }).passthrough(),
      },
      async (input) => {
        const runId = input.runId as string;
        await emitRunEvent(runId, "TOOL_CALL_STARTED", { tool: tool.name });
        logger.info("python tool started", { runId, tool: tool.name });

        try {
          const result = await runPythonTool({
            pythonBin: cfg.python.bin,
            scriptPath: resolvedPath,
            toolName: tool.name,
            input,
            timeoutMs: tool.timeoutMs,
          });

          if (!result.ok) {
            const errorMessage = result.error ?? "Python tool failed";
            await logToolCall(runId, tool.name, input, { error: errorMessage }, "error", errorMessage);
            await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: tool.name, ok: false, error: errorMessage });
            logger.error("python tool failed", { runId, tool: tool.name, error: errorMessage });

            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify({ error: errorMessage }, null, 2),
                },
              ],
              isError: true,
            };
          }

          const output = result.result ?? {};
          const stored = await storePythonResult(runId, tool.name, output);

          let responsePayload: Record<string, unknown>;
          if (output && typeof output === "object" && !Array.isArray(output)) {
            responsePayload = { ...(output as Record<string, unknown>), evidence: stored };
          } else {
            responsePayload = { result: output, evidence: stored };
          }

          await logToolCall(runId, tool.name, input, responsePayload, "ok");
          await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: tool.name, ok: true });
          logger.info("python tool finished", { runId, tool: tool.name, documentId: stored.documentId });

          return {
            content: [
              {
                type: "text",
                text: JSON.stringify(responsePayload, null, 2),
              },
            ],
          };
        } catch (error) {
          const errorMsg = (error as Error).message;
          await logToolCall(runId, tool.name, input, { error: errorMsg }, "error", errorMsg);
          await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: tool.name, ok: false, error: errorMsg });
          logger.error("python tool exception", { runId, tool: tool.name, error: errorMsg });

          return {
            content: [
              {
                type: "text",
                text: JSON.stringify({ error: errorMsg }, null, 2),
              },
            ],
            isError: true,
          };
        }
      }
    );
  }
}
