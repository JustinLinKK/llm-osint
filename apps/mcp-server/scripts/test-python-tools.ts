#!/usr/bin/env node
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import crypto from "node:crypto";
import dotenv from "dotenv";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const rootDir = resolve(__dirname, "../../..");
dotenv.config({ path: resolve(rootDir, ".env") });

const runId = process.env.RUN_ID ?? crypto.randomUUID();
const databaseUrl = process.env.DATABASE_URL ?? "postgresql://osint:osint@postgres:5432/osint";
const toolName = process.env.MCP_TEST_TOOL ?? "x_get_user_posts_api";
const username = process.env.X_TEST_USERNAME ?? "openai";
const maxResults = Number(process.env.X_TEST_MAX_RESULTS ?? "5");
const serverUrl = process.env.MCP_SERVER_URL ?? "http://localhost:3001/mcp";
const testArgsJson = process.env.MCP_TEST_ARGS;

async function ensureRun() {
  const pool = new pg.Pool({ connectionString: databaseUrl });
  try {
    await pool.query(
      "INSERT INTO runs(run_id, prompt, seeds, status) VALUES ($1, $2, $3::jsonb, $4) ON CONFLICT (run_id) DO NOTHING",
      [runId, `${toolName} test`, "[]", "created"]
    );
  } finally {
    await pool.end();
  }
}

async function main() {
  await ensureRun();

  const transport = new StreamableHTTPClientTransport(new URL(serverUrl));

  const client = new Client({ name: "osint-mcp-client", version: "1.0.0" }, { capabilities: {} });
  await client.connect(transport);

  let toolArgs: Record<string, unknown>;
  if (testArgsJson) {
    const parsed = JSON.parse(testArgsJson) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("MCP_TEST_ARGS must be a JSON object");
    }
    toolArgs = parsed as Record<string, unknown>;
  } else {
    toolArgs = {
      runId,
      username,
      max_results: maxResults,
    };
  }

  if (!("runId" in toolArgs)) {
    toolArgs.runId = runId;
  }

  const result = await client.callTool({
    name: toolName,
    arguments: toolArgs,
  });

  console.log(
    JSON.stringify(
      {
        serverUrl,
        runId,
        toolName,
        arguments: toolArgs,
        result,
      },
      null,
      2
    )
  );
  await client.close();
}

main().catch((error) => {
  console.error("Test failed:", error);
  process.exit(1);
});
