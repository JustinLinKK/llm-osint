#!/usr/bin/env node
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import crypto from "node:crypto";
import dotenv from "dotenv";
import { resolve } from "node:path";
import pg from "pg";

const rootDir = resolve(__dirname, "../../..");
dotenv.config({ path: resolve(rootDir, ".env") });

const runId = process.env.RUN_ID ?? crypto.randomUUID();
const databaseUrl = process.env.DATABASE_URL ?? "postgresql://osint:osint@postgres:5432/osint";

async function ensureRun() {
  const pool = new pg.Pool({ connectionString: databaseUrl });
  try {
    await pool.query(
      "INSERT INTO runs(run_id, prompt, seeds, status) VALUES ($1, $2, $3::jsonb, $4) ON CONFLICT (run_id) DO NOTHING",
      [runId, "graph ingest test", "[]", "created"]
    );
  } finally {
    await pool.end();
  }
}

async function main() {
  await ensureRun();

  const transport = new StdioClientTransport({
    command: "yarn",
    args: ["tsx", "src/index.ts"],
    cwd: resolve("apps/mcp-server"),
  });

  const client = new Client({ name: "osint-mcp-client", version: "1.0.0" }, { capabilities: {} });
  await client.connect(transport);

  const result = await client.callTool({
    name: "ingest_graph_entity",
    arguments: {
      runId,
      entityType: "Location",
      propertiesJson: JSON.stringify({
        name: "Metropolitan Museum of Art",
        address: "1000 5th Ave, New York, NY 10028",
        country: "US",
      }),
      evidenceJson: JSON.stringify({
        snippetText: "The museum is located at 1000 5th Ave, New York, NY 10028.",
        sourceUrl: "https://www.metmuseum.org/visit",
      }),
    },
  });

  console.log(JSON.stringify(result, null, 2));
  await client.close();
}

main().catch((error) => {
  console.error("Test failed:", error);
  process.exit(1);
});
