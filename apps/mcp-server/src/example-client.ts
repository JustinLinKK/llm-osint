#!/usr/bin/env node
/**
 * Example MCP client that demonstrates how to connect to and use the MCP server.
 * 
 * This shows how to:
 * 1. Load configuration from .env file
 * 2. Spawn the MCP server as a subprocess
 * 3. Initialize the connection
 * 4. List available tools
 * 5. Call the fetch_url tool
 */

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import dotenv from "dotenv";
import { resolve } from "path";
import { fileURLToPath } from "url";
import { dirname } from "path";

// Load environment variables from workspace root
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const rootDir = resolve(__dirname, "../../..");
dotenv.config({ path: resolve(rootDir, ".env") });

async function main() {
  console.log("Starting MCP client example...\n");
  console.log(`Loading config from: ${rootDir}/.env\n`);

  // 1. Create stdio transport (this will spawn the server process)
  // Environment variables are loaded from .env file and passed to the server
  const transport = new StdioClientTransport({
    command: "yarn",
    args: ["tsx", "src/index.ts"],
    env: {
      ...process.env,
      // These are already set from .env, but we make them explicit here
      DATABASE_URL: process.env.DATABASE_URL || "postgresql://osint:osint@postgres:5432/osint",
      MINIO_ENDPOINT: process.env.MINIO_ENDPOINT || "http://minio:9000",
      MINIO_ACCESS_KEY: process.env.MINIO_ACCESS_KEY || "minio",
      MINIO_SECRET_KEY: process.env.MINIO_SECRET_KEY || "minio12345",
      MINIO_BUCKET: process.env.MINIO_BUCKET || "osint-raw",
    },
  });

  const client = new Client(
    {
      name: "osint-mcp-client",
      version: "1.0.0",
    },
    {
      capabilities: {},
    }
  );

  // 2. Connect to the server (transport will spawn the process)
  console.log("Connecting to MCP server...");
  await client.connect(transport);
  console.log("✓ Connected to MCP server\n");

  try {
    // 3. List available tools
    console.log("Fetching available tools...");
    const toolsResponse = await client.listTools();
    console.log("✓ Available tools:");
    toolsResponse.tools.forEach((tool) => {
      console.log(`  - ${tool.name}: ${tool.description}`);
    });
    console.log();

    // 4. Create a test run first (the runId must exist in the database)
    console.log("Creating test run in database...");
    const { default: pg } = await import("pg");
    const dbPool = new pg.Pool({
      connectionString: process.env.DATABASE_URL || "postgresql://osint:osint@postgres:5432/osint",
    });
    
    const testRunId = "550e8400-e29b-41d4-a716-446655440000";
    await dbPool.query(
      `INSERT INTO runs(run_id, prompt, seeds, status) 
       VALUES ($1, $2, $3::jsonb, $4) 
       ON CONFLICT (run_id) DO NOTHING`,
      [testRunId, "Test investigation", JSON.stringify([{ type: "url", value: "https://example.com" }]), "created"]
    );
    console.log(`✓ Test run created with ID: ${testRunId}\n`);

    // 5. Call the fetch_url tool
    const testUrl = "https://example.com";

    console.log(`Calling fetch_url tool with:`);
    console.log(`  runId: ${testRunId}`);
    console.log(`  url: ${testUrl}\n`);

    const result = await client.callTool({
      name: "fetch_url",
      arguments: {
        runId: testRunId,
        url: testUrl,
      },
    });

    console.log("✓ Tool execution result:");
    console.log(JSON.stringify(result, null, 2));
  } catch (error) {
    console.error("Error:", error);
  } finally {
    // 6. Cleanup
    console.log("\nClosing connection...");
    await client.close();
    console.log("✓ Connection closed");
  }
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
