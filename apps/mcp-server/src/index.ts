import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import crypto from "node:crypto";
import { v4 as uuidv4 } from "uuid";
import { pool } from "./clients/pg.js";
import { minio, ensureBucket } from "./clients/minio.js";
import { cfg } from "./config.js";

const USER_AGENT = "osint-mcp-bot/1.0";

// Helper: Emit run event
async function emitRunEvent(
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

// Helper: Log tool call
async function logToolCall(
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

// Helper: Store document to MinIO and Postgres
async function storeDocument(
  runId: string,
  url: string,
  bytes: Buffer,
  contentType: string
): Promise<{ documentId: string; objectKey: string; etag: string | null; sourceType: string; sha256: string }> {
  const sha256 = crypto.createHash("sha256").update(bytes).digest("hex");

  await ensureBucket(cfg.minio.bucket);

  const sourceType = contentType.startsWith("text/html")
    ? "html"
    : contentType.startsWith("application/pdf")
    ? "pdf"
    : contentType.startsWith("image/")
    ? "image"
    : "text";

  const objectKey = `runs/${runId}/raw/${sourceType}/${sha256}.${sourceType}`;

  const putRes = await minio.putObject(
    cfg.minio.bucket,
    objectKey,
    bytes,
    bytes.length,
    { "Content-Type": contentType }
  );

  const etag = (putRes as any).etag ?? null;
  const documentId = uuidv4();

  await pool.query("BEGIN");
  try {
    await pool.query(
      `INSERT INTO documents(
        document_id, run_id, source_url, source_domain, source_type,
        content_type, sha256, trust_tier, extraction_state
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, 3, 'pending')`,
      [
        documentId,
        runId,
        url,
        new URL(url).hostname,
        sourceType,
        contentType,
        sha256,
      ]
    );

    await pool.query(
      `INSERT INTO document_objects(
        object_id, document_id, kind, bucket, object_key, version_id, etag, size_bytes, content_type
      ) VALUES ($1, $2, 'raw', $3, $4, $5, $6, $7, $8)`,
      [uuidv4(), documentId, cfg.minio.bucket, objectKey, null, etag, bytes.length, contentType]
    );

    await pool.query("COMMIT");
  } catch (e) {
    await pool.query("ROLLBACK");
    throw e;
  }

  return { documentId, objectKey, etag, sourceType, sha256 };
}

// Helper: Make HTTP request
async function makeHttpRequest(url: string): Promise<{ bytes: Buffer; contentType: string } | null> {
  const headers = {
    "User-Agent": USER_AGENT,
    Accept: "*/*",
  };

  try {
    const response = await fetch(url, { headers });
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status} ${response.statusText}`);
    }

    const bytes = Buffer.from(await response.arrayBuffer());
    const contentType = response.headers.get("content-type") ?? "application/octet-stream";

    return { bytes, contentType };
  } catch (error) {
    console.error("Error making HTTP request:", error);
    return null;
  }
}

// MCP Server setup
const server = new McpServer(
  {
    name: "osint-mcp-server",
    version: "1.0.0",
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

// Register fetch_url tool
server.registerTool(
  "fetch_url",
  {
    description: "Fetch a URL via HTTP GET and store the raw response to MinIO + Postgres",
    inputSchema: {
      runId: z.string().uuid().describe("Run ID (UUID)"),
      url: z.string().url().describe("URL to fetch"),
    },
  },
  async ({ runId, url }) => {
    await emitRunEvent(runId, "TOOL_CALL_STARTED", { tool: "fetch_url", url });

    try {
      // Make HTTP request
      const result = await makeHttpRequest(url);
      if (!result) {
        throw new Error("Failed to fetch URL");
      }

      const { bytes, contentType } = result;

      // Store to MinIO and Postgres
      const { documentId, objectKey, etag, sourceType, sha256 } = await storeDocument(
        runId,
        url,
        bytes,
        contentType
      );

      const output = {
        documentId,
        bucket: cfg.minio.bucket,
        objectKey,
        etag,
        sizeBytes: bytes.length,
        contentType,
        sourceType,
        sha256,
      };

      // Log success
      await logToolCall(runId, "fetch_url", { url }, output, "ok");
      await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "fetch_url", url, ok: true, documentId });

      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(output, null, 2),
          },
        ],
      };
    } catch (error) {
      const errorMsg = (error as Error).message;

      // Log failure
      await logToolCall(runId, "fetch_url", { url }, { error: errorMsg }, "error", errorMsg);
      await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "fetch_url", url, ok: false, error: errorMsg });

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

// Start server with stdio transport
async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("OSINT MCP Server running on stdio");
}

main().catch((error) => {
  console.error("Fatal error in main():", error);
  process.exit(1);
});
