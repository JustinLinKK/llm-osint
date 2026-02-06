import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import crypto from "node:crypto";
import { v4 as uuidv4 } from "uuid";
import { pool } from "../clients/pg.js";
import { minio, ensureBucket } from "../clients/minio.js";
import { cfg } from "../config.js";
import { emitRunEvent, logToolCall } from "./helpers.js";
import { logger } from "../utils/logger.js";

const USER_AGENT = "osint-mcp-bot/1.0";

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

export function registerFetchUrl(server: McpServer) {
  server.registerTool(
    "fetch_url",
    {
      description:
        "Fetch a public URL via HTTP GET. Use when you need raw source bytes for evidence. Stores raw content to MinIO and provenance to Postgres. Returns documentId, objectKey, contentType, sha256.",
      inputSchema: {
        runId: z.string().uuid().describe("Run ID (UUID)"),
        url: z.string().url().describe("URL to fetch"),
      },
    },
    async ({ runId, url }) => {
      await emitRunEvent(runId, "TOOL_CALL_STARTED", { tool: "fetch_url", url });
      logger.info("fetch_url started", { runId, url });

      try {
        const result = await makeHttpRequest(url);
        if (!result) {
          throw new Error("Failed to fetch URL");
        }

        const { bytes, contentType } = result;

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
          versionId: null,
          sizeBytes: bytes.length,
          contentType,
          sourceType,
          sha256,
          evidence: {
            documentId,
            bucket: cfg.minio.bucket,
            objectKey,
            versionId: null,
            etag,
            sizeBytes: bytes.length,
            contentType,
            sha256,
          },
        };

        await logToolCall(runId, "fetch_url", { url }, output, "ok");
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "fetch_url", url, ok: true, documentId });
        logger.info("fetch_url finished", { runId, documentId, bytes: bytes.length });

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

        await logToolCall(runId, "fetch_url", { url }, { error: errorMsg }, "error", errorMsg);
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "fetch_url", url, ok: false, error: errorMsg });
        logger.error("fetch_url failed", { runId, error: errorMsg });

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
