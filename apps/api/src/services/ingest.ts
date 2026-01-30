import crypto from "node:crypto";
import { v4 as uuidv4 } from "uuid";
import { pool } from "../clients/pg.js";
import { minio, ensureBucket } from "../clients/minio.js";
import { cfg } from "../config.js";
import { emitEvent } from "./events.js";

export type Seed = { type: string; value: string };

export async function createRun(prompt: string, seeds: Seed[] = [], constraints: Record<string, unknown> = {}) {
  const runId = uuidv4();
  await pool.query(
    `INSERT INTO runs(run_id, prompt, seeds, constraints, status)
     VALUES ($1, $2, $3::jsonb, $4::jsonb, 'created')`,
    [runId, prompt, JSON.stringify(seeds), JSON.stringify(constraints)]
  );
  await emitEvent(runId, "RUN_CREATED", { prompt, seeds, constraints });
  return runId;
}

export async function ingestRawBytes(params: {
  runId: string;
  sourceType: "html" | "pdf" | "image" | "audio" | "video" | "text" | "json";
  sourceUrl?: string;
  contentType?: string;
  bytes: Buffer;
  title?: string;
  trustTier?: number;
}) {
  const {
    runId, sourceType, sourceUrl, contentType,
    bytes, title, trustTier = 3
  } = params;

  await emitEvent(runId, "TOOL_CALL_STARTED", {
    tool: "upload_user_text",
    sourceType,
    sourceUrl: sourceUrl ?? null,
    contentType: contentType ?? null,
    sizeBytes: bytes.length
  });

  // 1) Hash for dedupe
  const sha256 = crypto.createHash("sha256").update(bytes).digest("hex");

  // 2) Store in MinIO (versioning will create version_id)
  await ensureBucket(cfg.minio.bucket);

  const ext = sourceType; // simplistic; you can map types to extensions later
  const objectKey = `runs/${runId}/raw/${sourceType}/${sha256}.${ext}`;

  const putRes = await minio.putObject(
    cfg.minio.bucket,
    objectKey,
    bytes,
    bytes.length,
    { "Content-Type": contentType ?? "application/octet-stream" }
  );

  // putRes typically has etag; versionId may be exposed via headers in some SDK flows
  // We'll treat version_id as optional and fetch it if needed later.
  const etag = (putRes as any).etag ?? null;

  // 3) Insert document + object pointers
  const documentId = uuidv4();

  await pool.query("BEGIN");
  try {
    await pool.query(
      `INSERT INTO documents(
        document_id, run_id, source_url, source_domain, source_type,
        content_type, sha256, trust_tier, title, extraction_state
      ) VALUES (
        $1, $2, $3, $4, $5,
        $6, $7, $8, $9, 'pending'
      )`,
      [
        documentId,
        runId,
        sourceUrl ?? null,
        sourceUrl ? new URL(sourceUrl).hostname : null,
        sourceType,
        contentType ?? null,
        sha256,
        trustTier,
        title ?? null
      ]
    );

    await pool.query(
      `INSERT INTO document_objects(
        object_id, document_id, kind, bucket, object_key, version_id, etag, size_bytes, content_type
      ) VALUES (
        $1, $2, 'raw', $3, $4, $5, $6, $7, $8
      )`,
      [
        uuidv4(),
        documentId,
        cfg.minio.bucket,
        objectKey,
        null,          // version_id: nullable; you can fill this later if needed
        etag,
        bytes.length,
        contentType ?? null
      ]
    );

    await pool.query("COMMIT");
  } catch (e) {
    await pool.query("ROLLBACK");
    await emitEvent(runId, "TOOL_CALL_FINISHED", {
      tool: "upload_user_text",
      ok: false,
      error: (e as Error).message
    });
    throw e;
  }

  await emitEvent(runId, "TOOL_CALL_FINISHED", {
    tool: "upload_user_text",
    ok: true,
    documentId,
    bucket: cfg.minio.bucket,
    objectKey,
    etag
  });

  return { documentId, sha256, bucket: cfg.minio.bucket, objectKey, etag };
}
