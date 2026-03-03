import Fastify from "fastify";
import neo4j from "neo4j-driver";
import path from "node:path";
import { cfg } from "./config.js";
import { createRun, ingestRawBytes } from "./services/ingest.js";
import { listRunEvents } from "./services/events.js";
import { pool } from "./clients/pg.js";
import { minio } from "./clients/minio.js";
import { neo4jDriver } from "./clients/neo4j.js";

const app = Fastify({ logger: true });

type StoredObjectRef = {
  bucket: string;
  objectKey: string;
};

function parsePositiveInt(raw: unknown, fallback: number, max: number): number {
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) return fallback;
  return Math.min(Math.floor(value), max);
}

function parseNonNegativeInt(raw: unknown, fallback = 0): number {
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0) return fallback;
  return Math.floor(value);
}

function normalizeNeo4jValue(value: unknown): unknown {
  if (value === null || value === undefined) return value;
  if (typeof value === "bigint") return Number(value);
  if (Array.isArray(value)) return value.map((item) => normalizeNeo4jValue(item));
  if (typeof value === "object") {
    if (
      "toNumber" in (value as Record<string, unknown>) &&
      typeof (value as { toNumber?: unknown }).toNumber === "function"
    ) {
      return (value as { toNumber: () => number }).toNumber();
    }
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([k, v]) => [k, normalizeNeo4jValue(v)])
    );
  }
  return value;
}

function pickFirstString(
  props: Record<string, unknown>,
  keys: string[]
): string | null {
  for (const key of keys) {
    const value = props[key];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return null;
}

function truncateGraphText(value: string, maxLength: number): string {
  const normalized = value.replace(/\s+/g, " ").trim();
  if (normalized.length <= maxLength) return normalized;
  return `${normalized.slice(0, Math.max(0, maxLength - 3))}...`;
}

function formatRelationType(type: string): string {
  return type
    .toLowerCase()
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function deriveGraphNodeDisplay(
  labels: string[],
  props: Record<string, unknown>,
  fallbackId: string
): string {
  const preferred = pickFirstString(props, [
    "canonical_name",
    "display_name",
    "displayName",
    "name",
    "title",
    "username",
    "handle",
    "domain",
    "address",
    "email",
    "uri",
    "url",
    "snippet_id",
    "sourceTool"
  ]);
  if (preferred) return truncateGraphText(preferred, 96);

  if (props.type === "Snippet" || labels.includes("Snippet")) {
    const text = pickFirstString(props, ["text", "toolSummary"]);
    if (text) return `Snippet: ${truncateGraphText(text, 84)}`;
  }

  if (props.type === "Article" || labels.includes("Article")) {
    const articleRef = pickFirstString(props, ["uri", "url"]);
    if (articleRef) return truncateGraphText(articleRef, 96);
  }

  const stableKey = pickFirstString(props, [
    "node_id",
    "canonical_name_normalized",
    "snippet_id"
  ]);
  const primaryLabel = pickFirstString(props, ["type", "osint_bucket"]) ?? labels[0] ?? "Entity";
  if (stableKey) return `${primaryLabel}: ${truncateGraphText(stableKey, 72)}`;

  const sourceTool = pickFirstString(props, ["sourceTool"]);
  if (sourceTool) return `${primaryLabel}: ${truncateGraphText(sourceTool, 72)}`;

  return primaryLabel;
}

function buildStage2Markdown(finalReport?: string | null, evidenceAppendix?: string | null): string | null {
  const parts = [finalReport?.trim(), evidenceAppendix?.trim()].filter(
    (value): value is string => Boolean(value)
  );
  return parts.length ? parts.join("\n\n") : null;
}

function sanitizeFilename(value: string, fallback: string): string {
  const normalized = value
    .replace(/[<>:"/\\|?*\x00-\x1f]+/g, "-")
    .replace(/\s+/g, " ")
    .trim();
  return normalized || fallback;
}

function inferFilenameFromObjectKey(objectKey: string, fallbackBase: string): string {
  const basename = path.posix.basename(objectKey);
  return sanitizeFilename(basename || fallbackBase, fallbackBase);
}

async function cleanupRunGraphEvidence(documentIds: string[]) {
  if (!documentIds.length) return;
  const session = neo4jDriver.session();
  try {
    await session.run(
      `MATCH ()-[r]->()
       WHERE r.evidence_document_id IN $documentIds
          OR any(docId IN coalesce(r.evidence_document_ids, []) WHERE docId IN $documentIds)
       WITH r,
            [docId IN coalesce(r.evidence_document_ids, CASE WHEN r.evidence_document_id IS NULL THEN [] ELSE [r.evidence_document_id] END)
             WHERE NOT docId IN $documentIds] AS remainingDocIds
       FOREACH (_ IN CASE WHEN size(remainingDocIds) = 0 THEN [1] ELSE [] END | DELETE r)
       FOREACH (_ IN CASE WHEN size(remainingDocIds) > 0 THEN [1] ELSE [] END |
         SET r.evidence_document_ids = remainingDocIds,
             r.evidence_document_id = head(remainingDocIds)
       )`,
      { documentIds }
    );
    await session.run(
      `MATCH (n)
       WHERE n.evidence_document_id IN $documentIds
          OR any(docId IN coalesce(n.evidence_document_ids, []) WHERE docId IN $documentIds)
       WITH n,
            [docId IN coalesce(n.evidence_document_ids, CASE WHEN n.evidence_document_id IS NULL THEN [] ELSE [n.evidence_document_id] END)
             WHERE NOT docId IN $documentIds] AS remainingDocIds
       OPTIONAL MATCH (n)-[r]-()
       WITH n, remainingDocIds, count(r) AS remainingRelCount
       FOREACH (_ IN CASE WHEN size(remainingDocIds) = 0 AND remainingRelCount = 0 THEN [1] ELSE [] END | DETACH DELETE n)
       FOREACH (_ IN CASE WHEN size(remainingDocIds) > 0 THEN [1] ELSE [] END |
         SET n.evidence_document_ids = remainingDocIds,
             n.evidence_document_id = head(remainingDocIds)
       )
       FOREACH (_ IN CASE WHEN size(remainingDocIds) = 0 AND remainingRelCount > 0 THEN [1] ELSE [] END |
         REMOVE n.evidence_document_ids, n.evidence_document_id
       )`,
      { documentIds }
    );
  } finally {
    await session.close();
  }
}

async function cleanupStoredObjects(objects: StoredObjectRef[]) {
  if (!objects.length) return;
  await Promise.allSettled(
    objects.map((item) =>
      minio.removeObject(item.bucket, item.objectKey).catch(() => undefined)
    )
  );
}

app.get("/health", async () => ({ ok: true }));

app.post("/runs", async (req, reply) => {
  const body = req.body as { prompt: string; seeds?: any[]; constraints?: Record<string, unknown> };
  if (!body?.prompt) return reply.code(400).send({ error: "prompt required" });
  const runId = await createRun(body.prompt, body.seeds ?? [], body.constraints ?? {});
  return { runId };
});

app.get("/runs", async (req) => {
  const query = req.query as { limit?: string; offset?: string; status?: string };
  const limit = parsePositiveInt(query.limit, 30, 100);
  const offset = parseNonNegativeInt(query.offset, 0);

  const params: unknown[] = [];
  let whereSql = "";
  if (query.status?.trim()) {
    params.push(query.status.trim());
    whereSql = `WHERE r.status = $${params.length}`;
  }

  params.push(limit);
  const limitPos = params.length;
  params.push(offset);
  const offsetPos = params.length;

  const { rows } = await pool.query(
    `SELECT r.run_id, r.title, r.created_at, r.status, r.prompt,
            COALESCE(rr.run_id::text, rep.report_id::text) AS report_id,
            COALESCE(rr.status, rep.status) AS report_status,
            COALESCE(rr.updated_at, rep.created_at) AS report_created_at
     FROM runs r
     LEFT JOIN LATERAL (
       SELECT run_id, status, updated_at
       FROM report_runs
       WHERE run_id = r.run_id
       ORDER BY updated_at DESC
       LIMIT 1
     ) rr ON true
     LEFT JOIN LATERAL (
       SELECT report_id, status, created_at
       FROM reports
       WHERE run_id = r.run_id
       ORDER BY created_at DESC
       LIMIT 1
     ) rep ON true
     ${whereSql}
     ORDER BY r.created_at DESC
     LIMIT $${limitPos} OFFSET $${offsetPos}`,
    params
  );

  const countRes = await pool.query(
    `SELECT COUNT(*)::int AS total FROM runs r ${whereSql}`,
    query.status?.trim() ? [query.status.trim()] : []
  );

  return {
    items: rows.map((row) => ({
      runId: row.run_id,
      title: row.title,
      createdAt: row.created_at,
      status: row.status,
      prompt: row.prompt,
      latestReport: row.report_id
        ? {
            reportId: row.report_id,
            status: row.report_status,
            createdAt: row.report_created_at
          }
        : null
    })),
    page: {
      limit,
      offset,
      total: countRes.rows[0]?.total ?? 0
    }
  };
});

app.get("/runs/:runId/events", async (req, reply) => {
  const { runId } = req.params as { runId: string };

  reply.raw.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive"
  });

  reply.raw.write("retry: 1000\n\n");

  let lastTs: Date | null = null;

  const sendEvents = async () => {
    const events = await listRunEvents(runId, lastTs);
    for (const event of events) {
      lastTs = new Date(event.ts);
      reply.raw.write(`event: run_event\nid: ${event.event_id}\ndata: ${JSON.stringify(event)}\n\n`);
    }
  };

  const keepAlive = () => {
    reply.raw.write(": ping\n\n");
  };

  const intervalId = setInterval(() => {
    void sendEvents();
  }, 1000);

  const keepAliveId = setInterval(keepAlive, 15000);

  req.raw.on("close", () => {
    clearInterval(intervalId);
    clearInterval(keepAliveId);
  });

  await sendEvents();
});

// Minimal test ingest: upload arbitrary text as a "document"
app.post("/runs/:runId/ingest-text", async (req, reply) => {
  const { runId } = req.params as { runId: string };
  const body = req.body as { text: string; sourceUrl?: string; contentType?: string; title?: string };

  if (!body?.text) return reply.code(400).send({ error: "text required" });

  const res = await ingestRawBytes({
    runId,
    sourceType: "text",
    sourceUrl: body.sourceUrl,
    contentType: body.contentType ?? "text/plain",
    bytes: Buffer.from(body.text, "utf-8"),
    title: body.title
  });

  return res;
});

app.get("/runs/:runId", async (req, reply) => {
  const { runId } = req.params as { runId: string };

  const { rows } = await pool.query(
    `SELECT r.run_id, r.created_at, r.created_by, r.status, r.title, r.prompt, r.seeds, r.constraints, r.notes,
            COALESCE(rr.run_id::text, rep.report_id::text) AS report_id,
            COALESCE(rr.status, rep.status) AS report_status,
            COALESCE(rr.updated_at, rep.created_at) AS report_created_at,
            rep.markdown_bucket, rep.markdown_object_key, rep.markdown_version_id,
            rep.json_bucket, rep.json_object_key, rep.json_version_id
     FROM runs r
     LEFT JOIN LATERAL (
       SELECT run_id, status, updated_at
       FROM report_runs
       WHERE run_id = r.run_id
       ORDER BY updated_at DESC
       LIMIT 1
     ) rr ON true
     LEFT JOIN LATERAL (
       SELECT * FROM reports
       WHERE run_id = r.run_id
       ORDER BY created_at DESC
       LIMIT 1
     ) rep ON true
     WHERE r.run_id = $1`,
    [runId]
  );

  if (rows.length === 0) return reply.code(404).send({ error: "run not found" });

  const row = rows[0];
  return {
    run: {
      runId: row.run_id,
      createdAt: row.created_at,
      createdBy: row.created_by,
      status: row.status,
      title: row.title,
      prompt: row.prompt,
      seeds: row.seeds,
      constraints: row.constraints,
      notes: row.notes
    },
    latestReport: row.report_id
      ? {
          reportId: row.report_id,
          status: row.report_status,
          createdAt: row.report_created_at,
          markdown: {
            bucket: row.markdown_bucket,
            objectKey: row.markdown_object_key,
            versionId: row.markdown_version_id
          },
          json: {
            bucket: row.json_bucket,
            objectKey: row.json_object_key,
            versionId: row.json_version_id
          }
        }
      : null
  };
});

app.patch("/runs/:runId/title", async (req, reply) => {
  const { runId } = req.params as { runId: string };
  const body = req.body as { title?: string };
  const title = (body?.title ?? "").trim();

  if (!title) return reply.code(400).send({ error: "title required" });
  if (title.length > 160) return reply.code(400).send({ error: "title too long (max 160)" });

  const { rowCount } = await pool.query(`UPDATE runs SET title = $2 WHERE run_id = $1`, [runId, title]);
  if (!rowCount) return reply.code(404).send({ error: "run not found" });

  return { ok: true, runId, title };
});

app.delete("/runs/:runId", async (req, reply) => {
  const { runId } = req.params as { runId: string };
  const client = await pool.connect();

  let documentIds: string[] = [];
  let objects: StoredObjectRef[] = [];

  try {
    await client.query("BEGIN");

    const docsRes = await client.query<{ document_id: string }>(
      `SELECT document_id::text
       FROM documents
       WHERE run_id = $1`,
      [runId]
    );
    documentIds = docsRes.rows.map((row) => row.document_id);

    const objectRes = await client.query<{ bucket: string | null; object_key: string | null }>(
      `SELECT bucket, object_key
       FROM document_objects o
       JOIN documents d ON d.document_id = o.document_id
       WHERE d.run_id = $1
       UNION
       SELECT bucket, object_key
       FROM artifacts
       WHERE run_id = $1
       UNION
       SELECT markdown_bucket AS bucket, markdown_object_key AS object_key
       FROM reports
       WHERE run_id = $1
       UNION
       SELECT json_bucket AS bucket, json_object_key AS object_key
       FROM reports
       WHERE run_id = $1`,
      [runId]
    );
    objects = objectRes.rows
      .filter((row) => row.bucket && row.object_key)
      .map((row) => ({ bucket: row.bucket as string, objectKey: row.object_key as string }));

    const deleteRes = await client.query(`DELETE FROM runs WHERE run_id = $1`, [runId]);
    if (!deleteRes.rowCount) {
      await client.query("ROLLBACK");
      return reply.code(404).send({ error: "run not found" });
    }

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }

  await Promise.allSettled([cleanupRunGraphEvidence(documentIds), cleanupStoredObjects(objects)]);
  return { ok: true, runId };
});

app.get("/runs/:runId/files", async (req, reply) => {
  const { runId } = req.params as { runId: string };
  const query = req.query as { limit?: string; offset?: string };
  const limit = parsePositiveInt(query.limit, 50, 200);
  const offset = parseNonNegativeInt(query.offset, 0);

  const runExists = await pool.query(`SELECT 1 FROM runs WHERE run_id = $1`, [runId]);
  if (!runExists.rowCount) return reply.code(404).send({ error: "run not found" });

  const { rows } = await pool.query(
    `SELECT d.document_id,
            d.run_id,
            d.source_url,
            d.source_domain,
            d.source_type,
            d.retrieved_at,
            d.content_type,
            d.title,
            d.extraction_state,
            o.object_id,
            o.kind AS object_kind,
            o.bucket,
            o.object_key,
            o.version_id,
            o.etag,
            o.size_bytes,
            o.content_type AS object_content_type,
            o.created_at AS object_created_at
     FROM documents d
     LEFT JOIN document_objects o
       ON o.document_id = d.document_id
     WHERE d.run_id = $1
     ORDER BY d.retrieved_at DESC, o.created_at DESC
     LIMIT $2 OFFSET $3`,
    [runId, limit, offset]
  );

  const countRes = await pool.query(`SELECT COUNT(*)::int AS total FROM documents WHERE run_id = $1`, [runId]);

  return {
    runId,
    items: rows.map((row) => ({
      documentId: row.document_id,
      sourceUrl: row.source_url,
      sourceDomain: row.source_domain,
      sourceType: row.source_type,
      retrievedAt: row.retrieved_at,
      title: row.title,
      contentType: row.content_type,
      extractionState: row.extraction_state,
      object: row.object_id
        ? {
            objectId: row.object_id,
            kind: row.object_kind,
            bucket: row.bucket,
            objectKey: row.object_key,
            versionId: row.version_id,
            etag: row.etag,
            sizeBytes: row.size_bytes,
            contentType: row.object_content_type,
            createdAt: row.object_created_at
          }
        : null
    })),
    page: {
      limit,
      offset,
      total: countRes.rows[0]?.total ?? 0
    }
  };
});

app.get("/runs/:runId/files/:documentId/download", async (req, reply) => {
  const { runId, documentId } = req.params as { runId: string; documentId: string };

  const { rows } = await pool.query(
    `SELECT d.document_id,
            d.title,
            d.content_type,
            d.source_url,
            o.bucket,
            o.object_key,
            o.version_id,
            o.content_type AS object_content_type
     FROM documents d
     JOIN document_objects o
       ON o.document_id = d.document_id
     WHERE d.run_id = $1
       AND d.document_id = $2
     ORDER BY CASE WHEN o.kind = 'raw' THEN 0 ELSE 1 END, o.created_at DESC
     LIMIT 1`,
    [runId, documentId]
  );

  if (!rows.length) return reply.code(404).send({ error: "file not found" });

  const row = rows[0];
  const stream = await minio.getObject(row.bucket, row.object_key, row.version_id ? { versionId: row.version_id } : {});
  const fallbackBase = sanitizeFilename(`document-${documentId}`, `document-${documentId}`);
  const filename =
    sanitizeFilename(row.title ?? "", "") ||
    inferFilenameFromObjectKey(row.object_key, fallbackBase);

  reply.header("Content-Type", row.object_content_type || row.content_type || "application/octet-stream");
  reply.header("Content-Disposition", `attachment; filename="${filename}"`);
  return reply.send(stream);
});

app.get("/runs/:runId/graph", async (req, reply) => {
  const { runId } = req.params as { runId: string };
  const query = req.query as {
    nodeLimit?: string;
    nodeOffset?: string;
    edgeLimit?: string;
    edgeOffset?: string;
  };

  const nodeLimit = parsePositiveInt(query.nodeLimit, 80, 400);
  const nodeOffset = parseNonNegativeInt(query.nodeOffset, 0);
  const edgeLimit = parsePositiveInt(query.edgeLimit, 120, 600);
  const edgeOffset = parseNonNegativeInt(query.edgeOffset, 0);
  const nodeLimitInt = neo4j.int(nodeLimit);
  const nodeOffsetInt = neo4j.int(nodeOffset);
  const edgeLimitInt = neo4j.int(edgeLimit);
  const edgeOffsetInt = neo4j.int(edgeOffset);

  const docRes = await pool.query(`SELECT document_id::text FROM documents WHERE run_id = $1`, [runId]);
  const documentIds: string[] = docRes.rows.map((row) => row.document_id);
  if (!documentIds.length) {
    return {
      runId,
      nodes: [],
      edges: [],
      page: {
        nodes: { limit: nodeLimit, offset: nodeOffset, total: 0 },
        edges: { limit: edgeLimit, offset: edgeOffset, total: 0 }
      }
    };
  }

  const session = neo4jDriver.session();
  try {
    const edgeTotalResult = await session.run(
      `MATCH (a)-[r]->(b)
       WHERE r.evidence_document_id IN $documentIds
          OR any(docId IN coalesce(r.evidence_document_ids, []) WHERE docId IN $documentIds)
          OR a.evidence_document_id IN $documentIds
          OR any(docId IN coalesce(a.evidence_document_ids, []) WHERE docId IN $documentIds)
          OR b.evidence_document_id IN $documentIds
          OR any(docId IN coalesce(b.evidence_document_ids, []) WHERE docId IN $documentIds)
       RETURN count(DISTINCT r) AS total`,
      { documentIds }
    );
    const totalEdges = Number(
      normalizeNeo4jValue(edgeTotalResult.records[0]?.get("total")) ?? 0
    );

    const edgeResult = await session.run(
      `MATCH (a)-[r]->(b)
       WHERE r.evidence_document_id IN $documentIds
          OR any(docId IN coalesce(r.evidence_document_ids, []) WHERE docId IN $documentIds)
          OR a.evidence_document_id IN $documentIds
          OR any(docId IN coalesce(a.evidence_document_ids, []) WHERE docId IN $documentIds)
          OR b.evidence_document_id IN $documentIds
          OR any(docId IN coalesce(b.evidence_document_ids, []) WHERE docId IN $documentIds)
       WITH DISTINCT a, r, b
       ORDER BY coalesce(r.rel_type, type(r)), elementId(r)
      SKIP $edgeOffset
      LIMIT $edgeLimit
      RETURN elementId(r) AS id,
              elementId(a) AS source,
              elementId(b) AS target,
              coalesce(r.rel_type, type(r)) AS type,
              properties(r) AS props`,
      { documentIds, edgeLimit: edgeLimitInt, edgeOffset: edgeOffsetInt }
    );

    const edges = edgeResult.records.map((record) => {
      const type = String(record.get("type"));
      return {
        id: String(record.get("id")),
        source: String(record.get("source")),
        target: String(record.get("target")),
        type,
        display: formatRelationType(type),
        properties: normalizeNeo4jValue(record.get("props")) as Record<string, unknown>
      };
    });

    const nodeTotalResult = await session.run(
      `MATCH (n)
       WHERE n.evidence_document_id IN $documentIds
          OR any(docId IN coalesce(n.evidence_document_ids, []) WHERE docId IN $documentIds)
          OR EXISTS {
            MATCH (n)-[r]-(m)
            WHERE r.evidence_document_id IN $documentIds
               OR any(docId IN coalesce(r.evidence_document_ids, []) WHERE docId IN $documentIds)
               OR m.evidence_document_id IN $documentIds
               OR any(docId IN coalesce(m.evidence_document_ids, []) WHERE docId IN $documentIds)
          }
       RETURN count(DISTINCT n) AS total`,
      { documentIds }
    );
    const totalNodes = Number(
      normalizeNeo4jValue(nodeTotalResult.records[0]?.get("total")) ?? 0
    );

    const nodeResult = await session.run(
      `MATCH (n)
       WHERE n.evidence_document_id IN $documentIds
          OR any(docId IN coalesce(n.evidence_document_ids, []) WHERE docId IN $documentIds)
          OR EXISTS {
            MATCH (n)-[r]-(m)
            WHERE r.evidence_document_id IN $documentIds
               OR any(docId IN coalesce(r.evidence_document_ids, []) WHERE docId IN $documentIds)
               OR m.evidence_document_id IN $documentIds
               OR any(docId IN coalesce(m.evidence_document_ids, []) WHERE docId IN $documentIds)
          }
       WITH DISTINCT n
       ORDER BY coalesce(n.canonical_name, n.node_id, elementId(n))
       SKIP $nodeOffset
       LIMIT $nodeLimit
       RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props`,
      { documentIds, nodeLimit: nodeLimitInt, nodeOffset: nodeOffsetInt }
    );

    const nodeRecords = nodeResult.records.map((record) => {
      const props = normalizeNeo4jValue(record.get("props")) as Record<string, unknown>;
      const labels = normalizeNeo4jValue(record.get("labels")) as string[];
      const id = String(record.get("id"));
      return {
        id,
        labels,
        properties: props,
        display: deriveGraphNodeDisplay(labels, props, id)
      };
    });

    const nodeIds = new Set(nodeRecords.map((node) => node.id));
    const missingEndpointIds = Array.from(
      new Set(
        edges.flatMap((edge) => [edge.source, edge.target]).filter((nodeId) => nodeId && !nodeIds.has(nodeId))
      )
    );

    if (missingEndpointIds.length > 0) {
      const missingNodeResult = await session.run(
        `MATCH (n)
         WHERE elementId(n) IN $nodeIds
         RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props`,
        { nodeIds: missingEndpointIds }
      );

      for (const record of missingNodeResult.records) {
        const props = normalizeNeo4jValue(record.get("props")) as Record<string, unknown>;
        const labels = normalizeNeo4jValue(record.get("labels")) as string[];
        const id = String(record.get("id"));
        if (nodeIds.has(id)) continue;
        nodeIds.add(id);
        nodeRecords.push({
          id,
          labels,
          properties: props,
          display: deriveGraphNodeDisplay(labels, props, id)
        });
      }
    }

    return {
      runId,
      nodes: nodeRecords,
      edges,
      page: {
        nodes: { limit: nodeLimit, offset: nodeOffset, total: totalNodes },
        edges: { limit: edgeLimit, offset: edgeOffset, total: totalEdges }
      }
    };
  } finally {
    await session.close();
  }
});

app.get("/runs/:runId/report", async (req, reply) => {
  const { runId } = req.params as { runId: string };

  const stage2Res = await pool.query(
    `SELECT run_id, report_type, status, refine_round, quality_ok, final_report, evidence_appendix, created_at, updated_at
     FROM report_runs
     WHERE run_id = $1
     ORDER BY updated_at DESC
     LIMIT 1`,
    [runId]
  );

  if (stage2Res.rows.length > 0) {
    const report = stage2Res.rows[0];
    const [sectionsRes, claimsRes, evidenceRes] = await Promise.all([
      pool.query(
        `SELECT section_id, section_order, title, content, citation_keys, created_at
         FROM section_drafts
         WHERE run_id = $1
         ORDER BY section_order ASC, created_at ASC`,
        [runId]
      ),
      pool.query(
        `SELECT claim_id, section_id, claim_text, confidence, impact, evidence_keys, conflict_flags, created_at
         FROM claim_ledger
         WHERE run_id = $1
         ORDER BY created_at ASC, section_id ASC, claim_id ASC`,
        [runId]
      ),
      pool.query(
        `SELECT citation_key, section_id, document_id, snippet, source_url, score, object_ref, created_at
         FROM evidence_refs
         WHERE run_id = $1
         ORDER BY created_at ASC, section_id ASC, citation_key ASC`,
        [runId]
      )
    ]);

    return {
      reportId: report.run_id,
      runId: report.run_id,
      status: report.status,
      createdAt: report.updated_at ?? report.created_at,
      markdown: buildStage2Markdown(report.final_report, report.evidence_appendix),
      json: {
        reportType: report.report_type,
        qualityOk: report.quality_ok,
        refineRound: report.refine_round,
        finalReport: report.final_report,
        evidenceAppendix: report.evidence_appendix,
        sectionDrafts: sectionsRes.rows.map((row) => ({
          sectionId: row.section_id,
          sectionOrder: row.section_order,
          title: row.title,
          content: row.content,
          citationKeys: row.citation_keys ?? [],
          createdAt: row.created_at
        })),
        claimLedger: claimsRes.rows.map((row) => ({
          claimId: row.claim_id,
          sectionId: row.section_id,
          text: row.claim_text,
          confidence: row.confidence,
          impact: row.impact,
          evidenceKeys: row.evidence_keys ?? [],
          conflictFlags: row.conflict_flags ?? [],
          createdAt: row.created_at
        })),
        evidenceRefs: evidenceRes.rows.map((row) => ({
          citationKey: row.citation_key,
          sectionId: row.section_id,
          documentId: row.document_id,
          snippet: row.snippet,
          sourceUrl: row.source_url,
          score: row.score,
          objectRef: row.object_ref ?? {},
          createdAt: row.created_at
        }))
      },
      citations: evidenceRes.rows.map((row) => ({
        citationKey: row.citation_key,
        sectionId: row.section_id,
        sourceUrl: row.source_url,
        documentId: row.document_id
      }))
    };
  }

  const { rows } = await pool.query(
    `SELECT * FROM reports WHERE run_id = $1 ORDER BY created_at DESC LIMIT 1`,
    [runId]
  );

  if (rows.length === 0) return reply.code(404).send({ error: "report not found" });

  const report = rows[0];

  const readObject = async (bucket?: string, objectKey?: string) => {
    if (!bucket || !objectKey) return null;
    const stream = await minio.getObject(bucket, objectKey);
    const chunks: Buffer[] = [];
    for await (const chunk of stream) {
      chunks.push(chunk as Buffer);
    }
    return Buffer.concat(chunks).toString("utf-8");
  };

  const markdown = await readObject(report.markdown_bucket, report.markdown_object_key);
  const jsonText = await readObject(report.json_bucket, report.json_object_key);
  let json: any = null;
  if (jsonText) {
    try {
      json = JSON.parse(jsonText);
    } catch {
      json = null;
    }
  }

  return {
    reportId: report.report_id,
    runId: report.run_id,
    status: report.status,
    createdAt: report.created_at,
    markdown,
    json,
    citations: json?.citations ?? null
  };
});

app.listen({ host: "0.0.0.0", port: cfg.apiPort }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});
