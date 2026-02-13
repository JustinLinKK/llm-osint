import Fastify from "fastify";
import { cfg } from "./config.js";
import { createRun, ingestRawBytes } from "./services/ingest.js";
import { listRunEvents } from "./services/events.js";
import { pool } from "./clients/pg.js";
import { minio } from "./clients/minio.js";
import { neo4jDriver } from "./clients/neo4j.js";

const app = Fastify({ logger: true });

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
            rep.report_id, rep.status AS report_status, rep.created_at AS report_created_at
     FROM runs r
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
            rep.report_id, rep.status AS report_status, rep.created_at AS report_created_at,
            rep.markdown_bucket, rep.markdown_object_key, rep.markdown_version_id,
            rep.json_bucket, rep.json_object_key, rep.json_version_id
     FROM runs r
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
    const nodeTotalResult = await session.run(
      `MATCH (n)
       WHERE n.evidence_document_id IN $documentIds
       RETURN count(DISTINCT n) AS total`,
      { documentIds }
    );
    const totalNodes = Number(
      normalizeNeo4jValue(nodeTotalResult.records[0]?.get("total")) ?? 0
    );

    const nodeResult = await session.run(
      `MATCH (n)
       WHERE n.evidence_document_id IN $documentIds
       WITH DISTINCT n
       ORDER BY coalesce(n.name, n.uri, n.address, n.email, elementId(n))
       SKIP $nodeOffset
       LIMIT $nodeLimit
       RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props`,
      { documentIds, nodeLimit, nodeOffset }
    );

    const nodes = nodeResult.records.map((record) => {
      const props = normalizeNeo4jValue(record.get("props")) as Record<string, unknown>;
      return {
        id: String(record.get("id")),
        labels: normalizeNeo4jValue(record.get("labels")) as string[],
        properties: props,
        display:
          (props.name as string | undefined) ??
          (props.uri as string | undefined) ??
          (props.address as string | undefined) ??
          (props.email as string | undefined) ??
          String(record.get("id"))
      };
    });

    const edgeTotalResult = await session.run(
      `MATCH (a)-[r]->(b)
       WHERE r.evidence_document_id IN $documentIds
          OR a.evidence_document_id IN $documentIds
          OR b.evidence_document_id IN $documentIds
       RETURN count(DISTINCT r) AS total`,
      { documentIds }
    );
    const totalEdges = Number(
      normalizeNeo4jValue(edgeTotalResult.records[0]?.get("total")) ?? 0
    );

    const edgeResult = await session.run(
      `MATCH (a)-[r]->(b)
       WHERE r.evidence_document_id IN $documentIds
          OR a.evidence_document_id IN $documentIds
          OR b.evidence_document_id IN $documentIds
       WITH DISTINCT a, r, b
       ORDER BY type(r), elementId(r)
       SKIP $edgeOffset
       LIMIT $edgeLimit
       RETURN elementId(r) AS id,
              elementId(a) AS source,
              elementId(b) AS target,
              type(r) AS type,
              properties(r) AS props`,
      { documentIds, edgeLimit, edgeOffset }
    );

    const edges = edgeResult.records.map((record) => ({
      id: String(record.get("id")),
      source: String(record.get("source")),
      target: String(record.get("target")),
      type: String(record.get("type")),
      properties: normalizeNeo4jValue(record.get("props")) as Record<string, unknown>
    }));

    return {
      runId,
      nodes,
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
