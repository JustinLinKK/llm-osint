import Fastify from "fastify";
import { cfg } from "./config.js";
import { createRun, ingestRawBytes } from "./services/ingest.js";
import { listRunEvents } from "./services/events.js";
import { pool } from "./clients/pg.js";
import { minio } from "./clients/minio.js";

const app = Fastify({ logger: true });

app.get("/health", async () => ({ ok: true }));

app.post("/runs", async (req, reply) => {
  const body = req.body as { prompt: string; seeds?: any[]; constraints?: Record<string, unknown> };
  if (!body?.prompt) return reply.code(400).send({ error: "prompt required" });
  const runId = await createRun(body.prompt, body.seeds ?? [], body.constraints ?? {});
  return { runId };
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
    `SELECT r.run_id, r.created_at, r.created_by, r.status, r.prompt, r.seeds, r.constraints, r.notes,
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
