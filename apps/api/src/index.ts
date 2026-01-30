import Fastify from "fastify";
import { cfg } from "./config.js";
import { createRun, ingestRawBytes } from "./services/ingest.js";

const app = Fastify({ logger: true });

app.get("/health", async () => ({ ok: true }));

app.post("/runs", async (req, reply) => {
  const body = req.body as { prompt: string; seeds?: any[]; constraints?: Record<string, unknown> };
  if (!body?.prompt) return reply.code(400).send({ error: "prompt required" });
  const runId = await createRun(body.prompt, body.seeds ?? [], body.constraints ?? {});
  return { runId };
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

app.listen({ host: "0.0.0.0", port: cfg.apiPort }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});
