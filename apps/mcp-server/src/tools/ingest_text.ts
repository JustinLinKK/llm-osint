import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import crypto from "node:crypto";
import { v4 as uuidv4 } from "uuid";
import { pool } from "../clients/pg.js";
import { cfg } from "../config.js";
import { emitRunEvent, logToolCall } from "./helpers.js";
import { logger } from "../utils/logger.js";

type Chunk = {
  chunkId: string;
  chunkIndex: number;
  charStart: number;
  charEnd: number;
  text: string;
  vectorId: string | null;
  sectionTitle: string | null;
};

type EvidenceObjectRef = {
  bucket?: string;
  objectKey?: string;
  versionId?: string;
  etag?: string;
  documentId?: string;
};

type EvidenceProps = {
  evidence_bucket?: string;
  evidence_object_key?: string;
  evidence_version_id?: string;
  evidence_etag?: string;
  evidence_document_id?: string;
};

function parseJson<T>(label: string, value?: string): T | undefined {
  if (!value) return undefined;
  try {
    return JSON.parse(value) as T;
  } catch (error) {
    throw new Error(`Invalid ${label} JSON`);
  }
}

function buildEvidenceProps(ref?: EvidenceObjectRef): EvidenceProps | null {
  if (!ref) return null;
  const props: EvidenceProps = {};
  if (ref.bucket) props.evidence_bucket = ref.bucket;
  if (ref.objectKey) props.evidence_object_key = ref.objectKey;
  if (ref.versionId) props.evidence_version_id = ref.versionId;
  if (ref.etag) props.evidence_etag = ref.etag;
  if (ref.documentId) props.evidence_document_id = ref.documentId;
  return Object.keys(props).length ? props : null;
}

function hasEvidenceLink(ref?: EvidenceObjectRef): boolean {
  return Boolean((ref?.bucket && ref?.objectKey) || ref?.documentId);
}

function chunkText(text: string, maxChars = 2000, overlap = 200) {
  const cleaned = text.replace(/\r\n/g, "\n").trim();
  if (!cleaned) return [] as Chunk[];

  const sections = splitSections(cleaned);
  if (sections.length === 0) return [] as Chunk[];

  const chunks: Chunk[] = [];
  let index = 0;

  for (const section of sections) {
    let start = section.contentStart;
    const endLimit = section.contentEnd;
    while (start < endLimit) {
      let end = Math.min(start + maxChars, endLimit);
      if (end < endLimit) {
        const window = cleaned.slice(start, end);
        const lastBreak = Math.max(window.lastIndexOf("\n"), window.lastIndexOf(" "));
        if (lastBreak > 0) {
          end = start + lastBreak;
        }
      }

      const chunkTextValue = cleaned.slice(start, end).trim();
      if (chunkTextValue) {
        chunks.push({
          chunkId: uuidv4(),
          chunkIndex: index,
          charStart: start,
          charEnd: end,
          text: chunkTextValue,
          vectorId: null,
          sectionTitle: section.title,
        });
        index += 1;
      }

      if (end >= endLimit) break;
      start = Math.max(section.contentStart, end - overlap);
    }
  }

  return chunks;
}

function splitSections(text: string) {
  const lines = text.split("\n");
  const sections: Array<{ title: string | null; contentStart: number; contentEnd: number }> = [];
  let offset = 0;
  let currentTitle: string | null = null;
  let currentStart = 0;

  const pushSection = (endOffset: number) => {
    const trimmedStart = Math.min(currentStart, endOffset);
    const trimmedEnd = Math.max(trimmedStart, endOffset);
    if (trimmedEnd > trimmedStart) {
      sections.push({ title: currentTitle, contentStart: trimmedStart, contentEnd: trimmedEnd });
    }
  };

  for (const line of lines) {
    const lineStart = offset;
    const lineEnd = offset + line.length;
    const normalized = line.trim();

    if (isHeadingLine(normalized)) {
      pushSection(lineStart);
      currentTitle = normalized.replace(/^#+\s*/, "").trim() || normalized;
      currentStart = lineEnd + 1;
    }

    offset = lineEnd + 1;
  }

  pushSection(text.length);

  if (sections.length === 0) {
    return [{ title: null, contentStart: 0, contentEnd: text.length }];
  }

  return sections;
}

function isHeadingLine(line: string) {
  if (!line) return false;
  if (/^#{1,6}\s+/.test(line)) return true;
  if (line.length > 80) return false;
  const letters = line.replace(/[^A-Za-z]/g, "");
  if (letters.length >= 4 && letters === letters.toUpperCase()) {
    return true;
  }
  return false;
}

async function ensureQdrantCollection(vectorSize: number) {
  const url = cfg.qdrant.url.replace(/\/$/, "");
  const collection = cfg.qdrant.collection;

  const getRes = await fetch(`${url}/collections/${collection}`);
  if (getRes.ok) return;
  if (getRes.status !== 404) {
    throw new Error(`Qdrant check failed: ${getRes.status}`);
  }

  const createRes = await fetch(`${url}/collections/${collection}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      vectors: {
        size: vectorSize,
        distance: "Cosine",
      },
    }),
  });

  if (!createRes.ok) {
    const errorText = await createRes.text();
    throw new Error(`Qdrant create failed: ${createRes.status} ${errorText}`);
  }
}

async function embedTexts(texts: string[]) {
  if (!cfg.openrouter.apiKey) {
    throw new Error("OPENROUTER_API_KEY not set");
  }

  const response = await fetch("https://openrouter.ai/api/v1/embeddings", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${cfg.openrouter.apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: cfg.openrouter.embedModel,
      input: texts,
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`OpenRouter embeddings failed: ${response.status} ${errorText}`);
  }

  const data = await response.json();
  return (data.data || []).map((item: { embedding: number[] }) => item.embedding);
}

async function upsertQdrantPoints(points: Array<{ id: string; vector: number[]; payload: Record<string, unknown> }>) {
  const url = cfg.qdrant.url.replace(/\/$/, "");
  const collection = cfg.qdrant.collection;

  const response = await fetch(`${url}/collections/${collection}/points?wait=true`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ points }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Qdrant upsert failed: ${response.status} ${errorText}`);
  }
}

export function registerIngestText(server: McpServer) {
  server.registerTool(
    "ingest_text",
    {
      description:
        "Ingest plain text evidence. Use when you already have text (e.g., LLM-parsed, OCR output, or extracted content). Chunks by section headings, embeds via OpenRouter, upserts to Qdrant, and writes chunks to Postgres. Returns documentId and counts.",
      inputSchema: {
        runId: z.string().uuid().describe("Run ID (UUID)"),
        text: z.string().min(1).describe("Raw text input"),
        sourceUrl: z.string().url().optional().describe("Optional source URL"),
        title: z.string().optional().describe("Optional title"),
        maxChars: z.number().int().min(200).max(10000).optional().describe("Max characters per chunk"),
        overlap: z.number().int().min(0).max(2000).optional().describe("Overlap characters"),
        evidenceJson: z
          .string()
          .optional()
          .describe("JSON string evidence link to MinIO (bucket, objectKey, versionId, etag, documentId)"),
      },
    },
    async ({ runId, text, sourceUrl, title, maxChars, overlap, evidenceJson }) => {
      await emitRunEvent(runId, "TOOL_CALL_STARTED", { tool: "ingest_text" });
      logger.info("ingest_text started", { runId, sourceUrl: sourceUrl ?? null });

      try {
        const warnings: string[] = [];
        const evidenceRef = parseJson<EvidenceObjectRef>("evidence", evidenceJson);
        if (!hasEvidenceLink(evidenceRef)) {
          warnings.push("Missing evidence reference for vector chunks");
        }
        const evidenceProps = buildEvidenceProps(evidenceRef);

        const sha256 = crypto.createHash("sha256").update(text).digest("hex");
        const sourceDomain = sourceUrl ? new URL(sourceUrl).hostname : null;

        let documentId: string | null = null;
        const existing = await pool.query(
          "SELECT document_id FROM documents WHERE run_id = $1 AND sha256 = $2",
          [runId, sha256]
        );
        if (existing.rows[0]?.document_id) {
          documentId = existing.rows[0].document_id;
        } else {
          documentId = uuidv4();
          await pool.query(
            `INSERT INTO documents(
              document_id, run_id, source_url, source_domain, source_type,
              content_type, sha256, trust_tier, extraction_state, title
            ) VALUES ($1, $2, $3, $4, 'text', 'text/plain', $5, 3, 'parsed', $6)`,
            [documentId, runId, sourceUrl ?? null, sourceDomain, sha256, title ?? null]
          );
        }

        const existingChunks = await pool.query(
          `SELECT chunk_id, chunk_index, char_start, char_end, text, vector_id
           FROM chunks
           WHERE document_id = $1
           ORDER BY chunk_index ASC`,
          [documentId]
        );

        let chunks: Chunk[] = existingChunks.rows.map((row) => ({
          chunkId: row.chunk_id as string,
          chunkIndex: row.chunk_index as number,
          charStart: row.char_start as number,
          charEnd: row.char_end as number,
          text: row.text as string,
          vectorId: row.vector_id as string | null,
          sectionTitle: null,
        }));

        if (chunks.length === 0) {
          chunks = chunkText(text, maxChars ?? 2000, overlap ?? 200);

          if (chunks.length === 0) {
            throw new Error("No chunks produced from text");
          }

          await pool.query("BEGIN");
          try {
            for (const chunk of chunks) {
              await pool.query(
                `INSERT INTO chunks(
                  chunk_id, document_id, kind, chunk_index, char_start, char_end, text,
                  evidence_bucket, evidence_object_key, evidence_version_id, evidence_etag, evidence_document_id
                ) VALUES ($1, $2, 'body', $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (document_id, kind, chunk_index) DO NOTHING`,
                [
                  chunk.chunkId,
                  documentId,
                  chunk.chunkIndex,
                  chunk.charStart,
                  chunk.charEnd,
                  chunk.text,
                  evidenceProps?.evidence_bucket ?? null,
                  evidenceProps?.evidence_object_key ?? null,
                  evidenceProps?.evidence_version_id ?? null,
                  evidenceProps?.evidence_etag ?? null,
                  evidenceProps?.evidence_document_id ?? null,
                ]
              );
            }
            await pool.query("COMMIT");
          } catch (err) {
            await pool.query("ROLLBACK");
            throw err;
          }
        } else if (evidenceProps) {
          await pool.query(
            `UPDATE chunks
             SET evidence_bucket = $2,
                 evidence_object_key = $3,
                 evidence_version_id = $4,
                 evidence_etag = $5,
                 evidence_document_id = $6
             WHERE document_id = $1`,
            [
              documentId,
              evidenceProps.evidence_bucket ?? null,
              evidenceProps.evidence_object_key ?? null,
              evidenceProps.evidence_version_id ?? null,
              evidenceProps.evidence_etag ?? null,
              evidenceProps.evidence_document_id ?? null,
            ]
          );
        }

        const chunksToEmbed = chunks.filter((chunk) => !chunk.vectorId);
        let embeddings: number[][] = [];
        if (chunksToEmbed.length > 0) {
          const texts = chunksToEmbed.map((chunk) => chunk.text);
          embeddings = await embedTexts(texts);
          if (!embeddings.length) {
            throw new Error("No embeddings returned");
          }

          await ensureQdrantCollection(embeddings[0].length);

          const points = chunksToEmbed.map((chunk, index) => ({
            id: chunk.chunkId,
            vector: embeddings[index],
            payload: {
              run_id: runId,
              document_id: documentId,
              chunk_id: chunk.chunkId,
              chunk_index: chunk.chunkIndex,
              char_start: chunk.charStart,
              char_end: chunk.charEnd,
              source_type: "text",
              content_type: "text/plain",
              title: title ?? null,
              section_title: chunk.sectionTitle,
              evidence_bucket: evidenceProps?.evidence_bucket ?? null,
              evidence_object_key: evidenceProps?.evidence_object_key ?? null,
              evidence_version_id: evidenceProps?.evidence_version_id ?? null,
              evidence_etag: evidenceProps?.evidence_etag ?? null,
              evidence_document_id: evidenceProps?.evidence_document_id ?? null,
            },
          }));

          await upsertQdrantPoints(points);

          for (const chunk of chunksToEmbed) {
            await pool.query(
              "UPDATE chunks SET vector_id = $1, embedding_model = $2 WHERE chunk_id = $3",
              [chunk.chunkId, cfg.openrouter.embedModel, chunk.chunkId]
            );
          }
        }

        const output = {
          documentId,
          chunkCount: chunks.length,
          vectorCount: embeddings.length,
          collection: cfg.qdrant.collection,
          embeddingModel: cfg.openrouter.embedModel,
          evidenceLinked: hasEvidenceLink(evidenceRef),
          warnings,
        };

        await logToolCall(runId, "ingest_text", { sourceUrl, title, maxChars, overlap, evidence: evidenceRef }, output, "ok");
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "ingest_text", ok: true, documentId });
        logger.info("ingest_text finished", { runId, documentId, chunkCount: chunks.length });

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
        await logToolCall(runId, "ingest_text", { sourceUrl, title, maxChars, overlap }, { error: errorMsg }, "error", errorMsg);
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "ingest_text", ok: false, error: errorMsg });
        logger.error("ingest_text failed", { runId, error: errorMsg });

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
