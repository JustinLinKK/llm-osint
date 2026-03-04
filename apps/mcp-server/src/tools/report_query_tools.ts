import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import neo4j from "neo4j-driver";
import { pool } from "../clients/pg.js";
import { neo4jDriver } from "../clients/neo4j.js";
import { cfg } from "../config.js";
import { embedQueryText } from "../embeddings.js";
import { ensureQdrantCollection } from "../qdrant.js";
import { logToolCall } from "./helpers.js";
import { logger } from "../utils/logger.js";

type QdrantHit = {
  id?: string;
  score?: number;
  payload?: Record<string, unknown>;
};

async function qdrantSearch(
  vector: number[],
  limit: number,
  runId?: string
): Promise<QdrantHit[]> {
  const url = cfg.qdrant.url.replace(/\/$/, "");
  const collection = cfg.qdrant.collection;
  const filter = runId
    ? {
        must: [{ key: "run_id", match: { value: runId } }],
      }
    : undefined;

  const response = await fetch(`${url}/collections/${collection}/points/search`, {
    method: "POST",
    signal: AbortSignal.timeout(15000),
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      vector,
      limit,
      with_payload: true,
      filter,
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Qdrant search failed: ${response.status} ${errorText}`);
  }

  const data = await response.json();
  const rows = data?.result;
  if (!Array.isArray(rows)) {
    return [];
  }
  return rows as QdrantHit[];
}

export function registerReportQueryTools(server: McpServer) {
  registerVectorSearch(server);
  registerVectorGetDocument(server);
  registerGraphGetEntity(server);
  registerGraphNeighbors(server);
  registerGraphSearchEntities(server);
}

function registerVectorSearch(server: McpServer) {
  server.registerTool(
    "vector_search",
    {
      description:
        "Search vector chunks by semantic query. Returns ranked snippets with document and object evidence refs.",
      inputSchema: {
        runId: z.string().uuid().optional().describe("Optional run scope filter"),
        query: z.string().min(2).describe("Semantic search query"),
        k: z.number().int().min(1).max(50).optional().describe("Result count"),
        filters: z
          .record(z.string(), z.any())
          .optional()
          .describe("Reserved filters map (currently ignored except runId)"),
      },
    },
    async ({ runId, query, k }) => {
      const limit = k ?? 8;
      const toolInput = { runId, query, k: limit };

      try {
        const vector = await embedQueryText(query);
        await ensureQdrantCollection(vector.length);
        const hits = await qdrantSearch(vector, limit, runId);
        const chunkIds = hits
          .map((item) => {
            const payload = (item.payload ?? {}) as Record<string, unknown>;
            return String(payload.chunk_id ?? payload.chunkId ?? item.id ?? "");
          })
          .filter((item) => item.length > 0);
        const chunkMap = new Map<string, string>();
        if (chunkIds.length) {
          const res = await pool.query(
            `SELECT chunk_id, text FROM chunks WHERE chunk_id = ANY($1::uuid[])`,
            [chunkIds]
          );
          for (const row of res.rows) {
            chunkMap.set(String(row.chunk_id), String(row.text ?? ""));
          }
        }

        const results = hits.map((item) => {
          const payload = (item.payload ?? {}) as Record<string, unknown>;
          const chunkId = String(payload.chunk_id ?? payload.chunkId ?? item.id ?? "");
          const snippet = chunkMap.get(chunkId) ?? String(payload.text ?? "");
          return {
            document_id: String(payload.document_id ?? payload.documentId ?? ""),
            chunk_id: chunkId,
            snippet,
            score: typeof item.score === "number" ? item.score : 0.0,
            sourceUrl: typeof payload.source_url === "string" ? payload.source_url : null,
            objectRef: {
              bucket: payload.evidence_bucket ?? null,
              objectKey: payload.evidence_object_key ?? null,
              versionId: payload.evidence_version_id ?? null,
              etag: payload.evidence_etag ?? null,
              documentId: payload.evidence_document_id ?? null,
            },
          };
        });

        if (runId) {
          await logToolCall(runId, "vector_search", toolInput, { count: results.length }, "ok");
        }
        return { content: [{ type: "text", text: JSON.stringify({ results }, null, 2) }] };
      } catch (error) {
        const errorMsg = (error as Error).message;
        if (runId) {
          await logToolCall(runId, "vector_search", toolInput, { error: errorMsg }, "error", errorMsg);
        }
        logger.error("vector_search failed", { runId: runId ?? null, error: errorMsg });
        return {
          content: [{ type: "text", text: JSON.stringify({ error: errorMsg }, null, 2) }],
          isError: true,
        };
      }
    }
  );
}

function registerVectorGetDocument(server: McpServer) {
  server.registerTool(
    "vector_get_document",
    {
      description:
        "Fetch document text and metadata by document_id from Postgres chunks/documents.",
      inputSchema: {
        document_id: z.string().uuid().describe("Document UUID"),
      },
    },
    async ({ document_id }) => {
      try {
        const docRes = await pool.query(
          `SELECT d.document_id, d.run_id, d.source_url, d.title, d.source_domain
           FROM documents d
           WHERE d.document_id = $1
           LIMIT 1`,
          [document_id]
        );
        if (!docRes.rows.length) {
          throw new Error("Document not found");
        }
        const doc = docRes.rows[0];

        const chunksRes = await pool.query(
          `SELECT chunk_index, text, evidence_bucket, evidence_object_key, evidence_version_id, evidence_etag, evidence_document_id
           FROM chunks
           WHERE document_id = $1
           ORDER BY chunk_index ASC`,
          [document_id]
        );

        const text = chunksRes.rows.map((row) => String(row.text ?? "")).join("\n\n");
        const first = chunksRes.rows[0];
        const objectRef = first
          ? {
              bucket: first.evidence_bucket ?? null,
              objectKey: first.evidence_object_key ?? null,
              versionId: first.evidence_version_id ?? null,
              etag: first.evidence_etag ?? null,
              documentId: first.evidence_document_id ?? null,
            }
          : {};

        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(
                {
                  document_id,
                  runId: String(doc.run_id),
                  sourceUrl: doc.source_url ?? null,
                  title: doc.title ?? null,
                  sourceDomain: doc.source_domain ?? null,
                  text,
                  objectRef,
                },
                null,
                2
              ),
            },
          ],
        };
      } catch (error) {
        const errorMsg = (error as Error).message;
        logger.error("vector_get_document failed", { document_id, error: errorMsg });
        return {
          content: [{ type: "text", text: JSON.stringify({ error: errorMsg }, null, 2) }],
          isError: true,
        };
      }
    }
  );
}

function registerGraphGetEntity(server: McpServer) {
  server.registerTool(
    "graph_get_entity",
    {
      description: "Fetch one graph entity node by stable ID.",
      inputSchema: {
        entityId: z.string().min(1).describe("Stable entity ID"),
      },
    },
    async ({ entityId }) => {
      const session = neo4jDriver.session();
      try {
        const result = await session.run(
          `MATCH (n)
           WHERE coalesce(n.node_id, n.person_id, n.org_id, n.location_id, n.address, n.uri, n.name, n.domain, n.email) = $entityId
           RETURN labels(n) as labels, properties(n) as props
           LIMIT 1`,
          { entityId }
        );
        if (!result.records.length) {
          throw new Error("Entity not found");
        }
        const rec = result.records[0];
        const labels = rec.get("labels");
        const props = rec.get("props");
        return {
          content: [{ type: "text", text: JSON.stringify({ entityId, labels, properties: props }, null, 2) }],
        };
      } catch (error) {
        const errorMsg = (error as Error).message;
        return {
          content: [{ type: "text", text: JSON.stringify({ error: errorMsg }, null, 2) }],
          isError: true,
        };
      } finally {
        await session.close();
      }
    }
  );
}

function registerGraphNeighbors(server: McpServer) {
  server.registerTool(
    "graph_neighbors",
    {
      description: "Get 1-2 hop neighbors for an entity with relationship labels and minimal properties.",
      inputSchema: {
        entityId: z.string().min(1).describe("Stable entity ID"),
        depth: z.number().int().min(1).max(2).optional().describe("Traversal depth"),
        relTypes: z.array(z.string()).optional().describe("Optional relationship allowlist"),
      },
    },
    async ({ entityId, depth, relTypes }) => {
      const session = neo4jDriver.session();
      try {
        const hops = depth ?? 1;
        const result = await session.run(
          `MATCH (n)
           WHERE coalesce(n.node_id, n.person_id, n.org_id, n.location_id, n.address, n.uri, n.name, n.domain, n.email) = $entityId
           MATCH p=(n)-[r*1..${hops}]-(m)
           WITH m, [rel IN relationships(p) | coalesce(rel.rel_type, type(rel))] as relTypesFound
           WHERE $relTypes IS NULL OR any(t IN relTypesFound WHERE t IN $relTypes)
           RETURN DISTINCT labels(m) as labels, properties(m) as props, relTypesFound
           LIMIT 200`,
          { entityId, relTypes: relTypes && relTypes.length ? relTypes : null }
        );

        const neighbors = result.records.map((rec) => ({
          labels: rec.get("labels"),
          properties: rec.get("props"),
          relTypes: rec.get("relTypesFound"),
        }));

        return { content: [{ type: "text", text: JSON.stringify({ entityId, neighbors }, null, 2) }] };
      } catch (error) {
        const errorMsg = (error as Error).message;
        return {
          content: [{ type: "text", text: JSON.stringify({ error: errorMsg }, null, 2) }],
          isError: true,
        };
      } finally {
        await session.close();
      }
    }
  );
}

function registerGraphSearchEntities(server: McpServer) {
  server.registerTool(
    "graph_search_entities",
    {
      description:
        "Fallback graph entity search by query string over common ID/name/url/email/domain properties.",
      inputSchema: {
        query: z.string().min(2).describe("Search query"),
        limit: z.number().int().min(1).max(100).optional().describe("Result limit"),
      },
    },
    async ({ query, limit }) => {
      const session = neo4jDriver.session();
      try {
        const maxRows = limit ?? 20;
        const result = await session.run(
          `MATCH (n)
           WHERE toLower(coalesce(n.node_id, '')) CONTAINS toLower($query)
              OR toLower(coalesce(n.person_id, '')) CONTAINS toLower($query)
              OR toLower(coalesce(n.org_id, '')) CONTAINS toLower($query)
              OR toLower(coalesce(n.location_id, '')) CONTAINS toLower($query)
              OR toLower(coalesce(n.address, '')) CONTAINS toLower($query)
              OR toLower(coalesce(n.uri, '')) CONTAINS toLower($query)
              OR toLower(coalesce(n.name, '')) CONTAINS toLower($query)
              OR toLower(coalesce(n.domain, '')) CONTAINS toLower($query)
              OR toLower(coalesce(n.email, '')) CONTAINS toLower($query)
              OR toLower(coalesce(n.canonical_name, '')) CONTAINS toLower($query)
              OR any(v IN coalesce(n.alt_names, []) WHERE toLower(toString(v)) CONTAINS toLower($query))
              OR any(v IN coalesce(n.merge_keys, []) WHERE toLower(toString(v)) CONTAINS toLower($query))
              OR any(v IN coalesce(n.attributes, []) WHERE toLower(toString(v)) CONTAINS toLower($query))
              OR any(v IN coalesce(n.filter_terms, []) WHERE toLower(toString(v)) CONTAINS toLower($query))
           RETURN labels(n) as labels, properties(n) as props
           LIMIT $limit`,
          { query, limit: neo4j.int(maxRows) }
        );

        const entities = result.records.map((rec) => {
          const props = (rec.get("props") ?? {}) as Record<string, unknown>;
          return {
            entityId:
              (props.node_id as string | undefined) ??
              (props.person_id as string | undefined) ??
              (props.org_id as string | undefined) ??
              (props.location_id as string | undefined) ??
              (props.address as string | undefined) ??
              (props.uri as string | undefined) ??
              (props.domain as string | undefined) ??
              (props.email as string | undefined) ??
              (props.name as string | undefined) ??
              "",
            labels: rec.get("labels"),
            properties: props,
          };
        });

        return { content: [{ type: "text", text: JSON.stringify({ entities }, null, 2) }] };
      } catch (error) {
        const errorMsg = (error as Error).message;
        return {
          content: [{ type: "text", text: JSON.stringify({ error: errorMsg }, null, 2) }],
          isError: true,
        };
      } finally {
        await session.close();
      }
    }
  );
}
