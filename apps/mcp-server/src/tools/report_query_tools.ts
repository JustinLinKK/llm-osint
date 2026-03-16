import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import crypto from "node:crypto";
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

type GraphScope = "run" | "hybrid" | "global";

function normalizeGraphScope(value?: string): GraphScope {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (normalized === "global") return "global";
  if (normalized === "hybrid") return "hybrid";
  return "run";
}

function normalizeGraphName(value: string): string {
  return String(value ?? "")
    .toLowerCase()
    .trim()
    .replace(/[\W_]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function stableGraphId(prefix: string, ...parts: string[]): string {
  const digest = crypto.createHash("sha256").update(parts.join("|")).digest("hex").slice(0, 20);
  return `${prefix}_${digest}`;
}

function uniqueGraphStrings(values: Array<unknown>): string[] {
  const output: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (!text) continue;
    const normalized = normalizeGraphName(text);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    output.push(text);
  }
  return output;
}

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
  registerVectorLookupRefs(server);
  registerGraphExportJson(server);
  registerGraphGetEntity(server);
  registerGraphNeighbors(server);
  registerGraphSearchEntities(server);
  registerGraphApplyNormalizationPlan(server);
  registerGraphApplyAdjudications(server);
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
            sourceDomain: typeof payload.source_domain === "string" ? payload.source_domain : null,
            retrievedAt: typeof payload.retrieved_at === "string" ? payload.retrieved_at : null,
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

type ExactEvidenceRef = {
  documentId?: string;
  chunkId?: string;
  bucket?: string;
  objectKey?: string;
  versionId?: string;
  etag?: string;
};

function normalizeEvidenceRef(raw: unknown): ExactEvidenceRef | null {
  if (!raw || typeof raw !== "object") return null;
  const row = raw as Record<string, unknown>;
  const objectRef = typeof row.objectRef === "object" && row.objectRef ? (row.objectRef as Record<string, unknown>) : {};
  const documentId = String(row.documentId ?? objectRef.documentId ?? "").trim();
  const chunkId = String(row.chunkId ?? objectRef.chunkId ?? "").trim();
  const bucket = String(row.bucket ?? objectRef.bucket ?? "").trim();
  const objectKey = String(row.objectKey ?? objectRef.objectKey ?? "").trim();
  const versionId = String(row.versionId ?? objectRef.versionId ?? "").trim();
  const etag = String(row.etag ?? objectRef.etag ?? "").trim();
  if (!documentId && !chunkId && !(bucket && objectKey)) {
    return null;
  }
  return {
    documentId: documentId || undefined,
    chunkId: chunkId || undefined,
    bucket: bucket || undefined,
    objectKey: objectKey || undefined,
    versionId: versionId || undefined,
    etag: etag || undefined,
  };
}

async function lookupEvidenceByChunkIds(chunkIds: string[]) {
  if (!chunkIds.length) return [];
  const res = await pool.query(
    `SELECT
       c.chunk_id::text AS chunk_id,
       c.document_id::text AS document_id,
       c.text,
       c.chunk_index,
       d.run_id::text AS run_id,
       d.source_url,
       d.source_domain,
       d.title,
       d.retrieved_at,
       c.evidence_bucket,
       c.evidence_object_key,
       c.evidence_version_id,
       c.evidence_etag,
       c.evidence_document_id::text AS evidence_document_id
     FROM chunks c
     JOIN documents d ON d.document_id = c.document_id
     WHERE c.chunk_id = ANY($1::uuid[])
     ORDER BY c.chunk_index ASC`,
    [chunkIds]
  );
  return res.rows.map((row) => ({
    document_id: String(row.document_id ?? ""),
    chunk_id: String(row.chunk_id ?? ""),
    snippet: String(row.text ?? ""),
    chunkIndex: Number(row.chunk_index ?? 0),
    runId: String(row.run_id ?? ""),
    sourceUrl: row.source_url ?? null,
    sourceDomain: row.source_domain ?? null,
    title: row.title ?? null,
    retrievedAt: row.retrieved_at instanceof Date ? row.retrieved_at.toISOString() : row.retrieved_at ?? null,
    objectRef: {
      bucket: row.evidence_bucket ?? null,
      objectKey: row.evidence_object_key ?? null,
      versionId: row.evidence_version_id ?? null,
      etag: row.evidence_etag ?? null,
      documentId: row.evidence_document_id ?? null,
      chunkId: String(row.chunk_id ?? "") || null,
    },
    matchedBy: "chunkId",
  }));
}

async function lookupEvidenceByDocumentIds(documentIds: string[], runId?: string) {
  if (!documentIds.length) return [];
  const params: unknown[] = [documentIds];
  const scopeClause = runId ? `AND d.run_id = $2::uuid` : "";
  if (runId) params.push(runId);
  const res = await pool.query(
    `SELECT
       c.chunk_id::text AS chunk_id,
       c.document_id::text AS document_id,
       c.text,
       c.chunk_index,
       d.run_id::text AS run_id,
       d.source_url,
       d.source_domain,
       d.title,
       d.retrieved_at,
       c.evidence_bucket,
       c.evidence_object_key,
       c.evidence_version_id,
       c.evidence_etag,
       c.evidence_document_id::text AS evidence_document_id
     FROM (
       SELECT
         c.*,
         ROW_NUMBER() OVER (PARTITION BY c.document_id ORDER BY c.chunk_index ASC) AS doc_rank
       FROM chunks c
       WHERE c.document_id = ANY($1::uuid[])
     ) c
     JOIN documents d ON d.document_id = c.document_id
     WHERE c.doc_rank <= 3
       ${scopeClause}
     ORDER BY c.document_id ASC, c.chunk_index ASC`,
    params
  );
  return res.rows.map((row) => ({
    document_id: String(row.document_id ?? ""),
    chunk_id: String(row.chunk_id ?? ""),
    snippet: String(row.text ?? ""),
    chunkIndex: Number(row.chunk_index ?? 0),
    runId: String(row.run_id ?? ""),
    sourceUrl: row.source_url ?? null,
    sourceDomain: row.source_domain ?? null,
    title: row.title ?? null,
    retrievedAt: row.retrieved_at instanceof Date ? row.retrieved_at.toISOString() : row.retrieved_at ?? null,
    objectRef: {
      bucket: row.evidence_bucket ?? null,
      objectKey: row.evidence_object_key ?? null,
      versionId: row.evidence_version_id ?? null,
      etag: row.evidence_etag ?? null,
      documentId: row.evidence_document_id ?? null,
      chunkId: String(row.chunk_id ?? "") || null,
    },
    matchedBy: "documentId",
  }));
}

async function lookupEvidenceByObjectRefs(objectRefs: ExactEvidenceRef[], runId?: string) {
  if (!objectRefs.length) return [];
  const rows: Array<Record<string, unknown>> = [];
  for (const ref of objectRefs) {
    const params: unknown[] = [ref.bucket ?? null, ref.objectKey ?? null, ref.documentId ?? null];
    let runClause = "";
    if (runId) {
      params.push(runId);
      runClause = `AND d.run_id = $4::uuid`;
    }
    const res = await pool.query(
      `SELECT
         c.chunk_id::text AS chunk_id,
         c.document_id::text AS document_id,
         c.text,
         c.chunk_index,
         d.run_id::text AS run_id,
         d.source_url,
         d.source_domain,
         d.title,
         d.retrieved_at,
         c.evidence_bucket,
         c.evidence_object_key,
         c.evidence_version_id,
         c.evidence_etag,
         c.evidence_document_id::text AS evidence_document_id
       FROM chunks c
       JOIN documents d ON d.document_id = c.document_id
       WHERE (
         ($1::text IS NOT NULL AND $2::text IS NOT NULL AND c.evidence_bucket = $1::text AND c.evidence_object_key = $2::text)
         OR ($3::uuid IS NOT NULL AND c.document_id = $3::uuid)
       )
       ${runClause}
       ORDER BY c.document_id ASC, c.chunk_index ASC
       LIMIT 6`,
      params
    );
    for (const row of res.rows) {
      rows.push({
        document_id: String(row.document_id ?? ""),
        chunk_id: String(row.chunk_id ?? ""),
        snippet: String(row.text ?? ""),
        chunkIndex: Number(row.chunk_index ?? 0),
        runId: String(row.run_id ?? ""),
        sourceUrl: row.source_url ?? null,
        sourceDomain: row.source_domain ?? null,
        title: row.title ?? null,
        retrievedAt: row.retrieved_at instanceof Date ? row.retrieved_at.toISOString() : row.retrieved_at ?? null,
        objectRef: {
          bucket: row.evidence_bucket ?? null,
          objectKey: row.evidence_object_key ?? null,
          versionId: row.evidence_version_id ?? null,
          etag: row.evidence_etag ?? null,
          documentId: row.evidence_document_id ?? null,
          chunkId: String(row.chunk_id ?? "") || null,
        },
        matchedBy: "objectRef",
      });
    }
  }
  return rows;
}

function registerVectorLookupRefs(server: McpServer) {
  server.registerTool(
    "vector_lookup_refs",
    {
      description:
        "Resolve exact evidence references into chunk snippets and document metadata using document IDs, chunk IDs, or object refs.",
      inputSchema: {
        runId: z.string().uuid().optional().describe("Optional run scope"),
        refsJson: z.string().describe("JSON array of evidence refs ({documentId, chunkId, objectRef})"),
      },
    },
    async ({ runId, refsJson }) => {
      const toolInput = { runId, refsJson };
      try {
        const parsed = JSON.parse(refsJson) as unknown;
        if (!Array.isArray(parsed)) {
          throw new Error("refsJson must be a JSON array");
        }
        const refs = parsed.map(normalizeEvidenceRef).filter((item): item is ExactEvidenceRef => Boolean(item));
        const chunkIds = [...new Set(refs.map((item) => item.chunkId).filter((item): item is string => Boolean(item)))];
        const documentIds = [...new Set(refs.map((item) => item.documentId).filter((item): item is string => Boolean(item)))];
        const objectRefs = refs.filter((item) => Boolean(item.bucket && item.objectKey));

        const [chunkRows, documentRows, objectRows] = await Promise.all([
          lookupEvidenceByChunkIds(chunkIds),
          lookupEvidenceByDocumentIds(documentIds, runId),
          lookupEvidenceByObjectRefs(objectRefs, runId),
        ]);
        const deduped = new Map<string, Record<string, unknown>>();
        for (const row of [...chunkRows, ...documentRows, ...objectRows]) {
          const key = `${String(row.document_id ?? "")}|${String(row.chunk_id ?? "")}|${String((row.objectRef as Record<string, unknown> | undefined)?.objectKey ?? "")}`;
          if (!deduped.has(key)) {
            deduped.set(key, row);
          }
        }
        const results = [...deduped.values()];
        if (runId) {
          await logToolCall(runId, "vector_lookup_refs", toolInput, { count: results.length }, "ok");
        }
        return { content: [{ type: "text", text: JSON.stringify({ results }, null, 2) }] };
      } catch (error) {
        const errorMsg = (error as Error).message;
        if (runId) {
          await logToolCall(runId, "vector_lookup_refs", toolInput, { error: errorMsg }, "error", errorMsg);
        }
        logger.error("vector_lookup_refs failed", { runId: runId ?? null, error: errorMsg });
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
        runId: z.string().uuid().optional().describe("Optional run scope"),
        scope: z.enum(["run", "hybrid", "global"]).optional().describe("Graph query scope when runId is provided"),
        entityId: z.string().min(1).describe("Stable entity ID"),
        includeSuppressed: z.boolean().optional().describe("Include suppressed nodes (default: false)"),
      },
    },
    async ({ runId, scope, entityId, includeSuppressed }) => {
      const session = neo4jDriver.session();
      try {
        const queryScope = normalizeGraphScope(scope);
        const result = await session.run(
          `MATCH (n)
           WHERE coalesce(n.node_id, n.person_id, n.org_id, n.location_id, n.address, n.uri, n.name, n.domain, n.email) = $entityId
             AND ($includeSuppressed = true OR coalesce(n.suppressed, false) = false)
             AND (
               $runId IS NULL
               OR $scope = 'global'
               OR ($scope = 'run' AND coalesce(n.run_id, '') = $runId)
               OR ($scope = 'hybrid' AND (coalesce(n.run_id, '') = $runId OR coalesce(n.external_context, false) = true))
             )
           RETURN labels(n) as labels, properties(n) as props
           LIMIT 1`,
          { entityId, runId: runId ?? null, scope: queryScope, includeSuppressed: Boolean(includeSuppressed) }
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

function registerGraphExportJson(server: McpServer) {
  server.registerTool(
    "graph_export_json",
    {
      description:
        "Export the run-scoped graph as JSON with nodes and relations for planner conflict analysis and graph-aware reasoning.",
      inputSchema: {
        runId: z.string().uuid().describe("Run ID scope"),
        scope: z.enum(["run", "hybrid", "global"]).optional().describe("Graph query scope (default: run)"),
        maxNodes: z.number().int().min(1).max(5000).optional().describe("Maximum node count to export"),
        maxRelations: z.number().int().min(1).max(10000).optional().describe("Maximum relation count to export"),
        includeSuppressed: z.boolean().optional().describe("Include suppressed nodes and relations (default: false)"),
      },
    },
    async ({ runId, scope, maxNodes, maxRelations, includeSuppressed }) => {
      const session = neo4jDriver.session();
      const toolInput = {
        runId,
        scope,
        maxNodes: maxNodes ?? 600,
        maxRelations: maxRelations ?? 1200,
        includeSuppressed: Boolean(includeSuppressed),
      };
      try {
        const queryScope = normalizeGraphScope(scope);
        const nodeLimit = neo4j.int(maxNodes ?? 600);
        const relationLimit = neo4j.int(maxRelations ?? 1200);

        const nodesResult = await session.run(
          `MATCH (n)
           WHERE (
             $scope = 'global'
             OR ($scope = 'run' AND coalesce(n.run_id, '') = $runId)
             OR ($scope = 'hybrid' AND (coalesce(n.run_id, '') = $runId OR coalesce(n.external_context, false) = true))
           )
           AND ($includeSuppressed = true OR coalesce(n.suppressed, false) = false)
           RETURN labels(n) as labels, properties(n) as props
           ORDER BY coalesce(n.node_id, n.person_id, n.org_id, n.location_id, n.address, n.uri, n.domain, n.email, n.name, '')
           LIMIT $limit`,
          { runId, scope: queryScope, limit: nodeLimit, includeSuppressed: Boolean(includeSuppressed) }
        );

        const nodes = nodesResult.records.map((rec) => {
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

        const relationsResult = await session.run(
          `MATCH (src)-[r]->(dst)
           WHERE (
             $scope = 'global'
             OR (
               $scope = 'run'
               AND coalesce(r.run_id, '') = $runId
               AND coalesce(src.run_id, '') = $runId
               AND coalesce(dst.run_id, '') = $runId
             )
             OR (
               $scope = 'hybrid'
               AND (coalesce(r.run_id, '') = $runId OR coalesce(r.external_context, false) = true)
               AND (coalesce(src.run_id, '') = $runId OR coalesce(src.external_context, false) = true)
               AND (coalesce(dst.run_id, '') = $runId OR coalesce(dst.external_context, false) = true)
             )
           )
           AND ($includeSuppressed = true OR (coalesce(r.suppressed, false) = false AND coalesce(src.suppressed, false) = false AND coalesce(dst.suppressed, false) = false))
           RETURN
             labels(src) as srcLabels,
             properties(src) as srcProps,
             labels(dst) as dstLabels,
             properties(dst) as dstProps,
             type(r) as edgeType,
             properties(r) as relProps
           ORDER BY coalesce(r.edge_id, '')
           LIMIT $limit`,
          { runId, scope: queryScope, limit: relationLimit, includeSuppressed: Boolean(includeSuppressed) }
        );

        const relations = relationsResult.records.map((rec) => {
          const srcProps = (rec.get("srcProps") ?? {}) as Record<string, unknown>;
          const dstProps = (rec.get("dstProps") ?? {}) as Record<string, unknown>;
          const relProps = (rec.get("relProps") ?? {}) as Record<string, unknown>;
          return {
            edgeId: (relProps.edge_id as string | undefined) ?? "",
            edgeType: String(rec.get("edgeType") ?? ""),
            relType: (relProps.rel_type as string | undefined) ?? String(rec.get("edgeType") ?? ""),
            canonicalName: (relProps.canonical_name as string | undefined) ?? "",
            srcEntityId:
              (srcProps.node_id as string | undefined) ??
              (srcProps.person_id as string | undefined) ??
              (srcProps.org_id as string | undefined) ??
              (srcProps.location_id as string | undefined) ??
              (srcProps.address as string | undefined) ??
              (srcProps.uri as string | undefined) ??
              (srcProps.domain as string | undefined) ??
              (srcProps.email as string | undefined) ??
              (srcProps.name as string | undefined) ??
              "",
            dstEntityId:
              (dstProps.node_id as string | undefined) ??
              (dstProps.person_id as string | undefined) ??
              (dstProps.org_id as string | undefined) ??
              (dstProps.location_id as string | undefined) ??
              (dstProps.address as string | undefined) ??
              (dstProps.uri as string | undefined) ??
              (dstProps.domain as string | undefined) ??
              (dstProps.email as string | undefined) ??
              (dstProps.name as string | undefined) ??
              "",
            srcLabels: rec.get("srcLabels"),
            dstLabels: rec.get("dstLabels"),
            properties: relProps,
          };
        });

        const output = {
          runId,
          scope: queryScope,
          nodeCount: nodes.length,
          relationCount: relations.length,
          nodes,
          relations,
        };
        await logToolCall(runId, "graph_export_json", toolInput, { nodeCount: nodes.length, relationCount: relations.length }, "ok");
        return { content: [{ type: "text", text: JSON.stringify(output, null, 2) }] };
      } catch (error) {
        const errorMsg = (error as Error).message;
        await logToolCall(runId, "graph_export_json", toolInput, { error: errorMsg }, "error", errorMsg);
        logger.error("graph_export_json failed", { runId, error: errorMsg });
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
        runId: z.string().uuid().describe("Run ID scope"),
        entityId: z.string().min(1).describe("Stable entity ID"),
        scope: z.enum(["run", "hybrid", "global"]).optional().describe("Graph query scope (default: run)"),
        depth: z.number().int().min(1).max(2).optional().describe("Traversal depth"),
        relTypes: z.array(z.string()).optional().describe("Optional relationship allowlist"),
        includeSuppressed: z.boolean().optional().describe("Include suppressed nodes and relations (default: false)"),
      },
    },
    async ({ runId, entityId, scope, depth, relTypes, includeSuppressed }) => {
      const session = neo4jDriver.session();
      try {
        const queryScope = normalizeGraphScope(scope);
        const hops = depth ?? 1;
        const result = await session.run(
          `MATCH (n)
           WHERE coalesce(n.node_id, n.person_id, n.org_id, n.location_id, n.address, n.uri, n.name, n.domain, n.email) = $entityId
             AND ($includeSuppressed = true OR coalesce(n.suppressed, false) = false)
             AND (
               $scope = 'global'
               OR (
                 $scope = 'run'
                 AND coalesce(n.run_id, '') = $runId
               )
               OR (
                 $scope = 'hybrid'
                 AND (coalesce(n.run_id, '') = $runId OR coalesce(n.external_context, false) = true)
               )
             )
           MATCH p=(n)-[r*1..${hops}]-(m)
           WITH p, m, [rel IN relationships(p) | coalesce(rel.rel_type, type(rel))] as relTypesFound
           WHERE (
               $scope = 'global'
               OR (
                 $scope = 'run'
                 AND coalesce(m.run_id, '') = $runId
                 AND all(rel IN relationships(p) WHERE coalesce(rel.run_id, '') = $runId)
               )
               OR (
                 $scope = 'hybrid'
                 AND (coalesce(m.run_id, '') = $runId OR coalesce(m.external_context, false) = true)
                 AND all(rel IN relationships(p) WHERE coalesce(rel.run_id, '') = $runId OR coalesce(rel.external_context, false) = true)
               )
             )
             AND ($includeSuppressed = true OR (coalesce(m.suppressed, false) = false AND all(rel IN relationships(p) WHERE coalesce(rel.suppressed, false) = false)))
             AND ($relTypes IS NULL OR any(t IN relTypesFound WHERE t IN $relTypes))
           RETURN DISTINCT labels(m) as labels, properties(m) as props, relTypesFound
           LIMIT 200`,
          {
            runId,
            scope: queryScope,
            entityId,
            relTypes: relTypes && relTypes.length ? relTypes : null,
            includeSuppressed: Boolean(includeSuppressed),
          }
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
        runId: z.string().uuid().describe("Run ID scope"),
        query: z.string().min(2).describe("Search query"),
        scope: z.enum(["run", "hybrid", "global"]).optional().describe("Graph query scope (default: run)"),
        limit: z.number().int().min(1).max(100).optional().describe("Result limit"),
        includeSuppressed: z.boolean().optional().describe("Include suppressed nodes (default: false)"),
      },
    },
    async ({ runId, query, scope, limit, includeSuppressed }) => {
      const session = neo4jDriver.session();
      try {
        const queryScope = normalizeGraphScope(scope);
        const maxRows = limit ?? 20;
        const result = await session.run(
          `MATCH (n)
           WHERE (
                 $scope = 'global'
                 OR ($scope = 'run' AND coalesce(n.run_id, '') = $runId)
                 OR ($scope = 'hybrid' AND (coalesce(n.run_id, '') = $runId OR coalesce(n.external_context, false) = true))
               )
             AND ($includeSuppressed = true OR coalesce(n.suppressed, false) = false)
             AND (
              toLower(coalesce(n.node_id, '')) CONTAINS toLower($query)
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
             )
           RETURN labels(n) as labels, properties(n) as props
           LIMIT $limit`,
          { runId, scope: queryScope, query, limit: neo4j.int(maxRows), includeSuppressed: Boolean(includeSuppressed) }
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

type GraphAdjudicationInput = {
  caseId?: string;
  targetType?: string;
  targetId?: string;
  fieldName?: string;
  chosenValue?: unknown;
  confidence?: number;
  status?: string;
  unresolvedFields?: unknown[];
};

type GraphNormalizationActionInput = {
  actionType?: string;
  sourceEntityId?: string;
  targetEntityId?: string;
  relationId?: string;
  srcEntityId?: string;
  dstEntityId?: string;
  relType?: string;
  canonicalName?: unknown;
  entityType?: string;
  aliases?: unknown[];
  rationale?: string;
};

function normalizeStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return uniqueGraphStrings(value);
}

function mergeStringArrays(...values: unknown[]): string[] {
  const collected: unknown[] = [];
  for (const value of values) {
    if (Array.isArray(value)) {
      collected.push(...value);
    } else if (typeof value === "string" && value.trim()) {
      collected.push(value.trim());
    }
  }
  return uniqueGraphStrings(collected);
}

function normalizeRelationType(value: string): string {
  const normalized = normalizeGraphName(value).replace(/\s+/g, "_").toUpperCase();
  return normalized || "RELATED_TO";
}

async function loadEntityProps(
  session: ReturnType<typeof neo4jDriver.session>,
  runId: string,
  entityId: string
): Promise<Record<string, unknown> | null> {
  const result = await session.run(
    `MATCH (n:Entity {node_id: $entityId})
     WHERE coalesce(n.run_id, '') = $runId
     RETURN properties(n) AS props
     LIMIT 1`,
    { runId, entityId }
  );
  if (!result.records.length) return null;
  return (result.records[0].get("props") ?? {}) as Record<string, unknown>;
}

function buildRootEntityProps(
  runId: string,
  canonicalName: string,
  aliases: string[],
  entityType = "Person",
  existing: Record<string, unknown> | null = null,
  targetEntityId = ""
): Record<string, unknown> {
  const normalizedType = entityType.trim() || "Person";
  const canonical = canonicalName.trim() || "Primary Target";
  const nodeId =
    targetEntityId ||
    String(existing?.node_id ?? stableGraphId("ent", runId, normalizeGraphName(normalizedType), normalizeGraphName(canonical)));
  const canonicalId =
    String(existing?.canonical_id ?? stableGraphId("entc", normalizeGraphName(normalizedType), normalizeGraphName(canonical)));
  const altNames = mergeStringArrays(existing?.alt_names, aliases).filter(
    (item) => normalizeGraphName(item) !== normalizeGraphName(canonical)
  );
  const mergeKeys = mergeStringArrays(
    existing?.merge_keys,
    [`name:${normalizeGraphName(normalizedType)}:${normalizeGraphName(canonical)}`],
    ...altNames.map((item) => `name:${normalizeGraphName(normalizedType)}:${normalizeGraphName(item)}`)
  );
  const filterTerms = mergeStringArrays(existing?.filter_terms, canonical, altNames);
  const now = new Date().toISOString();
  return {
    ...(existing ?? {}),
    node_id: nodeId,
    canonical_id: canonicalId,
    run_scoped_id: String(existing?.run_scoped_id ?? nodeId),
    run_id: runId,
    external_context: Boolean(existing?.external_context ?? false),
    type: String(existing?.type ?? normalizedType),
    raw_type: String(existing?.raw_type ?? normalizedType),
    canonical_name: String(existing?.canonical_name ?? canonical),
    canonical_name_normalized: normalizeGraphName(String(existing?.canonical_name ?? canonical)),
    alt_names: altNames,
    alt_names_normalized: altNames.map((item) => normalizeGraphName(item)),
    merge_keys: mergeKeys,
    filter_terms: filterTerms,
    source_tools: mergeStringArrays(existing?.source_tools, "graph_apply_normalization_plan"),
    suppressed: false,
    is_primary_root: true,
    updated_at: now,
    created_at: String(existing?.created_at ?? now),
    ingested_at: String(existing?.ingested_at ?? now),
  };
}

function buildMergedEntityProps(
  target: Record<string, unknown>,
  source: Record<string, unknown>,
  targetEntityId: string,
  sourceEntityId: string
): Record<string, unknown> {
  const targetCanonical = String(target.canonical_name ?? targetEntityId).trim() || targetEntityId;
  const altNames = mergeStringArrays(
    target.alt_names,
    source.alt_names,
    source.canonical_name
  ).filter((item) => normalizeGraphName(item) !== normalizeGraphName(targetCanonical));
  const mergeKeys = mergeStringArrays(target.merge_keys, source.merge_keys);
  const now = new Date().toISOString();
  return {
    ...target,
    alt_names: altNames,
    alt_names_normalized: altNames.map((item) => normalizeGraphName(item)),
    merge_keys: mergeKeys,
    source_tools: mergeStringArrays(target.source_tools, source.source_tools, "graph_apply_normalization_plan"),
    source_urls: mergeStringArrays(target.source_urls, source.source_urls),
    filter_terms: mergeStringArrays(target.filter_terms, source.filter_terms, source.canonical_name, source.alt_names),
    evidence_object_keys: mergeStringArrays(target.evidence_object_keys, source.evidence_object_keys),
    adjudication_case_ids: mergeStringArrays(target.adjudication_case_ids, source.adjudication_case_ids),
    merged_from_ids: mergeStringArrays(target.merged_from_ids, source.merged_from_ids, sourceEntityId),
    suppressed: false,
    updated_at: now,
    is_primary_root: Boolean(target.is_primary_root ?? false),
  };
}

function buildRelationUpsertProps(
  runId: string,
  srcId: string,
  dstId: string,
  relProps: Record<string, unknown>,
  relTypeOverride?: string,
  canonicalNameOverride?: string
): Record<string, unknown> {
  const relType = normalizeRelationType(String(relTypeOverride ?? relProps.rel_type ?? "RELATED_TO"));
  const canonicalName = String(canonicalNameOverride ?? relProps.canonical_name ?? relType).trim() || relType;
  const edgeId = stableGraphId("rel", runId, srcId, dstId, normalizeGraphName(relType), normalizeGraphName(canonicalName));
  const altNames = normalizeStringArray(relProps.alt_names);
  const now = new Date().toISOString();
  return {
    ...relProps,
    edge_id: edgeId,
    run_scoped_id: edgeId,
    run_id: runId,
    external_context: Boolean(relProps.external_context ?? false),
    src_id: srcId,
    dst_id: dstId,
    rel_type: relType,
    raw_relation_type: String(relProps.raw_relation_type ?? relType),
    rel_type_normalized: normalizeGraphName(relType),
    canonical_name: canonicalName,
    canonical_name_normalized: normalizeGraphName(canonicalName),
    alt_names: altNames,
    alt_names_normalized: altNames.map((item) => normalizeGraphName(item)),
    suppressed: false,
    updated_at: now,
    created_at: String(relProps.created_at ?? now),
    ingested_at: String(relProps.ingested_at ?? now),
    source_tool: String(relProps.source_tool ?? "graph_apply_normalization_plan"),
  };
}

function registerGraphApplyNormalizationPlan(server: McpServer) {
  server.registerTool(
    "graph_apply_normalization_plan",
    {
      description:
        "Apply deterministic Stage-1 graph normalization actions such as ensuring a root node, merging duplicate entities, and suppressing graph noise.",
      inputSchema: {
        runId: z.string().uuid().describe("Run ID scope"),
        actionsJson: z.string().describe("JSON array of normalization actions"),
        dryRun: z.boolean().optional().describe("Validate and summarize actions without mutating the graph"),
      },
    },
    async ({ runId, actionsJson, dryRun }) => {
      const session = neo4jDriver.session();
      const toolInput = { runId, actionsJson, dryRun: Boolean(dryRun) };
      try {
        const parsed = JSON.parse(actionsJson) as unknown;
        if (!Array.isArray(parsed)) {
          throw new Error("actionsJson must be a JSON array");
        }
        const appliedEntityIds = new Set<string>();
        const appliedRelationIds = new Set<string>();
        const appliedActionTypes = new Set<string>();
        const dryRunMode = Boolean(dryRun);

        for (const raw of parsed as GraphNormalizationActionInput[]) {
          const actionType = String(raw.actionType ?? "").trim();
          if (!actionType) continue;
          appliedActionTypes.add(actionType);
          if (dryRunMode) {
            continue;
          }

          if (actionType === "ensure_root_entity") {
            const targetEntityId = String(raw.targetEntityId ?? "").trim();
            const canonicalName = normalizeAdjudicationValue(raw.canonicalName) ?? "";
            const aliases = normalizeStringArray(raw.aliases);
            let existing = targetEntityId ? await loadEntityProps(session, runId, targetEntityId) : null;
            if (!existing && !canonicalName) {
              continue;
            }
            const props = buildRootEntityProps(
              runId,
              canonicalName || String(existing?.canonical_name ?? ""),
              aliases,
              String(raw.entityType ?? existing?.type ?? "Person"),
              existing,
              targetEntityId
            );
            await session.run(
              `MERGE (n:Entity {node_id: $nodeId})
               SET n += $props`,
              { nodeId: String(props.node_id), props }
            );
            appliedEntityIds.add(String(props.node_id));
            continue;
          }

          if (actionType === "suppress_entity") {
            const sourceEntityId = String(raw.sourceEntityId ?? raw.targetEntityId ?? "").trim();
            if (!sourceEntityId) continue;
            await session.run(
              `MATCH (n:Entity {node_id: $entityId})
               WHERE coalesce(n.run_id, '') = $runId
               SET n.suppressed = true,
                   n.suppression_reason = 'graph_normalization_noise',
                   n.suppressed_at = $now,
                   n.updated_at = $now`,
              { runId, entityId: sourceEntityId, now: new Date().toISOString() }
            );
            appliedEntityIds.add(sourceEntityId);
            continue;
          }

          if (actionType === "suppress_relation") {
            const relationId = String(raw.relationId ?? "").trim();
            if (!relationId) continue;
            await session.run(
              `MATCH ()-[r:RELATED_TO {edge_id: $relationId}]-()
               WHERE coalesce(r.run_id, '') = $runId
               SET r.suppressed = true,
                   r.suppression_reason = 'graph_normalization_noise',
                   r.suppressed_at = $now,
                   r.updated_at = $now`,
              { runId, relationId, now: new Date().toISOString() }
            );
            appliedRelationIds.add(relationId);
            continue;
          }

          if (actionType === "ensure_relation") {
            const srcEntityId = String(raw.srcEntityId ?? "").trim();
            const dstEntityId = String(raw.dstEntityId ?? "").trim();
            if (!srcEntityId || !dstEntityId) continue;
            const relProps = buildRelationUpsertProps(
              runId,
              srcEntityId,
              dstEntityId,
              {},
              String(raw.relType ?? "RELATED_TO"),
              normalizeAdjudicationValue(raw.canonicalName) ?? undefined
            );
            await session.run(
              `MATCH (src:Entity {node_id: $srcId})
               MATCH (dst:Entity {node_id: $dstId})
               MERGE (src)-[r:RELATED_TO {edge_id: $edgeId}]->(dst)
               SET r += $props`,
              {
                srcId: srcEntityId,
                dstId: dstEntityId,
                edgeId: String(relProps.edge_id),
                props: relProps,
              }
            );
            appliedRelationIds.add(String(relProps.edge_id));
            continue;
          }

          if (actionType === "merge_entity_into") {
            const sourceEntityId = String(raw.sourceEntityId ?? "").trim();
            const targetEntityId = String(raw.targetEntityId ?? "").trim();
            if (!sourceEntityId || !targetEntityId || sourceEntityId === targetEntityId) continue;
            const source = await loadEntityProps(session, runId, sourceEntityId);
            const target = await loadEntityProps(session, runId, targetEntityId);
            if (!source || !target) continue;

            const mergedTargetProps = buildMergedEntityProps(target, source, targetEntityId, sourceEntityId);
            await session.run(
              `MATCH (n:Entity {node_id: $entityId})
               WHERE coalesce(n.run_id, '') = $runId
               SET n += $props`,
              { runId, entityId: targetEntityId, props: mergedTargetProps }
            );

            const outgoing = await session.run(
              `MATCH (:Entity {node_id: $entityId})-[r:RELATED_TO]->(dst:Entity)
               WHERE coalesce(r.run_id, '') = $runId
               RETURN properties(r) AS relProps, dst.node_id AS otherId`,
              { runId, entityId: sourceEntityId }
            );
            const incoming = await session.run(
              `MATCH (src:Entity)-[r:RELATED_TO]->(:Entity {node_id: $entityId})
               WHERE coalesce(r.run_id, '') = $runId
               RETURN properties(r) AS relProps, src.node_id AS otherId`,
              { runId, entityId: sourceEntityId }
            );

            for (const record of outgoing.records) {
              const relProps = (record.get("relProps") ?? {}) as Record<string, unknown>;
              const otherId = String(record.get("otherId") ?? "").trim();
              if (!otherId || otherId === targetEntityId) {
                await session.run(
                  `MATCH (:Entity {node_id: $sourceId})-[r:RELATED_TO]->(:Entity {node_id: $otherId})
                   WHERE coalesce(r.run_id, '') = $runId
                   SET r.suppressed = true,
                       r.suppression_reason = 'merged_duplicate_rewrite',
                       r.updated_at = $now`,
                  { runId, sourceId: sourceEntityId, otherId, now: new Date().toISOString() }
                );
                continue;
              }
              const nextProps = buildRelationUpsertProps(runId, targetEntityId, otherId, relProps);
              await session.run(
                `MATCH (src:Entity {node_id: $srcId})
                 MATCH (dst:Entity {node_id: $dstId})
                 MERGE (src)-[r:RELATED_TO {edge_id: $edgeId}]->(dst)
                 SET r += $props`,
                {
                  srcId: targetEntityId,
                  dstId: otherId,
                  edgeId: String(nextProps.edge_id),
                  props: nextProps,
                }
              );
              await session.run(
                `MATCH (:Entity {node_id: $sourceId})-[r:RELATED_TO]->(:Entity {node_id: $otherId})
                 WHERE coalesce(r.run_id, '') = $runId
                 SET r.suppressed = true,
                     r.suppression_reason = 'merged_duplicate_rewrite',
                     r.merged_into_relation = $mergedInto,
                     r.updated_at = $now`,
                {
                  runId,
                  sourceId: sourceEntityId,
                  otherId,
                  mergedInto: String(nextProps.edge_id),
                  now: new Date().toISOString(),
                }
              );
              appliedRelationIds.add(String(nextProps.edge_id));
            }

            for (const record of incoming.records) {
              const relProps = (record.get("relProps") ?? {}) as Record<string, unknown>;
              const otherId = String(record.get("otherId") ?? "").trim();
              if (!otherId || otherId === targetEntityId) {
                await session.run(
                  `MATCH (:Entity {node_id: $otherId})-[r:RELATED_TO]->(:Entity {node_id: $sourceId})
                   WHERE coalesce(r.run_id, '') = $runId
                   SET r.suppressed = true,
                       r.suppression_reason = 'merged_duplicate_rewrite',
                       r.updated_at = $now`,
                  { runId, sourceId: sourceEntityId, otherId, now: new Date().toISOString() }
                );
                continue;
              }
              const nextProps = buildRelationUpsertProps(runId, otherId, targetEntityId, relProps);
              await session.run(
                `MATCH (src:Entity {node_id: $srcId})
                 MATCH (dst:Entity {node_id: $dstId})
                 MERGE (src)-[r:RELATED_TO {edge_id: $edgeId}]->(dst)
                 SET r += $props`,
                {
                  srcId: otherId,
                  dstId: targetEntityId,
                  edgeId: String(nextProps.edge_id),
                  props: nextProps,
                }
              );
              await session.run(
                `MATCH (:Entity {node_id: $otherId})-[r:RELATED_TO]->(:Entity {node_id: $sourceId})
                 WHERE coalesce(r.run_id, '') = $runId
                 SET r.suppressed = true,
                     r.suppression_reason = 'merged_duplicate_rewrite',
                     r.merged_into_relation = $mergedInto,
                     r.updated_at = $now`,
                {
                  runId,
                  sourceId: sourceEntityId,
                  otherId,
                  mergedInto: String(nextProps.edge_id),
                  now: new Date().toISOString(),
                }
              );
              appliedRelationIds.add(String(nextProps.edge_id));
            }

            await session.run(
              `MATCH (n:Entity {node_id: $entityId})
               WHERE coalesce(n.run_id, '') = $runId
               SET n.suppressed = true,
                   n.suppression_reason = 'merged_duplicate',
                   n.merged_into = $targetEntityId,
                   n.updated_at = $now,
                   n.is_primary_root = false`,
              {
                runId,
                entityId: sourceEntityId,
                targetEntityId,
                now: new Date().toISOString(),
              }
            );
            appliedEntityIds.add(sourceEntityId);
            appliedEntityIds.add(targetEntityId);
          }
        }

        const output = {
          dryRun: dryRunMode,
          appliedEntityIds: [...appliedEntityIds],
          appliedRelationIds: [...appliedRelationIds],
          actionTypes: [...appliedActionTypes],
        };
        await logToolCall(runId, "graph_apply_normalization_plan", toolInput, output, "ok");
        return { content: [{ type: "text", text: JSON.stringify(output, null, 2) }] };
      } catch (error) {
        const errorMsg = (error as Error).message;
        await logToolCall(runId, "graph_apply_normalization_plan", toolInput, { error: errorMsg }, "error", errorMsg);
        logger.error("graph_apply_normalization_plan failed", { runId, error: errorMsg });
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

function normalizeAdjudicationValue(value: unknown): string | null {
  if (Array.isArray(value)) {
    const joined = value
      .map((item) => String(item ?? "").trim())
      .filter(Boolean)
      .join(" | ");
    return joined || null;
  }
  const text = String(value ?? "").trim();
  return text || null;
}

function registerGraphApplyAdjudications(server: McpServer) {
  server.registerTool(
    "graph_apply_adjudications",
    {
      description:
        "Apply resolved Stage-1 conflict adjudications to run-scoped graph nodes or relations and stamp conflict metadata.",
      inputSchema: {
        runId: z.string().uuid().describe("Run ID scope"),
        resolutionsJson: z.string().describe("JSON array of adjudication payloads"),
      },
    },
    async ({ runId, resolutionsJson }) => {
      const session = neo4jDriver.session();
      const toolInput = { runId, resolutionsJson };
      try {
        const parsed = JSON.parse(resolutionsJson) as unknown;
        if (!Array.isArray(parsed)) {
          throw new Error("resolutionsJson must be a JSON array");
        }
        const appliedEntityIds = new Set<string>();
        const appliedRelationIds = new Set<string>();
        const caseIds: string[] = [];
        for (const raw of parsed as GraphAdjudicationInput[]) {
          const caseId = String(raw.caseId ?? "").trim();
          const targetType = String(raw.targetType ?? "").trim().toLowerCase();
          const targetId = String(raw.targetId ?? "").trim();
          const fieldName = String(raw.fieldName ?? "").trim().toLowerCase();
          const status = String(raw.status ?? "applied").trim().toLowerCase();
          const confidence = typeof raw.confidence === "number" ? raw.confidence : 0.0;
          const chosenValue = normalizeAdjudicationValue(raw.chosenValue);
          const unresolvedFields = Array.isArray(raw.unresolvedFields)
            ? raw.unresolvedFields.map((item) => String(item ?? "").trim()).filter(Boolean)
            : [];
          if (!caseId || !targetId || !fieldName) continue;
          caseIds.push(caseId);

          if (targetType === "entity") {
            const props: Record<string, unknown> = {
              last_adjudicated_at: new Date().toISOString(),
              resolution_confidence: confidence,
              unresolved_fields: unresolvedFields,
              adjudication_case_ids: [caseId],
            };
            if (chosenValue) {
              if (fieldName === "canonical_name") {
                props.canonical_name = chosenValue;
                props.canonical_name_normalized = chosenValue.toLowerCase().trim().replace(/[\W_]+/g, " ").replace(/\s+/g, " ").trim();
              } else {
                props[`resolved_${fieldName}`] = chosenValue;
              }
            }
            await session.run(
              `MATCH (n:Entity {node_id: $targetId})
               WHERE coalesce(n.run_id, '') = $runId
               SET n += $props,
                   n.adjudication_status = $status,
                   n.adjudication_case_ids = coalesce(n.adjudication_case_ids, []) + $caseId
               RETURN n.node_id AS nodeId`,
              { runId, targetId, props, status, caseId }
            );
            appliedEntityIds.add(targetId);
            continue;
          }

          if (targetType === "relation") {
            const props: Record<string, unknown> = {
              last_adjudicated_at: new Date().toISOString(),
              resolution_confidence: confidence,
              unresolved_fields: unresolvedFields,
              adjudication_case_ids: [caseId],
            };
            if (chosenValue) {
              if (fieldName === "rel_type" || fieldName === "relation_type") {
                props.rel_type = chosenValue;
                props.rel_type_normalized = chosenValue.toLowerCase().trim().replace(/[\W_]+/g, " ").replace(/\s+/g, " ").trim();
                props.canonical_name = chosenValue;
                props.canonical_name_normalized = props.rel_type_normalized;
              } else {
                props[`resolved_${fieldName}`] = chosenValue;
              }
            }
            await session.run(
              `MATCH ()-[r:RELATED_TO {edge_id: $targetId}]-()
               WHERE coalesce(r.run_id, '') = $runId
               SET r += $props,
                   r.adjudication_status = $status,
                   r.adjudication_case_ids = coalesce(r.adjudication_case_ids, []) + $caseId
               RETURN r.edge_id AS edgeId`,
              { runId, targetId, props, status, caseId }
            );
            appliedRelationIds.add(targetId);
          }
        }

        const output = {
          appliedEntityIds: [...appliedEntityIds],
          appliedRelationIds: [...appliedRelationIds],
          caseIds: [...new Set(caseIds)],
        };
        await logToolCall(runId, "graph_apply_adjudications", toolInput, output, "ok");
        return { content: [{ type: "text", text: JSON.stringify(output, null, 2) }] };
      } catch (error) {
        const errorMsg = (error as Error).message;
        await logToolCall(runId, "graph_apply_adjudications", toolInput, { error: errorMsg }, "error", errorMsg);
        logger.error("graph_apply_adjudications failed", { runId, error: errorMsg });
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
