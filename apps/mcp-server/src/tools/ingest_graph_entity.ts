import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import crypto from "node:crypto";
import { neo4jDriver } from "../clients/neo4j.js";
import { embedTexts } from "../embeddings.js";
import { emitRunEvent, logToolCall } from "./helpers.js";
import { logger } from "../utils/logger.js";

const LEGACY_ENTITY_TYPES = [
  "Person",
  "Organization",
  "Location",
  "Email",
  "Domain",
  "Article",
  "Snippet",
] as const;

type EvidenceObjectRef = {
  bucket?: string;
  objectKey?: string;
  versionId?: string;
  etag?: string;
  documentId?: string;
};

type EvidenceInput = {
  documentId?: string;
  chunkId?: string;
  sourceUrl?: string;
  snippetId?: string;
  snippetText?: string;
  objectRef?: EvidenceObjectRef;
};

type LegacyRelationInput = {
  type: string;
  targetType: string;
  targetId?: string;
  targetProperties?: Record<string, unknown>;
  evidenceRef?: EvidenceObjectRef;
};

type LegacyBatchEntityInput = {
  entityType: string;
  entityId?: string;
  properties?: Record<string, unknown>;
  evidence?: EvidenceInput;
  relations?: LegacyRelationInput[];
};

type LegacyRelationTripletInput = {
  srcType: string;
  srcId?: string;
  srcProperties?: Record<string, unknown>;
  relType: string;
  dstType: string;
  dstId?: string;
  dstProperties?: Record<string, unknown>;
  evidenceRef?: EvidenceObjectRef;
};

type GraphEntityInput = {
  node_id?: string;
  type?: string;
  canonical_name?: string;
  alt_names?: string[];
  attributes?: string[];
  osint_bucket?: string;
  source_tools?: string[];
  evidence?: EvidenceInput;
};

type GraphRelationInput = {
  edge_id?: string;
  src_id: string;
  dst_id: string;
  rel_type?: string;
  canonical_name?: string;
  alt_names?: string[];
  source_tool?: string;
  evidenceRef?: EvidenceObjectRef;
};

type GraphEntityRecord = {
  node_id: string;
  type: string;
  canonical_name: string;
  canonical_name_normalized: string;
  alt_names: string[];
  alt_names_normalized: string[];
  attributes: string[];
  osint_bucket: string;
  filter_terms: string[];
  source_tools: string[];
  created_at: string;
  updated_at: string;
  evidence_document_id?: string;
  evidence_document_ids?: string[];
  evidence_bucket?: string;
  evidence_object_key?: string;
  evidence_version_id?: string;
  evidence_etag?: string;
  source_url?: string;
  snippet_text?: string;
  snippet_id?: string;
  embedding_text: string;
  embedding: number[];
};

type GraphRelationRecord = {
  edge_id: string;
  src_id: string;
  dst_id: string;
  rel_type: string;
  rel_type_normalized: string;
  canonical_name: string;
  canonical_name_normalized: string;
  alt_names: string[];
  alt_names_normalized: string[];
  source_tool?: string;
  created_at: string;
  updated_at: string;
  evidence_document_id?: string;
  evidence_document_ids?: string[];
  evidence_bucket?: string;
  evidence_object_key?: string;
  evidence_version_id?: string;
  evidence_etag?: string;
  embedding_text: string;
  embedding: number[];
};

const ENTITY_EMBEDDING_SCORE_THRESHOLD = 120;
const RELATION_EMBEDDING_SCORE_THRESHOLD = 220;

const toolSchema = {
  runId: z.string().uuid().describe("Run ID (UUID)"),
  entityType: z.string().describe("Legacy target entity type"),
  entityId: z.string().optional().describe("Legacy optional unique identifier"),
  propertiesJson: z.string().optional().describe("Legacy JSON string of entity properties"),
  evidenceJson: z.string().optional().describe("Legacy JSON string of evidence pointers"),
  relationsJson: z.string().optional().describe("Legacy JSON string array of relationships"),
};

const batchToolSchema = {
  runId: z.string().uuid().describe("Run ID (UUID)"),
  entitiesJson: z.string().describe("JSON array of graph entities"),
};

const relationsToolSchema = {
  runId: z.string().uuid().describe("Run ID (UUID)"),
  relationsJson: z.string().describe("JSON array of graph relations"),
};

function utcNow(): string {
  return new Date().toISOString();
}

function parseJson<T>(label: string, value?: string): T | undefined {
  if (!value) return undefined;
  try {
    return JSON.parse(value) as T;
  } catch {
    throw new Error(`Invalid ${label} JSON`);
  }
}

function normalizeName(value: string): string {
  return value
    .toLowerCase()
    .trim()
    .replace(/[\W_]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function uniqueStrings(values: Array<unknown>): string[] {
  const output: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (!text) continue;
    const normalized = normalizeName(text);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    output.push(text);
  }
  return output;
}

function chooseCanonicalName(values: Array<unknown>): string {
  const candidates = uniqueStrings(values);
  if (!candidates.length) return "unknown";
  return [...candidates].sort((a, b) => {
    if (b.length !== a.length) return b.length - a.length;
    return a.localeCompare(b);
  })[0];
}

function ensureStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return uniqueStrings(value);
}

function stableId(prefix: string, ...parts: string[]): string {
  const digest = crypto.createHash("sha256").update(parts.join("|")).digest("hex").slice(0, 20);
  return `${prefix}_${digest}`;
}

function deriveOsintBucket(entityType: string, names: string[], attributes: string[]): string {
  const joined = normalizeName([entityType, ...names, ...attributes].join(" "));
  if (/(person|author|researcher|employee|director|founder|officer|student)/.test(joined)) return "person";
  if (/(organization|company|institution|agency|university|lab|firm|committee)/.test(joined)) return "organization";
  if (/(domain|website|hostname|repository|repo|account|username|handle|email|phone)/.test(joined)) return "digital_asset";
  if (/(location|city|country|address|region|state)/.test(joined)) return "location";
  if (/(article|paper|publication|grant|patent|conference|report)/.test(joined)) return "publication";
  if (/(snippet|evidence|document|archive)/.test(joined)) return "evidence";
  return "unknown";
}

function deriveFilterTerms(entityType: string, canonicalName: string, altNames: string[], attributes: string[], bucket: string): string[] {
  const terms = uniqueStrings([entityType, bucket, canonicalName, ...altNames, ...attributes]);
  const tokens: string[] = [];
  for (const term of terms) {
    tokens.push(normalizeName(term));
    for (const token of normalizeName(term).split(" ")) {
      if (token) tokens.push(token);
    }
  }
  return uniqueStrings(tokens);
}

function buildEvidenceProps(input?: EvidenceInput | EvidenceObjectRef): Record<string, string | string[]> {
  const ref = "objectRef" in (input ?? {}) ? (input as EvidenceInput).objectRef : (input as EvidenceObjectRef | undefined);
  const evidenceDocumentId = "documentId" in (input ?? {}) ? (input as EvidenceInput).documentId : ref?.documentId;
  const props: Record<string, string | string[]> = {};
  if (evidenceDocumentId) {
    props.evidence_document_id = evidenceDocumentId;
    props.evidence_document_ids = [evidenceDocumentId];
  }
  if (ref?.bucket) props.evidence_bucket = ref.bucket;
  if (ref?.objectKey) props.evidence_object_key = ref.objectKey;
  if (ref?.versionId) props.evidence_version_id = ref.versionId;
  if (ref?.etag) props.evidence_etag = ref.etag;
  return props;
}

function selectPrimaryName(properties: Record<string, unknown>, fallback?: string): string {
  const candidates = [
    properties.canonical_name,
    properties.name,
    properties.title,
    properties.uri,
    properties.url,
    properties.domain,
    properties.email,
    properties.address,
    properties.username,
    fallback,
  ];
  return chooseCanonicalName(candidates);
}

function propertyAttributes(properties: Record<string, unknown>): string[] {
  const attributes: string[] = [];
  for (const [key, value] of Object.entries(properties)) {
    if (value === null || value === undefined) continue;
    if (["name", "title", "uri", "url", "domain", "email", "address", "canonical_name"].includes(key)) continue;
    if (typeof value === "string" && value.trim()) attributes.push(`${key}: ${value.trim()}`);
    if (typeof value === "number" || typeof value === "boolean") attributes.push(`${key}: ${String(value)}`);
    if (Array.isArray(value)) {
      for (const item of value) {
        if (typeof item === "string" && item.trim()) attributes.push(`${key}: ${item.trim()}`);
      }
    }
  }
  return uniqueStrings(attributes);
}

function normalizeGraphEntity(input: GraphEntityInput): GraphEntityRecord {
  const names = uniqueStrings([input.canonical_name, ...(input.alt_names ?? [])]);
  const canonicalName = chooseCanonicalName(names);
  const altNames = names.filter((name) => normalizeName(name) !== normalizeName(canonicalName));
  const attributes = ensureStringArray(input.attributes);
  const entityType = String(input.type ?? "Unknown").trim() || "Unknown";
  const bucket = String(input.osint_bucket ?? deriveOsintBucket(entityType, names, attributes)).trim() || "unknown";
  const createdAt = utcNow();
  const updatedAt = createdAt;
  const nodeId = String(input.node_id ?? stableId("ent", normalizeName(entityType), normalizeName(canonicalName)));
  const evidenceProps = buildEvidenceProps(input.evidence);

  return {
    node_id: nodeId,
    type: entityType,
    canonical_name: canonicalName,
    canonical_name_normalized: normalizeName(canonicalName),
    alt_names: altNames,
    alt_names_normalized: altNames.map((item) => normalizeName(item)),
    attributes,
    osint_bucket: bucket,
    filter_terms: deriveFilterTerms(entityType, canonicalName, altNames, attributes, bucket),
    source_tools: ensureStringArray(input.source_tools),
    created_at: createdAt,
    updated_at: updatedAt,
    source_url: input.evidence?.sourceUrl?.trim() || undefined,
    snippet_text: input.evidence?.snippetText?.trim() || undefined,
    snippet_id: input.evidence?.snippetId?.trim() || undefined,
    embedding_text: "",
    embedding: [],
    ...evidenceProps,
  };
}

function normalizeGraphRelation(input: GraphRelationInput): GraphRelationRecord {
  const relType = String(input.rel_type ?? "RELATED_TO").trim() || "RELATED_TO";
  const canonicalName = String(input.canonical_name ?? relType).trim() || relType;
  const altNames = ensureStringArray(input.alt_names);
  const evidenceProps = buildEvidenceProps(input.evidenceRef);
  const createdAt = utcNow();
  return {
    edge_id: String(
      input.edge_id ??
        stableId("rel", input.src_id, input.dst_id, normalizeName(relType), normalizeName(canonicalName))
    ),
    src_id: input.src_id,
    dst_id: input.dst_id,
    rel_type: relType,
    rel_type_normalized: normalizeName(relType),
    canonical_name: canonicalName,
    canonical_name_normalized: normalizeName(canonicalName),
    alt_names: altNames,
    alt_names_normalized: altNames.map((item) => normalizeName(item)),
    source_tool: input.source_tool?.trim() || undefined,
    created_at: createdAt,
    updated_at: createdAt,
    embedding_text: "",
    embedding: [],
    ...evidenceProps,
  };
}

function buildEntityEmbeddingText(entity: GraphEntityRecord): string {
  return uniqueStrings([entity.canonical_name, ...entity.alt_names]).join(" | ");
}

function buildRelationEmbeddingText(relation: GraphRelationRecord, nodeLookup: Map<string, GraphEntityRecord>): string {
  const src = nodeLookup.get(relation.src_id);
  const dst = nodeLookup.get(relation.dst_id);
  const srcName = src?.canonical_name ?? relation.src_id;
  const dstName = dst?.canonical_name ?? relation.dst_id;
  const names = uniqueStrings([relation.canonical_name, ...relation.alt_names]).join(" | ");
  return `${srcName} | ${relation.rel_type} | ${names} | ${dstName}`.trim();
}

async function ensureEntityEmbeddings(entities: GraphEntityRecord[]) {
  const pending = entities.filter((item) => item.embedding.length === 0);
  if (!pending.length) return;
  const texts = pending.map((item) => {
    const text = buildEntityEmbeddingText(item);
    item.embedding_text = text;
    return text;
  });
  const vectors = await embedTexts(texts);
  for (const [index, vector] of vectors.entries()) {
    pending[index].embedding = vector;
  }
}

async function ensureRelationEmbeddings(relations: GraphRelationRecord[], nodeLookup: Map<string, GraphEntityRecord>) {
  const pending = relations.filter((item) => item.embedding.length === 0);
  if (!pending.length) return;
  const texts = pending.map((item) => {
    const text = buildRelationEmbeddingText(item, nodeLookup);
    item.embedding_text = text;
    return text;
  });
  const vectors = await embedTexts(texts);
  for (const [index, vector] of vectors.entries()) {
    pending[index].embedding = vector;
  }
}

async function ensureGraphEmbeddings(entities: GraphEntityRecord[], relations: GraphRelationRecord[]) {
  await ensureEntityEmbeddings(entities);
  const nodeLookup = new Map(entities.map((item) => [item.node_id, item]));
  await ensureRelationEmbeddings(relations, nodeLookup);
}

function toNumericArray(value: unknown): number[] {
  if (!Array.isArray(value)) return [];
  const output: number[] = [];
  for (const item of value) {
    const n = Number(item);
    if (!Number.isFinite(n)) return [];
    output.push(n);
  }
  return output;
}

function extractEvidenceDocumentIds(value: Record<string, unknown> | GraphEntityRecord | GraphRelationRecord): string[] {
  return uniqueStrings([
    value.evidence_document_id,
    ...(((value.evidence_document_ids as string[] | undefined) ?? [])),
  ]);
}

function cosineSimilarity(left: number[], right: number[]): number {
  if (!left.length || !right.length || left.length !== right.length) return Number.NEGATIVE_INFINITY;
  let dot = 0;
  let leftNorm = 0;
  let rightNorm = 0;
  for (let idx = 0; idx < left.length; idx += 1) {
    const a = left[idx];
    const b = right[idx];
    dot += a * b;
    leftNorm += a * a;
    rightNorm += b * b;
  }
  const denom = Math.sqrt(leftNorm) * Math.sqrt(rightNorm);
  return denom > 0 ? dot / denom : Number.NEGATIVE_INFINITY;
}

function averageEmbeddings(vectors: number[][]): number[] {
  const valid = vectors.filter((item) => item.length > 0);
  if (!valid.length) return [];
  const dimension = valid[0].length;
  const aligned = valid.filter((item) => item.length === dimension);
  if (!aligned.length) return [];
  const output = new Array<number>(dimension).fill(0);
  for (const vector of aligned) {
    for (let idx = 0; idx < dimension; idx += 1) {
      output[idx] += vector[idx];
    }
  }
  return output.map((value) => value / aligned.length);
}

function embeddingBandScore(left: number[], right: number[]): number {
  const score = cosineSimilarity(left, right);
  if (!Number.isFinite(score)) return 0;
  if (score >= 0.95) return 220;
  if (score >= 0.9) return 160;
  if (score >= 0.85) return 100;
  if (score >= 0.8) return 60;
  return 0;
}

function normalizeLegacyEntity(input: LegacyBatchEntityInput): GraphEntityRecord {
  const properties = input.properties ?? {};
  const canonicalName = selectPrimaryName(properties, input.entityId);
  const altNames = uniqueStrings([
    properties.username,
    properties.handle,
    properties.display_name,
    ...(Array.isArray(properties.aliases) ? properties.aliases : []),
  ]);
  const attributes = propertyAttributes(properties);
  return normalizeGraphEntity({
    node_id: input.entityId,
    type: input.entityType,
    canonical_name: canonicalName,
    alt_names: altNames,
    attributes,
    evidence: input.evidence,
  });
}

function normalizeLegacyRelationTarget(targetType: string, targetId: string | undefined, targetProperties: Record<string, unknown>, evidence?: EvidenceObjectRef): GraphEntityRecord {
  const canonicalName = selectPrimaryName(targetProperties, targetId);
  return normalizeGraphEntity({
    node_id: targetId,
    type: targetType,
    canonical_name: canonicalName,
    alt_names: [],
    attributes: propertyAttributes(targetProperties),
    evidence: evidence ? { objectRef: evidence } : undefined,
  });
}

function scoreEntityCandidate(existing: Record<string, unknown>, incoming: GraphEntityRecord): number {
  let score = 0;
  if (existing.node_id === incoming.node_id) score += 1000;
  if (existing.canonical_name_normalized === incoming.canonical_name_normalized) score += 400;
  const existingNames = new Set<string>([
    String(existing.canonical_name_normalized ?? ""),
    ...((existing.alt_names_normalized as string[] | undefined) ?? []),
  ]);
  for (const name of [incoming.canonical_name_normalized, ...incoming.alt_names_normalized]) {
    if (existingNames.has(name)) score += 75;
  }
  const existingTerms = new Set<string>((existing.filter_terms as string[] | undefined) ?? []);
  for (const term of incoming.filter_terms.slice(0, 8)) {
    if (existingTerms.has(term)) score += 20;
  }
  if (existing.osint_bucket === incoming.osint_bucket) score += 25;
  if (existing.type === incoming.type) score += 10;
  score += embeddingBandScore(toNumericArray(existing.embedding), incoming.embedding);
  return score;
}

function scoreRelationCandidate(existing: Record<string, unknown>, incoming: GraphRelationRecord): number {
  let score = 0;
  if (existing.edge_id === incoming.edge_id) score += 1000;
  if (existing.src_id === incoming.src_id) score += 250;
  if (existing.dst_id === incoming.dst_id) score += 250;
  if (existing.rel_type_normalized === incoming.rel_type_normalized) score += 200;
  if (existing.canonical_name_normalized === incoming.canonical_name_normalized) score += 150;
  score += embeddingBandScore(toNumericArray(existing.embedding), incoming.embedding);
  return score;
}

function mergeEntityRecords(existing: Record<string, unknown> | null, incoming: GraphEntityRecord): GraphEntityRecord {
  if (!existing) return incoming;
  const canonicalName = chooseCanonicalName([existing.canonical_name, incoming.canonical_name, ...(existing.alt_names as string[] ?? []), ...incoming.alt_names]);
  const altNames = uniqueStrings([
    ...(existing.alt_names as string[] ?? []),
    ...(incoming.alt_names ?? []),
    String(existing.canonical_name ?? ""),
    incoming.canonical_name,
  ]).filter((name) => normalizeName(name) !== normalizeName(canonicalName));
  const attributes = uniqueStrings([...(existing.attributes as string[] ?? []), ...incoming.attributes]);
  const entityType = chooseCanonicalName([existing.type, incoming.type]);
  const bucket = String(existing.osint_bucket ?? incoming.osint_bucket);
  const evidenceDocumentIds = extractEvidenceDocumentIds(existing).concat(extractEvidenceDocumentIds(incoming));
  const mergedEvidenceDocumentIds = uniqueStrings(evidenceDocumentIds);
  const mergedEmbeddingText = buildEntityEmbeddingText({
    ...incoming,
    canonical_name: canonicalName,
    alt_names: altNames,
  });
  return {
    node_id: String(existing.node_id ?? incoming.node_id),
    type: entityType,
    canonical_name: canonicalName,
    canonical_name_normalized: normalizeName(canonicalName),
    alt_names: altNames,
    alt_names_normalized: altNames.map((item) => normalizeName(item)),
    attributes,
    osint_bucket: bucket,
    filter_terms: deriveFilterTerms(entityType, canonicalName, altNames, attributes, bucket),
    source_tools: uniqueStrings([...(existing.source_tools as string[] ?? []), ...incoming.source_tools]),
    created_at: String(existing.created_at ?? incoming.created_at),
    updated_at: incoming.updated_at,
    evidence_document_id: mergedEvidenceDocumentIds[0] || undefined,
    evidence_document_ids: mergedEvidenceDocumentIds.length ? mergedEvidenceDocumentIds : undefined,
    evidence_bucket: String(existing.evidence_bucket ?? incoming.evidence_bucket ?? "") || undefined,
    evidence_object_key: String(existing.evidence_object_key ?? incoming.evidence_object_key ?? "") || undefined,
    evidence_version_id: String(existing.evidence_version_id ?? incoming.evidence_version_id ?? "") || undefined,
    evidence_etag: String(existing.evidence_etag ?? incoming.evidence_etag ?? "") || undefined,
    source_url: String(existing.source_url ?? incoming.source_url ?? "") || undefined,
    snippet_text: String(existing.snippet_text ?? incoming.snippet_text ?? "") || undefined,
    snippet_id: String(existing.snippet_id ?? incoming.snippet_id ?? "") || undefined,
    embedding_text: mergedEmbeddingText,
    embedding: averageEmbeddings([toNumericArray(existing.embedding), incoming.embedding]),
  };
}

function mergeRelationRecords(existing: Record<string, unknown> | null, incoming: GraphRelationRecord): GraphRelationRecord {
  if (!existing) return incoming;
  const canonicalName = chooseCanonicalName([existing.canonical_name, incoming.canonical_name, ...(existing.alt_names as string[] ?? []), ...incoming.alt_names]);
  const altNames = uniqueStrings([...(existing.alt_names as string[] ?? []), ...incoming.alt_names]).filter(
    (name) => normalizeName(name) !== normalizeName(canonicalName)
  );
  const relType = chooseCanonicalName([existing.rel_type, incoming.rel_type]);
  const evidenceDocumentIds = extractEvidenceDocumentIds(existing).concat(extractEvidenceDocumentIds(incoming));
  const mergedEvidenceDocumentIds = uniqueStrings(evidenceDocumentIds);
  return {
    edge_id: String(existing.edge_id ?? incoming.edge_id),
    src_id: incoming.src_id,
    dst_id: incoming.dst_id,
    rel_type: relType,
    rel_type_normalized: normalizeName(relType),
    canonical_name: canonicalName,
    canonical_name_normalized: normalizeName(canonicalName),
    alt_names: altNames,
    alt_names_normalized: altNames.map((item) => normalizeName(item)),
    source_tool: String(existing.source_tool ?? incoming.source_tool ?? "") || undefined,
    created_at: String(existing.created_at ?? incoming.created_at),
    updated_at: incoming.updated_at,
    evidence_document_id: mergedEvidenceDocumentIds[0] || undefined,
    evidence_document_ids: mergedEvidenceDocumentIds.length ? mergedEvidenceDocumentIds : undefined,
    evidence_bucket: String(existing.evidence_bucket ?? incoming.evidence_bucket ?? "") || undefined,
    evidence_object_key: String(existing.evidence_object_key ?? incoming.evidence_object_key ?? "") || undefined,
    evidence_version_id: String(existing.evidence_version_id ?? incoming.evidence_version_id ?? "") || undefined,
    evidence_etag: String(existing.evidence_etag ?? incoming.evidence_etag ?? "") || undefined,
    embedding_text: incoming.embedding_text,
    embedding: averageEmbeddings([toNumericArray(existing.embedding), incoming.embedding]),
  };
}

async function findExistingEntity(
  session: ReturnType<typeof neo4jDriver.session>,
  incoming: GraphEntityRecord
): Promise<Record<string, unknown> | null> {
  let result = await session.run(
    `MATCH (n:Entity)
     WHERE n.node_id = $nodeId
        OR n.canonical_name_normalized = $canonical
        OR any(name IN coalesce(n.alt_names_normalized, []) WHERE name IN $names)
        OR any(term IN coalesce(n.filter_terms, []) WHERE term IN $terms)
     RETURN properties(n) AS props
     LIMIT 50`,
    {
      nodeId: incoming.node_id,
      canonical: incoming.canonical_name_normalized,
      names: [incoming.canonical_name_normalized, ...incoming.alt_names_normalized],
      terms: incoming.filter_terms.slice(0, 16),
    }
  );
  if (!result.records.length && incoming.embedding.length) {
    result = await session.run(
      `MATCH (n:Entity)
       WHERE n.type = $type OR n.osint_bucket = $bucket
       RETURN properties(n) AS props
       LIMIT 100`,
      {
        type: incoming.type,
        bucket: incoming.osint_bucket,
      }
    );
  }

  let best: Record<string, unknown> | null = null;
  let bestScore = 0;
  for (const record of result.records) {
    const props = record.get("props") as Record<string, unknown>;
    const score = scoreEntityCandidate(props, incoming);
    if (score > bestScore) {
      best = props;
      bestScore = score;
    }
  }
  return bestScore >= ENTITY_EMBEDDING_SCORE_THRESHOLD ? best : null;
}

async function findExistingRelation(
  session: ReturnType<typeof neo4jDriver.session>,
  incoming: GraphRelationRecord
): Promise<Record<string, unknown> | null> {
  const result = await session.run(
    `MATCH (:Entity {node_id: $srcId})-[r:RELATED_TO]->(:Entity {node_id: $dstId})
     WHERE r.edge_id = $edgeId
        OR r.rel_type_normalized = $relType
        OR r.canonical_name_normalized = $canonical
     RETURN properties(r) AS props
     LIMIT 25`,
    {
      srcId: incoming.src_id,
      dstId: incoming.dst_id,
      edgeId: incoming.edge_id,
      relType: incoming.rel_type_normalized,
      canonical: incoming.canonical_name_normalized,
    }
  );

  let best: Record<string, unknown> | null = null;
  let bestScore = 0;
  for (const record of result.records) {
    const props = record.get("props") as Record<string, unknown>;
    const score = scoreRelationCandidate(props, incoming);
    if (score > bestScore) {
      best = props;
      bestScore = score;
    }
  }
  return bestScore >= RELATION_EMBEDDING_SCORE_THRESHOLD ? best : null;
}

async function upsertGraphEntity(
  session: ReturnType<typeof neo4jDriver.session>,
  incoming: GraphEntityRecord
): Promise<GraphEntityRecord> {
  if (!incoming.embedding_text) {
    incoming.embedding_text = buildEntityEmbeddingText(incoming);
  }
  const existing = await findExistingEntity(session, incoming);
  const merged = mergeEntityRecords(existing, incoming);
  await session.run(
    `MERGE (n:Entity {node_id: $node_id})
     SET n += $props`,
    { node_id: merged.node_id, props: merged }
  );
  return merged;
}

async function upsertGraphRelation(
  session: ReturnType<typeof neo4jDriver.session>,
  incoming: GraphRelationRecord
): Promise<GraphRelationRecord> {
  const existing = await findExistingRelation(session, incoming);
  const merged = mergeRelationRecords(existing, incoming);
  await session.run(
    `MATCH (src:Entity {node_id: $src_id})
     MATCH (dst:Entity {node_id: $dst_id})
     MERGE (src)-[r:RELATED_TO {edge_id: $edge_id}]->(dst)
     SET r += $props`,
    { src_id: merged.src_id, dst_id: merged.dst_id, edge_id: merged.edge_id, props: merged }
  );
  return merged;
}

function parseEntityBatchPayload(value: string): { entities: GraphEntityRecord[]; relations: GraphRelationRecord[] } {
  const parsed = parseJson<unknown>("entities", value);
  if (!Array.isArray(parsed)) throw new Error("entitiesJson must be a JSON array");

  const entityMap = new Map<string, GraphEntityRecord>();
  const relations: GraphRelationRecord[] = [];

  const addEntity = (entity: GraphEntityRecord) => {
    entityMap.set(entity.node_id, entity);
  };

  for (const item of parsed) {
    const record = item as Record<string, unknown>;
    if ("canonical_name" in record || "node_id" in record) {
      addEntity(normalizeGraphEntity(record as GraphEntityInput));
      continue;
    }

    const legacy = record as LegacyBatchEntityInput;
    const sourceEntity = normalizeLegacyEntity(legacy);
    addEntity(sourceEntity);
    for (const relation of legacy.relations ?? []) {
      const targetEntity = normalizeLegacyRelationTarget(
        relation.targetType,
        relation.targetId,
        relation.targetProperties ?? {},
        relation.evidenceRef
      );
      addEntity(targetEntity);
      relations.push(
        normalizeGraphRelation({
          src_id: sourceEntity.node_id,
          dst_id: targetEntity.node_id,
          rel_type: relation.type,
          canonical_name: relation.type,
          evidenceRef: relation.evidenceRef,
        })
      );
    }
  }

  return { entities: [...entityMap.values()], relations };
}

function parseRelationsPayload(value: string): { entities: GraphEntityRecord[]; relations: GraphRelationRecord[] } {
  const parsed = parseJson<unknown>("relations", value);
  if (!Array.isArray(parsed)) throw new Error("relationsJson must be a JSON array");

  const entityMap = new Map<string, GraphEntityRecord>();
  const relations: GraphRelationRecord[] = [];
  for (const item of parsed) {
    const record = item as Record<string, unknown>;
    if ("src_id" in record || "edge_id" in record) {
      relations.push(normalizeGraphRelation(record as GraphRelationInput));
      continue;
    }

    const legacy = record as LegacyRelationTripletInput;
    const srcEntity = normalizeLegacyRelationTarget(legacy.srcType, legacy.srcId, legacy.srcProperties ?? {});
    const dstEntity = normalizeLegacyRelationTarget(legacy.dstType, legacy.dstId, legacy.dstProperties ?? {});
    entityMap.set(srcEntity.node_id, srcEntity);
    entityMap.set(dstEntity.node_id, dstEntity);
    relations.push(
      normalizeGraphRelation({
        src_id: srcEntity.node_id,
        dst_id: dstEntity.node_id,
        rel_type: legacy.relType,
        canonical_name: legacy.relType,
        evidenceRef: legacy.evidenceRef,
      })
    );
  }
  return { entities: [...entityMap.values()], relations };
}

export function registerIngestGraphEntity(server: McpServer) {
  server.registerTool(
    "ingest_graph_entity",
    {
      description: "Legacy single-entity ingest wrapper. Stores graph nodes in the sample-style Entity schema.",
      inputSchema: toolSchema,
    },
    async ({ runId, entityType, entityId, propertiesJson, evidenceJson, relationsJson }) => {
      await emitRunEvent(runId, "TOOL_CALL_STARTED", { tool: "ingest_graph_entity" });
      logger.info("ingest_graph_entity started", { runId, entityType, entityId });

      const session = neo4jDriver.session();
      let properties: Record<string, unknown> = {};
      let evidence: EvidenceInput | undefined;
      let relations: LegacyRelationInput[] = [];
      try {
        properties = parseJson<Record<string, unknown>>("properties", propertiesJson) ?? {};
        evidence = parseJson<EvidenceInput>("evidence", evidenceJson);
        relations = parseJson<LegacyRelationInput[]>("relations", relationsJson) ?? [];
        const { entities, relations: normalizedRelations } = parseEntityBatchPayload(
          JSON.stringify([{ entityType, entityId, properties, evidence, relations } satisfies LegacyBatchEntityInput])
        );
        await ensureGraphEmbeddings(entities, normalizedRelations);

        const storedEntities: GraphEntityRecord[] = [];
        for (const entity of entities) {
          storedEntities.push(await upsertGraphEntity(session, entity));
        }
        for (const relation of normalizedRelations) {
          await upsertGraphRelation(session, relation);
        }

        const primary = storedEntities[0];
        const output = {
          entityType: primary?.type ?? entityType,
          key: primary?.node_id ?? entityId ?? "",
          relationCount: normalizedRelations.length,
          graphSchema: "sample_v1",
        };

        await logToolCall(runId, "ingest_graph_entity", { entityType, entityId, properties, evidence, relations }, output, "ok");
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "ingest_graph_entity", ok: true, entityType });
        logger.info("ingest_graph_entity finished", { runId, entityType, key: output.key });
        return { content: [{ type: "text", text: JSON.stringify(output, null, 2) }] };
      } catch (error) {
        const errorMsg = (error as Error).message;
        await logToolCall(runId, "ingest_graph_entity", { entityType, entityId, properties, evidence, relations }, { error: errorMsg }, "error", errorMsg);
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "ingest_graph_entity", ok: false, error: errorMsg });
        logger.error("ingest_graph_entity failed", { runId, entityType, error: errorMsg });
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

export function registerIngestGraphEntities(server: McpServer) {
  server.registerTool(
    "ingest_graph_entities",
    {
      description: "Batch graph ingest using the sample-style node schema with OSINT filter properties.",
      inputSchema: batchToolSchema,
    },
    async ({ runId, entitiesJson }) => {
      await emitRunEvent(runId, "TOOL_CALL_STARTED", { tool: "ingest_graph_entities" });
      logger.info("ingest_graph_entities started", { runId });

      const session = neo4jDriver.session();
      try {
        const parsed = parseEntityBatchPayload(entitiesJson);
        await ensureGraphEmbeddings(parsed.entities, parsed.relations);
        const storedEntities: GraphEntityRecord[] = [];
        for (const entity of parsed.entities) {
          storedEntities.push(await upsertGraphEntity(session, entity));
        }
        for (const relation of parsed.relations) {
          await upsertGraphRelation(session, relation);
        }

        const output = {
          count: storedEntities.length,
          relationCount: parsed.relations.length,
          graphSchema: "sample_v1",
          entities: storedEntities.map((entity) => ({
            nodeId: entity.node_id,
            type: entity.type,
            canonicalName: entity.canonical_name,
            osintBucket: entity.osint_bucket,
          })),
          warnings: [],
        };

        await logToolCall(runId, "ingest_graph_entities", { entitiesJson }, output, "ok");
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "ingest_graph_entities", ok: true, count: storedEntities.length });
        logger.info("ingest_graph_entities finished", { runId, count: storedEntities.length });
        return { content: [{ type: "text", text: JSON.stringify(output, null, 2) }] };
      } catch (error) {
        const errorMsg = (error as Error).message;
        await logToolCall(runId, "ingest_graph_entities", { entitiesJson }, { error: errorMsg }, "error", errorMsg);
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "ingest_graph_entities", ok: false, error: errorMsg });
        logger.error("ingest_graph_entities failed", { runId, error: errorMsg });
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

export function registerIngestGraphRelations(server: McpServer) {
  server.registerTool(
    "ingest_graph_relations",
    {
      description: "Batch relation ingest using the sample-style edge schema.",
      inputSchema: relationsToolSchema,
    },
    async ({ runId, relationsJson }) => {
      await emitRunEvent(runId, "TOOL_CALL_STARTED", { tool: "ingest_graph_relations" });
      logger.info("ingest_graph_relations started", { runId });

      const session = neo4jDriver.session();
      try {
        const parsed = parseRelationsPayload(relationsJson);
        await ensureGraphEmbeddings(parsed.entities, parsed.relations);
        for (const entity of parsed.entities) {
          await upsertGraphEntity(session, entity);
        }
        const storedRelations: GraphRelationRecord[] = [];
        for (const relation of parsed.relations) {
          storedRelations.push(await upsertGraphRelation(session, relation));
        }

        const output = {
          count: storedRelations.length,
          graphSchema: "sample_v1",
          warnings: [],
        };

        await logToolCall(runId, "ingest_graph_relations", { relationsJson }, output, "ok");
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "ingest_graph_relations", ok: true, count: storedRelations.length });
        logger.info("ingest_graph_relations finished", { runId, count: storedRelations.length });
        return { content: [{ type: "text", text: JSON.stringify(output, null, 2) }] };
      } catch (error) {
        const errorMsg = (error as Error).message;
        await logToolCall(runId, "ingest_graph_relations", { relationsJson }, { error: errorMsg }, "error", errorMsg);
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "ingest_graph_relations", ok: false, error: errorMsg });
        logger.error("ingest_graph_relations failed", { runId, error: errorMsg });
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
