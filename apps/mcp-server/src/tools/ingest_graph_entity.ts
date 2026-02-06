import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import crypto from "node:crypto";
import { neo4jDriver } from "../clients/neo4j.js";
import { cfg } from "../config.js";
import { emitRunEvent, logToolCall } from "./helpers.js";
import { logger } from "../utils/logger.js";

const ENTITY_TYPES = [
  "Person",
  "Organization",
  "Location",
  "Email",
  "Domain",
  "Article",
  "Snippet",
] as const;

type EntityType = (typeof ENTITY_TYPES)[number];

type RelationInput = {
  type: string;
  targetType: EntityType;
  targetId?: string;
  targetProperties?: Record<string, unknown>;
  evidenceRef?: EvidenceObjectRef;
};

type EvidenceInput = {
  documentId?: string;
  chunkId?: string;
  sourceUrl?: string;
  snippetId?: string;
  snippetText?: string;
  objectRef?: EvidenceObjectRef;
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

type BatchEntityInput = {
  entityType: EntityType;
  entityId?: string;
  properties?: Record<string, unknown>;
  evidence?: EvidenceInput;
  relations?: RelationInput[];
};

type RelationTripletInput = {
  srcType: EntityType;
  srcId?: string;
  srcProperties?: Record<string, unknown>;
  relType: string;
  dstType: EntityType;
  dstId?: string;
  dstProperties?: Record<string, unknown>;
  evidenceRef?: EvidenceObjectRef;
};


const toolSchema = {
  runId: z.string().uuid().describe("Run ID (UUID)"),
  entityType: z
    .string()
    .refine((value) => ENTITY_TYPES.includes(value as EntityType), "Invalid entityType")
    .describe("Target entity label"),
  entityId: z.string().optional().describe("Optional unique identifier (e.g., person_id, org_id)"),
  propertiesJson: z
    .string()
    .optional()
    .describe("JSON string of entity properties (name, address, uri, etc.)"),
  evidenceJson: z
    .string()
    .optional()
    .describe("JSON string of evidence pointers (documentId, chunkId, sourceUrl, snippetId, snippetText, objectRef)"),
  relationsJson: z
    .string()
    .optional()
    .describe("JSON string array of relationships to create"),
};

const batchToolSchema = {
  runId: z.string().uuid().describe("Run ID (UUID)"),
  entitiesJson: z
    .string()
    .describe("JSON array of entities to ingest in one call"),
};

const relationsToolSchema = {
  runId: z.string().uuid().describe("Run ID (UUID)"),
  relationsJson: z
    .string()
    .describe("JSON array of relation triplets to merge"),
};

async function geocodeToLatLon(address: string): Promise<{ lat: number | null; lon: number | null }> {
  const url = new URL("https://nominatim.openstreetmap.org/search");
  url.searchParams.set("format", "json");
  url.searchParams.set("q", address);
  url.searchParams.set("limit", "1");

  const response = await fetch(url.toString(), {
    headers: { "User-Agent": "osint-mcp-bot/1.0" },
  });

  if (!response.ok) {
    return { lat: null, lon: null };
  }

  const data = (await response.json()) as Array<{ lat: string; lon: string }>;
  if (!data.length) {
    return { lat: null, lon: null };
  }

  const lat = parseFloat(data[0].lat);
  const lon = parseFloat(data[0].lon);
  if (Number.isNaN(lat) || Number.isNaN(lon)) {
    return { lat: null, lon: null };
  }

  return { lat, lon };
}

function resolveEntityKey(entityType: EntityType, entityId?: string, properties?: Record<string, unknown>) {
  if (entityId) {
    return { key: entityId, propKey: `${entityType.toLowerCase()}_id` };
  }

  const props = properties ?? {};
  if (entityType === "Location" && props.location_id) {
    return { key: props.location_id as string, propKey: "location_id" };
  }
  if (entityType === "Email" && props.address_normalized) {
    return { key: props.address_normalized as string, propKey: "address" };
  }
  if (entityType === "Domain" && props.name_normalized) {
    return { key: props.name_normalized as string, propKey: "name" };
  }
  if ((entityType === "Person" || entityType === "Organization") && props.name_normalized) {
    return { key: props.name_normalized as string, propKey: "name" };
  }
  if (entityType === "Article" && props.uri_normalized) {
    return { key: props.uri_normalized as string, propKey: "uri" };
  }
  const fallbackKey =
    (props.person_id as string) ||
    (props.org_id as string) ||
    (props.location_id as string) ||
    (props.email as string) ||
    (props.address as string) ||
    (props.uri as string) ||
    (props.name as string);

  if (!fallbackKey) {
    throw new Error("Missing entityId or identifiable property for merge");
  }

  return { key: fallbackKey, propKey: entityType === "Location" ? "name" : `${entityType.toLowerCase()}_id` };
}

function normalizeEmail(address: string) {
  return address.trim().toLowerCase();
}

function normalizeName(name: string) {
  return name.trim().replace(/\s+/g, " ").toLowerCase();
}

function normalizeDomain(value: string) {
  const trimmed = value.trim().toLowerCase();
  return trimmed.replace(/^www\./, "");
}

function normalizeUrl(value: string) {
  try {
    const url = new URL(value);
    url.hash = "";
    url.hostname = url.hostname.toLowerCase();
    return url.toString().replace(/\/$/, "");
  } catch {
    return value.trim();
  }
}

function normalizeForEntity(entityType: EntityType, props: Record<string, unknown>) {
  if (entityType === "Email") {
    const address = props.address as string | undefined;
    if (address) {
      props.address_normalized = normalizeEmail(address);
      props.address = props.address_normalized;
    }
  }

  if (entityType === "Domain") {
    const name = (props.name as string | undefined) ?? (props.domain as string | undefined);
    if (name) {
      props.name_normalized = normalizeDomain(name);
      props.name = props.name_normalized;
    }
  }

  if (entityType === "Article") {
    const uri = (props.uri as string | undefined) ?? (props.url as string | undefined);
    if (uri) {
      props.uri_normalized = normalizeUrl(uri);
      props.uri = props.uri_normalized;
    }
  }

  if (entityType === "Person" || entityType === "Organization") {
    const name = props.name as string | undefined;
    if (name) {
      props.name_normalized = normalizeName(name);
    }
  }
}

function parseJson<T>(label: string, value?: string): T | undefined {
  if (!value) return undefined;
  try {
    return JSON.parse(value) as T;
  } catch (error) {
    throw new Error(`Invalid ${label} JSON`);
  }
}

function ensureEntityType(value: string): EntityType {
  if (!ENTITY_TYPES.includes(value as EntityType)) {
    throw new Error(`Invalid entityType: ${value}`);
  }
  return value as EntityType;
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

function stripExternalUrlProps(
  entityType: EntityType,
  props: Record<string, unknown>,
  warnings: string[],
  context: string
) {
  if (entityType === "Article") return;
  const keys = ["url", "uri", "source_url", "sourceUrl", "website", "homepage"];
  let removed = false;
  for (const key of keys) {
    if (key in props) {
      delete props[key];
      removed = true;
    }
  }
  if (removed) {
    warnings.push(`Removed external URL fields from ${context} (${entityType})`);
  }
}

async function upsertEntity(
  session: ReturnType<typeof neo4jDriver.session>,
  typedEntityType: EntityType,
  entityId: string | undefined,
  properties: Record<string, unknown>
) {
  const props = { ...properties } as Record<string, unknown>;
  normalizeForEntity(typedEntityType, props);

  let key: string;
  let propKey: string;

  if (typedEntityType === "Location") {
    const address = props.address as string | undefined;
    let lat = props.lat as number | undefined;
    let lon = props.lon as number | undefined;

    if ((typeof lat !== "number" || typeof lon !== "number") && address) {
      const geo = await geocodeToLatLon(address);
      lat = geo.lat ?? lat;
      lon = geo.lon ?? lon;
    }

    if (typeof lat !== "number" || typeof lon !== "number") {
      throw new Error("Location requires lat/lon or address to geocode.");
    }

    props.lat = lat;
    props.lon = lon;

    const threshold = cfg.location.mergeThresholdMeters;
    const existing = await session.run(
      `MATCH (l:Location)
       WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL
         AND point.distance(point({latitude: l.lat, longitude: l.lon}), point({latitude: $lat, longitude: $lon})) < $threshold
       RETURN l, id(l) as nodeId
       LIMIT 1`,
      { lat, lon, threshold }
    );

    if (existing.records.length) {
      const record = existing.records[0];
      const nodeId = record.get("nodeId") as number;
      const existingNode = record.get("l");
      const locationId = (existingNode.properties?.location_id as string) ?? crypto.randomUUID();

      await session.run(
        `MATCH (l) WHERE id(l) = $nodeId
         SET l += $props,
             l.location_id = coalesce(l.location_id, $locationId)
         RETURN l`,
        { nodeId, props, locationId }
      );

      key = locationId;
      propKey = "location_id";
    } else {
      const locationId = (props.location_id as string) ?? crypto.randomUUID();
      props.location_id = locationId;

      const mergeQuery = `MERGE (e:Location { location_id: $key })
        SET e += $props
        RETURN e`;
      await session.run(mergeQuery, { key: locationId, props });
      key = locationId;
      propKey = "location_id";
    }

    return { key, propKey, props, lat: props.lat ?? null, lon: props.lon ?? null };
  }

  const resolved = resolveEntityKey(typedEntityType, entityId, props);
  key = resolved.key;
  propKey = resolved.propKey;
  const mergeQuery = `MERGE (e:${typedEntityType} { ${propKey}: $key })
    SET e += $props
    RETURN e`;

  await session.run(mergeQuery, { key, props });
  return { key, propKey, props, lat: null, lon: null };
}

async function ingestEntityWithRelations(
  session: ReturnType<typeof neo4jDriver.session>,
  entity: BatchEntityInput
) {
  const typedEntityType = entity.entityType;
  const props = entity.properties ?? {};
  const evidence = entity.evidence;
  const relations = entity.relations ?? [];
  const warnings: string[] = [];

  stripExternalUrlProps(typedEntityType, props, warnings, `entity ${typedEntityType}`);
  const { key, propKey, lat, lon } = await upsertEntity(session, typedEntityType, entity.entityId, props);

  const evidenceProps = buildEvidenceProps(evidence?.objectRef);
  if (evidenceProps) {
    await session.run(
      `MATCH (e:${typedEntityType} { ${propKey}: $key })
       SET e += $evidenceProps
       RETURN e`,
      { key, evidenceProps }
    );
  } else if (!hasEvidenceLink(evidence?.objectRef)) {
    warnings.push(`Missing evidence reference for ${typedEntityType} ${key}`);
  }

  if (evidence?.snippetText || evidence?.snippetId) {
    const snippetId = evidence.snippetId ?? crypto.randomUUID();
    const snippetText = evidence.snippetText ?? "";
    const sourceUrl = typedEntityType === "Article" ? (evidence.sourceUrl ?? null) : null;
    if (evidence?.sourceUrl && typedEntityType !== "Article") {
      warnings.push(`Ignored external sourceUrl for ${typedEntityType} evidence`);
    }

    await session.run(
      `MERGE (s:Snippet {snippet_id: $snippet_id})
       SET s.text = $text, s.extracted_at = datetime()
       WITH s
       MATCH (e:${typedEntityType} { ${propKey}: $key })
       MERGE (s)-[:EVIDENCE_FOR]->(e)
       WITH s
       FOREACH (_ IN CASE WHEN $sourceUrl IS NULL THEN [] ELSE [1] END |
         MERGE (a:Article {uri: $sourceUrl})
         MERGE (a)-[:HAS_SNIPPET]->(s)
       )
       RETURN s`,
      { snippet_id: snippetId, text: snippetText, key, sourceUrl }
    );
  }

  if (relations.length) {
    for (const rel of relations) {
      const relProps = rel.targetProperties ?? {};
      const relEvidenceRef = rel.evidenceRef ?? evidence?.objectRef;
      const relEvidenceProps = buildEvidenceProps(relEvidenceRef);
      if (!relEvidenceProps && !hasEvidenceLink(relEvidenceRef)) {
        warnings.push(`Missing evidence reference for relation ${typedEntityType}-${rel.type}-${rel.targetType}`);
      }
      stripExternalUrlProps(rel.targetType, relProps, warnings, `relation target ${rel.targetType}`);
      normalizeForEntity(rel.targetType, relProps);
      const { key: targetKey, propKey: targetPropKey } = resolveEntityKey(
        rel.targetType as EntityType,
        rel.targetId,
        relProps
      );

      await session.run(
        `MERGE (a:${typedEntityType} { ${propKey}: $sourceKey })
         MERGE (b:${rel.targetType} { ${targetPropKey}: $targetKey })
         SET b += $targetProps
         MERGE (a)-[r:${rel.type}]->(b)
         SET r += $relEvidenceProps
         RETURN r`,
        {
          sourceKey: key,
          targetKey,
          targetProps: relProps,
          relEvidenceProps: relEvidenceProps ?? {},
        }
      );
    }
  }

  return {
    entityType: typedEntityType,
    key,
    propKey,
    lat,
    lon,
    evidenceLinked: Boolean(evidence?.snippetText || evidence?.snippetId || hasEvidenceLink(evidence?.objectRef)),
    relationCount: relations.length,
    warnings,
  };
}

export function registerIngestGraphEntity(server: McpServer) {
  server.registerTool(
    "ingest_graph_entity",
    {
      description:
        "Ingest graph entities and relationships with evidence. Use for Person/Organization/Location/Email/Domain/Article/Snippet. If address is provided for Location, tool will geocode to plus_code and merge by plus_code.",
      inputSchema: toolSchema,
    },
    async ({ runId, entityType, entityId, propertiesJson, evidenceJson, relationsJson }) => {
      await emitRunEvent(runId, "TOOL_CALL_STARTED", { tool: "ingest_graph_entity" });
      logger.info("ingest_graph_entity started", { runId, entityType, entityId });

      const session = neo4jDriver.session();
      let properties: Record<string, unknown> = {};
      let evidence: EvidenceInput | undefined;
      let relations: RelationInput[] = [];
      try {
        const typedEntityType = ensureEntityType(entityType);
        properties = parseJson<Record<string, unknown>>("properties", propertiesJson) ?? {};
        evidence = parseJson<EvidenceInput>("evidence", evidenceJson);
        relations = parseJson<RelationInput[]>("relations", relationsJson) ?? [];

        const output = await ingestEntityWithRelations(session, {
          entityType: typedEntityType,
          entityId,
          properties,
          evidence,
          relations,
        });

        await logToolCall(runId, "ingest_graph_entity", { entityType, entityId, properties, evidence, relations }, output, "ok");
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "ingest_graph_entity", ok: true, entityType });
        logger.info("ingest_graph_entity finished", { runId, entityType, key: output.key });

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
        await logToolCall(runId, "ingest_graph_entity", { entityType, entityId, properties, evidence, relations }, { error: errorMsg }, "error", errorMsg);
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "ingest_graph_entity", ok: false, error: errorMsg });
        logger.error("ingest_graph_entity failed", { runId, entityType, error: errorMsg });

        return {
          content: [
            {
              type: "text",
              text: JSON.stringify({ error: errorMsg }, null, 2),
            },
          ],
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
      description:
        "Batch ingest graph entities with evidence and optional relations. Use for lists of extracted entities to avoid one-by-one calls.",
      inputSchema: batchToolSchema,
    },
    async ({ runId, entitiesJson }) => {
      await emitRunEvent(runId, "TOOL_CALL_STARTED", { tool: "ingest_graph_entities" });
      logger.info("ingest_graph_entities started", { runId });

      const session = neo4jDriver.session();
      let entities: BatchEntityInput[] = [];
      try {
        const parsed = parseJson<unknown>("entities", entitiesJson);
        if (!Array.isArray(parsed)) {
          throw new Error("entitiesJson must be a JSON array");
        }

        entities = parsed.map((item) => {
          const record = item as BatchEntityInput;
          return {
            entityType: ensureEntityType(record.entityType),
            entityId: record.entityId,
            properties: record.properties ?? {},
            evidence: record.evidence,
            relations: record.relations ?? [],
          };
        });

        const results = [] as Array<Awaited<ReturnType<typeof ingestEntityWithRelations>>>;
        for (const entity of entities) {
          const output = await ingestEntityWithRelations(session, entity);
          results.push(output);
        }

        const output = {
          count: results.length,
          entities: results,
        };

        await logToolCall(runId, "ingest_graph_entities", { entities }, output, "ok");
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "ingest_graph_entities", ok: true, count: results.length });
        logger.info("ingest_graph_entities finished", { runId, count: results.length });

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
        await logToolCall(runId, "ingest_graph_entities", { entities }, { error: errorMsg }, "error", errorMsg);
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "ingest_graph_entities", ok: false, error: errorMsg });
        logger.error("ingest_graph_entities failed", { runId, error: errorMsg });

        return {
          content: [
            {
              type: "text",
              text: JSON.stringify({ error: errorMsg }, null, 2),
            },
          ],
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
      description:
        "Merge relation triplets between entities. Use after entity extraction to connect nodes.",
      inputSchema: relationsToolSchema,
    },
    async ({ runId, relationsJson }) => {
      await emitRunEvent(runId, "TOOL_CALL_STARTED", { tool: "ingest_graph_relations" });
      logger.info("ingest_graph_relations started", { runId });

      const session = neo4jDriver.session();
      let relations: RelationTripletInput[] = [];
      const warnings: string[] = [];
      try {
        const parsed = parseJson<unknown>("relations", relationsJson);
        if (!Array.isArray(parsed)) {
          throw new Error("relationsJson must be a JSON array");
        }

        relations = parsed.map((item) => {
          const record = item as RelationTripletInput;
          if (!record.relType) {
            throw new Error("Relation requires relType");
          }
          return {
            srcType: ensureEntityType(record.srcType),
            srcId: record.srcId,
            srcProperties: record.srcProperties ?? {},
            relType: record.relType,
            dstType: ensureEntityType(record.dstType),
            dstId: record.dstId,
            dstProperties: record.dstProperties ?? {},
          };
        });

        for (const relation of relations) {
          const srcProps = relation.srcProperties ?? {};
          const dstProps = relation.dstProperties ?? {};
          const relEvidenceProps = buildEvidenceProps(relation.evidenceRef);
          if (!relEvidenceProps && !hasEvidenceLink(relation.evidenceRef)) {
            warnings.push(`Missing evidence reference for relation ${relation.srcType}-${relation.relType}-${relation.dstType}`);
          }

          stripExternalUrlProps(relation.srcType, srcProps, warnings, `relation source ${relation.srcType}`);
          stripExternalUrlProps(relation.dstType, dstProps, warnings, `relation target ${relation.dstType}`);
          normalizeForEntity(relation.srcType, srcProps);
          normalizeForEntity(relation.dstType, dstProps);

          const { key: srcKey, propKey: srcPropKey } = resolveEntityKey(
            relation.srcType,
            relation.srcId,
            srcProps
          );
          const { key: dstKey, propKey: dstPropKey } = resolveEntityKey(
            relation.dstType,
            relation.dstId,
            dstProps
          );

          await session.run(
            `MERGE (a:${relation.srcType} { ${srcPropKey}: $srcKey })
             SET a += $srcProps
             MERGE (b:${relation.dstType} { ${dstPropKey}: $dstKey })
             SET b += $dstProps
             MERGE (a)-[r:${relation.relType}]->(b)
             SET r += $relEvidenceProps
             RETURN r`,
            {
              srcKey,
              dstKey,
              srcProps,
              dstProps,
              relEvidenceProps: relEvidenceProps ?? {},
            }
          );
        }

        const output = {
          count: relations.length,
          warnings,
        };

        await logToolCall(runId, "ingest_graph_relations", { relations }, output, "ok");
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "ingest_graph_relations", ok: true, count: relations.length });
        logger.info("ingest_graph_relations finished", { runId, count: relations.length });

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
        await logToolCall(runId, "ingest_graph_relations", { relations }, { error: errorMsg }, "error", errorMsg);
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "ingest_graph_relations", ok: false, error: errorMsg });
        logger.error("ingest_graph_relations failed", { runId, error: errorMsg });

        return {
          content: [
            {
              type: "text",
              text: JSON.stringify({ error: errorMsg }, null, 2),
            },
          ],
          isError: true,
        };
      } finally {
        await session.close();
      }
    }
  );
}
