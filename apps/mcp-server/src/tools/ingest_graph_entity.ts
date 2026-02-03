import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import crypto from "node:crypto";
import { neo4jDriver } from "../clients/neo4j.js";
import { cfg } from "../config.js";
import { emitRunEvent, logToolCall } from "./helpers.js";

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
    .describe("JSON string of evidence pointers (documentId, chunkId, sourceUrl, snippetId, snippetText)"),
  relationsJson: z
    .string()
    .optional()
    .describe("JSON string array of relationships to create"),
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

      const session = neo4jDriver.session();
      let properties: Record<string, unknown> = {};
      let evidence: { documentId?: string; chunkId?: string; sourceUrl?: string; snippetId?: string; snippetText?: string } | undefined;
      let relations: RelationInput[] = [];
      try {
        const typedEntityType = entityType as EntityType;
        properties = parseJson<Record<string, unknown>>("properties", propertiesJson) ?? {};
        evidence = parseJson<{ documentId?: string; chunkId?: string; sourceUrl?: string; snippetId?: string; snippetText?: string }>(
          "evidence",
          evidenceJson
        );
        relations = parseJson<RelationInput[]>("relations", relationsJson) ?? [];

        const props = { ...properties } as Record<string, unknown>;

        normalizeForEntity(typedEntityType, props);

        let key: string;
        let propKey: string;
        let node: unknown;

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
            node = existingNode;
          } else {
            const locationId = (props.location_id as string) ?? crypto.randomUUID();
            props.location_id = locationId;

            const mergeQuery = `MERGE (e:Location { location_id: $key })
              SET e += $props
              RETURN e`;
            const result = await session.run(mergeQuery, { key: locationId, props });
            node = result.records[0]?.get("e");
            key = locationId;
            propKey = "location_id";
          }
        } else {
          const resolved = resolveEntityKey(typedEntityType, entityId, props);
          key = resolved.key;
          propKey = resolved.propKey;
          const mergeQuery = `MERGE (e:${typedEntityType} { ${propKey}: $key })
      SET e += $props
      RETURN e`;

          const result = await session.run(mergeQuery, { key, props });
          node = result.records[0]?.get("e");
        }

        if (evidence?.snippetText || evidence?.snippetId) {
          const snippetId = evidence.snippetId ?? crypto.randomUUID();
          const snippetText = evidence.snippetText ?? "";
          const sourceUrl = evidence.sourceUrl ?? null;

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
               RETURN r`,
              {
                sourceKey: key,
                targetKey,
                targetProps: relProps,
              }
            );
          }
        }

        const output = {
          entityType: typedEntityType,
          key,
          propKey,
          lat: props.lat ?? null,
          lon: props.lon ?? null,
          evidenceLinked: Boolean(evidence?.snippetText || evidence?.snippetId),
          relationCount: relations.length,
        };

        await logToolCall(runId, "ingest_graph_entity", { entityType, entityId, properties, evidence, relations }, output, "ok");
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "ingest_graph_entity", ok: true, entityType });

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
