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
  raw_type?: string;
  canonical_name?: string;
  alt_names?: string[];
  attributes?: string[];
  merge_keys?: string[];
  osint_bucket?: string;
  source_tools?: string[];
  evidence?: EvidenceInput;
};

type GraphRelationInput = {
  edge_id?: string;
  src_id: string;
  dst_id: string;
  src_canonical_id?: string;
  dst_canonical_id?: string;
  rel_type?: string;
  raw_relation_type?: string;
  canonical_name?: string;
  alt_names?: string[];
  source_tool?: string;
  evidenceRef?: EvidenceObjectRef;
};

type GraphEntityRecord = {
  node_id: string;
  canonical_id: string;
  run_scoped_id: string;
  run_id: string;
  external_context: boolean;
  type: string;
  raw_type: string;
  canonical_name: string;
  canonical_name_normalized: string;
  alt_names: string[];
  alt_names_normalized: string[];
  attributes: string[];
  merge_keys: string[];
  osint_bucket: string;
  filter_terms: string[];
  source_tools: string[];
  created_at: string;
  updated_at: string;
  evidence_document_id?: string;
  evidence_document_ids?: string[];
  evidence_bucket?: string;
  evidence_object_key?: string;
  evidence_object_keys?: string[];
  evidence_version_id?: string;
  evidence_etag?: string;
  source_url?: string;
  source_urls?: string[];
  source_domain?: string;
  snippet_text?: string;
  snippet_id?: string;
  ingested_at: string;
  embedding_text: string;
  embedding: number[];
};

type GraphRelationRecord = {
  edge_id: string;
  canonical_id: string;
  run_scoped_id: string;
  run_id: string;
  external_context: boolean;
  src_id: string;
  dst_id: string;
  rel_type: string;
  raw_relation_type: string;
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
  evidence_object_keys?: string[];
  evidence_version_id?: string;
  evidence_etag?: string;
  ingested_at: string;
  embedding_text: string;
  embedding: number[];
};

const ENTITY_EMBEDDING_SCORE_THRESHOLD = 120;
const RELATION_EMBEDDING_SCORE_THRESHOLD = 220;
let graphSchemaBootstrapped = false;

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

const CONTRACT_ENTITY_TYPES = new Set<string>([
  "Person",
  "Organization",
  "Institution",
  "ContactPoint",
  "Website",
  "Domain",
  "Email",
  "Phone",
  "Handle",
  "Experience",
  "EducationalCredential",
  "Affiliation",
  "Role",
  "Publication",
  "Document",
  "Conference",
  "Repository",
  "Project",
  "Topic",
  "TimelineEvent",
  "TimeNode",
  "Occupation",
  "OrganizationProfile",
  "ImageObject",
]);

const CONTRACT_RELATION_TYPES = new Set<string>([
  "HAS_PROFILE",
  "HAS_DOCUMENT",
  "HAS_HANDLE",
  "HAS_EMAIL",
  "HAS_PHONE",
  "HAS_CONTACT_POINT",
  "HAS_DOMAIN",
  "HAS_CREDENTIAL",
  "HAS_EXPERIENCE",
  "HAS_AFFILIATION",
  "HAS_TIMELINE_EVENT",
  "HAS_OCCUPATION",
  "HAS_IMAGE",
  "HAS_ORGANIZATION_PROFILE",
  "HAS_ROLE",
  "HOLDS_ROLE",
  "WORKS_AT",
  "STUDIED_AT",
  "AFFILIATED_WITH",
  "MEMBER_OF",
  "ISSUED_BY",
  "OFFICER_OF",
  "DIRECTOR_OF",
  "FOUNDED",
  "COAUTHORED_WITH",
  "ADVISED_BY",
  "COLLEAGUE_OF",
  "COLLABORATED_WITH",
  "PUBLISHED",
  "PUBLISHED_IN",
  "MAINTAINS",
  "USES_LANGUAGE",
  "KNOWS_LANGUAGE",
  "RESEARCHES",
  "FOCUSES_ON",
  "HAS_TOPIC",
  "HAS_SKILL_TOPIC",
  "HAS_HOBBY_TOPIC",
  "HAS_INTEREST_TOPIC",
  "MENTIONS_TIMELINE_EVENT",
  "IN_TIME_NODE",
  "NEXT_TIME_NODE",
  "ABOUT",
  "FILED",
  "APPEARS_IN_ARCHIVE",
  "MENTIONS",
  "RELATED_TO",
  "SAME_CANONICAL_AS",
]);

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

function isUrlLike(value: string): boolean {
  return /^https?:\/\//i.test(String(value ?? "").trim());
}

function isLikelyDomain(value: string): boolean {
  return /^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}$/i.test(String(value ?? "").trim());
}

function looksLikePersonName(value: string): boolean {
  const text = String(value ?? "").trim();
  if (!text || isUrlLike(text) || isLikelyDomain(text)) return false;
  if (/[@/:]|github|linkedin|researchgate|duckduckgo/i.test(text)) return false;
  const normalized = normalizeName(text);
  const tokens = normalized.split(" ").filter(Boolean);
  if (tokens.length < 2 || tokens.length > 5) return false;
  if (tokens.some((token) => token.length === 1)) return false;
  return tokens.every((token) => /^[a-z][a-z'-]*$/.test(token));
}

function looksLikeOrganizationName(value: string): boolean {
  const normalized = normalizeName(String(value ?? ""));
  if (!normalized || isUrlLike(value) || isLikelyDomain(value)) return false;
  return /\b(university|college|institute|school|company|corporation|corp|lab|laboratory|agency|startup|committee|group)\b/.test(normalized);
}

function extractAttributeValues(attributes: string[], prefixes: string[]): string[] {
  const allowed = new Set(prefixes.map((value) => value.trim().toLowerCase()).filter(Boolean));
  const values: string[] = [];
  for (const attribute of attributes) {
    if (typeof attribute !== "string") continue;
    const separatorIndex = attribute.indexOf(":");
    if (separatorIndex < 0) continue;
    const key = attribute.slice(0, separatorIndex).trim().toLowerCase();
    if (!allowed.has(key)) continue;
    const value = attribute.slice(separatorIndex + 1).trim();
    if (value) values.push(value);
  }
  return uniqueStrings(values);
}

function graphAutoAliases(entityType: string, values: string[]): string[] {
  const aliases: string[] = [];
  const family = entityType.toLowerCase();
  const conservativeFamilies = new Set(["contactpoint", "educationalcredential", "experience", "affiliation", "timelineevent", "timenode", "occupation", "imageobject", "organizationprofile"]);
  const stopwords = new Set(["the", "of", "at", "for", "and", "in", "on", "to"]);
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (!text) continue;
    const withoutParens = text.replace(/\s*\([^)]*\)\s*/g, " ").replace(/\s+/g, " ").trim();
    if (!conservativeFamilies.has(family) && withoutParens && normalizeName(withoutParens) !== normalizeName(text)) {
      aliases.push(withoutParens);
    }
    if (!conservativeFamilies.has(family) && /\sat\s/i.test(text)) {
      aliases.push(text.replace(/\bat\b/gi, " ").replace(/\s+/g, " ").trim());
    }
    const match = text.match(/\(([^)]+)\)/);
    if (!conservativeFamilies.has(family) && match && /^[A-Za-z0-9._-]{2,16}$/.test(match[1].trim())) {
      aliases.push(match[1].trim());
    }
    if (["institution", "organization", "conference", "project"].includes(family)) {
      const acronym = (withoutParens || text)
        .match(/[A-Za-z0-9]+/g)
        ?.filter((word) => !stopwords.has(word.toLowerCase()))
        .map((word) => word[0]?.toUpperCase() ?? "")
        .join("") ?? "";
      if (acronym.length >= 2 && acronym.length <= 12) aliases.push(acronym);
    }
  }
  return uniqueStrings(aliases);
}

function graphEntityFamily(entityType: string): string {
  const normalized = entityType.toLowerCase();
  if (normalized === "institution" || normalized === "organization") return "org";
  if (normalized === "conference") return "conference";
  if (normalized === "publication" || normalized === "document") return "publication";
  if (normalized === "repository") return "repository";
  if (normalized === "language") return "topic";
  if (normalized === "website" || normalized === "domain" || normalized === "email" || normalized === "handle" || normalized === "phone") return "digital";
  if (normalized === "contactpoint") return "contact";
  if (normalized === "educationalcredential") return "credential";
  if (normalized === "organizationprofile") return "orgprofile";
  if (["experience", "affiliation", "timelineevent", "timenode", "occupation"].includes(normalized)) return normalized;
  if (normalized === "imageobject") return "image";
  if (["topic", "project", "award", "grant", "patent", "role"].includes(normalized)) return normalized;
  return normalized || "unknown";
}

function canonicalizeEntityType(entityType: string, canonicalName = "", attributes: string[] = []): string {
  const normalizedType = normalizeName(entityType);
  const joined = normalizeName([entityType, canonicalName, ...attributes].join(" "));
  const explicit: Record<string, string> = {
    article: "Publication",
    conference: "Conference",
    contactpoint: "ContactPoint",
    contact_point: "ContactPoint",
    document: "Document",
    domain: "Domain",
    educationalcredential: "EducationalCredential",
    educational_credential: "EducationalCredential",
    "educational institution": "Institution",
    educational_institution: "Institution",
    email: "Email",
    experience: "Experience",
    affiliation: "Affiliation",
    grant: "Grant",
    handle: "Handle",
    imageobject: "ImageObject",
    image_object: "ImageObject",
    institution: "Institution",
    ip: "Domain",
    location: "ContactPoint",
    occupation: "Occupation",
    organization: "Organization",
    patent: "Patent",
    person: "Person",
    phone: "Phone",
    profile: "Website",
    project: "Project",
    publication: "Publication",
    repository: "Repository",
    role: "Role",
    language: "Topic",
    snippet: "Document",
    timelineevent: "TimelineEvent",
    timeline_event: "TimelineEvent",
    timenode: "TimeNode",
    time_node: "TimeNode",
    topic: "Topic",
    organizationprofile: "OrganizationProfile",
    organization_profile: "OrganizationProfile",
    website: "Website",
  };
  if (explicit[normalizedType]) return explicit[normalizedType];
  if (/contact point|contact_type|contact surface/.test(joined)) return "ContactPoint";
  if (/educational credential|credential|degree|bachelor of|master of|doctor of philosophy|phd/.test(joined)) return "EducationalCredential";
  if (/experience|employment|work history|tenure/.test(joined)) return "Experience";
  if (/affiliation|member of|membership|relation/.test(joined)) return "Affiliation";
  if (/time node|time_key/.test(joined)) return "TimeNode";
  if (/timeline event|milestone|start_date|end_date|tenure_start|tenure_end|event_type/.test(joined)) return "TimelineEvent";
  if (/occupation|job family|profession/.test(joined)) return "Occupation";
  if (/image object|profile image|avatar|headshot/.test(joined)) return "ImageObject";
  if (/organization profile|org profile|company overview|institution overview|school overview|lab overview|subject org/.test(joined)) return "OrganizationProfile";
  if (/topic kind language|language kind|programming language|spoken language/.test(joined)) return "Topic";
  if (/orcid|researcher|author|person|advisor|coauthor|employee|founder|director/.test(joined)) return "Person";
  if (/university|college|institute|school|department|lab|laboratory/.test(joined)) return "Institution";
  if (/company|organization|corp|llc|committee|agency|firm|startup/.test(joined)) return "Organization";
  if (/conference|workshop|symposium|venue/.test(joined)) return "Conference";
  if (/repository|repo/.test(joined)) return "Repository";
  if (/project|framework|initiative|program/.test(joined)) return "Project";
  if (/programming language|spoken language/.test(joined)) return "Topic";
  if (/topic|theme|keyword|method/.test(joined)) return "Topic";
  if (/award|prize|fellowship|honor/.test(joined)) return "Award";
  if (/grant|award id|nsf|nih/.test(joined)) return "Grant";
  if (/patent|application number|inventor/.test(joined)) return "Patent";
  if (/role|position|title|officer|director/.test(joined)) return "Role";
  if (/paper|publication|preprint|journal|article/.test(joined)) return "Publication";
  if (isUrlLike(canonicalName)) {
    try {
      const parsed = new URL(canonicalName);
      const host = parsed.hostname.toLowerCase().replace(/^www\./, "");
      const parts = parsed.pathname.split("/").filter(Boolean);
      if (["github.com", "gitlab.com", "bitbucket.org"].includes(host) && parts.length >= 2) return "Repository";
    } catch {
      // Ignore malformed URL values.
    }
    if (/\.pdf$/i.test(canonicalName) || /thesis|dissertation|cv|resume|pdf/.test(joined)) return "Document";
    return "Website";
  }
  if (isLikelyDomain(canonicalName)) return "Domain";
  if (/^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$/i.test(canonicalName)) return "Email";
  if (/^\+?[0-9][0-9().\-\s]{6,}[0-9]$/.test(canonicalName)) return "Phone";
  if (canonicalName.startsWith("@") || /username|handle/.test(joined)) return "Handle";
  if (/city|country|location|address|state|region/.test(joined)) return "ContactPoint";
  const candidate = entityType.trim() || "Topic";
  return CONTRACT_ENTITY_TYPES.has(candidate) ? candidate : "Topic";
}

function entityTypeRank(entityType: string): number {
  const normalized = canonicalizeEntityType(entityType).toLowerCase();
  const rank: Record<string, number> = {
    person: 120,
    institution: 112,
    organization: 108,
    contactpoint: 104,
    experience: 103,
    educationalcredential: 102,
    role: 102,
    publication: 98,
    conference: 92,
    project: 88,
    topic: 84,
    organizationprofile: 83,
    repository: 80,
    award: 76,
    grant: 74,
    patent: 72,
    affiliation: 70,
    timelineevent: 68,
    timenode: 67,
    occupation: 66,
    location: 52,
    website: 40,
    document: 38,
    domain: 32,
    handle: 30,
    email: 30,
    phone: 30,
    imageobject: 28,
    ip: 28,
    snippet: 12,
    unknown: 0,
  };
  return rank[normalized] ?? 10;
}

function chooseEntityType(values: Array<unknown>): string {
  const counts = new Map<string, number>();
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (!text) continue;
    const canonical = canonicalizeEntityType(text);
    counts.set(canonical, (counts.get(canonical) ?? 0) + 1);
  }
  if (!counts.size) return "Unknown";
  return [...counts.entries()]
    .sort((left, right) => {
      if (right[1] !== left[1]) return right[1] - left[1];
      return entityTypeRank(right[0]) - entityTypeRank(left[0]);
    })[0]?.[0] ?? "Unknown";
}

function graphNameSignature(value: string): string {
  return normalizeName(value.replace(/\s*\([^)]*\)\s*/g, " ")).replace(/\b(?:the|of|at|for|and|in|on|to)\b/g, " ").replace(/\s+/g, " ").trim();
}

function graphRepositoryKey(values: string[], attributes: string[]): string {
  const candidates = [...values, ...extractAttributeValues(attributes, ["url", "id"])];
  for (const candidate of candidates) {
    const text = String(candidate ?? "").trim();
    if (!text) continue;
    if (/^https?:\/\//i.test(text)) {
      try {
        const parsed = new URL(text);
        const parts = parsed.pathname.split("/").filter(Boolean);
        if (parts.length >= 2) return `${parts[0].toLowerCase()}/${parts[1].toLowerCase()}`;
      } catch {
        // Ignore malformed URLs.
      }
    }
    if (text.includes("/") && !/\s/.test(text)) {
      const [owner, ...rest] = text.split("/");
      if (owner && rest.length) return `${owner.toLowerCase()}/${rest.join("/").toLowerCase()}`;
    }
  }
  return "";
}

function deriveMergeKeys(
  entityType: string,
  canonicalName: string,
  altNames: string[],
  attributes: string[],
  provided: string[] = []
): string[] {
  const family = graphEntityFamily(entityType);
  const names = uniqueStrings([canonicalName, ...altNames, ...graphAutoAliases(entityType, [canonicalName, ...altNames])]);
  const keys = [...provided];
  const allowHostMergeKey = family === "org" || family === "conference";
  for (const name of names) {
    const normalized = normalizeName(name);
    if (normalized) keys.push(`name:${family}:${normalized}`);
    const signature = graphNameSignature(name);
    if (signature && signature !== normalized && ["org", "conference", "topic", "project"].includes(family)) {
      keys.push(`sig:${family}:${signature}`);
    }
  }
  for (const value of [
    canonicalName,
    ...altNames,
    ...extractAttributeValues(attributes, ["url", "domain", "email", "handle", "username", "id", "doi", "arxiv_id"]),
  ]) {
    const text = String(value ?? "").trim();
    if (!text) continue;
    const lowered = text.toLowerCase();
    if (/^https?:\/\//i.test(lowered)) {
      keys.push(family === "digital" ? `url:${lowered.replace(/\/+$/, "")}` : `url:${family}:${lowered.replace(/\/+$/, "")}`);
      try {
        const host = new URL(text).hostname.toLowerCase().replace(/^www\./, "");
        if (host && allowHostMergeKey) keys.push(`host:${family}:${host}`);
      } catch {
        // Ignore malformed URLs.
      }
      continue;
    }
    if (/^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}$/.test(lowered)) {
      if (family === "digital") keys.push(`domain:${lowered}`);
      else if (allowHostMergeKey) keys.push(`host:${family}:${lowered}`);
      continue;
    }
    if (/^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$/.test(lowered)) {
      keys.push(`email:${lowered}`);
      continue;
    }
    if (text.startsWith("@") || (family === "digital" && !/\s/.test(text) && text.length <= 32)) {
      keys.push(`handle:${lowered.replace(/^@/, "")}`);
      continue;
    }
    if (family === "publication" && lowered.length <= 64 && (lowered.includes("/") || lowered.startsWith("10."))) {
      keys.push(`pubid:${lowered}`);
    }
  }
  for (const value of extractAttributeValues(attributes, ["company_number", "cik", "grant_id", "patent_id", "filing_id"])) {
    const normalized = String(value ?? "").trim().toLowerCase();
    if (normalized) keys.push(`id:${family}:${normalized}`);
  }
  if (["contact", "credential", "experience", "affiliation", "timelineevent", "timenode", "occupation", "orgprofile"].includes(family)) {
    const subjectValues = extractAttributeValues(attributes, ["subject"]);
    const orgValues = extractAttributeValues(attributes, ["organization", "institution", "employer", "company", "subject_org"]);
    const facetValues = extractAttributeValues(attributes, ["role", "occupation", "degree", "field", "relation", "contact_type", "event_type", "industry", "focus", "time_key"]);
    const dateValues = extractAttributeValues(attributes, ["date", "year", "start_date", "end_date", "tenure_start", "tenure_end"]);
    const directValues = extractAttributeValues(attributes, ["value", "email", "phone", "handle", "username"]);
    for (const value of directValues) {
      const normalized = String(value ?? "").trim().toLowerCase();
      if (normalized) keys.push(`value:${family}:${normalized}`);
    }
    const composite = [
      graphNameSignature(canonicalName),
      subjectValues[0] ? normalizeName(subjectValues[0]) : "",
      orgValues[0] ? normalizeName(orgValues[0]) : "",
      facetValues[0] ? normalizeName(facetValues[0]) : "",
      dateValues[0] ? normalizeName(dateValues[0]) : "",
      directValues[0] ? String(directValues[0]).trim().toLowerCase() : "",
    ]
      .filter(Boolean)
      .join("|");
    if (composite) keys.push(`composite:${family}:${composite}`);
  }
  if (family === "repository") {
    const repoKey = graphRepositoryKey(names, attributes);
    if (repoKey) keys.push(`repo:${repoKey}`);
  }
  return uniqueStrings(keys);
}

function chooseCanonicalName(values: Array<unknown>): string {
  const candidates = uniqueStrings(values);
  if (!candidates.length) return "unknown";
  return [...candidates].sort((a, b) => {
    if (b.length !== a.length) return b.length - a.length;
    return a.localeCompare(b);
  })[0];
}

function scoreEntityNameCandidate(entityType: string, value: string): number {
  const text = String(value ?? "").trim();
  if (!text) return Number.NEGATIVE_INFINITY;
  const normalized = normalizeName(text);
  const family = graphEntityFamily(canonicalizeEntityType(entityType, text));
  const preferredType = canonicalizeEntityType(entityType);
  const preferredNormalized = normalizeName(preferredType);
  const url = isUrlLike(text);
  const domain = isLikelyDomain(text);
  const tokens = normalized.split(" ").filter(Boolean);
  let score = 0;

  if (/snippet:|duckduckgo|github_identity_search|gitlab_identity_search/i.test(text)) score -= 280;
  if (!url && !domain) score += 50;
  if (url) score -= family === "digital" || family === "repository" ? 0 : 220;
  if (domain) score -= family === "digital" ? 0 : 140;
  score -= Math.max(0, text.length - 40);

  if (family === "person") {
    if (looksLikePersonName(text)) score += 260;
    if (/^\p{Lu}[\p{L}'-]+(?:\s+\p{Lu}[\p{L}'-]+){1,4}$/u.test(text)) score += 80;
    if (/[@/:]|\d/.test(text)) score -= 160;
    if (tokens.length >= 2 && tokens.length <= 4) score += 40;
  } else if (family === "org") {
    if (looksLikeOrganizationName(text)) score += 220;
    if (tokens.length >= 2) score += 25;
    if (/^\(?[A-Z0-9]{2,10}\)?$/.test(text)) score -= 50;
  } else if (family === "publication") {
    if (!url && tokens.length >= 4) score += 180;
    if (!url && text.length >= 20) score += 40;
  } else if (family === "digital") {
    if (url) score += 200;
    if (domain) score += 160;
    if (/^\+?[0-9][0-9().\-\s]{6,}[0-9]$/.test(text)) score += 140;
    if (preferredNormalized === "website" && !url && !domain) {
      score += 240;
      if (/profile|website|site|homepage|official|page/.test(normalized)) score += 60;
    }
  } else if (family === "repository") {
    if (/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(text)) score += 220;
    if (url) score += 120;
  } else if (family === "contact") {
    if (/^\+?[0-9][0-9().\-\s]{6,}[0-9]$/.test(text) || text.startsWith("@") || text.includes("@") || url) score += 200;
    if (normalized.includes("contact") || normalized.includes("surface")) score += 120;
  } else if (family === "credential") {
    if (/phd|doctor|master|bachelor|degree|credential/.test(normalized)) score += 200;
    if (/ from /i.test(text) || / at /i.test(text)) score += 80;
  } else if (family === "experience") {
    if (/ at /i.test(text)) score += 180;
    if (/engineer|student|scientist|founder|director|research/.test(normalized)) score += 80;
  } else if (family === "affiliation") {
    if (/ with /i.test(text) || / at /i.test(text)) score += 180;
  } else if (family === "timelineevent") {
    if (/started|joined|graduated|published|founded|appointed/.test(normalized)) score += 160;
    if (/\b(19|20)\d{2}\b/.test(text)) score += 80;
  } else if (family === "occupation") {
    if (tokens.length >= 1 && tokens.length <= 4) score += 120;
  } else if (family === "image") {
    if (url) score += 180;
    if (preferredNormalized === "imageobject" && !url && tokens.length >= 2) score += 220;
  }

  return score;
}

function chooseEntityCanonicalName(entityType: string, values: Array<unknown>): string {
  const candidates = uniqueStrings(values);
  if (!candidates.length) return "unknown";
  let best = candidates[0];
  let bestScore = scoreEntityNameCandidate(entityType, best);
  for (const candidate of candidates.slice(1)) {
    const score = scoreEntityNameCandidate(entityType, candidate);
    if (score > bestScore) {
      best = candidate;
      bestScore = score;
      continue;
    }
    if (score === bestScore && candidate.length < best.length) {
      best = candidate;
    }
  }
  return best;
}

function entityFamiliesCompatible(leftType: string, rightType: string): boolean {
  const leftFamily = graphEntityFamily(canonicalizeEntityType(leftType));
  const rightFamily = graphEntityFamily(canonicalizeEntityType(rightType));
  if (leftFamily === rightFamily) return true;
  const pair = [leftFamily, rightFamily].sort().join("|");
  return pair === "digital|repository";
}

function canonicalizeRelationType(relType: string): string {
  const normalized = normalizeName(relType).replace(/\s+/g, "_").toUpperCase();
  const mapping: Record<string, string> = {
    AFFILIATION: "AFFILIATED_WITH",
    AFFILIATED_WITH: "AFFILIATED_WITH",
    ALIAS_VARIANT: "RELATED_TO",
    ABOUT: "ABOUT",
    AUTHORSHIP: "PUBLISHED",
    CANDIDATE_MATCH: "RELATED_TO",
    COLLABORATION: "COAUTHORED_WITH",
    CONFERENCE: "PUBLISHED_IN",
    EDUCATION: "STUDIED_AT",
    HAS_AFFILIATION: "HAS_AFFILIATION",
    HAS_CONTACT_POINT: "HAS_CONTACT_POINT",
    HAS_CREDENTIAL: "HAS_CREDENTIAL",
    HAS_EXPERIENCE: "HAS_EXPERIENCE",
    HAS_IMAGE: "HAS_IMAGE",
    HAS_ORGANIZATION_PROFILE: "HAS_ORGANIZATION_PROFILE",
    HAS_OCCUPATION: "HAS_OCCUPATION",
    HAS_PHONE: "HAS_PHONE",
    HAS_ROLE: "HAS_ROLE",
    HAS_SKILL_TOPIC: "HAS_SKILL_TOPIC",
    HAS_HOBBY_TOPIC: "HAS_HOBBY_TOPIC",
    HAS_INTEREST_TOPIC: "HAS_INTEREST_TOPIC",
    HAS_TIMELINE_EVENT: "HAS_TIMELINE_EVENT",
    MENTIONS_TIMELINE_EVENT: "MENTIONS_TIMELINE_EVENT",
    IN_TIME_NODE: "IN_TIME_NODE",
    NEXT_TIME_NODE: "NEXT_TIME_NODE",
    ISSUED_BY: "ISSUED_BY",
    KNOWS_LANGUAGE: "KNOWS_LANGUAGE",
    PROFILE: "HAS_PROFILE",
    PUBLICATION: "PUBLISHED",
    RESIDENCE: "LOCATED_IN",
  };
  const candidate = mapping[normalized] ?? (normalized || "RELATED_TO");
  return CONTRACT_RELATION_TYPES.has(candidate) ? candidate : "RELATED_TO";
}

function ensureStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return uniqueStrings(value);
}

function stableId(prefix: string, ...parts: string[]): string {
  const digest = crypto.createHash("sha256").update(parts.join("|")).digest("hex").slice(0, 20);
  return `${prefix}_${digest}`;
}

function sourceDomainFromUrl(url?: string): string | undefined {
  const candidate = String(url ?? "").trim();
  if (!candidate || !isUrlLike(candidate)) return undefined;
  try {
    const host = new URL(candidate).hostname.trim().toLowerCase();
    if (!host) return undefined;
    return host.replace(/^www\./, "");
  } catch {
    return undefined;
  }
}

function deriveOsintBucket(entityType: string, names: string[], attributes: string[]): string {
  const joined = normalizeName([entityType, ...names, ...attributes].join(" "));
  if (/(person|author|researcher|employee|director|founder|officer|student)/.test(joined)) return "person";
  if (/(experience|credential|affiliation|occupation|timeline event|timelineevent|time node|timenode|contact point|contactpoint)/.test(joined)) return "person";
  if (/(organization profile|org profile|subject org)/.test(joined)) return "organization";
  if (/(organization|company|institution|agency|university|lab|firm|committee)/.test(joined)) return "organization";
  if (/(domain|website|hostname|repository|repo|account|username|handle|email|phone|imageobject|image object)/.test(joined)) return "digital_asset";
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

function selectPrimaryName(properties: Record<string, unknown>, fallback?: string, entityType = "Unknown"): string {
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
  return chooseEntityCanonicalName(entityType, candidates);
}

function attributeTitleCandidates(attributes: string[]): string[] {
  return extractAttributeValues(attributes, ["title", "label", "page_title", "site_title", "display_title", "semantic_title"]);
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

function normalizeGraphEntity(runId: string, input: GraphEntityInput): GraphEntityRecord {
  const inputNames = uniqueStrings([input.canonical_name, ...(input.alt_names ?? [])]);
  const rawEntityType = String(input.type ?? "Unknown").trim() || "Unknown";
  const attributes = ensureStringArray(input.attributes);
  const entityType = canonicalizeEntityType(rawEntityType, inputNames[0] ?? "", attributes);
  const names = uniqueStrings([...inputNames, ...attributeTitleCandidates(attributes), ...graphAutoAliases(entityType, inputNames)]);
  const canonicalName = chooseEntityCanonicalName(entityType, names);
  const altNames = names.filter((name) => normalizeName(name) !== normalizeName(canonicalName));
  const mergeKeys = deriveMergeKeys(entityType, canonicalName, altNames, attributes, ensureStringArray(input.merge_keys));
  const bucket = String(input.osint_bucket ?? deriveOsintBucket(entityType, names, attributes)).trim() || "unknown";
  const createdAt = utcNow();
  const updatedAt = createdAt;
  const canonicalId = stableId("entc", normalizeName(entityType), normalizeName(canonicalName));
  const runScopedId = String(input.node_id ?? stableId("ent", runId, normalizeName(entityType), normalizeName(canonicalName)));
  const sourceUrl = input.evidence?.sourceUrl?.trim() || undefined;
  const sourceDomain = sourceDomainFromUrl(sourceUrl);
  const evidenceProps = buildEvidenceProps(input.evidence);
  const evidenceObjectKey = typeof evidenceProps.evidence_object_key === "string" ? evidenceProps.evidence_object_key : undefined;

  return {
    node_id: runScopedId,
    canonical_id: canonicalId,
    run_scoped_id: runScopedId,
    run_id: runId,
    external_context: false,
    type: entityType,
    raw_type: rawEntityType,
    canonical_name: canonicalName,
    canonical_name_normalized: normalizeName(canonicalName),
    alt_names: altNames,
    alt_names_normalized: altNames.map((item) => normalizeName(item)),
    attributes,
    merge_keys: mergeKeys,
    osint_bucket: bucket,
    filter_terms: deriveFilterTerms(entityType, canonicalName, altNames, attributes, bucket),
    source_tools: ensureStringArray(input.source_tools),
    created_at: createdAt,
    updated_at: updatedAt,
    source_url: sourceUrl,
    source_urls: sourceUrl ? [sourceUrl] : undefined,
    source_domain: sourceDomain,
    snippet_text: input.evidence?.snippetText?.trim() || undefined,
    snippet_id: input.evidence?.snippetId?.trim() || undefined,
    evidence_object_keys: evidenceObjectKey ? [evidenceObjectKey] : undefined,
    ingested_at: createdAt,
    embedding_text: "",
    embedding: [],
    ...evidenceProps,
  };
}

function normalizeGraphRelation(runId: string, input: GraphRelationInput): GraphRelationRecord {
  const rawRelType = String(input.raw_relation_type ?? input.rel_type ?? "RELATED_TO").trim() || "RELATED_TO";
  const relType = canonicalizeRelationType(rawRelType);
  const canonicalName = String(input.canonical_name ?? relType).trim() || relType;
  const altNames = ensureStringArray(input.alt_names);
  const evidenceProps = buildEvidenceProps(input.evidenceRef);
  const createdAt = utcNow();
  const evidenceObjectKey = typeof evidenceProps.evidence_object_key === "string" ? evidenceProps.evidence_object_key : undefined;
  const canonicalId = stableId(
    "relc",
    String(input.src_canonical_id ?? input.src_id),
    String(input.dst_canonical_id ?? input.dst_id),
    normalizeName(relType),
    normalizeName(canonicalName)
  );
  const runScopedId = String(
    input.edge_id ??
      stableId(
        "rel",
        runId,
        input.src_id,
        input.dst_id,
        normalizeName(relType),
        normalizeName(canonicalName)
      )
  );
  return {
    edge_id: runScopedId,
    canonical_id: canonicalId,
    run_scoped_id: runScopedId,
    run_id: runId,
    external_context: false,
    src_id: input.src_id,
    dst_id: input.dst_id,
    rel_type: relType,
    raw_relation_type: rawRelType,
    rel_type_normalized: normalizeName(relType),
    canonical_name: canonicalName,
    canonical_name_normalized: normalizeName(canonicalName),
    alt_names: altNames,
    alt_names_normalized: altNames.map((item) => normalizeName(item)),
    source_tool: input.source_tool?.trim() || undefined,
    created_at: createdAt,
    updated_at: createdAt,
    ingested_at: createdAt,
    evidence_object_keys: evidenceObjectKey ? [evidenceObjectKey] : undefined,
    embedding_text: "",
    embedding: [],
    ...evidenceProps,
  };
}

function buildEntityEmbeddingText(entity: GraphEntityRecord): string {
  return uniqueStrings([entity.type, entity.canonical_name, ...entity.alt_names, ...entity.attributes.slice(0, 8)]).join(" | ");
}

function selectAttributeSnippets(attributes: string[], prefixes: string[], maxItems = 4): string[] {
  const allowed = new Set(prefixes.map((value) => value.trim().toLowerCase()).filter(Boolean));
  const preferred: string[] = [];
  const remainder: string[] = [];
  for (const attribute of attributes) {
    if (typeof attribute !== "string") continue;
    const trimmed = attribute.trim();
    if (!trimmed) continue;
    const separatorIndex = trimmed.indexOf(":");
    const key = separatorIndex >= 0 ? trimmed.slice(0, separatorIndex).trim().toLowerCase() : "";
    if (key && allowed.has(key)) preferred.push(trimmed);
    else remainder.push(trimmed);
  }
  return uniqueStrings([...preferred, ...remainder]).slice(0, maxItems);
}

function buildRelationEndpointEmbeddingText(label: string, entity: GraphEntityRecord | undefined, fallbackId: string): string[] {
  if (!entity) return [`${label}_id: ${fallbackId}`];
  const attributeSnippets = selectAttributeSnippets(
    entity.attributes,
    [
      "role",
      "organization",
      "institution",
      "company",
      "subject",
      "relation",
      "field",
      "topic",
      "industry",
      "focus",
      "research_area",
      "research_areas",
      "language",
      "platform",
      "venue",
      "year",
      "start_date",
      "end_date",
      "company_number",
      "jurisdiction",
      "cik",
      "url",
      "email",
      "phone",
    ],
    4
  );
  const aliasSummary = entity.alt_names.slice(0, 2).join("; ");
  return uniqueStrings([
    `${label}_type: ${entity.type}`,
    `${label}_name: ${entity.canonical_name}`,
    aliasSummary ? `${label}_aliases: ${aliasSummary}` : "",
    ...attributeSnippets.map((attribute) => `${label}_${attribute}`),
  ]);
}

function buildRelationEmbeddingText(relation: GraphRelationRecord, nodeLookup: Map<string, GraphEntityRecord>): string {
  const src = nodeLookup.get(relation.src_id);
  const dst = nodeLookup.get(relation.dst_id);
  const relationNames = uniqueStrings([relation.canonical_name, ...relation.alt_names]);
  return uniqueStrings([
    ...buildRelationEndpointEmbeddingText("source", src, relation.src_id),
    `relation_type: ${relation.rel_type}`,
    relation.canonical_name && relation.canonical_name !== relation.rel_type ? `relation_name: ${relation.canonical_name}` : "",
    relationNames.length > 1 ? `relation_aliases: ${relationNames.slice(0, 3).join("; ")}` : "",
    relation.source_tool ? `source_tool: ${relation.source_tool}` : "",
    ...buildRelationEndpointEmbeddingText("target", dst, relation.dst_id),
  ]).join(" | ");
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

function normalizeLegacyEntity(runId: string, input: LegacyBatchEntityInput): GraphEntityRecord {
  const properties = input.properties ?? {};
  const canonicalName = selectPrimaryName(properties, input.entityId, input.entityType);
  const altNames = uniqueStrings([
    properties.username,
    properties.handle,
    properties.display_name,
    ...(Array.isArray(properties.aliases) ? properties.aliases : []),
  ]);
  const attributes = propertyAttributes(properties);
  return normalizeGraphEntity(runId, {
    node_id: input.entityId,
    type: input.entityType,
    canonical_name: canonicalName,
    alt_names: altNames,
    attributes,
    evidence: input.evidence,
  });
}

function normalizeLegacyRelationTarget(
  runId: string,
  targetType: string,
  targetId: string | undefined,
  targetProperties: Record<string, unknown>,
  evidence?: EvidenceObjectRef
): GraphEntityRecord {
  const canonicalName = selectPrimaryName(targetProperties, targetId, targetType);
  return normalizeGraphEntity(runId, {
    node_id: targetId,
    type: targetType,
    canonical_name: canonicalName,
    alt_names: [],
    attributes: propertyAttributes(targetProperties),
    evidence: evidence ? { objectRef: evidence } : undefined,
  });
}

function scoreEntityCandidate(existing: Record<string, unknown>, incoming: GraphEntityRecord): number {
  const existingType = canonicalizeEntityType(String(existing.type ?? ""), String(existing.canonical_name ?? ""), ensureStringArray(existing.attributes));
  const incomingType = canonicalizeEntityType(incoming.type, incoming.canonical_name, incoming.attributes);
  if (existing.node_id !== incoming.node_id && !entityFamiliesCompatible(existingType, incomingType)) {
    return Number.NEGATIVE_INFINITY;
  }

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
  const existingMergeKeys = new Set<string>(((existing.merge_keys as string[] | undefined) ?? []).map((item) => String(item)));
  for (const key of incoming.merge_keys) {
    if (existingMergeKeys.has(key)) score += 180;
  }
  if (entityFamiliesCompatible(existingType, incomingType)) {
    const existingTerms = new Set<string>((existing.filter_terms as string[] | undefined) ?? []);
    for (const term of incoming.filter_terms.slice(0, 8)) {
      if (existingTerms.has(term)) score += 20;
    }
  }
  if (existing.osint_bucket === incoming.osint_bucket) score += 25;
  if (existingType === incomingType) score += 20;
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
  const entityType = chooseEntityType([existing.type, incoming.type]);
  const canonicalName = chooseEntityCanonicalName(entityType, [
    existing.canonical_name,
    incoming.canonical_name,
    ...(existing.alt_names as string[] ?? []),
    ...incoming.alt_names,
  ]);
  const altNames = uniqueStrings([
    ...(existing.alt_names as string[] ?? []),
    ...(incoming.alt_names ?? []),
    String(existing.canonical_name ?? ""),
    incoming.canonical_name,
  ]).filter((name) => normalizeName(name) !== normalizeName(canonicalName));
  const attributes = uniqueStrings([...(existing.attributes as string[] ?? []), ...incoming.attributes]);
  const bucket = String(existing.osint_bucket ?? incoming.osint_bucket);
  const mergeKeys = deriveMergeKeys(
    entityType,
    canonicalName,
    altNames,
    attributes,
    uniqueStrings([...(existing.merge_keys as string[] ?? []), ...incoming.merge_keys])
  );
  const evidenceDocumentIds = extractEvidenceDocumentIds(existing).concat(extractEvidenceDocumentIds(incoming));
  const mergedEvidenceDocumentIds = uniqueStrings(evidenceDocumentIds);
  const mergedEvidenceObjectKeys = uniqueStrings([
    ...((existing.evidence_object_keys as string[] | undefined) ?? []),
    ...((incoming.evidence_object_keys as string[] | undefined) ?? []),
    String(existing.evidence_object_key ?? ""),
    String(incoming.evidence_object_key ?? ""),
  ]);
  const mergedSourceUrls = uniqueStrings([
    ...((existing.source_urls as string[] | undefined) ?? []),
    ...((incoming.source_urls as string[] | undefined) ?? []),
    String(existing.source_url ?? ""),
    String(incoming.source_url ?? ""),
  ]);
  const mergedEmbeddingText = buildEntityEmbeddingText({
    ...incoming,
    canonical_name: canonicalName,
    alt_names: altNames,
    type: entityType,
    attributes,
  });
  return {
    node_id: String(existing.node_id ?? incoming.node_id),
    canonical_id: String(existing.canonical_id ?? incoming.canonical_id),
    run_scoped_id: String(existing.run_scoped_id ?? incoming.run_scoped_id ?? existing.node_id ?? incoming.node_id),
    run_id: String(existing.run_id ?? incoming.run_id),
    external_context: Boolean(existing.external_context ?? incoming.external_context ?? false),
    type: entityType,
    raw_type: String(incoming.raw_type || existing.raw_type || entityType),
    canonical_name: canonicalName,
    canonical_name_normalized: normalizeName(canonicalName),
    alt_names: altNames,
    alt_names_normalized: altNames.map((item) => normalizeName(item)),
    attributes,
    merge_keys: mergeKeys,
    osint_bucket: bucket,
    filter_terms: deriveFilterTerms(entityType, canonicalName, altNames, attributes, bucket),
    source_tools: uniqueStrings([...(existing.source_tools as string[] ?? []), ...incoming.source_tools]),
    created_at: String(existing.created_at ?? incoming.created_at),
    updated_at: incoming.updated_at,
    ingested_at: String(existing.ingested_at ?? incoming.ingested_at ?? incoming.updated_at),
    evidence_document_id: mergedEvidenceDocumentIds[0] || undefined,
    evidence_document_ids: mergedEvidenceDocumentIds.length ? mergedEvidenceDocumentIds : undefined,
    evidence_bucket: String(incoming.evidence_bucket ?? existing.evidence_bucket ?? "") || undefined,
    evidence_object_key: String(incoming.evidence_object_key ?? existing.evidence_object_key ?? "") || undefined,
    evidence_object_keys: mergedEvidenceObjectKeys.length ? mergedEvidenceObjectKeys : undefined,
    evidence_version_id: String(incoming.evidence_version_id ?? existing.evidence_version_id ?? "") || undefined,
    evidence_etag: String(incoming.evidence_etag ?? existing.evidence_etag ?? "") || undefined,
    source_url: String(incoming.source_url ?? existing.source_url ?? "") || undefined,
    source_urls: mergedSourceUrls.length ? mergedSourceUrls : undefined,
    source_domain: String(incoming.source_domain ?? existing.source_domain ?? "") || undefined,
    snippet_text: String(incoming.snippet_text ?? existing.snippet_text ?? "") || undefined,
    snippet_id: String(incoming.snippet_id ?? existing.snippet_id ?? "") || undefined,
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
  const relType = canonicalizeRelationType(chooseCanonicalName([existing.rel_type, incoming.rel_type]));
  const evidenceDocumentIds = extractEvidenceDocumentIds(existing).concat(extractEvidenceDocumentIds(incoming));
  const mergedEvidenceDocumentIds = uniqueStrings(evidenceDocumentIds);
  const mergedEvidenceObjectKeys = uniqueStrings([
    ...((existing.evidence_object_keys as string[] | undefined) ?? []),
    ...((incoming.evidence_object_keys as string[] | undefined) ?? []),
    String(existing.evidence_object_key ?? ""),
    String(incoming.evidence_object_key ?? ""),
  ]);
  return {
    edge_id: String(existing.edge_id ?? incoming.edge_id),
    canonical_id: String(existing.canonical_id ?? incoming.canonical_id),
    run_scoped_id: String(existing.run_scoped_id ?? incoming.run_scoped_id ?? existing.edge_id ?? incoming.edge_id),
    run_id: String(existing.run_id ?? incoming.run_id),
    external_context: Boolean(existing.external_context ?? incoming.external_context ?? false),
    src_id: incoming.src_id,
    dst_id: incoming.dst_id,
    rel_type: relType,
    raw_relation_type: String(incoming.raw_relation_type || existing.raw_relation_type || relType),
    rel_type_normalized: normalizeName(relType),
    canonical_name: canonicalName,
    canonical_name_normalized: normalizeName(canonicalName),
    alt_names: altNames,
    alt_names_normalized: altNames.map((item) => normalizeName(item)),
    source_tool: String(existing.source_tool ?? incoming.source_tool ?? "") || undefined,
    created_at: String(existing.created_at ?? incoming.created_at),
    updated_at: incoming.updated_at,
    ingested_at: String(existing.ingested_at ?? incoming.ingested_at ?? incoming.updated_at),
    evidence_document_id: mergedEvidenceDocumentIds[0] || undefined,
    evidence_document_ids: mergedEvidenceDocumentIds.length ? mergedEvidenceDocumentIds : undefined,
    evidence_bucket: String(incoming.evidence_bucket ?? existing.evidence_bucket ?? "") || undefined,
    evidence_object_key: String(incoming.evidence_object_key ?? existing.evidence_object_key ?? "") || undefined,
    evidence_object_keys: mergedEvidenceObjectKeys.length ? mergedEvidenceObjectKeys : undefined,
    evidence_version_id: String(incoming.evidence_version_id ?? existing.evidence_version_id ?? "") || undefined,
    evidence_etag: String(incoming.evidence_etag ?? existing.evidence_etag ?? "") || undefined,
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
     WHERE coalesce(n.run_id, '') = $runId
       AND (
            n.run_scoped_id = $runScopedId
         OR n.node_id = $nodeId
         OR n.canonical_name_normalized = $canonical
         OR any(name IN coalesce(n.alt_names_normalized, []) WHERE name IN $names)
         OR any(key IN coalesce(n.merge_keys, []) WHERE key IN $mergeKeys)
         OR any(term IN coalesce(n.filter_terms, []) WHERE term IN $terms)
       )
     RETURN properties(n) AS props
     LIMIT 100`,
    {
      runId: incoming.run_id,
      runScopedId: incoming.run_scoped_id,
      nodeId: incoming.node_id,
      canonical: incoming.canonical_name_normalized,
      names: [incoming.canonical_name_normalized, ...incoming.alt_names_normalized],
      mergeKeys: incoming.merge_keys.slice(0, 24),
      terms: incoming.filter_terms.slice(0, 16),
    }
  );
  if (!result.records.length && incoming.embedding.length) {
    result = await session.run(
      `MATCH (n:Entity)
       WHERE coalesce(n.run_id, '') = $runId
         AND (n.type = $type OR n.osint_bucket = $bucket)
       RETURN properties(n) AS props
       LIMIT 100`,
      {
        runId: incoming.run_id,
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
     WHERE coalesce(r.run_id, '') = $runId
       AND (
            r.run_scoped_id = $runScopedId
         OR r.edge_id = $edgeId
         OR r.rel_type_normalized = $relType
         OR r.canonical_name_normalized = $canonical
       )
     RETURN properties(r) AS props
     LIMIT 25`,
    {
      runId: incoming.run_id,
      runScopedId: incoming.run_scoped_id,
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
  await linkSameCanonicalEntities(session, merged);
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

async function linkSameCanonicalEntities(
  session: ReturnType<typeof neo4jDriver.session>,
  entity: GraphEntityRecord
): Promise<void> {
  if (!entity.canonical_id) return;
  const result = await session.run(
    `MATCH (other:Entity)
     WHERE other.canonical_id = $canonicalId
       AND other.node_id <> $nodeId
       AND coalesce(other.run_id, '') <> $runId
     RETURN other.node_id AS nodeId
     LIMIT 25`,
    { canonicalId: entity.canonical_id, nodeId: entity.node_id, runId: entity.run_id }
  );

  for (const record of result.records) {
    const otherNodeId = String(record.get("nodeId") ?? "").trim();
    if (!otherNodeId) continue;
    const edgeId = stableId("rel", entity.run_id, entity.node_id, otherNodeId, "same_canonical_as");
    const relation: GraphRelationRecord = {
      edge_id: edgeId,
      canonical_id: stableId("relc", entity.canonical_id, "same_canonical_as"),
      run_scoped_id: edgeId,
      run_id: entity.run_id,
      external_context: true,
      src_id: entity.node_id,
      dst_id: otherNodeId,
      rel_type: "SAME_CANONICAL_AS",
      raw_relation_type: "SAME_CANONICAL_AS",
      rel_type_normalized: normalizeName("SAME_CANONICAL_AS"),
      canonical_name: "SAME_CANONICAL_AS",
      canonical_name_normalized: normalizeName("SAME_CANONICAL_AS"),
      alt_names: [],
      alt_names_normalized: [],
      source_tool: "ingest_graph_entities",
      created_at: entity.updated_at,
      updated_at: entity.updated_at,
      ingested_at: entity.ingested_at,
      embedding_text: "SAME_CANONICAL_AS",
      embedding: [],
    };
    await session.run(
      `MATCH (src:Entity {node_id: $src_id})
       MATCH (dst:Entity {node_id: $dst_id})
       MERGE (src)-[r:RELATED_TO {edge_id: $edge_id}]->(dst)
       SET r += $props`,
      { src_id: relation.src_id, dst_id: relation.dst_id, edge_id: relation.edge_id, props: relation }
    );
  }
}

async function ensureGraphSchema(session: ReturnType<typeof neo4jDriver.session>): Promise<void> {
  if (graphSchemaBootstrapped) return;
  await session.run(
    "CREATE CONSTRAINT entity_node_id_unique IF NOT EXISTS FOR (n:Entity) REQUIRE n.node_id IS UNIQUE"
  );
  await session.run("CREATE INDEX entity_type_idx IF NOT EXISTS FOR (n:Entity) ON (n.type)");
  await session.run("CREATE INDEX entity_run_id_idx IF NOT EXISTS FOR (n:Entity) ON (n.run_id)");
  await session.run("CREATE INDEX entity_canonical_id_idx IF NOT EXISTS FOR (n:Entity) ON (n.canonical_id)");
  await session.run("CREATE INDEX entity_run_scoped_id_idx IF NOT EXISTS FOR (n:Entity) ON (n.run_scoped_id)");
  await session.run(
    "CREATE INDEX entity_canonical_name_norm_idx IF NOT EXISTS FOR (n:Entity) ON (n.canonical_name_normalized)"
  );
  await session.run("CREATE INDEX entity_osint_bucket_idx IF NOT EXISTS FOR (n:Entity) ON (n.osint_bucket)");
  try {
    await session.run(
      "CREATE INDEX related_to_rel_type_norm_idx IF NOT EXISTS FOR ()-[r:RELATED_TO]-() ON (r.rel_type_normalized)"
    );
    await session.run(
      "CREATE INDEX related_to_run_id_idx IF NOT EXISTS FOR ()-[r:RELATED_TO]-() ON (r.run_id)"
    );
  } catch (error) {
    logger.warn("Relationship rel_type_normalized index bootstrap skipped", {
      error: (error as Error).message,
    });
  }
  graphSchemaBootstrapped = true;
}

function parseEntityBatchPayload(runId: string, value: string): { entities: GraphEntityRecord[]; relations: GraphRelationRecord[] } {
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
      addEntity(normalizeGraphEntity(runId, record as GraphEntityInput));
      continue;
    }

    const legacy = record as LegacyBatchEntityInput;
    const sourceEntity = normalizeLegacyEntity(runId, legacy);
    addEntity(sourceEntity);
    for (const relation of legacy.relations ?? []) {
      const targetEntity = normalizeLegacyRelationTarget(
        runId,
        relation.targetType,
        relation.targetId,
        relation.targetProperties ?? {},
        relation.evidenceRef
      );
      addEntity(targetEntity);
      relations.push(
        normalizeGraphRelation(runId, {
          src_id: sourceEntity.node_id,
          dst_id: targetEntity.node_id,
          src_canonical_id: sourceEntity.canonical_id,
          dst_canonical_id: targetEntity.canonical_id,
          rel_type: relation.type,
          canonical_name: relation.type,
          evidenceRef: relation.evidenceRef,
        })
      );
    }
  }

  return { entities: [...entityMap.values()], relations };
}

function parseRelationsPayload(runId: string, value: string): { entities: GraphEntityRecord[]; relations: GraphRelationRecord[] } {
  const parsed = parseJson<unknown>("relations", value);
  if (!Array.isArray(parsed)) throw new Error("relationsJson must be a JSON array");

  const entityMap = new Map<string, GraphEntityRecord>();
  const relations: GraphRelationRecord[] = [];
  for (const item of parsed) {
    const record = item as Record<string, unknown>;
    if ("src_id" in record || "edge_id" in record) {
      relations.push(normalizeGraphRelation(runId, record as GraphRelationInput));
      continue;
    }

    const legacy = record as LegacyRelationTripletInput;
    const srcEntity = normalizeLegacyRelationTarget(runId, legacy.srcType, legacy.srcId, legacy.srcProperties ?? {});
    const dstEntity = normalizeLegacyRelationTarget(runId, legacy.dstType, legacy.dstId, legacy.dstProperties ?? {});
    entityMap.set(srcEntity.node_id, srcEntity);
    entityMap.set(dstEntity.node_id, dstEntity);
    relations.push(
      normalizeGraphRelation(runId, {
        src_id: srcEntity.node_id,
        dst_id: dstEntity.node_id,
        src_canonical_id: srcEntity.canonical_id,
        dst_canonical_id: dstEntity.canonical_id,
        rel_type: legacy.relType,
        canonical_name: legacy.relType,
        evidenceRef: legacy.evidenceRef,
      })
    );
  }
  return { entities: [...entityMap.values()], relations };
}

function validateGraphBatch(entities: GraphEntityRecord[], relations: GraphRelationRecord[]) {
  const entityErrors: string[] = [];
  const relationErrors: string[] = [];
  let entityTypeCoercions = 0;
  let relationTypeCoercions = 0;

  for (const entity of entities) {
    if (!entity.node_id || !entity.run_id || !entity.canonical_name) {
      entityErrors.push(`entity missing required fields: node_id/run_id/canonical_name (${entity.node_id || "unknown"})`);
      continue;
    }
    if (!CONTRACT_ENTITY_TYPES.has(entity.type)) {
      entityErrors.push(`entity type out of contract: ${entity.type} (${entity.node_id})`);
    }
    if (normalizeName(entity.raw_type) !== normalizeName(entity.type)) {
      entityTypeCoercions += 1;
    }
  }

  const nodeIds = new Set(entities.map((item) => item.node_id));
  for (const relation of relations) {
    if (!relation.edge_id || !relation.run_id || !relation.src_id || !relation.dst_id) {
      relationErrors.push(`relation missing required fields: edge_id/run_id/src_id/dst_id (${relation.edge_id || "unknown"})`);
      continue;
    }
    if (!CONTRACT_RELATION_TYPES.has(relation.rel_type)) {
      relationErrors.push(`relation type out of contract: ${relation.rel_type} (${relation.edge_id})`);
    }
    if (nodeIds.size > 0 && (!nodeIds.has(relation.src_id) || !nodeIds.has(relation.dst_id))) {
      relationErrors.push(`relation endpoint missing in batch: ${relation.edge_id}`);
    }
    if (normalizeName(relation.raw_relation_type) !== normalizeName(relation.rel_type)) {
      relationTypeCoercions += 1;
    }
  }

  if (entityErrors.length || relationErrors.length) {
    const lines = [...entityErrors.slice(0, 8), ...relationErrors.slice(0, 8)];
    throw new Error(`Graph blueprint contract violation: ${lines.join("; ")}`);
  }

  return {
    entityTypeCoercions,
    relationTypeCoercions,
    entityCount: entities.length,
    relationCount: relations.length,
  };
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
        await ensureGraphSchema(session);
        properties = parseJson<Record<string, unknown>>("properties", propertiesJson) ?? {};
        evidence = parseJson<EvidenceInput>("evidence", evidenceJson);
        relations = parseJson<LegacyRelationInput[]>("relations", relationsJson) ?? [];
        const { entities, relations: normalizedRelations } = parseEntityBatchPayload(
          runId,
          JSON.stringify([{ entityType, entityId, properties, evidence, relations } satisfies LegacyBatchEntityInput])
        );
        const contractMetrics = validateGraphBatch(entities, normalizedRelations);
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
          contractMetrics,
        };

        await logToolCall(runId, "ingest_graph_entity", { entityType, entityId, properties, evidence, relations }, output, "ok");
        await emitRunEvent(runId, "GRAPH_CONTRACT_VALIDATED", { tool: "ingest_graph_entity", ...contractMetrics });
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
        await ensureGraphSchema(session);
        const parsed = parseEntityBatchPayload(runId, entitiesJson);
        const contractMetrics = validateGraphBatch(parsed.entities, parsed.relations);
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
          contractMetrics,
          warnings: [],
        };

        await logToolCall(runId, "ingest_graph_entities", { entitiesJson }, output, "ok");
        await emitRunEvent(runId, "GRAPH_CONTRACT_VALIDATED", { tool: "ingest_graph_entities", ...contractMetrics });
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
        await ensureGraphSchema(session);
        const parsed = parseRelationsPayload(runId, relationsJson);
        const contractMetrics = validateGraphBatch(parsed.entities, parsed.relations);
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
          contractMetrics,
          warnings: [],
        };

        await logToolCall(runId, "ingest_graph_relations", { relationsJson }, output, "ok");
        await emitRunEvent(runId, "GRAPH_CONTRACT_VALIDATED", { tool: "ingest_graph_relations", ...contractMetrics });
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
