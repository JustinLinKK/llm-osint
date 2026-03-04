type RawGraphNode = {
  id: string;
  labels: string[];
  properties: Record<string, unknown>;
};

type RawGraphEdge = {
  id: string;
  source: string;
  target: string;
  type: string;
  properties: Record<string, unknown>;
};

export type GraphNodePayload = RawGraphNode & {
  display: string;
};

export type GraphEdgePayload = RawGraphEdge & {
  display: string;
};

export type GraphProjection = {
  nodes: GraphNodePayload[];
  edges: GraphEdgePayload[];
  totalNodes: number;
  totalEdges: number;
  rootNodeId: string | null;
  rootDisplay: string | null;
  recommendedLayout: string;
  recommendedEgoDepth: number;
};

type AnnotatedNode = GraphNodePayload & {
  effectiveType: string;
  family: string;
  mergeKeys: string[];
  noise: boolean;
  qualityScore: number;
};

type AnnotatedEdge = GraphEdgePayload & {
  semanticType: string;
};

const SEARCH_HOSTS = new Set([
  "duckduckgo.com",
  "html.duckduckgo.com",
  "google.com",
  "www.google.com",
  "bing.com",
  "www.bing.com",
  "search.yahoo.com",
  "example.com",
  "www.example.com",
]);

const ORG_TOKENS = new Set([
  "academy",
  "agency",
  "college",
  "company",
  "committee",
  "corporation",
  "corp",
  "department",
  "foundation",
  "group",
  "hospital",
  "inc",
  "institute",
  "laboratory",
  "lab",
  "llc",
  "ltd",
  "school",
  "startup",
  "university",
]);

const ORG_STOPWORDS = new Set(["the", "of", "at", "for", "and", "in", "on", "to"]);

function normalizeGraphText(value: string): string {
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
    const normalized = normalizeGraphText(text);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    output.push(text);
  }
  return output;
}

function pickFirstString(props: Record<string, unknown>, keys: string[]): string | null {
  for (const key of keys) {
    const value = props[key];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return null;
}

function pickStringArray(props: Record<string, unknown>, key: string): string[] {
  const value = props[key];
  if (!Array.isArray(value)) return [];
  return uniqueStrings(value.map((item) => String(item ?? "")));
}

function truncateGraphText(value: string, maxLength: number): string {
  const normalized = value.replace(/\s+/g, " ").trim();
  if (normalized.length <= maxLength) return normalized;
  return `${normalized.slice(0, Math.max(0, maxLength - 3))}...`;
}

function formatRelationType(type: string): string {
  return type
    .toLowerCase()
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function isUrlLike(value: string): boolean {
  return /^https?:\/\//i.test(value.trim());
}

function isLikelyDomain(value: string): boolean {
  return /^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}$/i.test(value.trim());
}

function looksLikeGraphInternalId(value: string): boolean {
  const text = String(value ?? "").trim();
  if (!text) return false;
  return (
    /^(?:\d+:)?[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:\d+$/i.test(text) ||
    /^ent_[0-9a-f]{8,}$/i.test(text) ||
    /^rel_[0-9a-f]{8,}$/i.test(text)
  );
}

function extractAttributeValues(props: Record<string, unknown>, keys: string[]): string[] {
  const allowed = new Set(keys.map((key) => normalizeGraphText(key)).filter(Boolean));
  const values: string[] = [];
  for (const attribute of pickStringArray(props, "attributes")) {
    const separatorIndex = attribute.indexOf(":");
    if (separatorIndex < 0) continue;
    const key = normalizeGraphText(attribute.slice(0, separatorIndex));
    if (!allowed.has(key)) continue;
    const value = attribute.slice(separatorIndex + 1).trim();
    if (value) values.push(value);
  }
  return uniqueStrings(values);
}

function splitDelimitedValues(value: string): string[] {
  return uniqueStrings(
    String(value ?? "")
      .split(/\s*(?:,|;|\||\band\b)\s*/i)
      .map((part) => part.trim())
      .filter(Boolean)
  );
}

function prettyPlatformLabel(value: string): string {
  const normalized = normalizeGraphText(value);
  const known: Record<string, string> = {
    github: "GitHub",
    gitlab: "GitLab",
    linkedin: "LinkedIn",
    researchgate: "ResearchGate",
    orcid: "ORCID",
    scholar: "Google Scholar",
    "google scholar": "Google Scholar",
    openreview: "OpenReview",
    arxiv: "arXiv",
    "personal site": "Personal Site",
    x: "X",
  };
  if (known[normalized]) return known[normalized];
  return value
    .trim()
    .split(/\s+/)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function derivedDisplayCandidates(type: string, props: Record<string, unknown>): string[] {
  const url = extractAttributeValues(props, ["url", "uri"])[0] ?? pickFirstString(props, ["url", "uri"]) ?? "";
  const platform = extractAttributeValues(props, ["platform"])[0] ?? "";
  const subject = extractAttributeValues(props, ["subject", "subject_org"])[0] ?? "";
  const contactType = extractAttributeValues(props, ["contact_type"])[0] ?? "";
  const explicitTitle = extractAttributeValues(props, ["title", "label", "page_title", "site_title", "display_title", "semantic_title"]);
  const candidates: string[] = [...explicitTitle];
  const normalizedType = normalizeGraphText(type);
  const platformLabel = platform ? prettyPlatformLabel(platform) : "";

  if (normalizedType === "website") {
    if (subject && platformLabel) candidates.push(`${platformLabel} profile for ${subject}`);
    if (subject && contactType) candidates.push(`${prettyPlatformLabel(contactType)} for ${subject}`);
    if (subject) candidates.push(`Website for ${subject}`);
    if (platformLabel) candidates.push(`${platformLabel} profile`);
  } else if (normalizedType === "document") {
    if (subject) candidates.push(`Document for ${subject}`);
  } else if (normalizedType === "imageobject") {
    if (subject) candidates.push(`Image of ${subject}`);
  } else if (normalizedType === "organizationprofile") {
    if (subject) candidates.push(`Profile of ${subject}`);
  }

  if (!candidates.length && url && normalizedType === "website") {
    try {
      const host = new URL(url).hostname.toLowerCase().replace(/^www\./, "");
      if (host) candidates.push(`${prettyPlatformLabel(host.split(".")[0] ?? host)} profile`);
    } catch {
      // ignore malformed URL
    }
  }
  return uniqueStrings(candidates);
}

function graphNameSignature(value: string): string {
  return normalizeGraphText(value.replace(/\s*\([^)]*\)\s*/g, " "))
    .replace(/\b(?:the|of|at|for|and|in|on|to)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokenSet(value: string): Set<string> {
  return new Set(
    graphNameSignature(value)
      .split(" ")
      .map((token) => token.trim())
      .filter(Boolean)
  );
}

function looksLikePersonName(value: string): boolean {
  if (!value || isUrlLike(value) || isLikelyDomain(value)) return false;
  if (/[@/:]|github|linkedin|researchgate|duckduckgo|example\.com/i.test(value)) return false;
  const normalized = normalizeGraphText(value);
  const tokens = normalized.split(" ").filter(Boolean);
  if (tokens.length < 2 || tokens.length > 5) return false;
  if (tokens.some((token) => token.length === 1)) return false;
  return tokens.every((token) => /^[a-z][a-z'-]*$/.test(token));
}

function looksLikeOrganizationName(value: string): boolean {
  if (!value || isUrlLike(value) || isLikelyDomain(value)) return false;
  const normalized = normalizeGraphText(value);
  const tokens = normalized.split(" ").filter(Boolean);
  if (!tokens.length) return false;
  return tokens.some((token) => ORG_TOKENS.has(token));
}

function normalizeGraphEntityType(rawType: unknown): string {
  const normalized = normalizeGraphText(String(rawType ?? ""));
  const explicit: Record<string, string> = {
    article: "Document",
    conference: "Conference",
    contactpoint: "ContactPoint",
    contact_point: "ContactPoint",
    document: "Document",
    domain: "Domain",
    educationalcredential: "EducationalCredential",
    educational_credential: "EducationalCredential",
    educationalinstitution: "Institution",
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
    ip: "IP",
    language: "Language",
    location: "Location",
    occupation: "Occupation",
    organizationprofile: "OrganizationProfile",
    organization_profile: "OrganizationProfile",
    organization: "Organization",
    patent: "Patent",
    person: "Person",
    phone: "Phone",
    profile: "Website",
    project: "Project",
    publication: "Publication",
    repository: "Repository",
    role: "Role",
    snippet: "Snippet",
    timelineevent: "TimelineEvent",
    timeline_event: "TimelineEvent",
    topic: "Topic",
    website: "Website",
  };
  if (explicit[normalized]) return explicit[normalized];
  if (!normalized) return "Entity";
  if (normalized.includes("institution") || normalized.includes("university") || normalized.includes("school")) {
    return "Institution";
  }
  if (normalized.includes("contact point") || normalized.includes("contactpoint")) return "ContactPoint";
  if (normalized.includes("educational credential") || normalized.includes("credential")) return "EducationalCredential";
  if (normalized.includes("experience")) return "Experience";
  if (normalized.includes("affiliation")) return "Affiliation";
  if (normalized.includes("timeline event") || normalized.includes("timelineevent")) return "TimelineEvent";
  if (normalized.includes("occupation")) return "Occupation";
  if (normalized.includes("image object") || normalized.includes("imageobject")) return "ImageObject";
  if (normalized.includes("organization profile") || normalized.includes("org profile") || normalized.includes("subject org")) {
    return "OrganizationProfile";
  }
  if (normalized.includes("language kind") || normalized.includes("programming language") || normalized.includes("spoken language")) {
    return "Language";
  }
  if (normalized.includes("organization") || normalized.includes("company") || normalized.includes("startup")) {
    return "Organization";
  }
  if (normalized.includes("publication") || normalized.includes("paper") || normalized.includes("article")) {
    return "Publication";
  }
  if (normalized.includes("profile")) return "Website";
  return String(rawType ?? "").trim() || "Entity";
}

function deriveEffectiveNodeType(props: Record<string, unknown>, labels: string[]): string {
  const storedType = normalizeGraphEntityType(
    pickFirstString(props, ["type"]) ?? labels[0] ?? "Entity"
  );
  const mergeKeys = pickStringArray(props, "merge_keys");
  const altNames = pickStringArray(props, "alt_names");
  const attributes = pickStringArray(props, "attributes").join(" ").toLowerCase();

  if (
    ["Website", "Document", "Domain", "Repository", "Entity"].includes(storedType) &&
    (mergeKeys.some((key) => key.startsWith("name:person:")) ||
      altNames.some((name) => looksLikePersonName(name)) ||
      /\b(education|affiliation|graduated|student|researcher|founder|employee|location|resides in)\b/.test(attributes))
  ) {
    return "Person";
  }

  if (
    ["Website", "Document", "Entity"].includes(storedType) &&
    (mergeKeys.some((key) => key.startsWith("name:org:") || key.startsWith("sig:org:")) ||
      altNames.some((name) => looksLikeOrganizationName(name)))
  ) {
    return "Organization";
  }

  return storedType;
}

function graphEntityFamily(type: string): string {
  const normalized = normalizeGraphText(type);
  if (normalized === "organization" || normalized === "institution") return "org";
  if (normalized === "publication" || normalized === "document") return "publication";
  if (normalized === "website" || normalized === "domain" || normalized === "email" || normalized === "handle" || normalized === "ip") {
    return "digital";
  }
  if (normalized === "repository") return "repository";
  if (normalized === "language") return "language";
  if (normalized === "contactpoint") return "contact";
  if (normalized === "educationalcredential") return "credential";
  if (normalized === "organizationprofile") return "orgprofile";
  if (["experience", "affiliation", "timelineevent", "occupation"].includes(normalized)) return normalized;
  if (normalized === "imageobject") return "image";
  if (normalized) return normalized;
  return "entity";
}

function familiesCompatible(left: string, right: string): boolean {
  if (left === right) return true;
  const pair = [left, right].sort().join("|");
  return pair === "digital|repository" || pair === "org|organization";
}

function scoreDisplayCandidate(type: string, value: string): number {
  const text = String(value ?? "").trim();
  if (!text) return Number.NEGATIVE_INFINITY;
  const normalized = normalizeGraphText(text);
  const family = graphEntityFamily(type);
  const normalizedType = normalizeGraphText(type);
  const url = isUrlLike(text);
  const domain = isLikelyDomain(text);
  const tokens = normalized.split(" ").filter(Boolean);

  let score = 0;
  if (/snippet:|duckduckgo|example\.com|github_identity_search|gitlab_identity_search/i.test(text)) score -= 280;
  if (looksLikeGraphInternalId(text)) score -= 520;
  if (url) score -= family === "digital" || family === "repository" ? 0 : 220;
  if (domain) score -= family === "digital" ? 0 : 140;
  if (!url && !domain) score += 60;
  score -= Math.max(0, text.length - 40);

  if (family === "person") {
    if (looksLikePersonName(text)) score += 260;
    if (/^\p{Lu}[\p{L}'-]+(?:\s+\p{Lu}[\p{L}'-]+){1,4}$/u.test(text)) score += 80;
    if (/[@/:]|\d/.test(text)) score -= 160;
    if (tokens.length >= 2 && tokens.length <= 4) score += 40;
  } else if (family === "org") {
    if (looksLikeOrganizationName(text)) score += 220;
    if (tokens.length >= 2) score += 30;
    if (/^\(?[A-Z0-9]{2,10}\)?$/.test(text)) score -= 50;
  } else if (family === "publication") {
    if (!url && tokens.length >= 4) score += 180;
    if (!url && text.length >= 20) score += 40;
  } else if (family === "digital") {
    if (url) score += 200;
    if (domain) score += 160;
    if (/^\+?[0-9][0-9().\-\s]{6,}[0-9]$/.test(text)) score += 140;
    if (normalizedType === "website" && !url && !domain) {
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
    if (normalizedType === "imageobject" && !url && tokens.length >= 2) score += 220;
  }

  return score;
}

function selectSemanticDisplay(type: string, props: Record<string, unknown>, fallbackId: string): string {
  const candidates = uniqueStrings([
    pickFirstString(props, ["canonical_name", "display_name", "displayName", "name", "title"]),
    ...derivedDisplayCandidates(type, props),
    ...pickStringArray(props, "alt_names"),
    pickFirstString(props, ["username", "handle", "domain", "address", "email", "uri", "url"]),
    fallbackId,
  ]);

  if (!candidates.length) return "Entity";
  const displayCandidates = candidates.filter((candidate) => !looksLikeGraphInternalId(candidate));
  const rankedCandidates = displayCandidates.length ? displayCandidates : candidates;

  let best = rankedCandidates[0];
  let bestScore = scoreDisplayCandidate(type, best);
  for (const candidate of rankedCandidates.slice(1)) {
    const score = scoreDisplayCandidate(type, candidate);
    if (score > bestScore) {
      best = candidate;
      bestScore = score;
      continue;
    }
    if (score === bestScore && candidate.length < best.length) {
      best = candidate;
    }
  }

  return truncateGraphText(best, 96);
}

function relationTypeFromRaw(rawType: string): string {
  const normalized = normalizeGraphText(rawType).replace(/\s+/g, "_").toUpperCase();
  const aliases: Record<string, string> = {
    ABOUT: "ABOUT",
    AFFILIATION: "AFFILIATED_WITH",
    AFFILIATED_WITH: "AFFILIATED_WITH",
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
    HAS_TIMELINE_EVENT: "HAS_TIMELINE_EVENT",
    ISSUED_BY: "ISSUED_BY",
    KNOWS_LANGUAGE: "KNOWS_LANGUAGE",
    PROFILE: "HAS_PROFILE",
    PUBLICATION: "PUBLISHED",
    PUBLICATION_RECORD: "PUBLISHED",
    RESIDENCE: "LOCATED_IN",
  };
  return aliases[normalized] ?? (normalized || "RELATED_TO");
}

function urlHost(value: string): string {
  if (!isUrlLike(value)) return "";
  try {
    return new URL(value).hostname.toLowerCase().replace(/^www\./, "");
  } catch {
    return "";
  }
}

function isNoiseNode(type: string, props: Record<string, unknown>, display: string): boolean {
  const canonical = pickFirstString(props, ["canonical_name", "url", "uri"]) ?? display;
  const host = urlHost(canonical);
  if (host && SEARCH_HOSTS.has(host)) return true;
  if (host && /duckduckgo|google|bing/.test(host)) return true;
  if (canonical.startsWith("snippet:")) return true;
  if (/^https?:\/\/html\.duckduckgo\.com\/html\/\?q=/i.test(canonical)) return true;
  if (type === "Snippet") return true;
  return false;
}

function nodePriority(type: string): number {
  const normalized = normalizeGraphText(type);
  const rank: Record<string, number> = {
    person: 120,
    institution: 105,
    organization: 100,
    experience: 98,
    educationalcredential: 97,
    affiliation: 96,
    contactpoint: 95,
    role: 95,
    publication: 92,
    conference: 88,
    project: 86,
    repository: 82,
    language: 81,
    topic: 80,
    organizationprofile: 79,
    award: 78,
    grant: 76,
    patent: 74,
    timelineevent: 72,
    occupation: 70,
    website: 40,
    document: 35,
    domain: 30,
    email: 28,
    phone: 28,
    handle: 26,
    imageobject: 24,
    location: 24,
    ip: 18,
    entity: 10,
  };
  return rank[normalized] ?? 12;
}

function deriveViewMergeKeys(type: string, props: Record<string, unknown>, display: string): string[] {
  const family = graphEntityFamily(type);
  const keys = pickStringArray(props, "merge_keys").filter((key) => {
    const normalized = normalizeGraphText(key);
    if (!normalized) return false;
    if (family === "publication") {
      return (
        normalized.startsWith("name publication ") ||
        normalized.startsWith("sig publication ") ||
        normalized.startsWith("url publication ") ||
        normalized.startsWith("pubid ") ||
        normalized.startsWith("id publication ")
      );
    }
    if (normalized.startsWith("host ")) {
      return family === "org" || family === "conference";
    }
    return true;
  });
  const names = uniqueStrings([display, pickFirstString(props, ["canonical_name"]), ...pickStringArray(props, "alt_names")]);
  for (const name of names) {
    if (looksLikeGraphInternalId(name)) continue;
    const normalized = normalizeGraphText(name);
    if (normalized) keys.push(`display:${family}:${normalized}`);
    const signature = graphNameSignature(name);
    if (signature && signature !== normalized && (family === "org" || family === "publication" || family === "language")) {
      keys.push(`sig:${family}:${signature}`);
    }
  }
  return uniqueStrings(keys);
}

function organizationDisplaysShouldMerge(left: AnnotatedNode, right: AnnotatedNode): boolean {
  if (left.family !== "org" || right.family !== "org") return false;
  const leftSig = graphNameSignature(left.display);
  const rightSig = graphNameSignature(right.display);
  if (!leftSig || !rightSig) return false;
  if (leftSig === rightSig) return true;
  const leftTokens = tokenSet(left.display);
  const rightTokens = tokenSet(right.display);
  const smaller = leftTokens.size <= rightTokens.size ? leftTokens : rightTokens;
  const larger = leftTokens.size <= rightTokens.size ? rightTokens : leftTokens;
  if (smaller.size < 2) return false;
  for (const token of smaller) {
    if (!larger.has(token)) return false;
  }
  return true;
}

function promptMentionsAlias(promptText: string, aliases: string[]): boolean {
  const promptNormalized = normalizeGraphText(promptText);
  return aliases.some((alias) => {
    const normalized = normalizeGraphText(alias);
    if (!normalized || normalized.length < 4) return false;
    return promptNormalized.includes(normalized);
  });
}

function selectPromptMatchedAlias(promptText: string, entityType: string, aliases: string[]): string | null {
  const promptNormalized = normalizeGraphText(promptText);
  if (!promptNormalized) return null;
  let best: string | null = null;
  let bestScore = Number.NEGATIVE_INFINITY;
  for (const alias of uniqueStrings(aliases)) {
    const normalized = normalizeGraphText(alias);
    if (!normalized || normalized.length < 4) continue;
    if (!promptNormalized.includes(normalized)) continue;
    const score = scoreDisplayCandidate(entityType, alias) + normalized.length * 2;
    if (score > bestScore) {
      best = alias;
      bestScore = score;
    }
  }
  return best;
}

function buildAdjacency(edges: GraphEdgePayload[]): Map<string, Set<string>> {
  const adjacency = new Map<string, Set<string>>();
  for (const edge of edges) {
    const sourceLinks = adjacency.get(edge.source) ?? new Set<string>();
    sourceLinks.add(edge.target);
    adjacency.set(edge.source, sourceLinks);
    const targetLinks = adjacency.get(edge.target) ?? new Set<string>();
    targetLinks.add(edge.source);
    adjacency.set(edge.target, targetLinks);
  }
  return adjacency;
}

function bfsDepths(adjacency: Map<string, Set<string>>, rootNodeId: string): Map<string, number> {
  const depths = new Map<string, number>();
  depths.set(rootNodeId, 0);
  const queue: string[] = [rootNodeId];
  while (queue.length) {
    const current = queue.shift();
    if (!current) continue;
    const depth = depths.get(current) ?? 0;
    for (const next of adjacency.get(current) ?? []) {
      if (depths.has(next)) continue;
      depths.set(next, depth + 1);
      queue.push(next);
    }
  }
  return depths;
}

function chooseClusterType(nodes: AnnotatedNode[]): string {
  const counts = new Map<string, number>();
  for (const node of nodes) {
    counts.set(node.effectiveType, (counts.get(node.effectiveType) ?? 0) + 1);
  }
  return [...counts.entries()]
    .sort((left, right) => {
      if (right[1] !== left[1]) return right[1] - left[1];
      return nodePriority(right[0]) - nodePriority(left[0]);
    })[0]?.[0] ?? "Entity";
}

function chooseRootNode(nodes: AnnotatedNode[], edges: GraphEdgePayload[], prompt: string, title: string): string | null {
  if (!nodes.length) return null;
  const adjacency = buildAdjacency(edges);
  const promptText = [prompt, title].filter(Boolean).join(" ");

  let bestNodeId: string | null = null;
  let bestScore = Number.NEGATIVE_INFINITY;
  for (const node of nodes) {
    const aliases = uniqueStrings([node.display, ...pickStringArray(node.properties, "alt_names")]);
    const degree = adjacency.get(node.id)?.size ?? 0;
    let score = degree * 12 + node.qualityScore + nodePriority(node.effectiveType);
    if (node.effectiveType === "Person") score += 280;
    if (promptMentionsAlias(promptText, aliases)) score += 220;
    if (pickStringArray(node.properties, "merge_keys").some((key) => key.startsWith("name:person:"))) score += 180;
    if (isUrlLike(node.display)) score -= 100;
    if (node.noise) score -= 400;
    if (score > bestScore) {
      bestScore = score;
      bestNodeId = node.id;
    }
  }
  return bestNodeId;
}

function syntheticNodeId(type: string, value: string): string {
  return `synthetic:${normalizeGraphText(type)}:${normalizeGraphText(value).replace(/\s+/g, "_").slice(0, 120)}`;
}

function upsertSyntheticNode(
  projectedNodes: Map<string, AnnotatedNode>,
  type: string,
  value: string,
  options: {
    altNames?: string[];
    attributes?: string[];
  } = {}
): AnnotatedNode | null {
  const canonicalValue = String(value ?? "").trim();
  if (!canonicalValue || looksLikeGraphInternalId(canonicalValue)) return null;
  const effectiveType = normalizeGraphEntityType(type);
  const id = syntheticNodeId(effectiveType, canonicalValue);
  const existing = projectedNodes.get(id);
  const baseProps: Record<string, unknown> = {
    ...(existing?.properties ?? {}),
    type: effectiveType,
    canonical_name: pickFirstString(existing?.properties ?? {}, ["canonical_name"]) ?? canonicalValue,
    alt_names: uniqueStrings([...(pickStringArray(existing?.properties ?? {}, "alt_names") ?? []), ...(options.altNames ?? [])]),
    attributes: uniqueStrings([...(pickStringArray(existing?.properties ?? {}, "attributes") ?? []), ...(options.attributes ?? [])]),
    synthetic: true,
  };
  const display = selectSemanticDisplay(effectiveType, baseProps, id);
  const mergeKeys = deriveViewMergeKeys(effectiveType, baseProps, display);
  const annotated: AnnotatedNode = {
    id,
    labels: existing?.labels?.length ? existing.labels : [effectiveType],
    properties: {
      ...baseProps,
      merge_keys: mergeKeys,
      semantic_name: display,
    },
    display,
    effectiveType,
    family: graphEntityFamily(effectiveType),
    mergeKeys,
    noise: isNoiseNode(effectiveType, baseProps, display),
    qualityScore: scoreDisplayCandidate(effectiveType, display),
  };
  projectedNodes.set(id, annotated);
  return annotated.noise ? null : annotated;
}

function upsertSyntheticEdge(
  projectedEdgesByKey: Map<string, AnnotatedEdge>,
  source: string | null,
  target: string | null,
  semanticType: string,
  properties: Record<string, unknown> = {}
): void {
  if (!source || !target || source === target) return;
  const key = `${source}|${target}|${semanticType}`;
  if (projectedEdgesByKey.has(key)) return;
  projectedEdgesByKey.set(key, {
    id: `synthetic:${key}`,
    source,
    target,
    type: semanticType,
    display: formatRelationType(semanticType),
    properties: {
      rel_type: semanticType,
      synthetic: true,
      ...properties,
    },
    semanticType,
  });
}

function augmentProjectedGraph(
  projectedNodes: Map<string, AnnotatedNode>,
  projectedEdgesByKey: Map<string, AnnotatedEdge>
): void {
  const seedNodes = [...projectedNodes.values()];
  for (const node of seedNodes) {
    if (node.noise) continue;
    if (node.effectiveType === "Publication" || node.effectiveType === "Document") {
      for (const venue of extractAttributeValues(node.properties, ["venue", "journal", "conference"])) {
        const venueNode = upsertSyntheticNode(projectedNodes, "Conference", venue);
        if (venueNode) upsertSyntheticEdge(projectedEdgesByKey, node.id, venueNode.id, "PUBLISHED_IN");
      }
      for (const authorField of extractAttributeValues(node.properties, ["co-authors", "coauthors", "authors", "author_names"])) {
        for (const authorName of splitDelimitedValues(authorField).slice(0, 12)) {
          if (!looksLikePersonName(authorName)) continue;
          const authorNode = upsertSyntheticNode(projectedNodes, "Person", authorName);
          if (authorNode) upsertSyntheticEdge(projectedEdgesByKey, authorNode.id, node.id, "PUBLISHED");
        }
      }
    }

    if (node.effectiveType === "Role") {
      for (const organization of extractAttributeValues(node.properties, ["organization", "company", "institution", "employer"])) {
        const type = looksLikeOrganizationName(organization) && /university|college|institute|school|lab/i.test(organization)
          ? "Institution"
          : "Organization";
        const organizationNode = upsertSyntheticNode(projectedNodes, type, organization);
        if (organizationNode) upsertSyntheticEdge(projectedEdgesByKey, node.id, organizationNode.id, "AFFILIATED_WITH");
      }
    }

    if (node.effectiveType === "ContactPoint") {
      for (const email of extractAttributeValues(node.properties, ["email", "value"])) {
        if (/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) {
          const emailNode = upsertSyntheticNode(projectedNodes, "Email", email);
          if (emailNode) upsertSyntheticEdge(projectedEdgesByKey, node.id, emailNode.id, "HAS_EMAIL");
        }
      }
      for (const phone of extractAttributeValues(node.properties, ["phone", "value"])) {
        if (/^\+?[0-9][0-9().\-\s]{6,}[0-9]$/.test(phone)) {
          const phoneNode = upsertSyntheticNode(projectedNodes, "Phone", phone);
          if (phoneNode) upsertSyntheticEdge(projectedEdgesByKey, node.id, phoneNode.id, "HAS_PHONE");
        }
      }
      for (const handle of extractAttributeValues(node.properties, ["handle", "username", "value"])) {
        if (handle.startsWith("@")) {
          const handleNode = upsertSyntheticNode(projectedNodes, "Handle", handle);
          if (handleNode) upsertSyntheticEdge(projectedEdgesByKey, node.id, handleNode.id, "HAS_HANDLE");
        }
      }
      for (const url of extractAttributeValues(node.properties, ["url", "value"])) {
        if (isUrlLike(url)) {
          const websiteNode = upsertSyntheticNode(projectedNodes, "Website", url);
          if (websiteNode) upsertSyntheticEdge(projectedEdgesByKey, node.id, websiteNode.id, "HAS_PROFILE");
        }
      }
    }

    if (node.effectiveType === "Experience") {
      for (const role of extractAttributeValues(node.properties, ["role"])) {
        const roleNode = upsertSyntheticNode(projectedNodes, "Role", role, { attributes: [`organization: ${extractAttributeValues(node.properties, ["organization"])[0] ?? ""}`] });
        if (roleNode) upsertSyntheticEdge(projectedEdgesByKey, node.id, roleNode.id, "HAS_ROLE");
      }
      for (const organization of extractAttributeValues(node.properties, ["organization", "institution", "company", "employer"])) {
        const type = /university|college|institute|school|lab/i.test(organization) ? "Institution" : "Organization";
        const organizationNode = upsertSyntheticNode(projectedNodes, type, organization);
        if (organizationNode) {
          const relType = /student|phd|master|bachelor|mba/i.test(pickStringArray(node.properties, "attributes").join(" ")) ? "STUDIED_AT" : "WORKS_AT";
          upsertSyntheticEdge(projectedEdgesByKey, node.id, organizationNode.id, relType);
        }
      }
    }

    if (node.effectiveType === "EducationalCredential") {
      for (const institution of extractAttributeValues(node.properties, ["institution", "organization", "school"])) {
        const institutionNode = upsertSyntheticNode(projectedNodes, "Institution", institution);
        if (institutionNode) upsertSyntheticEdge(projectedEdgesByKey, node.id, institutionNode.id, "ISSUED_BY");
      }
      for (const field of extractAttributeValues(node.properties, ["field"])) {
        const topicNode = upsertSyntheticNode(projectedNodes, "Topic", field);
        if (topicNode) upsertSyntheticEdge(projectedEdgesByKey, node.id, topicNode.id, "HAS_TOPIC");
      }
    }

    if (node.effectiveType === "Affiliation") {
      for (const organization of extractAttributeValues(node.properties, ["organization", "institution", "company"])) {
        const type = /university|college|institute|school|lab/i.test(organization) ? "Institution" : "Organization";
        const organizationNode = upsertSyntheticNode(projectedNodes, type, organization);
        if (organizationNode) upsertSyntheticEdge(projectedEdgesByKey, node.id, organizationNode.id, "AFFILIATED_WITH");
      }
    }

    if (node.effectiveType === "Award" || node.effectiveType === "Grant") {
      const orgKeys = node.effectiveType === "Award" ? ["issuer", "organization", "institution"] : ["institution", "organization"];
      for (const organization of extractAttributeValues(node.properties, orgKeys)) {
        const type = /university|college|institute|school|lab/i.test(organization) ? "Institution" : "Organization";
        const organizationNode = upsertSyntheticNode(projectedNodes, type, organization);
        if (organizationNode) upsertSyntheticEdge(projectedEdgesByKey, node.id, organizationNode.id, "AFFILIATED_WITH");
      }
    }

    if (node.effectiveType === "Repository" || node.effectiveType === "Project") {
      for (const language of extractAttributeValues(node.properties, ["language"])) {
        const languageNode = upsertSyntheticNode(projectedNodes, "Language", language, { attributes: ["language_kind: programming"] });
        if (languageNode) upsertSyntheticEdge(projectedEdgesByKey, node.id, languageNode.id, "USES_LANGUAGE");
      }
      for (const topicField of extractAttributeValues(node.properties, ["topics", "topic"])) {
        for (const topic of splitDelimitedValues(topicField).slice(0, 10)) {
          const topicNode = upsertSyntheticNode(projectedNodes, "Topic", topic);
          if (topicNode) upsertSyntheticEdge(projectedEdgesByKey, node.id, topicNode.id, "HAS_TOPIC");
        }
      }
    }

    if (node.effectiveType === "Organization" || node.effectiveType === "Institution") {
      const summary = extractAttributeValues(node.properties, ["summary"])[0] ?? "";
      const whyRelevant = extractAttributeValues(node.properties, ["why_relevant"])[0] ?? "";
      const industry = extractAttributeValues(node.properties, ["industry"])[0] ?? "";
      const focusValues = extractAttributeValues(node.properties, ["focus"]);
      if (summary || whyRelevant || industry || focusValues.length) {
        const profileNode = upsertSyntheticNode(projectedNodes, "OrganizationProfile", `Profile of ${node.display}`, {
          attributes: [
            `subject_org: ${node.display}`,
            ...(summary ? [`summary: ${summary}`] : []),
            ...(whyRelevant ? [`why_relevant: ${whyRelevant}`] : []),
            ...(industry ? [`industry: ${industry}`] : []),
            ...focusValues.map((value) => `focus: ${value}`),
          ],
        });
        if (profileNode) {
          upsertSyntheticEdge(projectedEdgesByKey, node.id, profileNode.id, "HAS_ORGANIZATION_PROFILE");
        }
      }
      for (const focusValue of extractAttributeValues(node.properties, ["focus", "industry"])) {
        const topicNode = upsertSyntheticNode(projectedNodes, "Topic", focusValue);
        if (topicNode) upsertSyntheticEdge(projectedEdgesByKey, node.id, topicNode.id, "FOCUSES_ON");
      }
    }

    if (node.effectiveType === "OrganizationProfile") {
      for (const focusValue of extractAttributeValues(node.properties, ["focus", "industry"])) {
        const topicNode = upsertSyntheticNode(projectedNodes, "Topic", focusValue);
        if (topicNode) upsertSyntheticEdge(projectedEdgesByKey, node.id, topicNode.id, "FOCUSES_ON");
      }
    }
  }

  const nodesById = projectedNodes;
  const publicationAuthors = new Map<string, Set<string>>();
  const publicationVenues = new Map<string, Set<string>>();
  for (const edge of projectedEdgesByKey.values()) {
    const sourceNode = nodesById.get(edge.source);
    const targetNode = nodesById.get(edge.target);
    if (!sourceNode || !targetNode) continue;
    if (
      edge.semanticType === "PUBLISHED" &&
      sourceNode.effectiveType === "Person" &&
      (targetNode.effectiveType === "Publication" || targetNode.effectiveType === "Document")
    ) {
      const authors = publicationAuthors.get(targetNode.id) ?? new Set<string>();
      authors.add(sourceNode.id);
      publicationAuthors.set(targetNode.id, authors);
    }
    if (
      edge.semanticType === "PUBLISHED_IN" &&
      (sourceNode.effectiveType === "Publication" || sourceNode.effectiveType === "Document") &&
      targetNode.effectiveType === "Conference"
    ) {
      const venues = publicationVenues.get(sourceNode.id) ?? new Set<string>();
      venues.add(targetNode.id);
      publicationVenues.set(sourceNode.id, venues);
    }
  }

  for (const [publicationId, authorIds] of publicationAuthors.entries()) {
    const authors = [...authorIds].slice(0, 10);
    for (let index = 0; index < authors.length; index += 1) {
      for (let inner = index + 1; inner < authors.length; inner += 1) {
        const [source, target] = [authors[index], authors[inner]].sort();
        upsertSyntheticEdge(projectedEdgesByKey, source, target, "COAUTHORED_WITH");
      }
      for (const venueId of publicationVenues.get(publicationId) ?? []) {
        upsertSyntheticEdge(projectedEdgesByKey, authors[index] ?? null, venueId, "PUBLISHED_IN");
      }
    }
  }
}

export function projectRunGraph(
  rawNodes: RawGraphNode[],
  rawEdges: RawGraphEdge[],
  options: {
    prompt?: string | null;
    title?: string | null;
    nodeLimit: number;
    nodeOffset: number;
    edgeLimit: number;
    edgeOffset: number;
  }
): GraphProjection {
  const annotatedNodes: AnnotatedNode[] = rawNodes.map((node) => {
    const effectiveType = deriveEffectiveNodeType(node.properties, node.labels);
    const display = selectSemanticDisplay(effectiveType, node.properties, node.id);
    const mergeKeys = deriveViewMergeKeys(effectiveType, node.properties, display);
    const noise = isNoiseNode(effectiveType, node.properties, display);
    const qualityScore = scoreDisplayCandidate(effectiveType, display);
    return {
      ...node,
      display,
      effectiveType,
      family: graphEntityFamily(effectiveType),
      mergeKeys,
      noise,
      qualityScore,
    };
  });

  const parent = new Map<string, string>();
  const find = (id: string): string => {
    let current = parent.get(id) ?? id;
    while ((parent.get(current) ?? current) !== current) {
      current = parent.get(current) ?? current;
    }
    let cursor = id;
    while ((parent.get(cursor) ?? cursor) !== current) {
      const next = parent.get(cursor);
      parent.set(cursor, current);
      if (!next) break;
      cursor = next;
    }
    return current;
  };
  const union = (left: string, right: string) => {
    const leftRoot = find(left);
    const rightRoot = find(right);
    if (leftRoot === rightRoot) return;
    parent.set(rightRoot, leftRoot);
  };

  for (const node of annotatedNodes) parent.set(node.id, node.id);

  const keyOwners = new Map<string, string>();
  for (const node of annotatedNodes) {
    for (const key of node.mergeKeys) {
      const ownerId = keyOwners.get(key);
      if (!ownerId) {
        keyOwners.set(key, node.id);
        continue;
      }
      const owner = annotatedNodes.find((candidate) => candidate.id === ownerId);
      if (owner && familiesCompatible(owner.family, node.family)) {
        union(owner.id, node.id);
      }
    }
  }

  for (let index = 0; index < annotatedNodes.length; index += 1) {
    for (let inner = index + 1; inner < annotatedNodes.length; inner += 1) {
      const left = annotatedNodes[index];
      const right = annotatedNodes[inner];
      if (organizationDisplaysShouldMerge(left, right)) {
        union(left.id, right.id);
      }
    }
  }

  const clusters = new Map<string, AnnotatedNode[]>();
  for (const node of annotatedNodes) {
    const rootId = find(node.id);
    const bucket = clusters.get(rootId) ?? [];
    bucket.push(node);
    clusters.set(rootId, bucket);
  }

  const projectedNodes = new Map<string, AnnotatedNode>();
  const rawToProjected = new Map<string, string>();
  for (const members of clusters.values()) {
    const type = chooseClusterType(members);
    const primary = [...members].sort((left, right) => {
      const scoreDelta =
        right.qualityScore + nodePriority(right.effectiveType) - (left.qualityScore + nodePriority(left.effectiveType));
      if (scoreDelta !== 0) return scoreDelta;
      return left.display.localeCompare(right.display);
    })[0];
    const display = selectSemanticDisplay(
      type,
      {
        ...primary.properties,
        alt_names: uniqueStrings(members.flatMap((member) => pickStringArray(member.properties, "alt_names")).concat(members.map((member) => member.display))),
      },
      primary.id
    );
    const properties = {
      ...primary.properties,
      type,
      semantic_name: display,
      alt_names: uniqueStrings(
        members.flatMap((member) => [
          member.display,
          ...pickStringArray(member.properties, "alt_names"),
          pickFirstString(member.properties, ["canonical_name"]),
        ])
      ).filter((value) => normalizeGraphText(value) !== normalizeGraphText(display)),
      merge_keys: uniqueStrings(members.flatMap((member) => member.mergeKeys)),
      member_count: members.length,
      member_node_ids: members.map((member) => member.id),
      source_types: uniqueStrings(members.map((member) => String(member.properties.type ?? member.effectiveType))),
    } satisfies Record<string, unknown>;
    const projected: AnnotatedNode = {
      id: primary.id,
      labels: uniqueStrings(members.flatMap((member) => member.labels)),
      properties,
      display,
      effectiveType: type,
      family: graphEntityFamily(type),
      mergeKeys: uniqueStrings(members.flatMap((member) => member.mergeKeys)),
      noise: members.every((member) => member.noise),
      qualityScore: Math.max(...members.map((member) => member.qualityScore)),
    };
    projectedNodes.set(projected.id, projected);
    for (const member of members) {
      rawToProjected.set(member.id, projected.id);
    }
  }

  const projectedEdgesByKey = new Map<string, AnnotatedEdge>();
  for (const edge of rawEdges) {
    const source = rawToProjected.get(edge.source);
    const target = rawToProjected.get(edge.target);
    if (!source || !target || source === target) continue;
    const sourceNode = projectedNodes.get(source);
    const targetNode = projectedNodes.get(target);
    if (!sourceNode || !targetNode) continue;
    if (sourceNode.noise || targetNode.noise) continue;
    const semanticType = relationTypeFromRaw(edge.type);
    const key = `${source}|${target}|${semanticType}`;
    const existing = projectedEdgesByKey.get(key);
    if (existing) continue;
    projectedEdgesByKey.set(key, {
      id: edge.id,
      source,
      target,
      type: semanticType,
      display: formatRelationType(semanticType),
      properties: {
        ...edge.properties,
        rel_type: semanticType,
      },
      semanticType,
    });
  }

  augmentProjectedGraph(projectedNodes, projectedEdgesByKey);

  const survivingNodes = [...projectedNodes.values()].filter((node) => !node.noise);
  const survivingEdges = [...projectedEdgesByKey.values()];
  const rootNodeId = chooseRootNode(survivingNodes, survivingEdges, options.prompt ?? "", options.title ?? "");
  const promptText = [options.prompt, options.title].filter(Boolean).join(" ");
  if (rootNodeId) {
    const rootNode = projectedNodes.get(rootNodeId);
    if (rootNode) {
      const preferredAlias = selectPromptMatchedAlias(promptText, rootNode.effectiveType, [
        rootNode.display,
        ...pickStringArray(rootNode.properties, "alt_names"),
      ]);
      if (preferredAlias) {
        const display = truncateGraphText(preferredAlias, 96);
        rootNode.display = display;
        rootNode.properties = {
          ...rootNode.properties,
          semantic_name: display,
          alt_names: uniqueStrings([rootNode.display, ...pickStringArray(rootNode.properties, "alt_names")]).filter(
            (value) => normalizeGraphText(value) !== normalizeGraphText(display)
          ),
        };
      }
    }
  }
  const adjacency = rootNodeId ? buildAdjacency(survivingEdges) : new Map<string, Set<string>>();
  const depths = rootNodeId ? bfsDepths(adjacency, rootNodeId) : new Map<string, number>();

  const orderedNodes = survivingNodes.sort((left, right) => {
    const leftDepth = depths.get(left.id) ?? Number.MAX_SAFE_INTEGER;
    const rightDepth = depths.get(right.id) ?? Number.MAX_SAFE_INTEGER;
    if (leftDepth !== rightDepth) return leftDepth - rightDepth;
    const priorityDelta = nodePriority(right.effectiveType) - nodePriority(left.effectiveType);
    if (priorityDelta !== 0) return priorityDelta;
    return left.display.localeCompare(right.display);
  });

  const pagedNodeIds = new Set(
    orderedNodes
      .slice(options.nodeOffset, options.nodeOffset + options.nodeLimit)
      .map((node) => node.id)
  );
  if (rootNodeId && !pagedNodeIds.has(rootNodeId)) {
    pagedNodeIds.add(rootNodeId);
  }

  const orderedEdges = survivingEdges.filter((edge) => pagedNodeIds.has(edge.source) && pagedNodeIds.has(edge.target));
  const pagedEdges = orderedEdges.slice(options.edgeOffset, options.edgeOffset + options.edgeLimit);
  for (const edge of pagedEdges) {
    pagedNodeIds.add(edge.source);
    pagedNodeIds.add(edge.target);
  }

  const nodes = orderedNodes
    .filter((node) => pagedNodeIds.has(node.id))
    .map((node) => ({
      id: node.id,
      labels: node.labels,
      properties: node.properties,
      display: node.display,
    }));

  const edges = pagedEdges.map((edge) => ({
    id: edge.id,
    source: edge.source,
    target: edge.target,
    type: edge.type,
    display: edge.display,
    properties: edge.properties,
  }));

  return {
    nodes,
    edges,
    totalNodes: survivingNodes.length,
    totalEdges: survivingEdges.length,
    rootNodeId,
    rootDisplay: rootNodeId ? projectedNodes.get(rootNodeId)?.display ?? null : null,
    recommendedLayout: rootNodeId ? "grouped" : "cose",
    recommendedEgoDepth: rootNodeId ? 2 : 1,
  };
}
