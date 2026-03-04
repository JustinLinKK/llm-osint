import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { createWriteStream } from "node:fs";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, extname, isAbsolute, join, relative, resolve } from "node:path";
import { pipeline } from "node:stream/promises";
import crypto from "node:crypto";
import { v4 as uuidv4 } from "uuid";
import { cfg } from "../config.js";
import { pool } from "../clients/pg.js";
import { minio, ensureBucket } from "../clients/minio.js";
import { emitRunEvent, logToolCall } from "./helpers.js";
import { runPythonTool } from "./python_bridge.js";
import { logger } from "../utils/logger.js";

type PythonToolConfig = {
  name: string;
  description: string;
  scriptPath: string;
  timeoutMs?: number;
};

type StoredResult = {
  documentId: string;
  bucket: string;
  objectKey: string;
  versionId: string | null;
  etag: string | null;
  sizeBytes: number;
  contentType: string;
  sha256: string;
};

type StoredFileArtifact = {
  documentId: string;
  sourcePath: string;
  sourceUrl: string | null;
  title: string | null;
  bucket: string;
  objectKey: string;
  versionId: string | null;
  etag: string | null;
  sizeBytes: number;
  contentType: string;
  sha256: string;
};

type ResponseMode = "compact" | "full";

type ResearchArtifactDescriptor = {
  sourcePath: string;
  sourceUrl?: string | null;
  title?: string | null;
  sourceType?: string | null;
  contentType?: string | null;
};

const PYTHON_TIMEOUT_FAST_MS = 60000;
const PYTHON_TIMEOUT_DEFAULT_MS = 90000;
const PYTHON_TIMEOUT_SLOW_MS = 120000;
const PYTHON_TIMEOUT_HEAVY_MS = 180000;

const configSchema = z.array(
  z.object({
    name: z.string().min(1),
    description: z.string().min(1),
    scriptPath: z.string().min(1),
    timeoutMs: z.number().int().positive().optional(),
  })
);

function kaliOsintToolPreset(): PythonToolConfig[] {
  const runnerPath = "apps/mcp-server/src/tools/tools_python/osint_tool_runner.py";
  return [
    {
      name: "osint_sherlock_username",
      description: "Enumerate username presence across social platforms with Sherlock",
      scriptPath: runnerPath,
      timeoutMs: 180000,
    },
    {
      name: "osint_maigret_username",
      description: "Deep username profiling with Maigret metadata",
      scriptPath: runnerPath,
      timeoutMs: PYTHON_TIMEOUT_HEAVY_MS,
    },
    {
      name: "osint_holehe_email",
      description: "Check service registrations linked to an email with holehe",
      scriptPath: runnerPath,
      timeoutMs: 120000,
    },
    {
      name: "osint_theharvester_email_domain",
      description: "Harvest emails, names, and hosts for a domain via theHarvester",
      scriptPath: runnerPath,
      timeoutMs: PYTHON_TIMEOUT_SLOW_MS,
    },
    {
      name: "osint_amass_domain",
      description: "Perform passive domain intel and subdomain enumeration with Amass",
      scriptPath: runnerPath,
      timeoutMs: PYTHON_TIMEOUT_HEAVY_MS,
    },
    {
      name: "osint_sublist3r_domain",
      description: "Discover subdomains with Sublist3r",
      scriptPath: runnerPath,
      timeoutMs: PYTHON_TIMEOUT_SLOW_MS,
    },
    {
      name: "osint_whatweb_target",
      description: "Fingerprint web technologies used by a target with WhatWeb",
      scriptPath: runnerPath,
      timeoutMs: 120000,
    },
    {
      name: "osint_exiftool_extract",
      description: "Extract EXIF/metadata from a local file or MinIO object with ExifTool",
      scriptPath: runnerPath,
      timeoutMs: 60000,
    },
    {
      name: "osint_phoneinfoga_number",
      description: "Collect phone number intelligence with PhoneInfoga",
      scriptPath: runnerPath,
      timeoutMs: PYTHON_TIMEOUT_SLOW_MS,
    },
    {
      name: "osint_reconng_domain",
      description: "Run Recon-ng modules for domain/entity profiling",
      scriptPath: runnerPath,
      timeoutMs: PYTHON_TIMEOUT_HEAVY_MS,
    },
    {
      name: "osint_spiderfoot_scan",
      description: "Run SpiderFoot scan for target footprint expansion",
      scriptPath: runnerPath,
      timeoutMs: PYTHON_TIMEOUT_HEAVY_MS,
    },
    {
      name: "osint_whatsmyname_username",
      description: "Cross-platform username checks using WhatsMyName data",
      scriptPath: runnerPath,
      timeoutMs: 180000,
    },
    // Disabled by default (requires API key).
    // HIBP key: https://haveibeenpwned.com/API/Key
    // {
    //   name: "osint_hibp_email",
    //   description: "Check email exposure in breaches via Have I Been Pwned API",
    //   scriptPath: runnerPath,
    //   timeoutMs: 120000,
    // },
    // Disabled by default (requires API key).
    // Shodan key: https://account.shodan.io/
    // {
    //   name: "osint_shodan_host",
    //   description: "Look up exposed services for an IP via Shodan API",
    //   scriptPath: runnerPath,
    //   timeoutMs: 120000,
    // },
    {
      name: "osint_dnsdumpster_domain",
      description: "Passive DNS and host mapping via DNSDumpster client",
      scriptPath: runnerPath,
      timeoutMs: PYTHON_TIMEOUT_SLOW_MS,
    },
    {
      name: "osint_maltego_manual",
      description: "Manual Maltego transform placeholder (GUI workflow)",
      scriptPath: runnerPath,
      timeoutMs: 60000,
    },
    {
      name: "osint_foca_manual",
      description: "Manual FOCA workflow placeholder (Windows GUI workflow)",
      scriptPath: runnerPath,
      timeoutMs: 60000,
    },
  ];
}

function researchIntegrationToolPreset(): PythonToolConfig[] {
  const wrapperPath = "apps/mcp-server/src/tools/tools_python/unified_research_mcp.py";
  return [
    {
      name: "web_search",
      description: "Search the web via Tavily and return ranked source snippets",
      scriptPath: wrapperPath,
      timeoutMs: PYTHON_TIMEOUT_SLOW_MS,
    },
    {
      name: "extract_webpage",
      description: "Extract webpage content via Tavily for one or more URLs",
      scriptPath: wrapperPath,
      timeoutMs: PYTHON_TIMEOUT_SLOW_MS,
    },
    {
      name: "crawl_webpage",
      description: "Crawl a website via Tavily and extract matched pages",
      scriptPath: wrapperPath,
      timeoutMs: PYTHON_TIMEOUT_HEAVY_MS,
    },
    {
      name: "map_webpage",
      description: "Map a website via Tavily and return discovered URLs",
      scriptPath: wrapperPath,
      timeoutMs: PYTHON_TIMEOUT_SLOW_MS,
    },
    {
      name: "tavily_research",
      description: "Run Tavily's async research workflow and return a report with cited sources",
      scriptPath: wrapperPath,
      timeoutMs: PYTHON_TIMEOUT_HEAVY_MS,
    },
    {
      name: "tavily_person_search",
      description: "Search a person via Tavily and return source-backed discovery results",
      scriptPath: wrapperPath,
      timeoutMs: PYTHON_TIMEOUT_SLOW_MS,
    },
    {
      name: "person_search",
      description: "Search web pages about a person and extract page content",
      scriptPath: wrapperPath,
      timeoutMs: PYTHON_TIMEOUT_DEFAULT_MS,
    },
    {
      name: "x_get_user_posts_api",
      description: "Fetch X posts via official API v2",
      scriptPath: wrapperPath,
      timeoutMs: PYTHON_TIMEOUT_SLOW_MS,
    },
    {
      name: "linkedin_download_html_ocr",
      description: "Download LinkedIn profile/activity HTML via Browserbase",
      scriptPath: wrapperPath,
      timeoutMs: PYTHON_TIMEOUT_HEAVY_MS,
    },
    {
      name: "google_serp_person_search",
      description: "Search person via Google SERP service and archive HTML results",
      scriptPath: wrapperPath,
      timeoutMs: PYTHON_TIMEOUT_SLOW_MS,
    },
    {
      name: "arxiv_search_and_download",
      description: "Search arXiv and download papers",
      scriptPath: wrapperPath,
      timeoutMs: PYTHON_TIMEOUT_HEAVY_MS,
    },
    {
      name: "arxiv_paper_ingest",
      description: "Fetch one arXiv paper, download its PDF, and extract topics/coauthors/contact signals",
      scriptPath: wrapperPath,
      timeoutMs: PYTHON_TIMEOUT_HEAVY_MS,
    },
    {
      name: "github_identity_search",
      description: "Resolve a GitHub identity and compact public repository/profile signals",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "gitlab_identity_search",
      description: "Resolve a GitLab identity and compact public project/profile signals",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "personal_site_search",
      description: "Resolve a personal site URL/domain and extract compact public contact/linkage signals",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "npm_author_search",
      description: "Search npm packages by author, maintainer username, or email",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "package_registry_search",
      description: "Aggregate public package registry searches across npm and crates.io",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "crates_author_search",
      description: "Search crates.io users and published crates for a person or username",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "wayback_fetch_url",
      description: "Fetch compact Wayback snapshot metadata for a target URL",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "open_corporates_search",
      description: "Resolve a company via OpenCorporates and fetch compact company/officer metadata",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "company_officer_search",
      description: "Search OpenCorporates officer roles for a person",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "company_filing_search",
      description: "Fetch company filing history via OpenCorporates or SEC submissions",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "sec_person_search",
      description: "Search SEC filings for person or company involvement",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "director_disclosure_search",
      description: "Extract structured director disclosures from SEC filing HTML",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "domain_whois_search",
      description: "Resolve domain ownership and registration metadata via RDAP",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "wayback_domain_timeline_search",
      description: "Fetch Wayback snapshot timeline metadata for a domain",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "historical_bio_diff",
      description: "Compare earliest and latest archived bio text for structured changes",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "sanctions_watchlist_search",
      description: "Check exact-name matches against public sanctions watchlists",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "alias_variant_generator",
      description: "Generate deterministic alias and username variants from a person name",
      scriptPath: wrapperPath,
      timeoutMs: 60000,
    },
    {
      name: "username_permutation_search",
      description: "Check direct public profile URL permutations across core platforms",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "cross_platform_profile_resolver",
      description: "Resolve cross-platform identity matches using deterministic profile features",
      scriptPath: wrapperPath,
      timeoutMs: 60000,
    },
    {
      name: "institution_directory_search",
      description: "Search a known institution domain for a direct directory/profile result",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "email_pattern_inference",
      description: "Infer likely public email address patterns for a domain and person name",
      scriptPath: wrapperPath,
      timeoutMs: 60000,
    },
    {
      name: "contact_page_extractor",
      description: "Fetch common contact/about/team pages and extract public contact signals",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "reddit_user_search",
      description: "Resolve a public Reddit profile via the public about endpoint",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "mastodon_profile_search",
      description: "Resolve a public Mastodon account via instance lookup",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "substack_author_search",
      description: "Resolve a public Substack author/publication page and extract linkage signals",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "medium_author_search",
      description: "Resolve a public Medium author page and extract article/linkage signals",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "coauthor_graph_search",
      description: "Build compact coauthor and venue overlap signals from publication data",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "org_staff_page_search",
      description: "Fetch common org staff/team pages and extract structured staff entries",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "board_member_overlap_search",
      description: "Compare officer/director lists to find shared board member overlaps",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "shared_contact_pivot_search",
      description: "Compare public emails, organizations, and addresses for shared contact pivots",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "orcid_search",
      description: "Search ORCID public records for an academic identity",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "semantic_scholar_search",
      description: "Search Semantic Scholar author profiles",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "dblp_author_search",
      description: "Search DBLP author records and optionally fetch publication lists",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "pubmed_author_search",
      description: "Search PubMed author records and summarize publication matches",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    {
      name: "grant_search_person",
      description: "Aggregate NIH and NSF grant results for a person",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    // Temporarily disabled until PatentSearch API integration is implemented.
    // {
    //   name: "patent_search_person",
    //   description: "Search PatentsView inventor records for a person",
    //   scriptPath: wrapperPath,
    //   timeoutMs: 120000,
    // },
    {
      name: "conference_profile_search",
      description: "Aggregate DBLP-based conference publication appearances for a person",
      scriptPath: wrapperPath,
      timeoutMs: 120000,
    },
    // Temporarily disabled until non-stub implementations exist.
    // {
    //   name: "google_scholar_profile_search",
    //   description: "Stub for Google Scholar profile search behind a feature flag",
    //   scriptPath: wrapperPath,
    //   timeoutMs: 60000,
    // },
    // {
    //   name: "researchgate_profile_search",
    //   description: "Stub for ResearchGate profile search behind a feature flag",
    //   scriptPath: wrapperPath,
    //   timeoutMs: 60000,
    // },
    // {
    //   name: "ssrn_author_search",
    //   description: "Stub for SSRN author search behind a feature flag",
    //   scriptPath: wrapperPath,
    //   timeoutMs: 60000,
    // },
  ];
}

function isKaliPythonRuntime(): boolean {
  const pythonBin = (cfg.python.bin || "").toLowerCase();
  return pythonBin.includes("/opt/osint-venv/");
}

function mergeToolConfigs(toolGroups: PythonToolConfig[][]): PythonToolConfig[] {
  const merged = new Map<string, PythonToolConfig>();
  for (const group of toolGroups) {
    for (const tool of group) {
      if (!merged.has(tool.name)) {
        merged.set(tool.name, tool);
      }
    }
  }
  return Array.from(merged.values());
}

function parseToolsJsonConfig(): PythonToolConfig[] {
  if (!cfg.python.toolsJson) return [];
  try {
    const parsed = JSON.parse(cfg.python.toolsJson) as unknown;
    return configSchema.parse(parsed);
  } catch (error) {
    console.error("Invalid MCP_PYTHON_TOOLS JSON:", error);
    return [];
  }
}

function parsePythonToolConfig(): PythonToolConfig[] {
  const explicitToolsets = (cfg.python.toolset || "")
    .split(",")
    .map((item) => item.trim().toLowerCase())
    .filter((item) => item.length > 0 && item !== "default");

  const toolGroups: PythonToolConfig[][] = [];
  if (explicitToolsets.length > 0) {
    for (const toolset of explicitToolsets) {
      if (toolset === "kali-osint") {
        toolGroups.push(kaliOsintToolPreset());
      } else if (toolset === "research-integration") {
        toolGroups.push(researchIntegrationToolPreset());
      }
    }
  } else {
    // Default behavior: always expose research integration tools.
    toolGroups.push(researchIntegrationToolPreset());

    // Kali runtime auto-enables its OSINT preset.
    if (isKaliPythonRuntime()) {
      toolGroups.push(kaliOsintToolPreset());
    }
  }
  toolGroups.push(parseToolsJsonConfig());
  return mergeToolConfigs(toolGroups);
}

function resolveToolPath(scriptPath: string): string {
  const resolved = resolve(cfg.paths.repoRoot, scriptPath);
  const rel = relative(cfg.paths.repoRoot, resolved);
  if (rel.startsWith("..")) {
    throw new Error(`Python tool path must be inside repo: ${scriptPath}`);
  }
  return resolved;
}

async function storePythonResult(runId: string, toolName: string, payload: unknown): Promise<StoredResult> {
  const bytes = Buffer.from(JSON.stringify(payload));
  const sha256 = crypto.createHash("sha256").update(bytes).digest("hex");
  const objectKey = `runs/${runId}/raw/python/${toolName}/${sha256}.json`;
  const contentType = "application/json";

  await ensureBucket(cfg.minio.bucket);

  const putRes = await minio.putObject(
    cfg.minio.bucket,
    objectKey,
    bytes,
    bytes.length,
    { "Content-Type": contentType }
  );

  const etag = (putRes as any).etag ?? null;
  const versionId = (putRes as any).versionId ?? null;

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
      ) VALUES ($1, $2, $3, $4, 'json', $5, $6, 3, 'parsed', $7)`,
      [documentId, runId, null, null, contentType, sha256, `python:${toolName}`]
    );
  }

  await pool.query(
    `INSERT INTO document_objects(
      object_id, document_id, kind, bucket, object_key, version_id, etag, size_bytes, content_type
    ) VALUES ($1, $2, 'raw', $3, $4, $5, $6, $7, $8)
    ON CONFLICT (document_id, kind) DO NOTHING`,
    [uuidv4(), documentId, cfg.minio.bucket, objectKey, versionId, etag, bytes.length, contentType]
  );

  return {
    documentId,
    bucket: cfg.minio.bucket,
    objectKey,
    versionId,
    etag,
    sizeBytes: bytes.length,
    contentType,
    sha256,
  };
}

function contentTypeForPath(filePath: string): string {
  const ext = extname(filePath).toLowerCase();
  if (ext === ".html" || ext === ".htm") return "text/html";
  if (ext === ".json") return "application/json";
  if (ext === ".pdf") return "application/pdf";
  if (ext === ".md") return "text/markdown";
  if (ext === ".txt" || ext === ".log") return "text/plain";
  return "application/octet-stream";
}

function safeName(value: string): string {
  return value.replace(/[^a-zA-Z0-9._-]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 120) || "artifact";
}

function asAbsolutePath(filePath: string): string {
  if (isAbsolute(filePath)) return filePath;
  return resolve(cfg.paths.repoRoot, filePath);
}

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === "string" && item.trim().length > 0);
}

function asObjectArray(value: unknown): Record<string, unknown>[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is Record<string, unknown> => typeof item === "object" && item !== null && !Array.isArray(item));
}

type PreparedPythonInput = {
  input: Record<string, unknown>;
  cleanup?: () => Promise<void>;
};

async function downloadMinioObjectToPath(
  bucket: string,
  objectKey: string,
  destinationPath: string,
  versionId?: string
): Promise<void> {
  const getOpts = versionId ? ({ versionId } as { versionId: string }) : undefined;
  const sourceStream = await minio.getObject(bucket, objectKey, getOpts);
  const destinationStream = createWriteStream(destinationPath);
  await pipeline(sourceStream, destinationStream);
}

async function prepareExiftoolInput(input: Record<string, unknown>): Promise<PreparedPythonInput> {
  const pathValue = typeof input.path === "string" ? input.path.trim() : "";
  if (pathValue) {
    return { input };
  }

  const objectKey = typeof input.objectKey === "string" ? input.objectKey.trim() : "";
  if (!objectKey) {
    return { input };
  }

  const bucket = typeof input.bucket === "string" && input.bucket.trim().length > 0
    ? input.bucket.trim()
    : cfg.minio.bucket;
  const versionId = typeof input.versionId === "string" && input.versionId.trim().length > 0
    ? input.versionId.trim()
    : undefined;
  const filenameHint = typeof input.filename === "string" && input.filename.trim().length > 0
    ? input.filename.trim()
    : basename(objectKey);
  const fileName = safeName(filenameHint) || "input.bin";
  const tempDir = await mkdtemp(join(tmpdir(), "mcp-exiftool-"));
  const tempPath = resolve(tempDir, fileName);

  await downloadMinioObjectToPath(bucket, objectKey, tempPath, versionId);

  return {
    input: {
      ...input,
      path: tempPath,
    },
    cleanup: async () => {
      await rm(tempDir, { recursive: true, force: true });
    },
  };
}

async function preparePythonToolInput(toolName: string, input: Record<string, unknown>): Promise<PreparedPythonInput> {
  if (toolName === "osint_exiftool_extract") {
    return prepareExiftoolInput(input);
  }
  return { input };
}

function addResearchArtifact(
  artifacts: Map<string, ResearchArtifactDescriptor>,
  descriptor: ResearchArtifactDescriptor
): void {
  const sourcePath = descriptor.sourcePath ? asAbsolutePath(descriptor.sourcePath) : "";
  if (!sourcePath) return;

  const existing = artifacts.get(sourcePath);
  if (!existing) {
    artifacts.set(sourcePath, { ...descriptor, sourcePath });
    return;
  }

  artifacts.set(sourcePath, {
    ...existing,
    sourcePath,
    sourceUrl: descriptor.sourceUrl ?? existing.sourceUrl,
    title: descriptor.title ?? existing.title,
    sourceType: descriptor.sourceType ?? existing.sourceType,
    contentType: descriptor.contentType ?? existing.contentType,
  });
}

function inferSourceType(sourcePath: string, contentType?: string | null): string {
  if (contentType?.startsWith("text/html")) return "html";
  if (contentType?.startsWith("application/pdf")) return "pdf";
  if (contentType?.startsWith("image/")) return "image";
  if (contentType === "application/json") return "json";
  const ext = extname(sourcePath).toLowerCase();
  if (ext === ".html" || ext === ".htm") return "html";
  if (ext === ".pdf") return "pdf";
  if (ext === ".json") return "json";
  if ([".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"].includes(ext)) return "image";
  return "text";
}

function inferResearchArtifacts(toolName: string, output: unknown): ResearchArtifactDescriptor[] {
  if (!output || typeof output !== "object" || Array.isArray(output)) {
    return [];
  }

  const payload = output as Record<string, unknown>;
  const artifacts = new Map<string, ResearchArtifactDescriptor>();
  for (const rawFile of asStringArray(payload.raw_files)) {
    addResearchArtifact(artifacts, { sourcePath: rawFile });
  }

  if (toolName === "x_get_user_posts_api") {
    const outputPath = payload.output_path;
    if (typeof outputPath === "string" && outputPath.trim()) {
      addResearchArtifact(artifacts, { sourcePath: outputPath });
    }
  }

  if (toolName === "linkedin_download_html_ocr") {
    for (const htmlPath of asStringArray(payload.html_files)) {
      addResearchArtifact(artifacts, { sourcePath: htmlPath, sourceType: "html", contentType: "text/html" });
    }
  }

  if (
    toolName === "google_serp_person_search"
    || toolName === "tavily_person_search"
    || toolName === "tavily_research"
    || toolName === "web_search"
    || toolName === "extract_webpage"
    || toolName === "crawl_webpage"
    || toolName === "map_webpage"
  ) {
    for (const key of [
      "summary_path",
      "api_response_path",
      "index_path",
      "create_response_path",
      "status_response_path",
      "report_path",
      "page_manifest_path",
    ]) {
      const value = payload[key];
      if (typeof value === "string" && value.trim()) {
        addResearchArtifact(artifacts, { sourcePath: value });
      }
    }

    const summary = payload.summary;
    if (summary && typeof summary === "object" && !Array.isArray(summary)) {
      const results = (summary as Record<string, unknown>).results;
      if (Array.isArray(results)) {
        const outputDir = typeof payload.output_dir === "string" ? payload.output_dir : "";
        for (const result of results) {
          if (!result || typeof result !== "object" || Array.isArray(result)) continue;
          const localFile = (result as Record<string, unknown>).local_file;
          if (typeof localFile !== "string" || !localFile.trim()) continue;
          const resolvedPath = outputDir ? resolve(asAbsolutePath(outputDir), localFile) : asAbsolutePath(localFile);
          addResearchArtifact(artifacts, {
            sourcePath: resolvedPath,
            sourceUrl: typeof (result as Record<string, unknown>).url === "string" ? (result as Record<string, unknown>).url as string : null,
            title: typeof (result as Record<string, unknown>).title === "string" ? (result as Record<string, unknown>).title as string : null,
            sourceType: "html",
            contentType: "text/html",
          });
        }
      }
    }
  }

  if (toolName === "arxiv_search_and_download") {
    const metadataPath = payload.metadata_path;
    if (typeof metadataPath === "string" && metadataPath.trim()) {
      addResearchArtifact(artifacts, { sourcePath: metadataPath });
    }
    const metadata = payload.metadata;
    if (metadata && typeof metadata === "object" && !Array.isArray(metadata)) {
      const entries = (metadata as Record<string, unknown>).entries;
      if (Array.isArray(entries)) {
        for (const entry of entries) {
          if (!entry || typeof entry !== "object" || Array.isArray(entry)) continue;
          const pdfFile = (entry as Record<string, unknown>).pdf_file;
          if (typeof pdfFile === "string" && pdfFile.trim()) {
            addResearchArtifact(artifacts, { sourcePath: pdfFile, sourceType: "pdf", contentType: "application/pdf" });
          }
        }
      }
    }
  }

  if (toolName === "arxiv_paper_ingest") {
    const metadataPath = payload.metadata_path;
    if (typeof metadataPath === "string" && metadataPath.trim()) {
      addResearchArtifact(artifacts, { sourcePath: metadataPath });
    }
    const paperTextPath = payload.paper_text_path;
    if (typeof paperTextPath === "string" && paperTextPath.trim()) {
      addResearchArtifact(artifacts, {
        sourcePath: paperTextPath,
        sourceType: "text",
        contentType: "text/plain",
      });
    }
    const paper = payload.paper;
    if (paper && typeof paper === "object" && !Array.isArray(paper)) {
      const paperRecord = paper as Record<string, unknown>;
      const pdfFile = paperRecord.pdf_file;
      if (typeof pdfFile === "string" && pdfFile.trim()) {
        addResearchArtifact(artifacts, {
          sourcePath: pdfFile,
          sourceUrl: typeof paperRecord.pdf_url === "string" ? paperRecord.pdf_url : null,
          title: typeof paperRecord.title === "string" ? paperRecord.title : null,
          sourceType: "pdf",
          contentType: "application/pdf",
        });
      }
      const extractedTextFile = paperRecord.paper_text_path;
      if (typeof extractedTextFile === "string" && extractedTextFile.trim()) {
        addResearchArtifact(artifacts, {
          sourcePath: extractedTextFile,
          sourceUrl: typeof paperRecord.abs_url === "string"
            ? paperRecord.abs_url
            : (typeof paperRecord.id_url === "string" ? paperRecord.id_url : null),
          title: typeof paperRecord.title === "string" ? `${paperRecord.title} extracted text` : null,
          sourceType: "text",
          contentType: "text/plain",
        });
      }
    }
  }

  if (toolName === "person_search") {
    for (const result of asObjectArray(payload.results)) {
      const htmlPath = result.html_path;
      if (typeof htmlPath === "string" && htmlPath.trim()) {
        addResearchArtifact(artifacts, {
          sourcePath: htmlPath,
          sourceUrl: typeof result.url === "string" ? result.url : null,
          title: typeof result.title === "string" ? result.title : null,
          sourceType: "html",
          contentType: "text/html",
        });
      }
    }
  }

  if (toolName === "extract_webpage" || toolName === "crawl_webpage") {
    for (const page of asObjectArray(payload.page_files)) {
      const filePath = page.path;
      if (typeof filePath === "string" && filePath.trim()) {
        addResearchArtifact(artifacts, {
          sourcePath: filePath,
          sourceUrl: typeof page.url === "string" ? page.url : null,
          title: typeof page.title === "string" ? page.title : null,
          sourceType: "text",
          contentType: typeof page.content_type === "string" ? page.content_type : null,
        });
      }
    }
  }

  return Array.from(artifacts.values());
}

async function upsertArtifactDocument(
  runId: string,
  descriptor: ResearchArtifactDescriptor,
  bytes: Buffer,
  sha256: string,
  contentType: string,
  objectKey: string,
  versionId: string | null,
  etag: string | null
): Promise<string> {
  const sourceUrl = descriptor.sourceUrl?.trim() ? descriptor.sourceUrl.trim() : null;
  const title = descriptor.title?.trim() ? descriptor.title.trim() : null;
  const sourceType = descriptor.sourceType?.trim() ? descriptor.sourceType.trim() : inferSourceType(descriptor.sourcePath, contentType);

  const existing = await pool.query(
    "SELECT document_id FROM documents WHERE run_id = $1 AND sha256 = $2",
    [runId, sha256]
  );
  const documentId = existing.rows[0]?.document_id ?? uuidv4();

  if (!existing.rows[0]?.document_id) {
    await pool.query(
      `INSERT INTO documents(
        document_id, run_id, source_url, source_domain, source_type,
        content_type, sha256, trust_tier, extraction_state, title
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, 3, 'parsed', $8)`,
      [
        documentId,
        runId,
        sourceUrl,
        sourceUrl ? new URL(sourceUrl).hostname : null,
        sourceType,
        contentType,
        sha256,
        title,
      ]
    );
  }

  await pool.query(
    `INSERT INTO document_objects(
      object_id, document_id, kind, bucket, object_key, version_id, etag, size_bytes, content_type
    ) VALUES ($1, $2, 'raw', $3, $4, $5, $6, $7, $8)
    ON CONFLICT (document_id, kind) DO NOTHING`,
    [uuidv4(), documentId, cfg.minio.bucket, objectKey, versionId, etag, bytes.length, contentType]
  );

  return documentId;
}

async function storePythonFileArtifact(runId: string, toolName: string, descriptor: ResearchArtifactDescriptor): Promise<StoredFileArtifact> {
  const sourcePath = asAbsolutePath(descriptor.sourcePath);
  const bytes = await readFile(sourcePath);
  const sha256 = crypto.createHash("sha256").update(bytes).digest("hex");
  const name = safeName(basename(sourcePath));
  const objectKey = `runs/${runId}/raw/python/${toolName}/artifacts/${sha256}_${name}`;
  const contentType = descriptor.contentType ?? contentTypeForPath(sourcePath);

  await ensureBucket(cfg.minio.bucket);
  const putRes = await minio.putObject(
    cfg.minio.bucket,
    objectKey,
    bytes,
    bytes.length,
    { "Content-Type": contentType }
  );

  const etag = (putRes as any).etag ?? null;
  const versionId = (putRes as any).versionId ?? null;
  const documentId = await upsertArtifactDocument(runId, descriptor, bytes, sha256, contentType, objectKey, versionId, etag);

  return {
    documentId,
    sourcePath,
    sourceUrl: descriptor.sourceUrl?.trim() ? descriptor.sourceUrl.trim() : null,
    title: descriptor.title?.trim() ? descriptor.title.trim() : null,
    bucket: cfg.minio.bucket,
    objectKey,
    versionId,
    etag,
    sizeBytes: bytes.length,
    contentType,
    sha256,
  };
}

async function storeResearchArtifacts(runId: string, toolName: string, output: unknown): Promise<StoredFileArtifact[]> {
  const descriptors = inferResearchArtifacts(toolName, output);
  if (!descriptors.length) return [];

  const artifacts: StoredFileArtifact[] = [];
  for (const descriptor of descriptors) {
    try {
      const artifact = await storePythonFileArtifact(runId, toolName, descriptor);
      artifacts.push(artifact);
    } catch (error) {
      logger.warn("python artifact store skipped", {
        runId,
        tool: toolName,
        sourcePath: descriptor.sourcePath,
        error: (error as Error).message,
      });
    }
  }
  return artifacts;
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function trimText(value: unknown, maxLen = 700): string {
  if (typeof value !== "string") return "";
  const normalized = value.replace(/\s+/g, " ").trim();
  if (normalized.length <= maxLen) return normalized;
  return `${normalized.slice(0, Math.max(0, maxLen - 1))}…`;
}

function minimalEvidenceRef(stored: StoredResult): Record<string, unknown> {
  return {
    documentId: stored.documentId,
    bucket: stored.bucket,
    objectKey: stored.objectKey,
  };
}

function compactArtifactDocuments(rawArtifacts: StoredFileArtifact[]): Record<string, unknown>[] {
  return rawArtifacts.map((artifact) => ({
    documentId: artifact.documentId,
    title: artifact.title,
    sourceUrl: artifact.sourceUrl,
    bucket: artifact.bucket,
    objectKey: artifact.objectKey,
    versionId: artifact.versionId,
    etag: artifact.etag,
    sizeBytes: artifact.sizeBytes,
    contentType: artifact.contentType,
    sha256: artifact.sha256,
  }));
}

function resolveResponseMode(input: Record<string, unknown>): ResponseMode {
  const requestedMode = typeof input.response_mode === "string" ? input.response_mode.trim().toLowerCase() : "";
  if (requestedMode === "full" || requestedMode === "compact") {
    return requestedMode;
  }

  const fullResponse = input.include_full_response === true || input.full_response === true;
  return fullResponse ? "full" : "compact";
}

function compactGenericResult(payload: Record<string, unknown>): Record<string, unknown> {
  const droppedKeys = new Set([
    "output_dir",
    "output_path",
    "download_dir",
    "metadata_path",
    "summary_path",
    "api_response_path",
    "index_path",
    "html_files",
    "pdf_files",
    "raw_files",
    "stdout",
    "stderr",
    "note",
    "rawArtifacts",
    "evidence",
  ]);

  const compact: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(payload)) {
    if (droppedKeys.has(key)) continue;
    if (key === "summary" || key === "metadata") continue;
    compact[key] = value;
  }
  return compact;
}

function compactResearchOutput(toolName: string, output: unknown, stored: StoredResult): Record<string, unknown> {
  if (!isObjectRecord(output)) {
    return { result: output, evidence: minimalEvidenceRef(stored) };
  }

  if (typeof output.error === "string" && output.error.trim()) {
    return { error: output.error, evidence: minimalEvidenceRef(stored) };
  }

  const compactBase = compactGenericResult(output);

  if (
    toolName === "google_serp_person_search"
    || toolName === "tavily_person_search"
    || toolName === "tavily_research"
    || toolName === "web_search"
  ) {
    const compact: Record<string, unknown> = {};
    if (typeof output.target_name === "string") compact.target_name = output.target_name;
    if (typeof output.query === "string") compact.query = output.query;

    const extracted = Array.isArray(output.extracted_results) ? output.extracted_results : [];
    const normalized = extracted
      .filter(isObjectRecord)
      .slice(0, 10)
      .map((item) => ({
        rank: typeof item.rank === "number" ? item.rank : null,
        title: trimText(item.title, 220),
        url: typeof item.url === "string" ? item.url : "",
        extracted_text: trimText(item.extracted_text, 500),
      }));
    compact.extracted_results = normalized;
    compact.evidence = minimalEvidenceRef(stored);
    return compact;
  }

  if (toolName === "extract_webpage" || toolName === "crawl_webpage") {
    const compact: Record<string, unknown> = {};
    if (typeof output.url === "string") compact.url = output.url;
    if (Array.isArray(output.urls)) compact.urls = output.urls;
    if (typeof output.results_found === "number") compact.results_found = output.results_found;
    if (typeof output.failed_results_count === "number") compact.failed_results_count = output.failed_results_count;

    const extracted = Array.isArray(output.extracted_pages) ? output.extracted_pages : [];
    compact.extracted_pages = extracted
      .filter(isObjectRecord)
      .slice(0, 15)
      .map((item) => ({
        title: trimText(item.title, 220),
        url: typeof item.url === "string" ? item.url : "",
        extracted_text: trimText(item.extracted_text || item.raw_content, 700),
      }));
    compact.evidence = minimalEvidenceRef(stored);
    return compact;
  }

  if (toolName === "map_webpage") {
    const compact: Record<string, unknown> = {};
    if (typeof output.url === "string") compact.url = output.url;
    if (typeof output.results_found === "number") compact.results_found = output.results_found;
    compact.urls = Array.isArray(output.urls)
      ? output.urls.filter((value): value is string => typeof value === "string").slice(0, 25)
      : [];
    compact.evidence = minimalEvidenceRef(stored);
    return compact;
  }

  if (toolName === "arxiv_search_and_download") {
    const compact: Record<string, unknown> = {};
    const metadata = isObjectRecord(output.metadata) ? output.metadata : {};

    if (typeof metadata.search_query === "string") compact.search_query = metadata.search_query;
    if (typeof metadata.author === "string") compact.author = metadata.author;
    if (typeof metadata.topic === "string") compact.topic = metadata.topic;
    if (typeof metadata.retrieved_results === "number") compact.retrieved_results = metadata.retrieved_results;
    if (typeof metadata.total_available === "number") compact.total_available = metadata.total_available;

    const extracted = Array.isArray(output.extracted_entries) ? output.extracted_entries : [];
    compact.extracted_entries = extracted
      .filter(isObjectRecord)
      .slice(0, 10)
      .map((item) => ({
        arxiv_id: typeof item.arxiv_id === "string" ? item.arxiv_id : "",
        title: trimText(item.title, 260),
        published: typeof item.published === "string" ? item.published : "",
        authors: Array.isArray(item.authors)
          ? item.authors.filter((value): value is string => typeof value === "string").slice(0, 12)
          : typeof item.authors === "string"
            ? item.authors
            : [],
        affiliations: Array.isArray(item.affiliations)
          ? item.affiliations.filter((value): value is string => typeof value === "string").slice(0, 12)
          : typeof item.affiliations === "string"
            ? item.affiliations
            : [],
        pdf_url: typeof item.pdf_url === "string" ? item.pdf_url : "",
        extracted_text: trimText(item.extracted_text, 700),
      }));
    compact.evidence = minimalEvidenceRef(stored);
    return compact;
  }

  if (toolName === "arxiv_paper_ingest") {
    const compact: Record<string, unknown> = {};
    const paper = isObjectRecord(output.paper) ? output.paper : {};

    if (typeof paper.arxiv_id === "string") compact.arxiv_id = paper.arxiv_id;
    if (typeof paper.title === "string") compact.title = trimText(paper.title, 260);
    if (typeof paper.published === "string") compact.published = paper.published;
    if (typeof paper.pdf_url === "string") compact.pdf_url = paper.pdf_url;
    if (Array.isArray(output.topics)) {
      compact.topics = output.topics.filter((value): value is string => typeof value === "string").slice(0, 12);
    }
    if (Array.isArray(output.emails)) {
      compact.emails = output.emails.filter((value): value is string => typeof value === "string").slice(0, 12);
    }

    const authorContacts = Array.isArray(output.author_contacts) ? output.author_contacts : [];
    compact.author_contacts = authorContacts
      .filter(isObjectRecord)
      .slice(0, 12)
      .map((item) => ({
        name: trimText(item.name, 140),
        email: typeof item.email === "string" ? item.email : null,
        match_confidence: typeof item.match_confidence === "number" ? item.match_confidence : null,
      }));

    const coauthors = Array.isArray(output.coauthors) ? output.coauthors : [];
    compact.coauthors = coauthors
      .filter(isObjectRecord)
      .slice(0, 12)
      .map((item) => ({
        name: trimText(item.name, 140),
        email: typeof item.email === "string" ? item.email : null,
        match_confidence: typeof item.match_confidence === "number" ? item.match_confidence : null,
      }));

    const extractedEntry = {
      arxiv_id: typeof paper.arxiv_id === "string" ? paper.arxiv_id : "",
      title: trimText(paper.title, 260),
      published: typeof paper.published === "string" ? paper.published : "",
      authors: Array.isArray(paper.authors)
        ? paper.authors.filter((value): value is string => typeof value === "string").slice(0, 16)
        : [],
      affiliations: Array.isArray(paper.affiliations)
        ? paper.affiliations.filter((value): value is string => typeof value === "string").slice(0, 12)
        : [],
      pdf_url: typeof paper.pdf_url === "string" ? paper.pdf_url : "",
      topics: Array.isArray(paper.topics)
        ? paper.topics.filter((value): value is string => typeof value === "string").slice(0, 12)
        : [],
      emails: Array.isArray(paper.emails)
        ? paper.emails.filter((value): value is string => typeof value === "string").slice(0, 12)
        : [],
      extracted_text: trimText(paper.summary || paper.text_excerpt, 700),
    };
    compact.paper = extractedEntry;
    compact.papers = [extractedEntry];
    compact.extracted_entries = [extractedEntry];
    compact.evidence = minimalEvidenceRef(stored);
    return compact;
  }

  if (toolName === "person_search") {
    const compact: Record<string, unknown> = {};
    if (typeof output.name === "string") compact.name = output.name;
    if (typeof output.count === "number") compact.count = output.count;

    const results = Array.isArray(output.results) ? output.results : [];
    compact.results = results
      .filter(isObjectRecord)
      .slice(0, 10)
      .map((item) => ({
        title: trimText(item.title, 220),
        url: typeof item.url === "string" ? item.url : "",
        extracted_text: trimText(item.extracted_text || item.snippet, 700),
        skipped: item.skipped === true,
        error: typeof item.error === "string" && item.error.trim() ? item.error : null,
      }));
    compact.evidence = minimalEvidenceRef(stored);
    return compact;
  }

  if (
    [
      "orcid_search",
      "semantic_scholar_search",
      "dblp_author_search",
      "pubmed_author_search",
      "grant_search_person",
      "patent_search_person",
      "conference_profile_search",
      "google_scholar_profile_search",
      "researchgate_profile_search",
      "ssrn_author_search",
    ].includes(toolName)
  ) {
    const compact: Record<string, unknown> = {
      tool: typeof output.tool === "string" ? output.tool : toolName,
      query: isObjectRecord(output.query) ? output.query : {},
      evidence: minimalEvidenceRef(stored),
    };
    if (typeof output.status === "string") compact.status = output.status;
    if (typeof output.message === "string") compact.message = output.message;

    const candidates = Array.isArray(output.candidates) ? output.candidates : [];
    compact.candidates = candidates
      .filter(isObjectRecord)
      .slice(0, 10)
      .map((item) => ({
        canonical_name: trimText(item.canonical_name, 140),
        source: typeof item.source === "string" ? item.source : "",
        source_id: typeof item.source_id === "string" ? item.source_id : "",
        confidence: typeof item.confidence === "number" ? item.confidence : null,
        affiliations: Array.isArray(item.affiliations)
          ? item.affiliations.filter((value): value is string => typeof value === "string").slice(0, 6)
          : [],
        topics: Array.isArray(item.topics)
          ? item.topics.filter((value): value is string => typeof value === "string").slice(0, 6)
          : [],
        external_ids: isObjectRecord(item.external_ids) ? item.external_ids : {},
        works_summary: isObjectRecord(item.works_summary) ? item.works_summary : {},
        evidence: Array.isArray(item.evidence)
          ? item.evidence.filter(isObjectRecord).slice(0, 3)
          : [],
      }));

    const records = Array.isArray(output.records) ? output.records : [];
    compact.records = records.filter(isObjectRecord).slice(0, 10);
    return compact;
  }

  if (toolName === "x_get_user_posts_api") {
    const compact: Record<string, unknown> = {};
    if (typeof output.username === "string") compact.username = output.username;

    const nestedResult = isObjectRecord(output.result) ? output.result : {};
    const user = isObjectRecord(nestedResult.user) ? nestedResult.user : {};
    compact.user = {
      id: typeof user.id === "string" ? user.id : "",
      name: trimText(user.name, 120),
      username: trimText(user.username, 120),
    };
    if (typeof nestedResult.tweet_count === "number") {
      compact.tweet_count = nestedResult.tweet_count;
    }

    const extracted = Array.isArray(output.extracted_posts) ? output.extracted_posts : [];
    compact.extracted_posts = extracted
      .filter(isObjectRecord)
      .slice(0, 20)
      .map((item) => ({
        id: typeof item.id === "string" ? item.id : "",
        created_at: typeof item.created_at === "string" ? item.created_at : "",
        extracted_text: trimText(item.extracted_text, 700),
      }));
    compact.evidence = minimalEvidenceRef(stored);
    return compact;
  }

  if (toolName === "linkedin_download_html_ocr") {
    const compact: Record<string, unknown> = {
      reset_session: output.reset_session === true,
      session_reset: output.session_reset === true,
      evidence: minimalEvidenceRef(stored),
    };
    return compact;
  }

  return {
    ...compactBase,
    evidence: minimalEvidenceRef(stored),
  };
}

export function registerPythonTools(server: McpServer) {
  const tools = parsePythonToolConfig();
  if (!tools.length) return;

  for (const tool of tools) {
    const resolvedPath = resolveToolPath(tool.scriptPath);

    server.registerTool(
      tool.name,
      {
        description: tool.description,
        inputSchema: z.object({
          runId: z.string().uuid(),
          response_mode: z.enum(["compact", "full"]).optional(),
          include_full_response: z.boolean().optional(),
          full_response: z.boolean().optional(),
        }).passthrough(),
      },
      async (input) => {
        const runId = input.runId as string;
        await emitRunEvent(runId, "TOOL_CALL_STARTED", { tool: tool.name });
        logger.info("python tool started", { runId, tool: tool.name });

        let preparedInputCleanup: (() => Promise<void>) | undefined;
        try {
          const preparedInput = await preparePythonToolInput(tool.name, input);
          preparedInputCleanup = preparedInput.cleanup;

          const result = await runPythonTool({
            pythonBin: cfg.python.bin,
            scriptPath: resolvedPath,
            toolName: tool.name,
            input: preparedInput.input,
            timeoutMs: tool.timeoutMs,
          });

          if (!result.ok) {
            const errorMessage = result.error ?? "Python tool failed";
            await logToolCall(runId, tool.name, input, { error: errorMessage }, "error", errorMessage);
            await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: tool.name, ok: false, error: errorMessage });
            logger.error("python tool failed", { runId, tool: tool.name, error: errorMessage });

            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify({ error: errorMessage }, null, 2),
                },
              ],
              isError: true,
            };
          }

          const output = result.result ?? {};
          const rawArtifacts = await storeResearchArtifacts(runId, tool.name, output);
          const stored = await storePythonResult(runId, tool.name, output);
          const artifactDocuments = compactArtifactDocuments(rawArtifacts);

          const responseMode = resolveResponseMode(input);

          let fullResponsePayload: Record<string, unknown>;
          if (output && typeof output === "object" && !Array.isArray(output)) {
            fullResponsePayload = { ...(output as Record<string, unknown>), evidence: stored };
          } else {
            fullResponsePayload = { result: output, evidence: stored };
          }
          if (rawArtifacts.length > 0) {
            fullResponsePayload.rawArtifacts = rawArtifacts;
            fullResponsePayload.artifactDocuments = artifactDocuments;
          }
          const responsePayload = responseMode === "full"
            ? fullResponsePayload
            : compactResearchOutput(tool.name, output, stored);
          if (artifactDocuments.length > 0) {
            responsePayload.artifactDocuments = artifactDocuments;
          }

          await logToolCall(runId, tool.name, input, responsePayload, "ok");
          await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: tool.name, ok: true });
          logger.info("python tool finished", { runId, tool: tool.name, documentId: stored.documentId });

          return {
            content: [
              {
                type: "text",
                text: JSON.stringify(responsePayload, null, 2),
              },
            ],
          };
        } catch (error) {
          const errorMsg = (error as Error).message;
          await logToolCall(runId, tool.name, input, { error: errorMsg }, "error", errorMsg);
          await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: tool.name, ok: false, error: errorMsg });
          logger.error("python tool exception", { runId, tool: tool.name, error: errorMsg });

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
          if (preparedInputCleanup) {
            await preparedInputCleanup().catch((cleanupError: unknown) => {
              logger.warn("python tool temp cleanup failed", {
                runId,
                tool: tool.name,
                error: (cleanupError as Error).message,
              });
            });
          }
        }
      }
    );
  }
}
