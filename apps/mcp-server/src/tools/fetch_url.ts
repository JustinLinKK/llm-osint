import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import crypto from "node:crypto";
import { v4 as uuidv4 } from "uuid";
import { pool } from "../clients/pg.js";
import { minio, ensureBucket } from "../clients/minio.js";
import { cfg } from "../config.js";
import { emitRunEvent, logToolCall } from "./helpers.js";
import { logger } from "../utils/logger.js";

const MAX_EXTRACTED_LINKS = 200;
const RETRYABLE_STATUS_CODES = new Set([403, 408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524]);

type RequestProfile = {
  name: string;
  headers: Record<string, string>;
};

type HttpResult = {
  bytes: Buffer;
  contentType: string;
  finalUrl: string;
  statusCode: number;
  attempts: number;
  requestProfile: string;
};

class FetchRequestError extends Error {
  statusCode?: number;
  retryable: boolean;
  profile: string;

  constructor(message: string, options: { statusCode?: number; retryable?: boolean; profile: string }) {
    super(message);
    this.name = "FetchRequestError";
    this.statusCode = options.statusCode;
    this.retryable = options.retryable ?? false;
    this.profile = options.profile;
  }
}

const REQUEST_PROFILES: RequestProfile[] = [
  {
    name: "chrome-desktop",
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
      Accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,application/pdf;q=0.9,image/avif,image/webp,image/apng,image/svg+xml,image/*;q=0.8,*/*;q=0.7",
      "Accept-Language": "en-US,en;q=0.9",
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
    },
  },
  {
    name: "firefox-desktop",
    headers: {
      "User-Agent":
        "Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0",
      Accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,application/pdf;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.7",
      "Accept-Language": "en-US,en;q=0.8",
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
    },
  },
];

function normalizeHost(hostname: string): string {
  return hostname.toLowerCase().replace(/^www\./, "");
}

function decodeHtmlEntities(value: string): string {
  return value
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">");
}

function dedupeStrings(items: string[]): string[] {
  const seen = new Set<string>();
  const ordered: string[] = [];
  for (const item of items) {
    const normalized = item.trim();
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    ordered.push(normalized);
  }
  return ordered;
}

function extractHtmlHints(html: string, pageUrl: string): {
  title: string | null;
  links: string[];
  sameHostLinks: string[];
} {
  const titleMatch = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  const title = titleMatch ? decodeHtmlEntities(titleMatch[1].replace(/\s+/g, " ").trim()) : null;
  const sourceHost = normalizeHost(new URL(pageUrl).hostname);
  const linkMatches = html.matchAll(/<a\b[^>]*?\bhref\s*=\s*["']([^"'#]+)["']/gi);
  const links: string[] = [];

  for (const match of linkMatches) {
    const href = decodeHtmlEntities((match[1] ?? "").trim());
    if (!href || href.startsWith("javascript:") || href.startsWith("mailto:") || href.startsWith("tel:")) {
      continue;
    }

    try {
      const normalized = new URL(href, pageUrl);
      if (!["http:", "https:"].includes(normalized.protocol)) {
        continue;
      }
      normalized.hash = "";
      links.push(normalized.toString());
      if (links.length >= MAX_EXTRACTED_LINKS) {
        break;
      }
    } catch {
      continue;
    }
  }

  const dedupedLinks = dedupeStrings(links);
  const sameHostLinks = dedupedLinks.filter((candidate) => {
    try {
      return normalizeHost(new URL(candidate).hostname) === sourceHost;
    } catch {
      return false;
    }
  });

  return {
    title,
    links: dedupedLinks,
    sameHostLinks,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function looksLikePdf(bytes: Buffer): boolean {
  if (!bytes || bytes.length < 5) return false;
  return bytes.subarray(0, 5).toString("ascii") === "%PDF-";
}

function inferSourceType(contentType: string, bytes: Buffer): "html" | "pdf" | "image" | "text" {
  if (contentType.startsWith("text/html")) return "html";
  if (contentType.startsWith("application/pdf")) return "pdf";
  if (looksLikePdf(bytes)) return "pdf";
  if (contentType.startsWith("image/")) return "image";
  return "text";
}

function isTimeoutError(error: unknown): boolean {
  return error instanceof Error && (error.name === "TimeoutError" || error.name === "AbortError");
}

function getRetryDelayMs(attempt: number): number {
  return cfg.fetchUrl.retryDelayMs * Math.max(1, attempt);
}

async function makeSingleHttpRequest(url: string, profile: RequestProfile): Promise<HttpResult> {
  const response = await fetch(url, {
    headers: profile.headers,
    redirect: "follow",
    signal: AbortSignal.timeout(cfg.fetchUrl.timeoutMs),
  });
  if (!response.ok) {
    throw new FetchRequestError(`HTTP error ${response.status} ${response.statusText}`, {
      statusCode: response.status,
      retryable: RETRYABLE_STATUS_CODES.has(response.status),
      profile: profile.name,
    });
  }

  const bytes = Buffer.from(await response.arrayBuffer());
  const contentType = response.headers.get("content-type") ?? "application/octet-stream";
  const finalUrl = response.url || url;

  return {
    bytes,
    contentType,
    finalUrl,
    statusCode: response.status,
    attempts: 1,
    requestProfile: profile.name,
  };
}

async function makeHttpRequest(url: string): Promise<HttpResult> {
  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= cfg.fetchUrl.maxAttempts; attempt += 1) {
    const profile = REQUEST_PROFILES[(attempt - 1) % REQUEST_PROFILES.length]!;
    try {
      const result = await makeSingleHttpRequest(url, profile);
      return {
        ...result,
        attempts: attempt,
      };
    } catch (error) {
      const err =
        error instanceof FetchRequestError
          ? error
          : new FetchRequestError(
              isTimeoutError(error)
                ? `Request timed out after ${cfg.fetchUrl.timeoutMs}ms`
                : error instanceof Error
                ? error.message
                : "Unknown fetch error",
              {
                retryable: isTimeoutError(error) || error instanceof TypeError,
                profile: profile.name,
              }
            );
      lastError = err;

      logger.warn("fetch_url attempt failed", {
        url,
        attempt,
        maxAttempts: cfg.fetchUrl.maxAttempts,
        requestProfile: profile.name,
        statusCode: err.statusCode,
        retryable: err.retryable,
        error: err.message,
      });

      if (!err.retryable || attempt >= cfg.fetchUrl.maxAttempts) {
        break;
      }

      await sleep(getRetryDelayMs(attempt));
    }
  }

  throw lastError ?? new Error("Failed to fetch URL");
}

async function storeDocument(
  runId: string,
  url: string,
  bytes: Buffer,
  contentType: string,
  sourceType: "html" | "pdf" | "image" | "text"
): Promise<{ documentId: string; objectKey: string; etag: string | null; sourceType: string; sha256: string }> {
  const sha256 = crypto.createHash("sha256").update(bytes).digest("hex");

  await ensureBucket(cfg.minio.bucket);

  const objectKey = `runs/${runId}/raw/${sourceType}/${sha256}.${sourceType}`;

  const putRes = await minio.putObject(
    cfg.minio.bucket,
    objectKey,
    bytes,
    bytes.length,
    { "Content-Type": contentType }
  );

  const etag = (putRes as any).etag ?? null;
  const documentId = uuidv4();

  await pool.query("BEGIN");
  try {
    await pool.query(
      `INSERT INTO documents(
        document_id, run_id, source_url, source_domain, source_type,
        content_type, sha256, trust_tier, extraction_state
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, 3, 'pending')`,
      [
        documentId,
        runId,
        url,
        new URL(url).hostname,
        sourceType,
        contentType,
        sha256,
      ]
    );

    await pool.query(
      `INSERT INTO document_objects(
        object_id, document_id, kind, bucket, object_key, version_id, etag, size_bytes, content_type
      ) VALUES ($1, $2, 'raw', $3, $4, $5, $6, $7, $8)`,
      [uuidv4(), documentId, cfg.minio.bucket, objectKey, null, etag, bytes.length, contentType]
    );

    await pool.query("COMMIT");
  } catch (e) {
    await pool.query("ROLLBACK");
    throw e;
  }

  return { documentId, objectKey, etag, sourceType, sha256 };
}

export function registerFetchUrl(server: McpServer) {
  server.registerTool(
    "fetch_url",
    {
      description:
        "Fetch a public URL via HTTP GET. Use when you need raw source bytes for evidence. Stores raw content to MinIO and provenance to Postgres. Returns documentId, objectKey, contentType, sha256.",
      inputSchema: {
        runId: z.string().uuid().describe("Run ID (UUID)"),
        url: z.string().url().describe("URL to fetch"),
      },
    },
    async ({ runId, url }) => {
      await emitRunEvent(runId, "TOOL_CALL_STARTED", { tool: "fetch_url", url });
      logger.info("fetch_url started", { runId, url });

      try {
        const result = await makeHttpRequest(url);
        const { bytes, finalUrl, statusCode, attempts, requestProfile } = result;
        const sourceType = inferSourceType(result.contentType, bytes);
        const contentType = sourceType === "pdf" ? "application/pdf" : result.contentType;
        const pdfDetectedByMagic = sourceType === "pdf" && !result.contentType.startsWith("application/pdf");
        const htmlText = sourceType === "html" ? bytes.toString("utf-8") : null;
        const crawlHints = htmlText ? extractHtmlHints(htmlText, finalUrl) : {
          title: null,
          links: [],
          sameHostLinks: [],
        };

        const { documentId, objectKey, etag, sha256 } = await storeDocument(
          runId,
          finalUrl,
          bytes,
          contentType,
          sourceType
        );

        const output = {
          documentId,
          bucket: cfg.minio.bucket,
          objectKey,
          etag,
          versionId: null,
          sizeBytes: bytes.length,
          contentType,
          sourceType,
          pdfDetectedByMagic,
          url,
          finalUrl,
          statusCode,
          requestAttempts: attempts,
          requestProfile,
          timeoutMs: cfg.fetchUrl.timeoutMs,
          sha256,
          title: crawlHints.title,
          links: crawlHints.links,
          sameHostLinks: crawlHints.sameHostLinks,
          evidence: {
            documentId,
            bucket: cfg.minio.bucket,
            objectKey,
            versionId: null,
            etag,
            sizeBytes: bytes.length,
            contentType,
            sha256,
          },
        };

        await logToolCall(runId, "fetch_url", { url }, output, "ok");
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "fetch_url", url, ok: true, documentId });
        logger.info("fetch_url finished", { runId, documentId, bytes: bytes.length });

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

        await logToolCall(runId, "fetch_url", { url }, { error: errorMsg }, "error", errorMsg);
        await emitRunEvent(runId, "TOOL_CALL_FINISHED", { tool: "fetch_url", url, ok: false, error: errorMsg });
        logger.error("fetch_url failed", { runId, error: errorMsg });

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
