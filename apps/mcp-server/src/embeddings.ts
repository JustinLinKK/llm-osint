import { cfg } from "./config.js";
import { logger } from "./utils/logger.js";

type EmbeddingsResponse = {
  data?: Array<{ embedding?: number[] }>;
};

type EmbeddingTarget = {
  apiUrl: string;
  apiKey?: string;
  model: string;
  providerLabel: string;
};

type FetchLikeError = Error & { cause?: { code?: string; message?: string } };

function getEmbeddingApiUrl() {
  if (cfg.embeddings.provider === "openrouter") {
    return cfg.embeddings.apiUrl ?? "https://openrouter.ai/api/v1/embeddings";
  }
  if (!cfg.embeddings.apiUrl) {
    throw new Error("EMBEDDING_API_URL not set for non-openrouter embedding provider");
  }
  return cfg.embeddings.apiUrl;
}

function getEmbeddingApiKey() {
  if (cfg.embeddings.provider === "openrouter") {
    return cfg.embeddings.apiKey ?? cfg.openrouter.apiKey;
  }
  return cfg.embeddings.apiKey;
}

function isRetryableStatus(status: number): boolean {
  return status === 408 || status === 409 || status === 425 || status === 429 || status >= 500;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function describeFetchError(error: unknown): string {
  const err = error as FetchLikeError;
  const parts = [err?.message ?? String(error)];
  const causeCode = err?.cause?.code;
  const causeMessage = err?.cause?.message;
  if (causeCode) parts.push(`cause=${causeCode}`);
  else if (causeMessage) parts.push(`cause=${causeMessage}`);
  return parts.join(" | ");
}

function providerHint(target: EmbeddingTarget): string {
  if (
    target.providerLabel === "vllm"
    && /worker-embedding/i.test(target.apiUrl)
  ) {
    return " Hint: worker-embedding may be down. Run `yarn infra:restart:embedding`.";
  }
  return "";
}

async function requestEmbeddings(
  texts: string[],
  timeoutMs: number,
  target: EmbeddingTarget
): Promise<number[][]> {
  const body = JSON.stringify({
    model: target.model,
    input: texts,
  });
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  if (target.apiKey) {
    headers.Authorization = `Bearer ${target.apiKey}`;
  }

  for (let attempt = 1; attempt <= cfg.embeddings.maxAttempts; attempt += 1) {
    let response: Response;
    try {
      response = await fetch(target.apiUrl, {
        method: "POST",
        signal: AbortSignal.timeout(timeoutMs),
        headers,
        body,
      });
    } catch (error) {
      const retryable = attempt < cfg.embeddings.maxAttempts;
      if (retryable) {
        await sleep(cfg.embeddings.retryDelayMs * attempt);
        continue;
      }
      throw new Error(
        `Embedding fetch failed (${target.providerLabel}) after ${attempt} attempts `
          + `to ${target.apiUrl}: ${describeFetchError(error)}.${providerHint(target)}`
      );
    }

    if (!response.ok) {
      const errorText = await response.text();
      const shouldRetry = isRetryableStatus(response.status) && attempt < cfg.embeddings.maxAttempts;
      if (shouldRetry) {
        await sleep(cfg.embeddings.retryDelayMs * attempt);
        continue;
      }
      throw new Error(
        `Embedding request failed (${target.providerLabel}) ${response.status} `
          + `from ${target.apiUrl}: ${errorText}`
      );
    }

    const data = (await response.json()) as EmbeddingsResponse;
    const embeddings = (data.data ?? []).map((item) => item.embedding);
    if (!embeddings.length) {
      throw new Error(`Embedding response empty from ${target.apiUrl}`);
    }
    if (embeddings.some((embedding) => !Array.isArray(embedding))) {
      throw new Error("Embedding response missing vector");
    }
    return embeddings as number[][];
  }

  throw new Error(`Embedding retries exhausted for ${target.apiUrl}`);
}

function buildPrimaryTarget(): EmbeddingTarget {
  return {
    apiUrl: getEmbeddingApiUrl(),
    apiKey: getEmbeddingApiKey(),
    model: cfg.embeddings.model,
    providerLabel: cfg.embeddings.provider,
  };
}

function buildOpenRouterFallbackTarget(): EmbeddingTarget | null {
  if (!cfg.embeddings.fallbackToOpenRouter) return null;
  if ((cfg.embeddings.provider ?? "").toLowerCase() === "openrouter") return null;
  if (!cfg.openrouter.apiKey) return null;

  return {
    apiUrl: "https://openrouter.ai/api/v1/embeddings",
    apiKey: cfg.openrouter.apiKey,
    model: cfg.openrouter.embedModel,
    providerLabel: "openrouter-fallback",
  };
}

export async function embedTexts(
  texts: string[],
  timeoutMs = cfg.embeddings.timeoutMs
): Promise<number[][]> {
  const primary = buildPrimaryTarget();
  try {
    return await requestEmbeddings(texts, timeoutMs, primary);
  } catch (primaryError) {
    const fallback = buildOpenRouterFallbackTarget();
    if (!fallback) {
      throw primaryError;
    }
    logger.warn("primary embedding provider failed; trying OpenRouter fallback", {
      primaryProvider: primary.providerLabel,
      primaryApiUrl: primary.apiUrl,
      fallbackModel: fallback.model,
      error: (primaryError as Error).message,
    });
    try {
      return await requestEmbeddings(texts, timeoutMs, fallback);
    } catch (fallbackError) {
      throw new Error(
        `Primary embedding failed: ${(primaryError as Error).message}; `
          + `fallback failed: ${(fallbackError as Error).message}`
      );
    }
  }
}

export async function embedQueryText(
  query: string,
  timeoutMs = cfg.embeddings.queryTimeoutMs
): Promise<number[]> {
  const embeddings = await embedTexts([query], timeoutMs);
  if (!embeddings[0]) {
    throw new Error("Embedding response missing vector");
  }
  return embeddings[0];
}
