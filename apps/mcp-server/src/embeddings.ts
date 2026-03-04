import { cfg } from "./config.js";

type EmbeddingsResponse = {
  data?: Array<{ embedding?: number[] }>;
};

function getEmbeddingHeaders() {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };

  const apiKey = getEmbeddingApiKey();
  if (apiKey) {
    headers.Authorization = `Bearer ${apiKey}`;
  }

  return headers;
}

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

export async function embedTexts(
  texts: string[],
  timeoutMs = cfg.embeddings.timeoutMs
): Promise<number[][]> {
  const response = await fetch(getEmbeddingApiUrl(), {
    method: "POST",
    signal: AbortSignal.timeout(timeoutMs),
    headers: getEmbeddingHeaders(),
    body: JSON.stringify({
      model: cfg.embeddings.model,
      input: texts,
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Embedding request failed: ${response.status} ${errorText}`);
  }

  const data = (await response.json()) as EmbeddingsResponse;
  const embeddings = (data.data ?? []).map((item) => item.embedding);
  if (embeddings.some((embedding) => !Array.isArray(embedding))) {
    throw new Error("Embedding response missing vector");
  }
  return embeddings as number[][];
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
