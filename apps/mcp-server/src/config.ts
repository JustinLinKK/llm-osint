import dotenv from "dotenv";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const repoRoot = resolve(__dirname, "../../..");
dotenv.config({ path: resolve(repoRoot, ".env") });

export const cfg = {
  paths: {
    repoRoot,
  },
  databaseUrl:
    process.env.DATABASE_URL ??
    "postgresql://osint:osint@postgres:5432/osint",

  qdrant: {
    url: process.env.QDRANT_URL ?? "http://qdrant:6333",
    collection: process.env.QDRANT_COLLECTION ?? "osint_chunks",
  },

  embeddings: {
    provider: process.env.EMBEDDING_PROVIDER ?? "openrouter",
    apiUrl: process.env.EMBEDDING_API_URL,
    apiKey: process.env.EMBEDDING_API_KEY,
    model:
      process.env.EMBEDDING_MODEL ??
      process.env.OPENROUTER_EMBED_MODEL ??
      "openai/text-embedding-3-small",
    timeoutMs: Math.max(1000, Number(process.env.EMBEDDING_TIMEOUT_MS ?? "180000")),
    queryTimeoutMs: Math.max(1000, Number(process.env.EMBEDDING_QUERY_TIMEOUT_MS ?? "60000")),
  },

  openrouter: {
    apiKey: process.env.OPENROUTER_API_KEY,
    embedModel: process.env.OPENROUTER_EMBED_MODEL ?? "openai/text-embedding-3-small",
  },

  location: {
    mergeThresholdMeters: Number(process.env.LOCATION_MERGE_THRESHOLD_METERS ?? "1000"),
  },

  neo4j: {
    uri: process.env.NEO4J_URI ?? "bolt://neo4j:7687",
    user: process.env.NEO4J_USER ?? "neo4j",
    password: process.env.NEO4J_PASSWORD ?? "neo4jpassword",
  },

  minio: {
    endpoint: (process.env.MINIO_ENDPOINT ?? "http://minio:9000").replace(/^https?:\/\//, ""),
    useSSL: (process.env.MINIO_ENDPOINT ?? "http://minio:9000").startsWith("https://"),
    accessKey: process.env.MINIO_ACCESS_KEY ?? "minio",
    secretKey: process.env.MINIO_SECRET_KEY ?? "minio12345",
    bucket: process.env.MINIO_BUCKET ?? "osint-raw"
  },

  fetchUrl: {
    timeoutMs: Math.max(1000, Number(process.env.FETCH_URL_TIMEOUT_MS ?? "15000")),
    maxAttempts: Math.max(1, Number(process.env.FETCH_URL_MAX_ATTEMPTS ?? "3")),
    retryDelayMs: Math.max(100, Number(process.env.FETCH_URL_RETRY_DELAY_MS ?? "750")),
  },

  python: {
    bin: process.env.PYTHON_BIN ?? "python3",
    toolsJson: process.env.MCP_PYTHON_TOOLS ?? "[]",
    toolset: process.env.MCP_TOOLSET ?? "default",
  }
};
