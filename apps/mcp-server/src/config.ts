import dotenv from "dotenv";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
dotenv.config({ path: resolve(__dirname, "../../..", ".env") });

export const cfg = {
  databaseUrl:
    process.env.DATABASE_URL ??
    "postgresql://osint:osint@postgres:5432/osint",

  qdrant: {
    url: process.env.QDRANT_URL ?? "http://qdrant:6333",
    collection: process.env.QDRANT_COLLECTION ?? "osint_chunks",
  },

  openrouter: {
    apiKey: process.env.OPENROUTER_API_KEY,
    embedModel: process.env.OPENROUTER_EMBED_MODEL ?? "openai/text-embedding-3-small",
  },

  minio: {
    endpoint: (process.env.MINIO_ENDPOINT ?? "http://minio:9000").replace(/^https?:\/\//, ""),
    useSSL: (process.env.MINIO_ENDPOINT ?? "http://minio:9000").startsWith("https://"),
    accessKey: process.env.MINIO_ACCESS_KEY ?? "minio",
    secretKey: process.env.MINIO_SECRET_KEY ?? "minio12345",
    bucket: process.env.MINIO_BUCKET ?? "osint-raw"
  }
};
