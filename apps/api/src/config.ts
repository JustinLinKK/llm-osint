export const cfg = {
  apiPort: Number(process.env.API_PORT ?? 3000),

  databaseUrl:
    process.env.DATABASE_URL ??
    `postgresql://osint:osint@postgres:5432/osint`,

  minio: {
    endpoint: (process.env.MINIO_ENDPOINT ?? "http://minio:9000").replace(/^https?:\/\//, ""),
    useSSL: (process.env.MINIO_ENDPOINT ?? "http://minio:9000").startsWith("https://"),
    accessKey: process.env.MINIO_ACCESS_KEY ?? "minio",
    secretKey: process.env.MINIO_SECRET_KEY ?? "minio12345",
    bucket: process.env.MINIO_BUCKET ?? "osint-raw"
  }
};
