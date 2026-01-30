import { Client } from "minio";
import { cfg } from "../config.js";

export const minio = new Client({
  endPoint: cfg.minio.endpoint.split(":")[0],
  port: Number(cfg.minio.endpoint.split(":")[1] ?? (cfg.minio.useSSL ? 443 : 80)),
  useSSL: cfg.minio.useSSL,
  accessKey: cfg.minio.accessKey,
  secretKey: cfg.minio.secretKey
});

export async function ensureBucket(bucket: string) {
  const exists = await minio.bucketExists(bucket).catch(() => false);
  if (!exists) await minio.makeBucket(bucket);
}
