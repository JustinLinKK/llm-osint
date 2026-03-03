import { cfg } from "./config.js";

type QdrantCollectionResponse = {
  result?: {
    config?: {
      params?: {
        vectors?:
          | { size?: number | string }
          | Record<string, { size?: number | string }>;
      };
    };
  };
};

function parseVectorSize(data: QdrantCollectionResponse): number | null {
  const vectors = data.result?.config?.params?.vectors;
  if (!vectors || typeof vectors !== "object") return null;

  if ("size" in vectors) {
    const size = Number(vectors.size);
    return Number.isFinite(size) ? size : null;
  }

  for (const value of Object.values(vectors)) {
    const size = Number(value?.size);
    if (Number.isFinite(size)) return size;
  }

  return null;
}

async function createCollection(vectorSize: number) {
  const url = cfg.qdrant.url.replace(/\/$/, "");
  const collection = cfg.qdrant.collection;
  const createRes = await fetch(`${url}/collections/${collection}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      vectors: {
        size: vectorSize,
        distance: "Cosine",
      },
    }),
  });

  if (!createRes.ok) {
    const errorText = await createRes.text();
    throw new Error(`Qdrant create failed: ${createRes.status} ${errorText}`);
  }
}

export async function ensureQdrantCollection(vectorSize: number) {
  const url = cfg.qdrant.url.replace(/\/$/, "");
  const collection = cfg.qdrant.collection;

  const getRes = await fetch(`${url}/collections/${collection}`);
  if (getRes.status === 404) {
    await createCollection(vectorSize);
    return;
  }
  if (!getRes.ok) {
    throw new Error(`Qdrant check failed: ${getRes.status}`);
  }

  const data = (await getRes.json()) as QdrantCollectionResponse;
  const currentSize = parseVectorSize(data);
  if (currentSize === null || currentSize === vectorSize) {
    return;
  }

  const recreateRes = await fetch(`${url}/collections/${collection}`, {
    method: "DELETE",
  });
  if (!recreateRes.ok) {
    const errorText = await recreateRes.text();
    throw new Error(`Qdrant delete failed: ${recreateRes.status} ${errorText}`);
  }

  await createCollection(vectorSize);
}
