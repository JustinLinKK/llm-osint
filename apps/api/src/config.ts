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
  },

  neo4j: {
    uri: process.env.NEO4J_URI ?? "bolt://neo4j:7687",
    user: process.env.NEO4J_USER ?? "neo4j",
    password: process.env.NEO4J_PASSWORD ?? "neo4jpassword"
  },

  langgraph: {
    autostart: (process.env.LANGGRAPH_AUTOSTART ?? "true").toLowerCase() !== "false",
    pythonBin: process.env.LANGGRAPH_PYTHON_BIN ?? process.env.PYTHON_BIN ?? "python3",
    scriptPath: process.env.LANGGRAPH_SCRIPT_PATH ?? "",
    maxIterations: Math.max(1, Number(process.env.LANGGRAPH_MAX_ITERATIONS ?? 1) || 1),
    workdir: process.env.LANGGRAPH_WORKDIR ?? process.cwd()
  }
};
