export const cfg = {
  databaseUrl:
    process.env.DATABASE_URL ??
    "postgresql://osint:osint@postgres:5432/osint",
  temporal: {
    address: process.env.TEMPORAL_ADDRESS ?? "temporal:7233",
    namespace: process.env.TEMPORAL_NAMESPACE ?? "default",
    taskQueue: process.env.TEMPORAL_TASK_QUEUE ?? "osint-run"
  }
};
