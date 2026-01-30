import { NativeConnection, Worker } from "@temporalio/worker";
import * as activities from "./activities.js";
import { cfg } from "./config.js";

async function main() {
  const connection = await NativeConnection.connect({
    address: cfg.temporal.address
  });

  const worker = await Worker.create({
    connection,
    namespace: cfg.temporal.namespace,
    taskQueue: cfg.temporal.taskQueue,
    workflowsPath: new URL("./workflows/runWorkflow.js", import.meta.url).pathname,
    activities
  });

  await worker.run();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
