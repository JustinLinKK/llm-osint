import { proxyActivities } from "@temporalio/workflow";
import type { Activities } from "../activities.js";

const planOptions = {
  startToCloseTimeout: "5 minutes",
  retry: { initialInterval: "1s", maximumInterval: "1 minute", backoffCoefficient: 2, maximumAttempts: 3 }
} as const;

const collectOptions = {
  startToCloseTimeout: "10 minutes",
  retry: { initialInterval: "2s", maximumInterval: "2 minutes", backoffCoefficient: 2, maximumAttempts: 3 }
} as const;

const processOptions = {
  startToCloseTimeout: "20 minutes",
  retry: { initialInterval: "2s", maximumInterval: "3 minutes", backoffCoefficient: 2, maximumAttempts: 3 }
} as const;

const synthesizeOptions = {
  startToCloseTimeout: "10 minutes",
  retry: { initialInterval: "2s", maximumInterval: "2 minutes", backoffCoefficient: 2, maximumAttempts: 3 }
} as const;

const planActivities = proxyActivities<Activities>(planOptions);
const collectActivities = proxyActivities<Activities>(collectOptions);
const processActivities = proxyActivities<Activities>(processOptions);
const synthesizeActivities = proxyActivities<Activities>(synthesizeOptions);

export async function RunWorkflow(runId: string) {
  await planActivities.plan(runId);
  await collectActivities.collect(runId);
  await processActivities.process(runId);
  await synthesizeActivities.synthesize(runId);
}
