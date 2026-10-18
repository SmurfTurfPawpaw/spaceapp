import { sleep } from "@temporalio/workflow";
import { dotGoalWorkflow as production } from "../../src/workflows.js";

// Deliberately incompatible test-only change: never use this as production code.
export async function dotGoalWorkflow(input: { goalId: string }) {
  await sleep("1 second");
  return production(input);
}
