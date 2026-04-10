import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

/**
 * Global Skeleton Planner — produces the batch execution skeleton (DAG)
 * describing the order in which shared components run and how they fan out
 * to per-query tails. One sequential LLM agent in Phase 2 step 2.2.
 */
export const config = {
  name: "MQO Global Skeleton Planner",
  configKey: "mqo_skeleton_planner",
  promptPath: resolve(__dirname, "prompt.md"),
  userPromptPath: resolve(__dirname, "user-prompt.md"),
  allowedTools: ["Read", "Write", "Edit", "Glob", "Grep", "Bash"],
};
