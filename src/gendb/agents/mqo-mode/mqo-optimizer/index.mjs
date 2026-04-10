import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

/**
 * MQO Optimizer — Strategy ε (profile-directed, scope-aware, single loop).
 *
 * Per iteration:
 *   1. Measure  (./mqo --all; optionally ./mqo --query Qi for per-query baselines)
 *   2. Diagnose (LLM reads profile.json and decides WHICH file to edit)
 *   3. Edit     (LLM patches shared/*.hpp, queries/qN.cpp, or mqo_main.cpp)
 *   4. Re-validate scoped to the edit
 *   5. Accept or rollback from snapshot
 */
export const config = {
  name: "MQO Optimizer",
  configKey: "mqo_optimizer",
  promptPath: resolve(__dirname, "prompt.md"),
  userPromptPath: resolve(__dirname, "user-prompt.md"),
  allowedTools: ["Read", "Write", "Edit", "Glob", "Grep", "Bash"],
};
