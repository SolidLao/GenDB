import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

/**
 * MQO Analyzer — adaptive agent that identifies shared work across queries.
 * Variants (selected by runMqoPhase2 based on batch size):
 *   - small-batch: single agent reads full batch + reasons holistically
 *   - large-batch (Stage 10): Surveyor → parallel Cluster Analyzers → Integrator
 */
export const config = {
  name: "MQO Analyzer",
  configKey: "mqo_analyzer_small",
  promptPath: resolve(__dirname, "small-batch.md"),
  largeBatchSurveyorPromptPath: resolve(__dirname, "large-batch-surveyor.md"),
  largeBatchClusterPromptPath: resolve(__dirname, "large-batch-cluster.md"),
  largeBatchIntegratorPromptPath: resolve(__dirname, "large-batch-integrator.md"),
  userPromptPath: resolve(__dirname, "user-prompt.md"),
  allowedTools: ["Read", "Write", "Edit", "Glob", "Grep", "Bash"],
};
