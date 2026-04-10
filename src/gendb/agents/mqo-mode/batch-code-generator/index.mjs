import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

/**
 * Batch Code Generator — emits the MQO artifact's shared components and the
 * mqo_main.cpp dispatcher. A single sequential LLM agent with a large output
 * surface (multi-file) — hence the longer timeout override in gendb.config.mjs.
 */
export const config = {
  name: "MQO Batch Code Generator",
  configKey: "mqo_batch_code_generator",
  promptPath: resolve(__dirname, "prompt.md"),
  userPromptPath: resolve(__dirname, "user-prompt.md"),
  allowedTools: ["Read", "Write", "Edit", "Glob", "Grep", "Bash"],
};
