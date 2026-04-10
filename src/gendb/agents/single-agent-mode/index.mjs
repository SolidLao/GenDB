import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

export const config = {
  name: "Single Agent",
  configKey: "single_agent",
  promptPaths: {
    "high-level": resolve(__dirname, "prompt-high-level.md"),
    "guided": resolve(__dirname, "prompt-guided.md"),
  },
  userPromptPath: resolve(__dirname, "user-prompt.md"),
  allowedTools: ["Read", "Write", "Edit", "Glob", "Grep", "Bash"],
};
