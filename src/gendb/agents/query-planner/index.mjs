import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export const config = {
  name: "Query Planner",
  configKey: "query_planner",
  promptPath: resolve(__dirname, "prompt.md"),
  allowedTools: ["Read", "Write", "Glob", "Grep", "Bash", "Skill"],
  model: "sonnet",
};
