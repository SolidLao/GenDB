import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export const config = {
  name: "Query Planner",
  configKey: "query_planner",
  promptPath: resolve(__dirname, "prompt.md"),
  allowedTools: ["Read", "Write", "Glob", "Grep", "Bash", "Skill"],
  domainSkillsPrompt: "## Domain Skills\nDomain skills (join optimization, scan optimization, aggregation, hash tables, parallelism, research papers, etc.) are available and will be loaded automatically when relevant. The experience skill contains critical correctness rules — always check it.",
  model: "sonnet",
};
