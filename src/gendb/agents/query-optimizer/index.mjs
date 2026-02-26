import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export const config = {
  name: "Query Optimizer",
  configKey: "query_optimizer",
  promptPath: resolve(__dirname, "prompt.md"),
  allowedTools: ["Read", "Write", "Glob", "Grep", "Bash", "Skill"],
  domainSkillsPrompt: "## Domain Skills\nDomain skills (gendb-code-patterns, hash tables, join optimization, scan optimization, parallelism, etc.) are available and will be loaded automatically when relevant. The experience skill contains critical correctness rules — always check it.",
  model: "sonnet",
};
