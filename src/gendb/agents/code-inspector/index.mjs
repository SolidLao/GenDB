import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export const config = {
  name: "Code Inspector",
  configKey: "code_inspector",
  promptPath: resolve(__dirname, "prompt.md"),
  allowedTools: ["Read", "Glob", "Grep", "Skill"],
  domainSkillsPrompt: "## Domain Skills\nDomain skills (experience, hash tables, gendb-code-patterns, etc.) are available and will be loaded automatically when relevant. The experience skill contains critical correctness rules — always check it.",
  model: "sonnet",
};
