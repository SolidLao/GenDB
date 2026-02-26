import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

export const config = {
  name: "Storage/Index Designer",
  configKey: "storage_designer",
  promptPath: resolve(__dirname, "prompt.md"),
  allowedTools: ["Read", "Write", "Edit", "Glob", "Grep", "Bash", "Skill"],
  domainSkillsPrompt: "## Domain Skills\nDomain skills (gendb-storage-format, indexing, data loading, etc.) are available and will be loaded automatically when relevant. The experience skill contains critical correctness rules — always check it.",
  model: "sonnet",
};
