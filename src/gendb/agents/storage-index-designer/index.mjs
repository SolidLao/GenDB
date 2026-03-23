import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

export const config = {
  name: "Storage/Index Designer",
  configKey: "storage_designer",
  promptPath: resolve(__dirname, "prompt.md"),
  allowedTools: ["Read", "Write", "Edit", "Glob", "Grep", "Bash", "Skill"],
  domainSkillsPrompt: "## Domain Skills\nOptimization skills from prior runs are available. Each skill contains proven C++ patterns, performance evidence, and anti-patterns. To load a skill, call `Skill(name=\"<skill-name>\")`.\n\nReview the skill descriptions in your context. Before finalizing storage and index decisions, load any skill whose description matches the query patterns, data characteristics (column types, cardinalities), and hardware profile (cache hierarchy, core count, memory bandwidth). Only load skills that are relevant to the specific workload, data, and hardware.",
  model: "sonnet",
};
