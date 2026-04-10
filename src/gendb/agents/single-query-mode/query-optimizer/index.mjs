import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export const config = {
  name: "Query Optimizer",
  configKey: "query_optimizer",
  promptPath: resolve(__dirname, "prompt.md"),
  allowedTools: ["Read", "Write", "Glob", "Grep", "Bash", "Skill"],
  domainSkillsPrompt: "## Domain Skills\nOptimization skills from prior runs are available. Each skill contains proven C++ patterns, performance evidence, and anti-patterns. To load a skill, call `Skill(name=\"<skill-name>\")`.\n\nReview the skill descriptions in your context. After analyzing profiling results, load any skill whose description matches the identified bottleneck, the query's characteristics (join types, aggregation pattern, data access pattern, column types), and hardware profile (cache hierarchy, core count, memory bandwidth). Only load skills that are relevant to the specific query, data, and hardware.",
  model: "sonnet",
};
