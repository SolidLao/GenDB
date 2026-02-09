import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

export const config = {
  name: "Orchestrator Agent",
  promptPath: resolve(__dirname, "prompt.md"),
  allowedTools: ["Read", "Write"],
  model: "sonnet",
};
