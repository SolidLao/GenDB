import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

export const config = {
  name: "Storage/Index Designer",
  promptPath: resolve(__dirname, "prompt.md"),
  allowedTools: ["Read", "Write", "Edit", "Glob", "Grep", "Bash"],
  model: "sonnet",
};
