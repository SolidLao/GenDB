import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

export const config = {
  name: "Learner",
  promptPath: resolve(__dirname, "prompt.md"),
  allowedTools: ["Read", "Write", "Glob", "Grep"],
  model: "sonnet",
};
