import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

export const config = {
  name: "Query Rewriter",
  promptPath: resolve(__dirname, "prompt.md"),
  allowedTools: ["Read", "Write", "Edit", "Glob", "Grep"],
  model: "sonnet",
};
