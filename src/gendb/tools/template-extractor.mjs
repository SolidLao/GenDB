/**
 * Node.js wrapper for SQL template extraction.
 *
 * Calls sql-parser.py with --extract-template to produce parameterized SQL
 * and a params schema from a concrete SQL query.
 */

import { execFileSync } from "child_process";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const PARSER_SCRIPT = resolve(__dirname, "sql-parser.py");

/**
 * Extract a parameterized template from a SQL query.
 *
 * @param {string} sql - The concrete SQL query
 * @returns {{ templateSql: string, params: Array<{name: string, type: string, default: any, description: string}> }}
 */
export function extractTemplate(sql) {
  try {
    const result = execFileSync("python3", [PARSER_SCRIPT, "--extract-template", "--sql", sql], {
      encoding: "utf-8",
      timeout: 15000,
      maxBuffer: 1024 * 1024,
    });
    const parsed = JSON.parse(result.trim());
    return {
      templateSql: parsed.template_sql,
      params: parsed.parameters || [],
    };
  } catch (err) {
    console.error(`[template-extractor] Failed to extract template: ${err.message}`);
    // Fallback: return original SQL with no params
    return { templateSql: sql, params: [] };
  }
}
