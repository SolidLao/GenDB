/**
 * Node.js wrapper for the SQLGlot-based SQL structural feature extractor.
 *
 * Single source of truth for SQL feature extraction — used by:
 *   - retrieval.mjs (runtime query classification)
 *   - populate.mjs (L1 structural signature generation)
 *   - LLM agents (via CLI)
 */

import { execFileSync } from "child_process";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const PARSER_SCRIPT = resolve(__dirname, "sql-parser.py");

/**
 * Extract structural features from a SQL query using SQLGlot.
 *
 * @param {string} sql - The SQL query to parse
 * @returns {object} Structural features
 */
export function extractStructuralFeatures(sql) {
  try {
    const result = execFileSync("python3", [PARSER_SCRIPT, "--sql", sql], {
      encoding: "utf-8",
      timeout: 10000,
      maxBuffer: 1024 * 1024,
    });
    return JSON.parse(result.trim());
  } catch (err) {
    console.error(`[sql-parser] Failed to parse SQL: ${err.message}`);
    // Return empty features on failure (will score as novel)
    return {
      join_count: 0,
      join_types: [],
      has_aggregation: false,
      aggregate_functions: [],
      has_group_by: false,
      has_order_by: false,
      has_subquery: false,
      has_like: false,
      has_in: false,
      has_between: false,
      has_case: false,
      has_distinct: false,
      has_cte: false,
      has_window_function: false,
      has_union: false,
      has_limit: false,
      has_having: false,
      filter_types: [],
      tables: [],
    };
  }
}
