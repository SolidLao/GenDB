/**
 * Programmatic guide append utilities for GenDB.
 * Appends new column version and index entries to query guide files
 * without requiring an LLM call.
 */

import { readFile, writeFile } from "fs/promises";
import { existsSync } from "fs";

/**
 * Append a column version entry to a query guide file.
 * @param {string} guidePath - Absolute path to guide.md
 * @param {object} entry - Column version registry entry
 *   { id, source: { table, column, v0_encoding }, encoding, row_count, unique_values, files }
 */
export async function appendColumnVersionToGuide(guidePath, entry) {
  if (!existsSync(guidePath)) return;

  let content = await readFile(guidePath, "utf-8");

  const section = [
    "",
    `## Column Version: ${entry.id}`,
    `- Source: ${entry.source.table}.${entry.source.column} (original encoding: ${entry.source.v0_encoding})`,
    `- Encoding: ${entry.encoding}, ${entry.row_count} rows, ${entry.unique_values} unique values`,
  ];

  if (entry.files) {
    for (const [key, path] of Object.entries(entry.files)) {
      if (!key.endsWith("_format")) {
        const fmt = entry.files[`${key}_format`] || "";
        section.push(`- File: \`${path}\`${fmt ? ` (${fmt})` : ""}`);
      }
    }
  }

  section.push("");
  content += section.join("\n");
  await writeFile(guidePath, content);
}

/**
 * Append an index entry to a query guide file.
 * @param {string} guidePath - Absolute path to guide.md
 * @param {object} entry - Index info
 *   { name, type, table, columns, file_path, description }
 */
export async function appendIndexToGuide(guidePath, entry) {
  if (!existsSync(guidePath)) return;

  let content = await readFile(guidePath, "utf-8");

  const section = [
    "",
    `## Index: ${entry.name}`,
    `- Type: ${entry.type}`,
    `- Table: ${entry.table}`,
    `- Columns: ${Array.isArray(entry.columns) ? entry.columns.join(", ") : entry.columns}`,
    `- File: \`${entry.file_path}\``,
  ];

  if (entry.description) {
    section.push(`- ${entry.description}`);
  }

  section.push("");
  content += section.join("\n");
  await writeFile(guidePath, content);
}
