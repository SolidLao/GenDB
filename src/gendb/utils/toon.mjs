/**
 * TOON (Token-Oriented Object Notation) helpers for GenDB.
 * Replaces JSON for inter-agent artifact files to reduce token consumption.
 * Round-trip is lossless: decode(encode(x)) === x.
 */

import { encode, decode } from "@toon-format/toon";
import { readFile, writeFile } from "fs/promises";

/**
 * Read and parse a TOON file. Returns null on failure.
 * Also handles JSON files as fallback for backward compatibility.
 */
export async function readTOON(path) {
  try {
    const content = await readFile(path, "utf-8");
    // Try TOON first, fall back to JSON for backward compatibility
    try {
      return decode(content);
    } catch {
      return JSON.parse(content);
    }
  } catch {
    return null;
  }
}

/**
 * Encode an object as TOON and write to file.
 */
export async function writeTOON(path, obj) {
  await writeFile(path, encode(obj));
}
