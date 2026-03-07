/**
 * Agent provider registry for GenDB.
 *
 * To add a new provider:
 *   1. Create a new file in this directory (e.g., myagent.mjs)
 *   2. Export an async function runAgent(name, options) that returns
 *      { result, durationMs, tokens, costUsd, error? }
 *   3. Add an entry to the PROVIDERS map below
 */

const PROVIDERS = {
  claude: () => import("./claude.mjs"),
  codex: () => import("./codex.mjs"),
};

let activeProviderName = "claude";
let cachedProvider = null;

/**
 * Set the active agent provider by name.
 * Must be called before any runAgent() calls.
 */
export function setAgentProvider(name) {
  if (!PROVIDERS[name]) {
    const available = Object.keys(PROVIDERS).join(", ");
    throw new Error(`Unknown agent provider "${name}". Available: ${available}`);
  }
  activeProviderName = name;
  cachedProvider = null; // clear cache so next getProvider() loads the new one
}

/**
 * Get the name of the currently active provider.
 */
export function getAgentProviderName() {
  return activeProviderName;
}

/**
 * Get the list of available provider names.
 */
export function getAvailableProviders() {
  return Object.keys(PROVIDERS);
}

/**
 * Load and return the active provider module.
 * The module must export an async function runAgent(name, options).
 */
export async function getProvider() {
  if (!cachedProvider) {
    cachedProvider = await PROVIDERS[activeProviderName]();
  }
  return cachedProvider;
}
