/**
 * GenDB runtime hyperparameter configuration.
 * Defaults can be overridden via CLI args in the orchestrator.
 *
 * Structure:
 *   - Top level: pipeline settings (provider-agnostic)
 *   - providers.<name>: provider-specific settings (model, effort levels, pricing)
 */

export const defaults = {
  // --- Pipeline settings (provider-agnostic) ---
  agentProvider: "claude",  // "claude" or "codex" — select the underlying agent SDK
  targetBenchmark: "tpc-h", // "tpc-h" / "sec-edgar"
  scaleFactor: 10,          // 10 for tpc-h, 3 for sec-edgar
  optimizationTarget: "hot",  // "hot" (optimize avg hot runs) or "cold" (optimize cold run)
  maxOptimizationIterations: 5,
  stallThreshold: 5,        // consecutive non-improving iterations before adaptive stop
  maxConcurrentQueries: 22,
  queryExecutionTimeoutSec: 300, // per-query binary execution timeout (seconds)
  optimizationRuns: 3,      // per-execution: all runs same mode (hot or cold)
  correctnessFailureCap: 3, // consecutive correctness failures before escalating
  useSkills: false,
  useDba: false,

  // --- Agent timeouts (provider-agnostic) ---
  agentTimeoutMs: 30 * 60 * 1000, // 30 minutes per agent call
  agentTimeoutOverrides: {
    storage_designer: 45 * 60 * 1000, // 45 minutes — includes data ingestion + index building
    query_optimizer: 30 * 60 * 1000,  // 30 minutes
  },

  // --- Single-agent mode (provider-agnostic parts) ---
  singleAgent: {
    timeoutMs: 120 * 60 * 1000,  // 2 hours
    promptVariant: "high-level",  // "high-level" or "guided"
    maxOptimizationIterations: 5,
  },

  // --- Provider-specific settings ---
  providers: {
    claude: {
      model: "opus",
      agentModels: {
        workload_analyzer: "opus",
        storage_designer: "opus",
        dba: "opus",
        query_planner: "opus",
        code_generator: "opus",
        code_inspector: "opus",
        query_optimizer: "opus",
      },
      // Claude effort: "low" | "medium" | "high" | "max" (max = Opus only)
      agentEffortLevels: {
        workload_analyzer: "low",
        storage_designer: "medium",
        dba: "medium",
        query_planner: "medium",
        code_generator: "medium",
        code_inspector: "low",
        query_optimizer: "medium",
      },
      escalationModel: "opus",
      escalationEffortLevel: "high",
      singleAgent: {
        model: "opus",
        effortLevel: "medium",
      },
    },
    codex: {
      model: "gpt-5.4",
      agentModels: {
        workload_analyzer: "gpt-5.4",
        storage_designer: "gpt-5.4",
        dba: "gpt-5.4",
        query_planner: "gpt-5.4",
        code_generator: "gpt-5.4",
        code_inspector: "gpt-5.4",
        query_optimizer: "gpt-5.4",
      },
      // Codex effort: "minimal" | "low" | "medium" | "high" | "xhigh"
      agentEffortLevels: {
        workload_analyzer: "low",
        storage_designer: "medium",
        dba: "medium",
        query_planner: "medium",
        code_generator: "medium",
        code_inspector: "low",
        query_optimizer: "medium",
      },
      escalationModel: "gpt-5.4",
      escalationEffortLevel: "high",
      singleAgent: {
        model: "gpt-5.4",
        effortLevel: "medium",
      },
    },
  },
};

/**
 * Get the provider-specific config for the active (or given) provider.
 */
export function getProviderConfig(providerName) {
  const name = providerName || defaults.agentProvider;
  const config = defaults.providers[name];
  if (!config) {
    throw new Error(`No provider config for "${name}". Available: ${Object.keys(defaults.providers).join(", ")}`);
  }
  return config;
}
