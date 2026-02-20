/**
 * GenDB runtime hyperparameter configuration.
 * Defaults can be overridden via CLI args in the orchestrator.
 */

export const defaults = {
  maxOptimizationIterations: 5,
  stallThreshold: 5,  // consecutive non-improving iterations before adaptive stop
  scaleFactor: 10,
  targetBenchmark: "tpc-h",
  model: "haiku",
  optimizationTarget: "execution_time",
  maxConcurrentQueries: 22,
  agentModels: {
    workload_analyzer: "haiku",
    storage_designer: "sonnet",
    dba: "sonnet",
    query_planner: "sonnet",
    code_generator: "sonnet",
    code_inspector: "sonnet",
    query_optimizer: "sonnet",
  },
  agentThinkingBudgets: {
    workload_analyzer: 5000,    // simple analysis, haiku
    storage_designer: 10000,    // design + codegen — must NOT draft code in thinking
    dba: 10000,                 // moderate reasoning
    query_planner: 15000,       // strategy reasoning, no code output
    code_generator: 10000,      // must NOT draft C++ in thinking — use Write tool
    code_inspector: 8000,       // byte-level reasoning, sonnet
    query_optimizer: 10000,     // must NOT draft code in thinking — use Edit tool
  },
  queryExecutionTimeoutSec: 300, // per-query binary execution timeout (seconds)
  agentTimeoutMs: 15 * 60 * 1000, // 15 minutes per agent call
  agentTimeoutOverrides: {
    storage_designer: 45 * 60 * 1000, // 45 minutes — includes data ingestion + index building
  },
};