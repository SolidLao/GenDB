/**
 * GenDB runtime hyperparameter configuration.
 * Defaults can be overridden via CLI args in the orchestrator.
 */

export const defaults = {
  maxOptimizationIterations: 10,
  scaleFactor: 10,
  targetBenchmark: "tpc-h",
  model: "haiku",
  optimizationTarget: "execution_time",
  maxConcurrentQueries: 22,
  agentModels: {
    workload_analyzer: "haiku",
    storage_designer: "haiku",
    dba: "sonnet",
    code_generator: "sonnet",
    code_inspector: "haiku",
    query_optimizer: "sonnet",
  },
  agentTimeoutMs: 15 * 60 * 1000, // 15 minutes per agent call
  agentTimeoutOverrides: {
    storage_designer: 45 * 60 * 1000, // 45 minutes — includes data ingestion + index building
  },
};