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
    code_generator: "haiku",
    query_optimizer: "haiku",
  },
};