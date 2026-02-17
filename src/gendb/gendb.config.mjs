/**
 * GenDB runtime hyperparameter configuration.
 * Defaults can be overridden via CLI args in the orchestrator.
 */

export const defaults = {
  maxOptimizationIterations: 10,
  scaleFactor: 10,
  targetBenchmark: "tpc-h",
  model: "sonnet",
  optimizationTarget: "execution_time",
  maxConcurrentQueries: 22,
  agentModels: {
    workload_analyzer: "sonnet",
    storage_designer: "sonnet",
    code_generator: "sonnet",
    query_optimizer: "sonnet",
  },
};