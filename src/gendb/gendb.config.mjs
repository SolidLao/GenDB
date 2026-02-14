/**
 * GenDB runtime hyperparameter configuration.
 * Defaults can be overridden via CLI args in the orchestrator.
 */

export const defaults = {
  maxOptimizationIterations: 3,
  scaleFactor: 10,
  targetBenchmark: "tpc-h",
  model: "haiku",
  optimizationTarget: "execution_time",
  maxConcurrentQueries: 5,
  agentModels: {
    workload_analyzer: "haiku",
    storage_designer: "sonnet",
    orchestrator_agent: "haiku",
    code_generator: "sonnet",
    query_optimizer: "sonnet",
    learner: "sonnet",
  },
};
