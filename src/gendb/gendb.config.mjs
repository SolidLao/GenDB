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
};
