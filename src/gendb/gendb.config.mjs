/**
 * GenDB runtime hyperparameter configuration.
 * Defaults can be overridden via CLI args in the orchestrator.
 */

export const defaults = {
  maxOptimizationIterations: 5,
  stallThreshold: 5,  // consecutive non-improving iterations before adaptive stop
  scaleFactor: 10,    // 10 for tpc-h, 3 for sec-edgar
  targetBenchmark: "tpc-h", // "tpc-h" / "sec-edgar"
  model: "sonnet",
  optimizationTarget: "hot",  // "hot" (optimize avg hot runs) or "cold" (optimize cold run)
  maxConcurrentQueries: 22,
  agentModels: {
    workload_analyzer: "sonnet",
    storage_designer: "sonnet",
    dba: "sonnet",
    query_planner: "sonnet",
    code_generator: "sonnet",
    code_inspector: "sonnet",
    query_optimizer: "sonnet",
  },
  agentEffortLevels: {
    workload_analyzer: "low",     // simple analysis
    storage_designer: "medium",   // design + codegen
    dba: "medium",                // moderate reasoning
    query_planner: "medium",      // strategy reasoning
    code_generator: "medium",     // implementation
    code_inspector: "low",        // pattern matching
    query_optimizer: "medium",    // analysis + plan revision
  },
  escalationModel: "opus",          // model to use when escalating after repeated correctness failures
  escalationEffortLevel: "high",    // effort level for escalation model
  correctnessFailureCap: 3,         // consecutive correctness failures before escalating to code generator with escalation model
  queryExecutionTimeoutSec: 300, // per-query binary execution timeout (seconds)
  optimizationRuns: 3, // per-execution: all runs same mode (hot or cold based on optimizationTarget)
  agentTimeoutMs: 30 * 60 * 1000, // 30 minutes per agent call
  agentTimeoutOverrides: {
    storage_designer: 45 * 60 * 1000, // 45 minutes — includes data ingestion + index building
    query_optimizer: 30 * 60 * 1000,  // 30 minutes — reads code + storage, diagnoses, may build storage extensions
  },
  useSkills: false,
  useDba: false,
  singleAgent: {
    model: "sonnet",
    effortLevel: "medium",
    timeoutMs: 120 * 60 * 1000,  // 2 hours
    promptVariant: "high-level",      // "high-level" or "guided"
    maxOptimizationIterations: 5,
  },
};
