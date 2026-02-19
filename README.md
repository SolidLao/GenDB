# GenDB: Generative Database System

A multi-agent LLM system that generates customized database execution code for user-provided SQL workloads — no pre-built DBMS required.

## Overview

GenDB takes a different approach to query execution: instead of routing queries through a general-purpose DBMS, it uses a team of LLM agents to analyze your specific SQL workload and database, then generates tailored execution code optimized for that exact use case. The input is a SQL workload and dataset; the output is a standalone, high-performance execution engine specialized to your needs.

## Key Ideas

- **Workload-specific code generation** — generate execution code tuned to the actual queries and data, not a one-size-fits-all engine
- **7-agent architecture** — Workload Analyzer, Storage Designer, DBA, Query Planner, Code Generator, Code Inspector, Query Optimizer
- **Plan-first pipeline** — Query Planner designs structured JSON execution plans (join order, data structures, parallelism), Code Generator implements them, Optimizer can modify both plan and code
- **Timeout-resilient code generation** — Code Generator does full compile→run→validate internally (catching logical bugs early), but the orchestrator always calls `executeQuery()` as a safety net — surviving agent timeouts on complex queries. Fallback execution runs on agent crash/timeout to prevent iteration gaps.
- **Inactivity detection** — agents that produce no output for 5 minutes are killed early, catching `max_output_token` hangs without waiting for the full agent timeout
- **Fully adaptive code generation** — agents generate all data structures (hash tables, mmap loading, bitsets) inline, tailored to each query's specific types, cardinalities, and access patterns. Only date_utils.h and timing_utils.h are system infrastructure.
- **Unified Query Guide** — Storage Designer generates comprehensive per-query guides (`Qi_guide.md`) with column usage contracts, SQL→C++ conversion examples, table stats, query analysis, and index layouts — the sole reference for all Phase 2 agents
- **Deterministic failure diagnosis** — orchestrator detects common validation failure patterns (ratio errors, zero-output filters, row count mismatches) and provides actionable hints to the optimizer
- **Index-aware** — agents read pre-built index binary formats from the Query Guide and generate matching loader code inline
- **Experience base** — evolving knowledge of correctness bugs and performance anti-patterns, checked by Code Inspector before execution
- **Correctness anchors** — validated constants (date thresholds, revenue formulas) are extracted from passing code and made immutable during optimization
- **Adaptive iteration budget** — stall detection triggers after 2 consecutive non-improving iterations with 3x gap from baseline
- **Gap-tolerant iteration reporting** — summary tables and benchmarks scan all `iter_*` directories instead of breaking on the first missing one, so late-succeeding iterations are always visible
- **DBA agent** — optional pre-generation risk analysis (Stage A, `--dba-stage-a`), retrospective post-run (Stage B)
- **Code Inspector** — cheap Haiku-based review agent catches correctness issues (critical) and performance suggestions (non-blocking), now receives Query Guide for encoding verification
- **True per-query pipelining** — each query flows independently through Planner→Coder→Inspector→Execute→[fix]→[Optimizer→Inspector→Execute]* with no batch boundaries
- **Knowledge-driven autonomy** — Query Planner reads the knowledge base (40+ technique files) and encodes strategy into plan.json; downstream agents receive only the plan + Query Guide, avoiding redundant knowledge reads and input bloat
- **Programmatic optimization control** — continue/stop decisions and improvement checks are deterministic JavaScript functions, not LLM calls

## System Architecture

**Phase 1: Offline Data Storage + Pre-Generation Analysis**
```
Workload Analyzer → Storage/Index Designer → Per-Query Guides (Qi_guide.md) → [DBA Stage A (optional)]
```

**Phase 2: Online Per-Query Pipeline-Parallel Optimization**
```
Each query runs independently (true pipelining):
  Iter 0: Query Planner → Code Generator (compile+run+validate) → Inspector → Execute (safety net)
  Iter 1+: [shouldContinue() → Optimizer → Inspector → [fix] → Execute]* (adaptive budget)
  → done
```

**Phase 3: Post-Run Retrospective**
```
DBA Stage B → Review all results, identify patterns, write retrospective/
```

## Agent Workflow Diagram

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                         PHASE 1: OFFLINE DATA STORAGE                          ║
╠══════════════════════════════════════════════════════════════════════════════════╣
║                                                                                ║
║  ┌─────────────────────────┐     ┌──────────────────────────────────────────┐   ║
║  │   Workload Analyzer     │     │       Storage/Index Designer             │   ║
║  │   (Haiku)               │     │       (Haiku)                            │   ║
║  │                         │     │                                          │   ║
║  │  1. Detect hardware     │────>│  1. Read workload_analysis.json          │   ║
║  │  2. Profile data files  │     │  2. Design column encodings + indexes    │   ║
║  │  3. Analyze SQL queries │     │  3. Generate + run ingest.cpp            │   ║
║  │  4. Sample cardinalities│     │  4. Generate + run build_indexes.cpp     │   ║
║  │                         │     │  5. Write Qi_guide.md per query          │   ║
║  │  Out: workload_analysis │     │                                          │   ║
║  │       .json             │     │  Out: storage_design.json, .gendb/ data, │   ║
║  └─────────────────────────┘     │       query_guides/Qi_guide.md           │   ║
║                                  └──────────────┬───────────────────────────┘   ║
║                                                 │                               ║
║                                  ┌──────────────▼───────────────────────────┐   ║
║                                  │  DBA Stage A (optional, --dba-stage-a)   │   ║
║                                  │  (Sonnet)                                │   ║
║                                  │                                          │   ║
║                                  │  1. Predict correctness risks per query  │   ║
║                                  │  2. Update experience.md with warnings   │   ║
║                                  └──────────────────────────────────────────┘   ║
╠══════════════════════════════════════════════════════════════════════════════════╣
║                 PHASE 2: PER-QUERY PIPELINE-PARALLEL OPTIMIZATION              ║
║                    (Each query Qi runs independently)                           ║
╠══════════════════════════════════════════════════════════════════════════════════╣
║                                                                                ║
║  ┌─────────────────────────────┐                                               ║
║  │    Query Planner (Sonnet)   │  Inputs: Qi_guide.md, Knowledge Base          ║
║  │                             │                                               ║
║  │  1. Read INDEX.md + query   │  The ONLY agent that reads the full           ║
║  │     technique files         │  knowledge base (40+ technique files).         ║
║  │  2. Analyze cardinalities,  │  Designs strategy; downstream agents          ║
║  │     join graph, predicates  │  implement it.                                ║
║  │  3. Design logical plan     │                                               ║
║  │  4. Design physical plan    │                                               ║
║  │  5. Write plan.json         │                                               ║
║  └──────────────┬──────────────┘                                               ║
║                 │                                                               ║
║                 ▼  Iter 0                                                       ║
║  ┌─────────────────────────────┐                                               ║
║  │  Code Generator (Sonnet)    │  Inputs: plan.json, Qi_guide.md               ║
║  │                             │                                               ║
║  │  1. Read plan.json          │  Does NOT read knowledge base —               ║
║  │  2. Implement plan in C++   │  relies on plan.json for strategy             ║
║  │  3. Write .cpp file         │  and Qi_guide.md for data formats.            ║
║  │  4. Compile → Run → Validate│                                               ║
║  │  5. Fix if needed (2 tries) │                                               ║
║  └──────────────┬──────────────┘                                               ║
║                 │                                                               ║
║                 ▼                                                               ║
║  ┌─────────────────────────────┐                                               ║
║  │  Code Inspector (Haiku)     │  Inputs: .cpp file, experience.md,            ║
║  │                             │          Qi_guide.md                           ║
║  │  1. Read C++ source         │                                               ║
║  │  2. Check experience base   │  Cheap pre-execution review.                  ║
║  │  3. Detect correctness bugs │  Critical issues (C*) → fix before run.       ║
║  │  4. Flag perf anti-patterns │  Suggestions (P*) → non-blocking.             ║
║  │  5. Detect optimizer regress│                                               ║
║  └──────────────┬──────────────┘                                               ║
║                 │                                                               ║
║                 ▼                                                               ║
║  ┌─────────────────────────────┐                                               ║
║  │  Executor (non-LLM)        │  Orchestrator safety net — runs even           ║
║  │                             │  if Code Generator timed out.                  ║
║  │  1. Compile with -O3 -flto  │                                               ║
║  │  2. Run binary (300s limit) │                                               ║
║  │  3. Validate vs ground truth│                                               ║
║  │  4. Parse [TIMING] phases   │                                               ║
║  │  5. Extract correctness     │                                               ║
║  │     anchors from passing run│                                               ║
║  └──────────────┬──────────────┘                                               ║
║                 │                                                               ║
║                 ▼  Iter 1+ (adaptive budget, shouldContinue())                  ║
║  ┌─────────────────────────────┐                                               ║
║  │  Query Optimizer (Sonnet)   │  Inputs: .cpp, Qi_guide.md,                   ║
║  │                             │    execution_results, anchors, diagnosis       ║
║  │  1. Parse [TIMING] breakdown│                                               ║
║  │  2. Read plan.json          │  Does NOT read knowledge base —               ║
║  │  3. Read optimization hist  │  uses Sonnet's internal knowledge             ║
║  │  4. Check arch-level issues │  + Qi_guide.md + timing data.                 ║
║  │  5. Edit code (targeted)    │                                               ║
║  │  6. Compile (no run)        │  Output Discipline: Edit-only,                ║
║  │                             │  concise reasoning, no full-file output.       ║
║  └──────────────┬──────────────┘                                               ║
║                 │                                                               ║
║                 ▼                                                               ║
║          Inspector → Executor → [loop if shouldContinue()]                      ║
║                                                                                ║
║  Stall detection: 2 consecutive non-improving + 3x gap → stall recovery        ║
║  Correctness cap: 3 failures → stop                                            ║
║  Best result tracked across all iterations                                      ║
╠══════════════════════════════════════════════════════════════════════════════════╣
║                       PHASE 3: POST-RUN RETROSPECTIVE                          ║
╠══════════════════════════════════════════════════════════════════════════════════╣
║                                                                                ║
║  ┌─────────────────────────────┐                                               ║
║  │  DBA Stage B (Sonnet)       │                                               ║
║  │                             │                                               ║
║  │  1. Read all exec results   │                                               ║
║  │  2. Classify: SUCCESS/SLOW/ │                                               ║
║  │     FAILED per query        │                                               ║
║  │  3. Root-cause failures     │                                               ║
║  │  4. Identify bottlenecks    │                                               ║
║  │  5. Write retrospective     │                                               ║
║  └─────────────────────────────┘                                               ║
╚══════════════════════════════════════════════════════════════════════════════════╝
```

**Agents:**

| Agent | Model | Phase | Role |
|-------|-------|-------|------|
| **Workload Analyzer** | Haiku | 1 | Parse SQL workload, detect hardware, sample data |
| **Storage Designer** | Haiku | 1 | Design storage, generate + run ingestion, comprehensive per-query guides |
| **DBA** | Sonnet | 1 + 3 | Optional pre-gen risk analysis (Stage A), post-run retrospective (Stage B) |
| **Query Planner** | Sonnet | 2 | Iter 0: design structured JSON execution plan; sole knowledge base consumer |
| **Code Generator** | Sonnet | 2 | Iter 0: implement plan.json in C++, compile + run + validate (no knowledge base) |
| **Code Inspector** | Haiku | 2 | Review code against experience base + Query Guide, detect optimizer regressions |
| **Query Optimizer** | Sonnet | 2 | Iter 1+: targeted edits to plan/code, output-disciplined (no knowledge base) |

## System Utilities

Shared C++ headers in `src/gendb/utils/`, compiled via `-I` flag:

| Header | Purpose |
|--------|---------|
| `date_utils.h` | O(1) date extraction, epoch<->string conversion |
| `timing_utils.h` | GENDB_PHASE("name") RAII scoped timer macro |

All other data structures (hash tables, mmap loading, bitsets, heaps) are generated inline by agents, tailored to each query's specific types and access patterns.

## Knowledge Base

The Query Planner agent has access to a structured knowledge base (`src/gendb/knowledge/`) with 40+ technique files across 9 domains. It is the sole strategy hub — downstream agents (Code Generator, Optimizer) receive the plan and Query Guide instead, reducing input bloat. The Code Inspector reads only the experience base (`experience.md`) of correctness and performance entries.

## Benchmarks (TPC-H SF10)

| System | Total Time | vs DuckDB |
|--------|-----------|-----------|
| DuckDB | 2,231ms | 1.0x |
| GenDB | 37,817ms | 17x |

GenDB beats DuckDB on Q6 and Q14. All queries produce correct results validated against DuckDB ground truth.
