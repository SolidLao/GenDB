# GenDB: Generative Database System

A multi-agent LLM system that generates customized database execution code for user-provided SQL workloads — no pre-built DBMS required.

## Overview

GenDB takes a different approach to query execution: instead of routing queries through a general-purpose DBMS, it uses a team of LLM agents to analyze your specific SQL workload and database, then generates tailored execution code optimized for that exact use case. The input is a SQL workload and dataset; the output is a standalone, high-performance execution engine specialized to your needs.

## Key Ideas

- **Workload-specific code generation** — generate execution code tuned to the actual queries and data, not a one-size-fits-all engine
- **7-agent architecture** — Workload Analyzer, Storage Designer, DBA, Query Planner, Code Generator, Code Inspector, Query Optimizer
- **Plan-first pipeline** — Query Planner designs structured JSON execution plans (join order, data structures, parallelism), Code Generator implements them, Optimizer can modify both plan and code
- **Timeout-resilient code generation** — Code Generator does full compile→run→validate internally (catching logical bugs early), but the orchestrator always calls `executeQuery()` as a safety net — surviving agent timeouts on complex queries
- **Fully adaptive code generation** — agents generate all data structures (hash tables, mmap loading, bitsets) inline, tailored to each query's specific types, cardinalities, and access patterns. Only date_utils.h and timing_utils.h are system infrastructure.
- **Unified Query Guide** — Storage Designer generates comprehensive per-query guides (`Qi_guide.md`) with column usage contracts, SQL→C++ conversion examples, table stats, query analysis, and index layouts — the sole reference for all Phase 2 agents
- **Deterministic failure diagnosis** — orchestrator detects common validation failure patterns (ratio errors, zero-output filters, row count mismatches) and provides actionable hints to the optimizer
- **Index-aware** — agents read pre-built index binary formats from the Query Guide and generate matching loader code inline
- **Experience base** — evolving knowledge of correctness bugs and performance anti-patterns, checked by Code Inspector before execution
- **Correctness anchors** — validated constants (date thresholds, revenue formulas) are extracted from passing code and made immutable during optimization
- **Adaptive iteration budget** — stall detection triggers after 2 consecutive non-improving iterations with 3x gap from baseline
- **DBA agent** — optional pre-generation risk analysis (Stage A, `--dba-stage-a`), retrospective post-run (Stage B)
- **Code Inspector** — cheap Haiku-based review agent catches correctness issues (critical) and performance suggestions (non-blocking), now receives Query Guide for encoding verification
- **True per-query pipelining** — each query flows independently through Planner→Coder→Inspector→Execute→[fix]→[Optimizer→Inspector→Execute]* with no batch boundaries
- **Knowledge-driven autonomy** — agents receive deep domain knowledge (40+ technique files across 9 domains) and reason about which techniques to apply
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

**Agents:**

| Agent | Model | Phase | Role |
|-------|-------|-------|------|
| **Workload Analyzer** | Haiku | 1 | Parse SQL workload, detect hardware, sample data |
| **Storage Designer** | Haiku | 1 | Design storage, generate + run ingestion, comprehensive per-query guides |
| **DBA** | Sonnet | 1 + 3 | Optional pre-gen risk analysis (Stage A), post-run retrospective (Stage B) |
| **Query Planner** | Sonnet | 2 | Iter 0: design structured JSON execution plan (no code) |
| **Code Generator** | Sonnet | 2 | Iter 0: compile + run + validate C++ (orchestrator safety net on timeout) |
| **Code Inspector** | Haiku | 2 | Review code against experience base + Query Guide, detect optimizer regressions |
| **Query Optimizer** | Sonnet | 2 | Iter 1+: modify plan and/or code, guided by correctness anchors |

## System Utilities

Shared C++ headers in `src/gendb/utils/`, compiled via `-I` flag:

| Header | Purpose |
|--------|---------|
| `date_utils.h` | O(1) date extraction, epoch<->string conversion |
| `timing_utils.h` | GENDB_PHASE("name") RAII scoped timer macro |

All other data structures (hash tables, mmap loading, bitsets, heaps) are generated inline by agents, tailored to each query's specific types and access patterns.

## Knowledge Base

Agents have access to a structured knowledge base (`src/gendb/knowledge/`) with 40+ technique files across 9 domains, plus a consolidated experience base (`experience.md`) of correctness and performance entries.

## Benchmarks (TPC-H SF10)

| System | Total Time | vs DuckDB |
|--------|-----------|-----------|
| DuckDB | 2,231ms | 1.0x |
| GenDB | 37,817ms | 17x |

GenDB beats DuckDB on Q6 and Q14. All queries produce correct results validated against DuckDB ground truth.
