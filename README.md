# GenDB: Generative Database System

A multi-agent LLM system that generates customized database execution code for user-provided SQL workloads — no pre-built DBMS required.

## Overview

GenDB takes a different approach to query execution: instead of routing queries through a general-purpose DBMS, it uses a team of LLM agents to analyze your specific SQL workload and database, then generates tailored execution code optimized for that exact use case. The input is a SQL workload and dataset; the output is a standalone, high-performance execution engine specialized to your needs.

## Key Ideas

- **Workload-specific code generation** — generate execution code tuned to the actual queries and data, not a one-size-fits-all engine
- **7-agent architecture** — Workload Analyzer, Storage Designer, DBA, Query Planner, Code Generator, Code Inspector, Query Optimizer
- **Plan-first pipeline** — Query Planner designs structured JSON execution plans (join order, data structures, parallelism), Code Generator implements them, Optimizer can modify both plan and code
- **Utility library** — shared C++ headers (date_utils.h, hash_utils.h, mmap_utils.h, timing_utils.h) with advanced data structures (CompactHashMap, ConcurrentCompactHashMap, PartitionedHashMap, DenseBitmap, TopKHeap)
- **Experience base** — evolving knowledge of correctness bugs (C1-C15) and performance anti-patterns (P1-P8), checked by Code Inspector before execution
- **Correctness anchors** — validated constants (date thresholds, scale factors, revenue formulas) are extracted from passing code and made immutable during optimization
- **Adaptive iteration budget** — configurable stall threshold stops optimization early when no progress is being made
- **DBA agent** — predicts risks pre-generation (Stage A), writes retrospective post-run (Stage B)
- **Code Inspector** — cheap Haiku-based review agent catches known issues and optimizer-introduced regressions before execution
- **True per-query pipelining** — each query flows independently through Planner→Coder→Inspector→Execute→[Optimizer→Inspector→Execute]* with no batch boundaries
- **Knowledge-driven autonomy** — agents receive deep domain knowledge (40+ technique files across 9 domains) and reason about which techniques to apply
- **Programmatic optimization control** — continue/stop decisions and improvement checks are deterministic JavaScript functions, not LLM calls

## System Architecture

**Phase 1: Offline Data Storage + Pre-Generation Analysis**
```
Workload Analyzer → Storage/Index Designer → Per-Query Storage Guides → DBA Stage A (predict risks, extend utilities)
```

**Phase 2: Online Per-Query Pipeline-Parallel Optimization**
```
Each query runs independently (true pipelining):
  Iter 0: Query Planner → Code Generator → Inspector → [fix if critical] → Execute
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
| **Storage Designer** | Haiku | 1 | Design storage, generate + run ingestion, per-query guides |
| **DBA** | Sonnet | 1 + 3 | Predict risks, extend utilities, post-run retrospective |
| **Query Planner** | Sonnet | 2 | Iter 0: design structured JSON execution plan (no code) |
| **Code Generator** | Sonnet | 2 | Iter 0: implement plan in C++ with utility library |
| **Code Inspector** | Haiku | 2 | Review code against experience base, detect optimizer regressions |
| **Query Optimizer** | Sonnet | 2 | Iter 1+: modify plan and/or code, guided by correctness anchors |

## Utility Library

Shared C++ headers in `src/gendb/utils/`, compiled via `-I` flag:

| Header | Purpose |
|--------|---------|
| `date_utils.h` | O(1) date extraction, epoch<->string conversion |
| `hash_utils.h` | CompactHashMap/Set, ConcurrentCompactHashMap, PartitionedHashMap, DenseBitmap, TopKHeap |
| `mmap_utils.h` | MmapColumn<T> RAII wrapper for zero-copy column access |
| `timing_utils.h` | GENDB_PHASE("name") RAII scoped timer macro |

## Knowledge Base

Agents have access to a structured knowledge base (`src/gendb/knowledge/`) with 40+ technique files across 9 domains, plus an experience base (`experience.md`) of 15 correctness and 8 performance entries.

## Benchmarks (TPC-H SF10)

| System | Total Time | vs DuckDB |
|--------|-----------|-----------|
| DuckDB | 2,231ms | 1.0x |
| GenDB | 37,817ms | 17x |

GenDB beats DuckDB on Q6 and Q14. All queries produce correct results validated against DuckDB ground truth.
