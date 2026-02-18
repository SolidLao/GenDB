# GenDB: Generative Database System

A multi-agent LLM system that generates customized database execution code for user-provided SQL workloads — no pre-built DBMS required.

## Overview

GenDB takes a different approach to query execution: instead of routing queries through a general-purpose DBMS, it uses a team of LLM agents to analyze your specific SQL workload and database, then generates tailored execution code optimized for that exact use case. The input is a SQL workload and dataset; the output is a standalone, high-performance execution engine specialized to your needs.

## Key Ideas

- **Workload-specific code generation** — generate execution code tuned to the actual queries and data, not a one-size-fits-all engine
- **6-agent architecture** — Workload Analyzer, Storage Designer, DBA, Code Generator, Code Inspector, Query Optimizer
- **Utility library** — shared C++ headers (date_utils.h, hash_utils.h, mmap_utils.h, timing_utils.h) eliminate repeated reimplementation of common patterns
- **Experience base** — evolving knowledge of correctness bugs and performance anti-patterns, checked by Code Inspector before execution
- **DBA agent** — predicts risks pre-generation (Stage A), writes retrospective post-run (Stage B)
- **Code Inspector** — cheap Haiku-based review agent catches known issues before execution
- **RAII phase timing** — `GENDB_PHASE("name")` replaces 6-line `#ifdef` blocks with one-line scoped timers
- **True per-query pipelining** — each query flows independently through CodeGen→Inspector→Execute→[Optimize→Inspector→Execute]* with no batch boundaries
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
  CodeGen → Inspector → [fix if critical] → Execute
  → [shouldContinue() → Optimizer → Inspector → [fix] → Execute]* (up to 10 iterations)
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
| **Code Generator** | Sonnet | 2 | Iteration 0: logical+physical plan → C++ with utility library |
| **Code Inspector** | Haiku | 2 | Review code against experience base before execution |
| **Query Optimizer** | Sonnet | 2 | Iterations 1+: bottleneck-targeted optimization with utility library |

## Utility Library

Shared C++ headers in `src/gendb/utils/`, compiled via `-I` flag:

| Header | Purpose |
|--------|---------|
| `date_utils.h` | O(1) date extraction, epoch↔string conversion |
| `hash_utils.h` | CompactHashMap/Set with Robin Hood hashing (2-5x faster than std::unordered_map) |
| `mmap_utils.h` | MmapColumn<T> RAII wrapper for zero-copy column access |
| `timing_utils.h` | GENDB_PHASE("name") RAII scoped timer macro |

## Knowledge Base

Agents have access to a structured knowledge base (`src/gendb/knowledge/`) with 40+ technique files across 9 domains, plus an experience base (`experience.md`) of known correctness and performance issues.

## Benchmarks (TPC-H SF10)

| System | Total Time | vs DuckDB |
|--------|-----------|-----------|
| DuckDB | 2,231ms | 1.0x |
| GenDB | 37,817ms | 17x |

GenDB beats DuckDB on Q6 and Q14. All queries produce correct results validated against DuckDB ground truth.
