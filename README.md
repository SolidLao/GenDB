# GenDB: Generative Database System

A multi-agent LLM system that generates customized database execution code for user-provided SQL workloads — no pre-built DBMS required.

## Overview

GenDB takes a different approach to query execution: instead of routing queries through a general-purpose DBMS, it uses a team of LLM agents to analyze your specific SQL workload and database, then generates tailored execution code optimized for that exact use case. The input is a SQL workload and dataset; the output is a standalone, high-performance execution engine specialized to your needs.

## Key Ideas

- **Workload-specific code generation** — generate execution code tuned to the actual queries and data, not a one-size-fits-all engine
- **Multi-agent architecture** — 4 specialized agents collaborate to analyze, generate, and optimize
- **Per-query storage guides** — Storage/Index Designer generates concise, strict-format markdown guides (~30-50 lines) listing data file paths, types, encodings, and index binary layouts for each query
- **True per-query pipelining** — each query flows independently through CodeGen→Execute→[Optimize→Execute]* with no batch boundaries; LLM calls gated by semaphore, execution serialized via ExecutionQueue
- **Per-query benchmark targets** — optimizer receives focused comparison table (current GenDB vs all engines for that specific query) instead of full 1300-line JSON dump
- **Stall detection** — after 3 consecutive non-improving iterations with >5x gap to best engine, triggers architectural restructure via optimizer
- **Anti-pattern scanning** — optimizer checks for 6 common anti-patterns (unordered_map, loop-based dates, eager string loading, redundant scans, hash tables for small domains, per-row subquery evaluation) before any analysis
- **Knowledge-driven autonomy** — agents receive deep domain knowledge (40+ technique files across 9 domains) and reason about which techniques to apply
- **Dual-mode timing** — `[TIMING]` instrumentation uses `#ifdef GENDB_PROFILE` guards; compiled in during optimization iterations, stripped from final build
- **Logical→Physical→Code planning** — Code Generator produces a logical plan and physical plan before writing any C++ code
- **Programmatic optimization control** — continue/stop decisions and improvement checks are deterministic JavaScript functions, not LLM calls
- **Rollback capability** — failed optimizations are automatically detected and rolled back
- **Self-contained per-query code** — each query gets a specialized .cpp file with all operations inlined
- **Aggressive compilation** — -O3 -march=native -fopenmp for auto-vectorization, -flto for link-time optimization

## System Architecture

**Phase 1: Offline Data Storage Optimization**
```
Workload Analyzer (with data sampling) → Storage/Index Designer → Data Ingestion → Index Building → Per-Query Storage Guides
```

**Phase 2: Online Per-Query Pipeline-Parallel Optimization**
```
Each query runs independently (true pipelining):
  CodeGen (LLM, semaphore-gated) → Execute (serial via ExecutionQueue)
  → [shouldContinue() → Query Optimizer (LLM) → Execute]* (up to 10 iterations)
  → done

All queries progress independently — no batch boundaries.
Final Assembly: collect best .cpp per query → main.cpp + Makefile
```

**Agents:**

| Agent | Phase | Role |
|-------|-------|------|
| **Workload Analyzer** | 1 | Parses SQL workload, detects hardware, samples actual data for statistics |
| **Storage/Index Designer** | 1 | Designs storage layout, generates + runs ingest.cpp and build_indexes.cpp, generates per-query storage guides |
| **Code Generator** | 2 | Iteration 0: produces logical+physical query plan, then generates correct code with per-query benchmark targets |
| **Query Optimizer** | 2 | Iterations 1+: three-level bottleneck analysis (anti-pattern scan → plan-level → operator-level), stall detection triggers architectural restructure |
| **Executor** | 2 | Non-LLM function: compile → run → validate → parse `[TIMING]` output |

## Knowledge Base

Agents have access to a structured knowledge base (`src/gendb/knowledge/`) with 40+ technique files across 9 domains:

| Domain | Topics |
|--------|--------|
| **techniques** | Beating general-purpose engines, O(1) date operations, semi-join/anti-join patterns, direct array lookup, bloom filter joins, late materialization |
| **parallelism** | Thread parallelism (morsel-driven), SIMD (AVX2/SSE), data partitioning |
| **storage** | Columnar vs row, compression, memory layout, string optimization, persistent binary storage, encoding handling |
| **indexing** | Hash indexes (multi-value), B+ Trees & sorted indexes, zone maps, bloom filters |
| **query-execution** | Query planning (logical→physical→code pipeline), vectorized execution, operator fusion, compiled queries, pipeline breakers, scan/filter optimization, sort/Top-K, subquery optimization |
| **joins** | Hash join variants, sort-merge join, join ordering, sampling-based order determination |
| **aggregation** | Hash aggregation, sorted aggregation, partial aggregation |
| **data-structures** | Compact hash tables, arena allocation, flat structures |
| **patterns** | Parallel hash join, zone map pruning |

## Benchmarks (TPC-H SF10, 22 queries)

| System | Total Time | vs DuckDB |
|--------|-----------|-----------|
| DuckDB | 2,231ms | 1.0x |
| GenDB | 37,817ms | 17x |

GenDB beats DuckDB on Q6 (25ms vs 19ms) and Q14 (14ms vs 67ms). Primary optimization targets: Q21 (46x gap), Q18 (25x), Q10 (17x), Q3 (16x) — all complex multi-table joins where generated code uses suboptimal data structures and execution plans. All 22 queries produce correct results validated against DuckDB ground truth.
