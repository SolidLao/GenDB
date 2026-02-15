# GenDB: Generative Database System

A multi-agent LLM system that generates customized database execution code for user-provided SQL workloads — no pre-built DBMS required.

## Overview

GenDB takes a different approach to query execution: instead of routing queries through a general-purpose DBMS, it uses a team of LLM agents to analyze your specific SQL workload and database, then generates tailored execution code optimized for that exact use case. The input is a SQL workload and dataset; the output is a standalone, high-performance execution engine specialized to your needs.

## Key Ideas

- **Workload-specific code generation** — generate execution code tuned to the actual queries and data, not a one-size-fits-all engine
- **Multi-agent architecture** — 4 specialized agents collaborate to analyze, generate, and optimize
- **Per-query storage guides** — Storage/Index Designer generates concise, strict-format markdown guides (~30-50 lines) listing data file paths, types, encodings, and index binary layouts for each query; Code Generator and Query Optimizer use these for file paths and struct layouts only, computing all constants themselves
- **Dual-mode timing** — `[TIMING]` instrumentation uses `#ifdef GENDB_PROFILE` guards; compiled in during optimization iterations (`-DGENDB_PROFILE`), stripped from final build for zero overhead
- **Filtered per-query context** — agents receive only the tables/columns relevant to the current query, embedded directly in prompts rather than full file paths
- **TPC-H-compliant validation** — per-column tolerance rules ($100 for SUM revenue, 1% for AVG, exact for COUNT) with detailed column-level failure reporting
- **Programmatic optimization control** — continue/stop decisions and improvement checks are deterministic JavaScript functions, not LLM calls
- **Pipeline parallelism** — Executor (compile+run+validate) is serial via ExecutionQueue; LLM agents run freely in parallel across queries
- **Per-agent LLM model selection** — each agent uses the most cost-effective model for its task via `agentModels` config
- **Sampling-based join ordering** — for multi-table joins, Query Optimizer can trigger empirical join order sampling when joins dominate execution time
- **Hardware-aware optimization** — agents detect CPU cores, cache sizes, disk type (SSD/HDD), and memory; hardware config propagated to per-query context
- **DECIMAL precision** — DECIMAL columns stored as scaled integers (`int64_t`) to avoid IEEE 754 boundary comparison errors
- **Knowledge-driven autonomy** — agents receive deep domain knowledge (35+ technique files) and reason about which techniques to apply
- **Self-contained per-query code** — each query gets a specialized .cpp file with all operations inlined (no shared operator library)
- **Aggressive compilation** — -O3 -march=native -fopenmp for auto-vectorization, -flto for link-time optimization
- **Rollback capability** — failed optimizations are automatically detected and rolled back

## System Architecture

**Phase 1: Offline Data Storage Optimization**
```
Workload Analyzer (with data sampling) → Storage/Index Designer → Data Ingestion → Index Building → Per-Query Storage Guides
```

**Phase 2: Online Per-Query Pipeline-Parallel Optimization**
```
Iteration 0:
  Code Generator (parallel per query) → qi.cpp
  → ExecutionQueue (serial) → execution_results.json

Iteration 1+ (pipeline-parallel per query):
  shouldContinue() (programmatic) → Query Optimizer (optimize + compile)
  → ExecutionQueue (serial) → execution_results.json
  → checkExecutionImprovement() (keep or rollback)

Final Assembly: collect best .cpp per query → main.cpp + Makefile
```

**Agents:**

| Agent | Phase | Role |
|-------|-------|------|
| **Workload Analyzer** | 1 | Parses SQL workload, detects hardware, samples actual data for statistics |
| **Storage/Index Designer** | 1 | Designs storage layout, generates + runs ingest.cpp and build_indexes.cpp, generates per-query storage guides |
| **Code Generator** | 2 | Iteration 0: generates correct code with `#ifdef GENDB_PROFILE` timing + local validation, uses storage guides for index-aware code |
| **Query Optimizer** | 2 | Iterations 1+: reads execution_results.json, analyzes bottlenecks, uses storage guides for index optimization, compiles |
| **Executor** | 2 | Non-LLM function: compile → run → validate → parse `[TIMING]` output |

## Knowledge Base

Agents have access to a structured knowledge base (`src/gendb/knowledge/`) with 35+ technique files across 8 domains:

| Domain | Topics |
|--------|--------|
| **parallelism** | Thread parallelism (morsel-driven), SIMD (AVX2/SSE), data partitioning |
| **storage** | Columnar vs row, compression, memory layout, string optimization, persistent binary storage, encoding handling |
| **indexing** | Hash indexes (multi-value), B+ Trees & sorted indexes, zone maps, bloom filters |
| **query-execution** | Vectorized execution, operator fusion, compiled queries, pipeline breakers, scan/filter optimization, sort/Top-K |
| **joins** | Hash join variants, sort-merge join, join ordering, sampling-based order determination |
| **aggregation** | Hash aggregation, sorted aggregation, partial aggregation |
| **data-structures** | Compact hash tables, arena allocation, flat structures |
| **patterns** | Parallel hash join, zone map pruning |

## Benchmarks (TPC-H SF10)

| Query | GenDB | DuckDB | PostgreSQL |
|-------|-------|--------|------------|
| Q1 (scan+agg) | **64ms** | 114ms | 15,656ms |
| Q3 (3-way join) | 322ms | **98ms** | 5,640ms |
| Q6 (scan+filter) | 33ms | **20ms** | 2,357ms |
| **Total** | **418ms** | **232ms** | **23,653ms** |

All queries produce correct results validated against DuckDB ground truth with TPC-H precision rules. GenDB beats PostgreSQL by 57x overall and is 1.8x of DuckDB. GenDB beats DuckDB on Q1 (1.8x faster). Q3 join performance is the primary optimization target.
