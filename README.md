# GenDB: Generative Database System

A multi-agent LLM system that generates customized database execution code for user-provided SQL workloads — no pre-built DBMS required.

## Overview

GenDB takes a different approach to query execution: instead of routing queries through a general-purpose DBMS, it uses a team of LLM agents to analyze your specific SQL workload and database, then generates tailored execution code optimized for that exact use case. The input is a SQL workload and dataset; the output is a standalone, high-performance execution engine specialized to your needs.

## Key Ideas

- **Parquet for storage only** — workload-optimized columnar format with sorted columns, row group statistics, dictionary encoding, compression, and sorted index files
- **Pure C++ code generation** — all query processing in hand-written C++ loops over raw arrays. No Arrow compute, no external runtime
- **Parallel I/O** — multi-threaded Parquet reading for fast data loading
- **Sorted indexes** — auxiliary binary index files for selective row group lookups on join keys and filter columns
- **parquet_reader.h** — fixed utility header that reads Parquet columns into raw C++ arrays and provides index lookup APIs
- **Per-query parallel optimization** — each query gets its own independent optimization pipeline running in parallel
- **Fused single-pass patterns** — filter + compute + aggregate in one loop
- **Multi-agent architecture** — specialized agents collaborate to analyze, generate, optimize, and evaluate
- **Hardware-aware optimization** — agents detect CPU cores, cache sizes, disk type, memory

## System Architecture

**Phase 1 (Analysis + Data Preparation):**
```
Workload Analyzer → workload_analysis.txt + parquet_config.json
                        ↓
              convert_to_parquet.py + build_indexes.py
                        ↓
              Generate Makefile + copy parquet_reader.h
```

**Phase 2 (Per-Query Parallel Optimization):**
```
Promise.all([
  QueryPipeline(Q1): CodeGen → Learner → [Orchestrator → [Optimizers] → Learner] × N
  QueryPipeline(Q3): CodeGen → Learner → [Orchestrator → [Optimizers] → Learner] × N
  QueryPipeline(Q6): CodeGen → Learner → [Orchestrator → [Optimizers] → Learner] × N
])
→ Assemble main.cpp + queries.h → Final build + validation
```

| Agent | Role | Phase |
|-------|------|-------|
| **Workload Analyzer** | Analyzes SQL, designs Parquet config + index recommendations | Phase 1 |
| **Query Code Generator** | Generates optimized pure C++ per query using parquet_reader.h | Phase 2, Iter 0 |
| **Learner** | Compiles, runs, validates, analyzes bottlenecks, recommends optimizations | Phase 2 |
| **Orchestrator Agent** | Reads recommendations, selects optimizations or stops (per-query) | Phase 2, Iter 1+ |
| **Execution Optimizer** | Thread parallelism, SIMD, open-addressing hash tables | Phase 2, Iter 1+ |
| **I/O Optimizer** | Row group pruning, column projection, index-based lookups | Phase 2, Iter 1+ |
| **Index Optimizer** | Builds sorted index files, modifies code to use indexes | Phase 2, Iter 1+ |
| **Join Order Optimizer** | Join reordering, build/probe side selection | Phase 2, Iter 1+ |
| **Query Rewriter** | Predicate pushdown, data structure optimization | Phase 2, Iter 1+ |

## Quick Start

```bash
# 1. Generate TPC-H data
bash benchmarks/tpc-h/setup_data.sh 1

# 2. Run GenDB pipeline
node src/gendb/orchestrator.mjs --sf 1
```

## Benchmarks

- **Primary benchmark**: TPC-H
- **Comparison baselines**: PostgreSQL, DuckDB
- Goal: generate workload-specific code that matches or outperforms general-purpose systems
