# GenDB: Generative Database System

A multi-agent LLM system that generates customized database execution code for user-provided SQL workloads — no pre-built DBMS required.

## Overview

GenDB takes a different approach to query execution: instead of routing queries through a general-purpose DBMS, it uses a team of LLM agents to analyze your specific SQL workload and database, then generates tailored execution code optimized for that exact use case. The input is a SQL workload and dataset; the output is a standalone, high-performance execution engine specialized to your needs.

## Key Ideas

- **Workload-specific code generation** — generate execution code tuned to the actual queries and data, not a one-size-fits-all engine
- **Multi-agent architecture** — specialized agents collaborate to analyze, generate, optimize, and evaluate
- **Per-query parallel optimization** — each query gets its own independent optimization pipeline, running in parallel
- **Hardware-aware optimization** — agents detect CPU cores, cache sizes, disk type (SSD/HDD), and memory to make adaptive decisions
- **Knowledge-driven autonomy** — agents receive deep domain knowledge and reason about which techniques to apply
- **Self-contained per-query code** — each query gets a specialized .cpp file with all operations inlined (no shared operator library)
- **Separate ingestion and indexing** — data ingestion and index building are independent steps; indexes can be added later without re-ingesting
- **Rollback capability** — failed optimizations are automatically detected and rolled back
- **File-based result validation** — query results compared against DuckDB ground truth via automated comparison tool
- **Cache-aware cost tracking** — token costs calculated with Anthropic's prompt caching discounts

## System Architecture

**Phase 1: Offline Data Storage Optimization**
```
Workload Analyzer → Storage/Index Designer → Data Ingestion → Index Building
```

**Phase 2: Online Per-Query Parallel Optimization**
```
For each query (in parallel, up to --max-concurrent):
  Code Generator → Learner → [Orchestrator Agent → Optimizer(s) → Learner] × N

Final Assembly: collect best .cpp per query → main.cpp + Makefile
```

**5 Optimization Agents** (conditionally invoked based on bottleneck):

| Agent | Bottleneck | Role |
|-------|-----------|------|
| **I/O Optimizer** | `io_bound` | mmap hints, column pruning, zone map skipping, SSD/HDD strategies |
| **Execution Optimizer** | `cpu_bound` | Thread parallelism (morsel-driven), SIMD vectorization |
| **Join Optimizer** | `join` | Join ordering, build/probe selection, algorithm choice (hash/sort-merge) |
| **Index Optimizer** | `index` | Build new indexes from binary data, modify code to use them |
| **Query Rewriter** | `semantic`/`rewrite` | Fix incorrect results, intent-based rewrites, C++ code optimization |

**All Agents:**

| Agent | Phase | Role |
|-------|-------|------|
| **Workload Analyzer** | 1 | Parses SQL workload, detects hardware, identifies optimization opportunities |
| **Storage/Index Designer** | 1 | Designs storage layout, generates + runs ingest.cpp and build_indexes.cpp |
| **Code Generator** | 2 | Generates self-contained per-query .cpp files |
| **Learner** | 2 | Compiles, runs, validates, profiles, analyzes bottlenecks, recommends optimizations |
| **Orchestrator Agent** | 2 | Decides what to optimize next (aggressive stance with rollback safety) |

## Knowledge Base

Agents have access to a structured knowledge base (`src/gendb/knowledge/`) with 27 technique files across 8 domains:

| Domain | Topics |
|--------|--------|
| **parallelism** | Thread parallelism (morsel-driven), SIMD (AVX2/SSE), data partitioning |
| **storage** | Columnar vs row, compression, memory layout, string optimization, persistent binary storage |
| **indexing** | Hash indexes, sorted indexes, zone maps, bloom filters |
| **query-execution** | Vectorized execution, operator fusion, compiled queries, pipeline breakers |
| **joins** | Hash join variants, sort-merge join, join ordering |
| **aggregation** | Hash aggregation, sorted aggregation, partial aggregation |
| **data-structures** | Compact hash tables, arena allocation, flat structures |
| **external-libs** | jemalloc/tcmalloc, abseil/folly, I/O libraries |

## Benchmarks

- **Primary benchmark**: TPC-H
- **Comparison baselines**: PostgreSQL, DuckDB
- Goal: generate workload-specific code that matches or outperforms general-purpose systems
