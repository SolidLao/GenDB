# GenDB: Generative Database System

A multi-agent LLM system that generates customized database execution code for user-provided SQL workloads — no pre-built DBMS required.

## Overview

GenDB takes a different approach to query execution: instead of routing queries through a general-purpose DBMS, it uses a team of LLM agents to analyze your specific SQL workload and database, then generates tailored execution code optimized for that exact use case. The input is a SQL workload and dataset; the output is a standalone, high-performance execution engine specialized to your needs.

## Key Ideas

- **Workload-specific code generation** — generate execution code tuned to the actual queries and data, not a one-size-fits-all engine
- **Multi-agent architecture** — specialized agents collaborate to analyze, generate, optimize, and evaluate
- **Pipeline parallelism** — Executor (compile+run+validate) is serial and fast (~5-10s); Learner (LLM analysis) runs in parallel with other queries' optimization steps
- **Per-agent LLM model selection** — each agent uses the most cost-effective model for its task (haiku for simple decisions, sonnet for code generation)
- **Per-operation timing** — generated code emits `[TIMING]` lines per operation (scan, join, aggregation, sort), enabling data-driven bottleneck identification
- **Data-driven join ordering** — for multi-table joins, orchestrator mandates sampling programs to empirically determine optimal join order when join dominates execution time
- **Hardware-aware optimization** — agents detect CPU cores, cache sizes, disk type (SSD/HDD), and memory; hardware config passed to optimizer and learner with HDD/SSD guardrails
- **DECIMAL precision** — DECIMAL columns stored as scaled integers (`int64_t`) to avoid IEEE 754 boundary comparison errors
- **SIMD safety templates** — correct AVX2 comparison patterns (<=, >=, BETWEEN) prevent inverted comparison bugs
- **Code safety patterns** — standalone hash structs (no `namespace std` specialization), Kahan summation for floating-point aggregation
- **Data-aware workload analysis** — workload analyzer samples actual data for accurate cardinalities and selectivity estimates
- **Knowledge-driven autonomy** — agents receive deep domain knowledge (30+ technique files) and reason about which techniques to apply
- **Self-contained per-query code** — each query gets a specialized .cpp file with all operations inlined (no shared operator library)
- **Aggressive compilation** — -O3 -march=native for auto-vectorization, -flto for link-time optimization
- **Separate ingestion and indexing** — data ingestion and index building are independent steps; indexes can be added later without re-ingesting
- **Rollback capability** — failed optimizations are automatically detected and rolled back
- **File-based result validation** — query results compared against DuckDB ground truth via automated comparison tool
- **Cache-aware cost tracking** — token costs calculated with Anthropic's prompt caching discounts

## System Architecture

**Phase 1: Offline Data Storage Optimization**
```
Workload Analyzer (with data sampling) → Storage/Index Designer → Data Ingestion → Index Building
```

**Phase 2: Online Per-Query Pipeline-Parallel Optimization**
```
Iteration 0:
  Orchestrator (batch strategy) → Code Generators (parallel)
  → Executor (serial, fast) → Learner (parallel LLM analysis)

Iteration 1+ (pipeline-parallel per query):
  Orchestrator Agent (decide) → Query Optimizer (optimize)
  → Executor (serial) → Learner (parallel)
  → improvement check

Final Assembly: collect best .cpp per query → main.cpp + Makefile
```

**All Agents:**

| Agent | Phase | Role | Model |
|-------|-------|------|-------|
| **Workload Analyzer** | 1 | Parses SQL workload, detects hardware, samples actual data for statistics | haiku |
| **Storage/Index Designer** | 1 | Designs storage layout, generates + runs ingest.cpp and build_indexes.cpp | sonnet |
| **Code Generator** | 2 | Iteration 0: generates correct code with per-operation timing + local validation | sonnet |
| **Query Optimizer** | 2 | Iterations 1+: optimizes all bottleneck categories simultaneously, preserves timing instrumentation | sonnet |
| **Executor** | 2 | Non-LLM function: compile → run → validate → parse `[TIMING]` output | (none) |
| **Learner** | 2 | LLM-only analysis: reads execution_results.json, analyzes bottlenecks using per-operation timing, recommends optimizations | sonnet |
| **Orchestrator Agent** | 2 | Strategic decisions: batch strategy (iter 0), continue/stop + timing-aware bottleneck prioritization (iter 1+) | haiku |

## Knowledge Base

Agents have access to a structured knowledge base (`src/gendb/knowledge/`) with 30+ technique files across 8 domains:

| Domain | Topics |
|--------|--------|
| **parallelism** | Thread parallelism (morsel-driven), SIMD (AVX2/SSE), data partitioning |
| **storage** | Columnar vs row, compression, memory layout, string optimization, persistent binary storage, encoding handling |
| **indexing** | Hash indexes, sorted indexes, zone maps, bloom filters |
| **query-execution** | Vectorized execution, operator fusion, compiled queries, pipeline breakers, scan/filter optimization, sort/Top-K, subquery optimization |
| **joins** | Hash join variants, sort-merge join, join ordering (with data-driven sampling) |
| **aggregation** | Hash aggregation, sorted aggregation, partial aggregation |
| **data-structures** | Compact hash tables, arena allocation, flat structures |
| **external-libs** | jemalloc/tcmalloc, abseil/folly, I/O libraries |

## Benchmarks

- **Primary benchmark**: TPC-H
- **Comparison baselines**: PostgreSQL, DuckDB
- Goal: generate workload-specific code that matches or outperforms general-purpose systems
