# GenDB: Generative Database System

A multi-agent LLM system that generates customized database execution code for user-provided SQL workloads — no pre-built DBMS required.

## Overview

GenDB takes a different approach to query execution: instead of routing queries through a general-purpose DBMS, it uses a team of LLM agents to analyze your specific SQL workload and database, then generates tailored execution code optimized for that exact use case. The input is a SQL workload and dataset; the output is a standalone, high-performance execution engine specialized to your needs.

## Key Ideas

- **Workload-specific code generation** — generate execution code tuned to the actual queries and data, not a one-size-fits-all engine
- **Multi-agent architecture** — specialized agents collaborate to analyze, generate, optimize, and evaluate; optimization agents are selected based on bottleneck type
- **Hardware-aware optimization** — agents automatically detect CPU cores, cache sizes, disk type (SSD/HDD), and available memory to make adaptive optimization decisions
- **Knowledge-driven autonomy** — agents receive deep domain knowledge and reason about which techniques to apply, rather than following fixed optimization steps
- **Exploitation/exploration balance** — each agent has a tuned balance: mechanical agents (Evaluator: 95/5) follow protocols, creative agents (Execution Optimizer: 30/70) freely explore parallel execution strategies
- **Incremental generation** — start with a simple correct baseline with reusable operator library, then iteratively optimize based on profiling feedback
- **Learn from existing systems** — a structured knowledge base captures how PostgreSQL, DuckDB, ClickHouse, and others solve performance problems
- **Flexible optimization** — no fixed optimization pipeline; conditional agent invocation based on bottleneck category (cpu_bound, io_bound, join_order, etc.)

## System Architecture

**Phase 1 (Baseline):**
```
Workload Analyzer → Storage/Index Designer → Code Generator
                                              → Physical Operator Agent → Evaluator
```

**Phase 2 (Optimization Loop):**
```
Learner → Orchestrator Agent → [Conditional Optimization Agent] → Evaluator
                                     ↓
                    ┌────────────────┴────────────────┬──────────────────┐
                    │                                 │                  │
          ┌─────────▼──────────┐           ┌─────────▼──────┐  ┌────────▼────────┐
          │ Query Rewriter     │           │ Join Order     │  │ Execution       │
          │ (query_structure)  │           │ Optimizer      │  │ Optimizer       │
          └────────────────────┘           │ (join_order)   │  │ (cpu_bound)     │
                                           └────────────────┘  └─────────────────┘
          ┌────────────────────┐           ┌─────────────────────────────────────┐
          │ I/O Optimizer      │           │ Physical Operator Agent             │
          │ (io_bound)         │           │ (algorithm)                         │
          └────────────────────┘           └─────────────────────────────────────┘
```

| Agent | Role | Phase |
|-------|------|-------|
| **Orchestrator** | Coordinates the pipeline, decides what to optimize next, selects optimization agents based on bottleneck category | Both |
| **Workload Analyzer** | Parses SQL workload, identifies parallelism opportunities, estimates cardinalities and selectivities | Phase 1 |
| **Storage/Index Designer** | Detects hardware (CPU cores, cache, SSD/HDD), designs data layouts, encodings, compression, and indexes | Phase 1 |
| **Code Generator** | Generates C++ code with reusable operator library (operators/*.h), includes parallelism as baseline feature | Phase 1 |
| **Physical Operator Agent** | Creates/optimizes reusable operator library (scan, hash join, hash aggregation) for algorithm-level bottlenecks | Both |
| **Evaluator** | Benchmarks generated code, validates semantic equivalence for query rewrites | Both |
| **Learner** | Diagnoses bottlenecks, categorizes by type (cpu_bound, io_bound, etc.), proposes hardware-aware optimizations | Phase 2 |
| **Query Rewriter** | Rewrites SQL queries (correlated subqueries → joins, add CTEs) for query_structure bottlenecks | Phase 2 |
| **Join Order Optimizer** | Optimizes physical join order and build/probe side selection for join_order bottlenecks | Phase 2 |
| **Execution Optimizer** | Adds thread parallelism (morsel-driven) and SIMD vectorization for cpu_bound bottlenecks | Phase 2 |
| **I/O Optimizer** | Optimizes storage access (madvise hints, column pruning, SSD/HDD strategies) for io_bound bottlenecks | Phase 2 |

## Knowledge Base

Agents have access to a structured knowledge base (`src/gendb/knowledge/`) with 27 technique files across 8 domains. **Parallelism is prioritized at the top** as the #1 performance optimization:

| Domain | Topics | Priority |
|--------|--------|----------|
| **parallelism** | Thread parallelism (morsel-driven), SIMD (AVX2/SSE), data partitioning, hardware detection | **CRITICAL** |
| **storage** | Columnar vs row, compression, memory layout, string optimization, persistent binary storage | High |
| **indexing** | Hash indexes, sorted indexes, zone maps, bloom filters | High |
| **query-execution** | Vectorized execution, operator fusion, compiled queries, pipeline breakers | Medium |
| **joins** | Hash join variants, sort-merge join, join ordering | Medium |
| **aggregation** | Hash aggregation, sorted aggregation, partial aggregation | Medium |
| **data-structures** | Compact hash tables, arena allocation, flat structures | Medium |
| **external-libs** | jemalloc/tcmalloc, abseil/folly, I/O libraries | Low |

Each file contains: what the technique is, when to use it, C++ implementation patterns, hardware detection commands (nproc, lscpu, lsblk), performance characteristics, real-world examples from production databases, and common pitfalls.

## Optimization Strategy

GenDB uses **conditional agent invocation** based on bottleneck type:

1. The **Evaluator** profiles the current generated code and identifies bottlenecks
2. The **Learner** reads relevant knowledge base files, diagnoses root causes, categorizes bottlenecks (`query_structure`, `join_order`, `cpu_bound`, `io_bound`, `algorithm`), and proposes hardware-aware optimizations
3. The **Orchestrator Agent** reviews recommendations and strategically selects which to apply, considering optimization history and rollback capability (per-query rollback enables higher-risk optimizations)
4. **Conditional optimization agent selection** based on bottleneck category:
   - `query_structure` → Query Rewriter (SQL-level optimization)
   - `join_order` → Join Order Optimizer (physical join reordering)
   - `cpu_bound` → Execution Optimizer (thread parallelism + SIMD)
   - `io_bound` → I/O Optimizer (madvise hints, column pruning)
   - `algorithm` → Physical Operator Agent (operator algorithm changes)
5. The selected agent implements optimizations, consulting the knowledge base for implementation patterns
6. Hardware detection (nproc, lscpu, lsblk) enables adaptive optimizations (thread count, morsel sizing, SSD vs HDD strategies)

## Benchmarks

- **Primary benchmark**: TPC-H
- **Comparison baselines**: PostgreSQL, DuckDB
- Goal: generate workload-specific code that matches or outperforms general-purpose systems on targeted workloads
