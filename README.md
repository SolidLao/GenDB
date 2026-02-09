# GenDB: Generative Database System

A multi-agent LLM system that generates customized database execution code for user-provided SQL workloads — no pre-built DBMS required.

## Overview

GenDB takes a different approach to query execution: instead of routing queries through a general-purpose DBMS, it uses a team of LLM agents to analyze your specific SQL workload and database, then generates tailored execution code optimized for that exact use case. The input is a SQL workload and dataset; the output is a standalone, high-performance execution engine specialized to your needs.

## Key Ideas

- **Workload-specific code generation** — generate execution code tuned to the actual queries and data, not a one-size-fits-all engine
- **Multi-agent architecture** — seven specialized agents collaborate to analyze, generate, optimize, and evaluate
- **Knowledge-driven autonomy** — agents receive deep domain knowledge and reason about which techniques to apply, rather than following fixed optimization steps
- **Exploitation/exploration balance** — each agent has a tuned balance: mechanical agents (Evaluator: 95/5) follow protocols, creative agents (Operator Specialist: 30/70) freely explore novel approaches
- **Incremental generation** — start with a simple correct baseline, then iteratively optimize based on profiling feedback
- **Learn from existing systems** — a structured knowledge base captures how PostgreSQL, DuckDB, ClickHouse, and others solve performance problems
- **Flexible optimization** — no fixed optimization pipeline; explore different optimization orders since they can lead to different performance outcomes

## System Architecture

```
                         ┌──────────────┐
                         │ Orchestrator │
                         └──────┬───────┘
                                │ coordinates all agents,
                                │ decides optimization order
                ┌───────────────┼───────────────┐
                │               │               │
       ┌────────▼──────┐ ┌─────▼──────┐ ┌──────▼───────┐
       │   Workload    │ │    Code    │ │   Storage/   │
       │   Analyzer    │ │  Generator │ │Index Designer│
       └───────────────┘ └────────────┘ └──────────────┘
                │               │               │
                │        ┌──────▼───────┐       │
                └───────►│   Operator   │◄──────┘
                         │  Specialist  │
                         └──────┬───────┘
                                │
                ┌───────────────┼───────────────┐
                │                               │
         ┌──────▼───────┐               ┌───────▼──────┐
         │  Evaluator   │──────────────►│   Learner    │
         └──────────────┘  feedback     └──────────────┘
```

| Agent | Role |
|-------|------|
| **Orchestrator** | Coordinates the pipeline, decides what to optimize next, passes optimization target and knowledge base to all agents |
| **Workload Analyzer** | Parses SQL workload to extract patterns, selectivities, join graphs, cardinality estimates, and optimization opportunities |
| **Code Generator** | Produces executable C++ code guided by storage design and knowledge base; external libraries allowed |
| **Storage/Index Designer** | Designs data layouts, encodings, compression, and index structures by reasoning about the specific workload |
| **Operator Specialist** | The creative engine — implements any technique (SIMD, custom hash tables, operator fusion, parallel execution) to optimize physical operators |
| **Evaluator** | Benchmarks generated code for correctness and performance, optionally profiles with `perf stat` |
| **Learner** | Reads knowledge base to diagnose bottlenecks; proposes both known and novel optimization techniques |

## Knowledge Base

Agents have access to a structured knowledge base (`src/gendb/knowledge/`) with 27 technique files across 8 domains:

| Domain | Topics |
|--------|--------|
| **storage** | Columnar vs row, compression, memory layout, string optimization |
| **indexing** | Hash indexes, sorted indexes, zone maps, bloom filters |
| **query-execution** | Vectorized execution, operator fusion, compiled queries, pipeline breakers |
| **joins** | Hash join variants, sort-merge join, join ordering |
| **aggregation** | Hash aggregation, sorted aggregation, partial aggregation |
| **parallelism** | SIMD, thread parallelism, data partitioning |
| **data-structures** | Compact hash tables, arena allocation, flat structures |
| **external-libs** | jemalloc/tcmalloc, abseil/folly, I/O libraries |

Each file contains: what the technique is, when to use it, C++ implementation patterns, performance characteristics, real-world examples from production databases, and common pitfalls.

## Optimization Strategy

GenDB does **not** follow a fixed optimization pipeline. Instead:

1. The **Evaluator** profiles the current generated code and identifies bottlenecks
2. The **Learner** reads relevant knowledge base files, diagnoses root causes, and proposes optimizations — including novel techniques not in the knowledge base
3. The **Orchestrator Agent** reviews recommendations and strategically selects which to apply, considering optimization history and remaining iteration budget
4. The **Operator Specialist** implements selected optimizations, consulting the knowledge base for implementation patterns
5. Different optimization orders are explored, since applying the same optimizations in different sequences can yield different performance

## Benchmarks

- **Primary benchmark**: TPC-H
- **Comparison baselines**: PostgreSQL, DuckDB
- Goal: generate workload-specific code that matches or outperforms general-purpose systems on targeted workloads
