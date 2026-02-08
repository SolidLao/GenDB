# GenDB: Generative Database System

A multi-agent LLM system that generates customized database execution code for user-provided SQL workloads — no pre-built DBMS required.

## Overview

GenDB takes a different approach to query execution: instead of routing queries through a general-purpose DBMS, it uses a team of LLM agents to analyze your specific SQL workload and database, then generates tailored execution code optimized for that exact use case. The input is a SQL workload and dataset; the output is a standalone, high-performance execution engine specialized to your needs.

## Key Ideas

- **Workload-specific code generation** — generate execution code tuned to the actual queries and data, not a one-size-fits-all engine
- **Multi-agent architecture** — seven specialized agents collaborate to analyze, generate, optimize, and evaluate
- **Incremental generation** — start with a working baseline, then iteratively optimize based on profiling feedback
- **Multi-language output** — generate code in the best-fit language (C++, Rust, etc.) for each component
- **Learn from existing systems** — study how mature DBMS implementations (PostgreSQL, DuckDB) solve problems, then adapt those strategies into generated code
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
| **Orchestrator** | Coordinates the pipeline, decides what to optimize next based on profiling results, explores different optimization orders |
| **Workload Analyzer** | Parses SQL workload and dataset to extract patterns, selectivities, join graphs, and access characteristics |
| **Code Generator** | Produces executable code from query plans, selecting appropriate language and compilation strategy |
| **Storage/Index Designer** | Designs data layouts, storage formats, and index structures tailored to the workload |
| **Operator Specialist** | Implements physical operators (joins, aggregations, sorts) optimized for the specific data and query patterns |
| **Evaluator** | Benchmarks generated code for correctness and performance, produces profiling data |
| **Learner** | Analyzes evaluation results and past attempts to guide the next round of optimization |

## Optimization Strategy

GenDB does **not** follow a fixed optimization pipeline. Instead:

1. The **Evaluator** profiles the current generated code and identifies bottlenecks
2. The **Orchestrator** reviews profiling data and decides which component to optimize next
3. Optimization hints/tips are available at each stage (storage layout, operator selection, code generation), but the **order** in which they are applied is dynamic
4. Different optimization orders are explored, since applying the same optimizations in different sequences can yield different performance
5. The **Learner** accumulates knowledge across iterations to make increasingly informed decisions

## Benchmarks

- **Primary benchmark**: TPC-H
- **Comparison baselines**: PostgreSQL, DuckDB
- Goal: generate workload-specific code that matches or outperforms general-purpose systems on targeted workloads
