# GenDB: Generative Database System

A multi-agent LLM system that generates customized database execution code for user-provided SQL workloads — no pre-built DBMS required.

## Overview

GenDB takes a different approach to query execution: instead of routing queries through a general-purpose DBMS, it uses a team of LLM agents to analyze your specific SQL workload and database, then generates tailored execution code optimized for that exact use case. The input is a SQL workload and dataset; the output is a standalone, high-performance execution engine specialized to your needs.

## Key Ideas

- **Workload-specific code generation** — generate execution code tuned to the actual queries and data, not a one-size-fits-all engine
- **7-agent architecture** — Workload Analyzer, Storage Designer, DBA, Query Planner, Code Generator, Code Inspector, Query Optimizer
- **4-layer prompt architecture** — each agent has: (1) identity prompt, (2) experience skill (always loaded), (3) domain skills (loaded on demand), (4) user prompt (task context via templates)
- **Skill system** — 12 concise domain skills replace 44 knowledge files. Skills are loaded selectively based on query characteristics and agent role, reducing token consumption ~40-50%.
- **Plan-first pipeline** — Query Planner designs lean JSON execution plans, Code Generator implements them, Optimizer can modify both plan and code
- **Configurable hot/cold optimization** — `optimizationTarget: "hot"` (default) optimizes avg(hot runs) only; `"cold"` optimizes cold run. Eliminates joint optimization bias.
- **Timeout-resilient code generation** — orchestrator always calls `executeQuery()` as safety net, surviving agent timeouts
- **Process group lifecycle management** — all spawned processes run in detached process groups; killed via `process.kill(-pid)` on timeout
- **Multi-run cold+hot execution** — each query runs 3 times: 1 cold (OS cache cleared) + 2 hot (cached)
- **User prompt templates** — externalized as `agents/*/user-prompt.md` with `{{placeholder}}` syntax, replacing inline string construction
- **Lean I/O contracts** — plan.json ~35 lines (vs ~150), execution_results.json trimmed of duplicated fields
- **Experience evolution** — DBA Stage B updates experience skill with frequency/severity metadata, capped at top-50 entries
- **Correctness anchors** — validated constants extracted from passing code, made immutable during optimization
- **Adaptive iteration budget** — stall detection after 2 consecutive non-improving iterations with 3x gap
- **Code Inspector** — pre-execution review against experience skill + Query Guide
- **True per-query pipelining** — each query flows independently through Planner→Coder→Inspector→Execute→[Optimizer→Inspector→Execute]*

## System Architecture

**Phase 1: Offline Data Storage + Pre-Generation Analysis**
```
Workload Analyzer → Storage/Index Designer → Per-Query Guides (Qi_guide.md) → [DBA Stage A (optional)]
```

**Phase 2: Online Per-Query Pipeline-Parallel Optimization**
```
Each query runs independently (true pipelining):
  Iter 0: Query Planner → Code Generator (compile+run+validate) → Inspector → Execute
  Iter 1+: [shouldContinue() → Optimizer → Inspector → Execute]* (adaptive budget)
```

**Phase 3: Post-Run Retrospective**
```
DBA Stage B → Review results, evolve experience skill, write retrospective
```

## Agents

| Agent | Model | Phase | Role |
|-------|-------|-------|------|
| **Workload Analyzer** | Sonnet | 1 | Parse SQL workload, detect hardware, sample data |
| **Storage Designer** | Sonnet | 1 | Design storage, generate + run ingestion, per-query guides |
| **DBA** | Sonnet | 1 + 3 | Pre-gen risk analysis (Stage A), post-run retrospective + experience evolution (Stage B) |
| **Query Planner** | Sonnet | 2 | Iter 0: design lean JSON execution plan with domain skills |
| **Code Generator** | Sonnet | 2 | Iter 0: implement plan in C++, compile + run + validate |
| **Code Inspector** | Sonnet | 2 | Review code against experience skill + Query Guide |
| **Query Optimizer** | Sonnet | 2 | Iter 1+: targeted edits to plan/code with domain skills |

## Skills

Domain skills (`src/gendb/skills/`) replace the old knowledge base. Loaded selectively per query and agent:

| Skill | Purpose |
|-------|---------|
| `experience.md` | Always loaded. Correctness + performance rules with frequency/severity. |
| `join-optimization.md` | Join strategies, pre-built index usage, sampling |
| `scan-optimization.md` | Predicate pushdown, late materialization, zone maps |
| `aggregation-optimization.md` | Hash/sorted/parallel aggregation patterns |
| `hash-tables.md` | Open-addressing, Robin Hood, bounded probing templates |
| `data-loading.md` | mmap, madvise, cold/hot I/O tradeoffs |
| `indexing.md` | Zone maps, hash indexes, construction guidelines |
| `parallelism.md` | Morsel-driven, OpenMP, SIMD, thread-local patterns |
| `gendb-storage-format.md` | Binary column format, type mappings, encodings |
| `gendb-code-patterns.md` | File structure, GENDB_PHASE, mmap pattern, compilation |
| `research-papers.md` | 30+ seminal paper references by topic |

## System Utilities

Shared C++ headers in `src/gendb/utils/`:

| Header | Purpose |
|--------|---------|
| `date_utils.h` | O(1) date extraction, epoch<->string conversion |
| `timing_utils.h` | GENDB_PHASE("name") RAII scoped timer macro |