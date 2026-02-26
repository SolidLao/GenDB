# GenDB: Generative Database System

A multi-agent LLM system that generates customized database execution code for user-provided SQL workloads — no pre-built DBMS required.

## Overview

GenDB takes a different approach to query execution: instead of routing queries through a general-purpose DBMS, it uses a team of LLM agents to analyze your specific SQL workload and database, then generates tailored C++ execution code optimized for that exact use case. The input is a SQL workload and dataset; the output is a standalone, high-performance execution engine specialized to your needs.

## Key Ideas

- **Workload-specific code generation** — generate execution code tuned to the actual queries and data, not a one-size-fits-all engine
- **Two operating modes** — default mode (5-agent, no skills) relies on LLM reasoning alone; skills mode (7-agent, `--use-skills`) adds domain skills and Code Inspector
- **Adaptive thinking** — each agent has structured thinking discipline (concise, phase-based reasoning) with configurable effort levels and model escalation (Sonnet → Opus) on correctness failures
- **Plan-first pipeline** — Query Planner designs lean JSON execution plans, Code Generator implements them, Optimizer can modify both plan and code
- **MVCC-style column versions** — optimizer can build derived column representations at storage level, breaking optimization ceilings caused by initial encoding choices
- **Multi-run execution** — each query runs 3 times per iteration in either hot mode (all cached) or cold mode (OS cache cleared before each run)
- **Adaptive iteration budget** — stall detection, correctness caps, competitive baseline checks, and model escalation drive when to stop or intensify optimization
- **True per-query pipelining** — each query flows independently through its pipeline stages, gated by semaphore for LLM calls and serialized for execution

## Operating Modes

### Default Mode (no skills, 5 agents)

The default mode (`useSkills: false`, `useDba: false`) uses a lean 5-agent pipeline. Agents rely solely on their identity prompts, query guides, and LLM reasoning — no external skills or knowledge base.

```
Phase 1: Workload Analyzer → Storage/Index Designer → Per-Query Guides (Qi_guide.md)
Phase 2: Query Planner → Code Generator → Execute → [Optimizer → Execute]*
```

Agents in default mode: Workload Analyzer, Storage Designer, Query Planner, Code Generator, Query Optimizer.

Code Inspector and DBA are **skipped**. The `Skill` tool is removed from agents' allowed tools.

### Skills Mode (`--use-skills`, 7 agents)

Skills mode adds the Code Inspector and DBA agents, and loads domain skills via the Claude Code skill system.

```
Phase 1: Workload Analyzer → Storage/Index Designer → [DBA Stage A] → Per-Query Guides
Phase 2: Query Planner → Code Generator → Inspector → Execute → [Optimizer → Inspector → Execute]*
Phase 3: DBA Stage B → Retrospective + experience evolution
```

Each agent gets a 4-layer prompt: (1) identity prompt, (2) experience skill (always loaded), (3) domain skills (loaded on demand), (4) user prompt (task context via templates).

Enable with: `--use-skills` (and optionally `--dba-stage-a` for DBA Stage A).

## Performance

### TPC-H SF10

![TPC-H SF10 Benchmark Results](benchmarks/tpc-h/results/sf10/figures/benchmark_results_combined.png)

### SEC-EDGAR (3 Years, 5GB)

![SEC-EDGAR SF3 Benchmark Results](benchmarks/sec-edgar/results/sf3/figures/benchmark_results_combined.png)

## Agents

| Agent | Default Mode | Skills Mode | Role |
|-------|-------------|-------------|------|
| **Workload Analyzer** | Yes | Yes | Parse SQL workload, detect hardware, sample data |
| **Storage Designer** | Yes | Yes | Design storage/indexes, generate + run ingestion, per-query guides |
| **DBA** | No | Yes | Pre-gen risk analysis (Stage A), post-run retrospective + experience evolution (Stage B) |
| **Query Planner** | Yes | Yes | Design lean JSON execution plan |
| **Code Generator** | Yes | Yes | Implement plan in C++, compile + run + validate |
| **Code Inspector** | No | Yes | Review code against experience skill + Query Guide |
| **Query Optimizer** | Yes | Yes | Targeted edits to plan/code; can build derived column versions |

All agents use Sonnet by default. On repeated correctness failures, the orchestrator escalates to Opus.

## Skills (Skills Mode Only)

Domain skills (`.claude/skills/`) are loaded selectively per query and agent:

| Skill | Purpose |
|-------|---------|
| `experience` | Always loaded. Correctness + performance rules with frequency/severity. |
| `data-structures` | When to use hash tables vs bloom filters vs direct arrays vs sorted arrays |
| `join-optimization` | Join strategies, pre-built index usage, sampling |
| `scan-optimization` | Predicate pushdown, late materialization, zone maps |
| `aggregation-optimization` | Hash/sorted/parallel aggregation patterns |
| `hash-tables` | Open-addressing, Robin Hood, bounded probing templates |
| `data-loading` | mmap, madvise, cold/hot I/O tradeoffs |
| `indexing` | Zone maps, hash indexes, construction guidelines |
| `parallelism` | Morsel-driven, OpenMP, SIMD, thread-local patterns |
| `gendb-storage-format` | Binary column format, type mappings, encodings |
| `gendb-code-patterns` | File structure, GENDB_PHASE, mmap pattern, compilation |
| `research-papers` | 30+ seminal paper references by topic |

## Project Structure

```
src/gendb/
  orchestrator.mjs          # Main pipeline orchestration
  config.mjs                # Configuration loader
  gendb.config.mjs          # Hyperparameters (models, timeouts, effort levels)
  agents/                   # 7 agents (prompt.md + index.mjs + user-prompt.md each)
  utils/                    # System utilities (date_utils.h, timing_utils.h, paths.mjs)
  tools/                    # compare_results.py and other tooling

.claude/skills/             # Domain skills (used in skills mode)

benchmarks/
  tpc-h/                    # TPC-H benchmark (queries, results, ground truth)
  sec-edgar/                # SEC-EDGAR financial statements benchmark

output/<workload>/<timestamp>/  # Per-run output with iteration history
```

## Usage

```bash
# Default mode (5-agent, no skills)
node src/gendb/orchestrator.mjs --benchmark tpc-h --sf 10

# Skills mode (7-agent, with domain skills + Code Inspector + DBA)
node src/gendb/orchestrator.mjs --benchmark tpc-h --sf 10 --use-skills

# Hot optimization (default), cold optimization
node src/gendb/orchestrator.mjs --benchmark tpc-h --optimization-target hot
node src/gendb/orchestrator.mjs --benchmark tpc-h --optimization-target cold

# Control optimization iterations and concurrency
node src/gendb/orchestrator.mjs --max-iterations 5 --stall-threshold 5 --max-concurrent 22
```
