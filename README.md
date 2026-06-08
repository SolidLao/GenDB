# GenDB: An LLM-Powered Generative Query Engine

**Instance-optimized query execution code generation tailored to your specific data, workloads, and hardware.**

> **2.8x** faster than DuckDB and Umbra on TPC-H &nbsp;|&nbsp; **5.0x** faster than DuckDB on real-world financial data &nbsp;

**[Website](https://solidlao.github.io/GenDB/)** &nbsp;|&nbsp; **[Paper](https://arxiv.org/abs/2603.02081)** &nbsp;|&nbsp; **[GitHub](https://github.com/SolidLao/GenDB)**

> **Try the [Interactive Demo, VLDB 2026](https://solidlao.github.io/GenDB/demo/)** — explore GenDB's full pipeline with a guided audio walkthrough. See how six AI agents analyze data, design storage, plan execution, generate code, and optimize it step by step. Browse pre-computed results, write your own queries, or upload your own data.

<p align="center">
  <img src="assets/GenDB.png" width="700" alt="GenDB System Overview">
</p>

**Input:** database schema, SQL queries, data files, hardware specs, and user configurations (optimization targets, iteration budgets). **Pipeline:** five specialized LLM agents — Workload Analyzer, Storage/Index Designer, Query Planner, Code Generator, and Query Optimizer — each equipped with tools for file I/O, terminal access, and web search, collaborating through structured JSON. **Output:** optimized storage structures, indexes, and one standalone executable per query — all tailored to the specific data, workload, and hardware.

> **Note:** This project is under active development. The stable version used for the [arxiv paper](https://arxiv.org/abs/2603.02081) is available in the [`arxiv-03-02-2026`](https://github.com/SolidLao/GenDB/tree/arxiv-03-02-2026) branch.

## News

- **2026-05-31** - [GenDB Demo](https://solidlao.github.io/GenDB/demo/) has been accepted to **VLDB 2026!**

- **2026-03-25** — **[Interactive Demo](https://solidlao.github.io/GenDB/demo/)** :sparkles: — The best way to understand GenDB. A guided audio walkthrough takes you through the full pipeline on TPC-H Q3: from SQL parameterization, data and hardware profiling, storage design, query planning, code generation, to iterative optimization (593ms → 27ms, 22× speedup). The GitHub Pages deployment includes pre-computed results for TPC-H and SEC-EDGAR. For real-time code generation on new queries or your own data, deploy the demo server with GenDB on your machine (`node src/demo/server.mjs`).
- **2026-03-23** — **SQL Template-Native Generation & Component Reuse.** GenDB now extracts parameterized SQL templates from queries and generates code at the template level. Queries sharing the same template structure (e.g., different date ranges or filter predicates) reuse the same generated code — no re-generation needed. Storage, indexes, and optimized query binaries are versioned and persisted across runs, with hardware fingerprinting to trigger automatic rebenchmarking when hardware changes. This enables incremental workload onboarding: run 5 queries today, add 17 more tomorrow, and GenDB only generates code for the new ones.
- **2026-03-23** — **Self-Evolving Memory System (experimental).** GenDB can now learn from past runs via a 6-layer Hierarchical Abstraction Graph and auto-generated optimization skills. This feature is still under active development and testing, and is disabled by default. Enable with `--memory-dir <path>`. See `src/gendb/memory/README.md`.
- **2026-03-09** — **Language comparison.** Compared GenDB's C++ output with Optimized C++ and Rust rewrites by Claude Code. See [results](docs/language-comparison.md).
- **2026-03-08** — **Multi-model support.** Added Opus 4.6, GPT-5.4, GPT-5.3-Codex as backbone models with [comparison results](docs/model-comparison.md).
- **2026-03-08** — **[Website](https://solidlao.github.io/GenDB/)** launched.
- **2026-03-07** — **OpenAI Codex agent provider** added (`--agent-provider codex`).
- **2026-03-02** — **[Paper](https://arxiv.org/abs/2603.02081)** released on arXiv.

## Why Generate?

Consider PostgreSQL. Since its origins as an OLTP database, the community has built extensions for nearly every emerging use case: PostGIS for geospatial, TimescaleDB for time-series, pgvector for embeddings, Citus for distributed analytics, pg_duckdb for OLAP, AGE for graph queries, and [hundreds more](https://pigsty.io/blog/pg/pg-eat-db-world/). In parallel, entirely new systems were purpose-built for each domain: DuckDB, ClickHouse, and Umbra for OLAP; Milvus and Pinecone for vector search; InfluxDB for time-series; Neo4j for graph; Snowflake and BigQuery for cloud analytics.

Each extension is [difficult](https://www.vldb.org/pvldb/vol18/p1962-kim.pdf) and fights the host system's architectural constraints :weary:. Each new system requires years of engineering :wrench: and significant monetary costs :moneybag:. And with every emerging technique — multimodal AI operators, GPU-native analytics, learned indexes — the cycle repeats :tired_face:.

**But actually there is a third option:** use LLMs to **generate per-query execution code**.

- **Performance** — Instance-optimized code exploits exact data distributions, join selectivities, group cardinalities, and hardware characteristics. No general-purpose engine can match this.
- **Extensibility** — Integrating new techniques requires *prompting*, not re-engineering a complex codebase. [Semantic queries](https://sembench.org), [GPU-native code](https://vldb.org/cidrdb/papers/2026/p12-yogatama.pdf) — all reachable through prompt updates.
- **Economics** — In production, [80% of queries repeat in 50% of clusters](https://www.vldb.org/pvldb/vol17/p3694-saxena.pdf), and long-running queries almost always recur as they correspond to regular transformation or analytical tasks. Generation cost is amortized over many executions.

## Current Landscape

[Paper](https://arxiv.org/pdf/2603.02081)

GenDB supports multiple LLM agent providers — currently **Claude** (via `@anthropic-ai/claude-agent-sdk`) and **OpenAI Codex** (via `@openai/codex-sdk`) — and is designed for easy extension to additional providers. It is evaluated on OLAP workloads (TPC-H, SEC-EDGAR). For developers, it automatically generates instance-optimized execution code whose correctness can be verified by manual inspection. For users without an SQL background, GenDB can be extended with a natural language interface, similar to conversational analytics services already deployed in production.

**When to use GenDB today.** GenDB is well suited for recurring workloads where upfront generation cost pays off over many executions. For ad-hoc queries, GenDB can be combined with a traditional DBMS in a hybrid architecture: the traditional system handles one-off queries, while GenDB accelerates frequent ones. As LLMs become faster and cheaper, we expect the generation overhead to shrink — the long-term target is per-query generation in seconds at minimal cost, making the hybrid boundary increasingly irrelevant.

**What's next.** We are actively developing and will continually update GenDB with support for more scenarios:

- **Semantic queries** — Generate code for multimodal data (images, audio, text) with AI-powered operators, moving beyond SQL's relational model
- **GPU-native generation** — Generate CUDA/GPU code targeting libcudf and cost-efficient GPU analytics, not just CPU
- **Self-evolving system** — Learn from past runs, accumulate optimization experience, improve generation quality over time (experimental support available via `--memory-dir`)

## Results

We evaluate on two benchmarks: **TPC-H**, a widely-used OLAP benchmark whose queries and optimization strategies are well-represented in LLM training data, and **SEC-EDGAR**, a new benchmark we constructed from real-world SEC financial filings. SEC-EDGAR serves as an unseen workload — its schemas and query patterns have rarely appeared in training corpora — to test whether GenDB generalizes beyond memorized optimizations.

**Main experiment runs:**
- **TPC-H:** `output/deprecated/tpc-h/2026-02-26T06-27-28`
- **SEC-EDGAR:** `output/deprecated/sec-edgar/2026-02-26T07-58-24`

**Latest runs (SQL template-native generation):**
- **TPC-H:** `output/tpc-h-sf10-3.19`
- **SEC-EDGAR:** `output/sec-edgar-sf3-3.19`

> **Note:** The `storage/` (and legacy `gendb/`) data folders within each run directory are excluded from this repository due to their large size (12GB for TPC-H, 3.3GB for SEC-EDGAR). As a result, the output runs cannot be directly executed from a fresh clone. To reproduce or re-run, download the storage folders from [Google Drive](https://drive.google.com/drive/folders/14FICOHv5MnEf_qedsvyGfj64ay8mMTnv?usp=sharing) and place them in the corresponding run directories (e.g., `output/tpc-h-sf10-3.19/storage/`).

**All engines are configured to use comparable hardware resources, and parallelism is fully enabled to ensure each system can fully demonstrate its performance. To ensure fair comparison, result or intermediate result caching, or pre-computed derived columns, are not allowed in GenDB.** GenDB outperforms all baselines on every query in both benchmarks.

| | GenDB | DuckDB | Umbra | ClickHouse | MonetDB | PostgreSQL |
|---|---|---|---|---|---|---|
| **TPC-H** | **214 ms** | 594 ms | 590 ms | 2,289 ms | 1,416 ms | 52,473 ms |
| **SEC-EDGAR** | **328 ms** | 1,549 ms | 1,280 ms | 2,047 ms | 28,096 ms | 41,634 ms |

**TPC-H (SF10):** 2.8x faster than DuckDB/Umbra. Up to 6.1x on complex multi-way joins (Q9). 11.2x faster than ClickHouse.

**SEC-EDGAR (5GB financial data):** 5.0x faster than DuckDB, 3.9x faster than Umbra. The performance gap widens on this unseen benchmark, confirming that GenDB's advantage comes from instance-level optimization rather than memorized strategies.

#### TPC-H SF10
![TPC-H SF10 Benchmark Results](benchmarks/figures/tpc-h/benchmark_results_combined.png)

#### SEC-EDGAR (3 Years, 5GB)
![SEC-EDGAR Benchmark Results](benchmarks/figures/sec-edgar/benchmark_results_combined.png)

**More results:** [Ablation: Multi-Agent vs Single-Agent](docs/ablation.md) &nbsp;|&nbsp; [Model Comparison](docs/model-comparison.md) &nbsp;|&nbsp; [Language Comparison: C++ vs Optimized C++ vs Rust](docs/language-comparison.md)

## How It Works

Five specialized LLM agents collaborate through a structured pipeline:

1. **Workload Analyzer** — Profiles hardware (cache hierarchy, cores, SIMD), samples data, extracts workload characteristics (join patterns, selectivity, group cardinalities)
2. **Storage/Index Designer** — Designs and builds optimized storage layouts with encoding, compression, and indexes (zone maps, hash indexes, bloom filters)
3. **Query Planner** — Generates resource-aware execution plans adapted to data and hardware
4. **Code Generator** — Implements the plan as optimized native code with system-level optimizations (memory-mapped I/O, SIMD, parallelism)
5. **Query Optimizer** — Iteratively refines code using runtime feedback (TPC-H Q18: 12s to 74ms in one iteration — **163x**)

**Instance-optimized code in action** — As one example of many possible optimizations, consider how GenDB adapts aggregation strategy based on group cardinality and cache topology. A general-purpose engine uses the same hash aggregation for every query. GenDB reasons about the specific workload and hardware (in this case: 32KB L1 / 1MB L2 / 44MB shared L3, 64 cores) to generate fundamentally different code:

**TPC-H Q1 — 6 groups (fits in L1):** The query aggregates `lineitem` by `returnflag` and `linestatus`, producing only 6 groups. GenDB skips hashing entirely — each thread uses a direct array of 6 accumulators (384 bytes), fitting comfortably in the 32KB L1 cache.
```cpp
// TPC-H Q1: 6 groups × 64B = 384B per thread → fits in 32KB L1
struct alignas(64) Accum { int64_t cnt, sum_qty, sum_price, ...; };
std::array<Accum, 6> local;  // per-thread, no hashing needed
int g = returnflag[i] * 2 + linestatus[i];  // direct index
local[g].cnt++;
local[g].sum_price += price[i];
```

**TPC-H Q3 — 4M groups (needs L3-aware design):** The query joins `customer`, `orders`, and `lineitem` and groups by `orderkey` — producing ~4M distinct groups. Per-thread hash tables would consume ~3GB across 64 threads, far exceeding the 44MB shared L3. GenDB uses a single shared hash table with column-separated layout: keys (16MB) and values (32MB) in separate arrays so probes only touch keys (L3 hit). Lock-free CAS eliminates both locks and a merge phase.
```cpp
// TPC-H Q3: 4M groups, shared table — keys and values separated
int32_t oa_keys[4194304];  // 4M × 4B = 16MB → fits in 44MB L3
double  oa_rev[4194304];   // 4M × 8B = 32MB
uint64_t slot = oa_probe(oa_keys, orderkey);
atomic_add_double(&oa_rev[slot], revenue);  // 64 threads, lock-free CAS
```

This is just one dimension of instance optimization. GenDB similarly adapts join strategies, scan/filter techniques, storage layouts, index selection, and parallelism patterns based on the specific data and hardware characteristics of each query.

## Quick Start

### Prerequisites

- **Node.js 18+** and npm
- **g++** with C++17 and OpenMP support (`sudo apt-get install build-essential`)
- **LLM agent access** (one of):
  - **Claude** — `export ANTHROPIC_API_KEY=your_key`, or log in to [Claude Code](https://docs.anthropic.com/en/docs/claude-code) with a Pro/Max/Team/Enterprise subscription plan
  - **Codex** — `export CODEX_API_KEY=your_key` (or `OPENAI_API_KEY`)

### Setup

```bash
# One-command setup: checks prerequisites, installs dependencies, downloads data
bash scripts/setup.sh

# Or customize data scale:
bash scripts/setup.sh --sf 10 --years 3    # TPC-H SF10 (~10GB) + SEC-EDGAR 3 years (~5GB)
bash scripts/setup.sh --skip-data           # Dependencies only, download data later
```

To download data separately:
```bash
bash benchmarks/tpc-h/setup_data.sh 10       # TPC-H at scale factor 10
bash benchmarks/sec-edgar/setup_data.sh 3     # SEC-EDGAR, 3 years (2022-2024)
```

### Operating Modes

GenDB supports four operating modes:

- **Multi-Agent (5 agents)** — Five specialized agents collaborate through a structured pipeline with JSON-based inter-agent communication. Default and best-performing mode. *This is the version used in paper experiments.*
- **Multi-Agent with Skills (7 agents)** — Extends the pipeline with a Code Inspector and DBA agent, each loading curated domain knowledge (join optimization, parallelism patterns, hash table design).
- **Single-Agent Guided** — One agent handles everything end-to-end, with a suggested 4-phase workflow and domain hints.
- **Single-Agent High-Level** — Minimal guidance: only I/O contracts and hard constraints, full freedom in approach.

All modes support `--optimization-target hot` (optimize for cached/warm runs, default) or `--optimization-target cold` (optimize for cold runs with OS cache cleared before each execution).

All modes support `--agent-provider claude` (default) or `--agent-provider codex` to select the underlying LLM agent.

```bash
# Multi-agent (5 agents, default, Claude)
node src/gendb/orchestrator.mjs --benchmark tpc-h --sf 10

# Multi-agent with Codex agent
node src/gendb/orchestrator.mjs --benchmark tpc-h --sf 10 --agent-provider codex --model gpt-5.3-codex

# Multi-agent with domain skills (7 agents)
node src/gendb/orchestrator.mjs --benchmark tpc-h --sf 10 --use-skills

# Single-agent guided
node src/gendb/single.mjs --benchmark tpc-h --sf 10 --single-agent-prompt guided

# Single-agent with Codex
node src/gendb/single.mjs --benchmark tpc-h --sf 10 --agent-provider codex --model gpt-5.3-codex

# Run all GenDB version configurations (multi-agent, single-agent guided/high-level)
bash scripts/ablation.sh

# Benchmark GenDB against baseline systems (DuckDB, Umbra, ClickHouse, etc.)
python3 benchmarks/benchmark.py --benchmark tpc-h --sf 10 --gendb-run output/tpc-h-sf10
```

## Project Structure

```
src/gendb/
  orchestrator.mjs          # Multi-agent pipeline orchestration
  single.mjs                # Single-agent mode entry point
  shared.mjs                # Shared utilities (runAgent dispatcher, templates)
  gendb.config.mjs          # Configuration (pipeline settings + per-provider config)
  providers/                # Agent provider implementations (extensible)
    index.mjs               #   Provider registry and lazy loader
    claude.mjs              #   Claude provider (@anthropic-ai/claude-agent-sdk)
    codex.mjs               #   Codex provider (@openai/codex-sdk)
  agents/                   # Agent prompts and logic
    workload-analyzer/      #   Workload & hardware profiling
    storage-index-designer/ #   Storage layout & index design
    query-planner/          #   Execution plan generation
    code-generator/         #   Native code generation
    query-optimizer/        #   Iterative optimization
    code-inspector/         #   Code review (skills mode)
    dba/                    #   DBA analysis (skills mode)
    memory-manager/         #   Post-run learning (memory mode)
    single-agent/           #   Single-agent prompts
  memory/                   # Self-evolving memory system (experimental)
  tools/                    # Result comparison, template extraction
  utils/                    # C++ utility headers (mmap, hashing, timing, dates)

docs/demo/                  # Interactive demo (static site, GitHub Pages)
  index.html                #   Demo page with control panel, pipeline visualization
  css/                      #   Styles (demo.css, viz.css, tutorial.css, shared.css)
  js/                       #   App logic, pipeline renderer, tutorial controller
    tutorial.js             #     Guided walkthrough with spotlight tour
    tutorial-script.js      #     Tour script data, subtitles, audio mappings
    viz-renderer.js         #     Two-column progressive canvas renderer
    data/                   #     Pre-computed benchmark + visualization data
  generate-audio.mjs        #   Audio narration generator (OpenAI TTS)
  audio/                    #   MP3 narration files (generated via generate-audio.mjs)

src/demo/                   # Demo tooling
  server.mjs                #   Local server for live code generation
  extract-data.mjs          #   Extract benchmark data for demo
  generate-viz.mjs          #   Generate visualization data via LLM
  viz-agent/                #   VIZ generation agent prompt and extractor

benchmarks/
  benchmark.py              # Benchmark GenDB against baseline systems
  tpc-h/                    # TPC-H benchmark (queries, data, ground truth, baselines)
  sec-edgar/                # SEC-EDGAR benchmark (queries, data, ground truth, baselines)
  lib/                      # Benchmark runner, plotting, system configs
  figures/                  # Generated benchmark result figures

scripts/
  setup.sh                  # Environment setup (prerequisites, dependencies, data)
  ablation.sh               # Execute GenDB in all version configurations
  memory_eval.sh            # Memory system evaluation (4 experiments)

.claude/skills/             # Auto-generated optimization skills (memory mode)
assets/                     # Project figures
output/                     # GenDB run outputs (per benchmark, versioned)
  tpc-h-sf10-3.19/          #   Latest TPC-H run (template-native)
  sec-edgar-sf3-3.19/       #   Latest SEC-EDGAR run (template-native)
  deprecated/               #   Pre-template runs (paper experiments)
```

## Citation

```bibtex
@misc{lao2026gendbgenerationqueryprocessing,
      title={GenDB: The Next Generation of Query Processing -- Synthesized, Not Engineered}, 
      author={Jiale Lao and Immanuel Trummer},
      year={2026},
      eprint={2603.02081},
      archivePrefix={arXiv},
      primaryClass={cs.DB},
      url={https://arxiv.org/abs/2603.02081}, 
}
```
