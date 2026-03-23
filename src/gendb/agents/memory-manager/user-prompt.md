# Task: Post-Run Differential Learning (Memory v3)

## Run Information
- Run directory: {{run_dir}}
- Run ID: {{run_id}}
- Benchmark: {{benchmark}}
- Scale factor: {{scale_factor}}

## Memory Directory
{{memory_dir}}

## Skills Directory
.claude/skills/

## Current Graph Stats
{{graph_stats}}

## Instructions

1. Read `{{run_dir}}/run.json` for run metadata
2. List all query directories in `{{run_dir}}/queries/`
3. For each query:
   a. Read `optimization_history.json` — extract the full trajectory
   b. Read the best iteration's `.cpp` file (from bestCppPath in run.json)
   c. Read the best iteration's `plan.json` if available
   d. **Analyze the trajectory**:
      - Identify **breakthroughs**: iterations where timing improved >30%
      - Identify **regressions**: iterations where timing got worse
      - Classify as: first-iteration success (low priority) or multi-iteration discovery (high priority)
   e. Compare against existing L0 nodes:
      - **NOVEL_SUCCESS**: No existing L0 for this query template
      - **SIGNIFICANT_IMPROVEMENT**: >{{differential_threshold}}% faster than existing L0
      - **FAMILIAR**: Matches existing knowledge and no new insights → skip

4. For non-FAMILIAR queries:
   - Create/update L0 node (consolidated best across runs)
   - Update L1 template if strategies changed

5. **Extract non-obvious knowledge** from breakthroughs:
   - Diff the C++ code between breakthrough iterations
   - Identify the key technique that caused the improvement
   - Generalize with placeholders (benchmark-agnostic)
   - Create or update skills in `.claude/skills/<skill-name>/` (flat, no category subdirs):
     - L3 operator techniques (e.g., `bitset-semi-join`)
     - L2 recurring operator combinations (e.g., `scan-filter-date`)
     - L4 high-level decision guides (e.g., `cache-conscious-aggregation`)
     - L5 cross-cutting principles (e.g., `bandwidth-vs-compute`)
   - For each skill: create SKILL.md, code-patterns/, evidence.json, gotchas.md

6. **Record anti-patterns** from regressions in relevant skill's `gotchas.md`

7. **Synthesize L2 patterns**: Look for L3 operator co-occurrence across L1 queries. Create new L2 pattern skills for recurring combinations.

8. Write summary to: {{summary_path}}

{{#if skill_usage}}
## Skill Usage from This Run
The following skills were invoked by agents during this run's query optimization.
Update each skill's evidence.json with usage stats.

{{skill_usage}}
{{/if}}

{{#if existing_skills}}
## Existing Skills (check before creating new ones)
{{existing_skills}}
{{/if}}

{{#if existing_templates}}
## Existing L1 Templates (for matching)
{{existing_templates}}
{{/if}}

## L0 Node Schema
```json
{
  "id": "L0_<benchmark>_<queryId>_sf<N>_<date>",
  "layer": 0,
  "content": {
    "query_id": "Q1",
    "sql": "SELECT ...",
    "benchmark": "tpc-h",
    "scale_factor": 10,
    "tables_accessed": ["lineitem"],
    "best_timing_ms": 43.65,
    "best_cpp_path": "/absolute/path/to/best.cpp",
    "key_optimizations": ["compact uint8 columns", "integer arithmetic"],
    "optimization_trajectory": [
      {"iter": 0, "timing_ms": 80, "strategy": "initial", "pass": true, "regression": false}
    ],
    "template_sql": "SELECT ... WHERE col BETWEEN :start_date AND :end_date",
    "param_schema": {},
    "hardware": "64-core, 376GB RAM"
  },
  "tags": ["<benchmark>"]
}
```

## Skill Reference Node Schema (for L2-L5 in HAG)
```json
{
  "id": "L3_bitset_semi_join",
  "layer": 3,
  "skill_path": ".claude/skills/bitset-semi-join/",
  "summary": "Atomic bitset replacing hash table for semi-joins",
  "content": { "description": "..." }
}
```

## Summary Format
```json
{
  "run_id": "...",
  "classifications": {"NOVEL_SUCCESS": 0, "SIGNIFICANT_IMPROVEMENT": 0, "FAMILIAR": 0},
  "nodes_created": 0,
  "nodes_updated": 0,
  "edges_created": 0,
  "skills_created": [],
  "skills_updated": [],
  "breakthroughs_found": 0,
  "regressions_recorded": 0
}
```
