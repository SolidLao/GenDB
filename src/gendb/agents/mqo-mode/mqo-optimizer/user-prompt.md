# MQO Optimizer — Iteration {{iteration}}

## Current Performance
- Batch total (dispatcher_total): **{{baseline_batch_ms}} ms**
- Best batch total so far: {{best_batch_ms}} ms (iteration {{best_iteration}})
- Consecutive non-improving iterations: {{stall_count}} / {{stall_threshold}}

{{#if benchmark_context}}
{{benchmark_context}}
{{/if}}

## Hardware Configuration
- CPU cores: {{cpu_cores}}
- L3 cache: {{l3_cache_mb}} MB
- Total memory: {{total_memory_gb}} GB

## Current Performance Profile

Path: `{{profile_path}}`
**Read this file first.**

Top regions by total_ms (from the profile):

{{hotspots_section}}

Batch total from profile (dispatcher_total): {{dispatcher_total_ms}} ms

## Optimization History

{{history_summary}}

**Do NOT repeat approaches that were already rejected.** Choose a different target or technique.

## Artifact source tree

Root: `{{mqo_dir}}`
- Main source:        `{{mqo_dir}}/mqo_main.cpp`
- Stage headers:      `{{mqo_dir}}/stages/*.hpp`
- Blueprint:          `{{blueprint_path}}`
- Skeleton:           `{{skeleton_path}}`

## Acceptance criteria (the orchestrator applies these after your edit)

- Correctness: no per-query CSV must differ from ground truth.
- Minimum relative batch-total improvement for acceptance: **{{min_improvement}}** (e.g., 0.02 = 2%).
- Maximum per-query regression slack for shared/dispatcher edits: **{{regression_slack}}** (e.g., 0.05 = 5%).
- If your edit does not improve batch_total_ms by ≥ `{{min_improvement}}` AND does not regress any query by > `{{regression_slack}}`, it will be rolled back.

## Your task

1. Read `{{profile_path}}` and the source code for the top-hotspot region(s).
2. Follow the Diagnostic Framework (Steps 1-4) from your system prompt:
   - Step 1: WHERE is time spent? (profile regions, history of failed approaches)
   - Step 2: WHY is it slow? (Q1: plan wrong? Q2: implementation/hardware mismatch? Q3: wasted work?)
   - Step 3: WHAT fix? (Category A/B/C/D)
   - Step 4: Apply the minimal targeted edit
3. Write `{{edit_scope_path}}` with the structured scope declaration (include diagnosis).
4. Write `{{edit_rationale_path}}` with a 200-500 word explanation.

Do NOT run the binary or `make`. Leave that to the orchestrator.
