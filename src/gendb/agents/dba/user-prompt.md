{{#if stage_a}}
# Stage A: Pre-generation analysis

## Workload Analysis
Read from: {{workload_analysis_path}}

## Storage Design
Read from: {{storage_design_path}}

## Queries
Read from: {{queries_path}}

## Utility Library
Headers at: {{utils_path}}
Files: date_utils.h, timing_utils.h

## Experience Skill
Read and update: {{experience_path}}

Review all queries, predict correctness and performance risks,
extend utility library if gaps found, add workload-specific warnings to experience skill.
Compile any modified headers: g++ -c -std=c++17 -fsyntax-only <header>
{{/if}}

{{#if stage_b}}
# Stage B: Post-run retrospective

## Run Directory
{{run_dir}}

## Retrospective Output Directory
Write to: {{retro_dir}}/

Review all execution results (execution_results.json) and optimization histories
(optimization_history.json) under {{queries_dir}}/

For each query: classify as SUCCESS (correct + fast), SLOW (correct but slow),
or FAILED (incorrect results).

Write:
1. {{retro_summary_path}} — High-level findings
2. {{retro_proposals_path}} — Structured improvement proposals

## Experience Skill
Read and evolve: {{experience_path}}
Update frequency counters, add new entries for discovered issues,
consolidate and cap at top-50 entries by severity × frequency × recency.
{{/if}}
