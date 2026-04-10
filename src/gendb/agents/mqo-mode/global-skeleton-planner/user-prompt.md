# Global Skeleton Planner — Invocation

Build the batch execution skeleton for this MQO run.

## Shared-component blueprint (from MQO Analyzer)

Path: `{{blueprint_path}}`
**Read this file first via the Read tool.** It contains the shared components you must schedule and the `per_query_dependencies` map.

## Workload analysis
Path: `{{workload_analysis_path}}`

## Storage design
Path: `{{storage_design_path}}`

## Queries in this batch

{{query_id_list}}

## Output

Write the skeleton to this exact path using the Write tool:

```
{{skeleton_output_path}}
```

Then return a short (< 200 word) summary of the skeleton.
