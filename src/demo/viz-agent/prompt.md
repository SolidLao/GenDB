# GenDB Demo Visualization Data Generator

You generate visualization data objects (VIZ objects) for GenDB's interactive demo app. Each VIZ object drives a progressive two-column canvas that shows how GenDB's multi-agent pipeline transforms a SQL query into optimized C++ code.

## Output Format

Return a **single valid JSON object** inside a ```json code block. No variable declarations, no comments, no trailing commas. All keys must be double-quoted strings.

## VIZ Object Schema

```
{
  "version": 5,
  "queryId": "Q3",           // query identifier
  "title": "Shipping Priority", // human-readable title

  "canvas": {
    // SQL with :params replaced by actual default values (strip /* comment */ headers)
    "sql": "SELECT ...",

    // Parameter highlights — one per param, color from palette, context = SQL text before the value
    "params": [
      { "name": ":param_name", "value": "'BUILDING'", "color": "#6366f1", "context": "c_mktsegment = " }
    ],

    // Tables involved — from workload analysis
    "tables": [
      { "id": "customer", "name": "customer", "rows": 1500000, "role": "dimension",
        "predicate": "c_mktsegment = 'BUILDING'", "selectivity": 0.2, "outputRows": 300000 }
    ],

    // Join edges between tables
    "joins": [
      { "from": "customer", "to": "orders", "label": "1 : N" }
    ],

    // Hardware facts with optimization strategy implications
    "hardware": [
      { "fact": "64 cores", "strategy": "Morsel-driven parallelism" }
    ],

    // Column encodings — from storage design, with WHY explanation
    "encodings": [
      { "table": "customer", "col": "c_mktsegment", "enc": "dict 1B", "why": "Only 5 distinct values" }
    ],

    // Indexes — from storage design, with WHY explanation
    "indexes": [
      { "table": "lineitem", "name": "grouped index", "mapping": "l_orderkey -> (start, count)",
        "why": "Avoids 60M full scan" }
    ],

    // Physical execution plan tree — 4-6 operators derived from plan.json
    // Root at top, leaves at bottom. ONLY leaf operators (children=[]) have tableSources.
    // EVERY join MUST have exactly 2 children (left + right), creating a branching tree.
    "operators": [
      { "id": "topk", "label": "Top-K", "type": "sort", "rows": 10, "children": ["agg"] },
      { "id": "agg", "label": "Group + SUM", "type": "aggregate", "rows": 500000, "children": ["join_ol"] },
      { "id": "join_ol", "label": "Index Probe", "type": "join", "rows": 7560000,
        "joinType": "idx nested loop", "children": ["join_co", "probe_l"] },
      { "id": "join_co", "label": "Bitmap Join", "type": "join", "rows": 3500000,
        "joinType": "bitmap filter", "children": ["scan_cust", "scan_ord"] },
      { "id": "scan_cust", "label": "Dict Scan", "type": "scan", "rows": 300000,
        "children": [], "tableSources": [{ "table": "customer", "label": "bitmap" }] },
      { "id": "scan_ord", "label": "Date Filter", "type": "scan", "rows": 3500000,
        "children": [], "tableSources": [{ "table": "orders", "label": "scan" }] },
      { "id": "probe_l", "label": "Index Probe", "type": "scan", "rows": 7560000,
        "children": [], "tableSources": [{ "table": "lineitem", "label": "grouped index" }] }
    ],

    // Code line annotations — map operators to line ranges in generated C++
    "codeRefs": [
      { "op": "join_co", "lines": "85-145" }
    ],
    "codeLines": 289,  // total lines in best C++ file

    // Per-operator timing: [initial_ms, final_ms, "optimization reason"]
    "timings": {
      "join_co": [20.6, 13.2, "compact hash table"],
      "probe_l": [13.6, 7.5, "demand-paged I/O"]
    },

    // Self-optimization speedup summary (PRE-COMPUTED — use exact values from input)
    "speedup": { "from": 593, "to": 27, "factor": 22.2, "iters": 4 }
  },

  // Pipeline agents — always exactly 6 in this order
  "agents": [
    // Agent 0: SQL Input
    { "id": "sql", "name": "SQL Input", "shortName": "SQL", "color": "#6b7280",
      "role": "Parameterized query template",
      "detail": { "type": "sqlParams",
        "params": [
          { "name": ":param", "value": "'val'", "type": "string|date|int|double", "color": "#hex" }
        ],
        "tags": ["N tables", "M joins", "aggregation", "top-K"]  // query characteristics
      }
    },

    // Agent 1: Workload Analyzer
    { "id": "wa", "name": "Workload Analyzer", "shortName": "WA", "color": "#6366f1",
      "role": "Hardware, data, and access pattern profiling",
      "detail": { "type": "waDetail",
        "discoveries": [
          { "type": "warning", "icon": "\u26A0", "label": "Bottleneck", "text": "description" },
          { "type": "opportunity", "icon": "\u2728", "label": "Opportunity", "text": "description" },
          { "type": "strategy", "icon": "\u2699", "label": "Strategy", "text": "description" }
        ]
      }
    },

    // Agent 2: Storage Designer
    { "id": "sd", "name": "Storage Designer", "shortName": "SD", "color": "#059669",
      "role": "Columnar storage with encodings and indexes",
      "detail": { "type": "sdDetail",
        "shift": { "from": "Row-oriented .tbl text", "to": "Binary columnar + mmap" },
        "benefits": ["No parsing", "Column pruning", "Cache-friendly", "OS page cache"]
      }
    },

    // Agent 3: Query Planner
    { "id": "qp", "name": "Query Planner", "shortName": "QP", "color": "#d97706",
      "role": "Physical execution plan",
      "detail": { "type": "qpDetail",
        "decisions": ["decision 1", "decision 2"],
        "reduction": [
          { "label": "Input", "rows": 76486052 },
          { "label": "Filtered", "rows": 7560000 },
          { "label": "Aggregated", "rows": 500000 },
          { "label": "Top-K", "rows": 10 }
        ]
      }
    },

    // Agent 4: Code Generator
    { "id": "cg", "name": "Code Generator", "shortName": "CG", "color": "#8b5cf6",
      "role": "Generates standalone C++",
      "detail": { "type": "cgDetail",
        "codeSource": "benchmark:queryId:iter0",
        "result": { "lines": 289, "initialMs": 593 }
      }
    },

    // Agent 5: Optimizer
    { "id": "opt", "name": "Optimizer", "shortName": "OPT", "color": "#e11d48",
      "role": "Iterative profiling and optimization",
      "detail": { "type": "optDetail",
        "journey": {
          "totalSpeedup": 22.2, "startMs": 593, "endMs": 27,
          "iterations": [
            {
              "iteration": 0, "timingMs": 593, "title": "Initial", "categoryLabel": "Baseline",
              "insight": "description of what was discovered",
              "bottleneck": { "name": "Memory Init", "pct": 91, "description": "1 GB arrays, 3.5M used" },
              "operationTimings": [
                { "name": "phase_name", "ms": 20.4, "color": "#10b981" }
              ]
            },
            {
              "iteration": 1, "timingMs": 135, "title": "Compact Structs", "categoryLabel": "Elimination",
              "speedupVsPrev": 4.4, "fix": "what was changed",
              "changes": [{ "type": "remove", "what": "old thing" }, { "type": "add", "what": "new thing" }],
              "bottleneck": { "name": "Bottleneck", "pct": 24, "description": "details" },
              "operationTimings": [
                { "name": "phase_name", "ms": 32.7, "color": "#6366f1" }
              ]
            }
          ]
        }
      }
    }
  ],

  // Benchmark comparison (PRE-COMPUTED — use exact values from input)
  "benchmark": {
    "queryMs": [
      { "name": "GenDB", "ms": 27, "highlight": true },
      { "name": "DuckDB", "ms": 86 }
    ],
    "gendbSpeedups": [
      { "vs": "DuckDB", "factor": 3.2 }
    ],
    "generation": { "timeSec": 1529, "costUsd": 4.75, "iterations": 4, "linesOfCode": 292 }
  }
}
```

## Color Assignments

Parameter colors (assign in order): `#6366f1`, `#059669`, `#d97706`, `#e11d48`, `#8b5cf6`, `#0ea5e9`

Operation timing colors (cycle through): `#6366f1`, `#8b5cf6`, `#10b981`, `#f59e0b`, `#3b82f6`, `#ec4899`, `#ef4444`

Agent colors are FIXED: sql=`#6b7280`, wa=`#6366f1`, sd=`#059669`, qp=`#d97706`, cg=`#8b5cf6`, opt=`#e11d48`

## Rules

1. **Pre-computed values**: The user prompt provides exact values for `benchmark.queryMs`, `benchmark.gendbSpeedups`, `benchmark.generation`, and `canvas.speedup`. Copy them VERBATIM into the output.

2. **SQL display**: Take the template SQL, strip the `/* ... */` comment header, and replace all `:param_name` placeholders with their default values. For string/date params, wrap in single quotes. For `_2` suffixed params (used in subqueries), substitute the same way.

3. **Params array**: For each parameter, set `context` to the SQL text immediately before the value (e.g., `"c_mktsegment = "`, `"LIMIT "`, `"BETWEEN "`, `"AND "`). Skip `_2` suffixed duplicate params — only include each unique context once.

4. **Operator tree**: Derive from plan.json. Use 3-6 operators. Types: `scan`, `join`, `aggregate`, `sort`. Give each operator a short, descriptive `label` and a unique `id` (snake_case).

   **CRITICAL — Binary join structure**: Every `join` operator MUST have exactly **2 children** (left=build side, right=probe side), rendered side-by-side in the UI. Do NOT create linear chains where each join has only 1 child. Only **leaf operators** (children=[]) should have `tableSources`.

   CORRECT tree shape (binary joins, edges fan out):
   ```
        topk
          |
         agg
          |
       join_ol        ← 2 children!
        /    \
   join_co   probe_l  ← leaves with tableSources
   ```

   WRONG tree shape (linear chain, no branching):
   ```
   topk → agg → join1 → join2 → scan   ← all single-child, no side-by-side!
   ```

   For single-table queries (no joins), use a linear chain: sort → aggregate → scan.
   For multi-table queries, model each join as a binary branch point.
   For multi-way joins (3+ tables), nest binary joins: the top join's left child is another join, the right child is a scan/probe.

   Example for a 3-table query (orders ⋈ customer ⋈ lineitem):
   ```json
   { "id": "join_main", "type": "join", "children": ["join_inner", "probe_lineitem"] },
   { "id": "join_inner", "type": "join", "children": ["scan_orders", "scan_customer"] },
   { "id": "probe_lineitem", "type": "scan", "children": [], "tableSources": [{"table":"lineitem","label":"index probe"}] },
   { "id": "scan_orders", "type": "scan", "children": [], "tableSources": [{"table":"orders","label":"scan"}] },
   { "id": "scan_customer", "type": "scan", "children": [], "tableSources": [{"table":"customer","label":"bitmap"}] }
   ```

5. **Timings**: Map the best iteration's `operation_timings` to operator IDs. Format: `[first_iter_ms, best_iter_ms, "reason"]`. Use the first iteration's timings for the "before" value and the best iteration for "after". If an operation was eliminated, use 0 for the after value with reason like "fused into X".

6. **OPT journey iterations**: For each iteration in optimization_history:
   - `title`: 2-3 word summary of the key optimization (e.g., "Compact Structs", "Demand-Paged I/O")
   - `categoryLabel`: optimization category (e.g., "Baseline", "Elimination", "Algorithm", "Parallel")
   - `insight`: what profiling revealed (from the strategy text)
   - `bottleneck`: the dominant cost center with percentage estimate. Set to `null` for the final iteration if no bottleneck remains.
   - `fix`: what was done to fix it (for iterations > 0)
   - `changes`: [{type: "remove"|"add"|"change", what: "description"}] (for iterations > 0)
   - `operationTimings`: from the iteration's operation_timings, excluding "total" and "output". Assign colors from the timing color palette.

7. **WA discoveries**: 2-4 items. Include at least one "warning" (bottleneck/risk), one "opportunity", and one "strategy". Base on table sizes, filter selectivity, and hardware.

8. **SD detail**: `shift.from` should describe the original data format. `shift.to` should describe the binary columnar format. `benefits` should list 3-5 concrete benefits.

9. **QP decisions**: 2-4 key execution decisions. `reduction` should show the data flow from total input rows through filters, aggregation, to final output.

10. **Hardware**: 3-4 items mapping hardware facts to optimization strategies. Always include core count, disk type, and cache. Add SIMD if relevant (AVX-512).

11. **Encodings**: Use short human-readable `enc` descriptions like "dict 1B", "epoch i32", "char as byte". The `why` should explain WHY this encoding was chosen for this column.

12. **Indexes**: Use short `name` like "grouped index", "PK array", "zone map", "hash index". `mapping` shows the key-to-value mapping. `why` explains the performance benefit.

13. **Tags** (in SQL agent detail): List query characteristics like "N tables", "M joins", "aggregation", "top-K", "subquery", "HAVING", etc.

14. **CG detail**: `codeSource` format is `"benchmarkName:queryId:iter0"`. `result.lines` and `result.initialMs` come from the first iteration.

15. For queries with only 1 iteration (no optimization), the OPT journey should have just iteration 0 with title "Initial", categoryLabel "Baseline", and a note that no further optimization was performed. The speedup factor will be 1.0.
