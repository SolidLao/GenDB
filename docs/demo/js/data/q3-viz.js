// GenDB Demo — Two-Column Progressive Canvas Data for TPC-H Q3
var Q3_VIZ = {
  version: 5, queryId: "Q3", title: "Shipping Priority",

  /* ═══ Canvas: unified diagram data ═══ */
  canvas: {
    /* SQL */
    sql: "SELECT l_orderkey,\n  SUM(l_extendedprice * (1 - l_discount)) AS revenue,\n  o_orderdate, o_shippriority\nFROM customer, orders, lineitem\nWHERE c_mktsegment = 'BUILDING'\n  AND c_custkey = o_custkey\n  AND l_orderkey = o_orderkey\n  AND o_orderdate < '1995-03-15'\n  AND l_shipdate > '1995-03-15'\nGROUP BY l_orderkey, o_orderdate, o_shippriority\nORDER BY revenue DESC, o_orderdate\nLIMIT 10;",
    params: [
      { name: ":c_mktsegment_eq", value: "'BUILDING'", color: "#6366f1", context: "c_mktsegment = " },
      { name: ":o_orderdate_upper", value: "'1995-03-15'", color: "#059669", context: "o_orderdate < " },
      { name: ":l_shipdate_lower", value: "'1995-03-15'", color: "#d97706", context: "l_shipdate > " },
      { name: ":limit", value: "10", color: "#e11d48", context: "LIMIT " }
    ],

    /* WA: tables with selectivity details */
    tables: [
      { id: "customer", name: "customer", rows: 1500000, role: "dimension",
        predicate: "c_mktsegment = 'BUILDING'", selectivity: 0.2, outputRows: 300000 },
      { id: "orders", name: "orders", rows: 15000000, role: "fact",
        predicate: "o_orderdate < '1995-03-15'", selectivity: 0.48, outputRows: 7200000 },
      { id: "lineitem", name: "lineitem", rows: 59986052, role: "fact",
        predicate: "l_shipdate > '1995-03-15'", selectivity: 0.54, outputRows: 32392000 }
    ],
    joins: [
      { from: "customer", to: "orders", label: "1 : N" },
      { from: "orders", to: "lineitem", label: "1 : N" }
    ],
    hardware: [
      { fact: "64 cores", strategy: "Morsel-driven parallelism" },
      { fact: "HDD", strategy: "Sequential I/O only" },
      { fact: "44 MB L3", strategy: "Bitmap fits in cache" },
      { fact: "AVX-512", strategy: "SIMD aggregation" }
    ],

    /* SD: encodings + indexes with WHY */
    encodings: [
      { table: "customer", col: "c_mktsegment", enc: "dict 1B", why: "Only 5 distinct values \u2014 dictionary is ideal" },
      { table: "orders", col: "o_orderdate", enc: "epoch i32", why: "Enables direct integer comparison for date filter" },
      { table: "lineitem", col: "l_shipdate", enc: "epoch i32", why: "Enables direct integer comparison for date filter" }
    ],
    indexes: [
      { table: "lineitem", name: "grouped index", mapping: "l_orderkey \u2192 (start, count)", why: "Avoids 60M full scan \u2014 O(1) contiguous lookup" },
      { table: "orders", name: "PK array", mapping: "o_orderkey \u2192 row_id", why: "O(1) metadata access for output" }
    ],

    /* QP: operator tree — join_ol branches into join_co (left) and probe_l (right) */
    operators: [
      { id: "topk", label: "Top-K", type: "sort", rows: 10, children: ["agg"] },
      { id: "agg", label: "Group + SUM", type: "aggregate", rows: 500000, children: ["join_ol"] },
      { id: "join_ol", label: "Index Probe", type: "join", rows: 7560000, joinType: "idx nested loop",
        children: ["join_co", "probe_l"] },
      { id: "join_co", label: "Bitmap Join", type: "join", rows: 3500000, joinType: "bitmap filter",
        children: ["scan_cust", "scan_ord"] },
      { id: "scan_cust", label: "Dict Scan", type: "scan", rows: 300000,
        children: [], tableSources: [{ table: "customer", label: "bitmap" }] },
      { id: "scan_ord", label: "Date Filter", type: "scan", rows: 3500000,
        children: [], tableSources: [{ table: "orders", label: "scan" }] },
      { id: "probe_l", label: "Index Probe", type: "scan", rows: 7560000,
        children: [], tableSources: [{ table: "lineitem", label: "grouped index" }] }
    ],

    /* CG: code annotations */
    codeRefs: [
      { op: "join_co", lines: "85\u2013145" },
      { op: "probe_l", lines: "147\u2013195" },
      { op: "topk", lines: "197\u2013230" }
    ],
    codeLines: 289,

    /* OPT: per-operator timing [before, after, reason] */
    timings: {
      join_co: [20.6, 13.2, "compact hash table"],
      probe_l: [13.6, 7.5, "demand-paged I/O"],
      agg: [20.6, 0, "fused into join"],
      topk: [20.6, 4.9, "parallel top-K"]
    },
    speedup: { from: 593, to: 27, factor: 22.2, iters: 4 }
  },

  /* ═══ Agents (pipeline bar + detail panels) ═══ */
  agents: [
    { id: "sql", name: "SQL Input", shortName: "SQL", color: "#6b7280",
      role: "Parameterized query template",
      detail: { type: "sqlParams",
        params: [
          { name: ":c_mktsegment_eq", value: "'BUILDING'", type: "string", color: "#6366f1" },
          { name: ":o_orderdate_upper", value: "'1995-03-15'", type: "date", color: "#059669" },
          { name: ":l_shipdate_lower", value: "'1995-03-15'", type: "date", color: "#d97706" },
          { name: ":limit", value: "10", type: "int", color: "#e11d48" }
        ],
        tags: ["3 tables", "2 joins", "aggregation", "top-K"] }
    },
    { id: "wa", name: "Workload Analyzer", shortName: "WA", color: "#6366f1",
      role: "Hardware, data, and access pattern profiling",
      detail: { type: "waDetail",
        discoveries: [
          { type: "warning", icon: "\u26A0", label: "Bottleneck", text: "lineitem (60M) dominates \u2014 must avoid full scan" },
          { type: "opportunity", icon: "\u2728", label: "Opportunity", text: "c_mktsegment: 5 values \u2192 dictionary + bitmap" },
          { type: "opportunity", icon: "\u2728", label: "Opportunity", text: "lineitem sorted by l_orderkey \u2192 grouped index" },
          { type: "strategy", icon: "\u2699", label: "Strategy", text: "Filters reduce 76M \u2192 7.5M before aggregation" }
        ] }
    },
    { id: "sd", name: "Storage Designer", shortName: "SD", color: "#059669",
      role: "Columnar storage with encodings and indexes",
      detail: { type: "sdDetail",
        shift: { from: "Row-oriented .tbl text", to: "Binary columnar + mmap" },
        benefits: ["No parsing", "Column pruning", "Cache-friendly", "OS page cache"] }
    },
    { id: "qp", name: "Query Planner", shortName: "QP", color: "#d97706",
      role: "Physical execution plan",
      detail: { type: "qpDetail",
        decisions: [
          "Bitmap semi-join: 187 KB fits L2 cache",
          "Grouped index avoids 60M full scan",
          "64-thread morsel-driven parallelism"
        ],
        reduction: [
          { label: "Input", rows: 76486052 }, { label: "Filtered", rows: 7560000 },
          { label: "Aggregated", rows: 500000 }, { label: "Top-K", rows: 10 }
        ] }
    },
    { id: "cg", name: "Code Generator", shortName: "CG", color: "#8b5cf6",
      role: "Generates standalone C++",
      detail: { type: "cgDetail", codeSource: "tpch:Q3:iter0", result: { lines: 289, initialMs: 593 } }
    },
    { id: "opt", name: "Optimizer", shortName: "OPT", color: "#e11d48",
      role: "Iterative profiling and optimization",
      detail: { type: "optDetail",
        journey: { totalSpeedup: 22.2, startMs: 593, endMs: 27,
          iterations: [
            { iteration: 0, timingMs: 593, title: "Initial", categoryLabel: "Baseline",
              insight: "1 GB zero-init for 60M slots, only 3.5M used",
              bottleneck: { name: "Memory Init", pct: 91, description: "1 GB arrays, 3.5M used" },
              operationTimings: [
                { name: "bitmap", ms: 0.2, color: "#8b5cf6" }, { name: "orders", ms: 20.4, color: "#10b981" },
                { name: "probe", ms: 13.6, color: "#f59e0b" }, { name: "agg", ms: 20.6, color: "#3b82f6" },
                { name: "overhead", ms: 538, color: "#ef4444" } ] },
            { iteration: 1, timingMs: 135, title: "Compact Structs", categoryLabel: "Elimination",
              speedupVsPrev: 4.4, fix: "56 MB compact struct. Fused aggregation.",
              changes: [{ type: "remove", what: "1 GB arrays" }, { type: "add", what: "56 MB struct" }],
              bottleneck: { name: "MAP_POPULATE", pct: 24, description: "Pre-faults 706 MB" },
              operationTimings: [
                { name: "load", ms: 32.7, color: "#6366f1" }, { name: "bitmap", ms: 4.2, color: "#8b5cf6" },
                { name: "orders", ms: 17.6, color: "#10b981" }, { name: "sort", ms: 21.7, color: "#ec4899" },
                { name: "probe", ms: 7.3, color: "#f59e0b" }, { name: "topk", ms: 6.2, color: "#3b82f6" } ] },
            { iteration: 2, timingMs: 68, title: "Demand-Paged I/O", categoryLabel: "Algorithm",
              speedupVsPrev: 2.0, fix: "No MAP_POPULATE. MADV_SEQUENTIAL. Two-pass scatter.",
              changes: [{ type: "remove", what: "MAP_POPULATE" }, { type: "add", what: "Two-pass scatter" }],
              bottleneck: { name: "MMap Destructor", pct: 54, description: "munmap inside timer" },
              operationTimings: [
                { name: "filter", ms: 6.1, color: "#8b5cf6" }, { name: "orders", ms: 11.3, color: "#10b981" },
                { name: "probe", ms: 8.2, color: "#f59e0b" }, { name: "topk", ms: 5.8, color: "#3b82f6" },
                { name: "overhead", ms: 37, color: "#ef4444" } ] },
            { iteration: 3, timingMs: 27, title: "RAII + Parallel", categoryLabel: "Elim + Parallel",
              speedupVsPrev: 2.5, fix: "MMap before timer. Parallel bitmap + top-K.",
              changes: [{ type: "change", what: "MMap before timer" }, { type: "add", what: "Parallel bitmap" }],
              bottleneck: null,
              operationTimings: [
                { name: "filter", ms: 3.6, color: "#8b5cf6" }, { name: "orders", ms: 9.6, color: "#10b981" },
                { name: "probe", ms: 7.5, color: "#f59e0b" }, { name: "topk", ms: 4.9, color: "#3b82f6" } ] }
          ] } }
    }
  ],

  /* ═══ Benchmark ═══ */
  benchmark: {
    queryMs: [
      { name: "GenDB", ms: 27, highlight: true }, { name: "Umbra", ms: 63 },
      { name: "DuckDB", ms: 86 }, { name: "MonetDB", ms: 203 },
      { name: "ClickHouse", ms: 426 }, { name: "PostgreSQL", ms: 2059 }
    ],
    gendbSpeedups: [
      { vs: "Umbra", factor: 2.3 }, { vs: "DuckDB", factor: 3.2 },
      { vs: "ClickHouse", factor: 15.8 }, { vs: "PostgreSQL", factor: 76.3 }
    ],
    generation: { timeSec: 1529, costUsd: 4.75, iterations: 4, linesOfCode: 292 }
  }
};
