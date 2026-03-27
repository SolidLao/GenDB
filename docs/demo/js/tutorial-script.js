/* ============================================================
   GenDB Demo — Tutorial Script Data
   Scene content, tour steps, subtitles, and timing.
   ============================================================ */

var TUTORIAL_SCRIPT = (function () {
  'use strict';

  /* ═══ Part 1: Cinematic Intro Scenes ═══ */
  var introScenes = [
    {
      id: 'logo',
      duration: 3500,
      subtitle: 'Welcome to GenDB.'
    },
    {
      id: 'problem',
      duration: 7000,
      subtitle: 'Traditional databases run every query through the same general-purpose engine \u2014 regardless of your data or hardware.'
    },
    {
      id: 'insight',
      duration: 7000,
      subtitle: 'GenDB uses AI agents to generate instance-optimized query processing code, tailored to your specific data and hardware.'
    },
    {
      id: 'pipeline',
      duration: 9000,
      subtitle: 'GenDB uses a multi-agent workflow \u2014 profiling your workload, designing storage, planning execution, generating code, and iteratively optimizing it.'
    },
    {
      id: 'results',
      duration: 6000,
      subtitle: 'On the TPC-H benchmark, GenDB\u2019s generated code outperforms every established database system we tested \u2014 fully automatic, zero manual tuning.'
    },
    {
      id: 'transition',
      duration: 3000,
      subtitle: "Let\u2019s see how it works."
    }
  ];

  /* ═══ Pipeline agents (for Scene 4) ═══ */
  var pipelineAgents = [
    { shortName: 'SQL', name: 'SQL Input', role: 'Parameterized query template', color: '#6b7280' },
    { shortName: 'WA', name: 'Workload Analyzer', role: 'Data & hardware profiling', color: '#6366f1' },
    { shortName: 'SD', name: 'Storage Designer', role: 'Columnar encodings & indexes', color: '#059669' },
    { shortName: 'QP', name: 'Query Planner', role: 'Physical execution plan', color: '#d97706' },
    { shortName: 'CG', name: 'Code Generator', role: 'Instance-optimized code', color: '#8b5cf6' },
    { shortName: 'OPT', name: 'Optimizer', role: 'Iterative profiling & tuning', color: '#e11d48' }
  ];

  /* ═══ Benchmark bars — TPC-H SF10 Full Workload (from main website) ═══ */
  var benchmarkBars = [
    { name: 'GenDB', ms: 185, color: '#059669', highlight: true },
    { name: 'Umbra', ms: 590, color: '#6366f1' },
    { name: 'DuckDB', ms: 594, color: '#818cf8' },
    { name: 'MonetDB', ms: 1416, color: '#9ca3af' },
    { name: 'ClickHouse', ms: 2407, color: '#6b7280' },
    { name: 'PostgreSQL', ms: 85551, color: '#4b5563' }
  ];

  /* ═══ Part 2: Guided Tour Steps ═══ */
  var tourSteps = [
    /* ── Control Panel ── */
    {
      id: 'control-panel',
      target: '#control-panel',
      action: 'highlight',
      tooltip: 'Browse pre-computed results, write new queries on existing benchmarks, or upload your own data and schema.',
      subtitle: 'This is your control panel \u2014 browse pre-computed results, write your own queries, or even bring your own data.',
      position: 'bottom',
      scrollTo: true
    },
    {
      id: 'browse-tab',
      target: '#pane-browse',
      action: 'highlight',
      tooltip: 'Select a benchmark and query to instantly view the full pipeline visualization with all agent contributions.',
      subtitle: 'We\u2019ve pre-computed results for TPC-H and SEC-EDGAR. Pick any query to instantly see the full pipeline.',
      position: 'bottom'
    },
    /* ── New Query Tab ── */
    {
      id: 'new-query-tab',
      target: '.control-tab[data-pane="new-query"]',
      action: 'click-tab',
      tooltip: 'Write any SQL query on an existing benchmark dataset. GenDB generates optimized code in real time.',
      subtitle: 'You can write any SQL you want, and GenDB generates optimized code for it in real time.',
      position: 'bottom'
    },
    {
      id: 'new-query-providers',
      target: '#gen-provider',
      action: 'highlight',
      tooltip: 'Choose Claude (Opus 4.6, Sonnet 4.6) or OpenAI (GPT-5.4, GPT-5.3-Codex). Different models produce different optimization strategies.',
      subtitle: 'You choose which LLM powers the generation \u2014 Claude or OpenAI. You can even compare how different models optimize the same query.',
      position: 'bottom'
    },
    /* ── Custom Data Tab ── */
    {
      id: 'custom-data-tab',
      target: '.control-tab[data-pane="custom-data"]',
      action: 'click-tab',
      tooltip: 'Upload your own data and queries. GenDB generates optimized code tailored to your dataset.',
      subtitle: 'Or bring your own data \u2014 GenDB generates optimized code for any dataset.',
      position: 'bottom'
    },
    {
      id: 'custom-data-pane',
      target: '#pane-custom-data',
      action: 'highlight',
      tooltip: 'Provide CREATE TABLE statements, drop CSV files (one per table), write your SQL, and pick a provider. GenDB handles the rest.',
      subtitle: 'Just provide your database schema, drop in CSV files, and write a query. GenDB takes care of storage design, execution planning, and code generation.',
      position: 'bottom'
    },
    /* ── Back to Browse + View Pipeline ── */
    {
      id: 'back-to-browse',
      target: '.control-tab[data-pane="browse"]',
      action: 'click-tab',
      tooltip: "Let's view a pre-computed result to walk through the full pipeline.",
      subtitle: 'Let\u2019s see the pipeline on TPC-H Q3 \u2014 a three-table join with aggregation and top-K.',
      position: 'bottom'
    },
    {
      id: 'view-pipeline',
      target: '#btn-browse',
      action: 'highlight',
      tooltip: "Click to load the pipeline visualization for the selected query.",
      subtitle: 'Click View Pipeline to dive in.',
      position: 'left'
    },
    {
      id: 'scroll-to-viz',
      target: '#btn-browse',
      action: 'click-and-scroll',
      delay: 1000
    },
    /* ── Pipeline Bar ── */
    {
      id: 'pipeline-bar',
      target: '.pipeline-bar',
      action: 'highlight',
      tooltip: 'Six agents run in sequence. Each one enriches the visualization. Click any agent to see its contribution.',
      subtitle: 'Here is the multi-agent pipeline. Each step builds on the previous, and you can click any to see exactly what it contributes.',
      position: 'bottom',
      scrollTo: true
    },
    /* ── SQL ── */
    {
      id: 'agent-sql',
      target: '.pipeline-node[data-idx="0"]',
      action: 'click-agent',
      tooltip: 'Extracts parameterized SQL templates. Queries sharing the same structure reuse the same generated code.',
      subtitle: 'The SQL Input step extracts a parameterized template from your query. Different filter values or date ranges can reuse the same generated code.',
      position: 'bottom'
    },
    {
      id: 'canvas-sql',
      target: '.cv-sql',
      action: 'highlight',
      tooltip: 'Predicate values are color-coded to show which parts become tunable parameters in the template.',
      subtitle: 'See the color-coded values? Those are the extracted parameters \u2014 swap them out for different workloads without regenerating.',
      position: 'right',
      scrollTo: true
    },
    /* ── WA ── */
    {
      id: 'agent-wa',
      target: '.pipeline-node[data-idx="1"]',
      action: 'click-agent',
      tooltip: 'Profiles target data statistics and hardware capabilities to identify bottlenecks and optimization opportunities.',
      subtitle: 'Now the Workload Analyzer kicks in \u2014 it profiles your data and hardware to spot bottlenecks and opportunities before any code is written.',
      position: 'bottom',
      scrollTo: 'pipeline'
    },
    {
      id: 'canvas-wa-tables',
      target: '.cv-tables',
      action: 'highlight',
      tooltip: 'Each table shows cardinality, filter predicate, and a selectivity bar. The customer segment filter retains only 20% of rows (1.5M \u2192 300K).',
      subtitle: 'The customer segment filter keeps just 20% of rows, from 1.5 million down to 300 thousand. That kind of insight is gold for optimization.',
      position: 'top',
      scrollTo: true
    },
    {
      id: 'canvas-wa-hw',
      target: '.cv-hw',
      action: 'highlight',
      tooltip: '64 cores \u2192 morsel-driven parallelism. HDD \u2192 prefer sequential I/O. 44 MB L3 \u2192 bitmap fits in cache. AVX-512 \u2192 SIMD aggregation.',
      subtitle: 'The hardware \u2014 64 cores enable morsel-driven parallelism, HDD favors sequential I/O, 44-megabyte L3 cache fits the customer bitmap, and AVX-512 unlocks SIMD aggregation.',
      position: 'top',
      scrollTo: true
    },
    /* ── SD ── */
    {
      id: 'agent-sd',
      target: '.pipeline-node[data-idx="2"]',
      action: 'click-agent',
      tooltip: 'Transforms raw text files into binary columnar storage with memory-mapped I/O, type-specific encodings, and targeted indexes.',
      subtitle: 'Next up, the Storage Designer \u2014 it transforms raw text files into binary columnar storage with memory-mapped I/O, picking the ideal encoding and index for each column.',
      position: 'bottom',
      scrollTo: 'pipeline'
    },
    {
      id: 'canvas-sd',
      target: '.cv-tables',
      action: 'highlight',
      tooltip: 'c_mktsegment: dictionary-encoded to 1 byte (5 values). Dates: epoch i32 for direct comparison. l_orderkey: grouped index mapping each key to a contiguous (start, count) range.',
      subtitle: 'Market segment has only 5 values, so it gets dictionary-encoded into a single byte. The grouped index on lineitem maps each order key to a contiguous range, turning a 60-million-row scan into O(1) lookup.',
      position: 'top',
      scrollTo: true
    },
    /* ── QP ── */
    {
      id: 'agent-qp',
      target: '.pipeline-node[data-idx="3"]',
      action: 'click-agent',
      tooltip: 'Generates the physical execution plan with join strategies, scan methods, and parallelism configuration.',
      subtitle: 'Now the Query Planner takes everything we\u2019ve learned and designs the actual execution strategy \u2014 join algorithms, scan methods, parallelism, all of it.',
      position: 'bottom',
      scrollTo: 'pipeline'
    },
    {
      id: 'canvas-qp',
      target: '.cv-tree',
      action: 'highlight',
      tooltip: 'For Q3: bitmap from 300K customers \u2192 semi-join filter on orders (3.5M rows) \u2192 grouped index probe on lineitem (7.5M rows) \u2192 aggregate 500K groups \u2192 top 10.',
      subtitle: 'Watch how it flows for Q3 \u2014 first a bitmap from 300 thousand qualifying customers, then a semi-join filter on orders, an index probe into lineitem, and finally the top 10 by revenue. 76 million input rows reduced to just 10.',
      position: 'left',
      scrollTo: true
    },
    /* ── CG ── */
    {
      id: 'agent-cg',
      target: '.pipeline-node[data-idx="4"]',
      action: 'click-agent',
      tooltip: 'Produces a standalone, self-contained C++ program implementing the full execution plan.',
      subtitle: 'This is where it all comes together \u2014 the Code Generator produces a complete, standalone C++ program. Not interpreted SQL, but native compiled code.',
      position: 'bottom',
      scrollTo: 'pipeline'
    },
    {
      id: 'canvas-cg',
      target: '.cv-gencode',
      action: 'highlight',
      tooltip: '289 lines for Q3. Line-range badges on operator nodes link each plan operator to its code section.',
      subtitle: 'For Q3, that\u2019s just 289 lines. And see those line-range badges on the operator tree? They link each operator directly to its code section, so you can trace exactly how the plan maps to code.',
      position: 'right',
      scrollTo: true
    },
    /* ── OPT ── */
    {
      id: 'agent-opt',
      target: '.pipeline-node[data-idx="5"]',
      action: 'click-agent',
      tooltip: 'Iteratively profiles the generated code, identifies bottlenecks, and rewrites them. Per-operator timing annotations show the improvements.',
      subtitle: 'And finally, the Optimizer \u2014 this is where the magic happens. It profiles the code, finds bottlenecks, and rewrites them, iterating until performance converges.',
      position: 'bottom',
      scrollTo: 'pipeline'
    },
    {
      id: 'canvas-opt',
      target: '.agent-detail-panel',
      action: 'highlight',
      tooltip: 'Iteration 0: 593 ms, 91% spent zero-initializing 1 GB for 3.5M entries. Over 4 iterations: compact structs, demand-paged mmap, parallel bitmap \u2192 27 ms (22\u00D7).',
      subtitle: 'Here\u2019s the story: the first version ran in 593 milliseconds \u2014 91% of that was wasted zero-initializing a 1 gigabyte array for just 3.5 million entries. The Optimizer spotted it, swapped in compact structures, added demand-paged I/O, parallelized the bitmap \u2014 and four iterations later, 27 milliseconds. A 22x speedup.',
      position: 'top',
      scrollTo: true
    },
    {
      id: 'canvas-opt-code',
      target: '.opt-code-section',
      action: 'highlight',
      tooltip: 'Browse the actual C++ code for each iteration. Use the dropdown to compare how the code evolves across optimization rounds.',
      subtitle: 'Browse the actual C++ code for each iteration. Use the dropdown to compare how the Optimizer rewrites the code at every step.',
      position: 'top',
      scrollTo: true
    },
    /* ── Benchmark ── */
    {
      id: 'benchmark',
      target: '.viz-bench-section',
      action: 'highlight',
      tooltip: 'GenDB vs Umbra, DuckDB, MonetDB, ClickHouse, and PostgreSQL on identical hardware. Only shown after completing the full pipeline.',
      subtitle: 'And the results speak for themselves \u2014 GenDB\u2019s generated code beats every established database system we tested. Fully automatic, zero manual tuning.',
      position: 'top',
      scrollTo: true
    },
    /* ── Finish ── */
    {
      id: 'finish',
      target: null,
      action: 'finish',
      tooltip: 'Click any agent to explore details. Switch queries to see different optimization strategies. Try the New Query or Custom Data tabs for live generation.',
      subtitle: 'That\u2019s GenDB. Click any agent to dig deeper, switch queries, or try generating code on your own data. Enjoy exploring!'
    }
  ];

  /* ═══ Audio file mapping — every step with a subtitle gets audio ═══ */
  var audioFiles = {
    // Part 1 scenes
    'intro-logo': 'audio/intro-logo.mp3',
    'intro-problem': 'audio/intro-problem.mp3',
    'intro-insight': 'audio/intro-insight.mp3',
    'intro-pipeline': 'audio/intro-pipeline.mp3',
    'intro-results': 'audio/intro-results.mp3',
    'intro-transition': 'audio/intro-transition.mp3',
    // Part 2 steps — every step with a subtitle
    'control-panel': 'audio/tour-control-panel.mp3',
    'browse-tab': 'audio/tour-browse-tab.mp3',
    'new-query-tab': 'audio/tour-new-query.mp3',
    'new-query-providers': 'audio/tour-new-query-providers.mp3',
    'custom-data-tab': 'audio/tour-custom-data.mp3',
    'custom-data-pane': 'audio/tour-custom-data-pane.mp3',
    'back-to-browse': 'audio/tour-back-to-browse.mp3',
    'view-pipeline': 'audio/tour-view-pipeline.mp3',
    'pipeline-bar': 'audio/tour-pipeline-bar.mp3',
    'agent-sql': 'audio/tour-agent-sql.mp3',
    'canvas-sql': 'audio/tour-canvas-sql.mp3',
    'agent-wa': 'audio/tour-agent-wa.mp3',
    'canvas-wa-tables': 'audio/tour-canvas-wa-tables.mp3',
    'canvas-wa-hw': 'audio/tour-canvas-wa-hw.mp3',
    'agent-sd': 'audio/tour-agent-sd.mp3',
    'canvas-sd': 'audio/tour-canvas-sd.mp3',
    'agent-qp': 'audio/tour-agent-qp.mp3',
    'canvas-qp': 'audio/tour-canvas-qp.mp3',
    'agent-cg': 'audio/tour-agent-cg.mp3',
    'canvas-cg': 'audio/tour-canvas-cg.mp3',
    'agent-opt': 'audio/tour-agent-opt.mp3',
    'canvas-opt': 'audio/tour-canvas-opt.mp3',
    'canvas-opt-code': 'audio/tour-canvas-opt-code.mp3',
    'benchmark': 'audio/tour-benchmark.mp3',
    'finish': 'audio/tour-finish.mp3'
  };

  return {
    introScenes: introScenes,
    pipelineAgents: pipelineAgents,
    benchmarkBars: benchmarkBars,
    tourSteps: tourSteps,
    audioFiles: audioFiles
  };
})();
