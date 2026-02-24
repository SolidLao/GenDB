---
name: scan-optimization
description: Scan and filter optimization for SQL queries. Load when planning, generating, or optimizing queries with WHERE filters on large tables. Covers predicate pushdown, late materialization, zone-map-guided scans, selection vectors, selectivity-based technique selection, SIMD filtering.
user-invocable: false
---

# Skill: Scan & Filter Optimization

## When to Load
Queries with filters on large tables, or optimizer targeting scan/filter phases.

## Selectivity-Based Technique Selection

These are guidelines based on typical workloads. The boundaries shift depending on payload width (wider payloads favor late materialization at higher selectivity), cache pressure, and per-row processing cost. When in doubt, the fused single-pass approach is the safest default.

| Filter Selectivity (approx.) | Recommended Technique | Why |
|------------------------------|----------------------|-----|
| High (most rows pass) | Fused single-pass (scan + filter + probe + aggregate in one loop) | Selection vector overhead not worthwhile |
| Moderate | Fused scan with branch hints (`__builtin_expect`) | Balance between scan cost and random access |
| Low (small fraction passes) | Selection vector (gather qualifying row indices first, then batch process) | Reduces random access in join/aggregation phases |
| Very low, or wide payload | Late materialization (load filter columns first, then load payload only for qualifying rows) | Dramatic I/O and cache reduction |

The key insight: the tradeoff is between branch misprediction cost (favors selection vectors for unpredictable branches) and materialization overhead (favors fused scan when most data is touched anyway). Wider payloads shift the crossover toward late materialization.

## Selection Vectors

### When to Use
- Selectivity <20% AND subsequent phases involve random access (hash probes, aggregation)
- NOT worthwhile when selectivity >80% (overhead exceeds benefit)

### Sizing
`reserve(n_rows * estimated_selectivity * 1.1)`

### Two-Phase Implementation
```
// Phase 1: parallel filter → thread-local selection vectors
std::vector<std::vector<uint32_t>> thread_selections(nthreads);
#pragma omp parallel
{
    int tid = omp_get_thread_num();
    auto& sel = thread_selections[tid];
    sel.reserve(morsel_size * selectivity * 1.1);
    #pragma omp for schedule(dynamic, morsel_size)
    for (uint32_t i = 0; i < n_rows; ++i) {
        if (filter_col[i] == target) sel.push_back(i);
    }
}
// Phase 2: merge + batch-process selection
// Concatenate thread_selections, then iterate over qualifying indices
```

## Late Materialization

### When to Use
- Selectivity <5% OR payload columns total >32 bytes per row
- Column load order: zone maps (see indexing skill) → filter columns → apply filter → load join keys for qualifying rows only → load payload for surviving rows

### Cost Model
Saves `(1 - selectivity) * payload_bytes * n_rows` bytes of memory bandwidth.
Example: 1% selectivity, 64B payload, 10M rows → saves 633MB of bandwidth.

## Key Principles
- Predicate pushdown: evaluate filters as early as possible, before joins
- Single-pass fused scans: combine filter + join probe + aggregation in one pass over fact table
- Predicate ordering: order by `(1 - selectivity) / eval_cost` descending. The most selective predicate is usually best first, but a cheap low-selectivity check (e.g., integer equality) may be better placed before an expensive high-selectivity check (e.g., hash probe). When evaluation costs are similar, most selective first is correct.

## SIMD Filtering

### When Beneficial
- Integer column comparisons, simple predicates (equality, range), large row counts
- Loop structure: process 8 int32_t values per AVX2 iteration, extract qualifying mask
- Most beneficial when per-row work is minimal (simple comparisons) and the scan is bandwidth-bound

### When NOT to Use
- Complex predicates, dictionary decode paths, very high selectivity (branch predictor already optimal)
- Compiler auto-vectorization with `-O3 -march=native` handles most cases — only use explicit intrinsics when compiler report shows a critical inner loop is not vectorized AND profiling shows the loop is a bottleneck

## GenDB-Specific
- Zone-map-guided scan: load zone maps first, skip non-qualifying blocks entirely (see indexing skill: Zone Maps for skip logic and format)
- Column-ordered loading: zone maps → filter columns → join keys → payload columns
- Dictionary-encoded string filters: load dict, find matching codes, filter by code (integer comparison)

## Specialization Philosophy
Generated code knows the exact query at compile time. Exploit this:
- No type dispatch — direct typed column access via reinterpret_cast
- No predicate interpretation — hardcoded comparisons
- No buffer pool — direct mmap access
- Fused pipelines — scan+filter+probe+aggregate in one tight loop

## Technique Keywords
predicate pushdown, late materialization, selection vectors, branch-free filtering,
SIMD filtering, vectorized selection, dictionary pre-filtering

## Reference Papers
- Abadi et al. 2008 — "Column-Stores vs. Row-Stores: How Different Are They Really?"
- Idreos et al. 2012 — "MonetDB: Two Decades of Research in Column-Oriented Database Architectures"
- Lang et al. 2016 — "Data Blocks: Hybrid OLTP and OLAP on Compressed Column-Main Memory Databases"

## Diagnostic-Driven Adaptation

| Symptom | Likely Cause | Adaptation |
|---------|-------------|------------|
| main_scan dominates despite low selectivity | Late materialization not applied, scanning full payload for all rows | Load filter columns first, then payload only for qualifying rows |
| main_scan dominates with high selectivity | Fused scan is appropriate but inner loop has too much work per row | Profile per-row cost; check hash probe sizes; consider reducing columns touched |
| data_loading dominates (>50% total) | Too many columns loaded eagerly, or missing zone-map guidance | Load only needed columns; apply zone-map-guided selective madvise |
| Switching to selection vectors slowed things down | Selectivity was higher than estimated, or materialization overhead exceeded savings | Revert to fused scan; selection vectors only help at low selectivity |

## Common Pitfalls
→ See experience skill: C19 (zone-map on unsorted column), P4 (multiple passes), P10 (ignoring zone maps)
