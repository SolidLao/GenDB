---
name: aggregation-optimization
description: Aggregation optimization for SQL queries with GROUP BY or aggregate functions (SUM, AVG, COUNT, MIN, MAX). Covers hash aggregation, sorted aggregation, thread-local merge, direct array lookup for low-cardinality groups, precision management for large-magnitude sums.
user-invocable: false
---

# Skill: Aggregation Optimization

## When to Load
Queries with GROUP BY or aggregate functions (SUM, AVG, COUNT, MIN, MAX).

## Algorithm Selection by Group Cardinality

These ranges are approximate guidelines. The actual crossover points depend on slot size (key + value bytes), cache hierarchy, and thread count. Always compute `groups × slot_size` and compare against cache levels.

| Group Cardinality (approx.) | Strategy | Notes |
|-----------------------------|----------|-------|
| Very low (flat array fits L1) | Direct flat array (if keys mappable to small int) | Zero hash, O(1) |
| Low (per-thread map fits L1/L2) | Direct array with lightweight hash or small hash map | Cache-resident per thread |
| Moderate (per-thread map fits LLC) | Thread-local hash tables + merge | See parallelism skill for thread-local sizing |
| High (total thread-local memory exceeds LLC) | Thread-local hash tables + partitioned merge, or scan-time partitioned aggregation | Evaluate Memory Budget Gate below |
| Very high (groups cannot be estimated) | Two-phase: sample to estimate, then partitioned | Memory management critical |

## Memory Budget Gate (MANDATORY — evaluate before choosing thread-local strategy)

Compute total memory: `total_agg_mem = nthreads × next_pow2(estimated_groups × 2) × slot_size_bytes`

Compare against hardware LLC (last-level cache) total capacity. Query the target machine's L3 size; do not assume a fixed number.

| total_agg_mem vs LLC | Strategy |
|---------------------|----------|
| ≤ LLC total | Per-thread hash maps (standard thread-local pattern) |
| > LLC total, per-partition map fits per-core cache | Per-thread maps + partitioned merge (see below) |
| > LLC total, per-partition map does NOT fit per-core cache | Strongly prefer scan-time partitioned aggregation (skip per-thread maps entirely) |

The deciding factor is whether the working set (per-thread or per-partition) fits the relevant cache level. When it doesn't, initialization and merge costs dominate — this scales with total_agg_mem, not with any fixed byte threshold.

### Scan-Time Partitioned Aggregation
Instead of allocating T full-cardinality hash maps:
1. During scan: each thread maintains P partition buffers (vectors of key-value tuples)
2. Partition assignment: `pid = hash(key) & (P-1)`, where P = next_pow2(nthreads)
3. After scan barrier: thread t drains ALL T buffers for partition t into one small hash map
   sized to fit the per-core private cache (L2 typically)
4. Zero contention, no large per-thread maps, embarrassingly parallel merge

This replaces the T full-cardinality maps + post-scan merge pattern.
Relationship to C9: C9 applies to per-thread maps when they ARE used. When scan-time
partitioned aggregation eliminates per-thread maps, C9 does not apply to the append-only
partition buffers.

VIOLATION DIAGNOSTIC: If build_joins + aggregation_merge > 2× main_scan, the aggregation
strategy is wrong — re-evaluate using this Memory Budget Gate.

## Precision Management for Aggregation

This is the authoritative section for floating-point precision in aggregation (experience skill C29 references here).

### General Principle
IEEE 754 double has ~15.95 significant decimal digits. Precision loss occurs when:
- The running SUM accumulator reaches magnitude >10^13, AND
- Output requires specific decimal digit accuracy (e.g., 2 decimal places for financial data)

### Detection
Data sampling during workload analysis shows column max value >= 10^10, AND query uses SUM/AVG with formatted output requiring decimal precision.

### Fix — int64_t Fixed-Point Accumulation
```
// Instead of: double sum += value;
// Use:
int64_t sum_cents = 0;
sum_cents += llround(value * 100.0);  // exact if individual value < ~10^13
// Output: printf("%lld.%02lld", sum_cents/100, llabs(sum_cents%100));
```

### Why This Works
Each individual `llround(v * 100.0)` is exact when `|v| < ~4.5 * 10^13` (because `v * 100` stays within double's exact integer range of 2^53 ~ 9 * 10^15). The int64_t accumulation is then perfectly exact regardless of sum magnitude.

### Why FP Compensation Fails
Kahan/Neumaier summation reduces error from summation ORDER, not from accumulator MAGNITUDE. When the sum itself exceeds 10^13, the ULP exceeds 0.01 and no reordering can recover sub-ULP precision.

### When NOT Needed
- If output tolerance is large (e.g., +/-$100 on $566B sums, as in TPC-H)
- If column max < 10^10
- If no decimal precision requirement in output
In these cases, double accumulation is fine.

## Key Principles
- Hash aggregation for medium-high cardinality (100-10M groups): O(n), pre-size hash table
- Sorted aggregation for pre-sorted input or very low cardinality: O(n) time, O(1) memory
- For <256 groups with dense integer keys: direct flat array lookup, zero hash overhead
- Partial aggregation for parallel execution: thread-local pre-agg → global merge

## GenDB-Specific
- Thread-local aggregation: each thread maintains own hash table, merge at end
- CRITICAL: Size each thread's hash table for FULL estimated group count, not groups/nthreads (see hash-tables skill: Sizing Rules)
- Merge strategy (MANDATORY — compute T×G and total_agg_mem before implementation; compare against hardware LLC):
  - **T×G < 100K**: Sequential merge into shared hash (acceptable at this scale)
  - **T×G 100K-2M**: Two-phase tree-merge: merge groups of 8 threads in parallel (log₈(T) rounds), then merge 8→1. Reduces sort input by 8×.
  - **T×G > 2M AND total_agg_mem ≤ LLC**: Partitioned merge: during aggregation, hash groups into P partitions (P=next_pow2(T)); during merge, each partition merged independently by one thread. Fully parallel, zero contention.
  - **T×G > 2M AND total_agg_mem > LLC**: Evaluate Memory Budget Gate above — may require scan-time partitioned aggregation.
  - **DEFAULT: Thread-local aggregation is strongly preferred (P20). A single shared aggregation map causes severe contention at scale; only consider it for very few groups where atomic updates are trivially cheap.**
  - VIOLATION DIAGNOSTIC: If build_joins + aggregation_merge > 2× main_scan, wrong strategy chosen — re-evaluate using Memory Budget Gate.
- CRITICAL: Size AGG_CAP per thread to next_pow2(estimated_groups × 2), not a large fixed ceiling. Over-sized per-thread tables waste memory and slow merge (P23, P25).
- GROUP BY key: MUST include ALL SQL GROUP BY columns. For composite GROUP BY keys, see data-structures skill: Composite Key Hashing

## HAVING Clause Optimization
- Pre-compute scalar subquery thresholds before main aggregation when possible
- Apply HAVING during merge phase (not as separate post-processing pass)
- This avoids materializing groups that will be filtered out

## Aggregation Patterns
- **Direct array**: key domain <256 → allocate flat array, O(1) access, no hashing
- **Open-addressing hash**: pre-sized 2x expected groups, Robin Hood probing
- **Partitioned hash**: divide key space into partitions, each thread owns a partition (contention-free)
- **Two-phase**: Phase 1 (parallel): thread-local hash tables. Phase 2 (merge): atomic updates to global table.
- **Streaming**: Input pre-sorted on GROUP BY key → single-pass O(n), O(1) memory

## Technique Keywords
hash aggregation, sorted aggregation, partial aggregation, thread-local merge,
partitioned aggregation, fixed-point accumulation, direct array aggregation

## Reference Papers
- Ye et al. 2011 — "Scalable Aggregation on Multicore Processors"
- Leis et al. 2014 — "Morsel-Driven Parallelism" (thread-local aggregation pattern)

## Diagnostic-Driven Adaptation

If the chosen aggregation strategy underperforms after implementation, use timing diagnostics to guide the next step:

| Symptom | Likely Cause | Adaptation |
|---------|-------------|------------|
| build_joins + aggregation_merge > 2× main_scan | Per-thread maps too large, initialization/merge dominates | Re-evaluate Memory Budget Gate; try scan-time partitioned aggregation |
| aggregation_merge > 20ms with thread-local maps | Too many groups × threads for sequential merge | Switch to partitioned merge or reduce partition count |
| main_scan unexpectedly slow with aggregation | Hash map probes cache-thrashing during scan | Check per-thread map fits per-core cache; if not, switch to scan-time partitioning |
| No improvement after strategy change | Bottleneck may be elsewhere (data loading, joins) | Profile other phases; aggregation may not be the dominant cost |

The goal is not to prescribe one strategy but to quickly diagnose and pivot when a strategy doesn't fit the workload.

## Common Pitfalls
→ See experience skill: C9 (hash table capacity), C15 (missing GROUP BY dimension), C29 (double precision), P3 (sort vs hash), P15 (merge cost), P20 (thread-local default)
