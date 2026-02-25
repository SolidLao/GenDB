---
name: join-optimization
description: Join optimization strategies for SQL queries. Load when planning, generating, or optimizing queries that join 2+ tables. Covers hash join variants, join ordering, pre-built index usage, sampling for selectivity estimation, anti-join and semi-join patterns.
user-invocable: false
---

# Skill: Join Optimization

## When to Load
Queries with 1+ table joins, or optimizer targeting join phases.

## Strategy Selection Framework

Decision framework based on build-side memory footprint and join type. For data structure implementation details, see hash-tables skill.

The key metric is `build_memory = next_pow2(build_cardinality × 2) × slot_size` compared to hardware LLC. Row count thresholds are approximate — always compute actual memory.

| Build-Side Memory | Join Type | Strategy | Notes |
|-------------------|----------|----------|-------|
| Fits per-core cache | Any | Direct array if key is dense int; small hash set otherwise | Cache-resident |
| Fits LLC | Inner/Semi/Anti | Hash join (standard approach) | See hash-tables skill |
| Exceeds LLC | Inner | Hash join with **bloom pre-filter**, or partitioned hash join | Bloom keeps probes cache-friendly |
| Exceeds LLC | Semi/Anti | Bloom filter pre-filtering strongly recommended | Existence check only — bloom ideal |
| Any | Any | **Pre-built mmap index always preferred when available** | Zero build cost (P11) |

## Key Principles
- **Always build the smaller side** (after applying all filters)
- **Size estimation must use FILTERED cardinality**, not raw table size
- **Memory budget:** if hash table exceeds L3 cache, performance degrades non-linearly — consider partitioning or alternative data structures (see data-structures skill)
- For 2+ joins: empirically measure join selectivities via sampling program (100 lines, ≤1% rows, <5s)
- Join order matters 10-100x: smallest intermediate results first

## Anti-Join (NOT EXISTS / NOT IN / LEFT JOIN ... IS NULL)

Semantics: keep outer rows with NO match in inner.
Key insight: only need existence check, not value retrieval → bloom filter ideal for large inner tables.

### Memory-Based Strategy
1. **Inner hash set fits in LLC:** Simple hash set, probe outer, keep non-matches
2. **Inner hash set exceeds LLC:** Bloom filter pre-filtering (see data-structures skill: Bloom Filter)
   - Build bloom on inner → scan outer → bloom-negative rows emit immediately → bloom-positive candidates collected → build small exact hash from matching inner subset → probe candidates, emit non-matches
3. **Pre-built index available:** Always prefer (zero build cost)

### Cost Model
Bloom filter approach avoids building full hash table:
- 10M inner rows: 12MB bloom vs 160-512MB hash table
- Eliminates 80-99% of outer probes via bloom negatives

## Semi-Join (EXISTS / IN)

Semantics: keep outer rows with at least one match in inner.
Same size-based framework as anti-join. Early termination on first match.

### Pattern
1. Build bloom/hash on inner
2. Probe outer: bloom-negative → skip; bloom-positive → verify against exact hash → emit match
3. Stop probing on first match per outer row

### Dense-Key Bitmap Semi-Join
When the join key is a dense sequential integer (e.g., primary keys numbered 1..N), consider a flat bitmap or byte-array indexed directly by key value instead of a hash table:
- **Bitmap** (N/8 bytes): `bitset[key >> 3] |= (1 << (key & 7))` — minimal memory, fits L2 for N up to ~2M
- **Byte array** (N bytes): `bitmap[key] = 1` — simpler code, fits L2 for N up to ~250K

Conditions favoring this approach:
- Key values are dense integers in a bounded range [1, N] (gaps are fine — just waste a few bits)
- N × element_size fits comfortably in L2 cache (typically ≤256KB-1MB)
- The semi-join only needs existence check, not value retrieval

Benefit: O(1) insert and lookup with zero hash overhead, no collision handling. Replaces hash index probe (~5 cache misses per probe on large hash) with single L2-resident memory access.

When NOT applicable: sparse or non-integer keys, or N so large that the bitmap exceeds LLC.

### Dimension Semi-Join Pushdown (fact-dimension joins with selective dimension filter)

When a fact table joins a dimension table AND the dimension has a selective filter:

1. Compute dimension filter selectivity: filtered_dim_rows / total_dim_rows
2. If selectivity is low (filtered rows are a small fraction of total) AND the fact table is large enough that per-row probe cost matters:
   a. Collect qualifying dimension keys into a Bloom filter (~1.5 bytes/key, see data-structures skill: Bloom Filter)
   b. During fact table scan: check Bloom BEFORE expensive hash probes
   c. Bloom-negative rows (majority) skip all downstream probes
3. When NOT to apply: if the dimension hash index working set already fits in cache AND the probe includes the dimension filter check (probe+filter is already cheap). The benefit comes from avoiding cache-missing hash probes — if probes are cache-resident anyway, the Bloom adds overhead with little gain.

Key insight: converts "probe then filter" → "cheap filter then probe", avoiding expensive
random-access hash probes for the majority of non-qualifying rows.

## Hash Join (Inner)

Default join pattern. Build smaller side, probe larger.
- Pre-size hash table: capacity = next_power_of_2(build_cardinality * 2)
- For data structure selection, see data-structures skill
- When the query also aggregates over the join result AND the GROUP BY key is a superset of (or equals) the join key, consider probe-aggregate fusion — see aggregation-optimization skill: Probe-Aggregate Fusion. This eliminates intermediate materialization and separate aggregation phases.
- When the build phase is a bottleneck and keys are unique (PK joins), consider CAS-based concurrent insert for parallel build — see hash-tables skill: CAS-Based Concurrent Insert

### Bloom Pre-Filter for L3-Exceeding Inner Joins
When the build-side hash table exceeds L3 cache but a full inner join (not just existence) is needed:
1. Build bloom filter from hash table keys (~1.5 bytes/item, fits L2/L3)
2. During probe scan: check bloom first (sequential-friendly, high throughput)
3. Only probe the large hash table on bloom-positive rows (typically 1-5% of probe side)
4. Net effect: random hash probes reduced by 90-99%, transforming LLC-thrashing probe into L2-friendly bloom scan

**When**: hash_table_bytes > hardware L3_cache_bytes AND probe_rows > 10× build_rows
**Implementation**: see data-structures skill: Bloom Filter for C++ pattern

### Partitioned Hash Join
When build side memory exceeds L3 cache → partition into L3-sized chunks using radix bits:
- Partition both sides by hash prefix (e.g., lower 4-8 bits)
- Each partition fits L3 cache → cache-friendly probing
- Cost model: build ≈ N_build * (hash + insert), probe ≈ N_probe * (hash + avg_probes * cache_miss_cost)

## Sort-Merge Join

When to use:
- Both sides already sorted on join key (common after index scan)
- When join is inequality-based (range join)
- Cost: O(N+M) when pre-sorted, O(N log N + M log M) otherwise

## Filtered Dimension Join Pattern

When dimension table is filtered before join (e.g., `WHERE stmt='IS'`), the build size is the FILTERED count, not raw table size.
- Pre-built indexes for common filter+join combinations can eliminate build phase entirely
- Reference: Storage Designer can create filtered indexes during ingestion

## Star Join
Filter all dimensions first, probe fact table once with multiple hash lookups fused in single pass.

## GenDB-Specific
- Pre-built hash indexes: read from .gendb/ via mmap, format described in Query Guide
- When Query Guide lists a pre-built hash index for a join key, use it (zero build cost)
- Hash table pattern: open-addressing, Robin Hood, bounded probing (for-loop with probe < cap)
- Thread-local hash tables sized for FULL key cardinality (not cardinality/nthreads)
- Multi-value hash index format: [uint32_t num_entries][entries...][uint32_t num_buckets][bucket_offset...] — see Query Guide for exact layout per index

## Technique Keywords
radix partitioning, grace hash join, symmetric hash join, semi-join reduction,
bloom filter pre-filtering, index nested loop join, star schema optimization, sort-merge join

## Reference Papers
- Selinger et al. 1979 — "Access Path Selection in a Relational DBMS" (System R optimizer)
- Moerkotte & Neumann 2006 — "Analysis of Two Existing and One New Dynamic Programming Algorithm" (DPccp)
- Balkesen et al. 2013 — "Main-Memory Hash Joins on Multi-Core CPUs"
- Ngo et al. 2012 — "Worst-Case Optimal Join Algorithms" (AGM bound)
- Blanas et al. 2011 — "Design and Evaluation of Main Memory Hash Join Algorithms for Multi-Core CPUs"

## Diagnostic-Driven Adaptation

If the chosen join strategy underperforms, use timing diagnostics to guide the next step:

| Symptom | Likely Cause | Adaptation |
|---------|-------------|------------|
| build_joins dominates total time | Hash table too large for cache, or runtime build when pre-built index exists | Check for pre-built mmap index; partition build into LLC-sized chunks |
| main_scan slow despite moderate build-side | Probe-side hash lookups cache-thrashing (hash table > LLC) | Add bloom pre-filter; or partition both sides |
| Bloom filter adds overhead without speedup | Filter selectivity too high (most rows pass bloom) OR hash table already cache-resident | Remove bloom; use direct hash probe |
| Join produces unexpected row counts | Cardinality estimation was wrong | Re-examine filtered cardinality; check 1:1 vs 1:N assumption |

Strategy tables give starting points, not final answers. Measure, diagnose, adapt.

## Common Pitfalls
→ See experience skill: C9 (hash table capacity), C24 (unbounded probing), P1 (std::unordered_map), P11 (pre-built indexes), P21 (bloom filter for anti-join), P23 (filtered cardinality), P32 (dense-key bitmap semi-join), P33 (probe-aggregate fusion)
