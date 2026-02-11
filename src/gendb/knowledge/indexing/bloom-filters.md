# Bloom Filters

## What It Is
Bloom filters are probabilistic data structures that test set membership with no false negatives but controlled false positives. Hash-based bit arrays enable space-efficient filtering for join reduction and existence checks.

## When To Use
- Semi-join reduction (filter probe side before expensive hash join)
- Existence checks on large datasets (avoid disk I/O for non-existent keys)
- Distributed joins (broadcast small bloom filter, filter before network shuffle)
- Multi-column predicates (combine bloom filters with AND/OR)
- NOT for: exact membership (use hash table) or deletions (use counting bloom filter)

## Key Implementation Ideas
- **Classic Bloom Filter**: k hash functions set k bits per element in a bit array; query returns "definitely not" or "probably yes"
- **Optimal Sizing Formula**: m = -n * ln(fpr) / (ln2)^2 bits; optimal k = (m/n) * ln2 hash functions
- **Bits-Per-Element Tradeoffs**: 1% FPR needs ~9.6 bits/element; 0.1% needs ~14.4; 0.01% needs ~19.2
- **Blocked Bloom Filter**: Constrain all k bit positions to a single cache line (512 bits), reducing lookup to 1 cache miss instead of k
- **Counting Bloom Filter**: Replace bits with 4-bit counters (0-15) to support deletions at 4x memory cost
- **Vectorized Probing**: Use SIMD to probe multiple keys in parallel with gathered cache lines
- **Partitioned Bloom Filter**: Divide bit array into k partitions, one hash per partition, for simpler SIMD implementation
- **Runtime Bloom Filters**: Build during hash join build phase; push down to scan operators for early filtering
- **Hash Function Selection**: Use XXHash or MurmurHash; derive k hashes from two base hashes (Kirsch-Mitzenmacker optimization)
- **Bloom Filter Composition**: AND of two filters reduces false positives; OR accumulates them

## Performance Characteristics
- 5-100x speedup on semi-joins (filter 90-99% of non-matching rows)
- Blocked filters: 1 cache miss per lookup vs k misses for classic (k typically 4-7)
- Memory: 8-20 bits/element (vs 32-64 bytes for hash table equivalent)

## Pitfalls
- Under-sizing causes high false positive rates that negate filtering benefit; over-sizing wastes cache
- Classic design incurs k cache misses per lookup; use blocked variant for performance
- Cannot resize without full rebuild; pre-allocate for expected cardinality
