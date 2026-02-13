# Bloom Filters

## What It Is
Bloom filters are probabilistic data structures that test set membership with no false negatives but controlled false positives. Hash-based bit arrays enable space-efficient filtering for join reduction and existence checks.

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
