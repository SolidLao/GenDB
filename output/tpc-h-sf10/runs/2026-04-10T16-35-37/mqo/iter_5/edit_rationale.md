# Iteration 5: Eliminate Q13 Thread-Local Array Explosion

## Quantitative Diagnosis

The `fused_scan_orders` region takes 249ms (19% of dispatcher_total at 1340ms). This stage executes Q4 and Q13 over 15M order rows.

**Q13's memory footprint is the root cause**: The code allocates 64 thread-local `vector<int32_t>` arrays, each with 1,500,001 entries (one per custkey). Each array = 6MB. Total: **64 × 6MB = 384MB**, which is **8.7× the 44MB L3 cache**.

The costs break down as:
1. **Allocation**: `assign(CUSTOMER_ROWS + 1, 0)` zero-fills 384MB across 64 vectors, triggering page faults and consuming memory bandwidth.
2. **Merge**: 64 iterations of an OpenMP parallel-for over 1.5M entries = 96M total iterations, with 64 thread barrier synchronizations (~320μs in barriers alone).
3. **Cache pollution**: 384MB of working set evicts other useful data from cache.

## Why Category B (Algorithm Change)

This is a classic Q2 diagnosis: the data structure (thread-local dense arrays) doesn't fit the hardware. The working set (384MB) is nearly 9× larger than L3 (44MB). No micro-optimization can fix this — the algorithm must change.

## The Fix

Replace 64 thread-local arrays with a **single shared array** using `#pragma omp atomic` for increments. This reduces the working set from 384MB to 6MB (fits comfortably in L3) and eliminates the merge phase entirely.

**Contention analysis**: With 1.5M distinct custkeys and 64 threads, two threads collide on the same custkey with probability ~1/23,437 per increment. At ~14.8M qualifying rows (99% pass the NOT LIKE filter), expected collisions are ~630 total — negligible overhead.

Additionally, the Q15 supplier revenue merge in `fused_scan_lineitem` was restructured: instead of 63 sequential passes (each an inner loop over 100K entries), a single parallel-for over suppliers with an inner sequential loop over threads. This improves cache locality (each supplier's data from all threads is merged in one pass) and uses all cores for the merge.

## Risks

- Atomic increments add ~1 cycle overhead per qualifying row vs. plain store, but this is dwarfed by the eliminated allocation and merge costs.
- If custkey distribution were highly skewed (many orders mapping to few custkeys), contention could increase. TPC-H data has uniform custkey distribution, so this is not a concern.
