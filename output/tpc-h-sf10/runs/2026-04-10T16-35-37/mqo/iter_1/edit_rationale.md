# Iteration 1: Q21 Finalize — Parallelization + Allocation Elimination

## Quantitative Diagnosis

The profile shows `Q21_finalize` at **1057ms**, consuming **36.9%** of the 2869ms dispatcher total. This is by far the largest single region. The next largest regions are `fused_scan_partsupp` (615ms) and `fused_scan_lineitem` (352ms), which are already parallelized.

The Q21 finalize processes candidate (orderkey, suppkey) pairs collected during the lineitem fused scan. For TPC-H SF10, SAUDI ARABIA has ~4000 of 100K suppliers, and with the receiptdate > commitdate filter (~38%) and order status 'F' (~50%), we expect ~460K candidates spanning ~300K+ unique orders.

For **each order**, the original code created:
- 1 `unordered_map` grouping (with `vector<int32_t>` values)
- 1 `vector<SuppInfo>` for order lines
- 2 `unordered_set<int32_t>` for all_suppkeys and late_suppkeys
- 1 `unordered_set<int32_t>` for candidate dedup

Each `unordered_set` has ~200 bytes of fixed overhead plus heap nodes. At ~300K orders, that's **millions of heap allocations** for sets that typically contain 1-7 elements. This is a textbook case of data structure/hardware mismatch (Category B) combined with wasted work (Category C: hash overhead for tiny sets) and no parallelism (Category D).

## Fix Applied

1. **Sort + group** candidates by orderkey instead of inserting into `unordered_map<int32_t, vector<int32_t>>`. This eliminates the grouping hash map entirely and is cache-friendly.

2. **Stack-allocated arrays** (`int32_t[128]`) with linear unique-insert replace all `unordered_set` instances. For TPC-H's 1-7 lineitems per order, linear scan over 1-7 elements is faster than any hash set due to zero allocation overhead and full L1 residency.

3. **OpenMP parallelization** with `schedule(dynamic, 256)` distributes the ~300K order groups across 64 cores. Each thread maintains a private `unordered_map<int32_t, int64_t>` for supplier wait counts (only ~4K SAUDI ARABIA suppliers), merged after the parallel region.

## Expected Impact

The combination of 64× parallelism and elimination of ~1.5M heap allocations should reduce Q21_finalize from ~1057ms to ~20-50ms, saving ~1000ms (~35% of batch total).

## Risks

- Stack arrays capped at 128 entries; TPC-H max is 7 lineitems/order, so this is safe with large margin.
- Dynamic scheduling adds minor overhead but ensures load balance across variable-size order groups.
