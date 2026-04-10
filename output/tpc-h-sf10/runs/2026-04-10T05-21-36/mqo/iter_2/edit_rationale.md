# Iteration 2 — Eliminate Q18 Atomic Contention + Parallelize Q21 Finalize

## Bottleneck Analysis

The profile shows `fused_scan_lineitem` at 2930.9ms (55.5% of batch time) as the dominant bottleneck, followed by `Q21_finalize` at 597.8ms (11.3%).

## Edit 1: Q18 Atomic → Thread-Local Accumulation

Inside the fused lineitem scan, the Q18 branch executes on **every single lineitem row** with no date filter — approximately 60 million rows at SF10. The original code used `#pragma omp atomic` to accumulate quantities into a shared `C.q18qs[ok]` array:

```cpp
#pragma omp atomic
C.q18qs[ok] += C.l_qty[i];
```

On x86-64, atomic addition of `double` values compiles to a compare-and-swap (CAS) loop since there is no hardware atomic floating-point add instruction (pre-AVX512). This CAS loop involves:
1. Loading the current value
2. Computing the sum
3. Attempting a CAS, retrying on failure

Even without contention, each CAS costs ~20-40 cycles due to the lock prefix and memory fence. With multiple threads hitting nearby array slots, false sharing on cache lines further degrades performance. Over 60M rows, this adds up to a massive overhead — potentially 500-1000ms of the scan time.

The fix replaces this with a thread-local `std::unordered_map<int32_t, double>` per thread, following the same pattern used by all other queries (Q3, Q10, Q17, Q20, Q21). After the parallel region, thread-local maps are merged into `C.q18qs[]` sequentially. The merge cost is negligible since there are only ~15M distinct orderkeys spread across threads.

## Edit 2: Parallelize Q21 Finalize

The Q21 finalize step at 597.8ms performs a nested loop: for each qualifying order, it scans all lineitem entries for that order to check EXISTS (other supplier) and NOT EXISTS (other late supplier). This is embarrassingly parallel across orders.

The fix:
1. Flattens the `by_ok` map into a vector of tasks for parallel iteration
2. Uses `#pragma omp parallel for schedule(dynamic, 256)` to distribute orders across threads
3. Each thread accumulates into a local `numwait` map, merged after the parallel region

Dynamic scheduling handles the varying per-order workload (orders have different numbers of line items).

## Expected Impact

- Q18 atomic elimination: ~15-20% reduction in fused_scan_lineitem time (conservative estimate)
- Q21 finalize parallelization: ~3-4x speedup on the 597ms step → ~150ms
- Combined: ~500-900ms batch total reduction (~15-25%)
