# Iteration 3: Eliminate Q18 Atomic Bottleneck in Fused Lineitem Scan

## Bottleneck Analysis

The profile shows `fused_scan_lineitem` at 2850ms, consuming 56% of batch time. Within the fused scan loop body, the Q18 branch stands out as uniquely problematic: it is the **only** query branch using `#pragma omp atomic` on a shared data structure (`C.q18qs[ok]`), and it executes on **every single lineitem row** (no filter predicate — just `la & Q18B`).

For SF10, lineitem has ~60 million rows. Each row triggers an atomic double-precision addition. On x86-64, atomic double adds are not natively supported — the compiler emits a compare-and-swap (CAS) loop, which costs ~20-50ns per operation even without contention. Under contention (multiple threads hitting nearby cache lines), this cost increases further due to cache-line bouncing across cores.

Conservative estimate: 60M × 30ns = **1.8 seconds** of atomic overhead, which accounts for ~63% of the 2850ms scan time.

## Change Description

Replaced the `#pragma omp atomic` Q18 accumulation with thread-local dense arrays:

1. **Allocation**: Before the parallel region, each thread gets its own `calloc`-allocated array of size `q18mx` (max orderkey + 1). Thanks to Linux's lazy page allocation, only pages actually written to consume physical memory. Since lineitem is sorted by orderkey and `schedule(static)` partitions contiguously, each thread touches only ~1/nt of the address space.

2. **Accumulation**: Inside the loop, `T.q18[ok] += C.l_qty[i]` replaces the atomic — a simple, branch-free, non-atomic store to thread-local memory.

3. **Merge**: After the parallel scan, a parallel `omp for` loop merges all thread-local arrays into the shared `C.q18qs[]` array. This merge touches each element exactly once per thread, is fully parallelizable, and benefits from sequential access patterns (cache-line friendly).

4. **Cleanup**: Thread-local arrays are freed after merge.

## Expected Impact

- **Scan latency**: ~20-40% reduction (eliminating ~1.2-1.8s of atomic overhead)
- **Memory**: Modest increase in virtual memory (lazy-allocated), ~60MB additional physical memory per thread for the touched pages
- **Correctness**: Preserved — final `C.q18qs[]` values are identical (commutative/associative summation)
- **Other queries**: Unaffected — no changes to any other query branch or accumulator

## Risk Assessment

Low risk. The pattern (thread-local arrays with post-merge) is already used successfully for Q15 (`T.q15[sk]`) in the same function. The Q18 merge is additionally parallelized with `omp parallel for` for efficiency.
