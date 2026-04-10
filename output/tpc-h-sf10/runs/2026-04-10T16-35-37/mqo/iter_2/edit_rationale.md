# Iteration 2: Hoist Redundant Lookups in Fused Lineitem Scan

## Quantitative Diagnosis

The fused lineitem scan processes 60M rows with 14+ query branches. The critical bottleneck
is redundant random memory access to the `orders_pk_index` array (60M entries × 4B = 240MB),
which is 5.5× the L3 cache size (44MB).

In the original code, `orders_pk_index[ok]` is independently looked up in 8 branches:
Q3, Q5, Q7, Q8, Q9, Q10, Q12, and Q21. Since Q5 has no pre-filter (it checks orow first),
every row performs this lookup when Q5 is active. With all 22 queries active in batch mode,
this means **8 redundant random accesses per row** into a cache-exceeding array.

Similarly, `o_orderdate[orow]` (60MB) is loaded in 5-6 branches, `o_custkey[orow]` (60MB) in
5 branches, and `s_nationkey[sk-1]` (400KB) in 5 branches. The compiler cannot perform
common subexpression elimination (CSE) across branches because intervening `unordered_map`
write operations (`acc.q3_revenue[ok] += ...`) create potential aliasing with the global
mmap'd arrays through the static pointers.

Estimated redundant cache misses: 60M rows × 7 extra lookups × ~50ns/miss = ~21 seconds
serial. With 64-thread parallelism: ~330ms of wasted time.

## Fix Category: B (Algorithm/Implementation Change for Hardware)

The root cause is a physical implementation mismatch: the same expensive random memory access
is repeated across branches because the compiler lacks aliasing information. The fix has two
parts:

1. **Hoist shared lookups**: Compute `orow`, `o_od` (orderdate), `o_ck` (custkey), and
   `snk` (supplier nationkey) once per row at the top of the loop, then reuse cached values
   in all branches. This reduces random accesses to `orders_pk_index` from 8× to 1× per row.

2. **Add `__restrict__` qualifiers**: Declare local `__restrict__` pointers for all column
   arrays at the start of the parallel region. This gives the compiler proof of non-aliasing,
   enabling it to keep values in registers across branches and potentially auto-vectorize
   sequential column scans.

3. **Defer rarely-used column loads**: Move `l_tax`, `l_returnflag`, `l_linestatus`, and
   `l_quantity` loads inside their specific branches (Q1, Q10, Q6, Q19) since they're not
   needed by most branches. This reduces memory bandwidth for the common case.

## Risks

- The hoisted orow lookup now runs unconditionally for ALL rows (when any of the 8 queries is
  active), even rows that might not qualify for any branch. However, since Q5 already did this
  unconditionally in the original code, the net effect is zero extra lookups.
- The `o_od` and `o_ck` loads are conditional on `orow >= 0`, which matches the original
  behavior exactly.
