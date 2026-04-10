# Iteration 1 — Edit Rationale

## Bottleneck Analysis

The profile reveals three major hotspots totaling ~1980ms (61% of 3220ms batch time):

1. **fused_scan_lineitem** (1082ms, 34%) — The main lineitem scan loop processes ~60M rows with multiple query branches. Critically, Q18's branch uses `#pragma omp atomic` to accumulate per-orderkey quantities into a shared dense array on every single row. Even without contention (lineitems are roughly sorted by orderkey), the locked instruction overhead at 60M invocations is substantial — estimated 150-300ms.

2. **Q21_finalize** (574ms, 18%) — The finalization uses a nested loop: for each candidate (orderkey, supplier) pair, it re-scans ALL lineitems for that orderkey to check EXISTS (another supplier) and NOT EXISTS (no other late supplier). This is O(candidates × lineitems_per_order), repeatedly accessing the same lineitem data with cache-unfriendly random access patterns.

3. **Q18_finalize** (325ms, 10%) — Linearly scans all ~15M orderkey slots (120MB of doubles) to find the handful where quantity sum exceeds 300. Most slots are zero or below threshold.

## Optimizations Applied

### 1. Q18 Atomic Elimination (fused_scan_lineitem)
Replaced `#pragma omp atomic` on `C.q18qs[ok]` with a thread-local `unordered_map<int32_t, double>`. After the parallel region, thread-local maps are merged sequentially into the shared array. This eliminates locked instructions from the hottest loop. The hashmap overhead is lower than atomic overhead because: (a) hashmap operations are ~5ns amortized, (b) they avoid the memory fence penalty, and (c) each thread's working set fits in L1/L2 cache.

### 2. Q21 Finalize — Single-Pass + Parallelization
Restructured the nested loop to scan each order's lineitems exactly once, collecting distinct supplier sets and late-supplier sets in small vectors. Then checks each candidate against these precomputed sets (O(1)-ish per candidate since supplier count per order is typically 1-7). Additionally, parallelized the entire loop with `#pragma omp parallel for schedule(dynamic, 256)` using thread-local numwait maps. This transforms the algorithm from serial O(C×L) to parallel O((L+C)/T) where C=candidates, L=lineitems, T=threads.

### 3. Q18 Finalize — Populated-Key Tracking
During thread-local merge, we track which orderkeys received any quantity contribution. The finalize loop now iterates only these populated keys (~1.5M unique orders with lineitems) instead of scanning the full 15M-slot array, avoiding ~90% of the memory bandwidth waste.

## Risk Assessment
- Thread-local Q18 hashmaps increase memory slightly but are bounded by per-thread unique orderkeys
- Q21 parallelization uses thread-local accumulators — no data races
- All changes preserve bitmask routing and `MQO_TIME_PHASE` instrumentation
