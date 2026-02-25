# Stage B Retrospective — TPC-H Run 2026-02-24T23-05-03

## Overall Results

| Query | Status  | Best Timing | Iters | First Pass | Dominant Bottleneck (final) |
|-------|---------|-------------|-------|------------|-----------------------------|
| Q1    | SUCCESS | 49.8 ms     | 5     | iter_0     | main_scan (44.7 ms)         |
| Q3    | SUCCESS | 99.2 ms     | 6     | iter_0     | main_scan (47.5 ms)         |
| Q6    | SUCCESS | 31.3 ms     | 1     | iter_0     | main_scan (30.1 ms)         |
| Q9    | SUCCESS | 56.1 ms     | 2     | iter_0     | main_scan (33.8 ms)         |
| Q18   | SUCCESS | 136.4 ms    | 6*    | iter_0     | subquery_precompute (116 ms)|

*Q18: best was iter_4 (136ms); iter_5 regressed to 163ms and was discarded.

**Summary: 5/5 SUCCESS, 0 SLOW, 0 FAILED. All queries produced correct results on first compilation.**

---

## Per-Query Analysis

### Q1 — Aggregation over lineitem, no joins
- **Trajectory**: 64.9 → 71.3 → 59.5 → 57.2 → **49.8 ms** (steady improvement)
- **iter_1 mini-regression**: output+aggregation_merge 0.28 ms → 1.15 ms before being optimized back down.
- **Key technique**: direct-array aggregation (6 slots indexed by `rf_code*2+ls_code`); 20KB total → L1-resident.
- **Bottleneck at plateau**: main_scan (44.7 ms = 89% of total). Already well-optimized; further gains require SIMD or compiler-vectorization tuning.
- **P35/P36 not triggered** (no dict LIKE filter; data_loading already <0.2 ms on hot runs).
- **Experience triggers**: C35 (long double for derived expressions), C11 (init_date_tables), C7 (add_days).

### Q3 — Customer/Orders/Lineitem 3-way join
- **Trajectory**: 195.1 → 142.6 → 138.1 → 137.4 → 137.1 → **99.2 ms**
- **iter_0 → iter_1**: data_loading collapsed 63.3 ms → 0.1 ms (P36 static mmap caching applied).
- **Plateau iters 2–4**: Three consecutive iterations within 1% of each other (137–138 ms). P31 plateau detection would have saved 2 wasted iterations.
- **Breakthrough at iter_5**: dim_filter collapsed 4.7 ms → 1.0 ms; build_joins 31.9 ms → 24.5 ms; ht_init phase appeared (8.5 ms) indicating a new HT init strategy; net gain ~38 ms.
- **Experience triggers**: P31 (plateau at iters 2–4 confirmed), P36 (data_loading), P30 (compact pre-filter build).

### Q6 — Scan-only with zone-map skip
- **Trajectory**: **31.3 ms** (single iteration, immediate success)
- Perfect application of zone-map block skipping (~85% blocks skipped on l_shipdate), fused block scan, scalar aggregation.
- No optimization iterations needed. Optimizer correctly recognized saturation on first pass.
- **Experience triggers**: C19 (zone-map correct column), C1, C7 (date range), C35 (scalar long double for revenue sum).

### Q9 — 5-way join with LIKE part filter
- **Trajectory**: 149.7 → **56.1 ms** (one-shot 63% improvement)
- **iter_0**: dim_filter = 102 ms (dominant). LIKE '%green%' over part dict scanning slowly.
- **iter_1**: dim_filter collapsed to 13.7 ms — P35 memmem raw dict scan applied. main_scan improved 38 ms → 33.8 ms.
- **Experience triggers**: P35 (memmem dict scan confirmed effective: 102 ms → 13.7 ms, −87%).
- Remaining dim_filter (13.7 ms) likely includes 5-table hash build; still meaningful residual.

### Q18 — Subquery HAVING + large aggregation
- **Trajectory**: 217.0 → 416.8 → 226.6 → 225.7 → **136.4** → 163.2 ms
- **iter_1 regression (+92%)**: subquery_precompute jumped 186 ms → 380 ms. P12 correctly identified regression; iter_2 reverted direction.
- **Iters 2–3 stagnation**: Both ~225 ms; subquery_precompute stuck at 185–197 ms. Optimizer tried different directions.
- **iter_4 breakthrough**: subquery_precompute 197 ms → 116 ms (41% improvement); total 226 ms → 136 ms (40% improvement). C36 orthogonal bit-range hash confirmed effective; P37 formula correctly applied (P1_CAP = next_power_of_2(15M/64×2) = 524288).
- **iter_5 discarded**: main_scan spiked 7.4 ms → 15.4 ms despite similar subquery_precompute (116 ms); total 163 ms > iter_4 best.
- **Residual bottleneck**: subquery_precompute (116 ms = 85% of total). 64-partition scatter/aggregate of 60M lineitem rows is inherently expensive.
- **Experience triggers**: C36 (orthogonal bit ranges), P37 (correct per-partition sizing), P12 (regression revert), P38 (not triggered — partitioned approach preferred over shared XADD at this cardinality).

---

## Pattern Analysis

### Recurring Issues
1. **main_scan dominance** (Q1, Q3, Q6, Q9): In 4/5 queries, main_scan is the final bottleneck (30–47 ms). No further HT/join tuning will help; SIMD vectorization of the scan loop is the next frontier.
2. **Plateau without triggering P31** (Q3): Iters 2–4 were all within 1% of each other. P31 would have stopped at iter_2 and saved 2 wasted iterations before the iter_5 breakthrough. However, the iter_5 breakthrough happened AFTER what would have been the plateau stop — suggesting P31's 3-iteration limit may need a "breakthrough escape" hatch.
3. **subquery_precompute dominance** (Q18): Even at best (116 ms), it is 85% of total. Further improvement needs either a smarter partition count, or alternative aggregation strategy (shared XADD at lower cardinality).
4. **Static mmap caching** (Q3): P36 eliminated 63 ms of redundant I/O on hot runs on first optimization step.

### What Worked Well
- **Direct-array aggregation** (Q1): L1-resident, zero hash overhead.
- **memmem dict scan** (Q9): 87% dim_filter speedup.
- **Partitioned aggregation with orthogonal bits** (Q18): 40% end-to-end improvement.
- **Zone-map scan** (Q6): Immediate best-case performance.
- **Regression detection + revert** (Q18 iter_1→iter_2): P12 logic functioned correctly.
