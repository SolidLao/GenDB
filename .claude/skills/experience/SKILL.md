---
name: experience
description: Correctness and performance rules learned from past GenDB runs. ALWAYS load this skill - it contains critical rules about date handling, hash tables, DECIMAL encoding, dictionary columns, and common bugs. Every agent should check these rules.
user-invocable: false
---

# Experience: Correctness & Performance Rules
# Updated by DBA agent after each run. Checked by Code Inspector.

## Correctness (Critical — always check)

C1: DATE columns = int32_t epoch days. Use date_utils.h for all conversions. NEVER write custom date functions. [freq: 8, sev: HIGH]
C2: Dictionary-encoded columns — load _dict.txt at runtime. NEVER hardcode dictionary codes. [freq: 3, sev: HIGH]
C7: DATE + INTERVAL — use gendb::add_years/add_months/add_days. NEVER +-N*365 or +-N*30. [freq: 4, sev: HIGH]
C9: Hash table capacity MUST use next_power_of_2(count * 2) for ≤50% load factor. Join build-side hash tables: size for full BUILD-SIDE cardinality per thread. Aggregation thread-local hash tables: size for full GROUP cardinality (use FILTERED count per P23, not raw table size). **Memory guard**: always compute total_agg_mem = nthreads × cap × slot_size and compare against hardware LLC — see aggregation-optimization skill: Memory Budget Gate for strategy selection. [freq: 8, sev: HIGH]
C11: Call gendb::init_date_tables() once at top of main() before any extract_year/month/day. [freq: 3, sev: HIGH]
C15: Hash map key MUST include ALL GROUP BY columns. Missing dimension → wrong aggregation groups. Diagnostic: if value AND count are both off by the SAME exact integer factor N simultaneously (e.g. both exactly 2× or 0.5×), a GROUP BY key dimension is missing. [freq: 5, sev: HIGH]
C19: Zone-map block skip ONLY on columns with zone-map indexes per Query Guide. Never on unsorted columns. [freq: 2, sev: HIGH]
C20: NEVER memset() for multi-byte sentinels. memset(buf, 0x80, n) → 0x80808080 ≠ INT32_MIN. Use std::fill(). [freq: 5, sev: HIGH]
C24: ALL hash tables MUST use bounded probing: for-loop with probe < cap, not unbounded while. [freq: 7, sev: HIGH]

## Correctness — Data-Dependent

C4: NOT EXISTS — check if ANY OTHER supplier Z!=suppkey has the condition. [freq: 2, sev: MED]
C6: LIKE on dictionary-encoded column — load dict, find matching codes, filter by code membership. [freq: 2, sev: MED]
C13: Optimizer MUST NOT change date constants from passing iterations. Preserve original values. [freq: 2, sev: HIGH]
C18: Dictionary output — decode code to string via dict[code].c_str(), not raw int16_t. [freq: 3, sev: MED]
C22: Database path — use EXACT path from Query Guide. Never derive from scale factor string. [freq: 2, sev: MED]
C23: Repeated crash/timeout with no [TIMING] → suspect C9 or C20. Audit ALL hash tables. [freq: 3, sev: HIGH]

C29: Large-magnitude double columns (values up to 10^15-10^16) — double has only ~15.95 sig-digit precision. Accumulating and printing %.2f produces last-digit mismatches vs reference (+-0.01). Kahan summation does NOT fix this. MANDATORY for ANY SUM/AVG aggregation where data sampling shows column max >= 10^10 AND output requires decimal precision. FP compensation (Kahan/Neumaier) does NOT fix this — including long double Kahan. Detect: validation fails with +-0.01 diffs in rows with values >= 10^13. Fix: accumulate as int64_t cents (iv = llround(v * 100.0); sum += iv) then output as sum/100 + "." + abs(sum%100). CRITICAL: if storage_design.json annotates a column with "C29 WARNING: SUM must accumulate as int64_t cents", treat this as a HARD CONSTRAINT — reject any KahanSum struct or double/long double accumulator for that column. Seen on SEC EDGAR num.value column (max 1e16): Q3 failed all 6 iters because Kahan was used instead of int64_t cents. [freq: 4, sev: HIGH]

## Correctness — Conditional (int64_t DECIMAL encoding only)

C3: DECIMAL scale_factor — every threshold, AVG divisor, HAVING comparison must use scale_factor from Query Guide. [freq: 2, sev: HIGH]
C5: SUM(col_a * col_b) with int64_t — divide by scale after each multiplication. [freq: 2, sev: HIGH]
C14: Revenue formula: ep * (scale_factor - discount_column) / scale_factor. Keep canonical pattern. (TPC-H example — the underlying PRINCIPLE of canonical formula preservation is general.) [freq: 2, sev: MED]
C17: Asymmetric scale factors in multi-column expressions — each product needs ONE /scale division. [freq: 1, sev: MED]

C30: Exact N× factor mismatch across ALL aggregate columns simultaneously = missing GROUP BY key. Diagnostic signature: expected_value / actual_value = exact integer N AND expected_count / actual_count = same N. This means N distinct groups with the same partial key were merged into one (summing N copies of the data). Q6 iter_2: BUENAVENTURA expected 210 filings / value 5.82e12, got 105 / 2.91e12 (exactly ½ — two groups split). Fix: audit hash map key struct against full GROUP BY clause; add missing dimension. [freq: 2, sev: HIGH]

## Correctness — Optimizer Behavioral

C25: rows_actual == 0 with rows_expected > 0 → filter elimination. Revert to last passing filter logic. Without a passing baseline, the optimizer has no safe anchor — establishing a correct (even slow) baseline is first priority. [freq: 3, sev: HIGH]
C26: Correct row count but wrong values after join restructure → revert build_joins + main_scan to last passing iteration. [freq: 2, sev: HIGH]
C27: Pre-built index refactoring requires ALL 5 elements updated atomically: (1) struct definition (PSSlot etc.), (2) mmap call for index file (xxx_index_raw), (3) header parse (cap, mask=cap-1, ht pointer from raw+4), (4) hash function name, (5) ALL probe loop sites. Missing any one → compile fail. Error signatures: 'XXXSlot does not name a type', 'xxx_raw not declared in this scope', 'is_mask/is_cap/is_ht not declared in this scope'. Most common missing piece: element (3) — file is mmap'd but cap/mask/ht never extracted from header before probe loop. [freq: 2, sev: HIGH]
C28: Hash function rename must propagate to ALL call sites. Error: 'ps_hash not declared; did you mean cps_hash?' — renamed in definition but probe loop still calls old name. [freq: 1, sev: HIGH]

C31: CSV output — always double-quote ALL string/dictionary output columns. Unquoted strings containing commas cause CSV column shift: parser splits at the comma, shifting every subsequent column by +1. Diagnostic: validation shows string_mismatch on multiple adjacent columns with correct row count; expected value in col N appears in col N+1 actual. Fix: printf("\"%s\"", str.c_str()) for EVERY string column, regardless of expected content. Seen Q4 iter_2: tlabel="Equity, Including Portion..." split to tlabel="Equity", stmt="Including Portion...". [freq: 2, sev: HIGH]

C32: Pre-built index header parse must declare ALL THREE variables at function scope before any loop that uses them: (1) uint32_t X_cap = *(const uint32_t*)X_raw; (2) uint32_t X_mask = X_cap - 1; (3) const XSlot* X_ht = (const XSlot*)(X_raw + 4); If declared inside a data_loading lambda, if-block, or #pragma section that is not visible to main_scan, the compiler reports 'X_mask not declared in this scope'. Seen Q6 iter_4: is_mask/is_cap/is_ht declared in wrong scope. Fix: hoist all three declarations to run_query() function scope. [freq: 2, sev: HIGH]

C33: TOP-N queries — always include a stable unique tiebreaker as the LAST sort key. When ORDER BY primary key has ties, std::sort / std::partial_sort produces non-deterministic ordering that differs from reference. Diagnostic: row count correct; first K rows match reference; rows K+1...N contain the correct SET of entities but in wrong ORDER (permuted). Fix: append unique secondary key (cik, entity_id, adsh, or lexicographic name) to every TOP-N comparator: if (a->val != b->val) return a->val > b->val; return a->id < b->id; Seen Q3 iter_0 (fixed in iter_1) and Q6 (name_code ASC tiebreaker applied in plan from iter_1). [freq: 3, sev: HIGH]

C34: Optimizer MUST lock in thread-local aggregation once validated — NEVER revert to shared map. Once thread-local aggregation_merge achieves >20% speedup over a shared-map baseline, preserve the thread-local struct, per-thread map allocation, and merge loop in ALL subsequent optimization iterations. Only hash function, key layout, or capacity may be tuned. Diagnostic for reversion: aggregation_merge spikes by >50% vs best previously seen value with no change in group cardinality or thread count. Seen Q2 iter_4 (aggregation_merge 19ms→87ms regression) and Q4 iter_5 (13ms→59ms regression) after optimizer "tried something different". Fix: track best aggregation_merge timing; if current iter's aggregation_merge > 1.5× best_seen, discard and revert the aggregation strategy to the best known version. [freq: 2, sev: HIGH]

## Performance (Suggestions — non-blocking)

P1: Replace std::unordered_map with open-addressing for >1000 entries. 2-5x faster. [impact: 2-5x, freq: 6]
P2: Don't copy mmap'd data into vectors. Use reinterpret_cast<T*> for zero-copy. [impact: 500ms+, freq: 4]
P3: Hash-based aggregation instead of sort+group for >1M elements. O(n) vs O(n log n). [impact: 2-3x, freq: 3]
P4: Single-pass over large tables. Don't mmap same file multiple times. [impact: 2-3x, freq: 3]
P5: Use GENDB_PHASE("name") from timing_utils.h, not manual #ifdef timing. [impact: N/A, freq: 2]
P6: LIMIT queries — std::partial_sort or min-heap. O(n log k) not O(n log n). [impact: 2x, freq: 2]
P11: Use pre-built hash indexes via mmap. Zero build cost vs 100-500ms runtime build. [impact: 100-500ms, freq: 6]
P12: Track best (timing_ms, source). If >20% regression, revert and try different direction. [impact: N/A, freq: 4]
P13: Zone-map-guided selective madvise when <50% blocks qualify. Don't WILLNEED entire column. [impact: 2-5x I/O, freq: 2]
P14: Separate data_loading phase (GENDB_PHASE). Consolidate all mmap + madvise there. [impact: diagnostics, freq: 3]
P15: Thread-local merge via shared hash table with atomics, not sequential unordered_map merge. [impact: 10-100x merge, freq: 5]
P16: data_loading >40% hot runtime OR >150ms absolute → apply zone-map-guided prefetch AND concurrent multi-index madvise (P27) AND parallel dict loading (P29). Q6 current run: data_loading=258ms (32% of 822ms) — fell under the 40% ratio threshold but remained the dominant bottleneck; absolute threshold now triggers. SEC-EDGAR run: Q6 iter_5 combined P27+P29 → 132ms→31ms (-76%). [impact: 30-76%, freq: 5]
P17: aggregation_merge >20ms → switch to thread-local per-key reduction then single merge pass. Universal win: Q1 (49ms→2ms), Q6 (320ms→3ms), Q24 (167ms→1ms). Apply by default in code generation. WARNING: shared unordered_map merge across 64 threads = catastrophic (Q6 iter_1: 46,800ms). If aggregation_merge stays >100ms across multiple iters with no improvement, thread-local merge was NOT applied at all. Even with thread-local + partitioned merge (P25), 300K groups × 64 threads = 19M slot scans → 261ms (Q6 current run); this is a fundamental lower bound for that group cardinality. SEC-EDGAR run: Q6 350ms→13ms, Q2 67ms→19ms. [impact: 10-46800ms, freq: 8]
P18: build_joins >300ms on orderkey join → use pre-built lineitem_orderkey_hash mmap index (P11). Eliminates build phase entirely. (TPC-H example — the underlying PRINCIPLE of pre-built index usage is general.) [impact: 300-400ms, freq: 1]
P19: Large PRE hash table (>10M rows → CAP=2^25, footprint >500MB) → build_joins 19-65s due to LLC cache thrashing during parallel CAS fill. Apply P11 (pre-built mmap index) for PRE-table joins. If no pre-built index available, partition build by hash-prefix buckets sized for L3 cache. Seen Q24 iter_0 (51s), best iter_2 (19s still slow). [impact: 19-65s, freq: 1]
P20: Thread-local aggregation_merge should be the DEFAULT pattern — a single shared aggregation map causes severe contention at scale (Q6 iter_1: 46.8s). Thread-local reduction eliminates 10-300ms+ overhead with no correctness risk. Exceptions: very low group count where atomic updates are trivially cheap. Q24 iter_3: 167ms→1ms. Catastrophic threshold is LOWER than expected: 50K groups × 64 threads → 350ms (Q6 SEC-EDGAR run iter_0). See aggregation-optimization skill for merge strategy selection. [impact: 10-46800ms, freq: 6]
P21: Anti-join or semi-join on build side >1M rows → use bloom filter pre-filtering (see data-structures skill: Bloom Filter, join-optimization skill: Strategy Selection Framework). Bloom uses ~1.5 bytes/item vs 8-16 bytes/item for hash table. Eliminates 80-99% of probes. [freq: 1, sev: HIGH]
P22: Hash table/array initialization >100MB on multi-core system → use mmap(MAP_ANONYMOUS) + #pragma omp parallel for to distribute page faults across cores. Single-threaded std::fill on large allocations causes sequential page fault stall (measured: 49s for 512MB). [freq: 1, sev: HIGH]
P23: Hash table capacity MUST be based on FILTERED row count, not raw table size. Over-sizing wastes L3 cache and causes LLC thrashing. Example: 548K filtered rows → cap = next_power_of_2(548K * 2) = 1M (16MB), not 9.6M * 2 = 33M (512MB). [freq: 2, sev: MED]
P25: Thread-local occupied-slot tracking + parallel sort-merge can still be slow at high thread counts if per-thread maps are oversized. Fix: (1) apply P23 to aggregation (size AGG_CAP to actual group cardinality, not a large ceiling); (2) use merge strategy from aggregation-optimization skill (partitioned merge or scan-time partitioned aggregation based on total memory vs LLC). If partitioned merge still dominates, evaluate scan-time partitioned aggregation via Memory Budget Gate. [impact: 100-400ms, freq: 2]
P26: Inner join with hash table > L3 cache and probe >> build → add bloom pre-filter on build keys. Reduces random hash probes by 90-99%. Diagnostic: main_scan slow despite moderate build-side cardinality; hash_table_bytes > L3_cache_mb × 1048576. See join-optimization skill: Bloom Pre-Filter. [impact: 30-60% main_scan, freq: 1]
P27: Large multi-index data_loading (2+ pre-built mmap index files each >30MB) → use concurrent madvise across index files. Sequential madvise(MADV_WILLNEED) serializes I/O prefetch; parallel sections let the OS prefetch all indexes simultaneously. Trigger: data_loading >150ms absolute with 2+ index files. Implementation: #pragma omp parallel sections { section: madvise(sub_idx_raw, sub_sz, MADV_WILLNEED); section: madvise(pre_idx_raw, pre_sz, MADV_WILLNEED); }. Also: only madvise columns actually referenced in the query plan. Seen Q6 current run: data_loading=258ms loading sub_adsh_index + pre_join_index sequentially. SEC-EDGAR run: Q6 iter_5 applied P27+P29 together → data_loading 132ms→31ms. [impact: 30-76% data_loading, freq: 2]
P28: Thread-local aggregation total memory (nthreads × AGG_CAP × slot_size) exceeding LLC → initialization dominates (page faults + std::fill) and merge scans thrash cache. Diagnostic: build_joins or aggregation_merge dominated by hash map initialization/traversal, not actual computation. Fix: evaluate aggregation-optimization skill: Memory Budget Gate — scan-time partitioned aggregation eliminates large per-thread maps entirely. [impact: 100-400ms, freq: 2]
P29: Parallel dict loading for 3+ dictionary columns. When a query references 3 or more dictionary-encoded columns across different tables (each requiring a separate _dict.txt mmap+parse), sequential loading serializes disk I/O and can consume 20-70ms per dict on HDD. Trigger: query uses 3+ dict columns AND data_loading >80ms. Fix: use #pragma omp parallel sections num_threads(N) where N = number of dict files, with each section loading one dict via mmap+memchr. Keep dict mmap backing pointers alive at function scope so string_views into them remain valid for the duration of run_query(). Seen Q6 iter_5 (sec-edgar): 5 dict files (uom, name, stmt, tag, plabel) loaded in parallel — sequential loading consumed ~70ms; parallel loading reduced data_loading from 132ms to 31ms (-76%). Combined with P27 concurrent index madvise for full effect. [impact: 70-100ms, freq: 1]
