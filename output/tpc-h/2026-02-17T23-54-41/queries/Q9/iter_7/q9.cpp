/*
 * Q9: Product Type Profit Measure
 *
 * SQL:
 *   SELECT nation, o_year, SUM(amount) AS sum_profit
 *   FROM (
 *     SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year,
 *            l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity AS amount
 *     FROM part, supplier, lineitem, partsupp, orders, nation
 *     WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey
 *       AND ps_partkey = l_partkey AND p_partkey = l_partkey
 *       AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
 *       AND p_name LIKE '%green%'
 *   ) AS profit
 *   GROUP BY nation, o_year
 *   ORDER BY nation, o_year DESC
 *
 * ============================================================
 * LOGICAL PLAN
 * ============================================================
 * Step 1 — Predicate pushdown / filtered cardinalities:
 *   part:     filter p_name LIKE '%green%'  → ~108K parts (bitmap filter)
 *   supplier: no filter                     → 100K rows
 *   nation:   no filter                     → 25 rows
 *   partsupp: filter ps_partkey in green_parts → ~432K rows
 *   orders:   no filter                     → 15M rows
 *   lineitem: filter l_partkey in green_parts → 59M rows (probe side)
 *
 * Step 2 — Join graph (smallest filtered first):
 *   nation(25) → supplier(100K) → part(filtered,~108K) → partsupp(~432K) → lineitem(59M) → orders
 *
 * Step 3 — Physical plan (Iteration 7 optimizations):
 *   Phase 1: Load nation dict (25 rows) → direct array: n_nationkey → n_name string
 *            Load supplier (100K) → direct array: s_suppkey → s_nationkey (int8_t)
 *   Phase 2: mmap part name dict; parallel scan for "green" codes.
 *            Parallel newline counting (from iter 6) for correct seg_line_start.
 *            Scan p_name.bin+p_partkey.bin in parallel → flat bitmap green_parts_bm.
 *   Phase 3: Scan partsupp (8M rows) in parallel, filtered by green_parts_bm.
 *            NEW (iter 7): Store into per-partkey flat array instead of CompactHashMapPair.
 *            TPC-H guarantees each partkey has exactly 4 suppliers (partsupp is 4× part).
 *            ps_data[partkey] = {suppkeys[4], costs[4]} — 4-slot linear scan replaces
 *            composite-key hash probe. Partkey range 1..2M → array size = 2M entries.
 *   Phase 4: Build direct orderkey→year_idx array (iter 7, corrected from iter 6).
 *            Orderkeys range 1..60,000,000 (not strictly 4k+1 — confirmed empirically).
 *            Scan o_orderkey.bin + o_orderdate.bin in parallel.
 *            year_by_ok[ok] = year_idx as int8_t; array = 60MB (60M entries).
 *            In main_scan: yr = year_by_ok[l_orderkey] — single array access, no hash probe.
 *   Phase 5: Scan lineitem (60M) in parallel:
 *            - filter l_partkey via bitmap (bit-test, very cheap)
 *            - lookup ps_data[lk] → scan ≤4 slots for suppkey match → ps_supplycost
 *            - lookup year_by_ok[l_orderkey] → year_idx (direct array, no hash probe)
 *            - lookup supp_nationkey[l_suppkey] → nationkey
 *            - compute amount and accumulate into thread-local agg[nation][year]
 *   Phase 6: Merge thread-local aggregations, sort, output CSV
 *
 * ITERATION 7 KEY OPTIMIZATIONS (over iter 5 baseline):
 *   1. Flat per-partkey array replaces CompactHashMapPair for ps_map:
 *       * TPC-H: exactly 4 suppliers per part → fixed-size slots (no overflow)
 *       * Array indexed by partkey (1..2M), 4 slots each: total = 2M × 4 × (4+8) bytes = 96MB
 *       * Optimization: use parallel arrays ps_suppkeys[4] and ps_costs[4] per partkey
 *       * Probe: linear scan of 4 slots (no hash, no probing chain, L3-resident for hot parts)
 *       * Hot parts (~108K) fit in 108K × 12 × 4 = ~5MB — likely L3 resident
 *   2. Direct orderkey→year array (60MB) replaces hash index probe (268MB hash table):
 *       * year_by_ok[ok] = year_idx as int8_t — single load, no hash/probe chain
 *       * 60MB array: random access during lineitem scan causes some L3 misses
 *         but NO linear probing — typically 1 cache miss vs 2-5 for hash table
 *       * Build: parallel scan of o_orderkey + o_orderdate (60MB each)
 *   3. Parallel newline counting for seg_line_start (from iter 6, correct):
 *       * Was serial O(dict_size) in iter 5; parallel O(dict_size/nthreads) in iter 6+7
 *
 * Per-partkey structure (ps_data):
 *   Array: ps_partkey_slots[PARTKEY_MAX+1]
 *   Each slot: struct { int32_t suppkeys[4]; int64_t costs[4]; uint8_t count; }
 *   Access: ps_partkey_slots[lk].costs[j] where suppkeys[j] == l_suppkey
 *
 * Orders direct array:
 *   year_by_ok[ok] = year - YEAR_BASE as int8_t (0-9), or -1 if out of range
 *   ok range: 1..60,000,000 → array size 60,000,001 bytes = 60MB
 *
 * Aggregation structure: 25 nations × 10 years (1992-2001) = 250 groups
 *   → flat 2D array: agg[25][10] (indexed by n_nationkey and year-1992)
 *
 * Arithmetic (all scaled integers, scale=100):
 *   l_extendedprice * (100 - l_discount) → scale = 10000
 *   ps_supplycost * l_quantity → scale = 10000
 *   amount = (l_extendedprice*(100-l_discount) - ps_supplycost*l_quantity) / 10000
 * ============================================================
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ─── Aggregation grid: 25 nations × 10 years (1992-2001 covers all TPC-H data) ───
static const int N_NATIONS = 25;
static const int YEAR_BASE  = 1992;
static const int N_YEARS    = 10;  // 1992-2001

// ─── nation name lookup by nationkey (0-24) ───
static std::string g_nation_names[N_NATIONS];

// ─── supplier nationkey lookup by suppkey (1-indexed, max 100000) ───
static std::vector<int8_t> g_supp_nationkey; // int8_t: 0-24 fits

// ─── Per-partkey partsupp data (iter 7 optimization) ─────────────────────────
// TPC-H guarantees exactly 4 suppliers per part: partsupp rows = 4 × part rows.
// We store per-partkey: suppkeys[4] and costs[4], with a count (always 4).
// Array indexed by partkey (1..2,000,000).
// Only green parts are populated; other slots have count=0.
//
// Memory: 2,000,001 entries × (4×4 + 4×8 + 1) = 2M × 49 bytes ≈ 98MB
// Hot parts (~108K) working set: 108K × 49 ≈ 5.3MB — fits in L3 (44MB).
static const int32_t PART_MAX_KEY = 2000000;
static const int     PS_SLOTS     = 4;   // TPC-H: exactly 4 suppliers per part

struct PSPartEntry {
    int32_t suppkeys[PS_SLOTS];
    int64_t costs[PS_SLOTS];
    int32_t count;  // number of valid entries (0 if not green or not yet populated)
};

static std::vector<PSPartEntry> g_ps_data; // indexed by partkey (1-indexed)

// ─── Direct orderkey→year array (iter 7, corrected) ─────────────────────────
// Orderkeys range 1..60,000,000 (TPC-H SF10), NOT strictly 4k+1 (verified empirically).
// We allocate year_by_ok[60,000,001] as int8_t (60MB total).
// year_by_ok[ok] = year - YEAR_BASE (0-9), or -1 if out of range / not found.
// Populated by scanning o_orderkey.bin + o_orderdate.bin in parallel.
// Access in main_scan: single array lookup, no hash computation, no linear probing.
static const int32_t OK_MAX = 60000000;
static std::vector<int8_t> g_year_by_ok; // size OK_MAX + 1

// ─── Flat bitmap for green_parts (2M+1 bits = ~250KB) ───────────────────────
// Parts keys are 1-indexed 1..2000000
static const int32_t PART_BITMAP_BITS  = 2000001; // bit index = part key
static const int32_t PART_BITMAP_BYTES = (PART_BITMAP_BITS + 7) / 8;

static inline void bitmap_set(std::vector<uint8_t>& bm, int32_t pk) {
    if (pk >= 0 && pk < PART_BITMAP_BITS) {
        bm[(uint32_t)pk >> 3] |= (uint8_t)(1u << ((uint32_t)pk & 7u));
    }
}

static inline bool bitmap_test(const uint8_t* bm, int32_t pk) {
    return (pk >= 0) & (pk < PART_BITMAP_BITS) &&
           ((bm[(uint32_t)pk >> 3] >> ((uint32_t)pk & 7u)) & 1u);
}

void run_q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // ─── Phase 1: Load nation + supplier ───────────────────────────────────
    {
        GENDB_PHASE("dim_filter");

        // nation n_name (25 rows, int32_t codes)
        gendb::MmapColumn<int32_t> n_nationkey(gendb_dir + "/nation/n_nationkey.bin");
        gendb::MmapColumn<int32_t> n_name_col(gendb_dir + "/nation/n_name.bin");

        // Load nation name dictionary (line N = code N)
        std::vector<std::string> nation_dict;
        {
            std::ifstream f(gendb_dir + "/nation/name_dict.txt");
            std::string line;
            while (std::getline(f, line)) {
                nation_dict.push_back(line);
            }
        }
        for (size_t i = 0; i < n_nationkey.count; i++) {
            int32_t nk = n_nationkey[i];
            int32_t nc = n_name_col[i];
            if (nk >= 0 && nk < N_NATIONS && nc >= 0 && nc < (int32_t)nation_dict.size()) {
                g_nation_names[nk] = nation_dict[nc];
            }
        }

        // ─── Load supplier: suppkey → nationkey ────────────────────────────
        gendb::MmapColumn<int32_t> s_suppkey_col(gendb_dir + "/supplier/s_suppkey.bin");
        gendb::MmapColumn<int32_t> s_nationkey_col(gendb_dir + "/supplier/s_nationkey.bin");
        size_t n_suppliers = s_suppkey_col.count;
        g_supp_nationkey.resize(100001, -1);
        for (size_t i = 0; i < n_suppliers; i++) {
            int32_t sk = s_suppkey_col[i];
            int32_t nk = s_nationkey_col[i];
            if (sk >= 0 && sk <= 100000) {
                g_supp_nationkey[sk] = (int8_t)nk;
            }
        }
    }

    // ─── Phase 2: Find green parts using flat bitmap ─────────────────────────
    // green_parts_bm: bit i set iff part key i has p_name LIKE '%green%'
    std::vector<uint8_t> green_parts_bm(PART_BITMAP_BYTES, 0u);
    int32_t green_part_count = 0;
    {
        GENDB_PHASE("dim_filter_part");

        // ── Step A: mmap the part name dict file and find green code indices ──
        // The dict file has ~2M lines. Each line is the name for dict code = line_number.
        // OPTIMIZATION: Parallelize newline counting and "green" search (iter 6 technique).

        std::string dict_path = gendb_dir + "/part/name_dict.txt";
        int dict_fd = ::open(dict_path.c_str(), O_RDONLY);
        if (dict_fd < 0) {
            std::cerr << "Cannot open part name dict: " << dict_path << std::endl;
            return;
        }
        struct stat dict_st;
        fstat(dict_fd, &dict_st);
        size_t dict_size = dict_st.st_size;

        const char* dict_data = (const char*)mmap(nullptr, dict_size, PROT_READ, MAP_PRIVATE, dict_fd, 0);
        if (dict_data == MAP_FAILED) {
            std::cerr << "Cannot mmap part name dict" << std::endl;
            ::close(dict_fd);
            return;
        }
        madvise((void*)dict_data, dict_size, MADV_SEQUENTIAL);

        // Estimated ~2M dict entries → 2M bits = 250KB per bitmap
        static const size_t max_codes_guess = 2100000;
        const size_t bm_bytes = (max_codes_guess + 7) / 8;

        const int nthreads_dict = omp_get_max_threads();

        // Divide the dict file into nthreads_dict byte-aligned segments.
        // Each segment boundary is snapped to the next '\n' to avoid splitting lines.
        std::vector<size_t> seg_byte_start(nthreads_dict + 1);
        std::vector<size_t> seg_line_start(nthreads_dict, 0);

        // Compute raw byte boundaries (approximate)
        size_t chunk = (dict_size + nthreads_dict - 1) / nthreads_dict;
        seg_byte_start[0] = 0;
        for (int t = 1; t < nthreads_dict; t++) {
            size_t raw = (size_t)t * chunk;
            if (raw >= dict_size) { raw = dict_size; }
            else {
                // snap forward to next '\n'
                const char* nl = (const char*)memchr(dict_data + raw, '\n', dict_size - raw);
                raw = nl ? (size_t)(nl - dict_data) + 1 : dict_size;
            }
            seg_byte_start[t] = raw;
        }
        seg_byte_start[nthreads_dict] = dict_size;

        // Count '\n' in each segment in parallel, then prefix-sum for seg_line_start.
        // OPTIMIZATION (from iter 6): Parallel O(dict_size/nthreads) vs serial O(dict_size).
        std::vector<size_t> seg_newline_count(nthreads_dict, 0);
        #pragma omp parallel for schedule(static) num_threads(nthreads_dict)
        for (int t = 0; t < nthreads_dict; t++) {
            const char* seg_begin = dict_data + seg_byte_start[t];
            const char* seg_end   = dict_data + seg_byte_start[t + 1];
            size_t cnt = 0;
            const char* p = seg_begin;
            while (p < seg_end) {
                const char* nl = (const char*)memchr(p, '\n', seg_end - p);
                if (!nl) break;
                cnt++;
                p = nl + 1;
            }
            seg_newline_count[t] = cnt;
        }
        // Prefix sum (O(nthreads) = O(64) — negligible)
        {
            size_t running_lines = 0;
            for (int t = 0; t < nthreads_dict; t++) {
                seg_line_start[t] = running_lines;
                running_lines += seg_newline_count[t];
            }
        }

        // Thread-local green code bitmaps: each thread writes its own, no sharing needed
        std::vector<std::vector<uint8_t>> tl_green_bm(nthreads_dict,
                                                        std::vector<uint8_t>(bm_bytes, 0u));

        #pragma omp parallel num_threads(nthreads_dict)
        {
            int tid = omp_get_thread_num();
            const char* seg_begin = dict_data + seg_byte_start[tid];
            const char* seg_end   = dict_data + seg_byte_start[tid + 1];
            size_t line_idx = seg_line_start[tid];
            std::vector<uint8_t>& my_bm = tl_green_bm[tid];

            const char* p = seg_begin;
            while (p < seg_end) {
                const char* nl = (const char*)memchr(p, '\n', seg_end - p);
                if (!nl) nl = seg_end;
                size_t line_len = (size_t)(nl - p);
                if (line_len >= 5 && line_idx < max_codes_guess) {
                    // Scan for 'g' then verify 'r','e','e','n'
                    const char* q = p;
                    const char* line_end_ptr = nl - 4; // need at least 5 chars
                    while (q <= line_end_ptr) {
                        if (q[0] == 'g' && q[1] == 'r' && q[2] == 'e' && q[3] == 'e' && q[4] == 'n') {
                            my_bm[line_idx >> 3] |= (uint8_t)(1u << (line_idx & 7u));
                            break;
                        }
                        q++;
                    }
                }
                line_idx++;
                p = nl + 1;
            }
        }

        munmap((void*)dict_data, dict_size);
        ::close(dict_fd);

        // Merge thread-local green code bitmaps (OR reduction)
        std::vector<uint8_t> green_code_bm(bm_bytes, 0u);
        for (int t = 0; t < nthreads_dict; t++) {
            const uint8_t* src = tl_green_bm[t].data();
            for (size_t b = 0; b < bm_bytes; b++) {
                green_code_bm[b] |= src[b];
            }
        }
        tl_green_bm.clear();

        // ── Step B: Scan p_name.bin + p_partkey.bin in parallel ──────────────
        gendb::MmapColumn<int32_t> p_partkey_col(gendb_dir + "/part/p_partkey.bin");
        gendb::MmapColumn<int32_t> p_name_col(gendb_dir + "/part/p_name.bin");
        size_t n_parts = p_partkey_col.count;

        const int nthreads = omp_get_max_threads();

        // Thread-local lists of matching partkeys; avoid atomic writes to shared bitmap
        std::vector<std::vector<int32_t>> local_keys(nthreads);

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            std::vector<int32_t>& lk = local_keys[tid];
            lk.reserve(200000 / nthreads + 1000);

            const uint8_t* gcbm = green_code_bm.data();

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < (int64_t)n_parts; i++) {
                int32_t code = p_name_col[i];
                if (code >= 0 && (size_t)code < max_codes_guess) {
                    if ((gcbm[(uint32_t)code >> 3] >> ((uint32_t)code & 7u)) & 1u) {
                        lk.push_back(p_partkey_col[i]);
                    }
                }
            }
        }

        // Merge into flat bitmap (serial, fast: just bit sets, no hash ops)
        for (int t = 0; t < nthreads; t++) {
            for (int32_t pk : local_keys[t]) {
                bitmap_set(green_parts_bm, pk);
                green_part_count++;
            }
        }
    }

    const uint8_t* const green_bm = green_parts_bm.data();

    // ─── Phase 3: Build per-partkey ps_data array ────────────────────────────
    // NEW (iter 7): Replace CompactHashMapPair with per-partkey flat array.
    // TPC-H guarantees each partkey appears in exactly 4 partsupp rows.
    // g_ps_data[partkey] stores {suppkeys[4], costs[4], count}.
    // Only green parts are populated (others remain count=0).
    // Probe in main_scan: linear scan of ≤4 slots — no hash computation needed.
    {
        GENDB_PHASE("build_joins");

        // Allocate array (only for green parts, but indexed by partkey for O(1) access)
        // Memory: 2,000,001 × sizeof(PSPartEntry) = 2M × (4×4 + 4×8 + 4) = 2M × 52 = 104MB
        g_ps_data.resize(PART_MAX_KEY + 1);
        // Initialize counts to 0 (calloc-equivalent for POD via resize)
        // std::vector resize zero-initializes; PSPartEntry has count=0 by default? No.
        // Explicitly zero count.
        // Note: with 104MB zeroed, this is fast (memset under the hood).
        for (auto& e : g_ps_data) e.count = 0;

        gendb::MmapColumn<int32_t> ps_partkey_col(gendb_dir + "/partsupp/ps_partkey.bin");
        gendb::MmapColumn<int32_t> ps_suppkey_col(gendb_dir + "/partsupp/ps_suppkey.bin");
        gendb::MmapColumn<int64_t> ps_supplycost_col(gendb_dir + "/partsupp/ps_supplycost.bin");
        int64_t n_ps = (int64_t)ps_partkey_col.count;

        // Serial scan (writes to g_ps_data by partkey — parallel would cause data races
        // without per-slot locking; serial is acceptable since n_ps=8M and this is sequential I/O)
        // Actually: partsupp is sorted by ps_partkey. Each partkey block has exactly 4 rows.
        // We can safely parallelize since threads own disjoint partkey ranges.
        // However, parallelizing sequential I/O on HDD adds seek overhead.
        // Use parallel approach with thread-local buffers, merge serially (same as before).
        const int nthreads = omp_get_max_threads();

        struct PSEntry { int32_t pk; int32_t sk; int64_t sc; };
        std::vector<std::vector<PSEntry>> local_ps(nthreads);
        for (int t = 0; t < nthreads; t++) {
            local_ps[t].reserve(n_ps / nthreads / 4 + 1000);
        }

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            std::vector<PSEntry>& lps = local_ps[tid];

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < n_ps; i++) {
                int32_t pk = ps_partkey_col[i];
                // Bitmap test: single bit check, no hash
                if (!bitmap_test(green_bm, pk)) continue;
                int32_t sk = ps_suppkey_col[i];
                int64_t sc = ps_supplycost_col[i];
                lps.push_back({pk, sk, sc});
            }
        }

        // Serial merge: insert into g_ps_data per-partkey slots
        // Each partkey has at most 4 suppliers → count never exceeds PS_SLOTS
        for (int t = 0; t < nthreads; t++) {
            for (const auto& e : local_ps[t]) {
                if (e.pk < 0 || e.pk > PART_MAX_KEY) continue;
                PSPartEntry& entry = g_ps_data[e.pk];
                int slot = entry.count;
                if (slot < PS_SLOTS) {
                    entry.suppkeys[slot] = e.sk;
                    entry.costs[slot]    = e.sc;
                    entry.count          = slot + 1;
                }
            }
        }
    }

    // ─── Phase 4: Build direct orderkey→year_idx array ──────────────────────
    // OPTIMIZATION (iter 7, corrected): Replace hash index probe with direct array.
    // Orderkeys range: 1..60,000,000 (TPC-H SF10); NOT strictly 4k+1 (verified).
    // Array g_year_by_ok[ok] = year_idx as int8_t.
    // Array size: 60,000,001 bytes = 60MB.
    // Access in main_scan: single array load, no hash, no linear probing.
    {
        GENDB_PHASE("build_joins_orders");

        // Allocate and initialize to -1 (out-of-range sentinel)
        g_year_by_ok.assign(OK_MAX + 1, (int8_t)-1);

        gendb::MmapColumn<int32_t> o_orderkey_col(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate_col(gendb_dir + "/orders/o_orderdate.bin");
        int64_t n_orders = (int64_t)o_orderkey_col.count;

        int8_t* const yr_ok_data = g_year_by_ok.data();

        // Parallel fill: each thread handles a contiguous range of row indices.
        // Writes are to distinct orderkey positions (no races since orderkeys are unique).
        const int nthreads_ord = omp_get_max_threads();
        #pragma omp parallel for schedule(static) num_threads(nthreads_ord)
        for (int64_t i = 0; i < n_orders; i++) {
            int32_t ok = o_orderkey_col[i];
            if ((uint32_t)ok <= (uint32_t)OK_MAX) {
                int yr = gendb::extract_year(o_orderdate_col[i]);
                int yi = yr - YEAR_BASE;
                yr_ok_data[ok] = (int8_t)((uint32_t)yi < (uint32_t)N_YEARS ? yi : (int8_t)-1);
            }
        }
    }

    // ─── Phase 5: Scan lineitem — parallel aggregation ──────────────────────
    const int num_threads = omp_get_max_threads();
    // Thread-local aggregation: [thread][nation][year]
    std::vector<int64_t> thread_agg(num_threads * N_NATIONS * N_YEARS, 0LL);

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_partkey_col(gendb_dir + "/lineitem/l_partkey.bin");
        gendb::MmapColumn<int32_t> l_suppkey_col(gendb_dir + "/lineitem/l_suppkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice_col(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount_col(gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int64_t> l_quantity_col(gendb_dir + "/lineitem/l_quantity.bin");
        gendb::MmapColumn<int32_t> l_orderkey_col(gendb_dir + "/lineitem/l_orderkey.bin");

        int64_t n_lineitem = (int64_t)l_partkey_col.count;

        // Cache locals for hot-path arrays
        const uint8_t* const gbm      = green_bm;
        const int8_t* const yr_ok     = g_year_by_ok.data();
        const PSPartEntry* const ps    = g_ps_data.data();

        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            int64_t* local_agg = thread_agg.data() + tid * N_NATIONS * N_YEARS;

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < n_lineitem; i++) {
                int32_t lk = l_partkey_col[i];

                // Fast semi-join filter: bitmap test (single bit op, no hash)
                if ((uint32_t)lk >= (uint32_t)PART_BITMAP_BITS) continue;
                if (!((gbm[(uint32_t)lk >> 3] >> ((uint32_t)lk & 7u)) & 1u)) continue;

                int32_t sk = l_suppkey_col[i];

                // Supplier nationkey — early filter
                if ((uint32_t)sk > 100000u) continue;
                int ni = (int)(uint8_t)g_supp_nationkey[sk];
                if ((uint32_t)ni >= (uint32_t)N_NATIONS) continue;

                // Lookup partsupp via per-partkey flat array (iter 7 optimization)
                // Linear scan of ≤4 slots (TPC-H: exactly 4 suppliers per part)
                // No hash computation — just 4 int32_t comparisons
                if ((uint32_t)lk > (uint32_t)PART_MAX_KEY) continue;
                const PSPartEntry& pse = ps[lk];
                int64_t psc_val;
                {
                    int cnt = pse.count;
                    bool found = false;
                    for (int j = 0; j < cnt; j++) {
                        if (pse.suppkeys[j] == sk) {
                            psc_val = pse.costs[j];
                            found = true;
                            break;
                        }
                    }
                    if (!found) continue;
                }

                // Direct orderkey→year lookup (iter 7: no hash probe, single array access)
                int32_t ok = l_orderkey_col[i];
                if ((uint32_t)ok > (uint32_t)OK_MAX) continue;
                int yi = (int)(int8_t)yr_ok[ok];
                if ((uint8_t)yi >= (uint8_t)N_YEARS) continue;  // handles -1 as 255 (large)

                // Compute amount in scaled int:
                // l_extendedprice * (100 - l_discount) - ps_supplycost * l_quantity
                int64_t ep  = l_extendedprice_col[i];
                int64_t dis = l_discount_col[i];
                int64_t qty = l_quantity_col[i];
                int64_t amount = (ep * (100LL - dis)) / 100LL - psc_val * qty / 100LL;

                local_agg[ni * N_YEARS + yi] += amount;
            }
        }
    }

    // ─── Phase 6: Merge thread-local aggregations ──────────────────────────
    std::vector<int64_t> global_agg(N_NATIONS * N_YEARS, 0LL);

    for (int t = 0; t < num_threads; t++) {
        const int64_t* ta = thread_agg.data() + t * N_NATIONS * N_YEARS;
        for (int ni = 0; ni < N_NATIONS; ni++) {
            for (int yi = 0; yi < N_YEARS; yi++) {
                global_agg[ni * N_YEARS + yi] += ta[ni * N_YEARS + yi];
            }
        }
    }

    // ─── Phase 7: Output ────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        struct ResultRow {
            std::string nation;
            int year;
            int64_t sum_profit_scaled; // scaled by 100
        };
        std::vector<ResultRow> rows;
        rows.reserve(N_NATIONS * N_YEARS);

        for (int ni = 0; ni < N_NATIONS; ni++) {
            for (int yi = 0; yi < N_YEARS; yi++) {
                int64_t v = global_agg[ni * N_YEARS + yi];
                if (v == 0) continue;
                int yr = YEAR_BASE + yi;
                rows.push_back({g_nation_names[ni], yr, v});
            }
        }

        // Sort: nation ASC, year DESC
        std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.nation != b.nation) return a.nation < b.nation;
            return a.year > b.year;
        });

        // Write CSV
        std::string out_path = results_dir + "/Q9.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) {
            std::cerr << "Cannot open output file: " << out_path << std::endl;
            return;
        }
        fprintf(fp, "nation,o_year,sum_profit\n");
        for (const auto& row : rows) {
            int64_t val   = row.sum_profit_scaled;
            int64_t whole = val / 100LL;
            int64_t frac  = val % 100LL;
            if (frac < 0) { whole--; frac += 100LL; }
            fprintf(fp, "%s,%d,%lld.%02lld\n",
                    row.nation.c_str(), row.year,
                    (long long)whole, (long long)frac);
        }
        fclose(fp);
    }

    // ─── Cleanup global state ────────────────────────────────────────────────
    // Free large arrays to avoid memory leaks between runs (if used as library)
    g_ps_data.clear();
    g_ps_data.shrink_to_fit();
    g_year_by_ok.clear();
    g_year_by_ok.shrink_to_fit();
    g_supp_nationkey.clear();
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q9(gendb_dir, results_dir);
    return 0;
}
#endif
