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
 * LOGICAL PLAN (Iteration 10)
 * ============================================================
 * Step 1 — Predicate pushdown / filtered cardinalities:
 *   part:     filter p_name LIKE '%green%'  → ~108K parts (bitmap filter)
 *   supplier: no filter                     → 100K rows
 *   nation:   no filter                     → 25 rows
 *   partsupp: filter ps_partkey in green_parts → ~432K rows (108K × 4 suppliers)
 *   orders:   no filter                     → 15M rows
 *   lineitem: filter l_partkey in green_parts → 59M rows (probe side)
 *
 * Step 2 — Join graph (smallest filtered first):
 *   nation(25) → supplier(100K) → part(filtered,~108K) → partsupp(~432K)
 *              → lineitem(59M) → orders
 *
 * Step 3 — Physical plan (Iteration 10 optimizations):
 *   Phase 1: Load nation dict (25 rows) → direct array: n_nationkey → n_name string
 *   Phase 2: Load supplier (100K) → direct array: s_suppkey → s_nationkey
 *   Phase 3: Single-pass dict scan + p_name.bin scan → green_parts bitmap (250KB, L3-resident)
 *            Optimization: eliminate the redundant line-counting pass in dim_filter_part.
 *            Build green_code_bm and seg_line_start in ONE combined parallel scan.
 *            Also build green_idx[2000001]: partkey → sequential index (0..N_green-1).
 *   Phase 4: Scan partsupp (8M) in parallel.
 *            Filter ps_partkey via bitmap (bit-test).
 *            Build ps_data[green_idx[pk]] = PSEntry4{suppkey[4], cost[4]} fixed-4 array.
 *            ps_data is ~5MB (108K parts × 4 × 12B) → L3-resident, no hash probing.
 *            PSEntry4 suppkeys stored as int32_t[4] for SIMD comparison.
 *   Phase 5: Build direct year_by_orderkey[60000001] int8_t array from o_orderkey + o_orderdate.
 *            Parallel memset + population. Replaces hash index.
 *   Phase 6: Scan lineitem (60M) in parallel:
 *            - filter l_partkey via bitmap (bit-test, very cheap)
 *            - lookup year via direct array (single indexed access)
 *            - lookup ps_data via green_idx[l_partkey] (direct array)
 *            - SIMD suppkey match: _mm_cmpeq_epi32 on suppkey[4] → bsf for slot index
 *            - lookup supp_nationkey[l_suppkey] (direct array)
 *            - compute amount and accumulate into thread-local agg[nation][year]
 *   Phase 7: Merge thread-local aggregations, sort, output CSV
 *
 * KEY OPTIMIZATIONS vs iter_9:
 *   1. dim_filter_part: Eliminate redundant line-counting pass.
 *      The current iter_9 does TWO parallel passes over the dict file:
 *        Pass 1: count '\n' per segment → prefix sum for seg_line_start
 *        Pass 2: scan for "green" tokens
 *      iter_10: Combine into ONE pass using atomic prefix-sum workaround:
 *        Each thread scans its segment, simultaneously counting lines AND recording green codes.
 *        Since seg_line_start[0] = 0 and each thread independently knows its start line from
 *        seg_byte_start[] boundaries, we use a single-pass with atomic line counter per thread.
 *        This halves the dict file I/O.
 *
 *   2. main_scan: SIMD suppkey comparison.
 *      Replace branchy if/else-if chain (4 compares, up to 3 branches) with:
 *        __m128i sv = _mm_set1_epi32(sk);
 *        __m128i keys = _mm_loadu_si128((__m128i*)e4.suppkey);
 *        int mask = _mm_movemask_epi8(_mm_cmpeq_epi32(sv, keys));
 *        int slot = __builtin_ctz(mask) >> 2;  // 0-3 or 4 if no match
 *      Single instruction vs branchy chain → eliminates branch mispredictions.
 *
 *   3. main_scan: Increase prefetch distance for year_by_orderkey (57MB array).
 *      Large array → more TLB misses. Prefetch distance increased 24→32 rows ahead.
 *
 *   4. build_joins_orders: Parallelize year array initialization (memset).
 *      60M bytes initialized serial in iter_9. Use parallel fill to reduce from ~5ms.
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
#include <immintrin.h>   // SSE4.1 / AVX2 intrinsics
#include <smmintrin.h>   // _mm_cmpeq_epi32, _mm_movemask_epi8

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

// ─── Fixed-4 partsupp entry: each green part has exactly 4 partsupp rows ─────
// TPC-H invariant: every part has exactly 4 supplier entries in partsupp.
// suppkey stored as int32_t[4] for SIMD comparison via _mm_cmpeq_epi32.
struct PSEntry4 {
    int32_t suppkey[4];
    int64_t cost[4];
};

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
    // green_idx[partkey] = sequential index 0..N_green-1 (0xFFFFFFFF if not green)
    // Used to index into ps_data fixed-4 array
    static const uint32_t NOT_GREEN = 0xFFFFFFFFu;
    std::vector<uint32_t> green_idx(PART_BITMAP_BITS, NOT_GREEN);
    int32_t green_part_count = 0;
    {
        GENDB_PHASE("dim_filter_part");

        // ── Step A: mmap the part name dict file and find green code indices ──
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

        static const size_t max_codes_guess = 2100000;
        const size_t bm_bytes = (max_codes_guess + 7) / 8;

        const int nthreads_dict = omp_get_max_threads();

        // Divide the dict file into nthreads_dict byte-aligned segments.
        std::vector<size_t> seg_byte_start(nthreads_dict + 1);
        std::vector<size_t> seg_line_start(nthreads_dict, 0);

        size_t chunk = (dict_size + nthreads_dict - 1) / nthreads_dict;
        seg_byte_start[0] = 0;
        for (int t = 1; t < nthreads_dict; t++) {
            size_t raw = (size_t)t * chunk;
            if (raw >= dict_size) { raw = dict_size; }
            else {
                const char* nl = (const char*)memchr(dict_data + raw, '\n', dict_size - raw);
                raw = nl ? (size_t)(nl - dict_data) + 1 : dict_size;
            }
            seg_byte_start[t] = raw;
        }
        seg_byte_start[nthreads_dict] = dict_size;

        // Thread-local green code bitmaps + line counts — ONE pass combines counting + scanning.
        // iter_9 did TWO passes: (1) count lines, (2) scan for green.
        // iter_10: SINGLE pass: each thread simultaneously counts lines AND marks green codes.
        std::vector<std::vector<uint8_t>> tl_green_bm(nthreads_dict,
                                                        std::vector<uint8_t>(bm_bytes, 0u));
        std::vector<size_t> seg_line_count(nthreads_dict, 0);

        #pragma omp parallel num_threads(nthreads_dict)
        {
            int tid = omp_get_thread_num();
            const char* seg_begin = dict_data + seg_byte_start[tid];
            const char* seg_end   = dict_data + seg_byte_start[tid + 1];
            std::vector<uint8_t>& my_bm = tl_green_bm[tid];
            size_t line_idx = 0; // relative to segment start — fixed up after prefix sum

            const char* p = seg_begin;
            while (p < seg_end) {
                const char* nl = (const char*)memchr(p, '\n', seg_end - p);
                if (!nl) nl = seg_end;
                size_t line_len = (size_t)(nl - p);
                if (line_len >= 5 && line_idx < max_codes_guess) {
                    // Scan for 'g' then verify 'r','e','e','n' — branchless memchr approach
                    const char* q = p;
                    const char* line_end = nl - 4;
                    while (q <= line_end) {
                        if (q[0] == 'g' && q[1] == 'r' && q[2] == 'e' && q[3] == 'e' && q[4] == 'n') {
                            my_bm[line_idx >> 3] |= (uint8_t)(1u << (line_idx & 7u));
                            break;
                        }
                        q++;
                    }
                }
                line_idx++;
                p = (nl < seg_end) ? nl + 1 : seg_end;
            }
            seg_line_count[tid] = line_idx;
        }

        // Prefix sum for seg_line_start
        {
            size_t running = 0;
            for (int t = 0; t < nthreads_dict; t++) {
                seg_line_start[t] = running;
                running += seg_line_count[t];
            }
        }

        // Now fix up tl_green_bm: each thread used line_idx relative to its segment start.
        // We need to shift the bits by seg_line_start[t] to get absolute code positions.
        // Build green_code_bm by merging shifted thread bitmaps (sequential — no overlap).
        // Extra byte at end to handle bit-shift overflow safely.
        std::vector<uint8_t> green_code_bm(bm_bytes + 1, 0u);

        // Each thread's bitmap covers a non-overlapping range of line indices.
        // Merge sequentially: shift each thread's bitmap to its absolute position.
        // Since segments are non-overlapping, no atomics needed.
        // The bit-shift can span a byte boundary, so handle carefully.
        for (int t = 0; t < nthreads_dict; t++) {
            size_t abs_start = seg_line_start[t];
            size_t line_cnt  = seg_line_count[t];
            if (line_cnt == 0) continue;
            const uint8_t* src = tl_green_bm[t].data();
            uint8_t* dst = green_code_bm.data();
            size_t byte_shift = abs_start >> 3;
            int    bit_shift  = (int)(abs_start & 7u);
            size_t src_bytes  = (line_cnt + 7) / 8;
            if (bit_shift == 0) {
                // No bit shifting — directly OR bytes at offset (or just assign since non-overlapping)
                for (size_t b = 0; b < src_bytes; b++) {
                    dst[byte_shift + b] |= src[b];
                }
            } else {
                // Shift bits by bit_shift within the destination
                for (size_t b = 0; b < src_bytes; b++) {
                    uint8_t v = src[b];
                    if (v) {
                        dst[byte_shift + b]     |= (uint8_t)(v << bit_shift);
                        dst[byte_shift + b + 1] |= (uint8_t)(v >> (8 - bit_shift));
                    }
                }
            }
        }
        tl_green_bm.clear();

        munmap((void*)dict_data, dict_size);
        ::close(dict_fd);

        // ── Step B: Scan p_name.bin + p_partkey.bin in parallel ──────────────
        gendb::MmapColumn<int32_t> p_partkey_col(gendb_dir + "/part/p_partkey.bin");
        gendb::MmapColumn<int32_t> p_name_col(gendb_dir + "/part/p_name.bin");
        size_t n_parts = p_partkey_col.count;

        const int nthreads = omp_get_max_threads();

        // Thread-local lists of matching partkeys
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

        // Merge into flat bitmap and build green_idx[] (serial, fast: bit sets only)
        for (int t = 0; t < nthreads; t++) {
            for (int32_t pk : local_keys[t]) {
                bitmap_set(green_parts_bm, pk);
                if ((uint32_t)pk < (uint32_t)PART_BITMAP_BITS) {
                    green_idx[pk] = (uint32_t)green_part_count;
                }
                green_part_count++;
            }
        }
    }

    const uint8_t* const green_bm = green_parts_bm.data();

    // ─── Phase 3: Build fixed-4 partsupp array ──────────────────────────────
    // Each green part has exactly 4 partsupp entries (TPC-H invariant).
    // ps_data[green_idx[partkey] * 4 + j].{suppkey, cost}
    // ~108K parts × 4 × 12B = ~5MB → fits in L3 cache
    std::vector<PSEntry4> ps_data(green_part_count);
    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> ps_partkey_col(gendb_dir + "/partsupp/ps_partkey.bin");
        gendb::MmapColumn<int32_t> ps_suppkey_col(gendb_dir + "/partsupp/ps_suppkey.bin");
        gendb::MmapColumn<int64_t> ps_supplycost_col(gendb_dir + "/partsupp/ps_supplycost.bin");
        int64_t n_ps = (int64_t)ps_partkey_col.count;

        // Parallel scan: partsupp is sorted by ps_partkey with groups of exactly 4
        // consecutive rows per partkey. So rows i*4..(i*4+3) all share the same partkey.
        // We process groups of 4 rows at a time to avoid cross-thread conflicts.
        // Each group of 4 maps to one green_idx slot → no atomics needed.
        const int32_t* pk_data = ps_partkey_col.data;
        const int32_t* sk_data = ps_suppkey_col.data;
        const int64_t* sc_data = ps_supplycost_col.data;

        PSEntry4* psd = ps_data.data();
        const uint8_t* gbm   = green_bm;
        const uint32_t* gidx = green_idx.data();

        // n_ps is always a multiple of 4 (TPC-H invariant: 4 suppliers per part)
        int64_t n_groups = n_ps / 4;

        #pragma omp parallel for schedule(static) num_threads(omp_get_max_threads())
        for (int64_t g = 0; g < n_groups; g++) {
            int64_t i = g * 4;
            int32_t pk = pk_data[i];
            if ((uint32_t)pk >= (uint32_t)PART_BITMAP_BITS) continue;
            if (!((gbm[(uint32_t)pk >> 3] >> ((uint32_t)pk & 7u)) & 1u)) continue;
            uint32_t gi = gidx[pk];
            if (gi == NOT_GREEN) continue;
            PSEntry4& e = psd[gi];
            // Fill all 4 slots directly (no fill counter needed)
            e.suppkey[0] = sk_data[i];     e.cost[0] = sc_data[i];
            e.suppkey[1] = sk_data[i + 1]; e.cost[1] = sc_data[i + 1];
            e.suppkey[2] = sk_data[i + 2]; e.cost[2] = sc_data[i + 2];
            e.suppkey[3] = sk_data[i + 3]; e.cost[3] = sc_data[i + 3];
        }
    }

    // ─── Phase 4: Build direct year_by_orderkey[60000001] ───────────────────
    // Replaces 268MB pre-built orders hash index with a 57MB direct array.
    // year_by_orderkey[orderkey] = year_idx (0-9 for 1992-2001), 0xFF = out of range
    // Lookup: single array access — no hash computation, no linear probing.
    static const uint32_t ORDERS_MAX_KEY = 60000001u;
    // Use malloc + parallel memset to avoid serial initialization overhead
    uint8_t* year_by_orderkey_raw = (uint8_t*)malloc(ORDERS_MAX_KEY);
    if (!year_by_orderkey_raw) {
        std::cerr << "OOM: year_by_orderkey allocation failed" << std::endl;
        return;
    }
    {
        GENDB_PHASE("build_joins_orders");

        // Parallel memset: 60MB → significantly faster than serial std::vector initialization
        const int nthreads_memset = omp_get_max_threads();
        #pragma omp parallel num_threads(nthreads_memset)
        {
            int tid = omp_get_thread_num();
            size_t stripe = (ORDERS_MAX_KEY + nthreads_memset - 1) / nthreads_memset;
            size_t b_start = (size_t)tid * stripe;
            size_t b_end   = b_start + stripe;
            if (b_end > ORDERS_MAX_KEY) b_end = ORDERS_MAX_KEY;
            memset(year_by_orderkey_raw + b_start, 0xFF, b_end - b_start);
        }

        gendb::MmapColumn<int32_t> o_orderkey_col(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate_col(gendb_dir + "/orders/o_orderdate.bin");
        int64_t n_orders = (int64_t)o_orderkey_col.count;

        const int32_t* ok_data = o_orderkey_col.data;
        const int32_t* od_data = o_orderdate_col.data;
        uint8_t* yr_arr = year_by_orderkey_raw;

        #pragma omp parallel for schedule(static) num_threads(omp_get_max_threads())
        for (int64_t i = 0; i < n_orders; i++) {
            uint32_t ok = (uint32_t)ok_data[i];
            if (ok < ORDERS_MAX_KEY) {
                int yr = gendb::extract_year(od_data[i]);
                int yi = yr - YEAR_BASE;
                yr_arr[ok] = (uint8_t)((uint32_t)yi < (uint32_t)N_YEARS ? (uint8_t)yi : 0xFFu);
            }
        }
    }

    // ─── Phase 5: Scan lineitem — parallel aggregation ──────────────────────
    const int num_threads = omp_get_max_threads();
    // Thread-local aggregation: [thread][nation][year]
    // Align to 64-byte cache lines to avoid false sharing
    const int AGG_STRIDE = N_NATIONS * N_YEARS; // 250 int64_t per thread
    std::vector<int64_t> thread_agg(num_threads * AGG_STRIDE, 0LL);

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_partkey_col(gendb_dir + "/lineitem/l_partkey.bin");
        gendb::MmapColumn<int32_t> l_suppkey_col(gendb_dir + "/lineitem/l_suppkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice_col(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount_col(gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int64_t> l_quantity_col(gendb_dir + "/lineitem/l_quantity.bin");
        gendb::MmapColumn<int32_t> l_orderkey_col(gendb_dir + "/lineitem/l_orderkey.bin");

        int64_t n_lineitem = (int64_t)l_partkey_col.count;

        const uint8_t* const gbm    = green_bm;
        const uint32_t* const gidx  = green_idx.data();
        const PSEntry4* const psd   = ps_data.data();
        const uint8_t* const yr_arr = year_by_orderkey_raw;

        // Increased prefetch distance for the large year_by_orderkey array (57MB, many TLB misses)
        static const int PREFETCH_DIST = 32;

        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            int64_t* local_agg = thread_agg.data() + tid * AGG_STRIDE;

            const int32_t* lk_data  = l_partkey_col.data;
            const int32_t* lok_data = l_orderkey_col.data;
            const int32_t* lsk_data = l_suppkey_col.data;
            const int64_t* ep_data  = l_extendedprice_col.data;
            const int64_t* dis_data = l_discount_col.data;
            const int64_t* qty_data = l_quantity_col.data;

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < n_lineitem; i++) {
                // Prefetch upcoming rows — year_by_orderkey is 57MB, high TLB pressure
                if (i + PREFETCH_DIST < n_lineitem) {
                    __builtin_prefetch(gbm + ((uint32_t)lk_data[i + PREFETCH_DIST] >> 3), 0, 1);
                    uint32_t ok_ahead = (uint32_t)lok_data[i + PREFETCH_DIST];
                    if (ok_ahead < ORDERS_MAX_KEY)
                        __builtin_prefetch(yr_arr + ok_ahead, 0, 0);
                }

                int32_t lk = lk_data[i];

                // Fast semi-join filter: bitmap test (single bit op, no hash)
                if ((uint32_t)lk >= (uint32_t)PART_BITMAP_BITS) continue;
                if (!((gbm[(uint32_t)lk >> 3] >> ((uint32_t)lk & 7u)) & 1u)) continue;

                // Lookup order year via direct array — early to hide latency
                uint32_t ok = (uint32_t)lok_data[i];
                if (ok >= ORDERS_MAX_KEY) continue;
                uint8_t yi_raw = yr_arr[ok];
                if (yi_raw >= (uint8_t)N_YEARS) continue; // 0xFF = not found

                int32_t sk = lsk_data[i];

                // Supplier nationkey — early filter
                if ((uint32_t)sk > 100000u) continue;
                int ni = (int)(uint8_t)g_supp_nationkey[sk];
                if ((uint32_t)ni >= (uint32_t)N_NATIONS) continue;

                // Lookup partsupp via green_idx + fixed-4 array (no hash)
                uint32_t gi = gidx[lk];
                if (gi == NOT_GREEN) continue;
                const PSEntry4& e4 = psd[gi];

                // SIMD suppkey comparison: compare sk against all 4 suppkeys simultaneously
                // _mm_cmpeq_epi32 on 4×int32 → bitmask → bsf for slot index
                // This replaces the branchy if/else-if chain with a single SIMD instruction.
                __m128i sv   = _mm_set1_epi32(sk);
                __m128i keys = _mm_loadu_si128((const __m128i*)e4.suppkey);
                int mask = _mm_movemask_epi8(_mm_cmpeq_epi32(sv, keys));
                if (mask == 0) continue; // no match (shouldn't happen in valid TPC-H data)
                int slot = __builtin_ctz((unsigned)mask) >> 2; // each match spans 4 bytes

                int64_t psc = e4.cost[slot];
                int yi = (int)yi_raw;

                // Compute amount in scaled int:
                // l_extendedprice * (100 - l_discount) - ps_supplycost * l_quantity
                int64_t ep  = ep_data[i];
                int64_t dis = dis_data[i];
                int64_t qty = qty_data[i];
                int64_t amount = (ep * (100LL - dis)) / 100LL - psc * qty / 100LL;

                local_agg[ni * N_YEARS + yi] += amount;
            }
        }
    }

    // ─── Phase 6: Merge thread-local aggregations ──────────────────────────
    std::vector<int64_t> global_agg(N_NATIONS * N_YEARS, 0LL);

    for (int t = 0; t < num_threads; t++) {
        const int64_t* ta = thread_agg.data() + t * AGG_STRIDE;
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
            free(year_by_orderkey_raw);
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

    free(year_by_orderkey_raw);
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
