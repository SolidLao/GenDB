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
 *   orders:   no filter                     → 15M rows (pre-built mmap index)
 *   lineitem: filter l_partkey in green_parts → 59M rows (probe side)
 *
 * Step 2 — Join graph (smallest filtered first):
 *   nation(25) → supplier(100K) → part(filtered,~108K) → partsupp(~432K) → lineitem(59M) → orders
 *
 * Step 3 — Physical plan (Iteration 6 optimizations):
 *   Phase 1: Load nation dict (25 rows) → direct array: n_nationkey → n_name string
 *   Phase 2: Load supplier (100K) → direct array: s_suppkey → s_nationkey
 *   Phase 3: mmap the part name dict file as raw bytes; use memmem() to find lines
 *            containing "green" → mark in a flat bitmap (250KB, bit per partkey).
 *            Then scan p_name.bin + p_partkey.bin in parallel to set bitmap bits
 *            for matching part keys.
 *            KEY CHANGE: green_parts is now a flat BIT ARRAY (not CompactHashSet).
 *            Bit-test is: (bitmap[pk >> 3] >> (pk & 7)) & 1
 *            → O(1) with NO hash, NO cache miss (250KB fits in L3).
 *   Phase 4: Scan partsupp (8M) in parallel with OpenMP.
 *            Filter ps_partkey via bitmap (cheap bit-test).
 *            Thread-local buffers accumulate {(ps_partkey,ps_suppkey),ps_supplycost}.
 *            After parallel phase, serially merge into CompactHashMapPair ps_map.
 *   Phase 5: OPTIMIZATION (iter 6): Replace orders hash index with direct array.
 *            o_orderkey follows pattern 4k+1 for k=0..14999999.
 *            → dense_idx = (orderkey - 1) / 4  maps to 0..14999999 (bijection).
 *            Build year_by_orderkey[dense_idx] as int8_t (15MB, fits in L3=44MB).
 *            In main_scan: yr = year_by_orderkey[(ok-1)/4]  — single array access,
 *            no hash computation, no linear probing, perfect cache behavior.
 *   Phase 6: Scan lineitem (60M) in parallel:
 *            - filter l_partkey via bitmap (bit-test, very cheap)
 *            - lookup ps_map[(l_partkey, l_suppkey)] → ps_supplycost
 *            - lookup year_by_orderkey[(l_orderkey-1)/4] → year_idx (direct array)
 *            - lookup supp_nationkey[l_suppkey] → nationkey
 *            - compute amount and accumulate into thread-local agg[nation][year]
 *   Phase 7: Merge thread-local aggregations, sort, output CSV
 *
 * ITERATION 6 KEY OPTIMIZATIONS:
 *   1. Direct orderkey→year array replaces orders hash index probe:
 *       * orderkeys are 4k+1, dense mapping (ok-1)/4 → 0..14.999.999
 *       * 15MB int8_t array fits in L3 (44MB), single load per qualifying row
 *       * Eliminates hash multiply + linear probing chain (2-5 cache misses saved)
 *   2. Parallel seg_line_start computation:
 *       * Newline counting now done in parallel (was serial over full dict)
 *       * O(nthreads) prefix sum instead of O(dict_size) serial pass
 *   3. Software prefetch in main_scan loop for ps_map (hide latency on probes)
 *
 * Orders dense array format (iter 6):
 *   year_by_orderkey[(ok-1)/4] = year - YEAR_BASE as int8_t
 *   Indexed by dense_idx = (orderkey-1)/4, 0..14999999
 *   Array size: 15MB
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

// ─── Direct orderkey→year array (iter 6 optimization) ───────────────────────
// TPC-H orderkeys follow pattern: ok = 4k+1, k=0..14999999
// Dense index: dense_idx = (ok - 1) / 4  (exact, no collision)
// year_by_orderkey[dense_idx] = year - YEAR_BASE (int8_t), or -1 if out of range
// Array size: 15M × 1B = 15MB — fits comfortably in L3 cache (44MB)
static std::vector<int8_t> g_year_by_orderkey;
static const int32_t ORDERKEY_MAX_DENSE = 15000000; // (60000000-1)/4 + 1

static inline int get_order_year_idx(int32_t ok) {
    // Dense index: (ok - 1) >> 2  (since ok = 4k+1, (ok-1)/4 = k exactly)
    uint32_t dense = (uint32_t)(ok - 1) >> 2;
    if (__builtin_expect(dense >= (uint32_t)ORDERKEY_MAX_DENSE, 0)) return -1;
    return (int)(int8_t)g_year_by_orderkey[dense];
}

// ─── Flat bitmap for green_parts (2M+1 bits = ~250KB) ───────────────────────
// Parts keys are 1-indexed 1..2000000
static const int32_t PART_BITMAP_BITS = 2000001; // bit index = part key
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
        //
        // OPTIMIZATION (iter 5): Parallelize the dict scan.
        //   - Pre-compute per-thread byte segment boundaries (aligned to '\n')
        //   - Pre-compute starting line index for each thread by counting '\n' in prior segments
        //   - Each thread scans its byte segment for "green" occurrences, marks in a
        //     thread-local green_code bitmap (no sharing needed — each thread owns its lines)
        //   - Merge: OR all thread bitmaps together
        //
        // This turns 2M sequential memchr+memmem calls into 64 parallel segments,
        // each with ~31K lines. Expected speedup: ~30-40x on the dict scan step.

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
        // We compute:
        //   seg_byte_start[t], seg_byte_end[t]  — byte range for thread t
        //   seg_line_start[t]                   — first dict code index for thread t

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
        // OPTIMIZATION (iter 6): This was a serial O(dict_size) pass; now O(dict_size/nthreads)
        // Each thread counts its own segment; O(nthreads) serial prefix sum follows.
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
        // Each bitmap is 250KB; total = 64 × 250KB = 16MB (fine for 376GB RAM)
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
                    // Scan for 'g' then verify 'r','e','e','n' — avoids full memmem overhead
                    const char* q = p;
                    const char* line_end = nl - 4; // need at least 5 chars
                    while (q <= line_end) {
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
        // bm_bytes = 250KB, nthreads_dict = 64 → 64 × 250KB passes over 250KB = fast
        std::vector<uint8_t> green_code_bm(bm_bytes, 0u);
        for (int t = 0; t < nthreads_dict; t++) {
            const uint8_t* src = tl_green_bm[t].data();
            for (size_t b = 0; b < bm_bytes; b++) {
                green_code_bm[b] |= src[b];
            }
        }
        tl_green_bm.clear(); // free 16MB of thread-local bitmaps

        // ── Step B: Scan p_name.bin + p_partkey.bin in parallel ──────────────
        // For each part row: if green_code_bm[p_name[i]] is set → set bit in green_parts_bm
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

    // ─── Phase 3: Build partsupp hash map (partkey,suppkey) → supplycost ────
    // Parallelized: thread-local accumulation, then serial merge into ps_map
    // Estimated qualifying partsupp rows: ~108K parts × 4 suppliers = ~432K entries
    gendb::CompactHashMapPair<int64_t> ps_map(green_part_count * 4 + 1000);
    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> ps_partkey_col(gendb_dir + "/partsupp/ps_partkey.bin");
        gendb::MmapColumn<int32_t> ps_suppkey_col(gendb_dir + "/partsupp/ps_suppkey.bin");
        gendb::MmapColumn<int64_t> ps_supplycost_col(gendb_dir + "/partsupp/ps_supplycost.bin");
        int64_t n_ps = (int64_t)ps_partkey_col.count;

        const int nthreads = omp_get_max_threads();

        // Thread-local buffers: vector of (key, value) pairs
        struct PSEntry { int32_t pk; int32_t sk; int64_t sc; };
        std::vector<std::vector<PSEntry>> local_ps(nthreads);

        // Each thread gets ~8M/64 = 125K rows, ~432K/64 pass filter
        // Estimated qualifying per thread: ~7K
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

        // Serial merge: insert all thread-local entries into ps_map
        for (int t = 0; t < nthreads; t++) {
            for (const auto& e : local_ps[t]) {
                ps_map.insert({e.pk, e.sk}, e.sc);
            }
        }
    }

    // ─── Phase 4: Build direct orderkey→year array ───────────────────────────
    // OPTIMIZATION (iter 6): Replace orders hash index lookup with direct array.
    //   o_orderkey follows pattern 4k+1 (k=0..14999999), giving dense_idx=(ok-1)/4.
    //   We scan o_orderkey.bin + o_orderdate.bin in parallel (both 60MB), mapping:
    //     g_year_by_orderkey[(ok-1)/4] = year - YEAR_BASE  (int8_t, 15MB total)
    //   This eliminates ALL hash computation from the orders join in main_scan:
    //     OLD: hash(orderkey) → linear probe → row_id → year_by_rowid[row_id]
    //     NEW: yr = g_year_by_orderkey[(ok-1)/4]  (single cache-warm array read)
    {
        GENDB_PHASE("build_joins_orders");

        gendb::MmapColumn<int32_t> o_orderkey_col(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate_col(gendb_dir + "/orders/o_orderdate.bin");
        size_t n_orders = o_orderkey_col.count;

        // Allocate 15MB direct lookup array (all slots init to -1)
        g_year_by_orderkey.assign(ORDERKEY_MAX_DENSE, (int8_t)-1);

        // Parallel fill: split orders rows across threads (both columns read sequentially)
        const int nthreads_ord = omp_get_max_threads();
        #pragma omp parallel for schedule(static) num_threads(nthreads_ord)
        for (int64_t i = 0; i < (int64_t)n_orders; i++) {
            int32_t ok = o_orderkey_col[i];
            uint32_t dense = (uint32_t)(ok - 1) >> 2;
            if (dense < (uint32_t)ORDERKEY_MAX_DENSE) {
                int yr = gendb::extract_year(o_orderdate_col[i]);
                int yi = yr - YEAR_BASE;
                g_year_by_orderkey[dense] = (int8_t)((uint32_t)yi < (uint32_t)N_YEARS ? yi : -1);
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

        // Direct year lookup array (15MB, no hash probe needed)
        const int8_t* const yr_data = g_year_by_orderkey.data();
        const uint8_t* const gbm = green_bm;

        // Prefetch distance: ~16 rows ahead (tune for HDD: latency ~8ms, but data is mmap'd)
        static const int64_t PREFETCH_DIST = 16;

        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            int64_t* local_agg = thread_agg.data() + tid * N_NATIONS * N_YEARS;

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < n_lineitem; i++) {
                // Prefetch upcoming rows into cache (helps with mmap'd column data)
                if (i + PREFETCH_DIST < n_lineitem) {
                    __builtin_prefetch(&l_partkey_col[i + PREFETCH_DIST], 0, 1);
                    __builtin_prefetch(&l_orderkey_col[i + PREFETCH_DIST], 0, 1);
                }

                int32_t lk = l_partkey_col[i];

                // Fast semi-join filter: bitmap test (single bit op, no hash)
                if ((uint32_t)lk >= (uint32_t)PART_BITMAP_BITS) continue;
                if (!((gbm[(uint32_t)lk >> 3] >> ((uint32_t)lk & 7u)) & 1u)) continue;

                int32_t sk = l_suppkey_col[i];

                // Supplier nationkey — early filter
                if ((uint32_t)sk > 100000u) continue;
                int ni = (int)(uint8_t)g_supp_nationkey[sk];
                if ((uint32_t)ni >= (uint32_t)N_NATIONS) continue;

                // Lookup partsupp
                const int64_t* psc = ps_map.find({lk, sk});
                if (!psc) continue;

                // ITER 6: Direct orderkey→year lookup (no hash probe, no linear probing)
                // dense_idx = (ok-1)/4 maps uniquely to row in year array (15MB, L3-resident)
                int32_t ok = l_orderkey_col[i];
                uint32_t dense = (uint32_t)(ok - 1) >> 2;
                if (__builtin_expect(dense >= (uint32_t)ORDERKEY_MAX_DENSE, 0)) continue;
                int yi = (int)(int8_t)yr_data[dense];
                if ((uint8_t)yi >= (uint8_t)N_YEARS) continue;  // handles -1 as large unsigned

                // Compute amount in scaled int:
                // l_extendedprice * (100 - l_discount) - ps_supplycost * l_quantity
                int64_t ep  = l_extendedprice_col[i];
                int64_t dis = l_discount_col[i];
                int64_t qty = l_quantity_col[i];
                int64_t amount = (ep * (100LL - dis)) / 100LL - (*psc) * qty / 100LL;

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
