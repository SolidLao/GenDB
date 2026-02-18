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
 * Step 3 — Physical plan (Iteration 4 optimizations):
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
 *   Phase 5: Load pre-built orders index via mmap (hash_single, zero build time).
 *   Phase 6: Scan lineitem (60M) in parallel:
 *            - filter l_partkey via bitmap (bit-test, very cheap)
 *            - lookup ps_map[(l_partkey, l_suppkey)] → ps_supplycost
 *            - lookup order index[l_orderkey] → row_id → o_orderdate → year
 *            - lookup supp_nationkey[l_suppkey] → nationkey
 *            - compute amount and accumulate into thread-local agg[nation][year]
 *   Phase 7: Merge thread-local aggregations, sort, output CSV
 *
 * KEY OPTIMIZATIONS vs iter_2:
 *   - Flat bitmap replaces CompactHashSet for green_parts:
 *       * 250KB bitmap fits in L3; bit-test is single cycle with no hash computation
 *       * Eliminates ~108K hash insertions + ~59M hash probes per row
 *   - Dict file parsed via mmap+memmem instead of line-by-line std::getline:
 *       * Avoids 1.999M string object allocations and getline overhead
 *       * memmem scans ~100MB/s with SIMD under the hood
 *   - Partsupp scan parallelized (was serial, 67ms → ~10ms with 64 threads)
 *
 * Orders index binary format (hash_single):
 *   [uint32_t capacity][capacity × {int32_t key, uint32_t row_id}]
 *   Hash function: h = (uint64_t)key * 0x9E3779B97F4A7C15 >> 32, slot = h & (cap-1)
 *   Empty slot: key = -1 (int32), row_id = 0xFFFFFFFF
 *   Linear probing on collision
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

// ─── Pre-built orders hash_single index: slot = {int32_t key, uint32_t row_id} ───
struct OrdersSlot {
    int32_t  key;    // o_orderkey value, or -1 if empty
    uint32_t row_id; // row index into orders column files, or 0xFFFFFFFF if empty
};

// Inline lookup for the orders index (mmap'd)
// Returns row_id for o_orderkey, or UINT32_MAX if not found
static inline uint32_t orders_index_lookup(const OrdersSlot* slots,
                                           uint32_t mask,
                                           int32_t orderkey) {
    uint32_t pos = (uint32_t)(((uint64_t)(uint32_t)orderkey * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
    while (true) {
        int32_t k = slots[pos].key;
        if (k == orderkey) return slots[pos].row_id;
        if (slots[pos].row_id == 0xFFFFFFFFU) return 0xFFFFFFFFU;
        pos = (pos + 1) & mask;
    }
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

        // Count '\n' in each segment to compute seg_line_start (exclusive prefix sum)
        // This is a sequential pass but only over segment boundaries (fast: count '\n' per segment)
        {
            size_t running_lines = 0;
            for (int t = 0; t < nthreads_dict; t++) {
                seg_line_start[t] = running_lines;
                const char* seg_begin = dict_data + seg_byte_start[t];
                const char* seg_end   = dict_data + seg_byte_start[t + 1];
                // Count '\n' in segment using memchr loop (fast, SIMD-accelerated libc)
                size_t seg_len = seg_end - seg_begin;
                const char* p = seg_begin;
                while (p < seg_end) {
                    const char* nl = (const char*)memchr(p, '\n', seg_end - p);
                    if (!nl) break;
                    running_lines++;
                    p = nl + 1;
                }
                (void)seg_len;
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

    // ─── Phase 4: Load pre-built orders index + build compact year array ─────
    // Format: [uint32_t capacity][capacity × OrdersSlot{int32_t key, uint32_t row_id}]
    //
    // OPTIMIZATION (iter 5): Build a compact int8_t year_by_rowid[15M] array.
    //   - o_orderdate.bin: 15M × int32_t = 60MB, sequential read → fast
    //   - year_by_rowid[row_id] = year - YEAR_BASE (0-9) as int8_t = 15MB
    //   - 15MB fits comfortably in L3 cache (44MB), reducing random-access pressure
    //   - In main_scan, after hash-probing the orders index to get row_id,
    //     we read year_by_rowid[row_id] (array lookup) instead of o_orderdate_col[row_id]
    //     (same cost), but this array is 4x smaller so better cache occupancy.
    //   - More importantly: since year_by_rowid values are computed once sequentially,
    //     the working set for order date resolution is 15MB (year_by_rowid only)
    //     instead of 60MB (full o_orderdate column), improving cache hit rates.

    const OrdersSlot* orders_slots = nullptr;
    uint32_t orders_mask = 0;
    int orders_idx_fd = -1;
    size_t orders_idx_size = 0;

    // Compact year array: row_id → year_idx (0-9 for 1992-2001), -1 = out of range
    std::vector<int8_t> year_by_rowid;

    {
        GENDB_PHASE("build_joins_orders");

        // Build compact year array from o_orderdate sequentially (60MB → 15MB)
        gendb::MmapColumn<int32_t> o_orderdate_col(gendb_dir + "/orders/o_orderdate.bin");
        size_t n_orders = o_orderdate_col.count;
        year_by_rowid.resize(n_orders);
        for (size_t i = 0; i < n_orders; i++) {
            int yr = gendb::extract_year(o_orderdate_col[i]);
            int yi = yr - YEAR_BASE;
            year_by_rowid[i] = (int8_t)((uint32_t)yi < (uint32_t)N_YEARS ? yi : -1);
        }

        std::string idx_path = gendb_dir + "/indexes/orders_o_orderkey_hash.bin";
        orders_idx_fd = ::open(idx_path.c_str(), O_RDONLY);
        if (orders_idx_fd < 0) {
            std::cerr << "Cannot open orders index: " << idx_path << std::endl;
            return;
        }
        struct stat st;
        fstat(orders_idx_fd, &st);
        orders_idx_size = st.st_size;

        void* ptr = mmap(nullptr, orders_idx_size, PROT_READ, MAP_PRIVATE, orders_idx_fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Cannot mmap orders index" << std::endl;
            ::close(orders_idx_fd);
            return;
        }
        madvise(ptr, orders_idx_size, MADV_RANDOM);

        uint32_t orders_cap = *reinterpret_cast<const uint32_t*>(ptr);
        orders_mask = orders_cap - 1;
        orders_slots = reinterpret_cast<const OrdersSlot*>(
            reinterpret_cast<const char*>(ptr) + sizeof(uint32_t));
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

        // Cache locals for the orders index (avoid pointer indirection in hot loop)
        const OrdersSlot* const o_slots = orders_slots;
        const uint32_t o_mask = orders_mask;
        // Use compact year_by_rowid (15MB, 4x smaller than o_orderdate 60MB) for better cache
        const int8_t* const yr_data = year_by_rowid.data();
        const uint8_t* const gbm = green_bm;

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

                // Lookup partsupp
                const int64_t* psc = ps_map.find({lk, sk});
                if (!psc) continue;

                // Lookup order year via pre-built index → compact year_by_rowid array
                // year_by_rowid is 15MB (vs 60MB for o_orderdate), better L3 cache utilization
                uint32_t row_id = orders_index_lookup(o_slots, o_mask, l_orderkey_col[i]);
                if (row_id == 0xFFFFFFFFU) continue;

                int yi = (int)yr_data[row_id];
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

    // ─── Phase 7: Cleanup mmap'd index ─────────────────────────────────────
    if (orders_slots) {
        void* base = reinterpret_cast<void*>(
            reinterpret_cast<char*>(const_cast<OrdersSlot*>(orders_slots)) - sizeof(uint32_t));
        munmap(base, orders_idx_size);
    }
    if (orders_idx_fd >= 0) ::close(orders_idx_fd);

    // ─── Phase 8: Output ────────────────────────────────────────────────────
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
