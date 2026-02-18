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
 *   part:     filter p_name LIKE '%green%'  → ~200K-400K parts (dict scan)
 *   supplier: no filter                     → 100K rows
 *   nation:   no filter                     → 25 rows
 *   partsupp: no filter                     → 8M rows
 *   orders:   no filter                     → 15M rows
 *   lineitem: no filter, but semi-join on p_partkey → 60M rows (probe side)
 *
 * Step 2 — Join graph (smallest filtered first):
 *   nation(25) → supplier → part(filtered) → partsupp → lineitem → orders
 *
 * Step 3 — Physical plan:
 *   Phase 1: Load nation dict (25 rows) → direct array: n_nationkey → n_name string
 *            Load supplier (100K) → direct array: s_suppkey → s_nationkey (1-indexed)
 *   Phase 2: Scan part name dict; find codes containing "green";
 *            Parallel scan p_name.bin → fill flat boolean bitmap green_bitmap[2M+1]
 *            FASTER than CompactHashSet: bitmap lookup = single array read (O(1), L3-resident)
 *   Phase 3: Parallel scan partsupp (8M), filter ps_partkey via green_bitmap
 *            Thread-local vectors of (ps_partkey, ps_suppkey, ps_supplycost) → serial merge
 *            → CompactHashMapPair<int64_t> ps_map: (ps_partkey,ps_suppkey) → ps_supplycost
 *   Phase 4: Load pre-built orders index via mmap (ZERO build time)
 *   Phase 5: Parallel lineitem scan (60M) with OpenMP:
 *            - filter l_partkey via green_bitmap (single array lookup, no hash)
 *            - early suppkey range check
 *            - lookup supp_nationkey[sk] → nation_idx
 *            - lookup ps_map[(l_partkey, l_suppkey)] → ps_supplycost
 *            - lookup orders index[l_orderkey] → row_id → o_orderdate → year
 *            - compute amount and accumulate into thread-local agg[nation][year]
 *   Phase 6: Merge thread-local aggregations, sort, output CSV
 *
 * KEY OPTIMIZATIONS vs iter_2:
 *   1. green_bitmap: flat bool[2000001] replaces CompactHashSet<int32_t>
 *      - 2MB, fits in L3 cache, single array lookup vs hash-probe
 *      - Benefits BOTH the partsupp scan (8M probes) and lineitem scan (60M probes)
 *   2. Parallel partsupp scan: was serial 67ms, now parallel with thread-local vectors
 *   3. Reordered lineitem hot loop: suppkey range check before ps_map probe
 *      to skip invalid rows without expensive composite key hash
 *
 * Orders index binary format (hash_single):
 *   [uint32_t capacity][capacity × {int32_t key, uint32_t row_id}]
 *   Hash function: h = (uint64_t)key * 0x9E3779B97F4A7C15 >> 32, slot = h & (cap-1)
 *   Empty slot: key = -1 (int32), row_id = 0xFFFFFFFF
 *   Linear probing on collision
 *
 * Aggregation: 25 nations × 10 years (1992-2001) = 250 groups
 *   → flat 2D array: agg[25][10] per thread (thread-local, no atomics needed)
 *
 * Arithmetic (all scaled integers, scale=100):
 *   l_extendedprice * (100 - l_discount) → scale = 10000
 *   ps_supplycost * l_quantity → scale = 10000
 *   amount = (l_extendedprice*(100-l_discount) - ps_supplycost*l_quantity) / 100
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
static inline uint32_t orders_index_lookup(const OrdersSlot* __restrict__ slots,
                                           uint32_t mask,
                                           int32_t orderkey) {
    uint32_t pos = (uint32_t)(((uint64_t)(uint32_t)orderkey * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
    while (true) {
        const OrdersSlot& s = slots[pos];
        if (s.key == orderkey) return s.row_id;
        if (s.row_id == 0xFFFFFFFFU) return 0xFFFFFFFFU;
        pos = (pos + 1) & mask;
    }
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

    // ─── Phase 2: Find green parts — flat bitmap (replaces CompactHashSet) ─
    // green_bitmap[partkey] = 1 if part name contains "green"
    // Part partkeys: 1..2,000,000 (2M entries). Bitmap = 2MB, fits in L3 cache.
    // Single array lookup = 1ns vs hash probe = 50-100ns → 50-100x faster per probe
    static const int MAX_PARTKEY = 2000001;
    std::vector<uint8_t> green_bitmap(MAX_PARTKEY, 0);
    {
        GENDB_PHASE("dim_filter_part");

        // Load p_name dict and find codes containing "green"
        std::vector<uint8_t> green_code;
        {
            std::ifstream f(gendb_dir + "/part/name_dict.txt");
            std::string line;
            while (std::getline(f, line)) {
                green_code.push_back(line.find("green") != std::string::npos ? 1 : 0);
            }
        }
        int32_t green_code_size = (int32_t)green_code.size();

        gendb::MmapColumn<int32_t> p_partkey_col(gendb_dir + "/part/p_partkey.bin");
        gendb::MmapColumn<int32_t> p_name_col(gendb_dir + "/part/p_name.bin");
        size_t n_parts = p_partkey_col.count;

        // Parallel scan: collect matching partkeys into thread-local vectors, then merge
        const int nthreads = omp_get_max_threads();
        std::vector<std::vector<int32_t>> local_keys(nthreads);

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            std::vector<int32_t>& lk = local_keys[tid];
            lk.reserve(400000 / nthreads + 1000);

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < (int64_t)n_parts; i++) {
                int32_t code = p_name_col[i];
                if (code >= 0 && code < green_code_size && green_code[code]) {
                    lk.push_back(p_partkey_col[i]);
                }
            }
        }

        // Merge into bitmap (serial, fast — just array writes)
        for (int t = 0; t < nthreads; t++) {
            for (int32_t pk : local_keys[t]) {
                if (pk > 0 && pk < MAX_PARTKEY) {
                    green_bitmap[pk] = 1;
                }
            }
        }
    }

    // ─── Phase 3: Build partsupp map — PARALLELIZED ─────────────────────────
    // Scan 8M partsupp rows, filter on green_bitmap (single array lookup),
    // collect into thread-local vectors, then build CompactHashMapPair serially.
    // Estimating ~20% of 8M = 1.6M green partsupp entries.
    gendb::CompactHashMapPair<int64_t> ps_map(1700000);
    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> ps_partkey_col(gendb_dir + "/partsupp/ps_partkey.bin");
        gendb::MmapColumn<int32_t> ps_suppkey_col(gendb_dir + "/partsupp/ps_suppkey.bin");
        gendb::MmapColumn<int64_t> ps_supplycost_col(gendb_dir + "/partsupp/ps_supplycost.bin");
        int64_t n_ps = (int64_t)ps_partkey_col.count;

        const int nthreads = omp_get_max_threads();

        // Thread-local storage for filtered partsupp entries
        struct PSEntry { int32_t pk; int32_t sk; int64_t cost; };
        std::vector<std::vector<PSEntry>> local_entries(nthreads);

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            std::vector<PSEntry>& le = local_entries[tid];
            le.reserve(1700000 / nthreads + 1000);

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < n_ps; i++) {
                int32_t pk = ps_partkey_col[i];
                // bitmap lookup: single array read — no hash computation
                if (pk <= 0 || pk >= MAX_PARTKEY || !green_bitmap[pk]) continue;
                le.push_back({pk, ps_suppkey_col[i], ps_supplycost_col[i]});
            }
        }

        // Serial merge: build hash map from collected entries
        for (int t = 0; t < nthreads; t++) {
            for (const auto& e : local_entries[t]) {
                ps_map.insert({e.pk, e.sk}, e.cost);
            }
        }
    }

    // ─── Phase 4: Load pre-built orders index (ZERO build time) ─────────────
    // Format: [uint32_t capacity][capacity × OrdersSlot{int32_t key, uint32_t row_id}]
    const OrdersSlot* orders_slots = nullptr;
    uint32_t orders_mask = 0;
    int orders_idx_fd = -1;
    size_t orders_idx_size = 0;

    // mmap the o_orderdate column for direct row_id → date access
    gendb::MmapColumn<int32_t> o_orderdate_col(gendb_dir + "/orders/o_orderdate.bin");

    {
        GENDB_PHASE("build_joins_orders");

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
        madvise(ptr, orders_idx_size, MADV_RANDOM); // random access pattern for hash probes

        uint32_t cap = *reinterpret_cast<const uint32_t*>(ptr);
        orders_mask = cap - 1;
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

        // Hint OS to prefetch lineitem columns sequentially
        madvise((void*)l_partkey_col.data,       n_lineitem * sizeof(int32_t), MADV_SEQUENTIAL);
        madvise((void*)l_suppkey_col.data,        n_lineitem * sizeof(int32_t), MADV_SEQUENTIAL);
        madvise((void*)l_extendedprice_col.data,  n_lineitem * sizeof(int64_t), MADV_SEQUENTIAL);
        madvise((void*)l_discount_col.data,        n_lineitem * sizeof(int64_t), MADV_SEQUENTIAL);
        madvise((void*)l_quantity_col.data,        n_lineitem * sizeof(int64_t), MADV_SEQUENTIAL);
        madvise((void*)l_orderkey_col.data,        n_lineitem * sizeof(int32_t), MADV_SEQUENTIAL);

        // Cache locals for hot loop
        const uint8_t* __restrict__ gbm    = green_bitmap.data();
        const OrdersSlot* __restrict__ o_slots = orders_slots;
        const uint32_t o_mask              = orders_mask;
        const int32_t* __restrict__ o_date_data = o_orderdate_col.data;
        const int8_t* __restrict__ supp_nk = g_supp_nationkey.data();

        // Raw pointers into mmap'd arrays for maximum speed
        const int32_t* __restrict__ lp = l_partkey_col.data;
        const int32_t* __restrict__ ls = l_suppkey_col.data;
        const int64_t* __restrict__ le = l_extendedprice_col.data;
        const int64_t* __restrict__ ld = l_discount_col.data;
        const int64_t* __restrict__ lq = l_quantity_col.data;
        const int32_t* __restrict__ lo = l_orderkey_col.data;

        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            int64_t* local_agg = thread_agg.data() + tid * N_NATIONS * N_YEARS;

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < n_lineitem; i++) {
                int32_t lk = lp[i];

                // Fast semi-join filter: bitmap lookup (1 array read, no hash)
                if (lk <= 0 || lk >= MAX_PARTKEY || !gbm[lk]) continue;

                int32_t sk = ls[i];

                // Suppkey range check before expensive composite hash probe
                if (__builtin_expect(sk <= 0 || sk > 100000, 0)) continue;

                // Supplier nationkey — fast array lookup
                int ni = (int)(uint8_t)supp_nk[sk];
                if (__builtin_expect(ni >= N_NATIONS, 0)) continue;

                // Lookup partsupp (composite key hash)
                const int64_t* psc = ps_map.find({lk, sk});
                if (!psc) continue;

                // Lookup order year via pre-built index (mmap'd, zero-build)
                uint32_t row_id = orders_index_lookup(o_slots, o_mask, lo[i]);
                if (row_id == 0xFFFFFFFFU) continue;

                int32_t od = o_date_data[row_id];
                int yr = gendb::extract_year(od);
                int yi = yr - YEAR_BASE;
                if (__builtin_expect((unsigned)yi >= (unsigned)N_YEARS, 0)) continue;

                // Compute amount:
                // (l_extendedprice * (100 - l_discount) - ps_supplycost * l_quantity) / 100
                int64_t ep  = le[i];
                int64_t dis = ld[i];
                int64_t qty = lq[i];
                int64_t amount = (ep * (100LL - dis)) / 100LL - (*psc) * qty / 100LL;

                local_agg[ni * N_YEARS + yi] += amount;
            }
        }
    }

    // ─── Phase 6: Merge thread-local aggregations ──────────────────────────
    int64_t global_agg[N_NATIONS][N_YEARS] = {};

    for (int t = 0; t < num_threads; t++) {
        const int64_t* ta = thread_agg.data() + t * N_NATIONS * N_YEARS;
        for (int ni = 0; ni < N_NATIONS; ni++) {
            for (int yi = 0; yi < N_YEARS; yi++) {
                global_agg[ni][yi] += ta[ni * N_YEARS + yi];
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
                int64_t v = global_agg[ni][yi];
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
