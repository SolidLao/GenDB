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
 *   lineitem: no filter, but semi-join on p_partkey → 59M rows (probe side)
 *
 * Step 2 — Join graph (smallest filtered first):
 *   nation(25) → supplier → part(filtered) → partsupp → lineitem → orders
 *
 * Step 3 — Physical plan:
 *   Phase 1: Load nation dict (25 rows) → direct array: n_nationkey → n_name string
 *   Phase 2: Load supplier (100K) → direct array: s_suppkey → s_nationkey (1-indexed, max=100000)
 *   Phase 3: Scan part name dict; find all dict codes containing "green";
 *            then scan p_name.bin in parallel to collect set of matching p_partkey values
 *            → CompactHashSet<int32_t> green_parts (~400K entries)
 *   Phase 4: Scan partsupp (8M), filter ps_partkey in green_parts
 *            → CompactHashMapPair<int64_t> ps_map: (ps_partkey,ps_suppkey) → ps_supplycost
 *   Phase 5: Load pre-built orders index via mmap (orders_o_orderkey_hash.bin, hash_single format)
 *            → ZERO build time: maps o_orderkey → row_id → o_orderdate
 *   Phase 6: Scan lineitem (60M) in parallel with OpenMP:
 *            - filter l_partkey in green_parts (fast hash set probe)
 *            - lookup ps_map[(l_partkey, l_suppkey)] → ps_supplycost
 *            - lookup order index[l_orderkey] → row_id → o_orderdate → year
 *            - lookup supp_nationkey[l_suppkey] → nationkey
 *            - compute amount = l_extendedprice*(100-l_discount) - ps_supplycost*l_quantity
 *            - accumulate into thread-local agg[nation_idx][year-1992]
 *   Phase 7: Merge thread-local aggregations, sort, output CSV
 *
 * KEY OPTIMIZATION vs iter_0/iter_1:
 *   - Eliminated 474ms hash table build for 15M orders by using the pre-built mmap index
 *   - Parallelized green-parts scan (was 133ms single-threaded over 2M rows)
 *   - schedule(static) for lineitem scan (more efficient for uniform-work loops)
 *
 * Orders index binary format (hash_single):
 *   [uint32_t capacity][capacity × {int32_t key, uint32_t row_id}]
 *   Hash function: h = (uint64_t)key * 0x9E3779B97F4A7C15 >> 32, slot = h & (cap-1)
 *   Empty slot: key = -1 (int32), row_id = 0xFFFFFFFF
 *   Linear probing on collision
 *
 * Aggregation structure: 25 nations × 7 years (1992-1998) = 175 groups
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
                                           uint32_t capacity,
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

    // ─── Phase 2: Find green parts (parallelized) ───────────────────────────
    gendb::CompactHashSet<int32_t> green_parts(500000);
    {
        GENDB_PHASE("dim_filter_part");

        // Load p_name dict and find codes containing "green"
        std::vector<bool> green_code;
        {
            std::ifstream f(gendb_dir + "/part/name_dict.txt");
            std::string line;
            while (std::getline(f, line)) {
                green_code.push_back(line.find("green") != std::string::npos);
            }
        }
        int32_t green_code_size = (int32_t)green_code.size();

        gendb::MmapColumn<int32_t> p_partkey_col(gendb_dir + "/part/p_partkey.bin");
        gendb::MmapColumn<int32_t> p_name_col(gendb_dir + "/part/p_name.bin");
        size_t n_parts = p_partkey_col.count;

        // Parallel scan: collect green partkeys into thread-local vectors, then merge
        const int nthreads = omp_get_max_threads();
        std::vector<std::vector<int32_t>> local_keys(nthreads);

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            std::vector<int32_t>& lk = local_keys[tid];
            lk.reserve(500000 / nthreads + 1000);

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < (int64_t)n_parts; i++) {
                int32_t code = p_name_col[i];
                if (code >= 0 && code < green_code_size && green_code[code]) {
                    lk.push_back(p_partkey_col[i]);
                }
            }
        }

        // Merge thread-local results into green_parts hash set (serial, small)
        for (int t = 0; t < nthreads; t++) {
            for (int32_t pk : local_keys[t]) {
                green_parts.insert(pk);
            }
        }
    }

    // ─── Phase 3: Build partsupp hash map (partkey,suppkey) → supplycost ───
    // Only keep entries where ps_partkey is a green part
    gendb::CompactHashMapPair<int64_t> ps_map(green_parts.size() * 4 + 1000);
    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> ps_partkey_col(gendb_dir + "/partsupp/ps_partkey.bin");
        gendb::MmapColumn<int32_t> ps_suppkey_col(gendb_dir + "/partsupp/ps_suppkey.bin");
        gendb::MmapColumn<int64_t> ps_supplycost_col(gendb_dir + "/partsupp/ps_supplycost.bin");
        size_t n_ps = ps_partkey_col.count;

        for (size_t i = 0; i < n_ps; i++) {
            int32_t pk = ps_partkey_col[i];
            if (!green_parts.contains(pk)) continue;
            int32_t sk = ps_suppkey_col[i];
            int64_t sc = ps_supplycost_col[i];
            ps_map.insert({pk, sk}, sc);
        }
    }

    // ─── Phase 4: Load pre-built orders index (ZERO build time) ─────────────
    // Format: [uint32_t capacity][capacity × OrdersSlot{int32_t key, uint32_t row_id}]
    // Replaces 474ms hash map build with instant mmap load
    const OrdersSlot* orders_slots = nullptr;
    uint32_t orders_cap = 0;
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

        // Read capacity from first 4 bytes
        orders_cap = *reinterpret_cast<const uint32_t*>(ptr);
        orders_mask = orders_cap - 1;
        // Slots start after the 4-byte header
        // Header is 4 bytes, but slots are 8-byte aligned: the slots array starts at byte 4
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
        const int32_t* const o_date_data = o_orderdate_col.data;

        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            // Pointer to this thread's aggregation slice
            int64_t* local_agg = thread_agg.data() + tid * N_NATIONS * N_YEARS;

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < n_lineitem; i++) {
                int32_t lk = l_partkey_col[i];

                // Fast semi-join filter: must be a green part
                if (!green_parts.contains(lk)) continue;

                int32_t sk = l_suppkey_col[i];

                // Lookup partsupp
                const int64_t* psc = ps_map.find({lk, sk});
                if (!psc) continue;

                // Supplier nationkey — early filter
                if (sk < 0 || sk > 100000) continue;
                int ni = (int)(uint8_t)g_supp_nationkey[sk];
                if (ni < 0 || ni >= N_NATIONS) continue;

                // Lookup order year via pre-built index (mmap'd, zero-build)
                uint32_t row_id = orders_index_lookup(o_slots, orders_cap, o_mask, l_orderkey_col[i]);
                if (row_id == 0xFFFFFFFFU) continue;

                int32_t od = o_date_data[row_id];
                int yr = gendb::extract_year(od);
                int yi = yr - YEAR_BASE;
                if (yi < 0 || yi >= N_YEARS) continue;

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
        // The mmap base is 4 bytes before slots (the header uint32_t)
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
