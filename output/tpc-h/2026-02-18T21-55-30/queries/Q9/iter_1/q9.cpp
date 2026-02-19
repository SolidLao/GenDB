// Q9: Product Type Profit Measure
// SELECT nation, o_year, SUM(amount) AS sum_profit
// FROM (
//   SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year,
//          l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
//   FROM part, supplier, lineitem, partsupp, orders, nation
//   WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey
//     AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
//     AND p_name LIKE '%green%'
// ) AS profit
// GROUP BY nation, o_year
// ORDER BY nation, o_year DESC;
//
// EXECUTION PLAN (iter_1):
//
// Step 1: Load nation (25 rows) → flat array[25] of n_name strings
// Step 2: Load supplier (100K rows) → flat array[100001] of s_nationkey
// Step 3: Scan part (2M rows), filter p_name LIKE '%green%' → bitset + sorted int32 vector (~108K keys)
// Step 4: Build (ps_partkey,ps_suppkey)→ps_supplycost map using prebuilt hash_ps_partkey index
//         (~108K partkeys × 4 suppkeys each = ~430K entries); store as CompactHashMapPair
// Step 5: Build orders orderkey → (year_index, nation_key) combined lookup:
//         Scan orders (15M rows), build CompactHashMap<int32_t, uint8_t> orderkey→year_idx
//         year_idx = YEAR(o_orderdate) - 1992 (0..6)
//         Orders orderkeys are dense [1..60M odd] — use year lookup to avoid large flat array
// Step 6: Scan lineitem (60M rows) in parallel:
//         - Bitset filter on l_partkey (green parts)
//         - Probe ps_map for (l_partkey, l_suppkey) → ps_supplycost
//         - Probe orders_year_map for l_orderkey → year_idx
//         - Look up supp_nationkey[l_suppkey] → nation_idx
//         - Accumulate amount into thread-local agg[nation_idx][year_idx]
//         This eliminates the large intermediate hit vector and sort entirely.
// Step 7: Merge 64 thread-local agg arrays → sort → output CSV

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>
#include <omp.h>
#include <string>
#include <vector>
#include <array>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Prebuilt index layout for hash_ps_partkey.bin:
//   [uint32_t table_size]
//   [table_size × {int32_t key, uint32_t offset, uint32_t count}]  (12 bytes each)
//   [uint32_t positions...]
// Empty slot sentinel: key == 0 (TPC-H partkeys are 1-based, never 0)
// ---------------------------------------------------------------------------
struct PsPartkeySlot {
    int32_t  key;
    uint32_t offset;
    uint32_t count;
};

// ---------------------------------------------------------------------------
// Custom open-addressing hash map: int32_t key → uint8_t value
// Used for orderkey → year_index (values 0..6, stored as uint8, 0xFF = empty)
// Sized for ~15M orders with ~2x load headroom.
// ---------------------------------------------------------------------------
struct OrderYearMap {
    struct Slot {
        int32_t  key;
        uint8_t  year_idx; // 0..6 = year-1992; 0xFF = empty
        uint8_t  _pad[3];
    };
    std::vector<Slot> table;
    uint32_t mask;

    explicit OrderYearMap(size_t expected) {
        size_t cap = 1;
        while (cap < expected * 2) cap <<= 1;
        table.resize(cap);
        mask = (uint32_t)(cap - 1);
        for (auto& s : table) { s.key = 0; s.year_idx = 0xFF; }
    }

    inline void insert(int32_t key, uint8_t yi) {
        uint32_t pos = (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & mask;
        while (table[pos].year_idx != 0xFF) {
            if (table[pos].key == key) { table[pos].year_idx = yi; return; }
            pos = (pos + 1) & mask;
        }
        table[pos].key = key;
        table[pos].year_idx = yi;
    }

    inline uint8_t find(int32_t key) const {
        uint32_t pos = (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & mask;
        for (;;) {
            const Slot& s = table[pos];
            if (s.year_idx == 0xFF) return 0xFF; // not found
            if (s.key == key) return s.year_idx;
            pos = (pos + 1) & mask;
        }
    }
};

void run_Q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string base = gendb_dir + "/";

    // -------------------------------------------------------------------------
    // Phase 0: Load small dimension tables in parallel
    //   - supplier: flat array[100001] of s_nationkey
    //   - nation: flat array[25] of n_name strings
    // -------------------------------------------------------------------------
    std::vector<int8_t> supp_nationkey(100002, -1); // int8_t: nationkey 0..24 fits in int8
    std::string nation_names[25];

    {
        GENDB_PHASE("dim_load");

        // Load supplier: s_suppkey → s_nationkey
        {
            gendb::MmapColumn<int32_t> s_suppkey(base + "supplier/s_suppkey.bin");
            gendb::MmapColumn<int32_t> s_nationkey_col(base + "supplier/s_nationkey.bin");
            size_t n = s_suppkey.size();
            for (size_t i = 0; i < n; i++) {
                int32_t sk = s_suppkey[i];
                if (sk >= 0 && sk <= 100001)
                    supp_nationkey[sk] = (int8_t)s_nationkey_col[i];
            }
        }

        // Load nation names: n_nationkey → n_name
        {
            gendb::MmapColumn<int32_t> n_nationkey(base + "nation/n_nationkey.bin");
            int fd = ::open((base + "nation/n_name.bin").c_str(), O_RDONLY);
            if (fd < 0) throw std::runtime_error("Cannot open nation/n_name.bin");
            struct stat st; fstat(fd, &st);
            size_t fsz = st.st_size;
            const char* raw = (const char*)mmap(nullptr, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
            ::close(fd);
            if ((void*)raw == MAP_FAILED) throw std::runtime_error("mmap nation/n_name.bin failed");
            // Parse length-prefixed strings
            size_t pos = 0;
            size_t nrow = n_nationkey.size();
            for (size_t i = 0; i < nrow && pos + 4 <= fsz; i++) {
                int32_t len;
                memcpy(&len, raw + pos, 4); pos += 4;
                if (pos + (size_t)len > fsz) break;
                int32_t nk = n_nationkey[i];
                if (nk >= 0 && nk < 25)
                    nation_names[nk] = std::string(raw + pos, len);
                pos += len;
            }
            munmap((void*)raw, fsz);
        }
    }

    // -------------------------------------------------------------------------
    // Phase 1: Scan part, filter p_name LIKE '%green%'
    //          → bitset of green p_partkey values (~108K keys)
    // p_partkey is in [1..2000000]; use 2M+1 sized bitset
    // -------------------------------------------------------------------------
    const size_t PART_BITSET_WORDS = (2000001 + 63) / 64;
    std::vector<uint64_t> green_bitset(PART_BITSET_WORDS, 0ULL);
    std::vector<int32_t>  green_partkeys;
    green_partkeys.reserve(120000);

    {
        GENDB_PHASE("part_filter");

        gendb::MmapColumn<int32_t> p_partkey(base + "part/p_partkey.bin");
        size_t nparts = p_partkey.size(); // 2,000,000

        // Open p_name as raw bytes (variable-length, cannot use typed mmap)
        int fd = ::open((base + "part/p_name.bin").c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open part/p_name.bin");
        struct stat st; fstat(fd, &st);
        size_t fsz = st.st_size;
        const char* raw = (const char*)mmap(nullptr, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        if ((void*)raw == MAP_FAILED) throw std::runtime_error("mmap part/p_name.bin failed");
        madvise((void*)raw, fsz, MADV_SEQUENTIAL);

        // Sequential scan — variable-length strings prevent easy parallelism
        // ~60MB p_name.bin at sequential read speed is ~15ms
        size_t pos = 0;
        for (size_t i = 0; i < nparts && pos + 4 <= fsz; i++) {
            int32_t len;
            memcpy(&len, raw + pos, 4);
            pos += 4;
            const char* s = raw + pos;
            pos += (size_t)len;

            // Search for "green" substring (5 chars)
            if (len >= 5) {
                bool found = false;
                const int lim = len - 4;
                for (int j = 0; j < lim; j++) {
                    // Quick first-char check before full comparison
                    if (s[j] == 'g' && s[j+1] == 'r' && s[j+2] == 'e'
                        && s[j+3] == 'e' && s[j+4] == 'n') {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    int32_t pk = p_partkey[i];
                    green_partkeys.push_back(pk);
                    if (pk > 0 && (size_t)pk <= 2000000) {
                        green_bitset[(size_t)pk >> 6] |= (1ULL << ((size_t)pk & 63));
                    }
                }
            }
        }
        munmap((void*)raw, fsz);
    }

    // -------------------------------------------------------------------------
    // Phase 2: Build (ps_partkey, ps_suppkey) → ps_supplycost map
    //          using prebuilt hash_ps_partkey index
    //          ~430K entries (108K green parts × 4 suppkeys each)
    // -------------------------------------------------------------------------
    gendb::CompactHashMapPair<int64_t> ps_map(500000);

    {
        GENDB_PHASE("build_ps_map");

        // mmap the prebuilt index
        int fd = ::open((base + "indexes/hash_ps_partkey.bin").c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open hash_ps_partkey.bin");
        struct stat st; fstat(fd, &st);
        size_t fsz = st.st_size;
        const char* raw = (const char*)mmap(nullptr, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        if ((void*)raw == MAP_FAILED) throw std::runtime_error("mmap hash_ps_partkey.bin failed");
        madvise((void*)raw, fsz, MADV_RANDOM); // random access pattern

        uint32_t table_size;
        memcpy(&table_size, raw, 4);
        const PsPartkeySlot* idx_table = reinterpret_cast<const PsPartkeySlot*>(raw + 4);
        const uint32_t* idx_positions  = reinterpret_cast<const uint32_t*>(
            raw + 4 + (size_t)table_size * sizeof(PsPartkeySlot));
        uint32_t idx_mask = table_size - 1;

        // Load partsupp columns (accessed by row position from index)
        gendb::MmapColumn<int32_t> ps_suppkey_col(base + "partsupp/ps_suppkey.bin");
        gendb::MmapColumn<int64_t> ps_supplycost_col(base + "partsupp/ps_supplycost.bin");

        // For each green partkey, probe the index and collect (pk, sk) → cost
        for (int32_t pk : green_partkeys) {
            uint32_t h = (uint32_t)((uint64_t)(uint32_t)pk * 0x9E3779B97F4A7C15ULL >> 32) & idx_mask;
            // Linear probe; empty slot: key == 0 (TPC-H partkeys are 1-based)
            // Also handle count==0 with key!=0 as deleted/unused (shouldn't happen in this index)
            for (uint32_t probe = 0; probe <= idx_mask; probe++) {
                const PsPartkeySlot& slot = idx_table[h];
                if (slot.key == 0) break;       // empty slot → key not in index
                if (slot.key == pk) {
                    // Found — collect all partsupp rows for this partkey
                    for (uint32_t j = slot.offset; j < slot.offset + slot.count; j++) {
                        uint32_t row = idx_positions[j];
                        int32_t  sk  = ps_suppkey_col[row];
                        int64_t  cost = ps_supplycost_col[row];
                        ps_map.insert({pk, sk}, cost);
                    }
                    break;
                }
                h = (h + 1) & idx_mask;
            }
        }
        munmap((void*)raw, fsz);
    }

    // -------------------------------------------------------------------------
    // Phase 3: Scan orders, build orderkey → year_index map
    //          year_index = YEAR(o_orderdate) - 1992 (0..6)
    //          Orders: 15M rows. Use custom hash map (int32_t→uint8_t) for low memory.
    //          Orders orderkeys: odd integers 1..60M (sparse) → use hash map, not array
    // -------------------------------------------------------------------------
    // Prefetch lineitem columns BEFORE building orders map to overlap I/O
    gendb::MmapColumn<int32_t> l_orderkey(base + "lineitem/l_orderkey.bin");
    gendb::MmapColumn<int32_t> l_partkey(base  + "lineitem/l_partkey.bin");
    gendb::MmapColumn<int32_t> l_suppkey(base  + "lineitem/l_suppkey.bin");
    gendb::MmapColumn<int64_t> l_quantity(base + "lineitem/l_quantity.bin");
    gendb::MmapColumn<int64_t> l_extendedprice(base + "lineitem/l_extendedprice.bin");
    gendb::MmapColumn<int64_t> l_discount(base + "lineitem/l_discount.bin");

    // Fire prefetch for all lineitem columns (overlaps with orders map build)
    {
        GENDB_PHASE("prefetch_lineitem");
        mmap_prefetch_all(l_orderkey, l_partkey, l_suppkey,
                          l_quantity, l_extendedprice, l_discount);
    }

    OrderYearMap order_year_map(20000000); // 2^25 = 33M slots for 15M orders

    {
        GENDB_PHASE("build_order_year_map");

        gendb::MmapColumn<int32_t> o_orderkey(base + "orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(base + "orders/o_orderdate.bin");
        mmap_prefetch_all(o_orderkey, o_orderdate);

        size_t n = o_orderkey.size();
        // Build sequentially — avoids contention on the hash map
        // (parallel insert to single hash map requires locking or partitioning)
        for (size_t i = 0; i < n; i++) {
            int year = gendb::extract_year(o_orderdate[i]);
            int yi   = year - 1992;
            if (yi >= 0 && yi <= 6) {
                order_year_map.insert(o_orderkey[i], (uint8_t)yi);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 4: Parallel lineitem scan — fused probe + aggregate
    //          For each lineitem row:
    //            1. Bitset filter: l_partkey in green_bitset?
    //            2. ps_map probe: (l_partkey, l_suppkey) → ps_supplycost?
    //            3. order_year_map probe: l_orderkey → year_idx?
    //            4. supp_nationkey lookup: l_suppkey → nation_idx
    //            5. Accumulate into thread-local agg[nation_idx * YEAR_COUNT + year_idx]
    //
    //          Eliminates intermediate hit vector, sort, and separate orders pass.
    // -------------------------------------------------------------------------
    static constexpr int YEAR_OFFSET = 1992;
    static constexpr int YEAR_COUNT  = 8; // 1992..1998 = 7, use 8 for alignment

    int nthreads = omp_get_max_threads();

    // Thread-local aggregation: 25 nations × 8 years = 200 int64_t per thread
    // Each array is 200 × 8 = 1600 bytes — fits in L1 cache
    std::vector<std::array<int64_t, 25 * YEAR_COUNT>> local_agg(nthreads);
    for (auto& arr : local_agg) arr.fill(0LL);

    {
        GENDB_PHASE("lineitem_scan_probe_agg");

        size_t n = l_orderkey.size(); // 59,986,052

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& agg = local_agg[tid];

            #pragma omp for schedule(static, 65536)
            for (size_t i = 0; i < n; i++) {
                int32_t pk = l_partkey[i];

                // Fast bitset filter: is this a green part?
                if (pk <= 0 || (size_t)pk > 2000000) continue;
                if (!(green_bitset[(size_t)pk >> 6] & (1ULL << ((size_t)pk & 63)))) continue;

                int32_t sk = l_suppkey[i];

                // Probe (partkey, suppkey) → ps_supplycost
                const int64_t* cost_ptr = ps_map.find({pk, sk});
                if (!cost_ptr) continue;

                // Probe orderkey → year_index
                int32_t ok = l_orderkey[i];
                uint8_t yi = order_year_map.find(ok);
                if (yi == 0xFF) continue; // no matching order (shouldn't happen)

                // Supplier → nation lookup (O(1) flat array)
                if (sk <= 0 || sk > 100001) continue;
                int8_t nk = supp_nationkey[sk];
                if (nk < 0) continue;

                // Compute amount:
                // amount_real = ep_real * (1 - disc_real) - cost_real * qty_real
                // All stored as scaled int64 (×100):
                //   ep_raw = ep_real*100, disc_raw = disc_real*100,
                //   cost_raw = cost_real*100, qty_raw = qty_real*100
                // amount_scaled (×100):
                //   = (ep_raw * (100 - disc_raw) - cost_raw * qty_raw) / 100
                //   Each product is ×10000; dividing by 100 gives scale¹ (×100).
                // Output directly as double (no further division needed).
                int64_t ep   = l_extendedprice[i];
                int64_t disc = l_discount[i];
                int64_t qty  = l_quantity[i];
                int64_t cost = *cost_ptr;

                int64_t amount = (ep * (100LL - disc) - cost * qty) / 100;
                // amount is now in units of 1/100 (scale¹)

                agg[(int)nk * YEAR_COUNT + (int)yi] += amount;
            }
        }
    }

    // Merge thread-local aggregation arrays
    std::array<int64_t, 25 * YEAR_COUNT> global_agg{};
    global_agg.fill(0LL);
    for (const auto& arr : local_agg) {
        for (int i = 0; i < 25 * YEAR_COUNT; i++)
            global_agg[i] += arr[i];
    }

    // -------------------------------------------------------------------------
    // Phase 5: Sort and output
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        struct ResultRow {
            std::string nation;
            int         o_year;
            int64_t     sum_profit; // units of 1/100 (scale¹)
        };

        std::vector<ResultRow> results;
        results.reserve(175);

        for (int nk = 0; nk < 25; nk++) {
            for (int yi = 0; yi < 7; yi++) { // only years 1992..1998
                int64_t val = global_agg[nk * YEAR_COUNT + yi];
                if (val == 0) continue; // skip groups with no matching rows
                results.push_back({nation_names[nk], YEAR_OFFSET + yi, val});
            }
        }

        // ORDER BY nation ASC, o_year DESC
        std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.nation != b.nation) return a.nation < b.nation;
            return a.o_year > b.o_year;
        });

        // Write CSV output
        std::string outpath = results_dir + "/Q9.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        if (!f) throw std::runtime_error("Cannot open output file: " + outpath);
        fprintf(f, "nation,o_year,sum_profit\n");
        for (const auto& r : results) {
            // sum_profit is in units of 1/100; divide by 100 for 2 decimal places
            double val = (double)r.sum_profit / 100.0;
            fprintf(f, "%s,%d,%.2f\n", r.nation.c_str(), r.o_year, val);
        }
        fclose(f);
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
    run_Q9(gendb_dir, results_dir);
    return 0;
}
#endif
