// Q9: Product Type Profit Measure
// Plan: 6-way join with p_name LIKE '%green%' filter
// Strategy: flat array lookups for nation/supplier, bitset for part filter,
//           custom hash maps for partsupp and orders,
//           morsel-driven parallel scan over lineitem with thread-local aggregation

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <omp.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// TPC-H standard nation names ordered by n_nationkey 0..24
static const char* TPCH_NATION_NAMES[25] = {
    "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT",
    "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA",
    "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
    "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA",
    "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES"
};

// -----------------------------------------------------------------------
// Custom hash map for (partkey, suppkey) composite key → supplycost
// -----------------------------------------------------------------------
struct PartSuppMap {
    struct Entry {
        uint64_t key;
        int64_t  value;
        bool     occupied;
    };
    std::vector<Entry> table;
    size_t mask;

    PartSuppMap() : mask(0) {}

    void reserve(size_t expected) {
        size_t cap = 1;
        while (cap < expected * 2) cap <<= 1;
        table.assign(cap, {0, 0, false});
        mask = cap - 1;
    }

    inline uint64_t hash_k(uint64_t k) const {
        return k * 0x9E3779B97F4A7C15ULL;
    }

    void insert(int32_t partkey, int32_t suppkey, int64_t val) {
        uint64_t k = ((uint64_t)(uint32_t)partkey << 32) | (uint32_t)suppkey;
        size_t pos = hash_k(k) & mask;
        while (table[pos].occupied) {
            if (table[pos].key == k) { table[pos].value = val; return; }
            pos = (pos + 1) & mask;
        }
        table[pos] = {k, val, true};
    }

    inline const int64_t* find(int32_t partkey, int32_t suppkey) const {
        uint64_t k = ((uint64_t)(uint32_t)partkey << 32) | (uint32_t)suppkey;
        size_t pos = hash_k(k) & mask;
        while (table[pos].occupied) {
            if (table[pos].key == k) return &table[pos].value;
            pos = (pos + 1) & mask;
        }
        return nullptr;
    }
};

// -----------------------------------------------------------------------
// Orders hash map: o_orderkey (int32_t) → o_orderdate (int32_t)
// -----------------------------------------------------------------------
struct OrdersMap {
    struct Entry {
        int32_t key;
        int32_t value;
        bool    occupied;
    };
    std::vector<Entry> table;
    size_t mask;

    OrdersMap() : mask(0) {}

    void reserve(size_t expected) {
        size_t cap = 1;
        while (cap < expected * 2) cap <<= 1;
        table.assign(cap, {0, 0, false});
        mask = cap - 1;
    }

    inline size_t hash_k(int32_t k) const {
        return (uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL;
    }

    void insert(int32_t key, int32_t val) {
        size_t pos = hash_k(key) & mask;
        while (table[pos].occupied) {
            if (table[pos].key == key) { table[pos].value = val; return; }
            pos = (pos + 1) & mask;
        }
        table[pos] = {key, val, true};
    }

    inline const int32_t* find(int32_t key) const {
        size_t pos = hash_k(key) & mask;
        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            pos = (pos + 1) & mask;
        }
        return nullptr;
    }
};

// -----------------------------------------------------------------------
// Main query function
// -----------------------------------------------------------------------
void run_Q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string base = gendb_dir + "/";

    // Derive path to .tbl data files from gendb_dir
    // gendb_dir: /path/tpc-h/gendb/tpch_sf10.gendb
    // data_dir:  /path/tpc-h/data/sf10/
    std::string gendb_path = gendb_dir;
    while (!gendb_path.empty() && gendb_path.back() == '/') gendb_path.pop_back();
    // Remove last component (e.g. "tpch_sf10.gendb")
    size_t s1 = gendb_path.rfind('/');
    std::string dirname = (s1 != std::string::npos) ? gendb_path.substr(s1 + 1) : gendb_path;
    std::string gendb_up = (s1 != std::string::npos) ? gendb_path.substr(0, s1) : ".";
    // Remove "gendb" directory
    size_t s2 = gendb_up.rfind('/');
    std::string bench_dir = (s2 != std::string::npos) ? gendb_up.substr(0, s2) : gendb_up;
    // Extract scale factor (e.g. "sf10" from "tpch_sf10.gendb")
    std::string sf = "sf10";
    {
        size_t sfpos = dirname.find("sf");
        if (sfpos != std::string::npos) {
            size_t dotpos = dirname.find('.', sfpos);
            sf = (dotpos != std::string::npos) ? dirname.substr(sfpos, dotpos - sfpos) : dirname.substr(sfpos);
        }
    }
    std::string data_dir = bench_dir + "/data/" + sf + "/";

    // ==============================================================
    // Phase 1: Load dimension tables (nation + supplier)
    // ==============================================================
    // supplier_nation_idx[s_suppkey] = nation agg index (0..24, alphabetical by name)
    static int8_t supplier_nation_idx[100001];
    memset(supplier_nation_idx, -1, sizeof(supplier_nation_idx));

    // Build nation_key_to_agg_idx and sorted_nation_names
    int8_t  nation_key_to_agg_idx[25];
    std::string sorted_nation_names[25];
    {
        struct NEntry { int key; const char* name; };
        NEntry entries[25];
        for (int i = 0; i < 25; i++) entries[i] = {i, TPCH_NATION_NAMES[i]};
        std::sort(entries, entries + 25, [](const NEntry& a, const NEntry& b){
            return strcmp(a.name, b.name) < 0;
        });
        for (int idx = 0; idx < 25; idx++) {
            nation_key_to_agg_idx[entries[idx].key] = (int8_t)idx;
            sorted_nation_names[idx] = entries[idx].name;
        }
    }

    {
        GENDB_PHASE("dim_filter");

        gendb::MmapColumn<int32_t> s_suppkey(base + "supplier/s_suppkey.bin");
        gendb::MmapColumn<int32_t> s_nationkey(base + "supplier/s_nationkey.bin");
        size_t s_rows = s_suppkey.size();
        for (size_t i = 0; i < s_rows; i++) {
            int32_t sk = s_suppkey[i];
            int32_t nk = s_nationkey[i];
            if (sk >= 0 && sk <= 100000 && nk >= 0 && nk < 25) {
                supplier_nation_idx[sk] = nation_key_to_agg_idx[nk];
            }
        }
    }

    // ==============================================================
    // Phase 2: Build part 'green' bitset from part.tbl
    // NOTE: p_name.bin uses a hash-based uint16 encoding (NOT a true dict)
    // where multiple different names share the same code — cannot use codes
    // to filter. Must read actual part names from part.tbl.
    // ==============================================================
    gendb::DenseBitmap part_green_bits(2000001);

    {
        GENDB_PHASE("part_filter");

        std::ifstream tbl(data_dir + "part.tbl");
        if (!tbl.is_open()) {
            throw std::runtime_error("Cannot open part.tbl at: " + data_dir + "part.tbl");
        }

        std::string line;
        line.reserve(256);
        while (std::getline(tbl, line)) {
            // Format: partkey|name|...
            // Fast parse: find first '|'
            const char* s = line.c_str();
            int32_t pk = 0;
            size_t j = 0;
            while (s[j] >= '0') { pk = pk * 10 + (s[j] - '0'); j++; }
            // s[j] == '|'
            j++; // skip '|'
            const char* name_start = s + j;
            // Find second '|'
            size_t name_end = j;
            while (s[name_end] && s[name_end] != '|') name_end++;
            size_t name_len = name_end - j;

            // Check for 'green' substring
            bool has_green = false;
            if (name_len >= 5) {
                const char* ns = name_start;
                const char* ne = ns + name_len - 4;
                for (const char* p = ns; p < ne; p++) {
                    if (p[0]=='g' && p[1]=='r' && p[2]=='e' && p[3]=='e' && p[4]=='n') {
                        has_green = true;
                        break;
                    }
                }
            }

            if (has_green && pk >= 1 && pk <= 2000000) {
                part_green_bits.set((size_t)pk);
            }
        }
    }

    // ==============================================================
    // Phase 3: Scan partsupp, probe bitset, build composite hashmap
    // ==============================================================
    PartSuppMap partsupp_map;

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> ps_partkey(base + "partsupp/ps_partkey.bin");
        gendb::MmapColumn<int32_t> ps_suppkey(base + "partsupp/ps_suppkey.bin");
        gendb::MmapColumn<int64_t> ps_supplycost(base + "partsupp/ps_supplycost.bin");
        size_t ps_rows = ps_partkey.size();

        // Count green entries for pre-sizing
        size_t green_count = 0;
        for (size_t i = 0; i < ps_rows; i++) {
            int32_t pk = ps_partkey[i];
            if (pk >= 1 && (size_t)pk <= 2000000 && part_green_bits.test((size_t)pk)) {
                ++green_count;
            }
        }
        partsupp_map.reserve(green_count + green_count / 4 + 64);

        for (size_t i = 0; i < ps_rows; i++) {
            int32_t pk = ps_partkey[i];
            if (pk >= 1 && (size_t)pk <= 2000000 && part_green_bits.test((size_t)pk)) {
                partsupp_map.insert(pk, ps_suppkey[i], ps_supplycost[i]);
            }
        }
    }

    // ==============================================================
    // Phase 4: Build orders hashmap (sequential — 15M rows)
    // ==============================================================
    OrdersMap orders_map;

    {
        gendb::MmapColumn<int32_t> o_orderkey(base + "orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(base + "orders/o_orderdate.bin");
        size_t o_rows = o_orderkey.size();

        orders_map.reserve(o_rows + o_rows / 4 + 64);

        for (size_t i = 0; i < o_rows; i++) {
            orders_map.insert(o_orderkey[i], o_orderdate[i]);
        }
    }

    // ==============================================================
    // Phase 5: Main lineitem scan with fused operations
    // amount = extprice*(1-discount) - supplycost*quantity
    // Accumulate:
    //   part1 = extprice_raw * (100 - discount_raw)  [scale 10000]
    //   part2 = supplycost_raw * quantity_raw          [scale 100]
    //   final = (part1 - part2*100) / 10000.0
    // ==============================================================
    const int NUM_NATIONS = 25;
    const int NUM_YEARS   = 8;   // years 1992..1999, indices 0..7
    const int YEAR_BASE   = 1992;

    int nthreads = omp_get_max_threads();
    if (nthreads > 64) nthreads = 64;

    std::vector<int64_t> tl_agg_a(nthreads * NUM_NATIONS * NUM_YEARS, 0);
    std::vector<int64_t> tl_agg_b(nthreads * NUM_NATIONS * NUM_YEARS, 0);

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_partkey(base  + "lineitem/l_partkey.bin");
        gendb::MmapColumn<int32_t> l_suppkey(base  + "lineitem/l_suppkey.bin");
        gendb::MmapColumn<int32_t> l_orderkey(base + "lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extprice(base + "lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount(base + "lineitem/l_discount.bin");
        gendb::MmapColumn<int64_t> l_quantity(base + "lineitem/l_quantity.bin");

        mmap_prefetch_all(l_partkey, l_suppkey, l_orderkey, l_extprice, l_discount, l_quantity);

        const size_t L_ROWS = l_partkey.size();
        const int32_t* lp  = l_partkey.data;
        const int32_t* ls  = l_suppkey.data;
        const int32_t* lok = l_orderkey.data;
        const int64_t* lep = l_extprice.data;
        const int64_t* ld  = l_discount.data;
        const int64_t* lq  = l_quantity.data;

        const PartSuppMap& psmap = partsupp_map;
        const OrdersMap&   omap  = orders_map;

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            int64_t* my_a = tl_agg_a.data() + tid * NUM_NATIONS * NUM_YEARS;
            int64_t* my_b = tl_agg_b.data() + tid * NUM_NATIONS * NUM_YEARS;

            #pragma omp for schedule(static, 65536)
            for (size_t i = 0; i < L_ROWS; i++) {
                int32_t partkey = lp[i];
                int32_t suppkey = ls[i];

                // Step 1: probe partsupp map (primary filter ~0.72% hit rate)
                const int64_t* ps_cost = psmap.find(partkey, suppkey);
                if (!ps_cost) continue;

                // Step 2: get nation agg index from supplier flat array
                if (suppkey < 0 || suppkey > 100000) continue;
                int8_t nidx = supplier_nation_idx[suppkey];
                if (nidx < 0) continue;

                // Step 3: probe orders map
                const int32_t* odate_ptr = omap.find(lok[i]);
                if (!odate_ptr) continue;

                // Step 4: extract year O(1)
                int32_t od = *odate_ptr;
                if (od < 0 || od >= 30000) continue;
                int yr = gendb::extract_year(od);
                int yidx = yr - YEAR_BASE;
                if (yidx < 0 || yidx >= NUM_YEARS) continue;

                // Step 5: compute amount components (integer arithmetic)
                // amount = extprice*(1-discount) - supplycost*quantity
                // = extprice_raw/100 * (1-discount_raw/100) - supplycost_raw/100 * quantity_raw
                // Accumulate at scale 10000:
                //   part1 = extprice_raw * (100 - discount_raw)    [scale=10000]
                //   part2 = supplycost_raw * quantity_raw            [scale=100]
                int64_t part1 = lep[i] * (100LL - ld[i]);  // scale 10000
                int64_t part2 = (*ps_cost) * lq[i];         // scale 100

                int cell = (int)nidx * NUM_YEARS + yidx;
                my_a[cell] += part1;
                my_b[cell] += part2;
            }
        }
    }

    // ==============================================================
    // Phase 6: Merge thread-local arrays
    // ==============================================================
    std::vector<int64_t> global_a(NUM_NATIONS * NUM_YEARS, 0);
    std::vector<int64_t> global_b(NUM_NATIONS * NUM_YEARS, 0);
    for (int t = 0; t < nthreads; t++) {
        const int64_t* ta = tl_agg_a.data() + t * NUM_NATIONS * NUM_YEARS;
        const int64_t* tb = tl_agg_b.data() + t * NUM_NATIONS * NUM_YEARS;
        for (int c = 0; c < NUM_NATIONS * NUM_YEARS; c++) {
            global_a[c] += ta[c];
            global_b[c] += tb[c];
        }
    }

    // ==============================================================
    // Phase 7: Collect and sort results
    // ==============================================================
    struct ResultRow {
        const char* nation;
        int year;
        double sum_profit;
    };

    std::vector<ResultRow> results;
    results.reserve(NUM_NATIONS * NUM_YEARS);

    for (int n = 0; n < NUM_NATIONS; n++) {
        for (int y = 0; y < NUM_YEARS; y++) {
            int cell = n * NUM_YEARS + y;
            int64_t a = global_a[cell];
            int64_t b = global_b[cell];
            if (a == 0 && b == 0) continue;
            // profit = (part1 - part2*100) / 10000.0
            double profit = (double)(a - b * 100LL) / 10000.0;
            results.push_back({sorted_nation_names[n].c_str(), YEAR_BASE + y, profit});
        }
    }

    // Sort: ORDER BY nation ASC, o_year DESC
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        int cmp = strcmp(a.nation, b.nation);
        if (cmp != 0) return cmp < 0;
        return a.year > b.year;
    });

    // ==============================================================
    // Phase 8: Output CSV
    // ==============================================================
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q9.csv";
        std::ofstream out(out_path);
        if (!out.is_open()) throw std::runtime_error("Cannot open output: " + out_path);

        out << "nation,o_year,sum_profit\n";
        char buf[64];
        for (const auto& r : results) {
            snprintf(buf, sizeof(buf), "%.2f", r.sum_profit);
            out << r.nation << ',' << r.year << ',' << buf << '\n';
        }
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
