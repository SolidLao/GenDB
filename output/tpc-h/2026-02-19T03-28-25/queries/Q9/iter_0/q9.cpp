// Q9: Product Type Profit Measure
// Strategy: bitset semi-join on part (p_name LIKE '%green%'),
//           composite hash map for partsupp, open-addressing hash map for orders year,
//           direct array for supplier->nationkey, parallel lineitem scan with thread-local agg.

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <cstdio>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

// ============================================================
// Constants
// ============================================================
static constexpr int YEAR_MIN  = 1992;
static constexpr int YEAR_RANGE = 10;   // 1992..2001 (TPC-H: 1992-1998)
static constexpr int NUM_NATIONS = 25;
static constexpr int MAX_THREADS = 64;

// ============================================================
// Helpers: mmap typed binary file
// ============================================================
template<typename T>
static const T* mmap_col(const std::string& path, size_t& out_count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_count = (size_t)st.st_size / sizeof(T);
    if (st.st_size == 0) { close(fd); return nullptr; }
    const T* p = reinterpret_cast<const T*>(
        mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);
    if (p == MAP_FAILED) { perror(("mmap: " + path).c_str()); exit(1); }
    return p;
}

// ============================================================
// Compact open-addressing hash map
// int64_t key → int64_t value; EMPTY key = 0
// Valid partsupp composite keys: pk*100001+sk >= 100002 (never 0)
// ============================================================
struct PSHashMap {
    struct Entry { int64_t key; int64_t val; };
    std::vector<Entry> tbl;
    size_t mask;

    explicit PSHashMap(size_t min_cap) {
        size_t c = 1;
        while (c < min_cap) c <<= 1;
        tbl.assign(c, {0LL, 0LL});
        mask = c - 1;
    }

    inline void insert(int64_t key, int64_t val) {
        size_t h = size_t(uint64_t(key) * 11400714819323198485ULL) & mask;
        while (tbl[h].key) h = (h + 1) & mask;
        tbl[h].key = key;
        tbl[h].val  = val;
    }

    inline const int64_t* find(int64_t key) const {
        size_t h = size_t(uint64_t(key) * 11400714819323198485ULL) & mask;
        while (tbl[h].key) {
            if (tbl[h].key == key) return &tbl[h].val;
            h = (h + 1) & mask;
        }
        return nullptr;
    }
};

// ============================================================
// Compact open-addressing hash map
// int32_t key → int16_t year; EMPTY key = 0
// TPC-H orderkeys: always >= 1, so key=0 is safe empty sentinel
// ============================================================
struct OYearMap {
    struct Entry { int32_t key; int16_t year; uint16_t _pad; };
    std::vector<Entry> tbl;
    uint32_t mask;

    explicit OYearMap(size_t min_cap) {
        size_t c = 1;
        while (c < min_cap) c <<= 1;
        tbl.assign(c, {0, 0, 0});
        mask = uint32_t(c - 1);
    }

    inline void insert(int32_t key, int16_t year) {
        uint32_t h = uint32_t(key) * 2654435761U & mask;
        while (tbl[h].key) h = (h + 1) & mask;
        tbl[h].key  = key;
        tbl[h].year = year;
    }

    inline int16_t find(int32_t key) const {
        uint32_t h = uint32_t(key) * 2654435761U & mask;
        while (tbl[h].key) {
            if (tbl[h].key == key) return tbl[h].year;
            h = (h + 1) & mask;
        }
        return -1;
    }
};

// ============================================================
// Per-thread aggregation (cache-line aligned to prevent false sharing)
// ============================================================
struct alignas(64) ThreadAgg {
    int64_t data[NUM_NATIONS][YEAR_RANGE];
};

// ============================================================
// Main query
// ============================================================
void run_q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string d = gendb_dir + "/";

    // -------------------------------------------------------
    // Part bitset: 2M bits, 250KB
    // -------------------------------------------------------
    static constexpr int BITSET_WORDS = (2000001 + 63) / 64;
    std::vector<uint64_t> part_bitset(BITSET_WORDS, 0ULL);

    // Supplier → nationkey direct lookup (indexed by s_suppkey, 0..100000)
    std::vector<int32_t> sup_nat(100001, -1);

    // Nation names indexed by n_nationkey
    std::string nation_names[NUM_NATIONS];

    // -------------------------------------------------------
    // Phase 1: dimension tables + part filter
    // -------------------------------------------------------
    {
        GENDB_PHASE("dim_filter");

        // --- Nation names ---
        {
            size_t cnt;
            const int32_t* n_natkey = mmap_col<int32_t>(d + "nation/n_nationkey.bin", cnt);
            const int16_t* n_name   = mmap_col<int16_t>(d + "nation/n_name.bin", cnt);

            // Load dict (either "code=value" or plain lines)
            std::vector<std::string> dict;
            {
                std::ifstream f(d + "nation/n_name_dict.txt");
                std::string line;
                while (std::getline(f, line)) {
                    size_t eq = line.find('=');
                    if (eq != std::string::npos) {
                        int code = std::stoi(line.substr(0, eq));
                        if (code >= (int)dict.size()) dict.resize(code + 1);
                        dict[code] = line.substr(eq + 1);
                    } else {
                        dict.push_back(line);
                    }
                }
            }
            for (size_t i = 0; i < cnt; ++i) {
                int32_t nk   = n_natkey[i];
                int16_t code = n_name[i];
                if (nk >= 0 && nk < NUM_NATIONS &&
                    code >= 0 && code < (int)dict.size()) {
                    nation_names[nk] = dict[code];
                }
            }
        }

        // --- Supplier nationkey array ---
        {
            size_t cnt;
            const int32_t* s_suppkey  = mmap_col<int32_t>(d + "supplier/s_suppkey.bin",  cnt);
            const int32_t* s_nationkey = mmap_col<int32_t>(d + "supplier/s_nationkey.bin", cnt);
            for (size_t i = 0; i < cnt; ++i) {
                int32_t sk = s_suppkey[i];
                if (sk >= 0 && sk <= 100000)
                    sup_nat[sk] = s_nationkey[i];
            }
        }

        // --- Part filter: p_name LIKE '%green%' → bitset ---
        {
            size_t pkey_cnt;
            const int32_t* p_partkey = mmap_col<int32_t>(d + "part/p_partkey.bin", pkey_cnt);

            // Open p_name.bin (variable-length strings: int32_t len + bytes)
            int fd_name = open((d + "part/p_name.bin").c_str(), O_RDONLY);
            if (fd_name < 0) { perror("open p_name.bin"); exit(1); }
            struct stat st_name; fstat(fd_name, &st_name);
            const char* name_data = (const char*)mmap(
                nullptr, st_name.st_size, PROT_READ, MAP_PRIVATE, fd_name, 0);
            close(fd_name);
            if (name_data == MAP_FAILED) { perror("mmap p_name.bin"); exit(1); }

            const char* ptr = name_data;
            const char* end_data = name_data + st_name.st_size;

            for (size_t i = 0; i < pkey_cnt && ptr + 4 <= end_data; ++i) {
                int32_t len = *reinterpret_cast<const int32_t*>(ptr);
                ptr += 4;
                const char* str = ptr;

                // Substring search for 'green'
                bool found = false;
                for (int j = 0; j <= len - 5 && !found; ++j) {
                    if (str[j]   == 'g' && str[j+1] == 'r' &&
                        str[j+2] == 'e' && str[j+3] == 'e' && str[j+4] == 'n')
                        found = true;
                }
                if (found) {
                    int32_t pk = p_partkey[i];
                    if (pk >= 0 && pk <= 2000000)
                        part_bitset[pk >> 6] |= (1ULL << (pk & 63));
                }
                ptr += len;
            }

            munmap((void*)name_data, st_name.st_size);
        }
    }

    // -------------------------------------------------------
    // Phase 2: Build hash structures
    // -------------------------------------------------------

    // Partsupp composite map: ~400K entries at SF10 (5% of 8M)
    // Capacity = 2^21 = 2,097,152 (~19% load) → fast probes
    PSHashMap ps_map(1 << 21);

    // Orders year map: ~15M entries
    // Capacity = 2^25 = 33,554,432 (~45% load) → low collision
    OYearMap o_year_map(1 << 25);

    {
        GENDB_PHASE("build_joins");

        // --- Partsupp composite map (filtered by part_bitset) ---
        {
            size_t cnt;
            const int32_t* ps_partkey   = mmap_col<int32_t>(d + "partsupp/ps_partkey.bin",   cnt);
            const int32_t* ps_suppkey   = mmap_col<int32_t>(d + "partsupp/ps_suppkey.bin",   cnt);
            const int64_t* ps_supplycost = mmap_col<int64_t>(d + "partsupp/ps_supplycost.bin", cnt);

            for (size_t i = 0; i < cnt; ++i) {
                int32_t pk = ps_partkey[i];
                if (!(part_bitset[pk >> 6] & (1ULL << (pk & 63)))) continue;
                int32_t sk  = ps_suppkey[i];
                int64_t key = (int64_t)pk * 100001LL + sk;
                ps_map.insert(key, ps_supplycost[i]);
            }
        }

        // --- Orders year map ---
        {
            size_t cnt;
            const int32_t* o_orderkey  = mmap_col<int32_t>(d + "orders/o_orderkey.bin",  cnt);
            const int32_t* o_orderdate = mmap_col<int32_t>(d + "orders/o_orderdate.bin", cnt);

            for (size_t i = 0; i < cnt; ++i) {
                int16_t yr = (int16_t)gendb::extract_year(o_orderdate[i]);
                o_year_map.insert(o_orderkey[i], yr);
            }
        }
    }

    // -------------------------------------------------------
    // Phase 3: Parallel lineitem scan (fused joins + aggregation)
    // -------------------------------------------------------

    // Thread-local aggregation: sum_profit_raw[nation][year_offset]
    // amount_raw = ext*(100-disc) - cost*qty; actual = raw/10000
    std::vector<ThreadAgg> thread_agg(MAX_THREADS);
    for (auto& ta : thread_agg) memset(ta.data, 0, sizeof(ta.data));

    {
        GENDB_PHASE("main_scan");

        size_t li_cnt;
        const int32_t* li_partkey      = mmap_col<int32_t>(d + "lineitem/l_partkey.bin",      li_cnt);
        const int32_t* li_suppkey      = mmap_col<int32_t>(d + "lineitem/l_suppkey.bin",      li_cnt);
        const int32_t* li_orderkey     = mmap_col<int32_t>(d + "lineitem/l_orderkey.bin",     li_cnt);
        const int64_t* li_extprice     = mmap_col<int64_t>(d + "lineitem/l_extendedprice.bin",li_cnt);
        const int64_t* li_discount     = mmap_col<int64_t>(d + "lineitem/l_discount.bin",     li_cnt);
        const int64_t* li_quantity     = mmap_col<int64_t>(d + "lineitem/l_quantity.bin",     li_cnt);

        const uint64_t* bitset_ptr = part_bitset.data();
        const int32_t*  sup_nat_ptr = sup_nat.data();

        #pragma omp parallel for schedule(dynamic, 16384) num_threads(MAX_THREADS)
        for (size_t i = 0; i < li_cnt; ++i) {
            // 1. Bitset semi-join on l_partkey (eliminates ~95% of rows)
            int32_t pk = li_partkey[i];
            if (!(bitset_ptr[pk >> 6] & (1ULL << (pk & 63)))) continue;

            int32_t sk = li_suppkey[i];
            int32_t ok = li_orderkey[i];

            // 2. Probe partsupp composite map for ps_supplycost
            int64_t ps_key = (int64_t)pk * 100001LL + sk;
            const int64_t* ps_cost_ptr = ps_map.find(ps_key);
            if (!ps_cost_ptr) continue;

            // 3. Probe orders year map
            int16_t yr = o_year_map.find(ok);
            if (yr < YEAR_MIN) continue;
            int year_offset = yr - YEAR_MIN;
            if (year_offset < 0 || year_offset >= YEAR_RANGE) continue;

            // 4. Supplier → nationkey (direct array)
            int32_t nat = sup_nat_ptr[sk];
            if (nat < 0 || nat >= NUM_NATIONS) continue;

            // 5. Compute profit contribution (integer arithmetic, scaled ×10000)
            //    amount_raw = ext*(100-disc) - cost*qty
            //    SQL amount  = amount_raw / 10000
            int64_t ext  = li_extprice[i];
            int64_t disc = li_discount[i];
            int64_t qty  = li_quantity[i];
            int64_t cost = *ps_cost_ptr;
            int64_t amount_raw = ext * (100LL - disc) - cost * qty;

            // 6. Accumulate into thread-local array
            int tid = omp_get_thread_num();
            thread_agg[tid].data[nat][year_offset] += amount_raw;
        }
    }

    // -------------------------------------------------------
    // Merge thread-local aggregations
    // -------------------------------------------------------
    int64_t global_agg[NUM_NATIONS][YEAR_RANGE] = {};
    for (int t = 0; t < MAX_THREADS; ++t)
        for (int n = 0; n < NUM_NATIONS; ++n)
            for (int y = 0; y < YEAR_RANGE; ++y)
                global_agg[n][y] += thread_agg[t].data[n][y];

    // -------------------------------------------------------
    // Phase 4: Sort and output
    // -------------------------------------------------------
    {
        GENDB_PHASE("output");

        struct Row {
            const char* nation;
            int         year;
            double      sum_profit;
        };
        std::vector<Row> rows;
        rows.reserve(200);

        for (int n = 0; n < NUM_NATIONS; ++n) {
            for (int y = 0; y < YEAR_RANGE; ++y) {
                if (global_agg[n][y] == 0) continue;
                rows.push_back({
                    nation_names[n].c_str(),
                    YEAR_MIN + y,
                    (double)global_agg[n][y] / 10000.0
                });
            }
        }

        // Sort: nation ASC, o_year DESC
        std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
            int cmp = strcmp(a.nation, b.nation);
            if (cmp != 0) return cmp < 0;
            return a.year > b.year;
        });

        // Write CSV
        std::string out_path = results_dir + "/Q9.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(("fopen: " + out_path).c_str()); exit(1); }
        fprintf(f, "nation,o_year,sum_profit\n");
        for (const auto& r : rows)
            fprintf(f, "%s,%d,%.2f\n", r.nation, r.year, r.sum_profit);
        fclose(f);

        std::printf("Wrote %zu rows to %s\n", rows.size(), out_path.c_str());
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q9(gendb_dir, results_dir);
    return 0;
}
#endif
