#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

// ─────────────────────────── mmap helper ────────────────────────────────────
template<typename T>
const T* mmap_col(const std::string& path, size_t& n) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    n = st.st_size / sizeof(T);
    if (st.st_size > 0) posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    auto* p = reinterpret_cast<const T*>(
        mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE|MAP_POPULATE, fd, 0));
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return p;
}

// ─────────────── PSHashMap: (ps_partkey, ps_suppkey) → ps_supplycost ────────
// Keys are uint64 = (ps_partkey<<32)|ps_suppkey; 0 is sentinel (partkeys ≥1)
struct PSHashMap {
    uint64_t* keys;
    double*   vals;
    uint32_t  cap, mask;
    PSHashMap() : keys(nullptr), vals(nullptr), cap(0), mask(0) {}
    ~PSHashMap() { delete[] keys; delete[] vals; }

    void init(uint32_t min_cap) {
        cap = 1; while (cap < min_cap) cap <<= 1;
        keys = new uint64_t[cap](); // zero-init
        vals = new double[cap]();
        mask = cap - 1;
    }
    inline uint32_t slot(uint64_t k) const {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33;
        return (uint32_t)k & mask;
    }
    void insert(uint64_t key, double val) {
        uint32_t i = slot(key);
        while (keys[i]) i = (i + 1) & mask;
        keys[i] = key; vals[i] = val;
    }
    // Concurrent insert: CAS on key slot; safe because partsupp keys are unique.
    // Build phase completes before probe phase (OMP barrier), so val write is safe.
    void insert_concurrent(uint64_t key, double val) {
        uint32_t i = slot(key);
        uint64_t zero = 0;
        while (!__atomic_compare_exchange_n(&keys[i], &zero, key,
                                            false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
            zero = 0;
            i = (i + 1) & mask;
        }
        // Slot is now claimed by this thread (unique keys guarantee no duplicate)
        vals[i] = val;
    }
    // Returns 0.0 if not found (supplycost ≥ 1.0 in TPC-H, so 0 is a safe sentinel)
    inline double find(uint64_t key) const {
        uint32_t i = slot(key);
        while (keys[i] != key) {
            if (!keys[i]) return 0.0;
            i = (i + 1) & mask;
        }
        return vals[i];
    }
};

// OHashMap removed — replaced by flat array indexed by o_orderkey

// ─────────────────────────── main query ─────────────────────────────────────
void run_Q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string d = gendb_dir + "/";

    // ── Load nation name dictionary ──────────────────────────────────────────
    std::string nation_names[25];
    {
        std::ifstream f(d + "nation/n_name_dict.txt");
        std::string line;
        while (std::getline(f, line)) {
            auto eq = line.find('=');
            if (eq == std::string::npos) continue;
            int code = std::stoi(line.substr(0, eq));
            if (code >= 0 && code < 25) nation_names[code] = line.substr(eq + 1);
        }
    }

    // ── Phase 1: Dimension tables (nation + supplier) ────────────────────────
    // nation_code_of_nk[n_nationkey] = n_name dict code
    uint8_t nation_code_of_nk[25] = {};
    // supp_nation[s_suppkey] = n_name dict code
    uint8_t supp_nation[100001] = {};

    {
        GENDB_PHASE("dim_filter");

        size_t n_nat;
        auto* n_nationkey = mmap_col<int32_t>(d + "nation/n_nationkey.bin", n_nat);
        auto* n_name      = mmap_col<uint8_t>(d + "nation/n_name.bin",      n_nat);
        for (size_t i = 0; i < n_nat; i++)
            nation_code_of_nk[n_nationkey[i]] = n_name[i];

        size_t n_supp;
        auto* s_suppkey   = mmap_col<int32_t>(d + "supplier/s_suppkey.bin",   n_supp);
        auto* s_nationkey = mmap_col<int32_t>(d + "supplier/s_nationkey.bin", n_supp);
        for (size_t i = 0; i < n_supp; i++)
            supp_nation[s_suppkey[i]] = nation_code_of_nk[s_nationkey[i]];
    }

    // ── Phase 2: Part filter bitset (LIKE '%green%') ─────────────────────────
    // ~2M parts → bitset of 2_000_001 bits ≈ 250 KB
    constexpr int PART_MAX   = 2000001;
    constexpr int BSET_WORDS = (PART_MAX + 63) / 64; // 31251
    static uint64_t part_bitset[BSET_WORDS];
    memset(part_bitset, 0, sizeof(part_bitset));

    {
        GENDB_PHASE("build_part_bitset");

        size_t n_part;
        auto* p_partkey      = mmap_col<int32_t> (d + "part/p_partkey.bin",      n_part);
        size_t n_offsets;
        auto* p_name_offsets = mmap_col<uint32_t>(d + "part/p_name_offsets.bin", n_offsets);
        size_t n_data;
        auto* p_name_data    = mmap_col<char>    (d + "part/p_name_data.bin",    n_data);

        // Parallel scan: atomic OR into shared bitset (relaxed, safe for OR)
        #pragma omp parallel for schedule(static, 32768)
        for (size_t i = 0; i < n_part; i++) {
            uint32_t start = p_name_offsets[i];
            uint32_t len   = p_name_offsets[i + 1] - start;
            std::string_view sv(p_name_data + start, len);
            if (sv.find("green") != std::string_view::npos) {
                int32_t pk   = p_partkey[i];
                int     word = pk >> 6;
                uint64_t bit = 1ULL << (pk & 63);
                __atomic_fetch_or(&part_bitset[word], bit, __ATOMIC_RELAXED);
            }
        }
    }

    // ── Phase 3: Build partsupp hash map (filtered by qualifying parts) ──────
    // ~384K qualifying entries; 1M slots (load ~38%)
    PSHashMap ps_map;
    ps_map.init(1u << 20);

    {
        GENDB_PHASE("build_partsupp");

        size_t n_ps;
        auto* ps_partkey    = mmap_col<int32_t>(d + "partsupp/ps_partkey.bin",    n_ps);
        auto* ps_suppkey    = mmap_col<int32_t>(d + "partsupp/ps_suppkey.bin",    n_ps);
        auto* ps_supplycost = mmap_col<double> (d + "partsupp/ps_supplycost.bin", n_ps);

        #pragma omp parallel for schedule(static, 65536)
        for (size_t i = 0; i < n_ps; i++) {
            int32_t pk = ps_partkey[i];
            if (part_bitset[pk >> 6] & (1ULL << (pk & 63))) {
                uint64_t key = ((uint64_t)(uint32_t)pk << 32) | (uint32_t)ps_suppkey[i];
                ps_map.insert_concurrent(key, ps_supplycost[i]);
            }
        }
    }

    // ── Phase 4: Build orders flat array (o_orderkey → year_offset) ──────────
    // TPC-H SF10 o_orderkey ≤ 60,000,000. Flat array = O(1) probe, no collisions.
    // ~60MB uint8_t; 255 = sentinel (not found).
    constexpr int32_t ORDER_KEY_MAX = 60000001;
    uint8_t* order_year = new uint8_t[ORDER_KEY_MAX];
    memset(order_year, 255, ORDER_KEY_MAX);

    {
        GENDB_PHASE("build_orders");

        size_t n_ord;
        auto* o_orderkey  = mmap_col<int32_t>(d + "orders/o_orderkey.bin",  n_ord);
        auto* o_orderdate = mmap_col<int32_t>(d + "orders/o_orderdate.bin", n_ord);

        // Parallel build: each o_orderkey is unique → no write conflicts
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n_ord; i++) {
            int yr = gendb::extract_year(o_orderdate[i]);
            order_year[o_orderkey[i]] = (uint8_t)(yr - 1992);
        }
    }

    // ── Phase 5: Parallel lineitem scan + aggregation ────────────────────────
    // Thread-local double accumulators (hardware FP, fast).
    int max_threads = omp_get_max_threads();
    constexpr int NGROUPS = 25 * 7; // 175
    std::vector<double> all_profits((size_t)max_threads * NGROUPS, 0.0);

    {
        GENDB_PHASE("main_scan");

        size_t n_li;
        auto* l_orderkey      = mmap_col<int32_t>(d + "lineitem/l_orderkey.bin",      n_li);
        auto* l_partkey       = mmap_col<int32_t>(d + "lineitem/l_partkey.bin",       n_li);
        auto* l_suppkey       = mmap_col<int32_t>(d + "lineitem/l_suppkey.bin",       n_li);
        auto* l_extendedprice = mmap_col<double> (d + "lineitem/l_extendedprice.bin", n_li);
        auto* l_discount      = mmap_col<double> (d + "lineitem/l_discount.bin",      n_li);
        auto* l_quantity      = mmap_col<double> (d + "lineitem/l_quantity.bin",      n_li);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            double* profit = all_profits.data() + (size_t)tid * NGROUPS;

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_li; i++) {
                int32_t pk = l_partkey[i];
                // Primary filter: qualifying part bitset
                if (!(part_bitset[pk >> 6] & (1ULL << (pk & 63)))) continue;

                int32_t sk = l_suppkey[i];
                int32_t ok = l_orderkey[i];

                // Lookup year_offset via flat array (O(1), no collision)
                uint8_t yo = order_year[ok];
                if (__builtin_expect(yo == 255, 0)) continue;

                // Lookup ps_supplycost
                uint64_t ps_key = ((uint64_t)(uint32_t)pk << 32) | (uint32_t)sk;
                double ps_cost = ps_map.find(ps_key);

                // Lookup nation code
                uint8_t nc = supp_nation[sk];

                // Compute amount and accumulate
                double amount = l_extendedprice[i] * (1.0 - l_discount[i])
                              - ps_cost * l_quantity[i];
                profit[(int)nc * 7 + (int)yo] += amount;
            }
        }
    }

    // ── Merge thread-local profits ────────────────────────────────────────────
    double merged[25][7] = {};
    for (int t = 0; t < max_threads; t++) {
        const double* tp = all_profits.data() + (size_t)t * NGROUPS;
        for (int nc = 0; nc < 25; nc++)
            for (int yo = 0; yo < 7; yo++)
                merged[nc][yo] += tp[nc * 7 + yo];
    }

    // ── Phase 6: Sort and output ─────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        struct Row { const std::string* nation; int o_year; double sum_profit; };
        std::vector<Row> rows;
        rows.reserve(175);

        for (int nc = 0; nc < 25; nc++)
            for (int yo = 0; yo < 7; yo++)
                rows.push_back({&nation_names[nc], 1992 + yo, merged[nc][yo]});

        // Sort: nation ASC, o_year DESC
        std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
            int cmp = a.nation->compare(*b.nation);
            if (cmp != 0) return cmp < 0;
            return a.o_year > b.o_year;
        });

        std::string out_path = results_dir + "/Q9.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); exit(1); }
        fprintf(f, "nation,o_year,sum_profit\n");
        for (const auto& r : rows)
            fprintf(f, "%s,%d,%.2f\n", r.nation->c_str(), r.o_year, r.sum_profit);
        fclose(f);
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
    run_Q9(gendb_dir, results_dir);
    return 0;
}
#endif
