#include <cstdint>
#include <cstring>
#include <cstdio>
#include <cassert>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <climits>
#include <iostream>

#include "date_utils.h"
#include "timing_utils.h"

// ── mmap helper ─────────────────────────────────────────────────────────────
template<typename T>
static const T* mmap_file(const std::string& path, size_t& n_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    size_t sz = st.st_size;
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    madvise(p, sz, MADV_SEQUENTIAL);
    posix_fadvise(fd, 0, sz, POSIX_FADV_SEQUENTIAL);
    close(fd);
    n_out = sz / sizeof(T);
    return reinterpret_cast<const T*>(p);
}

template<typename T>
static const T* mmap_file_bytes(const std::string& path, size_t& bytes_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    bytes_out = st.st_size;
    void* p = mmap(nullptr, bytes_out, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    madvise(p, bytes_out, MADV_SEQUENTIAL);
    close(fd);
    return reinterpret_cast<const T*>(p);
}

// ── local partsupp hash table slot (L3-resident, 0 = empty sentinel) ─────────
struct PSLocalSlot {
    int64_t key;   // composite (partkey<<32 | suppkey), 0 = empty
    double  cost;  // ps_supplycost
};
static_assert(sizeof(PSLocalSlot) == 16, "PSLocalSlot must be 16 bytes");

// ── inline hash functions ─────────────────────────────────────────────────────
static inline uint32_t ps_hash(int64_t key, uint32_t mask) {
    uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    h ^= h >> 33;
    return (uint32_t)(h & mask);
}

// ── main query function ───────────────────────────────────────────────────────
void run_q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string db = gendb_dir;

    // ── DATA LOADING ─────────────────────────────────────────────────────────
    size_t n_part, n_sup, n_nat, n_li;
    size_t n_ps, n_ord;
    const int32_t*  p_partkey_data;
    const char*     p_name_data;
    const int32_t*  s_suppkey_data;
    const int32_t*  s_nationkey_data;
    const int32_t*  n_nationkey_data;
    const char*     n_name_data;
    const int32_t*  l_partkey_data;
    const int32_t*  l_suppkey_data;
    const int32_t*  l_orderkey_data;
    const double*   l_extprice_data;
    const double*   l_discount_data;
    const double*   l_quantity_data;
    const int32_t*  ps_partkey_data;
    const int32_t*  ps_suppkey_data;
    const double*   ps_supplycost_data;
    const int32_t*  o_orderkey_data;
    const int32_t*  o_orderdate_data;

    {
        GENDB_PHASE("data_loading");
        size_t tmp;
        // Part
        p_partkey_data  = mmap_file<int32_t>(db + "/part/p_partkey.bin", n_part);
        p_name_data     = mmap_file_bytes<char>(db + "/part/p_name.bin", tmp);
        // Supplier
        s_suppkey_data   = mmap_file<int32_t>(db + "/supplier/s_suppkey.bin", n_sup);
        s_nationkey_data = mmap_file<int32_t>(db + "/supplier/s_nationkey.bin", tmp);
        // Nation
        n_nationkey_data = mmap_file<int32_t>(db + "/nation/n_nationkey.bin", n_nat);
        n_name_data      = mmap_file_bytes<char>(db + "/nation/n_name.bin", tmp);
        // Lineitem
        l_partkey_data  = mmap_file<int32_t>(db + "/lineitem/l_partkey.bin", n_li);
        l_suppkey_data  = mmap_file<int32_t>(db + "/lineitem/l_suppkey.bin", tmp);
        l_orderkey_data = mmap_file<int32_t>(db + "/lineitem/l_orderkey.bin", tmp);
        l_extprice_data = mmap_file<double> (db + "/lineitem/l_extendedprice.bin", tmp);
        l_discount_data = mmap_file<double> (db + "/lineitem/l_discount.bin", tmp);
        l_quantity_data = mmap_file<double> (db + "/lineitem/l_quantity.bin", tmp);
        // Partsupp columns (used to build local L3-resident hash table)
        ps_partkey_data    = mmap_file<int32_t>(db + "/partsupp/ps_partkey.bin",    n_ps);
        ps_suppkey_data    = mmap_file<int32_t>(db + "/partsupp/ps_suppkey.bin",    tmp);
        ps_supplycost_data = mmap_file<double> (db + "/partsupp/ps_supplycost.bin", tmp);
        // Orders — direct columns for O(1) year lookup array
        o_orderkey_data  = mmap_file<int32_t>(db + "/orders/o_orderkey.bin",  n_ord);
        o_orderdate_data = mmap_file<int32_t>(db + "/orders/o_orderdate.bin",  tmp);
    }

    // ── Local partsupp hash table (2M slots × 16B = 32MB, L3-resident) ─────────
    // Key sentinel = 0 (valid composite keys always have partkey>=1, suppkey>=1)
    static PSLocalSlot ps_local_ht[2097152];

    // Direct order_year lookup: int8_t[60000001], indexed by o_orderkey
    // Replaces 396MB orders hash index with 60MB array. Year stored as (year-1992), 0xFF=invalid.
    static int8_t order_year_arr[60000001];

    // Constants (defined early for use in parallel phases)
    constexpr int N_THREADS = 64;

    // ── DIM FILTER: part → partkey bitset (parallel) ─────────────────────────
    static bool partkey_bitset[2000001];
    {
        GENDB_PHASE("dim_filter");
        memset(partkey_bitset, 0, sizeof(partkey_bitset));
        #pragma omp parallel for num_threads(N_THREADS) schedule(static, 4096)
        for (size_t i = 0; i < n_part; i++) {
            const char* name = p_name_data + i * 56;
            if (strstr(name, "green") != nullptr) {
                int32_t pk = p_partkey_data[i];
                if (pk >= 0 && pk <= 2000000)
                    partkey_bitset[pk] = true;  // no conflict: different pk values
            }
        }
    }

    // ── BUILD: order_year_arr — parallel fill ──────────────────────────────────
    {
        GENDB_PHASE("build_order_year");
        // Parallel init: distributes BSS CoW page faults across all threads
        // (serial memset triggers 15K CoW faults sequentially = ~45ms penalty)
        #pragma omp parallel for num_threads(N_THREADS) schedule(static)
        for (size_t i = 0; i <= 60000000; i++) order_year_arr[i] = (int8_t)0xFF;
        #pragma omp parallel for num_threads(N_THREADS) schedule(static)
        for (size_t i = 0; i < n_ord; i++) {
            int32_t ok = o_orderkey_data[i];
            int32_t yr = gendb::extract_year(o_orderdate_data[i]) - 1992;
            if (ok >= 0 && ok <= 60000000 && yr >= 0 && yr <= 6)
                order_year_arr[ok] = (int8_t)yr;
        }
    }

    // ── BUILD: supplier → nationkey direct array ──────────────────────────────
    // Also build nation_name array
    static int32_t suppkey_to_nationkey[100001];
    static std::string nation_name[25];
    {
        GENDB_PHASE("build_joins");
        memset(suppkey_to_nationkey, 0, sizeof(suppkey_to_nationkey));
        for (size_t i = 0; i < n_sup; i++) {
            int32_t sk = s_suppkey_data[i];
            int32_t nk = s_nationkey_data[i];
            if (sk >= 1 && sk <= 100000)
                suppkey_to_nationkey[sk] = nk;
        }
        for (size_t i = 0; i < n_nat; i++) {
            int32_t nk = n_nationkey_data[i];
            if (nk >= 0 && nk < 25) {
                const char* nm = n_name_data + i * 26;
                nation_name[nk] = std::string(nm, strnlen(nm, 26));
            }
        }
    }

    // ── MAIN SCAN: parallel lineitem scan ──────────────────────────────────────
    // Aggregation: 25 nations × 7 years (1992..1998), slot = nation_idx*7 + (year-1992)
    constexpr int N_GROUPS = 175;

    static double thread_agg[N_THREADS][N_GROUPS];
    memset(thread_agg, 0, sizeof(thread_agg));

    {
        GENDB_PHASE("main_scan");
        #pragma omp parallel num_threads(N_THREADS)
        {
            int tid = omp_get_thread_num();
            double* local_agg = thread_agg[tid];

            #pragma omp for schedule(static, 65536)
            for (size_t i = 0; i < n_li; i++) {
                int32_t lpartkey = l_partkey_data[i];

                // Check part filter
                if (!partkey_bitset[lpartkey]) continue;

                int32_t lsuppkey  = l_suppkey_data[i];
                int32_t lorderkey = l_orderkey_data[i];

                // Probe partsupp hash index with composite key (lpartkey<<32 | (uint32_t)lsuppkey)
                int64_t ps_key = ((int64_t)lpartkey << 32) | (uint32_t)lsuppkey;
                uint32_t ps_slot_idx = ps_hash(ps_key, ps_mask);
                uint32_t ps_row = UINT32_MAX;
                for (uint32_t probe = 0; probe < ps_ht_size; probe++) {
                    const PSSlot& sl = ps_slots[ps_slot_idx];
                    if (sl.key == INT64_MIN) break;
                    if (sl.key == ps_key) {
                        ps_row = ps_positions[sl.offset];
                        break;
                    }
                    ps_slot_idx = (ps_slot_idx + 1) & ps_mask;
                }
                if (ps_row == UINT32_MAX) continue;
                double ps_supplycost = ps_supplycost_data[ps_row];

                // Direct order_year lookup: O(1), no hash overhead
                int8_t yr_off = (lorderkey >= 0 && lorderkey <= 60000000)
                                ? order_year_arr[lorderkey] : (int8_t)-1;
                if ((uint8_t)yr_off > 6) continue;  // 0xFF or out of 0..6
                int32_t year = 1992 + yr_off;

                // Get nation
                int32_t nation_idx = suppkey_to_nationkey[lsuppkey];

                // Compute amount
                double amount = l_extprice_data[i] * (1.0 - l_discount_data[i])
                              - ps_supplycost * l_quantity_data[i];

                // Accumulate
                int slot = nation_idx * 7 + (year - 1992);
                local_agg[slot] += amount;
            }
        }
    }

    // ── MERGE aggregates ───────────────────────────────────────────────────────
    double final_agg[N_GROUPS] = {};
    for (int t = 0; t < N_THREADS; t++)
        for (int s = 0; s < N_GROUPS; s++)
            final_agg[s] += thread_agg[t][s];

    // ── SORT & OUTPUT ──────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        struct Row {
            std::string nation;
            int year;
            double sum_profit;
        };

        std::vector<Row> rows;
        rows.reserve(N_GROUPS);
        for (int ni = 0; ni < 25; ni++) {
            for (int yi = 0; yi < 7; yi++) {
                double val = final_agg[ni * 7 + yi];
                if (val != 0.0 || true) { // emit all groups
                    rows.push_back({nation_name[ni], 1992 + yi, val});
                }
            }
        }

        // Sort: nation ASC, o_year DESC
        std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
            if (a.nation != b.nation) return a.nation < b.nation;
            return a.year > b.year;
        });

        // Write CSV
        std::string outpath = results_dir + "/Q9.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        if (!f) { perror("fopen"); exit(1); }
        fprintf(f, "nation,o_year,sum_profit\n");
        for (const auto& r : rows) {
            fprintf(f, "%s,%d,%.2f\n", r.nation.c_str(), r.year, r.sum_profit);
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
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q9(gendb_dir, results_dir);
    return 0;
}
#endif
