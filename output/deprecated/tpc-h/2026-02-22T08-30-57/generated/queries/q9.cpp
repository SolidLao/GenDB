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

namespace {

// ── mmap helper ─────────────────────────────────────────────────────────────
template<typename T>
static const T* mmap_file(const std::string& path, size_t& n_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    size_t sz = st.st_size;
    // No MAP_POPULATE: let parallel phases drive minor page faults concurrently
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
    // No MAP_POPULATE: let parallel phases drive minor page faults concurrently
    void* p = mmap(nullptr, bytes_out, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    madvise(p, bytes_out, MADV_SEQUENTIAL);
    close(fd);
    return reinterpret_cast<const T*>(p);
}

// ── partsupp hash index slot ─────────────────────────────────────────────────
struct PSSlot {
    int64_t  key;      // composite (partkey<<32 | suppkey), INT64_MIN = empty
    uint32_t offset;
    uint32_t count;
};
static_assert(sizeof(PSSlot) == 16, "PSSlot must be 16 bytes");

// ── orders hash index slot ────────────────────────────────────────────────────
struct OrdSlot {
    int32_t  key;      // o_orderkey, INT32_MIN = empty
    uint32_t offset;
    uint32_t count;
};
static_assert(sizeof(OrdSlot) == 12, "OrdSlot must be 12 bytes");

// ── inline hash functions ─────────────────────────────────────────────────────
static inline uint32_t ps_hash(int64_t key, uint32_t mask) {
    uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    h ^= h >> 33;
    return (uint32_t)(h & mask);
}

static inline uint32_t ord_hash(int32_t key, uint32_t mask) {
    return (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & mask;
}

// ── main query function ───────────────────────────────────────────────────────
} // end anonymous namespace

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
    const double*   ps_supplycost_data;
    const int32_t*  o_orderdate_data;

    // Index pointers
    const uint8_t*  ps_index_raw;
    size_t ps_index_bytes;
    const uint8_t*  ord_index_raw;
    size_t ord_index_bytes;

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
        // Partsupp supplycost — random access (by ps_row from hash probe)
        ps_supplycost_data = mmap_file<double>(db + "/partsupp/ps_supplycost.bin", n_ps);
        madvise((void*)ps_supplycost_data, n_ps * sizeof(double), MADV_RANDOM);
        // Orders orderdate — random access (by o_row from orders hash probe); no o_orderkey needed
        o_orderdate_data = mmap_file<int32_t>(db + "/orders/o_orderdate.bin", n_ord);
        madvise((void*)o_orderdate_data, n_ord * sizeof(int32_t), MADV_RANDOM);
        // partsupp index — random access, only ~5% slots hit
        {
            int fd = open((db + "/indexes/partsupp_keys_hash.bin").c_str(), O_RDONLY);
            if (fd < 0) { perror("open partsupp_keys_hash"); exit(1); }
            struct stat st; fstat(fd, &st);
            ps_index_bytes = st.st_size;
            void* p = mmap(nullptr, ps_index_bytes, PROT_READ, MAP_PRIVATE, fd, 0);
            if (p == MAP_FAILED) { perror("mmap partsupp"); exit(1); }
            madvise(p, ps_index_bytes, MADV_RANDOM);
            close(fd);
            ps_index_raw = reinterpret_cast<const uint8_t*>(p);
        }
        // orders_orderkey_hash index — replaces build_order_year, random access during main_scan
        {
            int fd = open((db + "/indexes/orders_orderkey_hash.bin").c_str(), O_RDONLY);
            if (fd < 0) { perror("open orders_orderkey_hash"); exit(1); }
            struct stat st; fstat(fd, &st);
            ord_index_bytes = st.st_size;
            void* p = mmap(nullptr, ord_index_bytes, PROT_READ, MAP_PRIVATE, fd, 0);
            if (p == MAP_FAILED) { perror("mmap orders"); exit(1); }
            madvise(p, ord_index_bytes, MADV_RANDOM);
            close(fd);
            ord_index_raw = reinterpret_cast<const uint8_t*>(p);
        }
    }

    // ── Parse index headers ────────────────────────────────────────────────────
    // partsupp_keys_hash: [uint32_t ht_size][uint32_t num_positions] then ht_size*PSSlot then num_positions*uint32_t
    uint32_t ps_ht_size;
    {
        const uint32_t* hdr = reinterpret_cast<const uint32_t*>(ps_index_raw);
        ps_ht_size  = hdr[0];
    }
    const PSSlot*   ps_slots     = reinterpret_cast<const PSSlot*>  (ps_index_raw + 8);
    const uint32_t* ps_positions = reinterpret_cast<const uint32_t*>(ps_index_raw + 8 + (size_t)ps_ht_size * sizeof(PSSlot));
    uint32_t ps_mask = ps_ht_size - 1;

    // orders_orderkey_hash: [uint32_t ht_size][uint32_t num_positions] then ht_size*OrdSlot then num_positions*uint32_t
    uint32_t ord_ht_size;
    {
        const uint32_t* hdr = reinterpret_cast<const uint32_t*>(ord_index_raw);
        ord_ht_size = hdr[0];
    }
    const OrdSlot*  ord_slots     = reinterpret_cast<const OrdSlot*> (ord_index_raw + 8);
    const uint32_t* ord_positions = reinterpret_cast<const uint32_t*>(ord_index_raw + 8 + (size_t)ord_ht_size * sizeof(OrdSlot));
    uint32_t ord_mask = ord_ht_size - 1;

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

                // Probe orders hash to get row index → o_orderdate → extract year
                uint32_t ord_slot_idx = ord_hash(lorderkey, ord_mask);
                uint32_t o_row = UINT32_MAX;
                for (uint32_t probe = 0; probe < ord_ht_size; probe++) {
                    const OrdSlot& osl = ord_slots[ord_slot_idx];
                    if (osl.key == INT32_MIN) break;
                    if (osl.key == lorderkey) {
                        o_row = ord_positions[osl.offset];
                        break;
                    }
                    ord_slot_idx = (ord_slot_idx + 1) & ord_mask;
                }
                if (o_row == UINT32_MAX) continue;
                int32_t yr_off = gendb::extract_year(o_orderdate_data[o_row]) - 1992;
                if (yr_off < 0 || yr_off > 6) continue;
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
