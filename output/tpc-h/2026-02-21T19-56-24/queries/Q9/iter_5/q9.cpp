// Q9: Product Type Profit Measure
// Strategy: part bitset (55B records) → parallel lineitem scan → direct index lookups
//           direct-array supplier lookup → flat [25×7] long-double aggregation
#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <omp.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include "timing_utils.h"

// ---- Inline partsupp hash table (built from green parts only) ----
// Stores supplycost directly to avoid an extra indirection.
// Key: (partkey << 32 | suppkey), INT64_MIN = empty slot
struct PSEntry {
    int64_t key;   // composite key; INT64_MIN = empty
    double  cost;  // ps_supplycost inline
};
static const uint32_t PS_HT_CAP = 1u << 20; // 1048576 slots, 16MB — fits in 44MB L3

// orders_orderkey_hash: {int32_t key; uint32_t row_idx;} — 8 bytes
struct SvEntry {
    int32_t  key;      // INT32_MIN = empty
    uint32_t row_idx;  // direct row index into target column
};

// ---- Hash functions matching build_indexes.cpp exactly ----
// 64-bit splitmix64/murmur mix for composite (partsupp) 64-bit key
static inline uint32_t hash64_slot(int64_t key, uint32_t cap) {
    uint64_t x = (uint64_t)key;
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    x ^= (x >> 31);
    return (uint32_t)x & (cap - 1u);
}

// 32-bit Knuth multiplicative hash for int32 keys (matches build_indexes.cpp msh32)
static inline uint32_t hash32_slot(int32_t key, uint32_t cap) {
    return (uint32_t)((uint64_t)(uint32_t)key * 2654435761ULL >> 32) & (cap - 1u);
}

// ---- Howard Hinnant year extraction from epoch days ----
static inline int year_from_epoch_days(int32_t d) {
    int z   = (int)d + 719468;
    int era = (z >= 0 ? z : z - 146096) / 146097;
    int doe = z - era * 146097;
    int yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int y   = yoe + era * 400;
    int doy = doe - (365*yoe + yoe/4 - yoe/100);
    return (doy >= 306) ? y + 1 : y;
}

// ---- mmap helper ----
static const void* mmap_open(const std::string& path, size_t& out_size, int& out_fd,
                               bool sequential = false, bool random_access = false) {
    out_fd = open(path.c_str(), O_RDONLY);
    if (out_fd < 0) { perror(path.c_str()); return nullptr; }
    struct stat st; fstat(out_fd, &st);
    out_size = (size_t)st.st_size;
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, out_fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); return nullptr; }
    madvise(p, out_size, MADV_WILLNEED);
    if (sequential)    posix_fadvise(out_fd, 0, out_size, POSIX_FADV_SEQUENTIAL);
    if (random_access) madvise(p, out_size, MADV_RANDOM);
    return p;
}

void run_Q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ===== Phase 0: I/O prefetch — issue fadvise WILLNEED for all required files =====
    {
        GENDB_PHASE("data_loading");
        // Open and advise sequential prefetch for all files consumed later.
        // This triggers kernel readahead in parallel with dimension-table work,
        // so by the time index_load/main_scan run the pages are warm.
        auto prefetch_file = [](const std::string& path) {
            int fd = open(path.c_str(), O_RDONLY);
            if (fd >= 0) {
                posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
                posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
                close(fd);
            }
        };
        // part columns (filter phase — missing from prior prefetch list)
        prefetch_file(gendb_dir + "/part/p_name.bin");
        prefetch_file(gendb_dir + "/part/p_partkey.bin");
        // partsupp columns (built into inline hash table)
        prefetch_file(gendb_dir + "/partsupp/ps_partkey.bin");
        prefetch_file(gendb_dir + "/partsupp/ps_suppkey.bin");
        prefetch_file(gendb_dir + "/partsupp/ps_supplycost.bin");
        // orders columns (built into direct-array)
        prefetch_file(gendb_dir + "/orders/o_orderkey.bin");
        prefetch_file(gendb_dir + "/orders/o_orderdate.bin");
        // lineitem columns (main scan)
        prefetch_file(gendb_dir + "/lineitem/l_partkey.bin");
        prefetch_file(gendb_dir + "/lineitem/l_suppkey.bin");
        prefetch_file(gendb_dir + "/lineitem/l_orderkey.bin");
        prefetch_file(gendb_dir + "/lineitem/l_extendedprice.bin");
        prefetch_file(gendb_dir + "/lineitem/l_discount.bin");
        prefetch_file(gendb_dir + "/lineitem/l_quantity.bin");
    }

    // ===== Phase 1: Load dimension tables =====
    // n_name_lookup.txt: 25 lines, alphabetically sorted nation names (code 0=ALGERIA, ...)
    char nation_names[25][32];  // string storage per alphabetical code
    memset(nation_names, 0, sizeof(nation_names));
    {
        FILE* fp = fopen((gendb_dir + "/nation/n_name_lookup.txt").c_str(), "r");
        if (!fp) { perror("n_name_lookup.txt"); return; }
        for (int i = 0; i < 25; i++) {
            if (!fgets(nation_names[i], 32, fp)) break;
            int len = (int)strlen(nation_names[i]);
            while (len > 0 && (nation_names[i][len-1] == '\n' || nation_names[i][len-1] == '\r'))
                nation_names[i][--len] = '\0';
        }
        fclose(fp);
    }

    // n_name.bin: 25 uint8_t bytes — n_name_bin[nationkey] = alphabetical code
    uint8_t nation_code[25] = {};
    {
        size_t sz; int fd;
        const uint8_t* data = (const uint8_t*)mmap_open(
            gendb_dir + "/nation/n_name.bin", sz, fd);
        memcpy(nation_code, data, sz < 25 ? sz : 25);
        munmap((void*)data, sz); close(fd);
    }

    // Precompute: for each nationkey (0-24) → nation name string
    // nation_name_for_nk[nk] = nation_names[nation_code[nk]]
    // We'll index agg by nationkey directly, decode at output time.

    // suppkey_to_nationkey: direct array indexed by s_suppkey (1-based, ≤100000)
    static int32_t suppkey_to_nationkey[100001];
    memset(suppkey_to_nationkey, 0, sizeof(suppkey_to_nationkey));

    {
        GENDB_PHASE("dim_filter");

        size_t sz_sk; int fd_sk;
        const int32_t* s_suppkey = (const int32_t*)mmap_open(
            gendb_dir + "/supplier/s_suppkey.bin", sz_sk, fd_sk, true);
        size_t sz_nk; int fd_nk;
        const int32_t* s_nationkey = (const int32_t*)mmap_open(
            gendb_dir + "/supplier/s_nationkey.bin", sz_nk, fd_nk, true);

        size_t n_sup = sz_sk / 4u;
        for (size_t i = 0; i < n_sup; i++)
            suppkey_to_nationkey[s_suppkey[i]] = s_nationkey[i];

        munmap((void*)s_suppkey,   sz_sk); close(fd_sk);
        munmap((void*)s_nationkey, sz_nk); close(fd_nk);
    }

    // ===== Phase 2: Build all join structures + mmap lineitem columns =====
    // Enable nested OpenMP so inner parallel fors work inside omp sections
    omp_set_nested(1);
    omp_set_max_active_levels(3);

    static const size_t PART_MAX  = 2000001u;
    static const size_t BITSET_SZ = (PART_MAX + 63u) / 64u;
    uint64_t* green_parts = new uint64_t[BITSET_SZ]();

    PSEntry* ps_ht_inline = new PSEntry[PS_HT_CAP];
    for (uint32_t i = 0; i < PS_HT_CAP; i++) ps_ht_inline[i].key = INT64_MIN;

    static const int32_t ORDER_MAXKEY = 60000000;
    uint8_t* orderkey_to_year = new uint8_t[(size_t)(ORDER_MAXKEY + 1)];
    memset(orderkey_to_year, 255u, (size_t)(ORDER_MAXKEY + 1));

    size_t sz_lpk = 0; int fd_lpk = -1;
    const int32_t* l_partkey = nullptr;
    size_t n_lineitem = 0;
    size_t sz_lsk = 0; int fd_lsk = -1;
    const int32_t* l_suppkey = nullptr;
    size_t sz_lok = 0; int fd_lok = -1;
    const int32_t* l_orderkey = nullptr;
    size_t sz_ep = 0; int fd_ep = -1;
    const double* l_extendedprice = nullptr;
    size_t sz_disc = 0; int fd_disc = -1;
    const double* l_discount = nullptr;
    size_t sz_qty = 0; int fd_qty = -1;
    const double* l_quantity = nullptr;

    {
        GENDB_PHASE("build_joins");

        // Sub-phase A: green_parts bitset — must complete before partsupp filter uses it
        {
            size_t sz_name; int fd_name;
            const char* p_name = (const char*)mmap_open(
                gendb_dir + "/part/p_name.bin", sz_name, fd_name, true);
            size_t sz_pk; int fd_pk;
            const int32_t* p_partkey = (const int32_t*)mmap_open(
                gendb_dir + "/part/p_partkey.bin", sz_pk, fd_pk, true);

            static const size_t NAME_STRIDE = 55u;
            size_t n_parts = sz_name / NAME_STRIDE;

            #pragma omp parallel for schedule(static) num_threads(64)
            for (size_t i = 0; i < n_parts; i++) {
                const char* name = p_name + i * NAME_STRIDE;
                if (memmem(name, NAME_STRIDE, "green", 5)) {
                    int32_t pk = p_partkey[i];
                    uint32_t word_idx = (uint32_t)pk >> 6u;
                    uint64_t bit_mask = 1ULL << ((uint32_t)pk & 63u);
                    __atomic_fetch_or(&green_parts[word_idx], bit_mask, __ATOMIC_RELAXED);
                }
            }

            munmap((void*)p_name,    sz_name); close(fd_name);
            munmap((void*)p_partkey, sz_pk);   close(fd_pk);
        }

        // Sub-phase B: Concurrent partsupp build (32 nested threads) +
        //              orders build (32 nested threads) + lineitem mmap
        #pragma omp parallel sections num_threads(3)
        {
            // B1: Build inline partsupp hash — lock-free parallel insertion via CAS
            #pragma omp section
            {
                size_t sz_ppk = 0, sz_psk = 0, sz_psc2 = 0;
                int fd_ppk = -1, fd_psk = -1, fd_psc2 = -1;
                const int32_t* ps_partkey_d = (const int32_t*)mmap_open(
                    gendb_dir + "/partsupp/ps_partkey.bin", sz_ppk, fd_ppk, true);
                const int32_t* ps_suppkey_d = (const int32_t*)mmap_open(
                    gendb_dir + "/partsupp/ps_suppkey.bin", sz_psk, fd_psk, true);
                const double*  ps_cost_d    = (const double*)mmap_open(
                    gendb_dir + "/partsupp/ps_supplycost.bin", sz_psc2, fd_psc2, true);
                size_t n_ps = sz_ppk / 4u;

                // 32 threads scan 8M rows; only ~440K pass filter → sparse CAS contention
                #pragma omp parallel for schedule(static) num_threads(32)
                for (size_t i = 0; i < n_ps; i++) {
                    int32_t ppk = ps_partkey_d[i];
                    if (!(green_parts[(uint32_t)ppk >> 6u] & (1ULL << ((uint32_t)ppk & 63u))))
                        continue;
                    int32_t psk  = ps_suppkey_d[i];
                    int64_t k    = ((int64_t)ppk << 32) | (uint32_t)psk;
                    double  cost = ps_cost_d[i];
                    uint32_t slot = hash64_slot(k, PS_HT_CAP);
                    int64_t expected = INT64_MIN;
                    for (;;) {
                        if (__atomic_compare_exchange_n(
                                &ps_ht_inline[slot].key, &expected, k,
                                /*weak=*/false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
                            ps_ht_inline[slot].cost = cost; // safe: we own slot after CAS
                            break;
                        }
                        if (expected == k) break; // duplicate key (shouldn't occur)
                        expected = INT64_MIN;
                        slot = (slot + 1u) & (PS_HT_CAP - 1u);
                    }
                }

                munmap((void*)ps_partkey_d, sz_ppk); close(fd_ppk);
                munmap((void*)ps_suppkey_d, sz_psk); close(fd_psk);
                munmap((void*)ps_cost_d,    sz_psc2); close(fd_psc2);
            }

            // B2: Build orders year array (32 nested threads, no write conflicts on unique PKs)
            #pragma omp section
            {
                size_t sz_ok = 0, sz_od = 0; int fd_ok = -1, fd_od = -1;
                const int32_t* ok_data = (const int32_t*)mmap_open(
                    gendb_dir + "/orders/o_orderkey.bin", sz_ok, fd_ok, true);
                const int32_t* od_data = (const int32_t*)mmap_open(
                    gendb_dir + "/orders/o_orderdate.bin", sz_od, fd_od, true);
                size_t n_ord = sz_ok / 4u;

                #pragma omp parallel for schedule(static) num_threads(32)
                for (size_t i = 0; i < n_ord; i++) {
                    int32_t okey = ok_data[i];
                    if ((uint32_t)okey <= (uint32_t)ORDER_MAXKEY) {
                        int yr = year_from_epoch_days(od_data[i]) - 1992;
                        if ((unsigned)yr < 7u)
                            orderkey_to_year[okey] = (uint8_t)yr;
                    }
                }
                munmap((void*)ok_data, sz_ok); close(fd_ok);
                munmap((void*)od_data, sz_od); close(fd_od);
            }

            // B3: Mmap lineitem columns (MAP_POPULATE: pages already warm from WILLNEED prefetch)
            #pragma omp section
            {
                const void* p;
                p = mmap_open(gendb_dir + "/lineitem/l_partkey.bin",       sz_lpk,  fd_lpk, true);
                l_partkey       = (const int32_t*)p; n_lineitem = sz_lpk / 4u;
                p = mmap_open(gendb_dir + "/lineitem/l_suppkey.bin",       sz_lsk,  fd_lsk, true);
                l_suppkey       = (const int32_t*)p;
                p = mmap_open(gendb_dir + "/lineitem/l_orderkey.bin",      sz_lok,  fd_lok, true);
                l_orderkey      = (const int32_t*)p;
                p = mmap_open(gendb_dir + "/lineitem/l_extendedprice.bin", sz_ep,   fd_ep,  true);
                l_extendedprice = (const double*)p;
                p = mmap_open(gendb_dir + "/lineitem/l_discount.bin",      sz_disc, fd_disc, true);
                l_discount      = (const double*)p;
                p = mmap_open(gendb_dir + "/lineitem/l_quantity.bin",      sz_qty,  fd_qty,  true);
                l_quantity      = (const double*)p;
            }
        }
    }

    // index_load work merged into build_joins above
    { GENDB_PHASE("index_load"); }

    // ===== Phase 3: Parallel lineitem scan + fused aggregation =====
    // Thread-local aggregation: [tid * 25 * 7 + nationkey * 7 + year_offset]
    const int N_THREADS = 64;
    long double* all_tagg = new long double[(size_t)N_THREADS * 25 * 7]();

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(N_THREADS)
        {
            int tid = omp_get_thread_num();
            long double* lagg = all_tagg + (size_t)tid * 25 * 7;

            #pragma omp for schedule(static, 65536)
            for (size_t i = 0; i < n_lineitem; i++) {
                // Bitset filter on l_partkey
                int32_t pk = l_partkey[i];
                if (!(green_parts[(uint32_t)pk >> 6u] & (1ULL << ((uint32_t)pk & 63u)))) continue;

                int32_t sk = l_suppkey[i];
                int32_t ok = l_orderkey[i];
                double ep   = l_extendedprice[i];
                double disc = l_discount[i];
                double qty  = l_quantity[i];

                // Probe inline partsupp hash (16MB → L3-resident, fast cache hits)
                int64_t ps_key = ((int64_t)pk << 32) | (uint32_t)sk;
                uint32_t ph = hash64_slot(ps_key, PS_HT_CAP);
                while (ps_ht_inline[ph].key != ps_key) {
                    if (__builtin_expect(ps_ht_inline[ph].key == INT64_MIN, 0)) goto ps_miss;
                    ph = (ph + 1u) & (PS_HT_CAP - 1u);
                }
                {
                    double supplycost = ps_ht_inline[ph].cost;

                    // Direct-array orders lookup: O(1), no hash probing
                    if (__builtin_expect((uint32_t)ok > (uint32_t)ORDER_MAXKEY, 0)) goto ps_miss;
                    uint8_t yr_u = orderkey_to_year[ok];
                    if (__builtin_expect(yr_u == 255u, 0)) goto ps_miss;
                    int yr_idx = (int)yr_u;

                    // Direct suppkey → nationkey lookup
                    int32_t nat = suppkey_to_nationkey[sk];

                    // Compute profit amount, accumulate with long double precision
                    double amount = ep * (1.0 - disc) - supplycost * qty;
                    lagg[(size_t)nat * 7 + (size_t)yr_idx] += (long double)amount;
                }
                ps_miss:;
            }
        }
    }

    // ===== Phase 4: Merge + sort + output =====
    {
        GENDB_PHASE("output");

        // Merge thread-local accumulators into global
        long double global_agg[25][7] = {};
        for (int t = 0; t < N_THREADS; t++) {
            const long double* lagg = all_tagg + (size_t)t * 25 * 7;
            for (int n = 0; n < 25; n++)
                for (int y = 0; y < 7; y++)
                    global_agg[n][y] += lagg[(size_t)n * 7 + y];
        }

        struct Result {
            char        nation[32];
            int         year;
            long double sum_profit;
        };
        std::vector<Result> results;
        results.reserve(175);

        for (int n = 0; n < 25; n++) {
            for (int y = 0; y < 7; y++) {
                if (global_agg[n][y] == 0.0L) continue;
                Result r;
                // Decode nationkey n → alphabetical code → nation name string
                uint8_t code = nation_code[n];
                strncpy(r.nation, nation_names[code], 31);
                r.nation[31] = '\0';
                r.year       = 1992 + y;
                r.sum_profit = global_agg[n][y];
                results.push_back(r);
            }
        }

        // Sort: nation ASC, o_year DESC
        std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
            int c = strcmp(a.nation, b.nation);
            if (c != 0) return c < 0;
            return a.year > b.year;
        });

        // Write CSV
        std::string out_path = results_dir + "/Q9.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }
        fprintf(f, "nation,o_year,sum_profit\n");
        for (const auto& r : results)
            fprintf(f, "%s,%d,%.2Lf\n", r.nation, r.year, r.sum_profit);
        fclose(f);
    }

    // ===== Cleanup =====
    delete[] green_parts;
    delete[] all_tagg;
    delete[] orderkey_to_year;
    delete[] ps_ht_inline;

    munmap((void*)l_partkey,       sz_lpk);   close(fd_lpk);
    munmap((void*)l_suppkey,       sz_lsk);   close(fd_lsk);
    munmap((void*)l_orderkey,      sz_lok);   close(fd_lok);
    munmap((void*)l_extendedprice, sz_ep);    close(fd_ep);
    munmap((void*)l_discount,      sz_disc);  close(fd_disc);
    munmap((void*)l_quantity,      sz_qty);   close(fd_qty);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q9(gendb_dir, results_dir);
    return 0;
}
#endif
