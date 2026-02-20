// Q9: Product Type Profit Measure
// Strategy: part bitset → parallel lineitem scan → mmap index lookups (partsupp+orders)
#include <algorithm>
#include <cassert>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <omp.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include <iostream>
#include "timing_utils.h"

// ---- Index entry types ----
struct HEntry64 {
    int64_t key;
    uint32_t offset;
    uint32_t count;
};

struct HEntry32 {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

// ---- Howard Hinnant year extraction from epoch days ----
static inline int year_from_epoch_days(int32_t d) {
    int z   = (int)d + 719468;
    int era = (z >= 0 ? z : z - 146096) / 146097;
    int doe = z - era * 146097;
    int yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int y   = yoe + era * 400;
    int doy = doe - (365*yoe + yoe/4 - yoe/100);
    // In shifted calendar (March-based), doy>=306 means Jan/Feb of next civil year
    return (doy >= 306) ? y + 1 : y;
}

// ---- Hash functions matching build_indexes.cpp exactly ----
// For HEntry64 (partsupp composite hash): 64-bit multiply-mix
static inline uint32_t hash64_slot(int64_t key, uint32_t cap) {
    uint64_t x = (uint64_t)key;
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    x ^= (x >> 31);
    return (uint32_t)x & (cap - 1u);
}

// ---- mmap helper ----
static const void* mmap_file(const std::string& path, size_t& out_size, int& out_fd,
                              bool populate = true, bool sequential = false) {
    out_fd = open(path.c_str(), O_RDONLY);
    if (out_fd < 0) { perror(path.c_str()); return nullptr; }
    struct stat st; fstat(out_fd, &st);
    out_size = st.st_size;
    int flags = MAP_PRIVATE | (populate ? MAP_POPULATE : 0);
    void* p = mmap(nullptr, out_size, PROT_READ, flags, out_fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); return nullptr; }
    if (sequential) posix_fadvise(out_fd, 0, out_size, POSIX_FADV_SEQUENTIAL);
    return p;
}

void run_Q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    const int N_THREADS = 64;

    // ===== Phase 1: Load dimension tables (nation + supplier) =====
    char nation_names[25][26];
    memset(nation_names, 0, sizeof(nation_names));

    // suppkey_to_nationkey: indexed by s_suppkey (1-based up to 100000)
    static int32_t suppkey_to_nationkey[100001];
    memset(suppkey_to_nationkey, 0, sizeof(suppkey_to_nationkey));

    {
        GENDB_PHASE("dim_filter");

        // Load nation names (25 × 26 bytes = 650 bytes)
        {
            size_t sz; int fd;
            const char* data = (const char*)mmap_file(gendb_dir + "/nation/n_name.bin", sz, fd);
            memcpy(nation_names, data, sz);
            munmap((void*)data, sz); close(fd);
        }

        // Load supplier s_suppkey and s_nationkey
        {
            size_t sz_sk; int fd_sk;
            const int32_t* s_suppkey = (const int32_t*)mmap_file(
                gendb_dir + "/supplier/s_suppkey.bin", sz_sk, fd_sk, true, true);
            size_t sz_nk; int fd_nk;
            const int32_t* s_nationkey = (const int32_t*)mmap_file(
                gendb_dir + "/supplier/s_nationkey.bin", sz_nk, fd_nk, true, true);
            size_t n_sup = sz_sk / 4;
            for (size_t i = 0; i < n_sup; i++) {
                suppkey_to_nationkey[s_suppkey[i]] = s_nationkey[i];
            }
            munmap((void*)s_suppkey, sz_sk); close(fd_sk);
            munmap((void*)s_nationkey, sz_nk); close(fd_nk);
        }
    }

    // ===== Phase 2: Scan part, build green_parts bitset =====
    // p_partkey values are in [1..2000000]; bitset size = 2000001 bits = 31251 uint64_t words
    static const size_t PART_MAX   = 2000001u;
    static const size_t BITSET_SZ  = (PART_MAX + 63u) / 64u; // 31251 words
    uint64_t* green_parts = new uint64_t[BITSET_SZ]();

    {
        GENDB_PHASE("build_joins");

        size_t sz_name; int fd_name;
        const char* p_name = (const char*)mmap_file(
            gendb_dir + "/part/p_name.bin", sz_name, fd_name, true, true);
        size_t sz_pk; int fd_pk;
        const int32_t* p_partkey = (const int32_t*)mmap_file(
            gendb_dir + "/part/p_partkey.bin", sz_pk, fd_pk, true, true);

        size_t n_parts = sz_name / 56u;

        // Parallel scan: 64 threads each handle ~31K parts (~1.75MB of p_name)
        // p_partkey is a sequential PK [1..2M], so threads touch non-overlapping
        // bitset words — RELAXED atomic OR has near-zero contention
        #pragma omp parallel for num_threads(N_THREADS) schedule(static)
        for (size_t i = 0; i < n_parts; i++) {
            if (strstr(p_name + i * 56u, "green")) {
                uint32_t pk = (uint32_t)p_partkey[i];
                __atomic_fetch_or(&green_parts[pk >> 6u],
                                  1ULL << (pk & 63u),
                                  __ATOMIC_RELAXED);
            }
        }

        // Skip munmap — pages released at process exit (avoids ~6ms PTE teardown)
        close(fd_name);
        close(fd_pk);
    }

    // ===== Phase 2b: Build orderkey→year_idx direct array (replaces orders hash) =====
    // TPC-H SF10: max o_orderkey <= 4 * 15,000,000 = 60,000,000. Skip the max-scan pass.
    // Parallelize memset + fill with 64 threads to reduce from serial 151ms.
    static const int32_t MAX_OK = 60000000;
    int32_t max_ok = MAX_OK;
    int8_t* ok_year = nullptr;  // ok_year[orderkey] = year_idx (0-6), -1 if unused

    {
        GENDB_PHASE("build_ordermap");

        size_t sz_ook; int fd_ook;
        const int32_t* o_orderkey_col = (const int32_t*)mmap_file(
            gendb_dir + "/orders/o_orderkey.bin", sz_ook, fd_ook, false, true);
        size_t sz_od; int fd_od;
        const int32_t* o_orderdate_col = (const int32_t*)mmap_file(
            gendb_dir + "/orders/o_orderdate.bin", sz_od, fd_od, false, true);

        size_t n_orders = sz_ook / 4u;
        const size_t ok_arr_size = (size_t)MAX_OK + 1u;
        ok_year = new int8_t[ok_arr_size];

        // Parallel memset in large chunks to utilize all 64 threads
        #pragma omp parallel for num_threads(N_THREADS) schedule(static)
        for (int64_t j = 0; j < (int64_t)ok_arr_size; j += 65536) {
            size_t len = ((int64_t)ok_arr_size - j < 65536) ? (size_t)((int64_t)ok_arr_size - j) : 65536u;
            memset(ok_year + j, -1, len);
        }

        // Parallel fill: each orderkey is unique (PK), no write conflicts
        #pragma omp parallel for num_threads(N_THREADS) schedule(static)
        for (size_t i = 0; i < n_orders; i++) {
            int yr = year_from_epoch_days(o_orderdate_col[i]) - 1992;
            ok_year[o_orderkey_col[i]] = (int8_t)yr;
        }
        // Skip munmap — pages released at process exit (avoids ~4ms PTE teardown)
        close(fd_ook);
        close(fd_od);
    }

    // ===== Load mmap'd index files and required columns =====

    // --- partsupp composite hash (no MAP_POPULATE — only ~5.75% pages touched randomly) ---
    // Layout: [uint32 num_pos][uint32 positions[num_pos]][uint32 cap][HEntry64 × cap]
    size_t sz_ps_idx; int fd_ps_idx;
    const uint8_t* ps_idx = (const uint8_t*)mmap_file(
        gendb_dir + "/indexes/partsupp_composite_hash.bin", sz_ps_idx, fd_ps_idx, false);
    const uint32_t  ps_num_pos  = *(const uint32_t*)(ps_idx);
    const uint32_t* ps_positions= (const uint32_t*)(ps_idx + 4u);
    const uint32_t  ps_cap      = *(const uint32_t*)(ps_idx + 4u + (size_t)ps_num_pos * 4u);
    const HEntry64* ps_ht       = (const HEntry64*) (ps_idx + 4u + (size_t)ps_num_pos * 4u + 4u);

    // --- ps_supplycost column (no MAP_POPULATE — accessed randomly for qualifying rows only) ---
    size_t sz_psc; int fd_psc;
    const double* ps_supplycost = (const double*)mmap_file(
        gendb_dir + "/partsupp/ps_supplycost.bin", sz_psc, fd_psc, false);

    // --- lineitem columns (no MAP_POPULATE — 64 threads demand-page in parallel) ---
    size_t sz_lpk; int fd_lpk;
    const int32_t* l_partkey = (const int32_t*)mmap_file(
        gendb_dir + "/lineitem/l_partkey.bin", sz_lpk, fd_lpk, false, true);
    size_t n_lineitem = sz_lpk / 4u;

    size_t sz_lsk; int fd_lsk;
    const int32_t* l_suppkey = (const int32_t*)mmap_file(
        gendb_dir + "/lineitem/l_suppkey.bin", sz_lsk, fd_lsk, false, true);

    size_t sz_lok; int fd_lok;
    const int32_t* l_orderkey = (const int32_t*)mmap_file(
        gendb_dir + "/lineitem/l_orderkey.bin", sz_lok, fd_lok, false, true);

    size_t sz_ep; int fd_ep;
    const double* l_extendedprice = (const double*)mmap_file(
        gendb_dir + "/lineitem/l_extendedprice.bin", sz_ep, fd_ep, false, true);

    size_t sz_disc; int fd_disc;
    const double* l_discount = (const double*)mmap_file(
        gendb_dir + "/lineitem/l_discount.bin", sz_disc, fd_disc, false, true);

    size_t sz_qty; int fd_qty;
    const double* l_quantity = (const double*)mmap_file(
        gendb_dir + "/lineitem/l_quantity.bin", sz_qty, fd_qty, false, true);

    // ===== Phase 3: Parallel lineitem scan =====
    // Use long double accumulators to avoid float rounding errors (2-decimal output)
    // Flat layout: [tid][nat][yr] = thread_agg[tid*25*7 + nat*7 + yr]
    long double* all_tagg = new long double[(size_t)N_THREADS * 25 * 7]();

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(N_THREADS)
        {
            int tid = omp_get_thread_num();
            long double* lagg = all_tagg + (size_t)tid * 25 * 7; // lagg[nat*7 + yr]

            #pragma omp for schedule(static, 65536)
            for (size_t i = 0; i < n_lineitem; i++) {
                // Fast bitset check on l_partkey
                int32_t pk = l_partkey[i];
                if (!(green_parts[(uint32_t)pk >> 6u] & (1ULL << ((uint32_t)pk & 63u)))) continue;

                int32_t sk = l_suppkey[i];
                int32_t ok = l_orderkey[i];
                double ep   = l_extendedprice[i];
                double disc = l_discount[i];
                double qty  = l_quantity[i];

                // Lookup partsupp composite hash: key = (pk<<32)|sk
                int64_t ps_key = ((int64_t)pk << 32) | (uint32_t)sk;
                uint32_t h = hash64_slot(ps_key, ps_cap);
                while (ps_ht[h].key != INT64_MIN && ps_ht[h].key != ps_key)
                    h = (h + 1u) & (ps_cap - 1u);
                if (__builtin_expect(ps_ht[h].key != ps_key, 0)) continue;
                double supplycost = ps_supplycost[ps_positions[ps_ht[h].offset]];

                // Direct array lookup: orderkey → year_idx (replaces 456MB hash probe)
                int yr_idx = (ok >= 0 && ok <= max_ok) ? (int)(uint8_t)ok_year[ok] : 255u;
                if (__builtin_expect((unsigned)yr_idx >= 7u, 0)) continue;

                int32_t nat = suppkey_to_nationkey[sk];
                // Compute amount in double to match SQL double arithmetic,
                // then widen to long double for accumulation to reduce summation error
                double amount = ep * (1.0 - disc) - supplycost * qty;
                lagg[nat * 7 + yr_idx] += (long double)amount;
            }
        }

        // Merge thread-local into global (sequential, tiny: 64*175 = 11200 adds)
    }

    // ===== Phase 4: Output =====
    {
        GENDB_PHASE("output");

        // Merge all thread accumulators into long double global
        long double global_agg[25][7] = {};
        for (int t = 0; t < N_THREADS; t++) {
            const long double* lagg = all_tagg + (size_t)t * 25 * 7;
            for (int n = 0; n < 25; n++)
                for (int y = 0; y < 7; y++)
                    global_agg[n][y] += lagg[n * 7 + y];
        }

        struct Result {
            char nation[27];
            int  year;
            long double sum_profit;
        };
        std::vector<Result> results;
        results.reserve(175);

        for (int n = 0; n < 25; n++) {
            for (int y = 0; y < 7; y++) {
                if (global_agg[n][y] == 0.0L) continue; // skip empty groups
                Result r;
                // nation_names[n] is null-padded 26-byte fixed string
                memcpy(r.nation, nation_names[n], 26);
                r.nation[26] = '\0';
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

        // Write CSV — %.2Lf for long double
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
    delete[] ok_year;

    // Skip munmap for all large file-backed regions (~2.5GB total).
    // Page table teardown for ~540K demand-faulted PTEs costs ~80ms.
    // The kernel reclaims all mappings automatically at process exit.
    close(fd_ps_idx);
    close(fd_psc);
    close(fd_lpk);
    close(fd_lsk);
    close(fd_lok);
    close(fd_ep);
    close(fd_disc);
    close(fd_qty);
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
