// Q9: Product Type Profit Measure
// Strategy:
//   1. Load nation (25 rows) and supplier (100K) as direct lookup arrays
//   2. Scan part (2M) with strstr("green") -> build 250KB bitset of qualifying partkeys
//   3. Use pre-built mmap indexes for partsupp and orders lookups (zero build cost)
//   4. Parallel morsel-driven scan of lineitem (60M rows) with fused joins + aggregation
//   5. Thread-local double[25][8] (1400B/thread) aggregation arrays, merged after parallel phase
//   6. Sort 175-element result set and write CSV

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <omp.h>
#include <cstring>
#include <cstdint>
#include <climits>
#include <string>
#include <vector>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <iomanip>
#include "date_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Hash index slot layouts (must match binary format exactly)
// ---------------------------------------------------------------------------

// partsupp_keys_hash: 16 bytes/slot
struct PSSlot {
    int64_t  key;     // (ps_partkey << 32) | (uint32_t)ps_suppkey; INT64_MIN = empty
    uint32_t offset;  // index into row_idx array
    uint32_t count;   // always 1 for PK
};

// orders_orderkey_hash: 12 bytes/slot
struct OrdSlot {
    int32_t  key;     // o_orderkey; INT32_MIN = empty
    uint32_t offset;  // index into row_idx array
    uint32_t count;   // always 1 for PK
};

// ---------------------------------------------------------------------------
// Hash functions (must match index builder exactly)
// ---------------------------------------------------------------------------

static inline uint32_t ps_hash(int64_t key, uint32_t mask) {
    uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    h ^= h >> 33;
    return (uint32_t)(h & mask);
}

static inline uint32_t ord_hash(int32_t key, uint32_t mask) {
    return (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & mask;
}

// ---------------------------------------------------------------------------
// mmap helper
// ---------------------------------------------------------------------------

struct MMapRegion {
    void*  ptr  = MAP_FAILED;
    size_t size = 0;

    template<typename T> const T* as() const { return reinterpret_cast<const T*>(ptr); }
    size_t count(size_t elem_size) const { return size / elem_size; }
};

static MMapRegion do_mmap(const std::string& path, bool sequential = false) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    if (fstat(fd, &st) != 0) { perror("fstat"); exit(1); }
    if (st.st_size == 0) { close(fd); return {}; }
    if (sequential) posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return {p, (size_t)st.st_size};
}

// ---------------------------------------------------------------------------
// Main query function
// ---------------------------------------------------------------------------

void run_Q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string S  = gendb_dir;
    const std::string IX = gendb_dir + "/indexes";

    // -----------------------------------------------------------------------
    // Phase 0: mmap all files + async prefetch with MADV_WILLNEED
    // Tiny tables (nation/supplier) are loaded synchronously first.
    // -----------------------------------------------------------------------
    MMapRegion mi_n_nationkey, mi_n_name;
    MMapRegion mi_s_suppkey, mi_s_nationkey;
    MMapRegion mi_p_partkey, mi_p_name;
    MMapRegion mi_ps_hash, mi_ord_hash;
    MMapRegion mi_ps_supplycost, mi_o_orderdate;
    MMapRegion mi_l_partkey, mi_l_suppkey, mi_l_orderkey;
    MMapRegion mi_l_extprice, mi_l_discount, mi_l_quantity;

    {
        GENDB_PHASE("data_loading");

        // Tiny dimension tables — load immediately
        mi_n_nationkey = do_mmap(S + "/nation/n_nationkey.bin");
        mi_n_name      = do_mmap(S + "/nation/n_name.bin");
        mi_s_suppkey   = do_mmap(S + "/supplier/s_suppkey.bin");
        mi_s_nationkey = do_mmap(S + "/supplier/s_nationkey.bin");

        // Large files — mmap (no MAP_POPULATE, let MADV_WILLNEED trigger async I/O)
        mi_p_partkey      = do_mmap(S + "/part/p_partkey.bin",        true);
        mi_p_name         = do_mmap(S + "/part/p_name.bin",           true);
        mi_ps_hash        = do_mmap(IX + "/partsupp_keys_hash.bin");
        mi_ord_hash       = do_mmap(IX + "/orders_orderkey_hash.bin");
        mi_ps_supplycost  = do_mmap(S + "/partsupp/ps_supplycost.bin");
        mi_o_orderdate    = do_mmap(S + "/orders/o_orderdate.bin");
        mi_l_partkey      = do_mmap(S + "/lineitem/l_partkey.bin",    true);
        mi_l_suppkey      = do_mmap(S + "/lineitem/l_suppkey.bin",    true);
        mi_l_orderkey     = do_mmap(S + "/lineitem/l_orderkey.bin",   true);
        mi_l_extprice     = do_mmap(S + "/lineitem/l_extendedprice.bin", true);
        mi_l_discount     = do_mmap(S + "/lineitem/l_discount.bin",   true);
        mi_l_quantity     = do_mmap(S + "/lineitem/l_quantity.bin",   true);

        // Issue parallel async prefetch for all large regions
        MMapRegion* large_files[] = {
            &mi_p_partkey, &mi_p_name,
            &mi_ps_hash, &mi_ord_hash,
            &mi_ps_supplycost, &mi_o_orderdate,
            &mi_l_partkey, &mi_l_suppkey, &mi_l_orderkey,
            &mi_l_extprice, &mi_l_discount, &mi_l_quantity
        };
        constexpr int N_LARGE = 12;

        #pragma omp parallel for schedule(static, 1) num_threads(N_LARGE)
        for (int f = 0; f < N_LARGE; f++) {
            madvise(large_files[f]->ptr, large_files[f]->size, MADV_WILLNEED);
        }

        // Hint sequential access for lineitem columns
        MMapRegion* lineitem_files[] = {
            &mi_l_partkey, &mi_l_suppkey, &mi_l_orderkey,
            &mi_l_extprice, &mi_l_discount, &mi_l_quantity
        };
        for (auto* r : lineitem_files) {
            madvise(r->ptr, r->size, MADV_SEQUENTIAL);
        }
    }

    // -----------------------------------------------------------------------
    // Phase 1: Build dimension lookup arrays
    // -----------------------------------------------------------------------

    // nation_names[nationkey] = null-terminated name string (nationkey 0..24)
    static char nation_names[25][26];
    {
        GENDB_PHASE("dim_filter");
        size_t n_nation = mi_n_nationkey.count(sizeof(int32_t));
        const int32_t* nk_col  = mi_n_nationkey.as<int32_t>();
        const char*    nm_col  = mi_n_name.as<char>();

        memset(nation_names, 0, sizeof(nation_names));
        for (size_t i = 0; i < n_nation; i++) {
            int32_t nk = nk_col[i];
            const char* src = nm_col + i * 26;
            memcpy(nation_names[nk], src, 26);
            nation_names[nk][25] = '\0';
        }

        // suppkey_to_nationkey[suppkey] = nationkey (suppkeys 1..100000)
        // Build inline in supplier section below
    }

    // Supplier: suppkey -> nationkey (direct array, suppkeys 1..100000)
    static int32_t suppkey_to_nk[100001];
    {
        size_t n_sup = mi_s_suppkey.count(sizeof(int32_t));
        const int32_t* sk_col  = mi_s_suppkey.as<int32_t>();
        const int32_t* snk_col = mi_s_nationkey.as<int32_t>();
        for (size_t i = 0; i < n_sup; i++) {
            suppkey_to_nk[sk_col[i]] = snk_col[i];
        }
    }

    // -----------------------------------------------------------------------
    // Part filter: build green bitset (partkeys 0..2000000, 250KB bitset)
    // -----------------------------------------------------------------------
    static uint64_t green_bitset[31251]; // ceil(2000001 / 64) = 31251 words
    memset(green_bitset, 0, sizeof(green_bitset));
    {
        GENDB_PHASE("part_filter");
        size_t n_part = mi_p_partkey.count(sizeof(int32_t));
        const int32_t* p_pk  = mi_p_partkey.as<int32_t>();
        const char*    p_nm  = mi_p_name.as<char>();

        for (size_t i = 0; i < n_part; i++) {
            const char* name = p_nm + i * 56;
            if (__builtin_expect(strstr(name, "green") != nullptr, 0)) {
                int32_t pk = p_pk[i];
                green_bitset[pk >> 6] |= (1ULL << (pk & 63));
            }
        }
    }

    // -----------------------------------------------------------------------
    // Parse pre-built hash index headers
    // -----------------------------------------------------------------------

    // partsupp_keys_hash
    const uint8_t* ps_raw    = mi_ps_hash.as<uint8_t>();
    uint32_t ps_ht_size      = *reinterpret_cast<const uint32_t*>(ps_raw);
    // uint32_t ps_num_pos   = *reinterpret_cast<const uint32_t*>(ps_raw + 4);  // unused
    const PSSlot*  ps_slots  = reinterpret_cast<const PSSlot*>(ps_raw + 8);
    const uint32_t* ps_ridxs = reinterpret_cast<const uint32_t*>(
                                    ps_raw + 8 + (size_t)ps_ht_size * sizeof(PSSlot));
    const uint32_t ps_mask   = ps_ht_size - 1;

    // orders_orderkey_hash
    const uint8_t* ord_raw    = mi_ord_hash.as<uint8_t>();
    uint32_t ord_ht_size      = *reinterpret_cast<const uint32_t*>(ord_raw);
    // uint32_t ord_num_pos   = *reinterpret_cast<const uint32_t*>(ord_raw + 4); // unused
    const OrdSlot*  ord_slots = reinterpret_cast<const OrdSlot*>(ord_raw + 8);
    const uint32_t* ord_ridxs = reinterpret_cast<const uint32_t*>(
                                    ord_raw + 8 + (size_t)ord_ht_size * sizeof(OrdSlot));
    const uint32_t ord_mask   = ord_ht_size - 1;

    // Payload columns (random access after filter)
    const double*  ps_supplycost = mi_ps_supplycost.as<double>();
    const int32_t* o_orderdate   = mi_o_orderdate.as<int32_t>();

    // Lineitem columns (sequential scan)
    size_t n_lineitem            = mi_l_partkey.count(sizeof(int32_t));
    const int32_t* l_partkey     = mi_l_partkey.as<int32_t>();
    const int32_t* l_suppkey     = mi_l_suppkey.as<int32_t>();
    const int32_t* l_orderkey    = mi_l_orderkey.as<int32_t>();
    const double*  l_extprice    = mi_l_extprice.as<double>();
    const double*  l_discount    = mi_l_discount.as<double>();
    const double*  l_quantity    = mi_l_quantity.as<double>();

    // -----------------------------------------------------------------------
    // Phase 3: Parallel fused lineitem scan + join + aggregation
    // agg[nation_idx][year_idx] where year_idx = year - 1992 (0..6)
    // Using 8 slots per nation for alignment (7 years used, 8th unused)
    // -----------------------------------------------------------------------
    constexpr int N_NATIONS = 25;
    constexpr int N_YEARS   = 8;  // 1992-1998 = indices 0-6; slot 7 unused

    const int nthreads = omp_get_max_threads();
    // Two arrays per thread: sum + Kahan compensation (to avoid FP precision loss over 60M rows)
    std::vector<double> thread_aggs((size_t)nthreads * N_NATIONS * N_YEARS, 0.0);
    std::vector<double> thread_comp((size_t)nthreads * N_NATIONS * N_YEARS, 0.0);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            double* agg  = thread_aggs.data() + (size_t)tid * N_NATIONS * N_YEARS;
            double* comp = thread_comp.data()  + (size_t)tid * N_NATIONS * N_YEARS;

            #pragma omp for schedule(dynamic, 65536) nowait
            for (int64_t i = 0; i < (int64_t)n_lineitem; i++) {
                // ---- Filter: check if l_partkey is a "green" part ----
                int32_t pk = l_partkey[i];
                if (!(green_bitset[(uint32_t)pk >> 6] & (1ULL << (pk & 63)))) continue;

                int32_t sk = l_suppkey[i];
                int32_t ok = l_orderkey[i];

                // ---- Join: partsupp hash lookup (ps_partkey, ps_suppkey) -> ps_supplycost ----
                int64_t ps_key = ((int64_t)pk << 32) | (uint32_t)sk;
                uint32_t ps_slot = ps_hash(ps_key, ps_mask);
                uint32_t ps_ridx = UINT32_MAX;
                for (uint32_t probe = 0; probe < 128; probe++) {
                    const PSSlot& s = ps_slots[ps_slot];
                    if (__builtin_expect(s.key == INT64_MIN, 0)) break; // empty slot
                    if (s.key == ps_key) {
                        ps_ridx = ps_ridxs[s.offset];
                        break;
                    }
                    ps_slot = (ps_slot + 1) & ps_mask;
                }
                if (__builtin_expect(ps_ridx == UINT32_MAX, 0)) continue;

                // ---- Join: orders hash lookup (o_orderkey) -> o_orderdate ----
                uint32_t ord_slot = ord_hash(ok, ord_mask);
                uint32_t ord_ridx = UINT32_MAX;
                for (uint32_t probe = 0; probe < 128; probe++) {
                    const OrdSlot& s = ord_slots[ord_slot];
                    if (__builtin_expect(s.key == INT32_MIN, 0)) break; // empty slot
                    if (s.key == ok) {
                        ord_ridx = ord_ridxs[s.offset];
                        break;
                    }
                    ord_slot = (ord_slot + 1) & ord_mask;
                }
                if (__builtin_expect(ord_ridx == UINT32_MAX, 0)) continue;

                // ---- Lookup: suppkey -> nationkey (direct array O(1)) ----
                int32_t nk = suppkey_to_nk[sk];

                // ---- Lookup: year from orderdate ----
                int32_t odate = o_orderdate[ord_ridx];
                int year_idx  = (int)gendb::YEAR_TABLE[odate] - 1992;
                if ((unsigned)year_idx > 6u) continue;

                // ---- Compute amount and Kahan-accumulate into thread-local agg ----
                double amount = l_extprice[i] * (1.0 - l_discount[i])
                              - ps_supplycost[ps_ridx] * l_quantity[i];
                int slot = nk * N_YEARS + year_idx;
                double y = amount - comp[slot];
                double t = agg[slot] + y;
                comp[slot] = (t - agg[slot]) - y;
                agg[slot]  = t;
            }
        } // end omp parallel
    }

    // -----------------------------------------------------------------------
    // Merge thread-local aggregations into thread 0's array
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("aggregation_merge");
        // First fold compensation into each thread's sum
        for (int t = 0; t < nthreads; t++) {
            double* s = thread_aggs.data() + (size_t)t * N_NATIONS * N_YEARS;
            double* c = thread_comp.data() + (size_t)t * N_NATIONS * N_YEARS;
            for (int j = 0; j < N_NATIONS * N_YEARS; j++) {
                s[j] += c[j];
            }
        }
        // Then merge thread sums into thread 0
        double* dst = thread_aggs.data();
        for (int t = 1; t < nthreads; t++) {
            const double* src = thread_aggs.data() + (size_t)t * N_NATIONS * N_YEARS;
            for (int j = 0; j < N_NATIONS * N_YEARS; j++) {
                dst[j] += src[j];
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 4: Sort and write CSV output
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        struct Result {
            const char* nation;  // points into static nation_names[nk]
            int         year;
            double      sum_profit;
        };

        // Include all (nation, year) pairs where the aggregated sum is non-zero.
        // For TPC-H SF10 all 175 combinations have data; amounts are always non-trivially
        // non-zero (sum of millions of dollar values cannot cancel to exactly 0.0).

        std::vector<Result> results;
        results.reserve(175);

        const double* agg = thread_aggs.data();
        for (int n = 0; n < N_NATIONS; n++) {
            for (int y = 0; y < 7; y++) {
                double val = agg[n * N_YEARS + y];
                if (val != 0.0) {
                    results.push_back({nation_names[n], 1992 + y, val});
                }
            }
        }

        // Sort: nation ASC, o_year DESC
        std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
            int cmp = strcmp(a.nation, b.nation);
            if (cmp != 0) return cmp < 0;
            return a.year > b.year;
        });

        // Write CSV
        std::string out_path = results_dir + "/Q9.csv";
        std::ofstream out(out_path);
        if (!out.is_open()) {
            std::cerr << "Cannot open output file: " << out_path << std::endl;
            exit(1);
        }
        out << "nation,o_year,sum_profit\n";
        out << std::fixed << std::setprecision(2);
        for (const auto& r : results) {
            out << r.nation << "," << r.year << "," << r.sum_profit << "\n";
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q9(gendb_dir, results_dir);
    return 0;
}
#endif
