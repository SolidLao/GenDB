#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cmath>
#include <cassert>
#include <string>
#include <vector>
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <omp.h>
#include "date_utils.h"
#include <iostream>
#include "timing_utils.h"

// ─── mmap helper ───────────────────────────────────────────────────────────────
struct MmapFile {
    void*  ptr  = MAP_FAILED;
    size_t size = 0;
    int    fd   = -1;

    bool open(const char* path) {
        fd = ::open(path, O_RDONLY);
        if (fd < 0) { perror(path); return false; }
        struct stat st; fstat(fd, &st); size = st.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (ptr == MAP_FAILED) { perror("mmap"); return false; }
        posix_fadvise(fd, 0, size, POSIX_FADV_SEQUENTIAL);
        return true;
    }
    void prefetch() { if (ptr != MAP_FAILED) madvise(ptr, size, MADV_WILLNEED); }

    template<typename T> const T* as() const { return reinterpret_cast<const T*>(ptr); }
    size_t count(size_t elem_sz) const { return size / elem_sz; }
};

// ─── year boundary table ────────────────────────────────────────────────────────
// epoch days for Jan 1 of each year (1992–1999)
// Verified: 1992 is a leap year (366 days), so 1993-01-01 = 8035+366 = 8401
static const int32_t YEAR_BOUNDS[8] = {8035, 8401, 8766, 9131, 9496, 9862, 10227, 10592};
// years: 1992=0, 1993=1, ..., 1998=6

static inline int epoch_to_year_idx(int32_t d) {
    // binary search in 8-element array → index into 1992..1998
    // returns 0..6 for 1992..1998
    int lo = 0, hi = 6;
    while (lo < hi) {
        int mid = (lo + hi + 1) >> 1;
        if (d >= YEAR_BOUNDS[mid]) lo = mid; else hi = mid - 1;
    }
    return lo; // year = 1992 + lo
}

// ─── Partsupp composite hash slot ──────────────────────────────────────────────
struct PSSlot {
    int64_t  key;
    uint32_t row_idx;
    uint32_t _pad;
};

static inline uint32_t ps_hash(int64_t key, uint32_t mask) {
    return (uint32_t)((uint64_t)key * 0x9E3779B97F4A7C15ULL >> 32) & mask;
}

static inline uint32_t lookup_ps(const PSSlot* ht, uint32_t mask, int32_t partkey, int32_t suppkey) {
    int64_t key = (int64_t)partkey * 100003LL + suppkey;
    uint32_t h = ps_hash(key, mask);
    for (uint32_t probe = 0; probe <= mask; ++probe) {
        const PSSlot& s = ht[(h + probe) & mask];
        if (s.key == key) return s.row_idx;
        if (s.key == INT64_MIN) return UINT32_MAX;
    }
    return UINT32_MAX;
}

// ─── Orders orderkey hash slot ──────────────────────────────────────────────────
struct OKSlot {
    int32_t  key;
    uint32_t row_idx;
};

static inline uint32_t ok_hash(int32_t key, uint32_t mask) {
    return ((uint32_t)key * 2654435761u) & mask;
}

static inline uint32_t lookup_ok(const OKSlot* ht, uint32_t mask, int32_t orderkey) {
    uint32_t h = ok_hash(orderkey, mask);
    for (uint32_t probe = 0; probe <= mask; ++probe) {
        const OKSlot& s = ht[(h + probe) & mask];
        if (s.key == orderkey) return s.row_idx;
        if (s.key == INT32_MIN) return UINT32_MAX;
    }
    return UINT32_MAX;
}

// ─── Main query function ────────────────────────────────────────────────────────
void run_Q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    auto path = [&](const char* rel) { return gendb_dir + "/" + rel; };

    // ── Phase 0: open + prefetch all columns ────────────────────────────────────
    MmapFile f_p_partkey, f_p_name;
    MmapFile f_l_partkey, f_l_suppkey, f_l_orderkey;
    MmapFile f_l_extprice, f_l_discount, f_l_quantity;
    MmapFile f_disc_lut, f_qty_lut;
    MmapFile f_s_suppkey, f_s_nationkey;
    MmapFile f_n_nationkey, f_n_name;
    MmapFile f_ps_hash, f_ps_supplycost;
    MmapFile f_ok_hash, f_o_orderdate;

    {
        GENDB_PHASE("data_loading");

        f_p_partkey.open(path("part/p_partkey.bin").c_str());
        f_p_name.open(path("part/p_name.bin").c_str());

        f_l_partkey.open(path("lineitem/l_partkey.bin").c_str());
        f_l_suppkey.open(path("lineitem/l_suppkey.bin").c_str());
        f_l_orderkey.open(path("lineitem/l_orderkey.bin").c_str());
        f_l_extprice.open(path("lineitem/l_extendedprice.bin").c_str());
        f_l_discount.open(path("lineitem/l_discount.bin").c_str());
        f_l_quantity.open(path("lineitem/l_quantity.bin").c_str());

        f_disc_lut.open(path("lineitem/l_discount_lookup.bin").c_str());
        f_qty_lut.open(path("lineitem/l_quantity_lookup.bin").c_str());

        f_s_suppkey.open(path("supplier/s_suppkey.bin").c_str());
        f_s_nationkey.open(path("supplier/s_nationkey.bin").c_str());

        f_n_nationkey.open(path("nation/n_nationkey.bin").c_str());
        f_n_name.open(path("nation/n_name.bin").c_str());

        f_ps_hash.open(path("partsupp/indexes/composite_hash.bin").c_str());
        f_ps_supplycost.open(path("partsupp/ps_supplycost.bin").c_str());

        f_ok_hash.open(path("orders/indexes/orderkey_hash.bin").c_str());
        f_o_orderdate.open(path("orders/o_orderdate.bin").c_str());

        // Issue concurrent prefetches
        #pragma omp parallel sections num_threads(8)
        {
            #pragma omp section
            { f_p_name.prefetch(); f_p_partkey.prefetch(); }
            #pragma omp section
            { f_l_partkey.prefetch(); f_l_suppkey.prefetch(); }
            #pragma omp section
            { f_l_orderkey.prefetch(); f_l_extprice.prefetch(); }
            #pragma omp section
            { f_l_discount.prefetch(); f_l_quantity.prefetch(); }
            #pragma omp section
            { f_ps_hash.prefetch(); f_ps_supplycost.prefetch(); }
            #pragma omp section
            { f_ok_hash.prefetch(); f_o_orderdate.prefetch(); }
            #pragma omp section
            { f_s_suppkey.prefetch(); f_s_nationkey.prefetch(); }
            #pragma omp section
            { f_n_nationkey.prefetch(); f_n_name.prefetch(); }
        }
    }

    // ── Phase 1: Dimension table loading + filter ───────────────────────────────
    // Part green bitset
    static uint64_t part_green_bitset[2000001 / 64 + 2] = {};
    size_t n_parts = 0;

    // Supplier direct lookup: s_nationkey_arr[s_suppkey]
    static int32_t s_nationkey_arr[100001];
    std::fill(s_nationkey_arr, s_nationkey_arr + 100001, -1);

    // Nation names: nation_name[n_nationkey] = char[26]
    static char nation_name[25][26];

    {
        GENDB_PHASE("dim_filter");

        // Part: scan p_name for 'green', build bitset
        {
            const int32_t* p_partkey = f_p_partkey.as<int32_t>();
            const char*    p_name    = f_p_name.as<char>();
            n_parts = f_p_partkey.count(sizeof(int32_t));

            for (size_t i = 0; i < n_parts; ++i) {
                const char* nm = p_name + i * 56;
                if (strstr(nm, "green") != nullptr) {
                    int32_t pk = p_partkey[i];
                    part_green_bitset[pk >> 6] |= (uint64_t)1 << (pk & 63);
                }
            }
        }

        // Supplier: build s_nationkey_arr[s_suppkey] = nationkey
        {
            const int32_t* s_suppkey    = f_s_suppkey.as<int32_t>();
            const int32_t* s_nationkey  = f_s_nationkey.as<int32_t>();
            size_t n_supp = f_s_suppkey.count(sizeof(int32_t));
            for (size_t i = 0; i < n_supp; ++i) {
                int32_t sk = s_suppkey[i];
                if (sk >= 0 && sk <= 100000) {
                    s_nationkey_arr[sk] = s_nationkey[i];
                }
            }
        }

        // Nation: build nation_name[n_nationkey]
        {
            const int32_t* n_nationkey = f_n_nationkey.as<int32_t>();
            const char*    n_name_ptr  = f_n_name.as<char>();
            size_t n_nations = f_n_nationkey.count(sizeof(int32_t));
            for (size_t i = 0; i < n_nations; ++i) {
                int32_t nk = n_nationkey[i];
                if (nk >= 0 && nk < 25) {
                    strncpy(nation_name[nk], n_name_ptr + i * 26, 25);
                    nation_name[nk][25] = '\0';
                }
            }
        }
    }

    // ── Phase 2: Load pre-built indexes (zero build cost) ──────────────────────
    const uint32_t ps_cap  = f_ps_hash.as<uint32_t>()[0];
    const PSSlot*  ps_ht   = reinterpret_cast<const PSSlot*>(f_ps_hash.as<uint8_t>() + 4);
    const uint32_t ps_mask = ps_cap - 1;
    const double*  ps_supplycost = f_ps_supplycost.as<double>();

    const uint32_t ok_cap  = f_ok_hash.as<uint32_t>()[0];
    const OKSlot*  ok_ht   = reinterpret_cast<const OKSlot*>(f_ok_hash.as<uint8_t>() + 4);
    const uint32_t ok_mask = ok_cap - 1;
    const int32_t* o_orderdate = f_o_orderdate.as<int32_t>();

    // Lineitem columns
    const int32_t* l_partkey  = f_l_partkey.as<int32_t>();
    const int32_t* l_suppkey  = f_l_suppkey.as<int32_t>();
    const int32_t* l_orderkey = f_l_orderkey.as<int32_t>();
    const double*  l_extprice = f_l_extprice.as<double>();
    const uint8_t* l_discount = f_l_discount.as<uint8_t>();
    const uint8_t* l_quantity = f_l_quantity.as<uint8_t>();
    const double*  disc_lut   = f_disc_lut.as<double>();
    const double*  qty_lut    = f_qty_lut.as<double>();

    size_t n_lineitem = f_l_partkey.count(sizeof(int32_t));

    // ── Phase 3: Parallel scan of lineitem ─────────────────────────────────────
    // Thread-local aggregation: [nation 0..24][year_idx 0..6]
    const int N_THREADS = 64;
    static double tl_agg[N_THREADS][25][8]; // 8 slots; years 1992-1998 use idx 0-6
    memset(tl_agg, 0, sizeof(tl_agg));

    {
        GENDB_PHASE("main_scan");

        const int MORSEL = 65536;
        const int64_t n_morsels = (int64_t)(n_lineitem + MORSEL - 1) / MORSEL;

        #pragma omp parallel for schedule(dynamic, 1) num_threads(N_THREADS)
        for (int64_t m = 0; m < n_morsels; ++m) {
            int tid = omp_get_thread_num();
            double (*my_agg)[8] = tl_agg[tid];

            size_t start = (size_t)m * MORSEL;
            size_t end   = std::min(start + (size_t)MORSEL, n_lineitem);

            for (size_t i = start; i < end; ++i) {
                int32_t pk = l_partkey[i];

                // Part green filter
                if (!(part_green_bitset[pk >> 6] & ((uint64_t)1 << (pk & 63))))
                    continue;

                int32_t sk = l_suppkey[i];
                int32_t nk = s_nationkey_arr[sk];
                if (nk < 0 || nk >= 25) continue;

                // Orders lookup → year
                int32_t ok = l_orderkey[i];
                uint32_t o_row = lookup_ok(ok_ht, ok_mask, ok);
                if (o_row == UINT32_MAX) continue;
                int32_t odate = o_orderdate[o_row];
                int yr_idx = epoch_to_year_idx(odate); // 0=1992..6=1998

                // Partsupp lookup → supplycost
                uint32_t ps_row = lookup_ps(ps_ht, ps_mask, pk, sk);
                if (ps_row == UINT32_MAX) continue;
                double supplycost = ps_supplycost[ps_row];

                // Amount computation
                double extprice = l_extprice[i];
                double disc  = disc_lut[l_discount[i]];
                double qty   = qty_lut[l_quantity[i]];
                double amount = extprice * (1.0 - disc) - supplycost * qty;

                my_agg[nk][yr_idx] += amount;
            }
        }
    }

    // ── Merge thread-local aggregations ────────────────────────────────────────
    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 1; t < N_THREADS; ++t) {
            for (int n = 0; n < 25; ++n)
                for (int y = 0; y < 8; ++y)
                    tl_agg[0][n][y] += tl_agg[t][n][y];
        }
    }

    // ── Phase 4: Output ─────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        struct Row {
            char   nation[26];
            int    o_year;
            double sum_profit;
        };
        std::vector<Row> results;
        results.reserve(175);

        for (int n = 0; n < 25; ++n) {
            for (int y = 0; y < 7; ++y) {
                double sp = tl_agg[0][n][y];
                if (sp == 0.0) continue;
                Row r;
                strncpy(r.nation, nation_name[n], 25); r.nation[25] = '\0';
                r.o_year = 1992 + y;
                r.sum_profit = sp;
                results.push_back(r);
            }
        }

        std::sort(results.begin(), results.end(), [](const Row& a, const Row& b) {
            int cmp = strcmp(a.nation, b.nation);
            if (cmp != 0) return cmp < 0;
            return a.o_year > b.o_year; // DESC
        });

        std::string out_path = results_dir + "/Q9.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { perror(out_path.c_str()); return; }

        fprintf(fp, "nation,o_year,sum_profit\n");
        for (const auto& r : results) {
            fprintf(fp, "%s,%d,%.2f\n", r.nation, r.o_year, r.sum_profit);
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
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q9(gendb_dir, results_dir);
    return 0;
}
#endif
