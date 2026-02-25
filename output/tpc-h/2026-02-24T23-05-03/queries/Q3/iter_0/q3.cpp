// Q3: Shipping Priority
// Strategy:
//   1. Bitmap filter for customer (c_mktsegment='BUILDING') → 187KB L2-resident bitmap
//   2. Build runtime SoA hash table from qualifying orders (inner join key + payload)
//   3. Scan lineitem in parallel: shipdate filter + probe HT + CAS-accumulate revenue
//   4. Collect non-empty HT slots → partial_sort top-10

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <climits>
#include <iostream>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "date_utils.h"
#include "timing_utils.h"

// ─── Constants ──────────────────────────────────────────────────────────────
static const int32_t  SENTINEL  = INT32_MIN;
static const uint32_t HT_CAP    = 4194304u;   // 2^22, supports ~1.455M orders at <35% load
static const uint32_t HT_MASK   = HT_CAP - 1u;
static const uint32_t BLOCK_SZ  = 65536u;

// ─── Hash function ───────────────────────────────────────────────────────────
inline uint32_t ht_hash(int32_t key) {
    return ((uint32_t)key * 2654435761u) & HT_MASK;
}

// ─── Per-slot byte spinlock + long double accumulation ───────────────────────
// C35: must use long double for multi-column derived expression SUM(ep*(1-disc))
// long double (80-bit extended) cannot be CAS'd atomically → use spinlock.
// Contention is minimal: 3.5M groups / 64 threads → ~55K groups per thread.
inline void spinlock_acquire(uint8_t* lock) {
    uint8_t expected = 0;
    while (!__atomic_compare_exchange_n(lock, &expected, 1,
            false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
        expected = 0;
        __builtin_ia32_pause();
    }
}
inline void spinlock_release(uint8_t* lock) {
    __atomic_store_n(lock, 0, __ATOMIC_RELEASE);
}

// ─── Bitmap helpers ───────────────────────────────────────────────────────────
inline void bitmap_set(uint8_t* bm, int32_t idx) {
    bm[idx >> 3] |= (uint8_t)(1u << (idx & 7));
}
inline bool bitmap_test(const uint8_t* bm, int32_t idx) {
    return (bm[idx >> 3] >> (idx & 7)) & 1;
}

// ─── Zone-map block layout ────────────────────────────────────────────────────
struct ZMBlock { int32_t mn, mx; uint32_t cnt; };

// ─── SoA hash table (global allocations) ─────────────────────────────────────
static int32_t*     g_ht_keys  = nullptr;  // 4M × 4  = 16 MB  (INT32_MIN = empty)
static int32_t*     g_ht_odate = nullptr;  // 4M × 4  = 16 MB  o_orderdate payload
static int32_t*     g_ht_ospri = nullptr;  // 4M × 4  = 16 MB  o_shippriority payload
static long double* g_ht_rev   = nullptr;  // 4M × 16 = 64 MB  long double revenue (C35)
static uint8_t*     g_ht_lock  = nullptr;  // 4M × 1  =  4 MB  per-slot spinlock

// ─── mmap helper ─────────────────────────────────────────────────────────────
struct MmapFile {
    void*  ptr  = nullptr;
    size_t size = 0;
    void open(const std::string& path, bool seq = true) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(("open: " + path).c_str()); return; }
        struct stat st; fstat(fd, &st);
        size = (size_t)st.st_size;
        ptr  = mmap(nullptr, size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (seq) {
            madvise(ptr, size, MADV_SEQUENTIAL);
            posix_fadvise(fd, 0, (off_t)size, POSIX_FADV_SEQUENTIAL);
        }
        close(fd);
    }
    void close_file() { if (ptr && size) { munmap(ptr, size); ptr = nullptr; size = 0; } }
    template<typename T> const T* as() const { return reinterpret_cast<const T*>(ptr); }
};

// ─── Query ───────────────────────────────────────────────────────────────────
void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t date_thr = gendb::date_str_to_epoch_days("1995-03-15");
    const int32_t ship_thr = date_thr;  // same date for both filters

    // Allocate SoA hash table
    posix_memalign((void**)&g_ht_keys,  64, HT_CAP * sizeof(int32_t));
    posix_memalign((void**)&g_ht_odate, 64, HT_CAP * sizeof(int32_t));
    posix_memalign((void**)&g_ht_ospri, 64, HT_CAP * sizeof(int32_t));
    posix_memalign((void**)&g_ht_rev,   64, HT_CAP * sizeof(long double));
    posix_memalign((void**)&g_ht_lock,  64, HT_CAP * sizeof(uint8_t));

    // C20: use std::fill for multi-byte sentinel, NEVER memset
    std::fill(g_ht_keys, g_ht_keys + HT_CAP, SENTINEL);
    // long double 0.0L: explicit fill (not memset, since ld may have padding bits)
    std::fill(g_ht_rev, g_ht_rev + HT_CAP, 0.0L);
    memset(g_ht_lock, 0, HT_CAP * sizeof(uint8_t));

    // Customer bitmap: indices [1, 1500000] → 1500001 bits → 187501 bytes
    static const int32_t CUST_MAX = 1500001;
    uint8_t* cust_bm = new uint8_t[(CUST_MAX + 7) / 8]();

    // ── Phase: data_loading ──────────────────────────────────────────────────
    MmapFile f_c_mkt, f_c_ckey;
    MmapFile f_o_okey, f_o_ckey, f_o_odate, f_o_ospri;
    MmapFile f_l_okey, f_l_ep, f_l_disc, f_l_ship;
    MmapFile f_ozm, f_lzm;
    {
        GENDB_PHASE("data_loading");
        // Customer
        f_c_mkt .open(gendb_dir + "/customer/c_mktsegment.bin");
        f_c_ckey.open(gendb_dir + "/customer/c_custkey.bin");
        // Orders
        f_o_okey .open(gendb_dir + "/orders/o_orderkey.bin");
        f_o_ckey .open(gendb_dir + "/orders/o_custkey.bin");
        f_o_odate.open(gendb_dir + "/orders/o_orderdate.bin");
        f_o_ospri.open(gendb_dir + "/orders/o_shippriority.bin");
        // Lineitem
        f_l_okey.open(gendb_dir + "/lineitem/l_orderkey.bin");
        f_l_ep  .open(gendb_dir + "/lineitem/l_extendedprice.bin");
        f_l_disc.open(gendb_dir + "/lineitem/l_discount.bin");
        f_l_ship.open(gendb_dir + "/lineitem/l_shipdate.bin");
        // Zone maps
        f_ozm.open(gendb_dir + "/orders/indexes/o_orderdate_zone_map.bin");
        f_lzm.open(gendb_dir + "/lineitem/indexes/l_shipdate_zone_map.bin");
    }

    // ── Phase: dim_filter — build customer bitmap ────────────────────────────
    {
        GENDB_PHASE("dim_filter");

        // C2: Load dictionary at runtime, find 'BUILDING' code
        int16_t building_code = -1;
        {
            std::ifstream df(gendb_dir + "/customer/c_mktsegment_dict.txt");
            std::string line;
            int16_t idx = 0;
            while (std::getline(df, line)) {
                if (line == "BUILDING") { building_code = idx; break; }
                idx++;
            }
        }
        if (building_code < 0) {
            fprintf(stderr, "ERROR: 'BUILDING' not found in c_mktsegment_dict.txt\n");
            return;
        }

        const int16_t* c_mkt  = f_c_mkt .as<int16_t>();
        const int32_t* c_ckey = f_c_ckey.as<int32_t>();
        const size_t   n_cust = f_c_ckey.size / sizeof(int32_t);

        for (size_t i = 0; i < n_cust; i++) {
            if (c_mkt[i] == building_code) {
                bitmap_set(cust_bm, c_ckey[i]);
            }
        }

        f_c_mkt .close_file();
        f_c_ckey.close_file();
    }

    // ── Phase: build_joins — scan orders → SoA HT ────────────────────────────
    {
        GENDB_PHASE("build_joins");

        const int32_t* o_okey  = f_o_okey .as<int32_t>();
        const int32_t* o_ckey  = f_o_ckey .as<int32_t>();
        const int32_t* o_odate = f_o_odate.as<int32_t>();
        const int32_t* o_ospri = f_o_ospri.as<int32_t>();
        const size_t   n_ord   = f_o_okey.size / sizeof(int32_t);

        // Orders zone map
        const uint32_t  o_nblocks = *f_ozm.as<uint32_t>();
        const ZMBlock*  o_blocks  = reinterpret_cast<const ZMBlock*>(
            static_cast<const uint8_t*>(f_ozm.ptr) + 4);

        #pragma omp parallel for schedule(dynamic, 4) num_threads(64)
        for (uint32_t b = 0; b < o_nblocks; b++) {
            // C19: zone-map skip — skip block if ALL dates >= threshold
            if (o_blocks[b].mn >= date_thr) continue;

            const uint32_t row_start = b * BLOCK_SZ;
            const uint32_t row_end   = row_start + o_blocks[b].cnt;

            for (uint32_t row = row_start; row < row_end; row++) {
                // Filter: o_orderdate < '1995-03-15'
                if (o_odate[row] >= date_thr) continue;
                // Semi-join: o_custkey in customer_bitmap
                if (!bitmap_test(cust_bm, o_ckey[row])) continue;

                // Insert into SoA HT via CAS (C24: bounded probing)
                const int32_t key = o_okey[row];
                const uint32_t h  = ht_hash(key);

                for (uint32_t probe = 0; probe < HT_CAP; probe++) {
                    const uint32_t idx = (h + probe) & HT_MASK;
                    int32_t expected   = SENTINEL;

                    if (__atomic_compare_exchange_n(
                            &g_ht_keys[idx], &expected, key,
                            /*weak=*/false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
                        // Slot claimed by us — write payload
                        g_ht_odate[idx] = o_odate[row];
                        g_ht_ospri[idx] = o_ospri[row];
                        break;
                    }
                    // expected now holds actual value at slot
                    if (expected == key) break;  // already inserted (PK uniqueness)
                }
            }
        }

        f_o_okey .close_file();
        f_o_ckey .close_file();
        f_o_odate.close_file();
        f_o_ospri.close_file();
        f_ozm    .close_file();
    }

    // ── Phase: main_scan — probe lineitem + accumulate revenue ───────────────
    {
        GENDB_PHASE("main_scan");

        const int32_t* l_okey  = f_l_okey.as<int32_t>();
        const double*  l_ep    = f_l_ep  .as<double>();
        const double*  l_disc  = f_l_disc.as<double>();
        const int32_t* l_ship  = f_l_ship.as<int32_t>();
        const size_t   n_line  = f_l_okey.size / sizeof(int32_t);

        // Lineitem zone map
        const uint32_t  l_nblocks = *f_lzm.as<uint32_t>();
        const ZMBlock*  l_blocks  = reinterpret_cast<const ZMBlock*>(
            static_cast<const uint8_t*>(f_lzm.ptr) + 4);

        #pragma omp parallel for schedule(dynamic, 4) num_threads(64)
        for (uint32_t b = 0; b < l_nblocks; b++) {
            // C19: zone-map skip — skip block if ALL dates <= threshold
            if (l_blocks[b].mx <= ship_thr) continue;

            const uint32_t row_start = b * BLOCK_SZ;
            const uint32_t row_end   = row_start + l_blocks[b].cnt;

            for (uint32_t row = row_start; row < row_end; row++) {
                // Filter: l_shipdate > '1995-03-15'
                if (l_ship[row] <= ship_thr) continue;

                // Probe orders HT (C24: bounded probing)
                const int32_t key = l_okey[row];
                const uint32_t h  = ht_hash(key);

                for (uint32_t probe = 0; probe < HT_CAP; probe++) {
                    const uint32_t idx = (h + probe) & HT_MASK;
                    const int32_t  k   = __atomic_load_n(&g_ht_keys[idx], __ATOMIC_RELAXED);
                    if (k == SENTINEL) break;  // empty slot → not found
                    if (k == key) {
                        // C35: long double accumulation for SUM(ep*(1-disc))
                        // Use per-slot spinlock since long double (80-bit) can't CAS atomically
                        const long double contrib = (long double)l_ep[row] *
                                                    (1.0L - (long double)l_disc[row]);
                        spinlock_acquire(&g_ht_lock[idx]);
                        g_ht_rev[idx] += contrib;
                        spinlock_release(&g_ht_lock[idx]);
                        break;
                    }
                }
            }
        }

        f_l_okey.close_file();
        f_l_ep  .close_file();
        f_l_disc.close_file();
        f_l_ship.close_file();
        f_lzm   .close_file();
    }

    // ── Phase: output ─────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        struct Result {
            int32_t     l_orderkey;
            long double revenue;
            int32_t     o_orderdate;
            int32_t     o_shippriority;
        };

        // Collect all occupied HT slots that have revenue > 0
        std::vector<Result> results;
        results.reserve(3600000);

        for (uint32_t i = 0; i < HT_CAP; i++) {
            const int32_t k = g_ht_keys[i];
            if (k != SENTINEL && g_ht_rev[i] > 0.0L) {
                results.push_back({k, g_ht_rev[i], g_ht_odate[i], g_ht_ospri[i]});
            }
        }

        // Partial sort: top-10 by revenue DESC, o_orderdate ASC, l_orderkey ASC (C33)
        auto cmp = [](const Result& a, const Result& b) {
            if (a.revenue != b.revenue)         return a.revenue > b.revenue;
            if (a.o_orderdate != b.o_orderdate) return a.o_orderdate < b.o_orderdate;
            return a.l_orderkey < b.l_orderkey;  // C33: stable unique tiebreaker
        };

        const size_t limit = std::min((size_t)10, results.size());
        if (results.size() > 10) {
            std::partial_sort(results.begin(), results.begin() + (ptrdiff_t)limit,
                              results.end(), cmp);
        } else {
            std::sort(results.begin(), results.end(), cmp);
        }

        // Write CSV
        const std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(("fopen: " + out_path).c_str()); return; }

        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[11];
        for (size_t i = 0; i < limit; i++) {
            const Result& r = results[i];
            gendb::epoch_days_to_date_str(r.o_orderdate, date_buf);
            fprintf(f, "%d,%.2Lf,\"%s\",%d\n",
                    r.l_orderkey, r.revenue, date_buf, r.o_shippriority);
        }
        fclose(f);
    }

    // Cleanup
    free(g_ht_keys);  free(g_ht_odate);
    free(g_ht_ospri); free(g_ht_rev);  free(g_ht_lock);
    delete[] cust_bm;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
