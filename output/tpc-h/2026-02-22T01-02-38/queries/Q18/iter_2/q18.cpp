// Q18: Large Volume Customer
// Strategy:
//   Phase 1 – Parallel scan of lineitem (l_orderkey + l_quantity), thread-local hash
//              aggregation, global merge, filter SUM > 300 → ~624 qualifying orderkeys
//   Phase 2 – Probe orders_orderkey_hash with qualifying orderkeys
//   Phase 3 – Probe customer_custkey_hash with o_custkey values
//   Phase 4 – Sort top-100, emit CSV

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <climits>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "date_utils.h"
#include <iostream>
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Hash function (same as pre-built indexes)
// ---------------------------------------------------------------------------
static inline uint32_t hash32(int32_t key, uint32_t mask) {
    return (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & mask;
}

// ---------------------------------------------------------------------------
// Index slot (shared layout for all pre-built hash indexes)
// ---------------------------------------------------------------------------
struct HtSlot {
    int32_t  key;
    uint32_t offset;
    uint32_t count;
};

// ---------------------------------------------------------------------------
// Global atomic hash map slot for subquery aggregation.
// Key and qty are co-located in 8 bytes → single cache-line fetch covers both.
// l_quantity is always integer 1..50 in TPC-H; max sum per orderkey = 350,
// so int32_t for qty is sufficient (threshold > 300).
// ---------------------------------------------------------------------------
struct GSlot {
    int32_t key;  // l_orderkey; INT32_MIN = empty sentinel
    int32_t qty;  // accumulated l_quantity (integer sum, max 350)
};

// ---------------------------------------------------------------------------
// Result row assembled from all three tables
// ---------------------------------------------------------------------------
struct ResultRow {
    std::string c_name;
    int32_t     c_custkey;
    int32_t     o_orderkey;
    int32_t     o_orderdate;
    double      o_totalprice;
    double      sum_qty;
};

// ---------------------------------------------------------------------------
// Helper: mmap a file read-only, close fd immediately (mmap holds reference)
// ---------------------------------------------------------------------------
static const void* mmap_ro(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); return nullptr; }
    struct stat st;
    fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* ptr = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    close(fd);
    return ptr;
}

// ---------------------------------------------------------------------------
// Main query function
// ---------------------------------------------------------------------------
void run_Q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    auto P = [&](const std::string& rel) { return gendb_dir + "/" + rel; };

    // =========================================================================
    // Phase 0: Data loading — mmap lineitem columns + async prefetch
    // =========================================================================
    const int32_t* l_orderkey = nullptr;
    const double*  l_quantity = nullptr;
    size_t         sz_lok = 0, sz_lq = 0;
    size_t         n_lineitem = 0;
    {
        GENDB_PHASE("data_loading");

        // Open without MAP_POPULATE so we can issue async WILLNEED below
        int fd_lok = open(P("lineitem/l_orderkey.bin").c_str(), O_RDONLY);
        int fd_lq  = open(P("lineitem/l_quantity.bin").c_str(),  O_RDONLY);
        struct stat st_lok, st_lq;
        fstat(fd_lok, &st_lok); sz_lok = (size_t)st_lok.st_size;
        fstat(fd_lq,  &st_lq);  sz_lq  = (size_t)st_lq.st_size;

        l_orderkey = (const int32_t*)mmap(nullptr, sz_lok, PROT_READ, MAP_PRIVATE, fd_lok, 0);
        l_quantity  = (const double*) mmap(nullptr, sz_lq,  PROT_READ, MAP_PRIVATE, fd_lq,  0);

        posix_fadvise(fd_lok, 0, sz_lok, POSIX_FADV_SEQUENTIAL);
        posix_fadvise(fd_lq,  0, sz_lq,  POSIX_FADV_SEQUENTIAL);
        close(fd_lok); close(fd_lq);

        // Concurrent async prefetch for cold-start I/O
        #pragma omp parallel sections
        {
            #pragma omp section
            madvise((void*)l_orderkey, sz_lok, MADV_WILLNEED);
            #pragma omp section
            madvise((void*)l_quantity, sz_lq,  MADV_WILLNEED);
        }

        n_lineitem = sz_lok / sizeof(int32_t);
    }

    // =========================================================================
    // Phase 1: Subquery — single atomic global hash map, parallel scan.
    //          Each thread inserts directly via CAS (key) + atomic_fetch_add (qty).
    //          No thread-local maps, no serial merge step — eliminates the dominant cost.
    //          33M slots at 45% load covers 15M distinct orderkeys safely.
    // =========================================================================
    std::vector<std::pair<int32_t, double>> qualifying; // (orderkey, sum_qty)
    {
        GENDB_PHASE("subquery_scan");

        static constexpr uint32_t GCAP  = 1u << 25;  // 33,554,432 slots
        static constexpr uint32_t GMASK = GCAP - 1;

        // 264 MB global map; key+qty co-located in 8 bytes (one cache-line fetch each)
        GSlot* gslots = new GSlot[GCAP];

        // Parallel init: first-touch initializes across NUMA nodes, distributes pages
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < GCAP; ++i) {
            gslots[i].key = INT32_MIN;
            gslots[i].qty = 0;
        }

        // Parallel scan: each thread atomically inserts (key, qty) into global map.
        // CAS for key insertion; lock-free int32 fetch_add for qty accumulation.
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n_lineitem; ++i) {
            int32_t key = l_orderkey[i];
            int32_t qty = (int32_t)l_quantity[i];  // l_quantity is integer 1..50
            uint32_t h  = hash32(key, GMASK);
            for (uint32_t p = 0; p < GCAP; ++p) {
                GSlot* sl = &gslots[h];
                int32_t k = __atomic_load_n(&sl->key, __ATOMIC_RELAXED);
                if (k == key) {
                    __atomic_fetch_add(&sl->qty, qty, __ATOMIC_RELAXED);
                    break;
                }
                if (k == INT32_MIN) {
                    int32_t expected = INT32_MIN;
                    if (__atomic_compare_exchange_n(&sl->key, &expected, key,
                                                   false,
                                                   __ATOMIC_RELAXED,
                                                   __ATOMIC_RELAXED)) {
                        __atomic_fetch_add(&sl->qty, qty, __ATOMIC_RELAXED);
                        break;
                    }
                    // CAS failed: another thread wrote this slot — check if same key
                    k = __atomic_load_n(&sl->key, __ATOMIC_RELAXED);
                    if (k == key) {
                        __atomic_fetch_add(&sl->qty, qty, __ATOMIC_RELAXED);
                        break;
                    }
                }
                h = (h + 1) & GMASK;  // linear probe
            }
            // If we exhaust all slots, the table is full — should never happen with correct GCAP
            __builtin_unreachable();
        }

        // Sequential collect: scan 264 MB, ~624 qualifying orderkeys emitted
        for (uint32_t i = 0; i < GCAP; ++i) {
            if (gslots[i].key != INT32_MIN && gslots[i].qty > 300) {
                qualifying.emplace_back(gslots[i].key, (double)gslots[i].qty);
            }
        }

        delete[] gslots;
    }

    munmap((void*)l_orderkey, sz_lok);
    munmap((void*)l_quantity,  sz_lq);

    // =========================================================================
    // Phase 2: Probe orders_orderkey_hash with qualifying orderkeys
    // =========================================================================
    size_t sz_ooh, sz_ook, sz_ock, sz_ood, sz_otp;
    const uint8_t*  ooh_raw       = (const uint8_t*) mmap_ro(P("indexes/orders_orderkey_hash.bin"), sz_ooh);
    const int32_t*  o_orderkey_c  = (const int32_t*) mmap_ro(P("orders/o_orderkey.bin"),             sz_ook);
    const int32_t*  o_custkey_c   = (const int32_t*) mmap_ro(P("orders/o_custkey.bin"),              sz_ock);
    const int32_t*  o_orderdate_c = (const int32_t*) mmap_ro(P("orders/o_orderdate.bin"),            sz_ood);
    const double*   o_totalprice_c= (const double*)  mmap_ro(P("orders/o_totalprice.bin"),           sz_otp);

    uint32_t        ooh_ht_size = *(const uint32_t*)ooh_raw;
    uint32_t        ooh_mask    = ooh_ht_size - 1;
    const HtSlot*   ooh_slots   = (const HtSlot*)(ooh_raw + 8);
    const uint32_t* ooh_pos     = (const uint32_t*)(ooh_raw + 8 + (size_t)ooh_ht_size * 12);

    // Intermediate rows before customer lookup
    struct OrdRow {
        int32_t o_orderkey;
        int32_t o_custkey;
        int32_t o_orderdate;
        double  o_totalprice;
        double  sum_qty;
    };
    std::vector<OrdRow> ord_rows;
    ord_rows.reserve(qualifying.size());

    {
        GENDB_PHASE("orders_probe");
        for (auto& [okey, sqty] : qualifying) {
            uint32_t h = hash32(okey, ooh_mask);
            for (uint32_t p = 0; p <= ooh_mask; ++p) {
                uint32_t s = (h + p) & ooh_mask;
                if (ooh_slots[s].key == okey) {
                    uint32_t ridx = ooh_pos[ooh_slots[s].offset];
                    ord_rows.push_back({
                        o_orderkey_c[ridx],
                        o_custkey_c[ridx],
                        o_orderdate_c[ridx],
                        o_totalprice_c[ridx],
                        sqty
                    });
                    break;
                }
                if (ooh_slots[s].key == INT32_MIN) break; // not found
            }
        }
    }

    munmap((void*)ooh_raw,       sz_ooh);
    munmap((void*)o_orderkey_c,  sz_ook);
    munmap((void*)o_custkey_c,   sz_ock);
    munmap((void*)o_orderdate_c, sz_ood);
    munmap((void*)o_totalprice_c,sz_otp);

    // =========================================================================
    // Phase 3: Probe customer_custkey_hash to resolve c_name
    // =========================================================================
    size_t sz_cch, sz_cname;
    const uint8_t* cch_raw   = (const uint8_t*)mmap_ro(P("indexes/customer_custkey_hash.bin"), sz_cch);
    const char*    c_name_c  = (const char*)   mmap_ro(P("customer/c_name.bin"),               sz_cname);

    uint32_t        cch_ht_size = *(const uint32_t*)cch_raw;
    uint32_t        cch_mask    = cch_ht_size - 1;
    const HtSlot*   cch_slots   = (const HtSlot*)(cch_raw + 8);
    const uint32_t* cch_pos     = (const uint32_t*)(cch_raw + 8 + (size_t)cch_ht_size * 12);

    std::vector<ResultRow> results;
    results.reserve(ord_rows.size());

    {
        GENDB_PHASE("customer_probe");
        for (auto& or_ : ord_rows) {
            uint32_t h = hash32(or_.o_custkey, cch_mask);
            for (uint32_t p = 0; p <= cch_mask; ++p) {
                uint32_t s = (h + p) & cch_mask;
                if (cch_slots[s].key == or_.o_custkey) {
                    uint32_t ridx = cch_pos[cch_slots[s].offset];
                    const char* nm = c_name_c + (size_t)ridx * 26;
                    results.push_back({
                        std::string(nm, strnlen(nm, 26)),
                        or_.o_custkey,
                        or_.o_orderkey,
                        or_.o_orderdate,
                        or_.o_totalprice,
                        or_.sum_qty
                    });
                    break;
                }
                if (cch_slots[s].key == INT32_MIN) break;
            }
        }
    }

    munmap((void*)cch_raw,  sz_cch);
    munmap((void*)c_name_c, sz_cname);

    // =========================================================================
    // Phase 4: Sort (o_totalprice DESC, o_orderdate ASC) and emit top-100
    // =========================================================================
    {
        GENDB_PHASE("sort_topk");
        std::partial_sort(results.begin(), results.begin() + std::min((size_t)100, results.size()), results.end(),
            [](const ResultRow& a, const ResultRow& b) {
                if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
                return a.o_orderdate < b.o_orderdate;
            });
    }

    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q18.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { perror(out_path.c_str()); return; }

        fprintf(fp, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");
        size_t lim = std::min((size_t)100, results.size());
        for (size_t i = 0; i < lim; ++i) {
            const ResultRow& r = results[i];
            char ds[16];
            gendb::epoch_days_to_date_str(r.o_orderdate, ds);
            fprintf(fp, "%s,%d,%d,%s,%.2f,%.2f\n",
                    r.c_name.c_str(),
                    r.c_custkey,
                    r.o_orderkey,
                    ds,
                    r.o_totalprice,
                    r.sum_qty);
        }
        fclose(fp);
    }
}

// ---------------------------------------------------------------------------
#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q18(gendb_dir, results_dir);
    return 0;
}
#endif
