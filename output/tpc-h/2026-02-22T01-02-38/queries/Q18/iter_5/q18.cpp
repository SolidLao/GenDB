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

// (LocalMap removed — replaced by two-pass partitioned aggregation in run_Q18)

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
// Helper: mmap a file read-only with MAP_POPULATE (for sequential full scans)
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
// Helper: mmap a file read-only WITHOUT MAP_POPULATE (for random/sparse access)
// — avoids forcing 700 MB of page-table entries for files accessed at ~624 points
// ---------------------------------------------------------------------------
static const void* mmap_ra(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); return nullptr; }
    struct stat st;
    fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* ptr = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
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
    // Phase 1: Subquery — two-pass partitioned aggregation
    //   Pass 1: Classify each lineitem row into one of NPART=64 partitions
    //           (top 6 bits of hash). Each thread writes only to its own
    //           partition vectors → zero cross-thread contention.
    //   Pass 2: Thread t aggregates all partition-t data with a 6 MB local
    //           hash table (2^19 slots, L3-resident). Avoids the prior
    //           sequential 400 MB global-merge bottleneck (~5.8 s of DRAM).
    // =========================================================================
    std::vector<std::pair<int32_t, double>> qualifying; // (orderkey, sum_qty)
    {
        GENDB_PHASE("subquery_scan");

        static constexpr int      NPART      = 64;
        static constexpr int      PART_SHIFT = 26;       // top 6 bits of 32-bit hash
        static constexpr uint32_t PCAP       = 1u << 19; // 524,288 local slots per partition
        static constexpr uint32_t PMASK      = PCAP - 1;

        struct KV { int32_t key; double val; };

        // Cap nthreads so that part_bufs[tid][*] is never out-of-bounds
        int nthreads = std::min(omp_get_max_threads(), NPART);

        // part_bufs[t][p]: KV pairs belonging to partition p, seen by thread t.
        // PRE-ALLOCATE all vectors upfront to eliminate 57K+ realloc/memcpy/free
        // calls that occur under 64-thread malloc contention (was ~800ms overhead).
        std::vector<KV> part_bufs[NPART][NPART];
        {
            // Each of the NPART×NPART slots gets ~n_lineitem/(nthreads*nthreads) entries.
            // Reserve 2.5× headroom to avoid ANY reallocation during Pass 1.
            size_t per_slot = n_lineitem / ((size_t)nthreads * nthreads) * 5 / 2 + 4096;
            for (int t = 0; t < nthreads; ++t)
                for (int p = 0; p < nthreads; ++p)
                    part_bufs[t][p].reserve(per_slot);
        }

        // Pre-allocate ALL partition hash tables as one contiguous block to
        // avoid 64 parallel new[] calls (malloc contention) inside Pass 2.
        // Total: 64 × 512K slots × (4+8) bytes = 384 MB — fine on 376 GB server.
        const size_t phts = (size_t)NPART * PCAP;
        int32_t* all_pkeys = new int32_t[phts];
        double*  all_pvals = new double[phts]();         // zero-initialized by ()
        std::fill(all_pkeys, all_pkeys + phts, INT32_MIN);

        // ---- Pass 1: Parallel classification — no shared writes ----
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_lineitem; ++i) {
                int32_t  key  = l_orderkey[i];
                double   val  = l_quantity[i];
                uint32_t h    = (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32);
                int      part = (int)(h >> PART_SHIFT); // [0, 63]
                part_bufs[tid][part].push_back({key, val});
            }
        }

        // ---- Pass 2: Parallel per-partition aggregation ----
        // Each thread t owns partition t (nthreads==NPART=64 so exactly 1 each).
        // Uses pre-allocated pkeys/pvals slice — no runtime malloc.
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            for (int p = tid; p < NPART; p += nthreads) {
                int32_t* pkeys = all_pkeys + (size_t)p * PCAP;
                double*  pvals = all_pvals + (size_t)p * PCAP;
                // pkeys/pvals already initialised to INT32_MIN / 0.0 above

                // Gather and aggregate all threads' contributions to partition p
                for (int t = 0; t < nthreads; ++t) {
                    for (const KV& kv : part_bufs[t][p]) {
                        uint32_t h = (uint32_t)((uint64_t)(uint32_t)kv.key * 0x9E3779B97F4A7C15ULL >> 32) & PMASK;
                        bool inserted = false;
                        for (uint32_t probe = 0; probe < PCAP; ++probe) {
                            uint32_t s = (h + probe) & PMASK;
                            if (pkeys[s] == kv.key) { pvals[s] += kv.val; inserted = true; break; }
                            if (pkeys[s] == INT32_MIN) { pkeys[s] = kv.key; pvals[s] = kv.val; inserted = true; break; }
                        }
                        if (!inserted) { fprintf(stderr, "ERROR: partition hash table exhausted\n"); abort(); }
                    }
                }

                // Collect qualifying (SUM > 300)
                std::vector<std::pair<int32_t,double>> local_q;
                for (uint32_t i = 0; i < PCAP; ++i)
                    if (pkeys[i] != INT32_MIN && pvals[i] > 300.0)
                        local_q.emplace_back(pkeys[i], pvals[i]);

                if (!local_q.empty()) {
                    #pragma omp critical
                    qualifying.insert(qualifying.end(), local_q.begin(), local_q.end());
                }
            }
        }

        delete[] all_pkeys;
        delete[] all_pvals;
        // part_bufs freed here as part_bufs[NPART][NPART] goes out of scope
    }

    munmap((void*)l_orderkey, sz_lok);
    munmap((void*)l_quantity,  sz_lq);

    // =========================================================================
    // Phase 2: Probe orders_orderkey_hash with qualifying orderkeys
    // =========================================================================
    // Use mmap_ra (no MAP_POPULATE) for all index/column files accessed at only
    // ~624 random positions — avoids page-faulting 700 MB of unnecessary data.
    size_t sz_ooh, sz_ook, sz_ock, sz_ood, sz_otp;
    const uint8_t*  ooh_raw       = (const uint8_t*) mmap_ra(P("indexes/orders_orderkey_hash.bin"), sz_ooh);
    const int32_t*  o_orderkey_c  = (const int32_t*) mmap_ra(P("orders/o_orderkey.bin"),             sz_ook);
    const int32_t*  o_custkey_c   = (const int32_t*) mmap_ra(P("orders/o_custkey.bin"),              sz_ock);
    const int32_t*  o_orderdate_c = (const int32_t*) mmap_ra(P("orders/o_orderdate.bin"),            sz_ood);
    const double*   o_totalprice_c= (const double*)  mmap_ra(P("orders/o_totalprice.bin"),           sz_otp);

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
    const uint8_t* cch_raw   = (const uint8_t*)mmap_ra(P("indexes/customer_custkey_hash.bin"), sz_cch);
    const char*    c_name_c  = (const char*)   mmap_ra(P("customer/c_name.bin"),               sz_cname);

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
        std::partial_sort(results.begin(), results.begin() + std::min((size_t)100, results.size()), results.end(), [](const ResultRow& a, const ResultRow& b) {
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
