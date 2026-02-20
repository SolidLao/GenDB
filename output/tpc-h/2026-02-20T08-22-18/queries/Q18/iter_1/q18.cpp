// Q18: Large Volume Customer
// Strategy: use pre-built lineitem_orderkey_hash index to find qualifying orderkeys
// (SUM(l_quantity) > 300), then scan orders, lookup customer names, compute sum_qty.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include <omp.h>

#include "date_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Hash index slot layout: {int32_t key, uint32_t offset, uint32_t count}
// ---------------------------------------------------------------------------
struct HtSlot {
    int32_t  key;
    uint32_t offset;
    uint32_t count;
};
static_assert(sizeof(HtSlot) == 12, "HtSlot must be 12 bytes");

// ---------------------------------------------------------------------------
// mmap helper
// ---------------------------------------------------------------------------
static const void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return p;
}

// ---------------------------------------------------------------------------
// Hash function matching the pre-built index
// ---------------------------------------------------------------------------
static inline uint32_t ht_hash(int32_t key, uint32_t cap) {
    return (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> (64 - 25));
    (void)cap;
}

// Probe the pre-built hash table; returns pointer to slot or nullptr
static inline const HtSlot* ht_lookup(const HtSlot* ht, uint32_t cap,
                                       int32_t key) {
    uint32_t slot = ht_hash(key, cap);
    for (uint32_t probe = 0; probe < cap; probe++) {
        uint32_t s = (slot + probe) & (cap - 1); // cap is power-of-2
        if (ht[s].key == INT32_MIN) return nullptr;
        if (ht[s].key == key) return &ht[s];
    }
    return nullptr;
}

// ---------------------------------------------------------------------------
// Result row
// ---------------------------------------------------------------------------
struct ResultRow {
    char      c_name[26];
    int32_t   c_custkey;
    int32_t   o_orderkey;
    int32_t   o_orderdate;
    double    o_totalprice;
    double    sum_qty;
};

void run_Q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // -----------------------------------------------------------------------
    // Memory-map all needed columns
    // -----------------------------------------------------------------------

    // lineitem
    size_t sz_lq;
    const double*  l_quantity_col = reinterpret_cast<const double*>(
        mmap_file(gendb_dir + "/lineitem/l_quantity.bin", sz_lq));
    {
        int fd = open((gendb_dir + "/lineitem/l_quantity.bin").c_str(), O_RDONLY);
        if (fd >= 0) { posix_fadvise(fd, 0, sz_lq, POSIX_FADV_SEQUENTIAL); close(fd); }
    }

    // orders
    size_t sz_ok, sz_oc, sz_od, sz_op;
    const int32_t* o_orderkey_col   = reinterpret_cast<const int32_t*>(
        mmap_file(gendb_dir + "/orders/o_orderkey.bin", sz_ok));
    const int32_t* o_custkey_col    = reinterpret_cast<const int32_t*>(
        mmap_file(gendb_dir + "/orders/o_custkey.bin", sz_oc));
    const int32_t* o_orderdate_col  = reinterpret_cast<const int32_t*>(
        mmap_file(gendb_dir + "/orders/o_orderdate.bin", sz_od));
    const double*  o_totalprice_col = reinterpret_cast<const double*>(
        mmap_file(gendb_dir + "/orders/o_totalprice.bin", sz_op));
    const size_t num_orders = sz_ok / sizeof(int32_t);

    // customer
    size_t sz_cname;
    const char* c_name_col = reinterpret_cast<const char*>(
        mmap_file(gendb_dir + "/customer/c_name.bin", sz_cname));
    // c_name is fixed char[26] per row; c_custkey is 1-based sequential → row = c_custkey - 1

    // lineitem_orderkey_hash index
    size_t sz_idx;
    const void* idx_raw = mmap_file(gendb_dir + "/indexes/lineitem_orderkey_hash.bin", sz_idx);
    {
        int fd = open((gendb_dir + "/indexes/lineitem_orderkey_hash.bin").c_str(), O_RDONLY);
        if (fd >= 0) { posix_fadvise(fd, 0, sz_idx, POSIX_FADV_SEQUENTIAL); close(fd); }
    }
    const uint32_t* idx_hdr = reinterpret_cast<const uint32_t*>(idx_raw);
    // uint32_t num_unique  = idx_hdr[0];
    uint32_t ht_capacity    = idx_hdr[1];
    // uint32_t num_rows    = idx_hdr[2];
    const HtSlot*   ht        = reinterpret_cast<const HtSlot*>(idx_hdr + 3);
    const uint32_t* positions = reinterpret_cast<const uint32_t*>(ht + ht_capacity);

    // -----------------------------------------------------------------------
    // Phase 1: Scan all hash table slots; collect qualifying orderkeys
    //          (those whose SUM(l_quantity) > 300.0)
    // Two-level software prefetch pipeline to hide double-indirection latency:
    //   slot+32: prefetch positions[] entries (break ptr-chase for l_quantity addr)
    //   slot+16: prefetch l_quantity[] entries (positions now in cache, hides DRAM)
    // -----------------------------------------------------------------------
    std::vector<int32_t> qualifying_vec;
    qualifying_vec.reserve(1024);

    {
        GENDB_PHASE("dim_filter");

        int nthreads = omp_get_max_threads();
        std::vector<std::vector<int32_t>> thread_qualifying(nthreads);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            std::vector<int32_t>& local = thread_qualifying[tid];
            local.reserve(256);

            #pragma omp for schedule(static, 524288)
            for (uint32_t slot = 0; slot < ht_capacity; slot++) {
                // Level-1 prefetch: bring positions[] entries for slot+32 into cache
                // so they're available when we need them to compute l_quantity addresses
                constexpr uint32_t AHEAD1 = 32;
                constexpr uint32_t AHEAD2 = 16;

                if (slot + AHEAD1 < ht_capacity) {
                    const HtSlot& fwd1 = ht[slot + AHEAD1];
                    if (fwd1.key != INT32_MIN) {
                        __builtin_prefetch(&positions[fwd1.offset], 0, 0);
                    }
                }

                // Level-2 prefetch: positions for slot+16 are now in cache;
                // use them to prefetch the l_quantity values we'll actually access
                if (slot + AHEAD2 < ht_capacity) {
                    const HtSlot& fwd2 = ht[slot + AHEAD2];
                    if (fwd2.key != INT32_MIN) {
                        uint32_t fwd2_off = fwd2.offset;
                        uint32_t fwd2_cnt = std::min(fwd2.count, 8u);
                        for (uint32_t j = fwd2_off; j < fwd2_off + fwd2_cnt; j++) {
                            __builtin_prefetch(l_quantity_col + positions[j], 0, 0);
                        }
                    }
                }

                if (ht[slot].key == INT32_MIN) continue;
                double sum_q = 0.0;
                uint32_t off = ht[slot].offset;
                uint32_t cnt = ht[slot].count;
                for (uint32_t i = off; i < off + cnt; i++) {
                    sum_q += l_quantity_col[positions[i]];
                }
                if (sum_q > 300.0) {
                    local.push_back(ht[slot].key);
                }
            }
        }

        // Merge thread-local vectors into qualifying_vec, then sort for binary search
        for (auto& v : thread_qualifying) {
            for (int32_t k : v) qualifying_vec.push_back(k);
        }
        std::sort(qualifying_vec.begin(), qualifying_vec.end());
    }

    // -----------------------------------------------------------------------
    // Phase 2: Scan orders; filter by qualifying_set; collect matching rows
    // -----------------------------------------------------------------------
    std::vector<ResultRow> result_rows;
    result_rows.reserve(1024);

    {
        GENDB_PHASE("main_scan");

        int nthreads = omp_get_max_threads();
        std::vector<std::vector<ResultRow>> thread_rows(nthreads);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            std::vector<ResultRow>& local = thread_rows[tid];
            local.reserve(256);

            #pragma omp for schedule(static, 65536) nowait
            for (size_t i = 0; i < num_orders; i++) {
                int32_t ok = o_orderkey_col[i];
                if (!std::binary_search(qualifying_vec.begin(), qualifying_vec.end(), ok)) continue;

                int32_t custkey  = o_custkey_col[i];
                int32_t odate    = o_orderdate_col[i];
                double  otprice  = o_totalprice_col[i];

                // customer name: c_custkey is 1-based sequential
                const char* cname_ptr = c_name_col + (size_t)(custkey - 1) * 26;

                ResultRow row;
                std::memcpy(row.c_name, cname_ptr, 26);
                row.c_name[25] = '\0';
                row.c_custkey    = custkey;
                row.o_orderkey   = ok;
                row.o_orderdate  = odate;
                row.o_totalprice = otprice;
                row.sum_qty      = 0.0; // filled in next phase

                local.push_back(row);
            }
        }

        // Merge
        for (auto& v : thread_rows) {
            for (auto& r : v) result_rows.push_back(r);
        }
    }

    // -----------------------------------------------------------------------
    // Phase 3: For each qualifying order, compute SUM(l_quantity) via index
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("build_joins");

        for (auto& row : result_rows) {
            const HtSlot* slot = ht_lookup(ht, ht_capacity, row.o_orderkey);
            if (!slot) continue; // shouldn't happen
            double sum_q = 0.0;
            for (uint32_t i = slot->offset; i < slot->offset + slot->count; i++) {
                sum_q += l_quantity_col[positions[i]];
            }
            row.sum_qty = sum_q;
        }
    }

    // -----------------------------------------------------------------------
    // Phase 4: Sort by (o_totalprice DESC, o_orderdate ASC), take top 100
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("sort_topk");

        std::sort(result_rows.begin(), result_rows.end(),
                  [](const ResultRow& a, const ResultRow& b) {
                      if (a.o_totalprice != b.o_totalprice)
                          return a.o_totalprice > b.o_totalprice;
                      return a.o_orderdate < b.o_orderdate;
                  });

        if (result_rows.size() > 100) result_rows.resize(100);
    }

    // -----------------------------------------------------------------------
    // Phase 5: Write CSV output
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q18.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); exit(1); }

        fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        for (const auto& row : result_rows) {
            char date_buf[16];
            gendb::epoch_days_to_date_str(row.o_orderdate, date_buf);
            fprintf(f, "%s,%d,%d,%s,%.2f,%.2f\n",
                    row.c_name,
                    row.c_custkey,
                    row.o_orderkey,
                    date_buf,
                    row.o_totalprice,
                    row.sum_qty);
        }

        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0]
                  << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q18(gendb_dir, results_dir);
    return 0;
}
#endif
