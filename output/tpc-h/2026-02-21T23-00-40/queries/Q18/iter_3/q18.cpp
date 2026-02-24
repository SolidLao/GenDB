// Q18: Large Volume Customer
// Strategy: 1-pass lineitem agg (thread-local maps) -> merge -> filter heavy keys
//           scan orders probing heavy_keys -> custkey_hash lookup -> top-100

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <string>
#include <vector>
#include <algorithm>
#include <climits>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <omp.h>
#include <iostream>
#include "date_utils.h"
#include "timing_utils.h"

// -------- mmap helpers --------
static void* mmap_seq(const char* path, size_t& out_size) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    posix_fadvise(fd, 0, out_size, POSIX_FADV_SEQUENTIAL);
    madvise(p, out_size, MADV_SEQUENTIAL);
    close(fd);
    return p;
}

static void* mmap_rnd(const char* path, size_t& out_size) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    posix_fadvise(fd, 0, out_size, POSIX_FADV_RANDOM);
    madvise(p, out_size, MADV_RANDOM);
    close(fd);
    return p;
}

// -------- Result tuple --------
struct ResultRow {
    char     c_name[27];   // null-terminated, padded to 27
    int32_t  c_custkey;
    int32_t  o_orderkey;
    int32_t  o_orderdate;
    double   o_totalprice;
    uint32_t sum_qty;      // integer quantity sum (values 1-50, max ~600)
};

// -------- custkey_hash slot --------
struct CustSlot {
    int32_t  key;
    uint32_t row_idx;
};

// -------- Compact open-addr hash map: int32_t key -> uint32_t val --------
// Uses Fibonacci hashing, linear probing, EMPTY sentinel = INT32_MIN
static constexpr int32_t EMPTY_KEY = INT32_MIN;

struct HashMap {
    int32_t*  keys;
    uint32_t* vals;
    uint32_t  cap;
    uint32_t  mask;

    void init(uint32_t capacity) {
        // capacity must be power-of-2
        cap  = capacity;
        mask = capacity - 1u;
        keys = new int32_t[capacity];
        vals = new uint32_t[capacity];
        std::fill(keys, keys + capacity, EMPTY_KEY);
        memset(vals, 0, capacity * sizeof(uint32_t));
    }

    void destroy() { delete[] keys; delete[] vals; keys = nullptr; vals = nullptr; }

    inline void add(int32_t key, uint32_t delta) {
        uint32_t h = ((uint32_t)key * 2654435761u) & mask;
        for (uint32_t probe = 0; probe <= mask; ++probe) {
            uint32_t slot = (h + probe) & mask;
            if (keys[slot] == key)      { vals[slot] += delta; return; }
            if (keys[slot] == EMPTY_KEY){ keys[slot] = key; vals[slot] = delta; return; }
        }
    }

    // Returns 0 if key not found (all valid sums are > 300, so 0 is safe sentinel)
    inline uint32_t get(int32_t key) const {
        uint32_t h = ((uint32_t)key * 2654435761u) & mask;
        for (uint32_t probe = 0; probe <= mask; ++probe) {
            uint32_t slot = (h + probe) & mask;
            if (keys[slot] == key)       return vals[slot];
            if (keys[slot] == EMPTY_KEY) return 0;
        }
        return 0;
    }
};

// -------- Main query function --------
void run_Q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string base = gendb_dir + "/";

    // Column paths
    const std::string p_lok  = base + "lineitem/l_orderkey.bin";
    const std::string p_lqty = base + "lineitem/l_quantity.bin";
    const std::string p_ook  = base + "orders/o_orderkey.bin";
    const std::string p_ock  = base + "orders/o_custkey.bin";
    const std::string p_ood  = base + "orders/o_orderdate.bin";
    const std::string p_otp  = base + "orders/o_totalprice.bin";
    const std::string p_cn   = base + "customer/c_name.bin";
    const std::string p_ch   = base + "customer/indexes/custkey_hash.bin";

    // Mmap'd pointers
    const int32_t* l_orderkey   = nullptr;
    const uint8_t* l_quantity   = nullptr;
    const int32_t* o_orderkey   = nullptr;
    const int32_t* o_custkey    = nullptr;
    const int32_t* o_orderdate  = nullptr;
    const double*  o_totalprice = nullptr;
    const char*    c_name_data  = nullptr;
    const uint32_t* cust_hash_raw = nullptr;

    size_t n_lineitem = 0, n_orders = 0;

    // ---- Phase 0: Data loading (parallel madvise prefetch) ----
    {
        GENDB_PHASE("data_loading");

        size_t sz0, sz1, sz2, sz3, sz4, sz5, sz6, sz7;

        // Issue prefetch in parallel across columns
        #pragma omp parallel sections num_threads(4)
        {
            #pragma omp section
            {
                l_orderkey = (const int32_t*)mmap_seq(p_lok.c_str(), sz0);
                madvise((void*)l_orderkey, sz0, MADV_WILLNEED);
            }
            #pragma omp section
            {
                l_quantity = (const uint8_t*)mmap_seq(p_lqty.c_str(), sz1);
                madvise((void*)l_quantity, sz1, MADV_WILLNEED);
                o_orderkey = (const int32_t*)mmap_seq(p_ook.c_str(), sz2);
                madvise((void*)o_orderkey, sz2, MADV_WILLNEED);
            }
            #pragma omp section
            {
                o_custkey   = (const int32_t*)mmap_seq(p_ock.c_str(), sz3);
                o_orderdate = (const int32_t*)mmap_seq(p_ood.c_str(), sz4);
                o_totalprice= (const double*) mmap_seq(p_otp.c_str(), sz5);
                madvise((void*)o_custkey,   sz3, MADV_WILLNEED);
                madvise((void*)o_orderdate, sz4, MADV_WILLNEED);
                madvise((void*)o_totalprice,sz5, MADV_WILLNEED);
            }
            #pragma omp section
            {
                c_name_data   = (const char*)    mmap_rnd(p_cn.c_str(), sz6);
                cust_hash_raw = (const uint32_t*)mmap_rnd(p_ch.c_str(), sz7);
                madvise((void*)c_name_data,   sz6, MADV_WILLNEED);
                madvise((void*)cust_hash_raw, sz7, MADV_WILLNEED);
            }
        }

        n_lineitem = sz0 / sizeof(int32_t);
        n_orders   = sz2 / sizeof(int32_t);
    }

    // Decode custkey_hash index header
    const uint32_t  cust_cap   = cust_hash_raw[0];
    const uint32_t  cust_mask  = cust_cap - 1u;
    const CustSlot* cust_slots = (const CustSlot*)(cust_hash_raw + 1);

    // ---- Phase 1: Parallel lineitem scan -> thread-local hash aggregation ----
    // Each thread accumulates SUM(l_quantity) per l_orderkey in a thread-local map
    // Local cap = 1M entries; ~15M unique orderkeys / 64 threads ≈ 234K avg unique/thread
    static constexpr int     NTHREADS  = 64;
    static constexpr uint32_t LOCAL_CAP = 1u << 20; // 1048576
    static constexpr uint32_t LOCAL_MASK= LOCAL_CAP - 1u;

    // Heap-allocate thread-local maps to avoid stack overflow
    struct TLMap { int32_t* keys; uint32_t* vals; };
    TLMap tl[NTHREADS];

    // Scatter buffers: part_buf[src_thread][dst_partition]
    // After phase 1, each thread scatters its TL map entries here (partitioned by Fibonacci hash top-6 bits).
    // Phase 2 reads part_buf[*][pid] for each merge thread pid — tiny vs reading all TL maps.
    std::vector<std::pair<int32_t,uint32_t>> part_buf[NTHREADS][NTHREADS];

    {
        GENDB_PHASE("dim_filter");  // phase 1: lineitem aggregate

        #pragma omp parallel num_threads(NTHREADS)
        {
            int tid = omp_get_thread_num();
            // Init this thread's local map in parallel (avoids 512MB sequential zero-fill)
            tl[tid].keys = new int32_t[LOCAL_CAP];
            tl[tid].vals = new uint32_t[LOCAL_CAP];
            std::fill(tl[tid].keys, tl[tid].keys + LOCAL_CAP, EMPTY_KEY);
            memset(tl[tid].vals, 0, LOCAL_CAP * sizeof(uint32_t));
            int32_t*  lkeys = tl[tid].keys;
            uint32_t* lvals = tl[tid].vals;

            size_t start = (size_t)tid       * n_lineitem / NTHREADS;
            size_t end   = (size_t)(tid + 1) * n_lineitem / NTHREADS;

            for (size_t i = start; i < end; ++i) {
                int32_t  ok  = l_orderkey[i];
                uint32_t qty = l_quantity[i];  // byte-packed: code == integer qty (1-50)

                uint32_t h = ((uint32_t)ok * 2654435761u) & LOCAL_MASK;
                for (uint32_t probe = 0; probe <= LOCAL_MASK; ++probe) {
                    uint32_t slot = (h + probe) & LOCAL_MASK;
                    if (lkeys[slot] == ok)       { lvals[slot] += qty; break; }
                    if (lkeys[slot] == EMPTY_KEY){ lkeys[slot] = ok; lvals[slot] = qty; break; }
                }
            }

            // Scatter this thread's TL map entries to 64 partition buffers.
            // Each entry goes to partition p = top-6 bits of Fibonacci hash.
            // Sequential scan of our own TL map (hardware prefetcher friendly).
            // part_buf[tid][p] is only written by thread tid → no contention.
            for (uint32_t i = 0; i < LOCAL_CAP; ++i) {
                int32_t k = lkeys[i];
                if (k == EMPTY_KEY) continue;
                uint32_t p = ((uint32_t)k * 2654435761u) >> 26;  // top 6 bits → 64 partitions
                part_buf[tid][p].emplace_back(k, lvals[i]);
            }

            // Free TL map immediately — no longer needed, reduces peak memory pressure.
            delete[] tl[tid].keys; tl[tid].keys = nullptr;
            delete[] tl[tid].vals; tl[tid].vals = nullptr;
        }
    }

    // ---- Phase 2: Scatter-merge -> partition maps -> filter heavy_keys ----
    // Each merge thread pid reads only part_buf[0..63][pid]: ~2.4MB total vs 512MB TL maps.
    // This reduces effective DRAM reads from ~16GB-equivalent to ~154MB (100x less traffic).
    HashMap heavy_map;

    {
        GENDB_PHASE("build_joins");

        static constexpr uint32_t PART_CAP  = 1u << 19; // 512K slots (~234K distinct keys → 46% load)
        static constexpr uint32_t PART_MASK = PART_CAP - 1u;

        std::vector<std::pair<int32_t,uint32_t>> local_heavy[NTHREADS];

        #pragma omp parallel num_threads(NTHREADS)
        {
            int pid = omp_get_thread_num();
            int32_t*  pm_keys = new int32_t[PART_CAP];
            uint32_t* pm_vals = new uint32_t[PART_CAP];
            std::fill(pm_keys, pm_keys + PART_CAP, EMPTY_KEY);
            memset(pm_vals, 0, PART_CAP * sizeof(uint32_t));

            // Read only this partition's scatter data from all source threads.
            // Total data per merge thread: ~2.4MB (vs 256MB in old approach).
            // Partition map (4MB) has high cache reuse: 234K insertions into 512K slots.
            for (int t = 0; t < NTHREADS; ++t) {
                for (const auto& [k, v] : part_buf[t][pid]) {
                    uint32_t h = ((uint32_t)k * 2654435761u) & PART_MASK;
                    for (uint32_t probe = 0; probe <= PART_MASK; ++probe) {
                        uint32_t slot = (h + probe) & PART_MASK;
                        if (pm_keys[slot] == k)        { pm_vals[slot] += v; break; }
                        if (pm_keys[slot] == EMPTY_KEY){ pm_keys[slot] = k; pm_vals[slot] = v; break; }
                    }
                }
            }

            // Filter heavy keys (sum_qty > 300)
            for (uint32_t i = 0; i < PART_CAP; ++i) {
                if (pm_keys[i] != EMPTY_KEY && pm_vals[i] > 300u) {
                    local_heavy[pid].emplace_back(pm_keys[i], pm_vals[i]);
                }
            }
            delete[] pm_keys;
            delete[] pm_vals;
        }

        // Build compact heavy_map from ~3K qualifying orderkeys (trivially fast)
        heavy_map.init(1u << 14);
        for (int t = 0; t < NTHREADS; ++t) {
            for (auto& [k, v] : local_heavy[t]) {
                heavy_map.add(k, v);
            }
        }
    }

    // Free scatter buffers before main_scan
    for (int t = 0; t < NTHREADS; ++t)
        for (int p = 0; p < NTHREADS; ++p)
            { std::vector<std::pair<int32_t,uint32_t>> tmp; part_buf[t][p].swap(tmp); }

    // ---- Phase 3: Scan orders, probe heavy_keys, lookup customer ----
    std::vector<ResultRow> results;
    results.reserve(4096);

    {
        GENDB_PHASE("main_scan");

        std::vector<std::vector<ResultRow>> thread_res(NTHREADS);
        for (auto& v : thread_res) v.reserve(128);

        #pragma omp parallel num_threads(NTHREADS)
        {
            int tid = omp_get_thread_num();
            auto& local = thread_res[tid];

            size_t start = (size_t)tid       * n_orders / NTHREADS;
            size_t end   = (size_t)(tid + 1) * n_orders / NTHREADS;

            for (size_t i = start; i < end; ++i) {
                int32_t ok = o_orderkey[i];
                uint32_t sum_qty = heavy_map.get(ok);
                if (sum_qty == 0u) continue;  // not a heavy key

                int32_t  ck = o_custkey[i];
                int32_t  od = o_orderdate[i];
                double   tp = o_totalprice[i];

                // Probe custkey_hash to find customer row index
                uint32_t ch  = ((uint32_t)ck * 2654435761u) & cust_mask;
                uint32_t row_idx = UINT32_MAX;
                for (uint32_t probe = 0; probe <= cust_mask; ++probe) {
                    uint32_t slot = (ch + probe) & cust_mask;
                    if (cust_slots[slot].key == ck)       { row_idx = cust_slots[slot].row_idx; break; }
                    if (cust_slots[slot].key == INT32_MIN) break;
                }
                if (row_idx == UINT32_MAX) continue; // shouldn't happen

                ResultRow row;
                memcpy(row.c_name, c_name_data + (size_t)row_idx * 26, 26);
                row.c_name[26] = '\0';
                row.c_custkey   = ck;
                row.o_orderkey  = ok;
                row.o_orderdate = od;
                row.o_totalprice= tp;
                row.sum_qty     = sum_qty;
                local.push_back(row);
            }
        }

        // Merge thread-local results
        for (auto& v : thread_res) {
            for (auto& r : v) results.push_back(r);
        }
    }

    // ---- Phase 4: Sort, top-100, output ----
    {
        GENDB_PHASE("output");

        // Sort: o_totalprice DESC, o_orderdate ASC
        std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
            return a.o_orderdate < b.o_orderdate;
        });

        size_t out_count = std::min(results.size(), (size_t)100);

        std::string out_path = results_dir + "/Q18.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror("fopen output"); exit(1); }

        fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        char date_buf[16];
        for (size_t i = 0; i < out_count; ++i) {
            const ResultRow& r = results[i];
            gendb::epoch_days_to_date_str(r.o_orderdate, date_buf);
            fprintf(f, "%s,%d,%d,%s,%.2f,%.2f\n",
                    r.c_name,
                    r.c_custkey,
                    r.o_orderkey,
                    date_buf,
                    r.o_totalprice,
                    (double)r.sum_qty);
        }

        fclose(f);
    }

    heavy_map.destroy();
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q18(gendb_dir, results_dir);
    return 0;
}
#endif
