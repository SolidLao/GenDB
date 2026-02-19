#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <atomic>
#include <climits>
#include <iostream>
#include "date_utils.h"
#include "timing_utils.h"

// ---- mmap helper ----
static void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return p;
}

// ---- Zone map entry layout: 24 bytes ----
struct ZoneMapEntry {
    double   min_val;
    double   max_val;
    uint32_t row_count;
    uint32_t _pad;
};

// ---- Bloom filter: 1MB = 8Mbit → ~0.8% FP rate for 630K keys, 3 hash functions ----
// Purpose: pre-filter lineitem l_orderkey BEFORE probing the orders hash map.
// Eliminates ~99% of expensive L3 cache misses for non-matching rows.
struct BloomFilter {
    static constexpr uint32_t NBITS = 1u << 23; // 8,388,608 bits = 1MB
    static constexpr uint32_t MASK  = NBITS - 1;
    uint8_t bits[NBITS / 8];

    BloomFilter() { memset(bits, 0, sizeof(bits)); }

    inline void insert(int32_t key) noexcept {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h        ) & MASK;
        uint32_t h2 = (uint32_t)(h >> 17  ) & MASK;
        uint32_t h3 = (uint32_t)(h >> 34  ) & MASK;
        bits[h1 >> 3] |= (uint8_t)(1u << (h1 & 7));
        bits[h2 >> 3] |= (uint8_t)(1u << (h2 & 7));
        bits[h3 >> 3] |= (uint8_t)(1u << (h3 & 7));
    }

    inline bool maybe_contains(int32_t key) const noexcept {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h        ) & MASK;
        uint32_t h2 = (uint32_t)(h >> 17  ) & MASK;
        uint32_t h3 = (uint32_t)(h >> 34  ) & MASK;
        return (bits[h1 >> 3] & (1u << (h1 & 7))) &&
               (bits[h2 >> 3] & (1u << (h2 & 7))) &&
               (bits[h3 >> 3] & (1u << (h3 & 7)));
    }
};

// ---- Orders hash map: open addressing, key=o_orderkey ----
// dense_id: compact sequential ID (0,1,2,...) assigned at insert time.
// Used to index flat double[] revenue arrays — eliminates aggregation hash overhead.
struct OrdersMap {
    struct Slot {
        int32_t  key;          // INT32_MIN = empty
        int32_t  orderdate;
        int32_t  shippriority;
        uint32_t dense_id;    // sequential ID for flat-array aggregation
    };

    Slot*    slots;
    uint32_t cap;
    uint32_t mask;
    uint32_t next_id;

    explicit OrdersMap(uint32_t cap_) : cap(cap_), mask(cap_ - 1), next_id(0) {
        slots = new Slot[cap_];
        for (uint32_t i = 0; i < cap_; i++) slots[i].key = INT32_MIN;
    }
    ~OrdersMap() { delete[] slots; }

    inline uint32_t hash_fn(int32_t k) const noexcept {
        return (uint32_t)(((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL) >> (64u - 20u)) & mask;
    }

    void insert(int32_t key, int32_t od, int32_t sp) noexcept {
        uint32_t h = hash_fn(key);
        while (slots[h].key != INT32_MIN) h = (h + 1) & mask;
        slots[h].key          = key;
        slots[h].orderdate    = od;
        slots[h].shippriority = sp;
        slots[h].dense_id     = next_id++;
    }

    inline const Slot* find(int32_t key) const noexcept {
        uint32_t h = hash_fn(key);
        while (true) {
            if (slots[h].key == key)       return &slots[h];
            if (slots[h].key == INT32_MIN) return nullptr;
            h = (h + 1) & mask;
        }
    }
};

// ============================================================

void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t  DATE_THRESHOLD = 9204; // epoch days for 1995-03-15
    const uint32_t num_threads    = (uint32_t)std::thread::hardware_concurrency();

    // ---- Phase 1: Build is_building bool array from customer table ----
    // Single-threaded: only 1.5MB of data, parallelism overhead not worth it.
    bool* is_building = nullptr;
    {
        GENDB_PHASE("dim_filter");

        size_t seg_sz, ck_sz;
        const int8_t*  c_mktsegment = reinterpret_cast<const int8_t*>(
            mmap_file(gendb_dir + "/customer/c_mktsegment.bin", seg_sz));
        const int32_t* c_custkey    = reinterpret_cast<const int32_t*>(
            mmap_file(gendb_dir + "/customer/c_custkey.bin", ck_sz));

        size_t n_cust = seg_sz / sizeof(int8_t);
        // Dense domain: c_custkey in [1..1500000]
        is_building = new bool[1500001]();
        for (size_t i = 0; i < n_cust; i++) {
            if (c_mktsegment[i] == 1)   // BUILDING = dict code 1
                is_building[c_custkey[i]] = true;
        }

        munmap((void*)c_mktsegment, seg_sz);
        munmap((void*)c_custkey,    ck_sz);
    }

    // ---- Phase 2: Scan orders; build hash map + bloom filter ----
    // Zone-map prunes ~51% of order blocks (ascending date sort → binary search).
    // Parallel scan into thread-local vectors, sequential insert into hash map.
    // ALSO build bloom filter over qualifying o_orderkey values.
    OrdersMap* orders_map = new OrdersMap(1u << 20); // 1M slots, ~60% load for 630K rows
    BloomFilter* bloom    = new BloomFilter();        // 1MB, built alongside hash map
    uint32_t n_qualifying_orders = 0;
    {
        GENDB_PHASE("build_joins");

        // Load zone map for o_orderdate
        size_t zm_sz;
        const uint8_t* zm_raw = reinterpret_cast<const uint8_t*>(
            mmap_file(gendb_dir + "/orders/indexes/o_orderdate_zonemap.bin", zm_sz));
        uint32_t num_blocks = *reinterpret_cast<const uint32_t*>(zm_raw);
        const ZoneMapEntry* zm = reinterpret_cast<const ZoneMapEntry*>(zm_raw + sizeof(uint32_t));

        // Find last qualifying block (data sorted ascending → stop at first block with min >= threshold)
        uint32_t qualifying_blocks = 0;
        for (uint32_t i = 0; i < num_blocks; i++) {
            if (zm[i].min_val < (double)DATE_THRESHOLD)
                qualifying_blocks = i + 1;
            else
                break;
        }

        size_t od_sz, ok_sz, oc_sz, sp_sz;
        const int32_t* o_orderdate    = reinterpret_cast<const int32_t*>(
            mmap_file(gendb_dir + "/orders/o_orderdate.bin", od_sz));
        const int32_t* o_orderkey     = reinterpret_cast<const int32_t*>(
            mmap_file(gendb_dir + "/orders/o_orderkey.bin", ok_sz));
        const int32_t* o_custkey      = reinterpret_cast<const int32_t*>(
            mmap_file(gendb_dir + "/orders/o_custkey.bin", oc_sz));
        const int32_t* o_shippriority = reinterpret_cast<const int32_t*>(
            mmap_file(gendb_dir + "/orders/o_shippriority.bin", sp_sz));

        madvise((void*)o_orderdate,    od_sz, MADV_SEQUENTIAL);
        madvise((void*)o_orderkey,     ok_sz, MADV_SEQUENTIAL);
        madvise((void*)o_custkey,      oc_sz, MADV_SEQUENTIAL);
        madvise((void*)o_shippriority, sp_sz, MADV_SEQUENTIAL);

        // Parallel morsel scan → thread-local result vectors
        std::vector<std::vector<std::tuple<int32_t,int32_t,int32_t>>> tl_results(num_threads);
        for (auto& v : tl_results) v.reserve(16384);

        std::atomic<uint32_t> blk_idx(0);
        auto worker = [&](uint32_t tid) {
            auto& results = tl_results[tid];
            uint32_t b;
            while ((b = blk_idx.fetch_add(1, std::memory_order_relaxed)) < qualifying_blocks) {
                size_t row_start = (size_t)b * 100000;
                size_t row_end   = row_start + zm[b].row_count;
                for (size_t r = row_start; r < row_end; r++) {
                    if (o_orderdate[r] < DATE_THRESHOLD && is_building[o_custkey[r]])
                        results.emplace_back(o_orderkey[r], o_orderdate[r], o_shippriority[r]);
                }
            }
        };

        {
            std::vector<std::thread> threads;
            threads.reserve(num_threads);
            for (uint32_t t = 0; t < num_threads; t++)
                threads.emplace_back(worker, t);
            for (auto& th : threads) th.join();
        }

        // Sequential merge into hash map + bloom filter (single pass over ~630K entries)
        for (auto& results : tl_results) {
            for (auto& [ok, od, sp] : results) {
                orders_map->insert(ok, od, sp);
                bloom->insert(ok);
            }
        }
        n_qualifying_orders = orders_map->next_id;

        munmap((void*)o_orderdate,    od_sz);
        munmap((void*)o_orderkey,     ok_sz);
        munmap((void*)o_custkey,      oc_sz);
        munmap((void*)o_shippriority, sp_sz);
        munmap((void*)zm_raw,         zm_sz);
    }

    // ---- Phase 3: Scan lineitem; bloom pre-filter + hash-join; flat-array aggregation ----
    // Key optimizations vs naive approach:
    //   1. Bloom filter rejects ~99% of non-matching l_orderkey values cheaply (1MB, L3-resident)
    //      → reduces hash table probes from ~32M to ~3.4M (9x reduction in L3 cache misses)
    //   2. Dense ID flat double[] arrays replace AggMap hash tables
    //      → aggregation becomes simple array[dense_id] += revenue with no hash overhead
    //   3. Prefetching hides memory latency for sequential column access
    std::vector<double*> tl_revenue(num_threads, nullptr);
    for (uint32_t t = 0; t < num_threads; t++)
        tl_revenue[t] = new double[n_qualifying_orders]();  // zero-initialized
    {
        GENDB_PHASE("main_scan");

        // Load l_shipdate zone map
        size_t lzm_sz;
        const uint8_t* lzm_raw = reinterpret_cast<const uint8_t*>(
            mmap_file(gendb_dir + "/lineitem/indexes/l_shipdate_zonemap.bin", lzm_sz));
        uint32_t l_num_blocks = *reinterpret_cast<const uint32_t*>(lzm_raw);
        const ZoneMapEntry* lzm = reinterpret_cast<const ZoneMapEntry*>(lzm_raw + sizeof(uint32_t));

        // Skip leading blocks where max_val <= threshold (data sorted ascending)
        uint32_t first_qual_block = l_num_blocks;
        for (uint32_t i = 0; i < l_num_blocks; i++) {
            if (lzm[i].max_val > (double)DATE_THRESHOLD) {
                first_qual_block = i;
                break;
            }
        }
        uint32_t qualifying_blocks = l_num_blocks - first_qual_block;

        size_t lsd_sz, lok_sz, lep_sz, ld_sz;
        const int32_t* l_shipdate      = reinterpret_cast<const int32_t*>(
            mmap_file(gendb_dir + "/lineitem/l_shipdate.bin", lsd_sz));
        const int32_t* l_orderkey      = reinterpret_cast<const int32_t*>(
            mmap_file(gendb_dir + "/lineitem/l_orderkey.bin", lok_sz));
        const double*  l_extendedprice = reinterpret_cast<const double*>(
            mmap_file(gendb_dir + "/lineitem/l_extendedprice.bin", lep_sz));
        const double*  l_discount      = reinterpret_cast<const double*>(
            mmap_file(gendb_dir + "/lineitem/l_discount.bin", ld_sz));

        madvise((void*)l_shipdate,      lsd_sz, MADV_SEQUENTIAL);
        madvise((void*)l_orderkey,      lok_sz, MADV_SEQUENTIAL);
        madvise((void*)l_extendedprice, lep_sz, MADV_SEQUENTIAL);
        madvise((void*)l_discount,      ld_sz,  MADV_SEQUENTIAL);

        std::atomic<uint32_t> blk_idx(0);
        const OrdersMap*  om = orders_map;  // read-only during parallel phase
        const BloomFilter* bf = bloom;      // read-only, shared across threads

        auto worker = [&](uint32_t tid) {
            double* revenue = tl_revenue[tid];
            uint32_t b_rel;
            while ((b_rel = blk_idx.fetch_add(1, std::memory_order_relaxed)) < qualifying_blocks) {
                uint32_t b = first_qual_block + b_rel;
                size_t row_start = (size_t)b * 100000;
                size_t row_end   = row_start + lzm[b].row_count;

                constexpr int PF = 32; // prefetch distance in rows
                for (size_t r = row_start; r < row_end; r++) {
                    // Prefetch upcoming column data to hide sequential access latency
                    if (r + PF < row_end) {
                        __builtin_prefetch(&l_shipdate[r + PF], 0, 0);
                        __builtin_prefetch(&l_orderkey[r + PF], 0, 0);
                    }

                    if (l_shipdate[r] <= DATE_THRESHOLD) continue;

                    int32_t lok = l_orderkey[r];

                    // Bloom filter: reject ~99.2% of non-matching keys without touching the hash map
                    if (!bf->maybe_contains(lok)) continue;

                    // Full hash table lookup (only ~1% FP + ~10% true matches reach here)
                    const OrdersMap::Slot* s = om->find(lok);
                    if (s) {
                        // Flat array accumulation: no hash, no collision, just array[dense_id] += rev
                        revenue[s->dense_id] += l_extendedprice[r] * (1.0 - l_discount[r]);
                    }
                }
            }
        };

        {
            std::vector<std::thread> threads;
            threads.reserve(num_threads);
            for (uint32_t t = 0; t < num_threads; t++)
                threads.emplace_back(worker, t);
            for (auto& th : threads) th.join();
        }

        munmap((void*)l_shipdate,      lsd_sz);
        munmap((void*)l_orderkey,      lok_sz);
        munmap((void*)l_extendedprice, lep_sz);
        munmap((void*)l_discount,      ld_sz);
        munmap((void*)lzm_raw,         lzm_sz);
    }

    // ---- Phase 4: Parallel merge + top-10 + output ----
    {
        GENDB_PHASE("output");

        // Parallel merge: each thread sums a chunk of global_revenue across all tl_revenue arrays.
        // Access pattern: sequential reads through each tl_revenue[s][lo..hi] → cache-friendly.
        // Speedup vs sequential: ~64x (one thread per chunk).
        std::vector<double> global_revenue(n_qualifying_orders, 0.0);

        if (n_qualifying_orders > 0) {
            std::vector<std::thread> mthreads;
            mthreads.reserve(num_threads);
            uint32_t chunk = (n_qualifying_orders + num_threads - 1) / num_threads;
            for (uint32_t t = 0; t < num_threads; t++) {
                mthreads.emplace_back([&, t]() {
                    uint32_t lo = t * chunk;
                    uint32_t hi = std::min(lo + chunk, n_qualifying_orders);
                    for (uint32_t i = lo; i < hi; i++) {
                        double sum = 0.0;
                        for (uint32_t s = 0; s < num_threads; s++)
                            sum += tl_revenue[s][i];
                        global_revenue[i] = sum;
                    }
                });
            }
            for (auto& th : mthreads) th.join();
        }

        // Free thread-local revenue arrays
        for (uint32_t t = 0; t < num_threads; t++)
            delete[] tl_revenue[t];
        tl_revenue.clear();

        // Collect results: scan orders_map slots to get (orderkey, orderdate, shippriority, revenue)
        struct Row {
            int32_t orderkey;
            int32_t orderdate;
            int32_t shippriority;
            double  revenue;
        };
        std::vector<Row> results;
        results.reserve(n_qualifying_orders);
        for (uint32_t i = 0; i < orders_map->cap; i++) {
            const auto& s = orders_map->slots[i];
            if (s.key != INT32_MIN)
                results.push_back({s.key, s.orderdate, s.shippriority,
                                   global_revenue[s.dense_id]});
        }

        // Top-10: ORDER BY revenue DESC, o_orderdate ASC
        size_t top_k = std::min((size_t)10, results.size());
        std::partial_sort(results.begin(), results.begin() + top_k, results.end(),
            [](const Row& a, const Row& b) {
                if (a.revenue != b.revenue) return a.revenue > b.revenue;
                return a.orderdate < b.orderdate;
            });

        // Write CSV
        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); exit(1); }
        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[12];
        for (size_t i = 0; i < top_k; i++) {
            const auto& r = results[i];
            gendb::epoch_days_to_date_str(r.orderdate, date_buf);
            fprintf(f, "%d,%.2f,%s,%d\n", r.orderkey, r.revenue, date_buf, r.shippriority);
        }
        fclose(f);
    }

    delete[] is_building;
    delete orders_map;
    delete bloom;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q3(gendb_dir, results_dir);
    return 0;
}
#endif
