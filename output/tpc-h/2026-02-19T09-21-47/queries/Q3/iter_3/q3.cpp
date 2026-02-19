#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <atomic>
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

// ---- Cache-line-friendly Bloom Filter ----
// 1MB, ~0.82% FP rate for 630K entries.
// All 3 bit-probes land in the SAME 64-byte cache line → single L3 access per check.
struct BloomFilter {
    struct alignas(64) Block { uint64_t words[8]; }; // 64 bytes = 512 bits

    static const uint32_t NUM_BLOCKS = 1u << 14; // 16384 × 64 bytes = 1MB
    static const uint32_t BLOCK_MASK = NUM_BLOCKS - 1;

    Block* blocks; // heap-allocated via calloc for zero-init

    BloomFilter() {
        blocks = static_cast<Block*>(calloc(NUM_BLOCKS, sizeof(Block)));
    }
    ~BloomFilter() { free(blocks); }

    void add(int32_t key) noexcept {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t blk = (uint32_t)(h >> 50) & BLOCK_MASK; // top 14 bits → block index
        Block& b = blocks[blk];
        // 3 probes within the same 512-bit block (independent bit ranges of h)
        b.words[(h >>  9) & 7] |= 1ULL << ((h >>  3) & 63);
        b.words[(h >> 18) & 7] |= 1ULL << ((h >> 22) & 63);
        b.words[(h >> 30) & 7] |= 1ULL << ((h >> 37) & 63);
    }

    bool may_contain(int32_t key) const noexcept {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t blk = (uint32_t)(h >> 50) & BLOCK_MASK;
        const Block& b = blocks[blk];
        return (b.words[(h >>  9) & 7] >> ((h >>  3) & 63) & 1) &&
               (b.words[(h >> 18) & 7] >> ((h >> 22) & 63) & 1) &&
               (b.words[(h >> 30) & 7] >> ((h >> 37) & 63) & 1);
    }
};

// ---- Orders hash map: open addressing, sentinel key=0 (TPC-H orderkeys >= 1) ----
// Uses calloc → OS provides zero pages lazily; no explicit memset loop needed.
struct OrdersMap {
    struct Slot {
        int32_t key;         // 0 = empty (valid since o_orderkey >= 1)
        int32_t orderdate;
        int32_t shippriority;
        int32_t _pad;
    };

    Slot*    slots;
    uint32_t cap;
    uint32_t mask;

    OrdersMap(uint32_t cap_) : cap(cap_), mask(cap_ - 1) {
        slots = static_cast<Slot*>(calloc(cap_, sizeof(Slot))); // zero-init: key=0 = empty
    }
    ~OrdersMap() { free(slots); }

    // Fibonacci hash, top 32 bits → mask to capacity (works for any power-of-2 cap)
    inline uint32_t hash_fn(int32_t k) const noexcept {
        return (uint32_t)(((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
    }

    void insert(int32_t key, int32_t od, int32_t sp) noexcept {
        uint32_t h = hash_fn(key);
        while (slots[h].key != 0) h = (h + 1) & mask;
        slots[h].key = key;
        slots[h].orderdate = od;
        slots[h].shippriority = sp;
    }

    const Slot* find(int32_t key) const noexcept {
        uint32_t h = hash_fn(key);
        while (true) {
            if (slots[h].key == key) return &slots[h];
            if (slots[h].key == 0)   return nullptr;
            h = (h + 1) & mask;
        }
    }
};

// ---- Aggregation hash map with filled_indices for O(sz) merge ----
// Sentinel key=0 + calloc → lazy OS pages. filled_indices tracks non-empty slots
// so merge_into() scans only the ~sz filled entries instead of all cap slots.
struct AggMap {
    struct Slot {
        int32_t orderkey;    // 0 = empty
        int32_t orderdate;
        int32_t shippriority;
        int32_t _pad;
        double  revenue;
    };

    Slot*     slots;
    uint32_t* filled_indices; // [0..sz-1] = indices of occupied slots
    uint32_t  cap;
    uint32_t  mask;
    uint32_t  sz;

    AggMap(uint32_t cap_) : cap(cap_), mask(cap_ - 1), sz(0) {
        slots          = static_cast<Slot*>(calloc(cap_, sizeof(Slot)));
        filled_indices = new uint32_t[cap_]; // written before read; no init needed
    }
    ~AggMap() { free(slots); delete[] filled_indices; }

    // Fibonacci hash, top 32 bits → uniform for any power-of-2 capacity
    inline uint32_t hash_fn(int32_t k) const noexcept {
        return (uint32_t)(((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
    }

    void accumulate(int32_t ok, int32_t od, int32_t sp, double rev) noexcept {
        uint32_t h = hash_fn(ok);
        while (true) {
            if (slots[h].orderkey == 0) {
                // New group: insert and track index
                slots[h].orderkey     = ok;
                slots[h].orderdate    = od;
                slots[h].shippriority = sp;
                slots[h].revenue      = rev;
                filled_indices[sz++]  = h;
                return;
            }
            if (slots[h].orderkey == ok) {
                slots[h].revenue += rev;
                return;
            }
            h = (h + 1) & mask;
        }
    }

    // O(sz) merge: iterate only filled entries, not all cap slots
    void merge_into(AggMap& dest) const noexcept {
        for (uint32_t i = 0; i < sz; i++) {
            const Slot& s = slots[filled_indices[i]];
            dest.accumulate(s.orderkey, s.orderdate, s.shippriority, s.revenue);
        }
    }
};

// ============================================================

void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t DATE_THRESHOLD = 9204; // epoch days for 1995-03-15

    const uint32_t num_threads = (uint32_t)std::thread::hardware_concurrency();

    // ---- Phase 1: Build is_building bool array from customer table ----
    bool* is_building = nullptr;
    {
        GENDB_PHASE("dim_filter");

        size_t seg_sz, ck_sz;
        const int8_t*  c_mktsegment = reinterpret_cast<const int8_t*>(
            mmap_file(gendb_dir + "/customer/c_mktsegment.bin", seg_sz));
        const int32_t* c_custkey    = reinterpret_cast<const int32_t*>(
            mmap_file(gendb_dir + "/customer/c_custkey.bin", ck_sz));

        size_t n_cust = seg_sz / sizeof(int8_t);

        // Dense domain: c_custkey in [1..1500000]; use calloc for OS lazy pages
        is_building = static_cast<bool*>(calloc(1500001, sizeof(bool)));
        for (size_t i = 0; i < n_cust; i++) {
            if (c_mktsegment[i] == 1) {  // BUILDING = dict code 1
                is_building[c_custkey[i]] = true;
            }
        }

        munmap((void*)c_mktsegment, seg_sz);
        munmap((void*)c_custkey, ck_sz);
    }

    // ---- Phase 2: Scan orders with zone-map pruning; build orders hash map + bloom filter ----
    // OrdersMap: 1M slots for ~630K rows (60% load factor)
    // BloomFilter: 1MB, ~0.82% FP rate — built alongside OrdersMap
    OrdersMap*  orders_map = new OrdersMap(1u << 20);
    BloomFilter* bloom     = new BloomFilter();
    {
        GENDB_PHASE("build_joins");

        // Load zone map
        size_t zm_sz;
        const uint8_t* zm_raw = reinterpret_cast<const uint8_t*>(
            mmap_file(gendb_dir + "/orders/indexes/o_orderdate_zonemap.bin", zm_sz));
        uint32_t num_blocks = *reinterpret_cast<const uint32_t*>(zm_raw);
        const ZoneMapEntry* zm = reinterpret_cast<const ZoneMapEntry*>(zm_raw + sizeof(uint32_t));

        // Find last qualifying block (ascending sort → break on first min >= threshold)
        uint32_t qualifying_blocks = 0;
        for (uint32_t i = 0; i < num_blocks; i++) {
            if (zm[i].min_val < (double)DATE_THRESHOLD)
                qualifying_blocks = i + 1;
            else
                break;
        }

        // mmap order columns
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
                    if (o_orderdate[r] < DATE_THRESHOLD && is_building[o_custkey[r]]) {
                        results.emplace_back(o_orderkey[r], o_orderdate[r], o_shippriority[r]);
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

        // Sequential merge into hash map + simultaneously populate bloom filter
        for (auto& results : tl_results) {
            for (auto& [ok, od, sp] : results) {
                orders_map->insert(ok, od, sp);
                bloom->add(ok);
            }
        }

        munmap((void*)o_orderdate,    od_sz);
        munmap((void*)o_orderkey,     ok_sz);
        munmap((void*)o_custkey,      oc_sz);
        munmap((void*)o_shippriority, sp_sz);
        munmap((void*)zm_raw,         zm_sz);
    }

    // ---- Phase 3: Scan lineitem with zone-map pruning; bloom filter + join + aggregate ----
    // Bloom filter rejects ~99% of non-matching orderkeys in O(1) L3 access.
    // AggMap uses filled_indices for fast O(sz) merge in Phase 4.
    std::vector<AggMap*> tl_agg(num_threads);
    for (uint32_t t = 0; t < num_threads; t++)
        tl_agg[t] = new AggMap(1u << 18); // 262144 slots per thread; calloc gives lazy OS pages
    {
        GENDB_PHASE("main_scan");

        // Load l_shipdate zone map
        size_t lzm_sz;
        const uint8_t* lzm_raw = reinterpret_cast<const uint8_t*>(
            mmap_file(gendb_dir + "/lineitem/indexes/l_shipdate_zonemap.bin", lzm_sz));
        uint32_t l_num_blocks = *reinterpret_cast<const uint32_t*>(lzm_raw);
        const ZoneMapEntry* lzm = reinterpret_cast<const ZoneMapEntry*>(lzm_raw + sizeof(uint32_t));

        // Skip blocks where max_val <= threshold (fully before cutoff date)
        uint32_t first_qual_block = l_num_blocks;
        for (uint32_t i = 0; i < l_num_blocks; i++) {
            if (lzm[i].max_val > (double)DATE_THRESHOLD) {
                first_qual_block = i;
                break;
            }
        }

        uint32_t qualifying_blocks = l_num_blocks - first_qual_block;

        // mmap lineitem columns
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

        auto worker = [&](uint32_t tid) {
            AggMap* agg = tl_agg[tid];
            uint32_t b_rel;
            while ((b_rel = blk_idx.fetch_add(1, std::memory_order_relaxed)) < qualifying_blocks) {
                uint32_t b = first_qual_block + b_rel;
                size_t row_start = (size_t)b * 100000;
                size_t row_end   = row_start + lzm[b].row_count;
                for (size_t r = row_start; r < row_end; r++) {
                    if (l_shipdate[r] > DATE_THRESHOLD) {
                        int32_t ok = l_orderkey[r];
                        // Bloom filter: single cache-line probe, rejects ~99% non-matching rows
                        // before touching the larger 10MB OrdersMap
                        if (bloom->may_contain(ok)) {
                            const OrdersMap::Slot* s = orders_map->find(ok);
                            if (s) {
                                double rev = l_extendedprice[r] * (1.0 - l_discount[r]);
                                agg->accumulate(ok, s->orderdate, s->shippriority, rev);
                            }
                        }
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

    // ---- Phase 4: Merge aggregates (O(sz) via filled_indices), top-10, output ----
    {
        GENDB_PHASE("output");

        // Merge thread-local maps into global using filled_indices:
        // Only iterates filled entries (~150K total) vs. previous O(16.7M) full-cap scan
        AggMap* global = new AggMap(1u << 18); // 262K slots for ~150K groups
        for (uint32_t t = 0; t < num_threads; t++) {
            tl_agg[t]->merge_into(*global); // O(tl_agg[t]->sz) not O(cap)
            delete tl_agg[t];
        }
        tl_agg.clear();

        // Collect results using filled_indices (avoids scanning empty slots)
        struct Row {
            int32_t orderkey;
            int32_t orderdate;
            int32_t shippriority;
            double  revenue;
        };

        std::vector<Row> results;
        results.reserve(global->sz);
        for (uint32_t i = 0; i < global->sz; i++) {
            const auto& s = global->slots[global->filled_indices[i]];
            results.push_back({s.orderkey, s.orderdate, s.shippriority, s.revenue});
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

        delete global;
    }

    free(is_building);
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
