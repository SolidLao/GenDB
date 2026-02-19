// Q3: Shipping Priority — GenDB iteration 0
// Strategy: customer bitset → orders zone-map scan + hash build → lineitem zone-map parallel scan + probe + aggregation → top-10
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Aggregation key and value
// ---------------------------------------------------------------------------
struct AggKey {
    int32_t orderkey;
    int32_t orderdate;
    int32_t shippriority;
};

struct AggValue {
    int64_t revenue_sum; // scaled by 100*100 = 10000
};

// ---------------------------------------------------------------------------
// Orders hash map entry
// ---------------------------------------------------------------------------
struct OrdersEntry {
    int32_t orderkey;    // key; 0 = empty
    int32_t orderdate;
    int32_t shippriority;
};

// Open-addressing hash map for orders join
// Capacity must be power of 2.
struct OrdersHashMap {
    static constexpr uint32_t EMPTY = 0;
    uint32_t capacity;
    uint32_t mask;
    std::vector<OrdersEntry> slots;

    explicit OrdersHashMap(uint32_t cap) : capacity(cap), mask(cap - 1), slots(cap, {0, 0, 0}) {}

    // Insert — orderkey is always > 0 for TPC-H
    void insert(int32_t okey, int32_t odate, int32_t oprio) {
        uint32_t h = gendb::hash_int((uint32_t)okey) & mask;
        while (slots[h].orderkey != EMPTY) {
            h = (h + 1) & mask;
        }
        slots[h] = {okey, odate, oprio};
    }

    // Returns pointer to entry or nullptr
    const OrdersEntry* find(int32_t okey) const {
        uint32_t h = gendb::hash_int((uint32_t)okey) & mask;
        while (true) {
            const OrdersEntry& e = slots[h];
            if (e.orderkey == EMPTY) return nullptr;
            if (e.orderkey == okey) return &e;
            h = (h + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------------------
// Aggregation hash map: key=(orderkey,orderdate,shippriority), val=revenue_sum
// Open addressing, capacity power of 2.
// ---------------------------------------------------------------------------
struct AggHashMap {
    struct Slot {
        int32_t orderkey;   // 0 = empty
        int32_t orderdate;
        int32_t shippriority;
        int64_t revenue;    // scaled x10000
    };

    uint32_t capacity;
    uint32_t mask;
    std::vector<Slot> slots;

    explicit AggHashMap(uint32_t cap) : capacity(cap), mask(cap - 1), slots(cap, {0, 0, 0, 0}) {}

    void update(int32_t okey, int32_t odate, int32_t oprio, int64_t rev) {
        uint32_t h = (gendb::hash_int((uint32_t)okey) ^ gendb::hash_int((uint32_t)odate)) & mask;
        while (true) {
            Slot& s = slots[h];
            if (s.orderkey == 0) {
                s.orderkey = okey;
                s.orderdate = odate;
                s.shippriority = oprio;
                s.revenue = rev;
                return;
            }
            if (s.orderkey == okey && s.orderdate == odate && s.shippriority == oprio) {
                s.revenue += rev;
                return;
            }
            h = (h + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------------------
// Zone map entry as described in the storage guide
// Layout: [uint32_t num_zones] then [min:int32_t, max:int32_t, count:uint32_t] per zone
// ---------------------------------------------------------------------------
struct ZoneEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t count;
};

// ---------------------------------------------------------------------------
// Main query runner
// ---------------------------------------------------------------------------
void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // Date threshold: 1995-03-15
    const int32_t DATE_THRESHOLD = gendb::date_str_to_epoch_days("1995-03-15");

    // -------------------------------------------------------------------------
    // Phase 1: Customer filter → bitset
    // -------------------------------------------------------------------------
    // Bitset for custkeys that match c_mktsegment = 'BUILDING'
    // custkey range [1, 1500000]
    constexpr int32_t MAX_CUSTKEY = 1500001;
    // Use 64-bit words: ceil(1500001/64) words
    constexpr int32_t BITSET_WORDS = (MAX_CUSTKEY + 63) / 64;
    std::vector<uint64_t> cust_bitset(BITSET_WORDS, 0ULL);

    int building_code = -1;
    {
        GENDB_PHASE("dim_filter");

        // Load c_mktsegment dictionary
        {
            std::string dict_path = gendb_dir + "/customer/c_mktsegment_dict.txt";
            std::ifstream dict_file(dict_path);
            if (!dict_file.is_open()) {
                std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
                return;
            }
            std::string word;
            int code = 0;
            while (std::getline(dict_file, word)) {
                if (word == "BUILDING") {
                    building_code = code;
                    break;
                }
                code++;
            }
        }

        if (building_code < 0) {
            std::cerr << "BUILDING not found in dictionary" << std::endl;
            return;
        }

        gendb::MmapColumn<int32_t> cust_key(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> cust_seg(gendb_dir + "/customer/c_mktsegment.bin");

        const size_t nrows = cust_key.size();
        const int32_t* keys = cust_key.data();
        const int32_t* segs = cust_seg.data();

        // Parallel scan — each thread sets bits into shared bitset (idempotent set, no CAS needed)
        const int nthreads = (int)std::thread::hardware_concurrency();
        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        std::atomic<size_t> cursor{0};
        constexpr size_t MORSEL = 65536;

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&]() {
                while (true) {
                    size_t start = cursor.fetch_add(MORSEL, std::memory_order_relaxed);
                    if (start >= nrows) break;
                    size_t end = std::min(start + MORSEL, nrows);
                    for (size_t i = start; i < end; i++) {
                        if (segs[i] == building_code) {
                            int32_t k = keys[i];
                            // Atomic OR into bitset word (words may be shared across threads)
                            uint32_t word_idx = (uint32_t)k >> 6;
                            uint64_t bit = 1ULL << ((uint32_t)k & 63);
                            __atomic_fetch_or(&cust_bitset[word_idx], bit, __ATOMIC_RELAXED);
                        }
                    }
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    // -------------------------------------------------------------------------
    // Phase 2: Load orders zone map, scan orders, build hash map
    // -------------------------------------------------------------------------
    // orders is sorted by o_orderdate → zone map is effective
    // predicate: o_orderdate < DATE_THRESHOLD AND o_custkey in cust_bitset

    // Capacity = next power of 2 >= 2 * 1,500,000 (estimated 1.5M qualifying rows, load factor 0.5)
    constexpr uint32_t ORDERS_CAP = 1 << 22; // 4194304
    OrdersHashMap orders_map(ORDERS_CAP);

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> ord_key(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> ord_cust(gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> ord_date(gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> ord_prio(gendb_dir + "/orders/o_shippriority.bin");

        const int32_t* okeys  = ord_key.data();
        const int32_t* ocusts = ord_cust.data();
        const int32_t* odates = ord_date.data();
        const int32_t* oprios = ord_prio.data();
        const size_t nrows = ord_key.size();

        // Load zone map
        std::string zm_path = gendb_dir + "/indexes/idx_orders_orderdate.bin";
        int zm_fd = open(zm_path.c_str(), O_RDONLY);
        bool use_zonemap = (zm_fd >= 0);
        uint32_t num_zones = 0;
        const ZoneEntry* zones = nullptr;
        void* zm_map = nullptr;
        size_t zm_size = 0;

        if (use_zonemap) {
            struct stat st;
            fstat(zm_fd, &st);
            zm_size = (size_t)st.st_size;
            zm_map = mmap(nullptr, zm_size, PROT_READ, MAP_PRIVATE, zm_fd, 0);
            close(zm_fd);
            if (zm_map == MAP_FAILED) { use_zonemap = false; }
            else {
                // Header: uint32_t num_zones
                num_zones = *(const uint32_t*)zm_map;
                zones = (const ZoneEntry*)((const char*)zm_map + sizeof(uint32_t));
            }
        }

        // Process orders with zone map pruning
        // block_size = 100000 rows (from storage guide)
        constexpr size_t BLOCK_SIZE = 100000;

        auto process_block = [&](size_t start, size_t end) {
            for (size_t i = start; i < end; i++) {
                if (odates[i] >= DATE_THRESHOLD) continue;
                int32_t ck = ocusts[i];
                if (ck < 0 || ck >= MAX_CUSTKEY) continue;
                uint32_t word_idx = (uint32_t)ck >> 6;
                uint64_t bit = 1ULL << ((uint32_t)ck & 63);
                if (!(cust_bitset[word_idx] & bit)) continue;
                orders_map.insert(okeys[i], odates[i], oprios[i]);
            }
        };

        if (use_zonemap && zones) {
            for (uint32_t z = 0; z < num_zones; z++) {
                // Skip zone if all dates >= DATE_THRESHOLD (min >= threshold means skip)
                if (zones[z].min_val >= DATE_THRESHOLD) continue;
                // Compute row range for this zone
                size_t zone_start = (size_t)z * BLOCK_SIZE;
                size_t zone_end = std::min(zone_start + (size_t)zones[z].count, nrows);
                if (zones[z].max_val < DATE_THRESHOLD) {
                    // All rows in block satisfy date predicate — only check custkey bitset
                    for (size_t i = zone_start; i < zone_end; i++) {
                        int32_t ck = ocusts[i];
                        if (ck < 0 || ck >= MAX_CUSTKEY) continue;
                        uint32_t word_idx = (uint32_t)ck >> 6;
                        uint64_t bit = 1ULL << ((uint32_t)ck & 63);
                        if (!(cust_bitset[word_idx] & bit)) continue;
                        orders_map.insert(okeys[i], odates[i], oprios[i]);
                    }
                } else {
                    process_block(zone_start, zone_end);
                }
            }
        } else {
            // No zone map: full scan
            process_block(0, nrows);
        }

        if (use_zonemap && zm_map && zm_map != MAP_FAILED) {
            munmap(zm_map, zm_size);
        }
    }

    // -------------------------------------------------------------------------
    // Phase 3: Parallel lineitem scan + probe + aggregation
    // -------------------------------------------------------------------------
    // Thread-local aggregation hash maps, then merge

    const int nthreads = (int)std::thread::hardware_concurrency();
    // Each thread gets its own AggHashMap. Initial capacity: 1<<21 = 2M (load factor ~0.7 for 1.44M groups)
    constexpr uint32_t AGG_CAP = 1 << 21; // 2097152 per thread

    // To avoid 64 * 2M * 28 bytes = huge memory, use 64 partitions instead of per-thread maps.
    // Actually with 64 threads and 1.44M groups that's ~22K groups per thread if well distributed.
    // But groups aren't thread-local — use smaller per-thread maps with aggressive pre-sizing.
    // Approach: thread-local maps with capacity 1<<20 = 1M (since each thread processes ~60M/64 = 937K rows,
    // but distinct groups are shared, so use partitioned approach by orderkey % nthreads.
    //
    // Simpler and correct: each thread has its own AggHashMap, then we merge them.
    // With 64 threads, each map may contain up to 1.44M groups in worst case — use 1<<21.
    // Total memory: 64 * 2M * (4+4+4+8) bytes = 64 * 40MB = 2.56GB — too much.
    //
    // Better: partition lineitem rows by (l_orderkey % nthreads). Thread t only processes rows
    // where l_orderkey % nthreads == t, so each thread has a disjoint key space.
    // Each thread's map has at most 1.44M / 64 ≈ 22K groups → very small maps.
    // But this requires random access pattern on lineitem (bad for HDD).
    //
    // Best for HDD: each thread scans its morsel sequentially, accumulates into a local map.
    // Use smaller per-thread capacity (1<<20 = 1M) and merge at end.
    // 64 * 1M * 20B = 1.28GB — acceptable (376GB RAM).
    // Actually 1.44M groups total, so per thread ~22K distinct groups on average.
    // Use small maps: capacity 1<<17 = 131072 per thread.

    constexpr uint32_t THREAD_AGG_CAP = 1 << 17; // 131072 — well above 22K avg groups/thread

    std::vector<AggHashMap> thread_agg;
    thread_agg.reserve(nthreads);
    for (int t = 0; t < nthreads; t++) {
        thread_agg.emplace_back(THREAD_AGG_CAP);
    }

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> li_okey(gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> li_price(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> li_disc(gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> li_ship(gendb_dir + "/lineitem/l_shipdate.bin");

        const int32_t* lokeys = li_okey.data();
        const int64_t* prices = li_price.data();
        const int64_t* discs  = li_disc.data();
        const int32_t* ships  = li_ship.data();
        const size_t nrows = li_okey.size();

        // Load lineitem shipdate zone map
        std::string zm_path = gendb_dir + "/indexes/idx_lineitem_shipdate.bin";
        int zm_fd = open(zm_path.c_str(), O_RDONLY);
        bool use_zonemap = (zm_fd >= 0);
        uint32_t num_zones = 0;
        const ZoneEntry* zones = nullptr;
        void* zm_map = nullptr;
        size_t zm_size = 0;

        if (use_zonemap) {
            struct stat st;
            fstat(zm_fd, &st);
            zm_size = (size_t)st.st_size;
            zm_map = mmap(nullptr, zm_size, PROT_READ, MAP_PRIVATE, zm_fd, 0);
            close(zm_fd);
            if (zm_map == MAP_FAILED) { use_zonemap = false; }
            else {
                num_zones = *(const uint32_t*)zm_map;
                zones = (const ZoneEntry*)((const char*)zm_map + sizeof(uint32_t));
            }
        }

        // Build list of qualifying zone ranges [start, end)
        // predicate: l_shipdate > DATE_THRESHOLD
        constexpr size_t BLOCK_SIZE = 100000;

        struct ZoneRange { size_t start; size_t end; bool full; };
        std::vector<ZoneRange> zranges;

        if (use_zonemap && zones) {
            zranges.reserve(num_zones);
            for (uint32_t z = 0; z < num_zones; z++) {
                // Skip zone if all dates <= DATE_THRESHOLD (max <= threshold means no row qualifies)
                if (zones[z].max_val <= DATE_THRESHOLD) continue;
                size_t zone_start = (size_t)z * BLOCK_SIZE;
                size_t zone_end = std::min(zone_start + (size_t)zones[z].count, nrows);
                bool full = (zones[z].min_val > DATE_THRESHOLD);
                zranges.push_back({zone_start, zone_end, full});
            }
        } else {
            zranges.push_back({0, nrows, false});
        }

        // Morsel-driven parallel scan over qualifying zones
        std::atomic<size_t> zone_cursor{0};

        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                AggHashMap& local_agg = thread_agg[t];

                while (true) {
                    size_t zi = zone_cursor.fetch_add(1, std::memory_order_relaxed);
                    if (zi >= zranges.size()) break;

                    const ZoneRange& zr = zranges[zi];
                    size_t start = zr.start;
                    size_t end = zr.end;

                    if (zr.full) {
                        // All rows in zone satisfy l_shipdate > threshold
                        for (size_t i = start; i < end; i++) {
                            int32_t okey = lokeys[i];
                            const OrdersEntry* oe = orders_map.find(okey);
                            if (!oe) continue;
                            // revenue = l_extendedprice * (100 - l_discount) / 100
                            // Both are scaled by 100, so:
                            // revenue_scaled = prices[i] * (100 - discs[i])  [scaled x10000]
                            int64_t rev = prices[i] * (100LL - discs[i]);
                            local_agg.update(okey, oe->orderdate, oe->shippriority, rev);
                        }
                    } else {
                        for (size_t i = start; i < end; i++) {
                            if (ships[i] <= DATE_THRESHOLD) continue;
                            int32_t okey = lokeys[i];
                            const OrdersEntry* oe = orders_map.find(okey);
                            if (!oe) continue;
                            int64_t rev = prices[i] * (100LL - discs[i]);
                            local_agg.update(okey, oe->orderdate, oe->shippriority, rev);
                        }
                    }
                }
            });
        }
        for (auto& th : threads) th.join();

        if (use_zonemap && zm_map && zm_map != MAP_FAILED) {
            munmap(zm_map, zm_size);
        }
    }

    // -------------------------------------------------------------------------
    // Phase 4: Merge thread-local aggregation maps
    // -------------------------------------------------------------------------
    // Build a single global aggregation map from thread-local maps.
    // Estimate: 1.44M groups. Use capacity = 1<<21 = 2M.
    constexpr uint32_t GLOBAL_AGG_CAP = 1 << 21;
    AggHashMap global_agg(GLOBAL_AGG_CAP);

    {
        GENDB_PHASE("aggregation_merge");

        for (int t = 0; t < nthreads; t++) {
            const AggHashMap& local = thread_agg[t];
            for (uint32_t s = 0; s < local.capacity; s++) {
                const AggHashMap::Slot& sl = local.slots[s];
                if (sl.orderkey == 0) continue;
                global_agg.update(sl.orderkey, sl.orderdate, sl.shippriority, sl.revenue);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 5: Top-10 by revenue DESC, o_orderdate ASC
    // -------------------------------------------------------------------------
    struct ResultRow {
        int32_t orderkey;
        int64_t revenue;  // scaled x10000
        int32_t orderdate;
        int32_t shippriority;
    };

    std::vector<ResultRow> top10;
    {
        GENDB_PHASE("sort_topk");

        // Use a min-heap of size 10 keyed on (revenue DESC, orderdate ASC)
        // Min-heap: smallest revenue at top → pop when heap has 11 elements.
        // Comparison: a < b means a should be popped first (worse rank).
        // Worse rank = lower revenue, or same revenue and later date.
        auto worse = [](const ResultRow& a, const ResultRow& b) -> bool {
            // Returns true if a is "worse" (should be at top of min-heap / popped first)
            if (a.revenue != b.revenue) return a.revenue < b.revenue;
            return a.orderdate > b.orderdate; // later date = worse (we want ASC)
        };

        std::priority_queue<ResultRow, std::vector<ResultRow>, decltype(worse)> heap(worse);

        for (uint32_t s = 0; s < global_agg.capacity; s++) {
            const AggHashMap::Slot& sl = global_agg.slots[s];
            if (sl.orderkey == 0) continue;
            ResultRow r{sl.orderkey, sl.revenue, sl.orderdate, sl.shippriority};
            heap.push(r);
            if (heap.size() > 10) heap.pop();
        }

        // Extract results in sorted order (revenue DESC, orderdate ASC)
        top10.resize(heap.size());
        for (int i = (int)heap.size() - 1; i >= 0; i--) {
            top10[i] = heap.top();
            heap.pop();
        }
    }

    // -------------------------------------------------------------------------
    // Phase 6: Output CSV
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) {
            std::cerr << "Failed to open output file: " << out_path << std::endl;
            return;
        }

        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        for (const ResultRow& r : top10) {
            // revenue is scaled x10000 (extendedprice x100 * (100-discount) where discount is x100)
            // So actual revenue = r.revenue / 10000.0
            // Print with 2 decimal places: r.revenue / 10000 integer part, (r.revenue % 10000) / 100 decimal
            int64_t rev_int  = r.revenue / 10000LL;
            int64_t rev_frac = (r.revenue % 10000LL) / 100LL;
            // Format: NNNNNNNN.DD
            fprintf(f, "%d,%lld.%02lld,%s,%d\n",
                r.orderkey,
                (long long)rev_int,
                (long long)rev_frac,
                gendb::epoch_days_to_date_str(r.orderdate).c_str(),
                r.shippriority);
        }

        fclose(f);
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
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q3(gendb_dir, results_dir);
    return 0;
}
#endif
