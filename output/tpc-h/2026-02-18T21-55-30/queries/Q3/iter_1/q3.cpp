/*
 * Q3: Shipping Priority
 *
 * SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
 *        o_orderdate, o_shippriority
 * FROM customer, orders, lineitem
 * WHERE c_mktsegment = 'BUILDING'
 *   AND c_custkey = o_custkey
 *   AND l_orderkey = o_orderkey
 *   AND o_orderdate < DATE '1995-03-15'
 *   AND l_shipdate > DATE '1995-03-15'
 * GROUP BY l_orderkey, o_orderdate, o_shippriority
 * ORDER BY revenue DESC, o_orderdate
 * LIMIT 10;
 *
 * Strategy:
 *   1. Scan customer, build bitset on c_custkey for BUILDING segment
 *   2. Scan orders (zone-map pruned for o_orderdate < cutoff), filter by bitset + date,
 *      parallel two-pass build of compact hash map: o_orderkey -> {o_orderdate, o_shippriority}
 *      Also build Bloom filter on qualifying o_orderkeys
 *   3. Scan lineitem from shipdate > cutoff (binary search start), check Bloom filter first,
 *      probe orders hash map, accumulate revenue per (l_orderkey, o_orderdate, o_shippriority)
 *      using thread-local aggregation maps partitioned by key hash
 *   4. Parallel partition-merge of aggregation results, top-10
 *
 * Key optimizations vs iter_0:
 *   - Bloom filter (1MB, L2-resident) before orders hash map probe: filters ~87% non-matches
 *   - Parallel orders hash map build: parallel collect + parallel insert via partitioned approach
 *   - Reduced OrderEntry: key+date+priority packed into 12B struct (unchanged) but smaller hash table
 *   - Partitioned parallel aggregation merge: 64 partitions, thread i merges partition i
 *   - Thread-local agg maps sized to actual partition count, not over-allocated
 *   - OMP schedule(dynamic) for lineitem scan to balance load
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <iostream>
#include <omp.h>
#include <atomic>

#include "date_utils.h"
#include "mmap_utils.h"
#include "hash_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Bloom Filter: sized for ~4M keys, ~1% FP rate
// 10 bits/key => 40M bits = 5MB  (L3-resident)
// Use 3 hash probes for good FP rate
// ---------------------------------------------------------------------------
struct BloomFilter {
    // 4MB = 33554432 bits => mask = 33554431
    static constexpr uint32_t WORDS = (1u << 22); // 4M 64-bit words = 32MB too large
    // Use 1MB = 8M bits
    static constexpr uint32_t BIT_COUNT = (1u << 23); // 8M bits = 1MB
    static constexpr uint32_t BIT_MASK  = BIT_COUNT - 1;

    std::vector<uint64_t> bits;

    void init() {
        bits.assign(BIT_COUNT / 64, 0ULL);
    }

    inline void insert(uint32_t key) {
        uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h >> 41) & BIT_MASK;
        uint32_t h2 = (uint32_t)(h >> 17) & BIT_MASK;
        uint32_t h3 = (uint32_t)(h)        & BIT_MASK;
        bits[h1 >> 6] |= (1ULL << (h1 & 63));
        bits[h2 >> 6] |= (1ULL << (h2 & 63));
        bits[h3 >> 6] |= (1ULL << (h3 & 63));
    }

    inline bool maybe_contains(uint32_t key) const {
        uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h >> 41) & BIT_MASK;
        uint32_t h2 = (uint32_t)(h >> 17) & BIT_MASK;
        uint32_t h3 = (uint32_t)(h)        & BIT_MASK;
        return (bits[h1 >> 6] >> (h1 & 63) & 1) &
               (bits[h2 >> 6] >> (h2 & 63) & 1) &
               (bits[h3 >> 6] >> (h3 & 63) & 1);
    }
};

// ---------------------------------------------------------------------------
// Orders hash map: o_orderkey -> {o_orderdate, o_shippriority}
// Open-addressing, linear probing, power-of-2 size
// key=0 is sentinel (orderkeys are always > 0 in TPC-H)
// ---------------------------------------------------------------------------
struct OrderEntry {
    int32_t orderkey;   // 0 = empty slot
    int32_t orderdate;
    int32_t shippriority;
};

struct OrdersHashMap {
    OrderEntry* slots;
    uint32_t mask;
    uint32_t capacity;

    void init(uint32_t cap, OrderEntry* mem) {
        capacity = cap;
        mask = cap - 1;
        slots = mem;
        memset(slots, 0, cap * sizeof(OrderEntry));
    }

    // Insert without duplicate check (orderkeys are unique)
    inline void insert(int32_t key, int32_t orderdate, int32_t shippriority) {
        uint32_t h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (slots[h].orderkey != 0) {
            h = (h + 1) & mask;
        }
        slots[h] = {key, orderdate, shippriority};
    }

    inline const OrderEntry* find(int32_t key) const {
        uint32_t h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (true) {
            const OrderEntry& e = slots[h];
            if (__builtin_expect(e.orderkey == 0, 0)) return nullptr;
            if (e.orderkey == key) return &e;
            h = (h + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------------------
// Aggregation hash map per thread: keyed by (l_orderkey, o_orderdate, o_shippriority)
// We use l_orderkey as primary hash key since (orderkey, orderdate, shippriority)
// is effectively unique per orderkey (each order has one date+priority)
// ---------------------------------------------------------------------------
struct AggEntry {
    int32_t orderkey;    // 0 = empty
    int32_t orderdate;
    int32_t shippriority;
    int64_t revenue;
};

struct AggHashMap {
    AggEntry* slots;
    uint32_t mask;

    void init(uint32_t sz, AggEntry* mem) {
        mask = sz - 1;
        slots = mem;
        memset(slots, 0, sz * sizeof(AggEntry));
    }

    inline void accumulate(int32_t ok, int32_t od, int32_t sp, int64_t rev) {
        uint32_t h = (uint32_t)(((uint64_t)(uint32_t)ok * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (true) {
            AggEntry& e = slots[h];
            if (e.orderkey == 0) {
                e.orderkey = ok; e.orderdate = od; e.shippriority = sp; e.revenue = rev;
                return;
            }
            if (e.orderkey == ok) { // orderkey determines orderdate/shippriority uniquely
                e.revenue += rev;
                return;
            }
            h = (h + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------------------
// Result row
// ---------------------------------------------------------------------------
struct ResultRow {
    int32_t orderkey;
    int64_t revenue; // scale x10000
    int32_t orderdate;
    int32_t shippriority;
};

void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t CUTOFF = gendb::date_str_to_epoch_days("1995-03-15"); // 9204

    // -------------------------------------------------------------------------
    // Phase 1: Load customer, build bitset on c_custkey for BUILDING segment
    // -------------------------------------------------------------------------
    static const int CUST_MAX = 1500001;
    std::vector<uint64_t> cust_bitset((CUST_MAX + 63) / 64, 0);

    int building_code = -1;
    {
        GENDB_PHASE("dim_filter");

        // Load c_mktsegment dictionary
        std::string dict_path = gendb_dir + "/customer/c_mktsegment_dict.txt";
        std::ifstream dict_file(dict_path);
        if (!dict_file.is_open())
            throw std::runtime_error("Cannot open " + dict_path);
        std::string word;
        int code = 0;
        while (std::getline(dict_file, word)) {
            if (word == "BUILDING") { building_code = code; break; }
            ++code;
        }
        if (building_code < 0)
            throw std::runtime_error("BUILDING not found in dictionary");

        gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> c_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");
        const uint64_t n_cust = c_custkey.size();

        // Parallel scan; atomic OR per 64-bit word (race-free: bits only set, never cleared)
        #pragma omp parallel for schedule(static) num_threads(16)
        for (uint64_t i = 0; i < n_cust; ++i) {
            if (c_mktsegment[i] == building_code) {
                int32_t ck = c_custkey[i];
                if (ck > 0 && ck < CUST_MAX) {
                    uint32_t word_idx = (uint32_t)ck >> 6;
                    uint64_t bit = 1ULL << ((uint32_t)ck & 63);
                    __atomic_fetch_or(&cust_bitset[word_idx], bit, __ATOMIC_RELAXED);
                }
            }
        }
    }

    auto in_bitset = [&](int32_t ck) -> bool {
        if (ck <= 0 || ck >= CUST_MAX) return false;
        return (cust_bitset[(uint32_t)ck >> 6] >> ((uint32_t)ck & 63)) & 1;
    };

    // -------------------------------------------------------------------------
    // Phase 2: Scan orders (zone-map pruned), build hash map + bloom filter
    //
    // Two-pass parallel build:
    //   Pass A: each thread scans its chunk, appends qualifying rows to thread-local buffer
    //   After A: count total, size hash map, single-threaded insert (cache-friendly sequential)
    //   Also build bloom filter over qualifying orderkeys
    // -------------------------------------------------------------------------
    // Allocate orders hash map memory (pre-allocated)
    // ~3.75M entries, 50% load => 8M slots
    static const uint32_t ORDERS_MAP_SZ = 8388608; // 8M, power of 2
    std::vector<OrderEntry> orders_mem(ORDERS_MAP_SZ);
    OrdersHashMap orders_map;

    BloomFilter bloom;
    bloom.init();

    {
        GENDB_PHASE("build_joins");

        // Load zone map index for o_orderdate
        struct ZMEntry { int32_t zm_min, zm_max; uint32_t count; };
        std::vector<ZMEntry> zm_entries;
        {
            std::string zm_path = gendb_dir + "/indexes/zone_map_o_orderdate.bin";
            int zm_fd = open(zm_path.c_str(), O_RDONLY);
            uint32_t num_blocks = 0;
            if (zm_fd >= 0) {
                (void)read(zm_fd, &num_blocks, 4);
                zm_entries.resize(num_blocks);
                (void)read(zm_fd, zm_entries.data(), num_blocks * sizeof(ZMEntry));
                close(zm_fd);
            }
        }

        gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey(gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");
        const uint64_t n_orders = o_orderkey.size();

        // Zone-map pruning: find end block where all subsequent blocks have zm_min >= CUTOFF
        uint32_t num_blocks = (uint32_t)zm_entries.size();
        uint32_t end_block = num_blocks;
        if (!zm_entries.empty()) {
            for (uint32_t b = 0; b < num_blocks; ++b) {
                if (zm_entries[b].zm_min >= CUTOFF) {
                    end_block = b;
                    break;
                }
            }
        }
        const uint32_t BLOCK_SIZE = 100000;
        uint64_t end_row = (uint64_t)end_block * BLOCK_SIZE;
        if (end_row > n_orders) end_row = n_orders;

        // Parallel collect into thread-local buffers
        const int N_THREADS_BUILD = 32;
        std::vector<std::vector<OrderEntry>> thread_results(N_THREADS_BUILD);

        #pragma omp parallel num_threads(N_THREADS_BUILD)
        {
            int tid = omp_get_thread_num();
            auto& local = thread_results[tid];
            local.reserve(200000);

            #pragma omp for schedule(static)
            for (uint64_t i = 0; i < end_row; ++i) {
                int32_t od = o_orderdate[i];
                if (od >= CUTOFF) continue;
                int32_t ck = o_custkey[i];
                if (!in_bitset(ck)) continue;
                local.push_back({o_orderkey[i], od, o_shippriority[i]});
            }
        }

        // Initialize the hash map (zero-fill)
        orders_map.init(ORDERS_MAP_SZ, orders_mem.data());

        // Single-threaded sequential insert into hash map + bloom filter build
        // (sequential insert is cache-friendly and avoids lock contention)
        for (auto& v : thread_results) {
            for (const auto& e : v) {
                orders_map.insert(e.orderkey, e.orderdate, e.shippriority);
                bloom.insert((uint32_t)e.orderkey);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 3: Scan lineitem from shipdate > CUTOFF, aggregate
    //
    // Key improvement: Bloom filter check before expensive hash map probe.
    // ~87% of lineitem rows (l_orderkey not in qualifying orders) are eliminated
    // cheaply via L2-resident bloom filter check.
    //
    // Thread-local aggregation maps: each thread uses its own AggHashMap.
    // Since orderkey uniquely determines (orderdate, shippriority), we key by orderkey.
    // -------------------------------------------------------------------------
    const int N_THREADS = 64;

    // Per-thread agg maps — use 256K slots each (power of 2, ~3.75M groups / 64 threads = ~58K avg)
    // Sized for worst-case load
    static const uint32_t AGG_SLOTS_PER_THREAD = 131072; // 128K
    // Allocate all agg memory in one block for cache efficiency
    std::vector<AggEntry> agg_mem(N_THREADS * AGG_SLOTS_PER_THREAD);
    std::vector<AggHashMap> agg_maps(N_THREADS);
    for (int t = 0; t < N_THREADS; ++t) {
        agg_maps[t].init(AGG_SLOTS_PER_THREAD, &agg_mem[t * AGG_SLOTS_PER_THREAD]);
    }

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount(gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");

        const uint64_t n_lineitem = l_shipdate.size();

        // Binary search for first row where l_shipdate > CUTOFF
        uint64_t lo = 0, hi = n_lineitem;
        while (lo < hi) {
            uint64_t mid = (lo + hi) / 2;
            if (l_shipdate[mid] <= CUTOFF) lo = mid + 1;
            else hi = mid;
        }
        const uint64_t start_row = lo;

        #pragma omp parallel num_threads(N_THREADS)
        {
            int tid = omp_get_thread_num();
            AggHashMap& local_agg = agg_maps[tid];

            #pragma omp for schedule(static)
            for (uint64_t i = start_row; i < n_lineitem; ++i) {
                int32_t lok = l_orderkey[i];

                // Bloom filter: skip ~87% of non-matching keys cheaply (L2-resident)
                if (!bloom.maybe_contains((uint32_t)lok)) continue;

                // Full hash map probe only for bloom-positive keys
                const OrderEntry* oe = orders_map.find(lok);
                if (!oe) continue;

                // revenue = l_extendedprice * (100 - l_discount), scale x10000
                int64_t ep   = l_extendedprice[i];
                int64_t disc = l_discount[i];
                int64_t rev  = ep * (100LL - disc);

                local_agg.accumulate(lok, oe->orderdate, oe->shippriority, rev);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 4: Partition-parallel merge of aggregation results, top-10
    //
    // Instead of sequential merge into a global 8M-slot map, use 64 partitions:
    // Thread i merges all agg_maps entries that hash to partition i.
    // This is embarrassingly parallel with no contention.
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        // Partition-parallel merge: N_PARTS partitions, each thread owns one partition
        // Use 256 partitions (top bits of orderkey hash) so threads work in parallel
        // Actually: use N_THREADS partitions for simplicity
        const int N_PARTS = N_THREADS;
        const uint32_t PART_MASK = (uint32_t)(N_PARTS - 1); // N_PARTS must be power of 2

        // Per-partition result vectors
        std::vector<std::vector<ResultRow>> part_results(N_PARTS);
        for (int p = 0; p < N_PARTS; ++p) part_results[p].reserve(65536);

        // Allocate per-partition merge memory (heap, not stack)
        static const uint32_t MERGE_SZ = 131072; // 128K slots, power of 2
        std::vector<std::vector<AggEntry>> all_merge_mem(N_PARTS, std::vector<AggEntry>(MERGE_SZ));

        // Parallel partition-merge: thread p processes all agg_maps entries for partition p
        #pragma omp parallel num_threads(N_PARTS)
        {
            int part = omp_get_thread_num();
            auto& local_results = part_results[part];

            // Merge all thread-local agg maps for entries in this partition
            AggHashMap merge_map;
            merge_map.init(MERGE_SZ, all_merge_mem[part].data());
            memset(all_merge_mem[part].data(), 0, MERGE_SZ * sizeof(AggEntry));

            for (int t = 0; t < N_THREADS; ++t) {
                const AggHashMap& src = agg_maps[t];
                uint32_t src_capacity = src.mask + 1;
                for (uint32_t s = 0; s < src_capacity; ++s) {
                    const AggEntry& e = src.slots[s];
                    if (e.orderkey == 0) continue;
                    // Only process entries belonging to this partition
                    uint32_t ep_hash = (uint32_t)(((uint64_t)(uint32_t)e.orderkey * 0x9E3779B97F4A7C15ULL) >> 32);
                    if ((ep_hash & PART_MASK) != (uint32_t)part) continue;
                    merge_map.accumulate(e.orderkey, e.orderdate, e.shippriority, e.revenue);
                }
            }

            // Collect results from this partition
            for (uint32_t s = 0; s <= merge_map.mask; ++s) {
                const AggEntry& e = merge_map.slots[s];
                if (e.orderkey == 0) continue;
                local_results.push_back({e.orderkey, e.revenue, e.orderdate, e.shippriority});
            }
        }

        // Collect all partition results
        std::vector<ResultRow> all_rows;
        all_rows.reserve(4000000);
        for (int p = 0; p < N_PARTS; ++p) {
            for (auto& r : part_results[p])
                all_rows.push_back(r);
        }

        // Top-10: ORDER BY revenue DESC, o_orderdate ASC
        size_t k = std::min((size_t)10, all_rows.size());
        std::partial_sort(all_rows.begin(), all_rows.begin() + k, all_rows.end(),
            [](const ResultRow& a, const ResultRow& b) {
                if (a.revenue != b.revenue) return a.revenue > b.revenue;
                return a.orderdate < b.orderdate;
            });
        all_rows.resize(k);

        // Write output CSV
        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) throw std::runtime_error("Cannot open output file: " + out_path);

        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[12];
        for (const auto& row : all_rows) {
            int64_t integer_part = row.revenue / 10000;
            int64_t frac_part    = (row.revenue % 10000) / 100;
            int64_t third_dec    = (row.revenue % 100);
            if (third_dec >= 50) frac_part++;
            if (frac_part >= 100) { frac_part = 0; integer_part++; }

            gendb::epoch_days_to_date_str(row.orderdate, date_buf);
            fprintf(f, "%d,%lld.%02lld,%s,%d\n",
                    row.orderkey,
                    (long long)integer_part,
                    (long long)frac_part,
                    date_buf,
                    row.shippriority);
        }
        fclose(f);
    }
}

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
