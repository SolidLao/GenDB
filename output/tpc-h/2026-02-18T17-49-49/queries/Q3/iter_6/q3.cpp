// Q3: Shipping Priority — iter_6
//
// Bottleneck analysis from iter_5 (360ms):
//   main_scan: 180ms (50%) — dominated by:
//     (a) 384MB staging pre-reservation (64 threads × 64 parts × 8192 entries × 12B)
//         that completely blows L3 cache (44MB), causing every HT probe to miss cache.
//     (b) scatter-gather overhead writing to 4096 vectors per lineitem row
//   build_joins: 138ms (38%) — CAS contention on shared orders HT +
//     64 thread-local bloom copies (147MB alloc+init+merge overhead)
//
// New architecture:
//
// Phase 1: dim_filter — sequential customer bitmap (unchanged, ~12ms)
//
// Phase 2: build_orders — partition-parallel, NO CAS, NO bloom thread-copies:
//   - Single pass over orders: each thread scans its zone-map range
//   - Partition assignment: p = hash(o_orderkey) & (NUM_PARTS-1)
//   - Thread-local collector vectors (one per thread), then thread p builds orders_ht[p]
//   - Bloom filter: single shared, built per-thread after partition assignment (no contention
//     since each partition is owned by exactly one thread)
//   - Expected: ~50-60ms (down from 138ms)
//
// Phase 3: main_scan — DIRECT thread-local aggregation, NO staging:
//   - Each thread has a CompactHashMap<int32_t, AggVal> (pre-sized to ~22K groups)
//   - For each qualifying lineitem: bloom check → probe partition's orders_ht[p] → insert/update local agg map
//   - No scatter, no staging vectors, no cache pollution
//   - Expected: ~60-80ms (down from 180ms)
//
// Phase 4: aggregation_merge — parallel by partition:
//   - Thread p collects from all 64 thread-local agg maps all entries with hash(key) & (NUM_PARTS-1) == p
//   - Builds final agg_parts[p] CompactHashMap
//   - Expected: ~10ms (unchanged)
//
// Phase 5: Top-K + output (unchanged)

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <climits>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ── Constants ────────────────────────────────────────────────────────────────
static constexpr int32_t DATE_1995_03_15 = 9204; // days since epoch
static constexpr int32_t MAX_CUSTKEY     = 1500001;
static constexpr int     NUM_THREADS     = 64;
static constexpr int     NUM_PARTS       = 64; // power-of-2 for fast modulo

// ── Aggregation value per group (keyed by l_orderkey) ────────────────────────
struct AggVal {
    int64_t revenue_sum;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// ── Top-K result entry ────────────────────────────────────────────────────────
struct TopKEntry {
    int64_t revenue_sum;
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

static bool final_cmp(const TopKEntry& a, const TopKEntry& b) {
    if (a.revenue_sum != b.revenue_sum) return a.revenue_sum > b.revenue_sum;
    return a.o_orderdate < b.o_orderdate;
}

// ── Orders partition hash table (one per partition, no concurrency needed) ───
// Custom open-addressing HT with parallel arrays for cache efficiency.
// Single-threaded build per partition → no CAS, no locking.
struct PartitionOrdersHT {
    static constexpr int32_t EMPTY = 0;

    int32_t* keys;     // key array (0 = empty)
    int32_t* dates;    // o_orderdate per slot
    int32_t* pris;     // o_shippriority per slot
    uint64_t mask;
    size_t   cap;

    PartitionOrdersHT() : keys(nullptr), dates(nullptr), pris(nullptr), mask(0), cap(0) {}

    ~PartitionOrdersHT() {
        if (keys)  { delete[] keys;  keys = nullptr; }
        if (dates) { delete[] dates; dates = nullptr; }
        if (pris)  { delete[] pris;  pris = nullptr; }
    }

    PartitionOrdersHT(const PartitionOrdersHT&) = delete;
    PartitionOrdersHT& operator=(const PartitionOrdersHT&) = delete;

    void init(size_t expected) {
        cap = 16;
        // ~2.5× expected for ~40% load factor — good for probe performance
        while (cap < expected * 5 / 2) cap <<= 1;
        mask = cap - 1;
        keys  = new int32_t[cap]();  // zero-initialized → all EMPTY
        dates = new int32_t[cap]();
        pris  = new int32_t[cap]();
    }

    inline void insert(int32_t key, int32_t date, int32_t priority) {
        uint64_t pos = gendb::hash_int(key) & mask;
        while (true) {
            if (keys[pos] == EMPTY) {
                keys[pos]  = key;
                dates[pos] = date;
                pris[pos]  = priority;
                return;
            }
            if (keys[pos] == key) return; // duplicate
            pos = (pos + 1) & mask;
        }
    }

    inline bool find(int32_t key, int32_t& out_date, int32_t& out_priority) const {
        uint64_t pos = gendb::hash_int(key) & mask;
        while (true) {
            int32_t k = keys[pos];
            if (k == EMPTY) return false;
            if (k == key) {
                out_date     = dates[pos];
                out_priority = pris[pos];
                return true;
            }
            pos = (pos + 1) & mask;
        }
    }
};

// ── Bloom filter ──────────────────────────────────────────────────────────────
// Sized for ~1.44M keys, ~10 bits/key ≈ 1.8MB — fits comfortably in L3
struct BloomFilter {
    std::vector<uint64_t> bits_;
    size_t total_bits_;

    void init(size_t capacity) {
        size_t nbits = capacity * 10 + 64;
        total_bits_  = (nbits + 63) & ~63ULL;
        bits_.assign(total_bits_ / 64, 0ULL);
    }

    inline void set_bit(size_t b) { bits_[b >> 6] |= (1ULL << (b & 63)); }
    inline bool get_bit(size_t b) const { return (bits_[b >> 6] >> (b & 63)) & 1ULL; }

    void insert(int32_t key) {
        uint64_t h  = gendb::hash_int(key);
        uint64_t h2 = h ^ (h >> 17) ^ (h << 31);
        set_bit(h  % total_bits_);
        set_bit(h2 % total_bits_);
    }

    bool may_contain(int32_t key) const {
        uint64_t h  = gendb::hash_int(key);
        uint64_t h2 = h ^ (h >> 17) ^ (h << 31);
        return get_bit(h  % total_bits_) && get_bit(h2 % total_bits_);
    }
};

// ── Collected order row (intermediate, used during orders build) ──────────────
struct OrderRow {
    int32_t o_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// ── Main query function ───────────────────────────────────────────────────────
void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // ── Phase 1: Build customer bitmap ────────────────────────────────────────
    gendb::DenseBitmap cust_bitmap(MAX_CUSTKEY);
    {
        GENDB_PHASE("dim_filter");

        int16_t building_code = -1;
        {
            std::ifstream dict(gendb_dir + "/customer/c_mktsegment_dict.txt");
            std::string line;
            int16_t code = 0;
            while (std::getline(dict, line)) {
                if (line == "BUILDING") { building_code = code; break; }
                ++code;
            }
        }
        if (building_code < 0) {
            std::cerr << "ERROR: 'BUILDING' not found in c_mktsegment dictionary\n";
            return;
        }

        gendb::MmapColumn<int16_t> c_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");
        gendb::MmapColumn<int32_t> c_custkey   (gendb_dir + "/customer/c_custkey.bin");
        size_t n = c_mktsegment.size();
        const int16_t* seg  = c_mktsegment.data;
        const int32_t* ckey = c_custkey.data;
        for (size_t i = 0; i < n; ++i) {
            if (seg[i] == building_code) cust_bitmap.set((size_t)ckey[i]);
        }
    }

    // ── Phase 2: Build partitioned orders HTs + bloom filter ─────────────────
    // Strategy: avoid CAS contention and thread-local bloom copies.
    //
    // Step 2a: Each thread scans zone-map-pruned orders blocks and collects
    //          qualifying rows into thread-local vectors (one vector per thread).
    //
    // Step 2b: Merge into per-partition vectors: partition p = hash(o_orderkey) & (NUM_PARTS-1).
    //          Thread p reads from all thread-local vectors and picks rows for partition p.
    //
    // Step 2c: Thread p builds orders_ht[p] from its partition rows + inserts into bloom.
    //
    // This approach has ZERO CAS, ZERO lock contention, and the bloom filter
    // is built by each thread into disjoint word regions (no atomic needed).

    // Per-partition orders HTs — each owned by thread p
    std::vector<PartitionOrdersHT> orders_ht(NUM_PARTS);

    // Shared bloom filter — built in step 2c with no contention (partition threads write to disjoint keys)
    // NOTE: Different keys may hash to same bloom word, but each partition's keys are
    // a fixed subset, so we need atomic OR on bloom words. We use fetch_or with relaxed ordering.
    BloomFilter orders_bloom;
    orders_bloom.init(1500000UL); // sized for ~1.44M keys

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey    (gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey     (gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate   (gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_shippriority);

        size_t total_orders = o_orderkey.size();

        // Zone map to skip orders blocks
        gendb::ZoneMapIndex zone_map(gendb_dir + "/indexes/orders_o_orderdate.bin");

        struct RowRange { size_t start, end; };
        std::vector<RowRange> qual_ranges;
        qual_ranges.reserve(zone_map.zones.size());
        for (const auto& z : zone_map.zones) {
            if (z.min >= DATE_1995_03_15) continue;
            size_t row_start = (size_t)z.row_offset;
            size_t row_end   = std::min(row_start + (size_t)z.row_count, total_orders);
            qual_ranges.push_back({row_start, row_end});
        }
        if (qual_ranges.empty())
            qual_ranges.push_back({0, total_orders});

        // Step 2a: Thread-local collection — each thread scans its morsels and keeps
        //          all qualifying rows in a flat vector (cheap push_back, no partitioning yet)
        std::vector<std::vector<OrderRow>> tl_rows(NUM_THREADS);
        // Estimate: ~1.44M total qualifying rows / NUM_THREADS = ~22500 per thread
        for (auto& v : tl_rows) v.reserve(25000);

        std::atomic<size_t> range_idx{0};
        auto orders_collect = [&](int tid) {
            auto& my_rows = tl_rows[tid];
            while (true) {
                size_t ri = range_idx.fetch_add(1, std::memory_order_relaxed);
                if (ri >= qual_ranges.size()) break;
                size_t start = qual_ranges[ri].start;
                size_t end   = qual_ranges[ri].end;

                const int32_t* okey  = o_orderkey.data     + start;
                const int32_t* odate = o_orderdate.data    + start;
                const int32_t* ocust = o_custkey.data      + start;
                const int32_t* oship = o_shippriority.data + start;

                for (size_t i = 0, n = end - start; i < n; ++i) {
                    if (odate[i] < DATE_1995_03_15 && cust_bitmap.test((size_t)ocust[i])) {
                        my_rows.push_back({okey[i], odate[i], oship[i]});
                    }
                }
            }
        };

        {
            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(orders_collect, t);
            for (auto& t : threads) t.join();
        }

        // Step 2b+2c: Thread p collects from ALL tl_rows the rows that belong to partition p,
        //             builds orders_ht[p], and inserts into bloom filter.
        //
        // Bloom filter words: since different partitions can write to the same bloom word
        // (bloom is not partitioned), we use atomic fetch_or on the bloom words.
        // Cast bloom bits to atomic<uint64_t>* for lock-free OR.
        // This is safe because std::atomic<uint64_t> is always lock-free on x86.
        static_assert(sizeof(std::atomic<uint64_t>) == sizeof(uint64_t), "atomic overhead");
        std::atomic<uint64_t>* bloom_words =
            reinterpret_cast<std::atomic<uint64_t>*>(orders_bloom.bits_.data());
        size_t bloom_tb = orders_bloom.total_bits_;

        // Count per-partition sizes so we can init HTs correctly
        std::vector<size_t> part_sizes(NUM_PARTS, 0);
        for (int t = 0; t < NUM_THREADS; ++t) {
            for (const auto& row : tl_rows[t]) {
                int p = (int)(gendb::hash_int(row.o_orderkey) & (uint64_t)(NUM_PARTS - 1));
                part_sizes[p]++;
            }
        }

        // Init all HTs upfront
        for (int p = 0; p < NUM_PARTS; ++p) {
            orders_ht[p].init(std::max(part_sizes[p], (size_t)64));
        }

        // Thread p builds its partition HT + bloom (parallel, no contention)
        auto orders_build = [&](int p) {
            auto& ht = orders_ht[p];
            for (int t = 0; t < NUM_THREADS; ++t) {
                for (const auto& row : tl_rows[t]) {
                    int rp = (int)(gendb::hash_int(row.o_orderkey) & (uint64_t)(NUM_PARTS - 1));
                    if (rp != p) continue;
                    ht.insert(row.o_orderkey, row.o_orderdate, row.o_shippriority);
                    // Insert into bloom — use atomic OR for thread safety
                    uint64_t h  = gendb::hash_int(row.o_orderkey);
                    uint64_t h2 = h ^ (h >> 17) ^ (h << 31);
                    size_t  b1 = h  % bloom_tb;
                    size_t  b2 = h2 % bloom_tb;
                    bloom_words[b1 >> 6].fetch_or(1ULL << (b1 & 63), std::memory_order_relaxed);
                    bloom_words[b2 >> 6].fetch_or(1ULL << (b2 & 63), std::memory_order_relaxed);
                }
            }
        };

        {
            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int p = 0; p < NUM_PARTS; ++p)
                threads.emplace_back(orders_build, p);
            for (auto& t : threads) t.join();
        }
    }

    // ── Phase 3: Scan lineitem + direct thread-local aggregation ─────────────
    // Key change from iter_5: NO staging buffers, NO scatter.
    // Each thread directly aggregates into its own CompactHashMap.
    // This keeps each thread's agg map cache-hot (~22K groups × ~20B = ~440KB per thread).
    //
    // After the parallel scan, Phase 4 merges thread-local maps in parallel by partition.

    using AggMap = gendb::CompactHashMap<int32_t, AggVal>;
    // Thread-local agg maps (one per thread)
    std::vector<AggMap> tl_agg(NUM_THREADS);

    // Final per-partition agg maps (output of merge phase)
    std::vector<AggMap> agg_parts(NUM_PARTS);

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey     (gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount      (gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate      (gendb_dir + "/lineitem/l_shipdate.bin");

        mmap_prefetch_all(l_orderkey, l_extendedprice, l_discount, l_shipdate);

        size_t total_lineitem = l_orderkey.size();

        // Zone-map skip: lineitem sorted by l_shipdate → skip blocks where max <= DATE_1995_03_15
        // Use binary search on block-end elements for O(log N) instead of O(N) scan
        static constexpr size_t LI_BLOCK = 100000;
        size_t num_blocks = (total_lineitem + LI_BLOCK - 1) / LI_BLOCK;
        size_t first_qualifying_block = num_blocks;
        {
            const int32_t* lship = l_shipdate.data;
            // Binary search: find smallest block b where lship[end-1] > DATE_1995_03_15
            size_t lo = 0, hi = num_blocks;
            while (lo < hi) {
                size_t mid = (lo + hi) / 2;
                size_t bend = std::min((mid + 1) * LI_BLOCK, total_lineitem);
                if (lship[bend - 1] > DATE_1995_03_15) {
                    hi = mid;
                } else {
                    lo = mid + 1;
                }
            }
            first_qualifying_block = lo;
        }

        size_t li_start_row = first_qualifying_block * LI_BLOCK;
        if (li_start_row < total_lineitem) {

        // Pre-size thread-local agg maps: ~1.44M groups / 64 threads ≈ 22500 per thread
        // Reserve with 1.5× slack for load factor
        for (auto& am : tl_agg) am.reserve(32768);

        static constexpr size_t MORSEL = 65536;
        size_t scan_rows   = total_lineitem - li_start_row;
        size_t num_morsels = (scan_rows + MORSEL - 1) / MORSEL;

        std::atomic<size_t> morsel_idx{0};

        auto li_worker = [&](int tid) {
            AggMap& my_agg = tl_agg[tid];

            while (true) {
                size_t mi = morsel_idx.fetch_add(1, std::memory_order_relaxed);
                if (mi >= num_morsels) break;

                size_t start = li_start_row + mi * MORSEL;
                size_t end   = std::min(start + MORSEL, total_lineitem);
                size_t n     = end - start;

                const int32_t* lkey   = l_orderkey.data      + start;
                const int32_t* lship  = l_shipdate.data      + start;
                const int64_t* lprice = l_extendedprice.data + start;
                const int64_t* ldisc  = l_discount.data      + start;

                for (size_t i = 0; i < n; ++i) {
                    if (lship[i] <= DATE_1995_03_15) continue;

                    int32_t ok = lkey[i];
                    if (!orders_bloom.may_contain(ok)) continue;

                    // Determine partition and probe the correct orders HT
                    int p = (int)(gendb::hash_int(ok) & (uint64_t)(NUM_PARTS - 1));
                    int32_t odate = 0, opri = 0;
                    if (!orders_ht[p].find(ok, odate, opri)) continue; // bloom FP

                    // Revenue: ep * (100 - discount) — correctness anchor
                    int64_t rev = lprice[i] * (100LL - ldisc[i]);

                    // Direct aggregation into thread-local map
                    AggVal* av = my_agg.find(ok);
                    if (av != nullptr) {
                        av->revenue_sum += rev;
                    } else {
                        my_agg.insert(ok, {rev, odate, opri});
                    }
                }
            }
        };

        {
            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(li_worker, t);
            for (auto& t : threads) t.join();
        }

        // ── Phase 4: Parallel aggregation merge by partition ─────────────────
        // Thread p collects all entries from tl_agg[0..63] where
        // hash(l_orderkey) & (NUM_PARTS-1) == p and merges them.
        {
            GENDB_PHASE("aggregation_merge");

            // Pre-count per-partition entries to reserve correctly
            std::vector<size_t> part_counts(NUM_PARTS, 0);
            for (int t = 0; t < NUM_THREADS; ++t) {
                for (auto [key, val] : tl_agg[t]) {
                    int p = (int)(gendb::hash_int(key) & (uint64_t)(NUM_PARTS - 1));
                    part_counts[p]++;
                }
            }
            for (int p = 0; p < NUM_PARTS; ++p) {
                agg_parts[p].reserve(std::max(part_counts[p], (size_t)64));
            }

            std::atomic<int> part_task{0};
            auto merge_worker = [&](int /*tid*/) {
                int p;
                while ((p = part_task.fetch_add(1, std::memory_order_relaxed)) < NUM_PARTS) {
                    AggMap& dst = agg_parts[p];
                    for (int t = 0; t < NUM_THREADS; ++t) {
                        for (auto [key, val] : tl_agg[t]) {
                            int rp = (int)(gendb::hash_int(key) & (uint64_t)(NUM_PARTS - 1));
                            if (rp != p) continue;
                            AggVal* av = dst.find(key);
                            if (av != nullptr) {
                                av->revenue_sum += val.revenue_sum;
                            } else {
                                dst.insert(key, val);
                            }
                        }
                    }
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(merge_worker, t);
            for (auto& t : threads) t.join();
        }

        } // end if li_start_row < total_lineitem
    }

    // ── Phase 5: Top-K extraction ─────────────────────────────────────────────
    std::vector<TopKEntry> top10;
    {
        GENDB_PHASE("sort_topk");

        size_t total_groups = 0;
        for (auto& ap : agg_parts) total_groups += ap.size();

        std::vector<TopKEntry> all;
        all.reserve(total_groups);

        for (auto& ap : agg_parts) {
            for (auto [key, val] : ap) {
                all.push_back({val.revenue_sum, key, val.o_orderdate, val.o_shippriority});
            }
        }

        size_t k = std::min((size_t)10, all.size());
        std::partial_sort(all.begin(), all.begin() + (ptrdiff_t)k, all.end(), final_cmp);
        top10.assign(all.begin(), all.begin() + (ptrdiff_t)k);
    }

    // ── Phase 6: Output results ───────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) {
            std::cerr << "ERROR: cannot open " << out_path << "\n";
            return;
        }

        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");

        for (const auto& e : top10) {
            // revenue_sum = ep_stored * (100 - disc_stored)
            // ep_stored = actual_price × 100, disc_stored = actual_disc × 100
            // revenue_sum = actual_revenue × 10000
            int64_t whole = e.revenue_sum / 10000LL;
            int64_t frac  = e.revenue_sum % 10000LL;
            int64_t cents = frac / 100;
            int64_t sub   = frac % 100;
            if (sub >= 50) cents++;
            if (cents >= 100) { whole++; cents -= 100; }

            char date_buf[12];
            gendb::epoch_days_to_date_str(e.o_orderdate, date_buf);
            fprintf(f, "%d,%lld.%02lld,%s,%d\n",
                    (int)e.l_orderkey,
                    (long long)whole,
                    (long long)cents,
                    date_buf,
                    (int)e.o_shippriority);
        }

        fclose(f);
    }
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
