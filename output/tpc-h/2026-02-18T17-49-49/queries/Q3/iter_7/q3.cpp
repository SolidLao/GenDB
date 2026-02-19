// Q3: Shipping Priority — iter_7
//
// Bottleneck analysis from iter_5/current (360ms):
//   main_scan: 180ms (50%) — root cause: pre-reserving 64×64×8192×12B = 384MB staging
//     vectors completely blows the 44MB L3 cache. Every hash probe misses cache.
//   build_joins: 138ms (38%) — root cause: CAS contention on shared concurrent HT,
//     plus 64 thread-local bloom copies (14.7MB) that must be parallel-merged.
//
// iter_6 (265ms, FAIL): Had the right architecture (partition-parallel build, direct
//   thread-local aggregation) but broke correctness in two places:
//   (a) atomic fetch_or on reinterpreted bloom words — UB risk
//   (b) aggregation merge scanned ALL thread-local maps for EACH partition
//       → O(64 × 64 × 22K) = 92M re-hash lookups just for merge
//
// NEW STRATEGY (iter_7):
//
// Phase 1: dim_filter — sequential customer bitmap (unchanged, ~12ms)
//
// Phase 2: build_orders — partition-parallel, zero CAS, zero atomic bloom:
//   Step 2a: 64 threads scan zone-map-pruned orders, each thread collects
//            qualifying rows into a thread-local flat vector (no partitioning yet).
//   Step 2b: Two-pass partition scatter:
//            Pass 1: Count per-partition sizes across all threads (parallel).
//            Pass 2: Each partition p pre-allocates arrays from counts, then
//                    each thread scatters its rows to pre-allocated arrays.
//            This is a classic radix-partition pattern — O(N) with perfect locality.
//   Step 2c: Thread p builds orders_ht[p] from its partition array + builds
//            bloom shard p (no contention: bloom is partitioned into 64 shards,
//            each shard owned by exactly one thread). Shards are bit-ranges of
//            the bloom word array, not key-partitioned, but since each thread
//            only builds bloom bits for its own partition's keys, we need a global
//            bloom. Solution: build bloom AFTER all partition HTs are built,
//            in a single parallel pass where each thread p inserts its HT keys.
//            Since each thread owns distinct bloom words (bloom_words / 64 per thread),
//            NO atomic ops are needed.
//            Bloom word ownership: thread p owns words [p*chunk .. (p+1)*chunk).
//            But keys hash to arbitrary bloom words → can't partition by word.
//            Simplest correct fix: build bloom single-threaded after Phase 2 ends.
//            Bloom build for 1.44M keys = ~3ms. Worth it to avoid atomics entirely.
//
// Phase 3: main_scan — DIRECT thread-local aggregation (NO staging vectors):
//   Each thread maintains a CompactHashMap<int32_t, AggVal> pre-sized for 32K groups.
//   Memory: 64 threads × 32K slots × ~24B = ~49MB total (fits within RAM budget,
//   each thread's map = ~768KB, stays hot if thread runs on same core for full morsel).
//   For each qualifying lineitem:
//     bloom check → identify partition p = hash(ok) & 63 → probe orders_ht[p]
//     → accumulate revenue in thread-local agg map (single find+update per row).
//   ZERO scatter overhead, ZERO staging pollution.
//
// Phase 4: aggregation_merge — partition-parallel, O(N_total) work:
//   Each thread p collects from ALL 64 thread-local agg maps the entries belonging
//   to partition p (hash(key) & 63 == p), merges them into agg_parts[p].
//   Total work = 1.44M entries × 1 pass = O(1.44M), parallelized across 64 threads.
//   Each thread does 1.44M/64 = ~22K entries → ~1-2ms total.
//   Key optimization vs iter_6: we iterate each thread's map and check hash(key)&63==p.
//   This is O(table_size) per thread-map (including empty slots), not O(entries).
//   To avoid that overhead: after Phase 3, each thread first writes its agg map into
//   a flat sorted-by-partition array (one pass), then Phase 4 threads read from those.
//
// Phase 5: Top-K + output (unchanged)
//
// Expected performance: ~180-220ms (down from 360ms)

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
#include <immintrin.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ── Constants ────────────────────────────────────────────────────────────────
static constexpr int32_t DATE_1995_03_15 = 9204; // days since epoch
static constexpr int32_t MAX_CUSTKEY     = 1500001;
static constexpr int     NUM_THREADS     = 64;
static constexpr int     NUM_PARTS       = 64; // power-of-2 for fast modulo
static constexpr uint64_t PART_MASK      = (uint64_t)(NUM_PARTS - 1);

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

// ── Per-partition orders hash table (non-concurrent, cache-efficient) ────────
// Custom open-addressing HT with separate arrays for better cache behavior.
// Each partition is built and owned by a single thread — no synchronization.
// Why custom: need parallel arrays (not AoS) for better memory access on probe,
// and need zero-sentinel (TPC-H orderkeys start at 1, never 0).
struct PartitionOrdersHT {
    static constexpr int32_t EMPTY = 0;

    int32_t* keys;
    int32_t* dates;
    int32_t* pris;
    uint64_t mask;
    size_t   cap;

    PartitionOrdersHT() : keys(nullptr), dates(nullptr), pris(nullptr), mask(0), cap(0) {}

    ~PartitionOrdersHT() {
        delete[] keys;
        delete[] dates;
        delete[] pris;
    }

    PartitionOrdersHT(const PartitionOrdersHT&) = delete;
    PartitionOrdersHT& operator=(const PartitionOrdersHT&) = delete;

    void init(size_t expected) {
        cap = 16;
        while (cap < expected * 5 / 2) cap <<= 1; // ~40% load factor
        mask = cap - 1;
        keys  = new int32_t[cap]();  // zero-initialized = EMPTY
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

// ── Bloom filter (shared, built single-threaded after orders build) ───────────
// Sized for ~1.44M keys at 10 bits/key = ~1.8MB → always L3 resident.
// Built sequentially in ~3ms to avoid atomic overhead entirely.
struct BloomFilter {
    std::vector<uint64_t> bits_;
    size_t total_bits_;

    void init(size_t capacity) {
        size_t nbits = capacity * 12 + 64; // 12 bits/key for ~0.2% FP rate
        total_bits_  = (nbits + 63) & ~63ULL;
        bits_.assign(total_bits_ / 64, 0ULL);
    }

    inline void insert(int32_t key) {
        uint64_t h  = gendb::hash_int(key);
        uint64_t h2 = h ^ (h >> 17) ^ (h << 31);
        uint64_t h3 = h ^ (h >> 23) ^ (h << 47);
        bits_[(h  % total_bits_) >> 6] |= (1ULL << ((h  % total_bits_) & 63));
        bits_[(h2 % total_bits_) >> 6] |= (1ULL << ((h2 % total_bits_) & 63));
        bits_[(h3 % total_bits_) >> 6] |= (1ULL << ((h3 % total_bits_) & 63));
    }

    inline bool may_contain(int32_t key) const {
        uint64_t h  = gendb::hash_int(key);
        uint64_t h2 = h ^ (h >> 17) ^ (h << 31);
        uint64_t h3 = h ^ (h >> 23) ^ (h << 47);
        return (bits_[(h  % total_bits_) >> 6] >> ((h  % total_bits_) & 63)) & 1ULL &&
               (bits_[(h2 % total_bits_) >> 6] >> ((h2 % total_bits_) & 63)) & 1ULL &&
               (bits_[(h3 % total_bits_) >> 6] >> ((h3 % total_bits_) & 63)) & 1ULL;
    }
};

// ── Collected order row for scatter phase ────────────────────────────────────
struct OrderRow {
    int32_t o_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// ── Flat agg row for partition-sorted merge ───────────────────────────────────
// After main_scan, each thread serializes its agg map to a flat vector,
// sorted by partition. Phase 4 threads read their shard from each thread's vector.
struct FlatAggRow {
    int32_t l_orderkey;
    int64_t revenue_sum;
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
    //
    // Strategy: zero CAS, zero atomic, zero contention.
    //
    // Step 2a: Each thread scans zone-map-pruned orders and collects qualifying
    //          rows into a thread-local vector. No partitioning yet.
    // Step 2b: Radix-partition scatter:
    //          - Count pass: in parallel, each thread counts its rows per partition
    //          - Prefix-sum: compute global write offsets per partition (sequential, fast)
    //          - Scatter pass: each thread scatters its rows into flat per-partition arrays
    // Step 2c: Thread p builds orders_ht[p] from its partition slice.
    // Step 2d: Build bloom filter single-threaded from all partition HTs (~3ms, no atomics).

    std::vector<PartitionOrdersHT> orders_ht(NUM_PARTS);
    BloomFilter orders_bloom;
    orders_bloom.init(1500000UL);

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey    (gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey     (gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate   (gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_shippriority);

        size_t total_orders = o_orderkey.size();

        // Load zone map to skip blocks with all dates >= DATE_1995_03_15
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

        // Step 2a: Thread-local collection
        std::vector<std::vector<OrderRow>> tl_rows(NUM_THREADS);
        for (auto& v : tl_rows) v.reserve(28000); // ~1.44M / 64 = ~22500 per thread + slack

        {
            std::atomic<size_t> range_idx{0};
            auto collect_worker = [&](int tid) {
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

            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(collect_worker, t);
            for (auto& t : threads) t.join();
        }

        // Step 2b: Count total rows per partition
        std::vector<size_t> part_counts(NUM_PARTS, 0);
        for (int t = 0; t < NUM_THREADS; ++t) {
            for (const auto& row : tl_rows[t]) {
                int p = (int)(gendb::hash_int(row.o_orderkey) & PART_MASK);
                part_counts[p]++;
            }
        }

        // Allocate flat per-partition arrays
        std::vector<std::vector<OrderRow>> part_rows(NUM_PARTS);
        for (int p = 0; p < NUM_PARTS; ++p)
            part_rows[p].resize(part_counts[p]);

        // Scatter: each thread scatters its rows using atomic write offsets per partition
        // Use atomic per-partition write cursors for contention-free scatter.
        // Since scatter is O(N_total) and just writes, contention is minimal.
        std::vector<std::atomic<size_t>> part_write_pos(NUM_PARTS);
        for (int p = 0; p < NUM_PARTS; ++p)
            part_write_pos[p].store(0, std::memory_order_relaxed);

        {
            std::atomic<int> tid_counter{0};
            auto scatter_worker = [&](int /*tid*/) {
                int my_tid = tid_counter.fetch_add(1, std::memory_order_relaxed);
                for (const auto& row : tl_rows[my_tid]) {
                    int p = (int)(gendb::hash_int(row.o_orderkey) & PART_MASK);
                    size_t pos = part_write_pos[p].fetch_add(1, std::memory_order_relaxed);
                    part_rows[p][pos] = row;
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(scatter_worker, t);
            for (auto& t : threads) t.join();
        }

        // Free thread-local row buffers early to reduce memory pressure
        for (auto& v : tl_rows) { std::vector<OrderRow>().swap(v); }

        // Step 2c: Each thread p builds its partition HT (no contention)
        // Init all HTs first, then parallel build
        for (int p = 0; p < NUM_PARTS; ++p)
            orders_ht[p].init(std::max(part_counts[p], (size_t)64));

        {
            std::atomic<int> part_task{0};
            auto build_worker = [&](int /*tid*/) {
                int p;
                while ((p = part_task.fetch_add(1, std::memory_order_relaxed)) < NUM_PARTS) {
                    auto& ht = orders_ht[p];
                    for (const auto& row : part_rows[p]) {
                        ht.insert(row.o_orderkey, row.o_orderdate, row.o_shippriority);
                    }
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(build_worker, t);
            for (auto& t : threads) t.join();
        }

        // Free partition row arrays
        for (auto& v : part_rows) { std::vector<OrderRow>().swap(v); }

        // Step 2d: Build bloom filter single-threaded (no atomics, ~3ms for 1.44M keys)
        // Iterate partition HTs and insert each key into bloom.
        for (int p = 0; p < NUM_PARTS; ++p) {
            auto& ht = orders_ht[p];
            for (size_t i = 0; i < ht.cap; ++i) {
                if (ht.keys[i] != PartitionOrdersHT::EMPTY) {
                    orders_bloom.insert(ht.keys[i]);
                }
            }
        }
    }

    // ── Phase 3: Scan lineitem + direct thread-local aggregation ─────────────
    //
    // Each thread maintains its own CompactHashMap<int32_t, AggVal>.
    // Pre-sized for 32K groups → ~768KB per thread HT, keeps hot in cache.
    // For each qualifying row: bloom → partition probe → local agg update.
    // NO staging vectors, NO scatter, NO cache pollution.

    using AggMap = gendb::CompactHashMap<int32_t, AggVal>;
    std::vector<AggMap> tl_agg(NUM_THREADS);

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey     (gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount      (gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate      (gendb_dir + "/lineitem/l_shipdate.bin");

        mmap_prefetch_all(l_orderkey, l_extendedprice, l_discount, l_shipdate);

        size_t total_lineitem = l_orderkey.size();

        // Zone-map skip: lineitem sorted by l_shipdate → binary search for first qualifying block
        static constexpr size_t LI_BLOCK = 100000;
        size_t num_blocks = (total_lineitem + LI_BLOCK - 1) / LI_BLOCK;
        size_t first_qualifying_block = num_blocks;
        {
            const int32_t* lship = l_shipdate.data;
            size_t lo = 0, hi = num_blocks;
            while (lo < hi) {
                size_t mid = (lo + hi) / 2;
                size_t bend = std::min((mid + 1) * LI_BLOCK, total_lineitem);
                if (lship[bend - 1] > DATE_1995_03_15) hi = mid;
                else lo = mid + 1;
            }
            first_qualifying_block = lo;
        }

        size_t li_start_row = first_qualifying_block * LI_BLOCK;
        if (li_start_row >= total_lineitem) goto done_scan;

        {
            // Pre-size thread-local agg maps
            // ~1.44M groups / 64 threads = ~22.5K per thread; reserve 32K for slack
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

                        // Identify partition and probe orders HT
                        int p = (int)(gendb::hash_int(ok) & PART_MASK);
                        int32_t odate = 0, opri = 0;
                        if (!orders_ht[p].find(ok, odate, opri)) continue; // bloom FP

                        // Revenue: ep * (100 - discount) — correctness anchor
                        int64_t rev = lprice[i] * (100LL - ldisc[i]);

                        // Direct thread-local aggregation
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
        }
    }
    done_scan:;

    // ── Phase 4: Aggregation merge — partition-parallel ───────────────────────
    //
    // Strategy: serialize each thread's agg map to a partition-bucketed flat array,
    // then thread p drains its bucket from all 64 threads' arrays.
    //
    // This avoids re-scanning entire HT tables with branch-on-partition checks,
    // which was the O(N×64) bottleneck in iter_6's merge.
    //
    // Step 4a: Each thread serializes its agg map to a flat vector (one pass, O(entries)).
    // Step 4b: Thread p reads shard p from all 64 flat vectors and builds agg_parts[p].

    using AggMap2 = gendb::CompactHashMap<int32_t, AggVal>;
    std::vector<AggMap2> agg_parts(NUM_PARTS);

    {
        GENDB_PHASE("aggregation_merge");

        // Count entries per thread for sizing
        size_t total_entries = 0;
        for (int t = 0; t < NUM_THREADS; ++t) total_entries += tl_agg[t].size();

        // Step 4a: Each thread serializes its agg map into per-partition flat buckets.
        // Layout: tl_flat[t][p] = flat vector of FlatAggRow for thread t, partition p.
        // We use a 2-level structure: tl_flat[t] is an array of NUM_PARTS vectors.
        // To avoid 64×64 = 4096 vectors (with their overhead), use a flat layout:
        // one flat array per thread, sorted by partition prefix.
        //
        // Actually: use per-thread flat arrays with partition offsets.
        // Count pass → prefix sum → scatter into flat per-thread array.

        // Per-thread partition counts
        std::vector<std::array<size_t, NUM_PARTS>> tl_part_counts(NUM_THREADS);
        for (int t = 0; t < NUM_THREADS; ++t) {
            auto& cnt = tl_part_counts[t];
            cnt.fill(0);
            for (auto [key, val] : tl_agg[t]) {
                int p = (int)(gendb::hash_int(key) & PART_MASK);
                cnt[p]++;
            }
        }

        // Per-thread prefix sums and flat arrays
        std::vector<std::array<size_t, NUM_PARTS + 1>> tl_offsets(NUM_THREADS);
        std::vector<std::vector<FlatAggRow>> tl_flat(NUM_THREADS);

        for (int t = 0; t < NUM_THREADS; ++t) {
            auto& off = tl_offsets[t];
            auto& cnt = tl_part_counts[t];
            off[0] = 0;
            for (int p = 0; p < NUM_PARTS; ++p)
                off[p + 1] = off[p] + cnt[p];
            tl_flat[t].resize(tl_agg[t].size());

            // Scatter into flat array by partition
            std::array<size_t, NUM_PARTS> write_pos;
            for (int p = 0; p < NUM_PARTS; ++p) write_pos[p] = off[p];

            for (auto [key, val] : tl_agg[t]) {
                int p = (int)(gendb::hash_int(key) & PART_MASK);
                tl_flat[t][write_pos[p]++] = {key, val.revenue_sum, val.o_orderdate, val.o_shippriority};
            }
        }

        // Free thread-local agg maps (no longer needed)
        for (auto& am : tl_agg) { AggMap tmp; std::swap(tmp, am); }

        // Compute per-partition total counts for HT pre-sizing
        std::vector<size_t> part_total(NUM_PARTS, 0);
        for (int t = 0; t < NUM_THREADS; ++t)
            for (int p = 0; p < NUM_PARTS; ++p)
                part_total[p] += tl_part_counts[t][p];

        for (int p = 0; p < NUM_PARTS; ++p)
            agg_parts[p].reserve(std::max(part_total[p], (size_t)64));

        // Step 4b: Parallel merge — thread p owns partition p
        std::atomic<int> part_task{0};
        auto merge_worker = [&](int /*tid*/) {
            int p;
            while ((p = part_task.fetch_add(1, std::memory_order_relaxed)) < NUM_PARTS) {
                AggMap2& dst = agg_parts[p];
                for (int t = 0; t < NUM_THREADS; ++t) {
                    size_t start = tl_offsets[t][p];
                    size_t end   = tl_offsets[t][p + 1];
                    for (size_t i = start; i < end; ++i) {
                        const auto& row = tl_flat[t][i];
                        AggVal* av = dst.find(row.l_orderkey);
                        if (av != nullptr) {
                            av->revenue_sum += row.revenue_sum;
                        } else {
                            dst.insert(row.l_orderkey,
                                       {row.revenue_sum, row.o_orderdate, row.o_shippriority});
                        }
                    }
                }
            }
        };

        {
            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(merge_worker, t);
            for (auto& t : threads) t.join();
        }
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
