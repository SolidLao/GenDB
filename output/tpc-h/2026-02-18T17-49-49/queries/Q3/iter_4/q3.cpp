// Q3: Shipping Priority — iter_4 optimizations
// Key architectural changes vs iter_1 (best so far at 395ms):
//
//  BOTTLENECK: build_joins (187ms, 47%)
//    Root cause: 2-pass approach with 64×64=4096 intermediate vectors.
//    Pass B has extremely poor cache behavior: thread p reads from 64 scattered
//    thread_bufs[0..63][p] buffers → thrashes L3, high cache-miss rate.
//
//  FIX 1: Single-pass concurrent orders build using ConcurrentCompactHashMap.
//    All 64 threads scan orders and insert directly via lock-free CAS.
//    Eliminates 4096 intermediate vectors (no intermediate memory pressure).
//    Pre-size map to 4× matched orders to reduce CAS contention (fewer probes).
//
//  FIX 2: Aggregation merge redesign — O(local) instead of O(N_THREADS × total).
//    Old: each merge thread p scans ALL 64 thread-local maps, filtering by partition.
//    New: each thread iterates ONLY its own local map and inserts into the right
//         global partition map. Sequential access to own map = cache friendly.
//         Partition maps use a simple spinlock (rarely contested since ~1.44M keys
//         spread across 64 partitions → ~22K entries/partition → very fast insert).
//
//  FIX 3: Prefetch ALL columns upfront (before customer scan) so HDD I/O for
//         orders + lineitem columns overlaps with the customer bitmap build.
//
//  FIX 4: OpenMP for lineitem scan instead of std::thread — reduced overhead.
//
//  Correctness anchors (DO NOT MODIFY):
//    revenue = ep * (100 - discount)  [scaled: ep_stored * (100 - disc_stored)]

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
#include <mutex>
#include <omp.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ── Constants ────────────────────────────────────────────────────────────────
static constexpr int32_t DATE_1995_03_15 = 9204; // days since epoch (1995-03-15)
static constexpr int32_t MAX_CUSTKEY     = 1500001;
static constexpr int     NUM_THREADS     = 64;
static constexpr size_t  NUM_PARTITIONS  = 64;

// ── Order payload stored in hash map ─────────────────────────────────────────
struct OrderPayload {
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// ── Aggregation value per group (keyed by l_orderkey) ─────────────────────────
struct AggVal {
    int64_t revenue_sum;   // scaled ×10000
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

// For final sort: revenue DESC, o_orderdate ASC
static bool final_cmp(const TopKEntry& a, const TopKEntry& b) {
    if (a.revenue_sum != b.revenue_sum) return a.revenue_sum > b.revenue_sum;
    return a.o_orderdate < b.o_orderdate;
}

// For TopKHeap: cmp(a,b)=true means a is "worse" (evicted sooner from top-k)
struct TopKCmp {
    bool operator()(const TopKEntry& a, const TopKEntry& b) const {
        if (a.revenue_sum != b.revenue_sum) return a.revenue_sum < b.revenue_sum;
        return a.o_orderdate > b.o_orderdate;
    }
};

// ── Bloom filter (2 hash functions, ~10 bits/key) ────────────────────────────
struct BloomFilter {
    std::vector<uint64_t> bits_;
    size_t total_bits_;

    void init(size_t capacity) {
        size_t nbits = capacity * 10 + 64;
        total_bits_ = (nbits + 63) & ~63ULL;
        bits_.assign(total_bits_ / 64, 0ULL);
    }

    inline void set_bit(size_t b) {
        bits_[b >> 6] |= (1ULL << (b & 63));
    }
    inline bool get_bit(size_t b) const {
        return (bits_[b >> 6] >> (b & 63)) & 1ULL;
    }

    void insert(int32_t key) {
        uint64_t h  = gendb::hash_int((int32_t)key);
        uint64_t h2 = h ^ (h >> 17) ^ (h << 31);
        set_bit(h  % total_bits_);
        set_bit(h2 % total_bits_);
    }

    bool may_contain(int32_t key) const {
        uint64_t h  = gendb::hash_int((int32_t)key);
        uint64_t h2 = h ^ (h >> 17) ^ (h << 31);
        return get_bit(h  % total_bits_) &&
               get_bit(h2 % total_bits_);
    }
};

// ── Main query function ───────────────────────────────────────────────────────
void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();
    omp_set_num_threads(NUM_THREADS);

    // ── Open all columns upfront and prefetch everything ──────────────────────
    // Prefetch orders + lineitem before customer scan so HDD I/O overlaps
    // with the cheap customer bitmap build.
    gendb::MmapColumn<int32_t> o_orderkey    (gendb_dir + "/orders/o_orderkey.bin");
    gendb::MmapColumn<int32_t> o_custkey     (gendb_dir + "/orders/o_custkey.bin");
    gendb::MmapColumn<int32_t> o_orderdate   (gendb_dir + "/orders/o_orderdate.bin");
    gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

    gendb::MmapColumn<int32_t> l_orderkey      (gendb_dir + "/lineitem/l_orderkey.bin");
    gendb::MmapColumn<int64_t> l_extendedprice (gendb_dir + "/lineitem/l_extendedprice.bin");
    gendb::MmapColumn<int64_t> l_discount      (gendb_dir + "/lineitem/l_discount.bin");
    gendb::MmapColumn<int32_t> l_shipdate      (gendb_dir + "/lineitem/l_shipdate.bin");

    // Fire all prefetches immediately — overlaps HDD I/O with CPU work below
    mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_shippriority);
    mmap_prefetch_all(l_orderkey, l_extendedprice, l_discount, l_shipdate);

    // ── Phase 1: Build customer bitset ────────────────────────────────────────
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

        // Sequential: 1.5M rows × 2 bytes = 3MB — fast, and bitset writes are non-atomic
        for (size_t i = 0; i < n; ++i) {
            if (seg[i] == building_code)
                cust_bitmap.set((size_t)ckey[i]);
        }
    }

    // ── Phase 2: Build orders hash map — single-pass concurrent ──────────────
    // FIX: Replace the old 2-pass (64×64 buffers → insert) with a single pass
    // using ConcurrentCompactHashMap (lock-free CAS insert).
    //
    // Estimated matches: ~1.44M orders. Pre-size 4× to reduce collision chains.
    // ConcurrentCompactHashMap uses linear probing + atomic CAS: each thread
    // inserts directly with no intermediate buffering. Eliminates the 4096-vector
    // memory pressure and the cache-unfriendly Pass B strided access pattern.
    //
    // Since ConcurrentCompactHashMap stores value atomically, we store
    // OrderPayload as two int32_t packed into one int64_t for atomic update.
    // (o_orderdate in high 32 bits, o_shippriority in low 32 bits)
    //
    // Alternative: use 64 partitions with per-partition spinlocks and
    // CompactHashMap. With ~1.44M / 64 = ~22K entries/partition and 64 threads,
    // contention per partition lock = 64/64 = 1 thread → effectively no wait.

    // We use 64 partitioned CompactHashMaps with spinlocks (std::atomic_flag).
    // This avoids the ConcurrentCompactHashMap's int64_t key restriction and
    // lets us store the full OrderPayload without packing tricks.

    struct alignas(64) PartitionedOrderMap {
        gendb::CompactHashMap<int32_t, OrderPayload> map;
        std::atomic_flag lock = ATOMIC_FLAG_INIT;
    };

    std::vector<PartitionedOrderMap> orders_pmaps(NUM_PARTITIONS);

    BloomFilter orders_bloom;
    std::atomic<size_t> total_matched_orders{0};

    {
        GENDB_PHASE("build_joins");

        size_t total_orders = o_orderkey.size();

        // Load zone map for o_orderdate — skip blocks with min >= DATE_1995_03_15
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

        // Pre-size each partition map.
        // Estimated: 1.44M matches / 64 partitions ≈ 22500 entries each.
        // Reserve 2× for headroom.
        for (size_t p = 0; p < NUM_PARTITIONS; ++p)
            orders_pmaps[p].map.reserve(48000);

        // Single-pass parallel scan: thread t grabs ranges via atomic counter,
        // filters, then inserts into the appropriate partition map via spinlock.
        // Spinlock contention is low: 64 threads × 64 partitions → expected wait
        // is ~1 thread per partition at any moment → effectively spin-free.
        std::atomic<size_t> range_idx{0};

        auto orders_scan = [&]() {
            size_t local_count = 0;
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
                        int32_t okey_val = okey[i];
                        size_t part = gendb::hash_int(okey_val) % NUM_PARTITIONS;

                        PartitionedOrderMap& pm = orders_pmaps[part];
                        // Spinlock: acquire
                        while (pm.lock.test_and_set(std::memory_order_acquire)) {
                            // spin — expected wait near zero with 64 partitions
                        }
                        pm.map.insert(okey_val, {odate[i], oship[i]});
                        pm.lock.clear(std::memory_order_release);
                        ++local_count;
                    }
                }
            }
            total_matched_orders.fetch_add(local_count, std::memory_order_relaxed);
        };

        {
            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(orders_scan);
            for (auto& t : threads) t.join();
        }

        // Build bloom filter: iterate all partition maps (sequential, cheap)
        size_t total_matched = total_matched_orders.load(std::memory_order_relaxed);
        orders_bloom.init(std::max(total_matched, (size_t)1));

        for (size_t p = 0; p < NUM_PARTITIONS; ++p) {
            for (auto [key, val] : orders_pmaps[p].map) {
                orders_bloom.insert(key);
            }
        }
    }

    // ── Fast partition lookup helper ───────────────────────────────────────────
    auto orders_find = [&](int32_t orderkey) -> const OrderPayload* {
        size_t part = gendb::hash_int(orderkey) % NUM_PARTITIONS;
        return orders_pmaps[part].map.find(orderkey);
    };

    // ── Phase 3: Scan lineitem, aggregate ─────────────────────────────────────
    // lineitem sorted by l_shipdate → skip early blocks where max <= DATE_1995_03_15
    static constexpr size_t LI_BLOCK = 100000;

    using AggMap = gendb::CompactHashMap<int32_t, AggVal>;

    struct alignas(64) ThreadState {
        AggMap agg;
        ThreadState() { agg.reserve(32768); }
    };

    std::vector<ThreadState> thread_states(NUM_THREADS);

    {
        GENDB_PHASE("main_scan");

        size_t total_lineitem = l_orderkey.size();
        size_t num_blocks = (total_lineitem + LI_BLOCK - 1) / LI_BLOCK;

        // Find first block that could have qualifying rows (l_shipdate > DATE_1995_03_15)
        // Since sorted ascending, once we find a block where last row > cutoff,
        // all subsequent blocks also have qualifying rows (at least partially).
        size_t first_qualifying_block = num_blocks; // pessimistic default
        {
            const int32_t* lship = l_shipdate.data;
            for (size_t b = 0; b < num_blocks; ++b) {
                size_t block_end = std::min((b + 1) * LI_BLOCK, total_lineitem);
                if (lship[block_end - 1] > DATE_1995_03_15) {
                    first_qualifying_block = b;
                    break;
                }
            }
        }

        static constexpr size_t MORSEL = 65536;
        size_t li_start_row = first_qualifying_block * LI_BLOCK;
        size_t num_morsels = (total_lineitem > li_start_row)
            ? (total_lineitem - li_start_row + MORSEL - 1) / MORSEL
            : 0;

        std::atomic<size_t> morsel_idx{0};

        auto lineitem_worker = [&](int tid) {
            ThreadState& ts = thread_states[tid];

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

                    int32_t orderkey = lkey[i];

                    // Bloom filter pre-check: ~70-80% of non-matching keys eliminated
                    if (!orders_bloom.may_contain(orderkey)) continue;

                    // revenue = ep * (100 - discount) — DO NOT MODIFY (correctness anchor)
                    int64_t rev = lprice[i] * (100LL - ldisc[i]);

                    // Probe thread-local aggregation map
                    AggVal* agg_val = ts.agg.find(orderkey);
                    if (agg_val != nullptr) {
                        agg_val->revenue_sum += rev;
                    } else {
                        const OrderPayload* op = orders_find(orderkey);
                        if (op == nullptr) continue;
                        ts.agg.insert(orderkey, {rev, op->o_orderdate, op->o_shippriority});
                    }
                }
            }
        };

        {
            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(lineitem_worker, t);
            for (auto& t : threads) t.join();
        }
    }

    // ── Phase 4: Aggregation merge — redesigned ───────────────────────────────
    // OLD approach (slow): each merge thread p iterates ALL 64 thread-local maps,
    //   filters entries by (hash(key) % 64 == p) → O(64 × total_agg_size) total
    //   work with terrible cache behavior (reading 63 other maps' entries).
    //
    // NEW approach (fast): each thread t iterates ONLY its OWN local map (hot in
    //   L3/L2 from the lineitem scan), and pushes each entry into the correct
    //   global partition map. Each global partition has a spinlock.
    //   Total work: O(total_agg_size) with sequential access to each local map.
    //   Spinlock contention: 64 threads / 64 partitions ≈ 1 thread/partition → minimal.

    using GlobalAggPart = gendb::CompactHashMap<int32_t, AggVal>;
    std::vector<GlobalAggPart> global_parts(NUM_PARTITIONS);
    std::vector<std::atomic_flag> part_locks(NUM_PARTITIONS);
    for (size_t p = 0; p < NUM_PARTITIONS; ++p) {
        part_locks[p].clear();  // initialize to unlocked
    }

    {
        GENDB_PHASE("aggregation_merge");

        // Pre-size global partitions
        size_t total_local = 0;
        for (auto& ts : thread_states) total_local += ts.agg.size();
        size_t per_part = std::max(total_local / NUM_PARTITIONS + 256, (size_t)512);
        for (auto& gp : global_parts) gp.reserve(per_part);

        // Each thread iterates its OWN local map (cache-warm) and inserts
        // into the appropriate global partition (with spinlock).
        auto merge_worker = [&](int tid) {
            ThreadState& ts = thread_states[tid];

            for (auto [key, val] : ts.agg) {
                size_t p = gendb::hash_int(key) % NUM_PARTITIONS;

                // Acquire spinlock for this partition
                while (part_locks[p].test_and_set(std::memory_order_acquire)) {
                    // spin
                }

                GlobalAggPart& gp = global_parts[p];
                AggVal* gv = gp.find(key);
                if (gv == nullptr) {
                    gp.insert(key, val);
                } else {
                    gv->revenue_sum += val.revenue_sum;
                }

                part_locks[p].clear(std::memory_order_release);
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

        std::vector<TopKEntry> all;
        size_t total_groups = 0;
        for (auto& gp : global_parts) total_groups += gp.size();
        all.reserve(total_groups);

        for (auto& gp : global_parts) {
            for (auto [key, val] : gp) {
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
