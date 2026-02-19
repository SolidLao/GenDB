// Q3: Shipping Priority — iter_1 optimizations
// Key changes vs iter_0:
//  1. Partitioned parallel orders build (64 partitions) — eliminates sequential insert bottleneck
//  2. Lineitem zone-map skipping on l_shipdate sort order — skip ~48% of lineitem blocks
//  3. Parallel aggregation merge — 64 threads each own 1 partition of global agg map
//  4. Use operator[] (find-or-insert) in hot agg loop — one hash lookup instead of two

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
static constexpr int32_t DATE_1995_03_15 = 9204; // days since epoch (1995-03-15)
static constexpr int32_t MAX_CUSTKEY     = 1500001;
static constexpr int     NUM_THREADS     = 64;
static constexpr size_t  NUM_PARTITIONS  = 64; // must match NUM_THREADS for clean ownership

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

// ── Simple Bloom filter (2 hash functions, ~10 bits/key) ─────────────────────
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

        for (size_t i = 0; i < n; ++i) {
            if (seg[i] == building_code)
                cust_bitmap.set((size_t)ckey[i]);
        }
    }

    // ── Phase 2: Build orders partitioned hash map (fully parallel) ───────────
    // Strategy: 64 partitions, thread t owns partition t.
    // Each orders-scan thread writes ONLY to its partition (no locking).
    // Partition assignment: hash(o_orderkey) % 64.
    //
    // Two-pass approach:
    //   Pass A (parallel): each thread scans its zone-map ranges and distributes
    //                      matched orders into per-thread thread-local buffers
    //                      per partition. Then in Pass B each thread inserts its
    //                      own partition's data from all 64 local buffers into
    //                      its partition map.
    // This avoids ANY sequential bottleneck and any synchronization during insert.

    // We use a 2D structure: thread_bufs[tid][partition] = vector of matched orders
    struct MatchedOrder { int32_t orderkey; int32_t orderdate; int32_t shippriority; };

    // Partitioned orders map: partition p is owned exclusively by thread p
    using OrdersPartMap = gendb::CompactHashMap<int32_t, OrderPayload>;
    std::vector<OrdersPartMap> orders_parts(NUM_PARTITIONS);

    BloomFilter orders_bloom;

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey    (gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey     (gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate   (gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        // Prefetch all orders columns
        mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_shippriority);

        size_t total_orders = o_orderkey.size();

        // Load zone map for o_orderdate
        gendb::ZoneMapIndex zone_map(gendb_dir + "/indexes/orders_o_orderdate.bin");

        // Find qualifying row ranges: keep zones where min < DATE_1995_03_15
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

        // Pass A: parallel scan → per-thread, per-partition buffers
        // thread_bufs[tid][part] = matched orders destined for partition `part`
        std::vector<std::vector<std::vector<MatchedOrder>>> thread_bufs(
            NUM_THREADS, std::vector<std::vector<MatchedOrder>>(NUM_PARTITIONS));

        std::atomic<size_t> range_idx{0};

        auto orders_scan = [&](int tid) {
            // Pre-allocate partition buffers to reduce reallocations
            for (auto& pb : thread_bufs[tid]) pb.reserve(1024);

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
                        thread_bufs[tid][part].push_back({okey_val, odate[i], oship[i]});
                    }
                }
            }
        };

        {
            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(orders_scan, t);
            for (auto& t : threads) t.join();
        }

        // Count total matched orders across all partitions for sizing
        std::vector<size_t> part_counts(NUM_PARTITIONS, 0);
        size_t total_matched = 0;
        for (int tid = 0; tid < NUM_THREADS; ++tid) {
            for (size_t p = 0; p < NUM_PARTITIONS; ++p) {
                part_counts[p] += thread_bufs[tid][p].size();
                total_matched  += thread_bufs[tid][p].size();
            }
        }

        // Pre-size partition maps
        for (size_t p = 0; p < NUM_PARTITIONS; ++p) {
            orders_parts[p].reserve(std::max(part_counts[p] + part_counts[p] / 2, (size_t)256));
        }

        // Initialize bloom filter
        orders_bloom.init(std::max(total_matched, (size_t)1));

        // Pass B: parallel insert — thread p inserts all data for partition p
        // Each thread exclusively owns its partition → zero contention
        std::atomic<size_t> part_idx{0};

        auto orders_insert = [&](int /*tid*/) {
            while (true) {
                size_t p = part_idx.fetch_add(1, std::memory_order_relaxed);
                if (p >= NUM_PARTITIONS) break;

                OrdersPartMap& pm = orders_parts[p];
                for (int src_tid = 0; src_tid < NUM_THREADS; ++src_tid) {
                    for (auto& m : thread_bufs[src_tid][p]) {
                        pm.insert(m.orderkey, {m.orderdate, m.shippriority});
                    }
                }
            }
        };

        // Bloom filter build is sequential (bit-twiddling, fast)
        // Run partition inserts in parallel, bloom after
        {
            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(orders_insert, t);
            for (auto& t : threads) t.join();
        }

        // Build bloom filter sequentially (cheap: ~1.44M keys, ~14ms)
        for (size_t p = 0; p < NUM_PARTITIONS; ++p) {
            for (auto [key, val] : orders_parts[p]) {
                orders_bloom.insert(key);
            }
        }
    }

    // ── Fast partition lookup helper ───────────────────────────────────────────
    // Inline lambda for probing the partitioned orders map
    auto orders_find = [&](int32_t orderkey) -> const OrderPayload* {
        size_t part = gendb::hash_int(orderkey) % NUM_PARTITIONS;
        return orders_parts[part].find(orderkey);
    };

    // ── Phase 3: Scan lineitem with zone-map skipping, aggregate ──────────────
    // lineitem is sorted by l_shipdate → skip early blocks where max <= DATE_1995_03_15
    // We implement manual zone-map: process blocks of 100000 rows, check first row
    // to determine if entire block can be skipped (since sorted, first row = min).
    // Block size matches the GenDB block size (100000 rows per the Storage Guide).

    using AggMap = gendb::CompactHashMap<int32_t, AggVal>;

    struct alignas(64) ThreadState {
        AggMap agg;
        gendb::TopKHeap<TopKEntry, TopKCmp> heap;
        ThreadState() : heap(10) { agg.reserve(32768); }
    };

    std::vector<ThreadState> thread_states(NUM_THREADS);

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey      (gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice (gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount       (gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate       (gendb_dir + "/lineitem/l_shipdate.bin");

        // Prefetch all lineitem columns
        mmap_prefetch_all(l_orderkey, l_extendedprice, l_discount, l_shipdate);

        size_t total_lineitem = l_orderkey.size();

        // Build qualifying morsel ranges using l_shipdate sort order zone-map skip.
        // lineitem block size = 100000 rows (from Storage Guide).
        // Since lineitem is sorted by l_shipdate, any block where the FIRST row
        // has l_shipdate > DATE_1995_03_15 means all rows in that block qualify.
        // Any block where the LAST row has l_shipdate <= DATE_1995_03_15 can be skipped.
        // Blocks in between need per-row checking (transition zone).
        static constexpr size_t LI_BLOCK = 100000;
        size_t num_blocks = (total_lineitem + LI_BLOCK - 1) / LI_BLOCK;

        // Find first block that could have qualifying rows (max of block > DATE_1995_03_15)
        // Since sorted ascending, once we find the first block where last row > cutoff,
        // all subsequent blocks will also qualify (at least partially).
        // Skip blocks where the entire block is <= DATE_1995_03_15.
        size_t first_qualifying_block = 0;
        {
            const int32_t* lship = l_shipdate.data;
            for (size_t b = 0; b < num_blocks; ++b) {
                size_t block_end = std::min((b + 1) * LI_BLOCK, total_lineitem);
                // Last row of block = block_end - 1 (since sorted, this is the max)
                if (lship[block_end - 1] > DATE_1995_03_15) {
                    first_qualifying_block = b;
                    break;
                }
                // All rows in this block are <= DATE_1995_03_15 → skip
                if (b == num_blocks - 1) {
                    first_qualifying_block = num_blocks; // nothing qualifies
                }
            }
        }

        // Build morsel list starting from first_qualifying_block
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

                    // Bloom filter pre-check
                    if (!orders_bloom.may_contain(orderkey)) continue;

                    // Compute revenue: ep * (100 - discount)
                    int64_t rev = lprice[i] * (100LL - ldisc[i]);

                    // Probe/update thread-local aggregation map using operator[]
                    // (find-or-insert in one operation)
                    AggVal* agg_val = ts.agg.find(orderkey);
                    if (agg_val != nullptr) {
                        agg_val->revenue_sum += rev;
                    } else {
                        // First time seeing this orderkey in this thread
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

    // ── Phase 4: Parallel aggregation merge ───────────────────────────────────
    // Strategy: partition the key space into NUM_PARTITIONS buckets.
    // Thread p merges ALL thread-local agg maps' entries that belong to partition p.
    // No locking: each thread writes exclusively to its partition's global map.
    using GlobalAggPart = gendb::CompactHashMap<int32_t, AggVal>;
    std::vector<GlobalAggPart> global_parts(NUM_PARTITIONS);

    {
        GENDB_PHASE("aggregation_merge");

        // Pre-size each global partition
        // Estimate: total unique groups spread evenly across partitions
        size_t total_local = 0;
        for (auto& ts : thread_states) total_local += ts.agg.size();
        size_t per_part = std::max(total_local / NUM_PARTITIONS + 256, (size_t)512);
        for (auto& gp : global_parts) gp.reserve(per_part);

        std::atomic<size_t> merge_part_idx{0};

        auto merge_worker = [&](int /*tid*/) {
            while (true) {
                size_t p = merge_part_idx.fetch_add(1, std::memory_order_relaxed);
                if (p >= NUM_PARTITIONS) break;

                GlobalAggPart& gp = global_parts[p];

                // Sweep all thread-local maps, pick up entries belonging to partition p
                for (auto& ts : thread_states) {
                    for (auto [key, val] : ts.agg) {
                        if (gendb::hash_int(key) % NUM_PARTITIONS != p) continue;
                        AggVal* gv = gp.find(key);
                        if (gv == nullptr) {
                            gp.insert(key, val);
                        } else {
                            gv->revenue_sum += val.revenue_sum;
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

        std::vector<TopKEntry> all;
        // Count total entries
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
            // ep_stored = actual_price * 100, disc_stored = actual_disc * 100
            // revenue_sum = actual_revenue * 10000
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
