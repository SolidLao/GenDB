// Q3: Shipping Priority
// Plan: customer bitset → orders hash map (zone-map pruned) → lineitem scan + aggregation → top-10

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
static constexpr int      NUM_THREADS     = 64;

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
    int64_t revenue_sum;   // for comparison (higher = better)
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// For final sort: revenue DESC, o_orderdate ASC
static bool final_cmp(const TopKEntry& a, const TopKEntry& b) {
    if (a.revenue_sum != b.revenue_sum) return a.revenue_sum > b.revenue_sum;
    return a.o_orderdate < b.o_orderdate;
}

// For min-heap used by TopKHeap (worst = smallest revenue, tie: largest orderdate)
// TopKHeap<T,Cmp>: cmp(a,b) true means a is "worse" (should be evicted sooner)
// We want to keep top revenue, so: a is worse than b if a.revenue < b.revenue
// or (same revenue) a.o_orderdate > b.o_orderdate
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
        total_bits_ = (nbits + 63) & ~63ULL; // round up to 64
        bits_.assign(total_bits_ / 64, 0ULL);
    }

    inline void set_bit(size_t b) {
        bits_[b >> 6] |= (1ULL << (b & 63));
    }

    inline bool get_bit(size_t b) const {
        return (bits_[b >> 6] >> (b & 63)) & 1ULL;
    }

    void insert(int32_t key) {
        uint64_t h = gendb::hash_int((int32_t)key);
        // Second hash via bit-mixing
        uint64_t h2 = h ^ (h >> 17) ^ (h << 31);
        set_bit(h  % total_bits_);
        set_bit(h2 % total_bits_);
    }

    bool may_contain(int32_t key) const {
        uint64_t h = gendb::hash_int((int32_t)key);
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

        // Load c_mktsegment dictionary and find BUILDING code
        int16_t building_code = -1;
        {
            std::ifstream dict(gendb_dir + "/customer/c_mktsegment_dict.txt");
            std::string line;
            int16_t code = 0;
            while (std::getline(dict, line)) {
                if (line == "BUILDING") {
                    building_code = code;
                    break;
                }
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
        const int16_t* seg = c_mktsegment.data;
        const int32_t* ckey = c_custkey.data;

        for (size_t i = 0; i < n; ++i) {
            if (seg[i] == building_code) {
                cust_bitmap.set((size_t)ckey[i]);
            }
        }
    }

    // ── Phase 2: Build orders hash map (zone-map pruned, parallel) ────────────
    gendb::CompactHashMap<int32_t, OrderPayload> orders_map;
    BloomFilter orders_bloom;

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey    (gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey     (gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate   (gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        size_t total_orders = o_orderkey.size();

        // Prefetch all orders columns
        mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_shippriority);

        // Load zone map for o_orderdate
        gendb::ZoneMapIndex zone_map(gendb_dir + "/indexes/orders_o_orderdate.bin");

        // Find qualifying row ranges: keep zones where min < DATE_1995_03_15
        // (i.e., zones that may contain rows with o_orderdate < cutoff)
        struct RowRange { size_t start, end; };
        std::vector<RowRange> qual_ranges;
        qual_ranges.reserve(zone_map.zones.size());

        for (const auto& z : zone_map.zones) {
            // Skip zones where ALL rows are >= DATE_1995_03_15
            if (z.min >= DATE_1995_03_15) continue;
            size_t row_start = (size_t)z.row_offset;
            size_t row_end   = std::min(row_start + (size_t)z.row_count, total_orders);
            qual_ranges.push_back({row_start, row_end});
        }

        // Fallback: if no zone map data, scan everything
        if (qual_ranges.empty()) {
            qual_ranges.push_back({0, total_orders});
        }

        // Parallel scan using thread-local buffers
        struct MatchedOrder { int32_t orderkey; int32_t orderdate; int32_t shippriority; };
        std::vector<std::vector<MatchedOrder>> thread_results(NUM_THREADS);
        for (auto& v : thread_results) v.reserve(30000);

        std::atomic<size_t> range_idx{0};

        auto orders_worker = [&](int tid) {
            auto& local = thread_results[tid];

            while (true) {
                size_t ri = range_idx.fetch_add(1, std::memory_order_relaxed);
                if (ri >= qual_ranges.size()) break;

                size_t start = qual_ranges[ri].start;
                size_t end   = qual_ranges[ri].end;

                const int32_t* okey  = o_orderkey.data    + start;
                const int32_t* odate = o_orderdate.data   + start;
                const int32_t* ocust = o_custkey.data     + start;
                const int32_t* oship = o_shippriority.data + start;

                for (size_t i = 0, n = end - start; i < n; ++i) {
                    if (odate[i] < DATE_1995_03_15 && cust_bitmap.test((size_t)ocust[i])) {
                        local.push_back({okey[i], odate[i], oship[i]});
                    }
                }
            }
        };

        {
            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(orders_worker, t);
            for (auto& t : threads) t.join();
        }

        // Count total matched orders for sizing
        size_t total_matched = 0;
        for (auto& v : thread_results) total_matched += v.size();

        // Reserve hash map and bloom filter
        orders_map.reserve(std::max(total_matched + total_matched / 2, (size_t)65536));
        orders_bloom.init(std::max(total_matched, (size_t)1));

        // Sequential insert into shared hash map + bloom filter
        for (auto& v : thread_results) {
            for (auto& m : v) {
                orders_map.insert(m.orderkey, {m.orderdate, m.shippriority});
                orders_bloom.insert(m.orderkey);
            }
        }
    }

    // ── Phase 3: Scan lineitem, aggregate ─────────────────────────────────────
    // Thread-local aggregation maps + top-k heaps
    using AggMap = gendb::CompactHashMap<int32_t, AggVal>;

    struct alignas(64) ThreadState {
        AggMap agg;
        gendb::TopKHeap<TopKEntry, TopKCmp> heap;
        ThreadState() : heap(10) { agg.reserve(65536); }
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
        static constexpr size_t MORSEL = 65536;
        size_t num_morsels = (total_lineitem + MORSEL - 1) / MORSEL;

        std::atomic<size_t> morsel_idx{0};

        // lineitem is NOT sorted by l_shipdate in actual data (it's sorted by l_orderkey).
        // Do per-row filtering only — no zone-skip.

        auto lineitem_worker = [&](int tid) {
            ThreadState& ts = thread_states[tid];

            while (true) {
                size_t mi = morsel_idx.fetch_add(1, std::memory_order_relaxed);
                if (mi >= num_morsels) break;

                size_t start = mi * MORSEL;
                size_t end   = std::min(start + MORSEL, total_lineitem);
                size_t n     = end - start;

                const int32_t* lkey   = l_orderkey.data      + start;
                const int32_t* lship  = l_shipdate.data      + start;
                const int64_t* lprice = l_extendedprice.data + start;
                const int64_t* ldisc  = l_discount.data      + start;

                for (size_t i = 0; i < n; ++i) {
                    if (lship[i] <= DATE_1995_03_15) continue;

                    int32_t orderkey = lkey[i];

                    // Bloom filter pre-check to avoid expensive hash map lookup
                    if (!orders_bloom.may_contain(orderkey)) continue;

                    // Compute revenue contribution: ep * (100 - discount)
                    // Both ep and discount scaled by 100, so result is ×10000
                    int64_t rev = lprice[i] * (100LL - ldisc[i]);

                    // Probe/update thread-local aggregation map
                    AggVal* agg_val = ts.agg.find(orderkey);
                    if (agg_val == nullptr) {
                        // Check orders_map for this orderkey
                        const OrderPayload* op = orders_map.find(orderkey);
                        if (op == nullptr) continue;
                        // Insert new group
                        AggVal new_val{rev, op->o_orderdate, op->o_shippriority};
                        ts.agg.insert(orderkey, new_val);
                    } else {
                        agg_val->revenue_sum += rev;
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

    // ── Phase 4: Merge aggregation maps ──────────────────────────────────────
    // Use PartitionedHashMap or a single global map with sequential merge
    AggMap global_agg;
    {
        GENDB_PHASE("aggregation_merge");

        // Estimate total unique groups
        size_t total_entries = 0;
        for (auto& ts : thread_states) total_entries += ts.agg.size();
        global_agg.reserve(std::max(total_entries * 2, (size_t)65536));

        for (auto& ts : thread_states) {
            for (auto [key, val] : ts.agg) {
                AggVal* gv = global_agg.find(key);
                if (gv == nullptr) {
                    global_agg.insert(key, val);
                } else {
                    gv->revenue_sum += val.revenue_sum;
                }
            }
        }
    }

    // ── Phase 5: Top-K extraction ─────────────────────────────────────────────
    std::vector<TopKEntry> top10;
    {
        GENDB_PHASE("sort_topk");

        // Collect all entries into a vector, then partial_sort for top 10
        std::vector<TopKEntry> all;
        all.reserve(global_agg.size());

        for (auto [key, val] : global_agg) {
            all.push_back({val.revenue_sum, key, val.o_orderdate, val.o_shippriority});
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
            // ep_stored   = actual_price * 100  (scale_factor=100)
            // disc_stored = actual_disc  * 100  (scale_factor=100)
            // So: revenue_sum = actual_price * 100 * (100 - actual_disc * 100)
            //   = actual_price * 100 * 100 * (1 - actual_disc)
            //   = actual_revenue * 10000
            // Output: revenue_sum / 10000 with 2 decimal places

            int64_t whole = e.revenue_sum / 10000LL;
            int64_t frac  = e.revenue_sum % 10000LL;
            // frac is in units of 0.0001; need 2 decimal places (cents)
            // cents = frac / 100, rounding half-up
            int64_t cents_full = frac; // 0..9999
            int64_t cents = cents_full / 100;
            int64_t sub   = cents_full % 100;
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
