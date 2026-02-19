// Q3: Shipping Priority — iter_3 optimizations
// Key changes vs iter_1 (best prior):
//  1. Replace 4096-vector thread_bufs[64][64] with single flat pre-allocated array
//     using atomic fetch-add → contiguous sequential writes, cache-friendly
//  2. Single non-partitioned CompactHashMap for orders (pre-sized to 1.44M)
//     built sequentially from flat buffer → eliminates partition-routing overhead
//  3. Fuse bloom filter insert with hash map insert (no separate pass)
//  4. Better bloom filter: power-of-2 mask instead of modulo (faster bit lookup)
//  5. Aggregation merge: sequential merge of thread-local maps into single global map
//     (thread-local maps are small; sequential merge ~few ms with linear scan)
//  6. Lineitem scan: keep zone-map block skip (HDD-friendly sequential access)
//
// Logical plan:
//   1. filter customer → bitmap (300K BUILDING customers, ~10ms)
//   2. scan qualifying orders zones → flat buffer → build orders hash map (single pass)
//   3. scan qualifying lineitem blocks → bloom pre-check → probe orders map → aggregate
//   4. merge thread-local agg maps → global map → top-10

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

// ── Order payload stored in hash map ─────────────────────────────────────────
struct OrderPayload {
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// ── Aggregation value per group ───────────────────────────────────────────────
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

// ── Bloom filter with power-of-2 mask (faster than modulo) ───────────────────
// Custom implementation: power-of-2 mask avoids modulo in hot path
struct BloomFilter {
    std::vector<uint64_t> bits_;
    uint64_t mask_;  // = total_bits - 1, power-of-2

    void init(size_t capacity) {
        // 10 bits/key, rounded up to power of 2
        size_t nbits = 1;
        size_t target = capacity * 10 + 64;
        while (nbits < target) nbits <<= 1;
        // nbits is power of 2 in bits; convert to 64-bit words
        bits_.assign(nbits / 64, 0ULL);
        mask_ = (uint64_t)(nbits - 1);
    }

    inline void insert(int32_t key) {
        uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
        uint64_t h2 = h ^ (h >> 17) ^ (h << 13);
        uint64_t b1 = h  & mask_;
        uint64_t b2 = h2 & mask_;
        bits_[b1 >> 6] |= (1ULL << (b1 & 63));
        bits_[b2 >> 6] |= (1ULL << (b2 & 63));
    }

    inline bool may_contain(int32_t key) const {
        uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
        uint64_t h2 = h ^ (h >> 17) ^ (h << 13);
        uint64_t b1 = h  & mask_;
        uint64_t b2 = h2 & mask_;
        return (bits_[b1 >> 6] >> (b1 & 63)) & 1ULL &
               (bits_[b2 >> 6] >> (b2 & 63)) & 1ULL;
    }
};

// ── Matched order struct (stored in flat pre-allocated buffer) ────────────────
struct MatchedOrder {
    int32_t orderkey;
    int32_t orderdate;
    int32_t shippriority;
    int32_t _pad;  // align to 16 bytes for cache efficiency
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

    // ── Phase 2: Build orders map from flat buffer (cache-friendly) ───────────
    // Strategy:
    //   Pass A (parallel): threads scan qualifying zone-map ranges and append
    //                      matched orders to a pre-allocated flat array via
    //                      atomic fetch-add. No partitioning, no fragmented
    //                      per-thread-per-partition vectors.
    //   Pass B (sequential): build single CompactHashMap from flat array.
    //                        1.44M insertions ≈ 20ms sequential, faster than
    //                        64×64=4096 fragmented vectors + parallel insert.
    //   Pass C (fused with B): bloom filter insert during hash map build.

    // Pre-allocate flat buffer — 15M orders × ~10% match = 1.5M upper bound
    static constexpr size_t ORDERS_FLAT_CAPACITY = 2000000;
    std::vector<MatchedOrder> flat_orders(ORDERS_FLAT_CAPACITY);
    std::atomic<size_t> flat_count{0};

    using OrdersMap = gendb::CompactHashMap<int32_t, OrderPayload>;
    OrdersMap orders_map;

    BloomFilter orders_bloom;

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey    (gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey     (gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate   (gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        // Start prefetching lineitem columns early (overlap I/O with orders build)
        gendb::MmapColumn<int32_t> l_orderkey      (gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice (gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount       (gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate       (gendb_dir + "/lineitem/l_shipdate.bin");

        // Fire all prefetch requests immediately to overlap I/O
        mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_shippriority);
        mmap_prefetch_all(l_orderkey, l_extendedprice, l_discount, l_shipdate);

        size_t total_orders = o_orderkey.size();

        // Load zone map for o_orderdate
        gendb::ZoneMapIndex zone_map(gendb_dir + "/indexes/orders_o_orderdate.bin");

        // Build qualifying row ranges from zone map
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

        // Pass A: parallel scan → flat buffer via atomic fetch-add
        // Each thread atomically claims a slot in flat_orders[], writes sequentially
        // within its claimed region. No partitioning, no lock contention.
        std::atomic<size_t> range_idx{0};

        auto orders_scan = [&](int /*tid*/) {
            while (true) {
                size_t ri = range_idx.fetch_add(1, std::memory_order_relaxed);
                if (ri >= qual_ranges.size()) break;

                size_t start = qual_ranges[ri].start;
                size_t end   = qual_ranges[ri].end;

                const int32_t* okey  = o_orderkey.data     + start;
                const int32_t* odate = o_orderdate.data    + start;
                const int32_t* ocust = o_custkey.data      + start;
                const int32_t* oship = o_shippriority.data + start;
                size_t n = end - start;

                // Local buffer to batch atomic updates (reduces atomic contention)
                // Write locally, then bulk-claim slots
                static constexpr size_t LOCAL_BUF = 4096;
                MatchedOrder local_buf[LOCAL_BUF];
                size_t local_cnt = 0;

                for (size_t i = 0; i < n; ++i) {
                    if (odate[i] < DATE_1995_03_15 && cust_bitmap.test((size_t)ocust[i])) {
                        local_buf[local_cnt++] = {okey[i], odate[i], oship[i], 0};
                        if (local_cnt == LOCAL_BUF) {
                            // Bulk claim slots
                            size_t base = flat_count.fetch_add(local_cnt, std::memory_order_relaxed);
                            if (base + local_cnt <= ORDERS_FLAT_CAPACITY) {
                                std::memcpy(&flat_orders[base], local_buf,
                                            local_cnt * sizeof(MatchedOrder));
                            }
                            local_cnt = 0;
                        }
                    }
                }
                // Flush remainder
                if (local_cnt > 0) {
                    size_t base = flat_count.fetch_add(local_cnt, std::memory_order_relaxed);
                    if (base + local_cnt <= ORDERS_FLAT_CAPACITY) {
                        std::memcpy(&flat_orders[base], local_buf,
                                    local_cnt * sizeof(MatchedOrder));
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

        size_t total_matched = flat_count.load(std::memory_order_relaxed);
        total_matched = std::min(total_matched, ORDERS_FLAT_CAPACITY);

        // Pass B: sequential build of single CompactHashMap + bloom filter
        // Single non-partitioned map eliminates partition-routing overhead in main_scan
        orders_map.reserve(total_matched + total_matched / 2 + 256);
        orders_bloom.init(std::max(total_matched, (size_t)1));

        for (size_t i = 0; i < total_matched; ++i) {
            const auto& m = flat_orders[i];
            orders_map.insert(m.orderkey, {m.orderdate, m.shippriority});
            orders_bloom.insert(m.orderkey);
        }

        // ── Phase 3: Scan lineitem with zone-map block skipping, aggregate ──────
        // lineitem is sorted by l_shipdate → skip early blocks where max <= DATE_1995_03_15
        // lineitem columns are already opened and prefetch-initiated above.

        using AggMap = gendb::CompactHashMap<int32_t, AggVal>;

        struct alignas(64) ThreadState {
            AggMap agg;
            ThreadState() { agg.reserve(65536); }
        };

        std::vector<ThreadState> thread_states(NUM_THREADS);

        {
            GENDB_PHASE("main_scan");

            size_t total_lineitem = l_orderkey.size();

            // Zone-map skip: lineitem sorted by l_shipdate, block size = 100000
            static constexpr size_t LI_BLOCK = 100000;
            size_t num_blocks = (total_lineitem + LI_BLOCK - 1) / LI_BLOCK;

            // Find first block where last row > DATE_1995_03_15 (partial or fully qualifying)
            size_t first_qualifying_block = num_blocks; // default: none qualify
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

                        // Bloom filter pre-check (L2-cache resident, cheap)
                        if (!orders_bloom.may_contain(orderkey)) continue;

                        // revenue: ep * (100 - discount) — correctness anchor
                        int64_t rev = lprice[i] * (100LL - ldisc[i]);

                        // Probe thread-local agg map first (hot path: same orderkey seen again)
                        AggVal* agg_val = ts.agg.find(orderkey);
                        if (agg_val != nullptr) {
                            agg_val->revenue_sum += rev;
                        } else {
                            // First encounter: probe orders map
                            const OrderPayload* op = orders_map.find(orderkey);
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

        } // end main_scan phase

        // ── Phase 4: Aggregation merge ────────────────────────────────────────
        // Sequential merge of thread-local agg maps into a single global map.
        // Thread-local maps are small (unique orderkeys seen per-thread); sequential
        // merge avoids the 64×64 partition-scan overhead of the partitioned approach.
        {
            GENDB_PHASE("aggregation_merge");

            // Count total entries to pre-size global map
            size_t total_local = 0;
            for (auto& ts : thread_states) total_local += ts.agg.size();

            gendb::CompactHashMap<int32_t, AggVal> global_agg;
            global_agg.reserve(total_local + total_local / 4 + 256);

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

            // ── Phase 5: Top-K extraction ─────────────────────────────────────
            std::vector<TopKEntry> top10;
            {
                GENDB_PHASE("sort_topk");

                size_t total_groups = global_agg.size();
                std::vector<TopKEntry> all;
                all.reserve(total_groups);

                for (auto [key, val] : global_agg) {
                    all.push_back({val.revenue_sum, key, val.o_orderdate, val.o_shippriority});
                }

                size_t k = std::min((size_t)10, all.size());
                std::partial_sort(all.begin(), all.begin() + (ptrdiff_t)k, all.end(), final_cmp);
                top10.assign(all.begin(), all.begin() + (ptrdiff_t)k);
            }

            // ── Phase 6: Output results ───────────────────────────────────────
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
        } // end aggregation_merge

        // lineitem columns go out of scope here (RAII mmap close)
    } // end build_joins phase
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
