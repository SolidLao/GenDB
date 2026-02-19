// Q3: Shipping Priority — iter_8
//
// Bottleneck analysis from iter_5 (360ms):
//   main_scan:       180ms (50%) — staging[64][64] pre-reserves ~384MB, scatter overhead
//   build_joins:     138ms (38%) — CAS contention on 64 threads + bloom merge overhead
//   aggregation_merge: 14ms (4%) — already parallel, minor
//
// Architectural changes:
//
// Phase 2 (build_joins): Replace CAS concurrent HT with partition-parallel build.
//   - 64 thread-local CompactHashMaps for orders, each thread owns 1 partition
//     (hash(o_orderkey) & 63 determines the partition). No CAS, no atomic contention.
//   - All orders qualify test runs in parallel; row written to partition-local map.
//   - After build, the 64 maps are accessed by partition during probe. No merge needed.
//   - Bloom filter: single power-of-2-sized bitmap, writes done thread-locally and
//     merged in parallel. Using power-of-2 bits avoids expensive modulo.
//
// Phase 3 (main_scan + aggregation_merge): Eliminate staging buffers entirely.
//   - Thread-local aggregation maps (CompactHashMap<int32_t, AggVal>) per thread.
//     Each thread accumulates directly during the lineitem scan — no scatter pass.
//   - After scan: parallel merge via partition ownership. Thread p collects all entries
//     where hash(l_orderkey) & 63 == p from all 64 thread maps into a final agg map.
//   - Result: eliminates 384MB staging pre-allocation, eliminates scatter pass,
//     keeps merge parallel at O(total_entries / 64) per thread.
//
// Additional micro-optimizations:
//   - Power-of-2 bloom filter (bitwise AND instead of modulo)
//   - Prefetch orders columns before bloom merge
//   - Reduced pre-allocation sizes (no more 8192×64×64 staging)

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
static constexpr int     NUM_PARTS       = 64; // power-of-2

// ── Aggregation value per group (keyed by l_orderkey) ────────────────────────
struct AggVal {
    int64_t revenue_sum;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// ── Orders payload stored in partition maps ───────────────────────────────────
struct OrderPayload {
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

// ── Bloom filter with power-of-2 size (bitwise AND, no modulo) ───────────────
// Custom: power-of-2 sizing enables cheap & instead of % for two independent hash probes.
struct BloomFilter {
    std::vector<uint64_t> bits_;
    uint64_t mask_bits_; // (total_bits - 1), total_bits is power-of-2

    void init(size_t capacity) {
        // 10 bits per key, rounded up to next power-of-2
        size_t nbits = 16;
        while (nbits < capacity * 10) nbits <<= 1;
        mask_bits_ = nbits - 1;
        bits_.assign(nbits / 64, 0ULL);
    }

    inline void set_bit(uint64_t b) { bits_[b >> 6] |= (1ULL << (b & 63)); }
    inline bool get_bit(uint64_t b) const { return (bits_[b >> 6] >> (b & 63)) & 1ULL; }

    inline void insert(int32_t key) {
        uint64_t h  = gendb::hash_int(key);
        uint64_t h2 = (h >> 17) | (h << 47); // rotate
        set_bit(h  & mask_bits_);
        set_bit(h2 & mask_bits_);
    }

    inline bool may_contain(int32_t key) const {
        uint64_t h  = gendb::hash_int(key);
        uint64_t h2 = (h >> 17) | (h << 47);
        return get_bit(h & mask_bits_) & get_bit(h2 & mask_bits_);
    }
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

    // ── Phase 2: Build partitioned orders maps + bloom filter ────────────────
    // 64 partition-local CompactHashMaps, no CAS contention.
    // Thread t scans its zone-map range, hashes o_orderkey → partition p,
    // appends to thread-local per-partition row buffer. After scan, thread p
    // merges all threads' partition-p buffers into its orders_part[p] map.
    //
    // This avoids CAS by making each partition's final map single-writer.

    using OrdersMap = gendb::CompactHashMap<int32_t, OrderPayload>;
    std::vector<OrdersMap> orders_part(NUM_PARTS);
    BloomFilter orders_bloom;

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey    (gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey     (gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate   (gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_shippriority);

        size_t total_orders = o_orderkey.size();

        // Load zone map for orders (sorted by o_orderdate)
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

        orders_bloom.init(1500000UL);
        size_t bloom_words = orders_bloom.bits_.size();

        // Thread-local staging: thread t collects rows for each of NUM_PARTS partitions
        // Using flat struct for compact memory: (key, date, priority)
        struct ORow { int32_t key, date, pri; };
        // staging[tid][part] = vector<ORow>
        // Estimated ~1.44M matched orders / 64 parts / 64 threads = ~352 rows per cell
        // Use 512 pre-reserve to avoid realloc
        std::vector<std::vector<std::vector<ORow>>> staging(
            NUM_THREADS, std::vector<std::vector<ORow>>(NUM_PARTS));
        for (int t = 0; t < NUM_THREADS; ++t)
            for (int p = 0; p < NUM_PARTS; ++p)
                staging[t][p].reserve(512);

        // Thread-local bloom bits (merged after)
        std::vector<std::vector<uint64_t>> tl_bloom(NUM_THREADS,
                                                     std::vector<uint64_t>(bloom_words, 0ULL));

        std::atomic<size_t> range_idx{0};

        auto orders_worker = [&](int tid) {
            auto& my_staging = staging[tid];
            auto& my_bloom   = tl_bloom[tid];
            const uint64_t bmask = orders_bloom.mask_bits_;

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
                        int32_t k = okey[i];
                        // Bloom insert (thread-local)
                        uint64_t h  = gendb::hash_int(k);
                        uint64_t h2 = (h >> 17) | (h << 47);
                        my_bloom[(h  & bmask) >> 6] |= (1ULL << ((h  & bmask) & 63));
                        my_bloom[(h2 & bmask) >> 6] |= (1ULL << ((h2 & bmask) & 63));
                        // Scatter to partition
                        int p = (int)(h & (uint64_t)(NUM_PARTS - 1));
                        my_staging[p].push_back({k, odate[i], oship[i]});
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

        // Phase 2b: parallel bloom merge + orders map build (same thread pool)
        // Thread p: merges bloom slice + builds orders_part[p] from all threads' staging[t][p]
        std::atomic<int> part_task{0};

        auto build_part_worker = [&](int /*tid*/) {
            // Bloom merge chunk
            int p;
            while ((p = part_task.fetch_add(1, std::memory_order_relaxed)) < NUM_PARTS) {
                // Merge bloom slice for this partition's word range
                size_t w_start = (size_t)p       * bloom_words / NUM_PARTS;
                size_t w_end   = (size_t)(p + 1) * bloom_words / NUM_PARTS;
                for (size_t w = w_start; w < w_end; ++w) {
                    uint64_t merged = 0;
                    for (int t = 0; t < NUM_THREADS; ++t) merged |= tl_bloom[t][w];
                    orders_bloom.bits_[w] = merged;
                }

                // Count total rows for this partition across all threads
                size_t total = 0;
                for (int t = 0; t < NUM_THREADS; ++t)
                    total += staging[t][p].size();

                // Build orders_part[p]
                if (total > 0) {
                    orders_part[p].reserve(total + total / 4 + 8);
                    for (int t = 0; t < NUM_THREADS; ++t) {
                        for (const auto& row : staging[t][p]) {
                            orders_part[p].insert(row.key, {row.date, row.pri});
                        }
                    }
                }
            }
        };

        {
            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(build_part_worker, t);
            for (auto& t : threads) t.join();
        }
    }

    // ── Phase 3: Scan lineitem + direct thread-local aggregation ─────────────
    // Key change: NO staging buffers. Each thread directly aggregates into its
    // own CompactHashMap<int32_t, AggVal> during the scan.
    // After scan, merge in parallel: thread p collects all entries where
    // hash(l_orderkey) & 63 == p from all 64 thread maps.
    //
    // This eliminates the scatter pass and the 384MB staging pre-allocation.

    using AggMap = gendb::CompactHashMap<int32_t, AggVal>;

    // Final partition maps (owned per partition after merge)
    std::vector<AggMap> agg_parts(NUM_PARTS);

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey     (gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount      (gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate      (gendb_dir + "/lineitem/l_shipdate.bin");

        mmap_prefetch_all(l_orderkey, l_extendedprice, l_discount, l_shipdate);

        size_t total_lineitem = l_orderkey.size();

        // Zone-map skip: lineitem sorted by l_shipdate → skip blocks where last elem <= threshold
        static constexpr size_t LI_BLOCK = 100000;
        size_t num_blocks = (total_lineitem + LI_BLOCK - 1) / LI_BLOCK;
        size_t first_qualifying_block = num_blocks;
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

        size_t li_start_row = first_qualifying_block * LI_BLOCK;
        if (li_start_row < total_lineitem) {

        static constexpr size_t MORSEL = 131072; // 128K rows per morsel (2× of iter_5)
        size_t total_rows   = total_lineitem - li_start_row;
        size_t num_morsels  = (total_rows + MORSEL - 1) / MORSEL;

        // Thread-local aggregation maps (no staging, direct accumulation)
        // Estimate: ~1.44M distinct orderkeys / 64 threads = ~22500 per thread
        std::vector<AggMap> tl_agg(NUM_THREADS);
        for (int t = 0; t < NUM_THREADS; ++t)
            tl_agg[t].reserve(32768); // power-of-2, ~22500 expected

        std::atomic<size_t> morsel_idx{0};

        // Phase 3a: parallel lineitem scan → direct thread-local aggregation
        auto li_scan_worker = [&](int tid) {
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

                    // Revenue: ep * (100 - discount)
                    int64_t rev = lprice[i] * (100LL - ldisc[i]);

                    // Direct aggregation into thread-local map
                    AggVal* av = my_agg.find(ok);
                    if (av != nullptr) {
                        av->revenue_sum += rev;
                    } else {
                        // Probe orders partition map
                        int p = (int)(gendb::hash_int(ok) & (uint64_t)(NUM_PARTS - 1));
                        const OrderPayload* op = orders_part[p].find(ok);
                        if (op == nullptr) continue; // bloom false positive
                        my_agg.insert(ok, {rev, op->o_orderdate, op->o_shippriority});
                    }
                }
            }
        };

        {
            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(li_scan_worker, t);
            for (auto& t : threads) t.join();
        }

        // Phase 3b: parallel merge — thread p collects all entries from tl_agg[*]
        //   where hash(l_orderkey) & 63 == p into agg_parts[p].
        // Each partition has ~1.44M / 64 ≈ 22500 distinct keys total across all threads.
        {
            GENDB_PHASE("aggregation_merge");
            std::atomic<int> merge_task{0};

            auto merge_worker = [&](int /*tid*/) {
                int p;
                while ((p = merge_task.fetch_add(1, std::memory_order_relaxed)) < NUM_PARTS) {
                    // Count total entries for partition p across all thread maps
                    size_t est = 0;
                    for (int t = 0; t < NUM_THREADS; ++t) {
                        // Upper bound: all entries in tl_agg[t] could map to partition p
                        est += tl_agg[t].size();
                    }
                    // Reserve est/NUM_PARTS + headroom
                    agg_parts[p].reserve(est / NUM_PARTS + 64);

                    for (int t = 0; t < NUM_THREADS; ++t) {
                        for (auto [key, val] : tl_agg[t]) {
                            int kp = (int)(gendb::hash_int(key) & (uint64_t)(NUM_PARTS - 1));
                            if (kp != p) continue;
                            AggVal* av = agg_parts[p].find(key);
                            if (av != nullptr) {
                                av->revenue_sum += val.revenue_sum;
                            } else {
                                agg_parts[p].insert(key, val);
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

    // ── Phase 4: Top-K extraction ─────────────────────────────────────────────
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

    // ── Phase 5: Output results ───────────────────────────────────────────────
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
