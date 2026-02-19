// Q3: Shipping Priority — iter_10
//
// Performance Profile (iter_5, current best: 360ms):
//   dim_filter:        12ms (3%)
//   build_joins:      138ms (38%)
//   aggregation_merge: 14ms (4%)
//   main_scan:        180ms (50%)  ← DOMINANT
//   sort_topk:         17ms (5%)
//
// Root cause of 180ms main_scan:
//   The scatter-then-aggregate approach (iter_5) allocates 64×64 staging buffers with
//   reserve(8192) each = 384MB upfront. This trashes memory bandwidth and cache.
//   Phase 3a writes rows to 64 scattered staging buffers (cache-hostile).
//   Phase 3b reads staging data then also probes orders_ht (atomic, expensive).
//
// New architecture:
//
// Phase 1 (dim_filter): same — bitmap of BUILDING customers (~12ms)
//
// Phase 2 (build_joins): same as iter_5 (parallel CAS HT + thread-local bloom merge)
//
// Phase 3 (main_scan): ARCHITECTURAL CHANGE — eliminate 384MB staging allocation
//   - Each of 64 threads gets its OWN CompactHashMap<int32_t, AggVal> (thread-local agg)
//   - Hot loop: shipdate filter → bloom check → HT probe → direct agg into thread-local map
//   - No scatter buffers, no staging vectors, no 2-pass overhead
//   - AggVal filled from orders_ht on first insert; subsequent hits just add to revenue_sum
//   - bloom filter eliminates ~70-80% of probes; orders_ht probe only ~22K times per thread
//
// Phase 4 (aggregation_merge): PARALLEL merge by key partition
//   - Thread p iterates all 64 thread-local maps, extracts keys where hash(key)%64 == p
//   - No locking needed (disjoint partitions); each thread builds its own result slice
//   - Replaces the serial scan across all maps
//
// Phase 5 (sort_topk): per-partition top-10 then global merge (avoids sorting 1.44M entries)
//
// Expected: eliminate 384MB staging alloc + 2-pass scatter → main_scan ~80-100ms, total ~220ms

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
static constexpr int32_t DATE_1995_03_15 = 9204;
static constexpr int32_t MAX_CUSTKEY     = 1500001;
static constexpr int     NUM_THREADS     = 64;
static constexpr int     NUM_PARTS       = 64; // power-of-2

// ── Aggregation value per group ───────────────────────────────────────────────
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

// ── Concurrent flat hash table for orders (CAS-based, parallel build) ────────
// Custom lock-free open-addressing HT for the parallel orders build phase.
// Key: int32_t o_orderkey (TPC-H orderkeys start at 1, never 0).
// Why custom: CompactHashMap does not support concurrent insert with CAS.
struct ConcurrentOrdersHT {
    static constexpr int32_t EMPTY = 0;

    std::atomic<int32_t>* keys;
    int32_t*               dates;
    int32_t*               pris;
    uint64_t               mask;
    size_t                 cap;

    ConcurrentOrdersHT() : keys(nullptr), dates(nullptr), pris(nullptr), mask(0), cap(0) {}

    ~ConcurrentOrdersHT() {
        delete[] keys;
        delete[] dates;
        delete[] pris;
    }

    ConcurrentOrdersHT(const ConcurrentOrdersHT&) = delete;
    ConcurrentOrdersHT& operator=(const ConcurrentOrdersHT&) = delete;

    void init(size_t expected) {
        cap = 16;
        while (cap < expected * 5 / 2) cap <<= 1;
        mask = cap - 1;
        keys  = new std::atomic<int32_t>[cap];
        dates = new int32_t[cap]();
        pris  = new int32_t[cap]();
        for (size_t i = 0; i < cap; ++i)
            keys[i].store(EMPTY, std::memory_order_relaxed);
    }

    inline void insert(int32_t key, int32_t date, int32_t priority) {
        uint64_t pos = gendb::hash_int(key) & mask;
        for (;;) {
            int32_t expected = EMPTY;
            if (keys[pos].compare_exchange_weak(
                    expected, key,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed)) {
                dates[pos] = date;
                pris[pos]  = priority;
                return;
            }
            if (expected == key) return;
            pos = (pos + 1) & mask;
        }
    }

    inline bool find(int32_t key, int32_t& out_date, int32_t& out_priority) const {
        uint64_t pos = gendb::hash_int(key) & mask;
        for (;;) {
            int32_t k = keys[pos].load(std::memory_order_acquire);
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
struct BloomFilter {
    std::vector<uint64_t> bits_;
    size_t total_bits_;

    void init(size_t capacity) {
        size_t nbits = capacity * 10 + 64;
        total_bits_ = (nbits + 63) & ~63ULL;
        bits_.assign(total_bits_ / 64, 0ULL);
    }

    inline bool may_contain(int32_t key) const {
        uint64_t h  = gendb::hash_int(key);
        uint64_t h2 = h ^ (h >> 17) ^ (h << 31);
        return (bits_[(h  % total_bits_) >> 6] >> ((h  % total_bits_) & 63)) & 1ULL &&
               (bits_[(h2 % total_bits_) >> 6] >> ((h2 % total_bits_) & 63)) & 1ULL;
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

    // ── Phase 2: Build orders HT + bloom filter (parallel CAS + thread-local bloom) ──
    ConcurrentOrdersHT orders_ht;
    BloomFilter orders_bloom;

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey    (gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey     (gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate   (gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_shippriority);

        size_t total_orders = o_orderkey.size();

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

        orders_ht.init(1500000UL);

        orders_bloom.init(1500000UL);
        size_t bloom_words = orders_bloom.bits_.size();

        // Thread-local bloom copies to avoid atomic contention during parallel HT build
        std::vector<std::vector<uint64_t>> tl_bloom(NUM_THREADS,
                                                    std::vector<uint64_t>(bloom_words, 0ULL));

        std::atomic<size_t> range_idx{0};

        auto orders_worker = [&](int tid) {
            auto& my_bloom = tl_bloom[tid];
            size_t tb = orders_bloom.total_bits_;

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
                        orders_ht.insert(k, odate[i], oship[i]);
                        uint64_t h  = gendb::hash_int(k);
                        uint64_t h2 = h ^ (h >> 17) ^ (h << 31);
                        my_bloom[(h  % tb) >> 6] |= (1ULL << ((h  % tb) & 63));
                        my_bloom[(h2 % tb) >> 6] |= (1ULL << ((h2 % tb) & 63));
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

        // Parallel bloom filter merge (bitwise OR)
        std::atomic<size_t> bloom_chunk{0};
        constexpr size_t BLOOM_CHUNK = 4096;
        auto bloom_merge_worker = [&](int /*tid*/) {
            while (true) {
                size_t start = bloom_chunk.fetch_add(BLOOM_CHUNK, std::memory_order_relaxed);
                if (start >= bloom_words) break;
                size_t end = std::min(start + BLOOM_CHUNK, bloom_words);
                for (size_t w = start; w < end; ++w) {
                    uint64_t merged = 0;
                    for (int t = 0; t < NUM_THREADS; ++t) merged |= tl_bloom[t][w];
                    orders_bloom.bits_[w] = merged;
                }
            }
        };
        {
            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(bloom_merge_worker, t);
            for (auto& t : threads) t.join();
        }
    }

    // ── Phase 3: Scan lineitem + thread-local aggregation (NO scatter staging) ──
    //
    // Critical change: each thread owns a CompactHashMap<int32_t, AggVal>.
    // Hot loop does direct agg into thread-local map — no scatter, no 2-pass overhead.
    // Memory: 64 maps × ~30K entries × ~24B/entry = ~46MB total (fits in 376GB RAM easily).
    // Cache behavior: each thread's agg map fits in ~720KB — very cache-friendly.
    //
    // orders_ht probe (atomic load) only happens on FIRST occurrence of each orderkey
    // per thread (~22K times each), not on every lineitem row (~480K rows per thread).

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

        // Zone-map skip: lineitem sorted by l_shipdate → skip early blocks
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
            // Pre-size thread-local agg maps: 32K slots each (covers ~22K groups with headroom)
            for (int t = 0; t < NUM_THREADS; ++t)
                tl_agg[t].reserve(32768);

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
                        if (__builtin_expect(lship[i] <= DATE_1995_03_15, 0)) continue;

                        int32_t ok = lkey[i];
                        if (__builtin_expect(!orders_bloom.may_contain(ok), 0)) continue;

                        // revenue_formula: ep * (100 - discount) [correctness anchor]
                        int64_t rev = lprice[i] * (100LL - ldisc[i]);

                        AggVal* av = my_agg.find(ok);
                        if (__builtin_expect(av != nullptr, 1)) {
                            // Fast path: already seen this orderkey in this thread's map
                            av->revenue_sum += rev;
                        } else {
                            // First occurrence: probe orders_ht for metadata
                            int32_t odate = 0, opri = 0;
                            if (__builtin_expect(orders_ht.find(ok, odate, opri), 1)) {
                                my_agg.insert(ok, {rev, odate, opri});
                            }
                            // else: bloom false positive, skip
                        }
                    }
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(li_worker, t);
            for (auto& t : threads) t.join();
        }
    }

    // ── Phase 4: Parallel merge by key partition ──────────────────────────────
    //
    // Thread p iterates ALL thread-local maps, extracts entries where
    // hash(key) & (NUM_PARTS-1) == p, merging duplicate keys by summing revenue.
    // Partitions are disjoint → no locking needed.
    // Each thread processes ~1.44M/64 = ~22K distinct keys → fast.

    std::vector<std::vector<TopKEntry>> part_results(NUM_PARTS);

    {
        GENDB_PHASE("aggregation_merge");

        size_t total_entries = 0;
        for (int t = 0; t < NUM_THREADS; ++t)
            total_entries += tl_agg[t].size();

        size_t est_per_part = std::max((total_entries / NUM_PARTS) * 3, (size_t)32);
        for (int p = 0; p < NUM_PARTS; ++p)
            part_results[p].reserve(est_per_part);

        std::atomic<int> part_task{0};

        auto merge_worker = [&](int /*tid*/) {
            int p;
            while ((p = part_task.fetch_add(1, std::memory_order_relaxed)) < NUM_PARTS) {
                auto& pr = part_results[p];

                // Use a small local CompactHashMap to merge keys across thread-local maps
                // (a key can appear in multiple thread-local maps if different threads
                //  processed different morsels with the same orderkey)
                gendb::CompactHashMap<int32_t, AggVal> part_map;
                part_map.reserve(est_per_part);

                for (int t = 0; t < NUM_THREADS; ++t) {
                    for (auto [key, val] : tl_agg[t]) {
                        if ((int)(gendb::hash_int(key) & (uint64_t)(NUM_PARTS - 1)) != p)
                            continue;
                        AggVal* existing = part_map.find(key);
                        if (existing) {
                            existing->revenue_sum += val.revenue_sum;
                        } else {
                            part_map.insert(key, val);
                        }
                    }
                }

                // Flatten to result vector
                for (auto [key, val] : part_map) {
                    pr.push_back({val.revenue_sum, key, val.o_orderdate, val.o_shippriority});
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(NUM_THREADS);
        for (int t = 0; t < NUM_THREADS; ++t)
            threads.emplace_back(merge_worker, t);
        for (auto& t : threads) t.join();
    }

    // ── Phase 5: Top-K extraction ─────────────────────────────────────────────
    std::vector<TopKEntry> top10;
    {
        GENDB_PHASE("sort_topk");

        // Per-partition partial sort (top-10 each), then global merge of 64×10=640 candidates
        std::vector<TopKEntry> candidates;
        candidates.reserve(NUM_PARTS * 10);

        for (int p = 0; p < NUM_PARTS; ++p) {
            auto& pr = part_results[p];
            if (pr.empty()) continue;
            size_t k = std::min((size_t)10, pr.size());
            std::partial_sort(pr.begin(), pr.begin() + (ptrdiff_t)k, pr.end(), final_cmp);
            for (size_t i = 0; i < k; ++i)
                candidates.push_back(pr[i]);
        }

        size_t k = std::min((size_t)10, candidates.size());
        std::partial_sort(candidates.begin(), candidates.begin() + (ptrdiff_t)k,
                          candidates.end(), final_cmp);
        top10.assign(candidates.begin(), candidates.begin() + (ptrdiff_t)k);
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
