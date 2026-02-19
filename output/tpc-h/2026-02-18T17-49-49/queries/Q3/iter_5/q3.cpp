// Q3: Shipping Priority — iter_5 FULL ARCHITECTURAL REWRITE
//
// Root cause of stall: build_joins (47%, 187ms) dominated by scatter-gather overhead —
//   thread_bufs[64][64] allocates 4096 vectors, then 2-pass scatter into 64 partition maps.
//   Also: aggregation_merge (35ms) is sequential O(N×64) scan of all thread maps.
//
// New architecture (3 phases instead of 5):
//
// Phase 1: dim_filter — same: bitmap of BUILDING customers (sequential, 1.5M rows, ~10ms)
//
// Phase 2: build_orders — SINGLE flat open-addressing hash table (no partitions, no scatter).
//   - Zone-map prune orders (o_orderdate), parallel morsel scan, CAS-based concurrent insert
//   - Custom flat HT: power-of-2 slots, atomic CAS on key field, payload packed in same slot
//   - ~1.44M matched orders → pre-size to 4M slots (3.5× load) → ~96MB but fits in RAM,
//     very low collision probability
//   - Bloom filter built in parallel over the flat HT
//
// Phase 3: lineitem_scan + radix-partitioned aggregation (no separate merge phase)
//   - Zone-map skip early lineitem blocks (sorted by l_shipdate)
//   - Parallel morsel workers: for each qualifying lineitem row (bloom+probe):
//       compute revenue; scatter to thread-local per-partition staging buffer
//       (staging[my_partition] or staging[target_partition])
//   - After scan: thread p owns partition p — reads all threads' staging[p] buffers
//       and builds a single compact agg map for partition p
//   - This replaces the old "thread-local agg maps + sequential merge" pattern
//     → merge is now parallel (64 threads each own 1 partition) with no hash map scanning
//
// Result: eliminates 187ms scatter-gather + 35ms merge = ~222ms saved.
// Expected: ~150-170ms total.

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

// ── Concurrent flat hash table for orders ────────────────────────────────────
// Custom lock-free open-addressing HT for the orders build phase.
// Uses sentinel key (0 = empty slot) and CAS to claim slots atomically.
// Key: int32_t o_orderkey (TPC-H orderkeys start at 1, never 0).
//
// Why custom: CompactHashMap does not support concurrent insert.
// Uses raw new[] to avoid std::vector problems with non-copyable atomic members.
// Layout: parallel arrays for keys (atomic) and payloads.
struct ConcurrentOrdersHT {
    static constexpr int32_t EMPTY = 0;

    std::atomic<int32_t>* keys;     // atomic key array (0 = empty)
    int32_t*               dates;    // o_orderdate per slot
    int32_t*               pris;     // o_shippriority per slot
    uint64_t               mask;
    size_t                 cap;

    ConcurrentOrdersHT() : keys(nullptr), dates(nullptr), pris(nullptr), mask(0), cap(0) {}

    ~ConcurrentOrdersHT() {
        if (keys)  { delete[] keys;  keys = nullptr; }
        if (dates) { delete[] dates; dates = nullptr; }
        if (pris)  { delete[] pris;  pris = nullptr; }
    }

    // No copy/move
    ConcurrentOrdersHT(const ConcurrentOrdersHT&) = delete;
    ConcurrentOrdersHT& operator=(const ConcurrentOrdersHT&) = delete;

    void init(size_t expected) {
        // Size to ~2.5× expected for ~40% load factor
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
            if (expected == key) return; // duplicate key
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

// ── Lineitem staging buffer for radix-partitioned aggregation ─────────────────
// Each qualifying lineitem row is buffered into the partition bucket matching
// hash(l_orderkey) & (NUM_PARTS-1). After the scan, thread p drains all buffers
// for partition p and builds the final agg map for that partition.
struct LIStagingRow {
    int32_t l_orderkey;
    int64_t revenue;     // ep * (100 - discount)
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

    // ── Phase 2: Build orders HT + bloom filter (fully parallel, no scatter) ─
    // Single flat CAS-based HT. No scatter-gather buffers.
    ConcurrentOrdersHT orders_ht;
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

        // Load zone map to skip orders blocks with min >= DATE_1995_03_15
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

        // Pre-size HT: estimated ~1.44M matched orders, size to ~3.6M (40% load)
        orders_ht.init(1500000UL); // conservative upper bound

        // Build bloom filter sized for ~1.44M keys
        orders_bloom.init(1500000UL);

        // Parallel scan of orders zones — direct CAS insert into flat HT
        std::atomic<size_t> range_idx{0};
        // Per-thread bloom filter bits to avoid atomic contention on bloom bits
        // We'll merge bloom filters after the scan.
        // Bloom has ~15M bits (1.44M × 10 bits). Use 64 thread-local copies, then OR-merge.
        size_t bloom_words = orders_bloom.bits_.size();

        // Thread-local bloom bits: each thread owns a private copy
        // After scan, merge by OR. Bloom merge is very fast (bitwise OR of ~15M/64 words each).
        std::vector<std::vector<uint64_t>> tl_bloom(NUM_THREADS,
                                                    std::vector<uint64_t>(bloom_words, 0ULL));

        auto orders_worker = [&](int tid) {
            auto& my_bloom = tl_bloom[tid];

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
                        // Insert into thread-local bloom copy
                        uint64_t h  = gendb::hash_int(k);
                        uint64_t h2 = h ^ (h >> 17) ^ (h << 31);
                        size_t tb   = orders_bloom.total_bits_;
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

        // Merge thread-local bloom filters in parallel (64 threads, each handles ~bloom_words/64)
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

    // ── Phase 3: Scan lineitem + radix-partitioned aggregation ───────────────
    // Radix partition: each qualifying lineitem row lands in partition
    // p = hash(l_orderkey) & (NUM_PARTS-1).
    // Thread-local staging: thread t maintains NUM_PARTS staging buffers.
    // After scan: thread p drains all threads' staging[p] buffers, builds agg map.
    //
    // This eliminates the separate aggregation_merge phase entirely.

    // AggMap keyed by l_orderkey, one per partition (owned by thread p after scan)
    using AggMap = gendb::CompactHashMap<int32_t, AggVal>;
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
        static constexpr size_t LI_BLOCK = 100000;
        size_t num_blocks = (total_lineitem + LI_BLOCK - 1) / LI_BLOCK;
        size_t first_qualifying_block = num_blocks; // default: nothing qualifies
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
        if (li_start_row >= total_lineitem) {
            // Nothing qualifies — output empty
        } else {

        static constexpr size_t MORSEL = 65536;
        size_t num_morsels = (total_lineitem - li_start_row + MORSEL - 1) / MORSEL;

        // Thread-local staging: thread t has NUM_PARTS buffers of LIStagingRow
        // Layout: staging[tid][part] = vector of LIStagingRow
        std::vector<std::vector<std::vector<LIStagingRow>>> staging(
            NUM_THREADS, std::vector<std::vector<LIStagingRow>>(NUM_PARTS));

        // Pre-reserve staging buffers to avoid repeated reallocs
        {
            size_t est_per_thread_per_part = 8192; // ~512KB per thread/part
            for (int t = 0; t < NUM_THREADS; ++t)
                for (int p = 0; p < NUM_PARTS; ++p)
                    staging[t][p].reserve(est_per_thread_per_part);
        }

        std::atomic<size_t> morsel_idx{0};

        // Phase 3a: parallel lineitem scan → scatter to staging buffers
        auto li_scan_worker = [&](int tid) {
            auto& my_staging = staging[tid];

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

                    // Scatter to correct partition
                    int p = (int)(gendb::hash_int(ok) & (uint64_t)(NUM_PARTS - 1));
                    my_staging[p].push_back({ok, rev});
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

        // Phase 3b: parallel aggregation — thread p owns partition p
        // Count total entries per partition for pre-sizing agg maps
        std::vector<size_t> part_counts(NUM_PARTS, 0);
        for (int t = 0; t < NUM_THREADS; ++t)
            for (int p = 0; p < NUM_PARTS; ++p)
                part_counts[p] += staging[t][p].size();

        for (int p = 0; p < NUM_PARTS; ++p) {
            // Estimate distinct l_orderkeys ~ partition count (upper bound)
            size_t est = std::max(part_counts[p], (size_t)256);
            agg_parts[p].reserve(est);
        }

        {
            GENDB_PHASE("aggregation_merge");
            std::atomic<int> part_task{0};

            auto agg_worker = [&](int /*tid*/) {
                int p;
                while ((p = part_task.fetch_add(1, std::memory_order_relaxed)) < NUM_PARTS) {
                    AggMap& am = agg_parts[p];

                    for (int t = 0; t < NUM_THREADS; ++t) {
                        for (const auto& row : staging[t][p]) {
                            int32_t ok = row.l_orderkey;
                            AggVal* av = am.find(ok);
                            if (av != nullptr) {
                                av->revenue_sum += row.revenue;
                            } else {
                                // First time: probe orders HT for metadata
                                int32_t odate = 0, opri = 0;
                                if (!orders_ht.find(ok, odate, opri)) continue; // bloom FP
                                am.insert(ok, {row.revenue, odate, opri});
                            }
                        }
                    }
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(NUM_THREADS);
            for (int t = 0; t < NUM_THREADS; ++t)
                threads.emplace_back(agg_worker, t);
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
