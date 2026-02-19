// Q3: Shipping Priority — iter_9 optimization
//
// Bottleneck analysis from iter_8 (360ms baseline from iter_5):
//   build_joins: 178ms (47%) — CAS contention on shared HT + massive thread-local bloom allocation
//   main_scan:   181ms (48%) — scatter to staging[64][64] pre-reserved at 8192 = 384MB allocated,
//                              only ~4-5M qualifying rows → massive TLB/cache thrashing
//
// Architectural changes in iter_9:
//
// Phase 2 (build_orders): Build orders HT SEQUENTIALLY (no CAS, no contention).
//   - Orders zone-map prunes to ~7.5M rows. Sequential scan is fast (~50ms).
//   - Then build SMALL bloom filter (128KB, L2-resident) in second pass over HT.
//   - Eliminates: CAS contention + 64×bloom-word-array allocation + OR-merge.
//
// Phase 3 (main_scan): ELIMINATE staging entirely.
//   - Per-partition spinlock (64 locks) protects 64 CompactHashMaps.
//   - Each worker thread directly aggregates qualifying rows into the correct partition.
//   - Since only ~4.5M of 30M rows qualify (after l_shipdate filter + bloom), lock contention
//     is low: ~70K hits per partition spread over 64 threads → ~1K hits per thread per partition.
//   - No staging allocation (was 384MB), no two-pass read.
//
// Phase 3b (aggregation_merge): Eliminated — aggregation is already done in Phase 3.
//
// Expected: build_joins ~40ms (sequential scan ~7.5M rows) + main_scan ~60ms → ~120ms total.

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

// ── Simple sequential flat open-addressing HT for orders ─────────────────────
// Built sequentially (no CAS needed). Read-only during lineitem probe phase.
// Single flat struct with parallel arrays for better cache layout during probe.
struct OrdersHT {
    static constexpr int32_t EMPTY = 0;

    int32_t* keys;    // o_orderkey (1-based, 0 = empty sentinel)
    int32_t* dates;   // o_orderdate
    int32_t* pris;    // o_shippriority
    uint64_t mask;
    size_t   cap;

    OrdersHT() : keys(nullptr), dates(nullptr), pris(nullptr), mask(0), cap(0) {}

    ~OrdersHT() {
        delete[] keys;
        delete[] dates;
        delete[] pris;
    }

    OrdersHT(const OrdersHT&) = delete;
    OrdersHT& operator=(const OrdersHT&) = delete;

    void init(size_t expected) {
        // Size to ~2.5× expected (~40% load factor) for low collision probability
        cap = 16;
        while (cap < expected * 5 / 2) cap <<= 1;
        mask  = cap - 1;
        keys  = new int32_t[cap]();  // zero-initialized (EMPTY=0)
        dates = new int32_t[cap]();
        pris  = new int32_t[cap]();
    }

    // Sequential insert (no atomics needed)
    inline void insert(int32_t key, int32_t date, int32_t priority) {
        uint64_t pos = gendb::hash_int(key) & mask;
        while (keys[pos] != EMPTY) {
            if (keys[pos] == key) return; // duplicate
            pos = (pos + 1) & mask;
        }
        keys[pos]  = key;
        dates[pos] = date;
        pris[pos]  = priority;
    }

    // Read-only find (safe for parallel probe after sequential build)
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

// ── Small L2-resident Bloom filter ────────────────────────────────────────────
// 128KB = 1<<20 bits → fits in L2 cache per core.
// ~1.44M keys × 10 bits each ≈ 1.37MB... Use 256KB (2<<20 bits) for ~1% FP rate.
// Actually for 1.44M keys: 1.44M × 10 / 8 = 1.8MB. Use 2MB for <2% FP rate.
// With 2MB bloom (2<<23 bits) we still get decent filtering while staying in L3.
//
// KEY INSIGHT: Use a fixed 128KB bloom (1<<20 bits) — higher FP rate (~5%) is OK
// because the real hash table probe will reject false positives. The benefit is
// the bloom stays in L2/L3 per core across all lineitem rows, saving cache misses.
struct SmallBloom {
    // 128KB = 131072 bytes = 1048576 bits
    static constexpr size_t NBYTES = 128 * 1024;
    static constexpr size_t NBITS  = NBYTES * 8;
    static constexpr size_t MASK   = NBITS - 1;

    uint8_t bits[NBYTES];

    SmallBloom() { memset(bits, 0, sizeof(bits)); }

    inline void insert(int32_t key) {
        uint64_t h = gendb::hash_int(key);
        // 3 hash positions for better FP rate
        uint32_t h1 = (uint32_t)(h         ) & MASK;
        uint32_t h2 = (uint32_t)(h >> 20   ) & MASK;
        uint32_t h3 = (uint32_t)(h >> 40   ) & MASK;
        bits[h1 >> 3] |= (1u << (h1 & 7));
        bits[h2 >> 3] |= (1u << (h2 & 7));
        bits[h3 >> 3] |= (1u << (h3 & 7));
    }

    inline bool may_contain(int32_t key) const {
        uint64_t h = gendb::hash_int(key);
        uint32_t h1 = (uint32_t)(h         ) & MASK;
        uint32_t h2 = (uint32_t)(h >> 20   ) & MASK;
        uint32_t h3 = (uint32_t)(h >> 40   ) & MASK;
        return (bits[h1 >> 3] & (1u << (h1 & 7))) &&
               (bits[h2 >> 3] & (1u << (h2 & 7))) &&
               (bits[h3 >> 3] & (1u << (h3 & 7)));
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

    // ── Phase 2: Build orders HT (sequential, no CAS) + small bloom filter ──
    // Sequential build eliminates CAS contention that cost 178ms in iter_8.
    // Zone-map prunes ~15M rows to ~7.5M. Single-threaded scan at ~2GB/s = ~15ms.
    OrdersHT orders_ht;
    SmallBloom orders_bloom;

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey    (gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey     (gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate   (gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        size_t total_orders = o_orderkey.size();

        // Load zone map to skip blocks with min >= DATE_1995_03_15
        gendb::ZoneMapIndex zone_map(gendb_dir + "/indexes/orders_o_orderdate.bin");

        // Pre-size HT: ~1.44M matched orders
        orders_ht.init(1500000UL);

        const int32_t* okey  = o_orderkey.data;
        const int32_t* odate = o_orderdate.data;
        const int32_t* ocust = o_custkey.data;
        const int32_t* oship = o_shippriority.data;

        // Sequential scan with zone-map pruning
        for (const auto& z : zone_map.zones) {
            if (z.min >= DATE_1995_03_15) continue; // skip zone entirely

            size_t row_start = (size_t)z.row_offset;
            size_t row_end   = std::min(row_start + (size_t)z.row_count, total_orders);

            for (size_t i = row_start; i < row_end; ++i) {
                if (odate[i] < DATE_1995_03_15 && cust_bitmap.test((size_t)ocust[i])) {
                    int32_t k = okey[i];
                    orders_ht.insert(k, odate[i], oship[i]);
                    orders_bloom.insert(k);
                }
            }
        }
        if (zone_map.zones.empty()) {
            // Fallback: no zone map, scan all
            for (size_t i = 0; i < total_orders; ++i) {
                if (odate[i] < DATE_1995_03_15 && cust_bitmap.test((size_t)ocust[i])) {
                    int32_t k = okey[i];
                    orders_ht.insert(k, odate[i], oship[i]);
                    orders_bloom.insert(k);
                }
            }
        }
    }

    // ── Phase 3: Scan lineitem + direct parallel aggregation ─────────────────
    // NO staging. Each thread directly aggregates into per-partition CompactHashMaps.
    // 64 partitions × 1 spinlock each. Contention is low: ~4.5M qualifying rows
    // split across 64 partitions = ~70K rows per partition across 64 threads.
    //
    // Per-partition spinlock avoids the scatter-read overhead of the old staging approach
    // and eliminates 384MB of staging allocation.

    using AggMap = gendb::CompactHashMap<int32_t, AggVal>;

    // Partition-level data: each partition has an AggMap + spinlock
    struct alignas(64) PartData {
        AggMap     agg_map;
        std::atomic_flag lock = ATOMIC_FLAG_INIT;
        char       _pad[64 - sizeof(AggMap) - sizeof(std::atomic_flag) > 0
                        ? 0 : 0]; // suppress padding warning
    };

    std::vector<PartData> parts(NUM_PARTS);
    // Pre-reserve each partition's agg map (~70K distinct orderkeys per partition / 64 = ~1100)
    // Actually distinct orderkeys ~ 1.44M / 64 = ~22500 per partition
    for (int p = 0; p < NUM_PARTS; ++p) {
        parts[p].agg_map.reserve(32768); // power-of-2, ~22500 keys
    }

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey     (gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount      (gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate      (gendb_dir + "/lineitem/l_shipdate.bin");

        mmap_prefetch_all(l_orderkey, l_extendedprice, l_discount, l_shipdate);

        size_t total_lineitem = l_orderkey.size();

        // Zone-map skip: lineitem sorted by l_shipdate → find first block where
        // block contains rows with l_shipdate > DATE_1995_03_15
        static constexpr size_t LI_BLOCK = 100000;
        size_t num_blocks = (total_lineitem + LI_BLOCK - 1) / LI_BLOCK;
        size_t li_start_row = 0;
        {
            const int32_t* lship = l_shipdate.data;
            // Binary search for first block whose last element > DATE_1995_03_15
            size_t lo = 0, hi = num_blocks;
            while (lo < hi) {
                size_t mid = (lo + hi) >> 1;
                size_t block_end = std::min((mid + 1) * LI_BLOCK, total_lineitem);
                if (lship[block_end - 1] <= DATE_1995_03_15) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            li_start_row = lo * LI_BLOCK;
        }

        if (li_start_row >= total_lineitem) goto done_scan;

        {
            static constexpr size_t MORSEL = 65536;
            size_t num_morsels = (total_lineitem - li_start_row + MORSEL - 1) / MORSEL;

            std::atomic<size_t> morsel_idx{0};

            // Parallel scan: each worker pulls morsels and directly aggregates
            auto li_worker = [&](int /*tid*/) {
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

                        // Determine partition
                        int p = (int)(gendb::hash_int(ok) & (uint64_t)(NUM_PARTS - 1));
                        auto& pd = parts[p];

                        // Acquire spinlock for this partition
                        while (pd.lock.test_and_set(std::memory_order_acquire)) {
                            // spin with pause hint
#if defined(__x86_64__) || defined(__i386__)
                            __asm__ volatile("pause" ::: "memory");
#endif
                        }

                        // Update aggregation map
                        AggVal* av = pd.agg_map.find(ok);
                        if (av != nullptr) {
                            av->revenue_sum += rev;
                        } else {
                            // First time: probe orders HT for metadata
                            int32_t odate = 0, opri = 0;
                            if (orders_ht.find(ok, odate, opri)) {
                                pd.agg_map.insert(ok, {rev, odate, opri});
                            }
                            // else: bloom false positive → skip
                        }

                        pd.lock.clear(std::memory_order_release);
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
    done_scan:;

    // ── Phase 4: Top-K extraction ─────────────────────────────────────────────
    std::vector<TopKEntry> top10;
    {
        GENDB_PHASE("sort_topk");

        size_t total_groups = 0;
        for (int p = 0; p < NUM_PARTS; ++p) total_groups += parts[p].agg_map.size();

        std::vector<TopKEntry> all;
        all.reserve(total_groups);

        for (int p = 0; p < NUM_PARTS; ++p) {
            for (auto [key, val] : parts[p].agg_map) {
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
