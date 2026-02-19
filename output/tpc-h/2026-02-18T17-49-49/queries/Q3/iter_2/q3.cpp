// Q3: Shipping Priority — iter_2 optimizations
//
// Plan:
//   Phase 1 (dim_filter):   Scan customer, build DenseBitmap for 'BUILDING' customers (~2ms)
//   Phase 2 (build_joins):  Parallel scan orders with zone-map skip, build SINGLE PartitionedHashMap
//                           (64 partitions). Each thread writes to its assigned partitions ONLY
//                           (thread t handles partitions [t*step .. (t+1)*step-1]).
//                           Also build L2-resident bloom filter (128KB, 3-hash) inline.
//   Phase 3 (main_scan):    Parallel lineitem scan with zone-map block skip.
//                           Each thread aggregates into its OWN partition of a partitioned agg map.
//                           Partition ownership: thread t → agg partitions [t*step..(t+1)*step-1].
//                           No merge needed — just flatten all agg partitions at the end.
//   Phase 4 (sort_topk):   std::partial_sort on all groups → top 10.
//   Phase 5 (output):       Write CSV.
//
// Key changes vs iter_1:
//   1. Orders build: replace 2-pass (64×64 buffer + separate insert threads) with
//      single-pass partition-aware insert. Each thread owns N_PARTS/64 order partitions
//      → direct insert, zero contention, no intermediate buffers.
//   2. Bloom filter: L2-resident 128KB static array (3 hashes, ~1% FP at 1.44M keys).
//      Built inline during orders scan — not post-hoc.
//   3. Aggregation: eliminate the aggregation_merge phase entirely by using a
//      PartitionedHashMap<64> for global agg where lineitem thread t exclusively
//      probes/updates partitions owned by t → no merge, no extra traversal.
//   4. OpenMP parallel for replaces manual std::thread + atomic morsel dispatch
//      for cleaner load balancing.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
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
static constexpr size_t  NUM_PARTS       = 64; // partitions for orders and agg maps

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

// ── L2-resident Bloom filter (128KB = 1M bits, 3 hash functions) ─────────────
// At 1.44M keys: 1M bits / 1.44M keys ≈ 0.7 bits/key → ~3% FP with 3 hashes.
// Sized to 2M bits (256KB) for ~1% FP at 1.44M keys.
// Uses static array to guarantee L2/L3 residency after warm-up.
struct alignas(64) BloomFilter256K {
    // 256KB = 262144 bytes = 2097152 bits
    static constexpr size_t NBYTES = 262144;
    static constexpr size_t NBITS  = NBYTES * 8;
    static constexpr size_t MASK   = NBITS - 1;

    uint8_t bits[NBYTES];

    void clear() { memset(bits, 0, NBYTES); }

    inline void set(size_t b) {
        bits[b >> 3] |= (uint8_t)(1u << (b & 7));
    }
    inline bool get(size_t b) const {
        return (bits[b >> 3] >> (b & 7)) & 1u;
    }

    void insert(uint64_t h) {
        uint32_t h1 = (uint32_t)(h)        & MASK;
        uint32_t h2 = (uint32_t)(h >> 22)  & MASK;
        uint32_t h3 = (uint32_t)(h >> 43)  & MASK;
        set(h1); set(h2); set(h3);
    }

    bool may_contain(uint64_t h) const {
        uint32_t h1 = (uint32_t)(h)        & MASK;
        uint32_t h2 = (uint32_t)(h >> 22)  & MASK;
        uint32_t h3 = (uint32_t)(h >> 43)  & MASK;
        return get(h1) & get(h2) & get(h3);
    }
};

// Thread-local bloom filter halves to avoid false sharing during parallel build.
// We OR them together into the global bloom at the end of orders build.
struct alignas(64) BloomShard {
    uint8_t bits[BloomFilter256K::NBYTES];
    void clear() { memset(bits, 0, BloomFilter256K::NBYTES); }
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

        // Parallel customer scan (1.5M rows, two columns)
        #pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
        for (size_t i = 0; i < n; ++i) {
            if (seg[i] == building_code) {
                // cust_bitmap.set is non-atomic byte write — safe since each byte
                // covers 8 custkeys; false sharing risk is minimal for set-only pattern.
                // We use atomic OR to avoid data races on the same byte.
                size_t key = (size_t)ckey[i];
                uint8_t& byte_ref = cust_bitmap.bits[key >> 3];
                uint8_t  mask_bit = (uint8_t)(1 << (key & 7));
                // Atomic byte OR (GCC builtin) — safe, fast
                __atomic_fetch_or(&byte_ref, mask_bit, __ATOMIC_RELAXED);
            }
        }
    }

    // ── Phase 2: Build orders partitioned hash map ────────────────────────────
    // Strategy: 64 partitions. Thread t owns partitions [t*STEP .. (t+1)*STEP-1].
    // During orders scan, each thread hashes o_orderkey → only inserts into its owned partitions,
    // buffering rows destined for other partitions into small per-target-partition buffers.
    // After scan, each thread flushes its own partition's received data.
    //
    // Simpler and faster: use the partitioned approach where we scatter rows to
    // thread-local partition buffers, then each thread inserts its partitions.
    // This matches iter_1 but with a fused bloom filter build (no post-hoc traversal).

    using OrdersPartMap = gendb::CompactHashMap<int32_t, OrderPayload>;
    // 64 partition maps for orders
    std::vector<OrdersPartMap> orders_parts(NUM_PARTS);

    // Global bloom filter (256KB, L3-resident)
    static BloomFilter256K orders_bloom;
    orders_bloom.clear();

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey    (gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey     (gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate   (gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

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

        struct MatchedOrder { int32_t orderkey; int32_t orderdate; int32_t shippriority; };

        // Per-thread, per-partition scatter buffers
        // Layout: scatter[tid * NUM_PARTS + part]
        // Use flat array of vectors to minimize allocation overhead
        std::vector<std::vector<MatchedOrder>> scatter(NUM_THREADS * NUM_PARTS);
        for (auto& v : scatter) v.reserve(256);

        // Thread-local bloom shards — OR-merged at end
        std::vector<BloomShard> bloom_shards(NUM_THREADS);
        for (auto& s : bloom_shards) s.clear();

        // Pass A: parallel scan → scatter into per-thread per-partition buffers
        // + build per-thread bloom shard
        std::atomic<size_t> range_idx_a{0};
        size_t num_ranges = qual_ranges.size();

        #pragma omp parallel num_threads(NUM_THREADS)
        {
            int tid = omp_get_thread_num();
            BloomShard& my_bloom = bloom_shards[tid];

            while (true) {
                size_t ri = range_idx_a.fetch_add(1, std::memory_order_relaxed);
                if (ri >= num_ranges) break;

                size_t start = qual_ranges[ri].start;
                size_t end   = qual_ranges[ri].end;

                const int32_t* okey  = o_orderkey.data     + start;
                const int32_t* odate = o_orderdate.data    + start;
                const int32_t* ocust = o_custkey.data      + start;
                const int32_t* oship = o_shippriority.data + start;

                for (size_t i = 0, n = end - start; i < n; ++i) {
                    if (odate[i] < DATE_1995_03_15 && cust_bitmap.test((size_t)ocust[i])) {
                        int32_t okey_val = okey[i];
                        uint64_t h = gendb::hash_int(okey_val);
                        size_t part = h % NUM_PARTS;
                        scatter[(size_t)tid * NUM_PARTS + part].push_back(
                            {okey_val, odate[i], oship[i]});
                        // Build bloom shard inline
                        uint32_t b1 = (uint32_t)(h)        & BloomFilter256K::MASK;
                        uint32_t b2 = (uint32_t)(h >> 22)  & BloomFilter256K::MASK;
                        uint32_t b3 = (uint32_t)(h >> 43)  & BloomFilter256K::MASK;
                        my_bloom.bits[b1 >> 3] |= (uint8_t)(1u << (b1 & 7));
                        my_bloom.bits[b2 >> 3] |= (uint8_t)(1u << (b2 & 7));
                        my_bloom.bits[b3 >> 3] |= (uint8_t)(1u << (b3 & 7));
                    }
                }
            }
        }

        // Count total matched orders and size partitions
        std::vector<size_t> part_counts(NUM_PARTS, 0);
        for (int tid = 0; tid < NUM_THREADS; ++tid) {
            for (size_t p = 0; p < NUM_PARTS; ++p) {
                part_counts[p] += scatter[(size_t)tid * NUM_PARTS + p].size();
            }
        }
        for (size_t p = 0; p < NUM_PARTS; ++p) {
            orders_parts[p].reserve(std::max(part_counts[p] + part_counts[p] / 2, (size_t)256));
        }

        // Pass B: parallel insert — each thread owns its partitions, inserts from all scatter bufs
        // Simultaneously merge bloom shards in parallel across bytes
        // We interleave bloom merge and hash map insert to overlap work.
        #pragma omp parallel num_threads(NUM_THREADS)
        {
            int tid = omp_get_thread_num();

            // Bloom merge: thread tid handles byte range [tid*step .. (tid+1)*step)
            size_t bloom_step = (BloomFilter256K::NBYTES + NUM_THREADS - 1) / NUM_THREADS;
            size_t bloom_start = (size_t)tid * bloom_step;
            size_t bloom_end   = std::min(bloom_start + bloom_step, BloomFilter256K::NBYTES);
            for (size_t b = bloom_start; b < bloom_end; ++b) {
                uint8_t merged = 0;
                for (int t2 = 0; t2 < NUM_THREADS; ++t2) {
                    merged |= bloom_shards[t2].bits[b];
                }
                orders_bloom.bits[b] = merged;
            }

            // Hash map insert: thread tid handles partitions [tid*step2 .. (tid+1)*step2)
            // With NUM_PARTS == NUM_THREADS: each thread handles exactly 1 partition.
            size_t p_start = (size_t)tid;
            size_t p_end   = p_start + 1;
            for (size_t p = p_start; p < p_end; ++p) {
                OrdersPartMap& pm = orders_parts[p];
                for (int src_tid = 0; src_tid < NUM_THREADS; ++src_tid) {
                    for (auto& m : scatter[(size_t)src_tid * NUM_PARTS + p]) {
                        pm.insert(m.orderkey, {m.orderdate, m.shippriority});
                    }
                }
            }
        }
    }

    // Inline partition lookup
    auto orders_find = [&](int32_t orderkey) -> const OrderPayload* {
        size_t part = gendb::hash_int(orderkey) % NUM_PARTS;
        return orders_parts[part].find(orderkey);
    };

    // ── Phase 3: Scan lineitem with zone-map skip + direct-partitioned aggregation ──
    // CRITICAL CHANGE: each lineitem thread t exclusively owns agg partitions
    // [t*AGG_STEP .. (t+1)*AGG_STEP-1] where AGG_STEP = NUM_PARTS / NUM_THREADS = 1.
    // Thread t determines l_orderkey's agg partition and ONLY updates it if it belongs
    // to t's partition range. Otherwise it buffers the row.
    //
    // Simpler approach: use NUM_PARTS=64 agg partitions, each thread uses thread-local
    // agg map, then merge. But to eliminate merge: directly partition by orderkey into
    // 64 global agg partitions, each exclusively owned by one thread.
    // This requires lineitem threads to scatter rows, then each thread flushes its partition.
    //
    // Implementation: same scatter approach as orders build — but for aggregation.
    // Scatter struct contains (orderkey, revenue) — but we need orderdate/shippriority
    // from the orders map too. So we do a two-phase approach for aggregation:
    //
    // Faster: keep thread-local aggregation maps BUT eliminate the separate merge phase
    // by having merge happen inline in phase 3 after each morsel using partition ownership.
    //
    // Cleaner solution: Use 64 partition-owned agg maps. During lineitem scan,
    // each thread accumulates rows in its thread-local agg map (keyed by orderkey),
    // partitioned by hash(orderkey) % NUM_THREADS. Thread t only touches its own partition
    // of the global agg map — directly. This means:
    //   - No aggregation_merge phase
    //   - No thread-local maps to merge
    //   - Direct ownership → zero contention

    // Global agg partitions: NUM_PARTS partitions, partition p owned by thread p
    using AggPartMap = gendb::CompactHashMap<int32_t, AggVal>;
    std::vector<AggPartMap> global_agg(NUM_PARTS);
    // Pre-size based on expected unique orders (~1.44M / 64 per partition ≈ 22500 each)
    for (auto& gp : global_agg) gp.reserve(32768);

    // Lineitem scatter buffers: lscatter[tid * NUM_PARTS + part] for rows going to partition p
    // We store (orderkey, revenue_contrib) but we need orderdate/shippriority too.
    // HOWEVER: we only look those up on first insert. So we store full info needed.
    struct LineScatterRow { int32_t orderkey; int64_t revenue; };
    // Actually simpler: scatter rows to the owning partition's thread, have that thread
    // do the orders map lookup and aggregation.

    // But this doubles the memory traffic. Better: keep thread-local agg map approach
    // (iter_1 style) BUT make the merge faster.
    //
    // Alternative: since merge is 35ms, let's keep thread-local agg but use a faster
    // merge strategy: instead of 64 threads each sweeping all 64 local maps (4096 pairs,
    // each doing a hash mod check), just have 64 threads do a DIRECT partition-copy merge
    // using preallocated per-partition output arrays.
    //
    // Fastest option: DO NOT merge at all. Have each lineitem thread t write
    // ONLY into agg partitions it owns (determined by hash(orderkey) % NUM_THREADS == t).
    // This requires checking ownership of each row → rows not owned by t are SKIPPED
    // by t and will be handled by the correct thread.
    // BUT: different threads scan different lineitem morsels, and two different morsels
    // could have the same orderkey. We need one thread to handle all rows for a given
    // orderkey partition.
    //
    // Solution: PARTITION the lineitem scan itself by orderkey hash.
    // After probing bloom filter + orders map, each thread ONLY aggregates rows where
    // hash(orderkey) % NUM_THREADS == tid. Rows from other partitions are skipped by
    // this thread and will be picked up by the correct thread — BUT that thread also
    // needs to scan those lineitem rows.
    //
    // This would require each thread to scan ALL lineitem rows (too slow).
    //
    // BEST solution: Keep scatter approach but limit memory.
    // Phase 3a: parallel lineitem scan → per-thread per-partition scatter of (orderkey, revenue)
    // Phase 3b: each thread (owns partition p) inserts/accumulates from all scatter[*][p].
    //           On first insert, look up orders map for (orderdate, shippriority).
    // This eliminates aggregation_merge while keeping full parallelism.

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey      (gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice (gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount       (gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate       (gendb_dir + "/lineitem/l_shipdate.bin");

        mmap_prefetch_all(l_orderkey, l_extendedprice, l_discount, l_shipdate);

        size_t total_lineitem = l_orderkey.size();

        // Zone-map skip: lineitem sorted by l_shipdate, skip early blocks
        static constexpr size_t LI_BLOCK = 100000;
        size_t num_blocks = (total_lineitem + LI_BLOCK - 1) / LI_BLOCK;
        size_t first_qualifying_block = 0;
        {
            const int32_t* lship = l_shipdate.data;
            for (size_t b = 0; b < num_blocks; ++b) {
                size_t block_end = std::min((b + 1) * LI_BLOCK, total_lineitem);
                if (lship[block_end - 1] > DATE_1995_03_15) {
                    first_qualifying_block = b;
                    break;
                }
                if (b == num_blocks - 1) {
                    first_qualifying_block = num_blocks;
                }
            }
        }

        size_t li_start_row = first_qualifying_block * LI_BLOCK;
        if (li_start_row >= total_lineitem) goto done_scan;

        {
            // Scatter buffers: lscatter[tid * NUM_PARTS + part] = (orderkey, revenue) pairs
            // We keep the revenue contribution; the owning thread looks up orders map.
            struct AggScatterRow { int32_t orderkey; int64_t revenue; };
            std::vector<std::vector<AggScatterRow>> lscatter(NUM_THREADS * NUM_PARTS);
            for (auto& v : lscatter) v.reserve(512);

            static constexpr size_t MORSEL = 65536;
            size_t num_morsels = (total_lineitem - li_start_row + MORSEL - 1) / MORSEL;

            std::atomic<size_t> morsel_idx{0};

            // Phase 3a: scan lineitem → scatter (orderkey, revenue) by partition
            #pragma omp parallel num_threads(NUM_THREADS)
            {
                int tid = omp_get_thread_num();

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
                        uint64_t h = gendb::hash_int(orderkey);

                        // Bloom filter pre-check (L3-resident 256KB)
                        if (!orders_bloom.may_contain(h)) continue;

                        // Revenue: ep * (100 - discount)
                        int64_t rev = lprice[i] * (100LL - ldisc[i]);

                        // Scatter to partition
                        size_t part = h % NUM_PARTS;
                        lscatter[(size_t)tid * NUM_PARTS + part].push_back({orderkey, rev});
                    }
                }
            }

            // Phase 3b: each thread owns partition p = tid, inserts from all lscatter[*][p]
            #pragma omp parallel num_threads(NUM_THREADS)
            {
                int tid = omp_get_thread_num();
                size_t p = (size_t)tid; // thread t owns agg partition t
                AggPartMap& ap = global_agg[p];

                for (int src_tid = 0; src_tid < NUM_THREADS; ++src_tid) {
                    for (auto& row : lscatter[(size_t)src_tid * NUM_PARTS + p]) {
                        AggVal* av = ap.find(row.orderkey);
                        if (av != nullptr) {
                            av->revenue_sum += row.revenue;
                        } else {
                            // First time: look up orders map
                            const OrderPayload* op = orders_find(row.orderkey);
                            if (op == nullptr) continue; // bloom false positive
                            ap.insert(row.orderkey,
                                      {row.revenue, op->o_orderdate, op->o_shippriority});
                        }
                    }
                }
            }
        }
    }
    done_scan:;

    // ── Phase 4: Top-K extraction (no separate aggregation_merge needed) ──────
    std::vector<TopKEntry> top10;
    {
        GENDB_PHASE("sort_topk");

        size_t total_groups = 0;
        for (auto& gp : global_agg) total_groups += gp.size();

        std::vector<TopKEntry> all;
        all.reserve(total_groups);

        for (auto& gp : global_agg) {
            for (auto [key, val] : gp) {
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
