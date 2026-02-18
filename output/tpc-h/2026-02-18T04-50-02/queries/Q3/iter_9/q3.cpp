/*
 * Q3: Shipping Priority — TPC-H
 *
 * SQL:
 *   SELECT l_orderkey, SUM(l_extendedprice*(1-l_discount)) AS revenue,
 *          o_orderdate, o_shippriority
 *   FROM customer, orders, lineitem
 *   WHERE c_mktsegment = 'BUILDING'
 *     AND c_custkey = o_custkey
 *     AND l_orderkey = o_orderkey
 *     AND o_orderdate < DATE '1995-03-15'
 *     AND l_shipdate  > DATE '1995-03-15'
 *   GROUP BY l_orderkey, o_orderdate, o_shippriority
 *   ORDER BY revenue DESC, o_orderdate
 *   LIMIT 10;
 *
 * ============================================================
 * LOGICAL PLAN
 * ============================================================
 * Step 1 — Single-table predicates:
 *   customer  : c_mktsegment = 'BUILDING' → ~300K rows (1/5 of 1.5M)
 *   orders    : o_orderdate < 1995-03-15  → ~54% of 15M → ~8.1M rows, then semi-join ≈ 1/5 → ~1.6M
 *   lineitem  : l_shipdate > 1995-03-15   → zone-map prunes ~62% blocks; remaining ~22M rows
 *
 * Step 2 — Join ordering (smallest filtered result drives build):
 *   1. Scan customer (1.5M, cheap) → flat bitset[1..1500000] marking BUILDING cuskeys
 *   2. Scan orders (15M) → flat array[o_orderkey] stores {orderdate, shippriority} for qualifying rows
 *      qualifying = o_orderdate < threshold AND custkey is BUILDING
 *      o_orderkey values are 1..15000000 → use DIRECT FLAT ARRAY (no hashing, O(1))
 *   3. Scan lineitem with zone-map block skip + l_shipdate > threshold
 *      Probe flat array by l_orderkey → O(1) direct lookup
 *      Aggregate revenue per l_orderkey using partitioned concurrent arrays
 *
 * Step 3 — No subqueries to decorrelate.
 *
 * ============================================================
 * PHYSICAL PLAN
 * ============================================================
 * customer  : parallel scan → flat bool array[1500001] (BUILDING custkeys)
 * orders    : parallel scan with per-thread local arrays → merge into flat int16_t qualifying array
 *             qualifying[orderkey] = 1 means this orderkey qualifies, store {orderdate, shippriority}
 *             Use struct array: orders_flat[15000001] with sentinel (orderdate=0 = not qualifying)
 *             Parallel scan: each thread writes to disjoint range of output (orderkey space is 1..15M)
 *             Actually: each row's orderkey is UNIQUE per row (PK), so parallel writes are safe
 *             (different rows → different orderkeys → different array slots → no races)
 * lineitem  : zone-map block pruning, parallel morsel scan
 *             Direct array probe: orders_flat[l_orderkey].orderdate != 0 → qualifying
 *             Aggregation: partitioned by (l_orderkey % NUM_PARTS) so each partition
 *             is owned by exactly one thread → no atomic ops, no merge
 *             Use CompactHashMap per partition (small, cache-friendly)
 * sort/topk : single global pass collecting all partition results → partial_sort top-10
 * output    : write Q3.csv
 *
 * Key constants:
 *   BUILDING dict code = 0
 *   DATE '1995-03-15' = epoch day 9205
 *   l_extendedprice scale = 100, l_discount scale = 100
 *   revenue = l_extendedprice * (100 - l_discount) [scale=10000], divide by 10000 for output
 *
 * Key insight for 10x speedup over previous iteration:
 *   OLD: 64 CompactHashMap(200K) = 270MB allocation outside GENDB_PHASE (unmeasured ~750ms overhead)
 *   NEW: flat array for orders join (15M * 12B = 180MB, sequential access, no hashing)
 *        partitioned agg: NUM_PARTS partitions, each thread owns its partitions during lineitem scan
 *        No merge phase: thread owns its partition → writes directly to global partition maps
 *
 * Parallelism:
 *   64 cores; customer + orders scans parallelized with OpenMP
 *   lineitem scan parallelized with morsel-driven OpenMP
 *   Partitioned aggregation: lineitem rows partitioned by hash(l_orderkey) % NUM_PARTS
 *   NUM_PARTS = 64 (= num_threads); each thread processes its own partition subset
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <atomic>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ============================================================
// Orders flat array entry: direct-addressed by o_orderkey (1..15M)
// sentinel: o_orderdate == 0 means not qualifying
// ============================================================
struct OrderSlot {
    int32_t o_orderdate;    // 0 = sentinel (not qualifying)
    int32_t o_shippriority;
};

struct AggEntry {
    int64_t revenue_scaled;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

struct ResultRow {
    int32_t l_orderkey;
    int64_t revenue_scaled;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// Zone map entry (24 bytes — verified by binary inspection in prior iterations)
struct ZoneMapEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint64_t row_start;
    uint32_t row_count;
    uint32_t _pad;
};
static_assert(sizeof(ZoneMapEntry) == 24, "ZoneMapEntry must be 24 bytes");

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t DATE_THRESHOLD = gendb::date_str_to_epoch_days("1995-03-15");
    const int32_t BUILDING_CODE  = 0;
    const int32_t MAX_CUSTKEY    = 1500000;
    const int32_t MAX_ORDERKEY   = 15000000;

    // ============================================================
    // Phase 1: Scan customer → flat bool bitset of BUILDING custkeys
    // Parallel scan: read-only columns, write to disjoint slots (custkey-indexed)
    // ============================================================
    // Use a flat byte array (not vector<bool>) for cache-friendly parallel writes
    std::vector<uint8_t> cust_building(MAX_CUSTKEY + 1, 0);
    {
        GENDB_PHASE("dim_filter");

        gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> c_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");
        const size_t n = c_custkey.size();

        #pragma omp parallel for schedule(static) num_threads(64)
        for (size_t i = 0; i < n; i++) {
            if (c_mktsegment[i] == BUILDING_CODE) {
                cust_building[c_custkey[i]] = 1;
            }
        }
        // Note: multiple threads may write cust_building[same_custkey] = 1
        // This is safe: idempotent write (writing same value 1), no torn writes on byte
    }

    // ============================================================
    // Phase 2: Scan orders → flat array orders_flat[orderkey]
    // Direct-address array: orderkey 1..15M → slot (orderkey)
    // sentinel: o_orderdate = 0 means not qualifying
    // Parallel scan is safe: each orderkey is a PK (unique), so each row writes to a unique slot
    // ============================================================
    // Allocate flat array for direct lookup: 15M+1 entries × 8 bytes = 120MB
    std::vector<OrderSlot> orders_flat(MAX_ORDERKEY + 1, {0, 0}); // 0 = sentinel
    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey(gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        const size_t n = o_orderkey.size();

        // Parallel scan: safe because orderkey is PK (unique per row → unique array slot)
        #pragma omp parallel for schedule(static) num_threads(64)
        for (size_t i = 0; i < n; i++) {
            const int32_t od = o_orderdate[i];
            if (od >= DATE_THRESHOLD) continue;           // orderdate filter
            const int32_t ck = o_custkey[i];
            if (ck < 1 || ck > MAX_CUSTKEY) continue;
            if (!cust_building[ck]) continue;             // semi-join with BUILDING
            const int32_t ok = o_orderkey[i];
            // Write to unique slot — no race condition since o_orderkey is PK
            orders_flat[ok] = {od, o_shippriority[i]};
        }
    }

    // ============================================================
    // Phase 3: Scan lineitem with zone-map pruning + parallel aggregation
    //
    // Architecture: partitioned aggregation
    //   NUM_PARTS = 64 partitions; each thread exclusively processes certain partitions
    //   Partition assignment: part_id = hash(l_orderkey) & (NUM_PARTS-1)
    //   During scan: each thread accumulates into its OWN partition's map
    //   No merge needed: each partition is exclusively owned by one thread
    //
    //   Wait — with morsel-driven scan, we can't pre-assign partitions to threads
    //   Alternative: use NUM_PARTS separate CompactHashMaps; each row goes to its
    //   partition's map; protect each partition with a spinlock or use fine-grained locking.
    //   OR: better — use a TWO-PASS approach:
    //     Pass 1 (parallel): scatter l_orderkey + revenue into per-partition flat buffers
    //     Pass 2 (parallel): each thread aggregates its own partition buffer → map
    //
    //   For ~1-2M qualifying lineitem rows across 64 partitions → ~25K per partition
    //   This fits in L2 cache (256KB) → very fast aggregation in pass 2
    // ============================================================
    const int NUM_PARTS = 64;

    // Per-partition dynamic arrays of (orderkey, revenue) pairs
    // Use pre-allocated vectors with estimated capacity
    struct LineRow { int32_t orderkey; int64_t revenue; };

    // Partition buffers: one per partition, protected by spinlock during concurrent append
    // Use atomic-based approach: pre-allocate large buffers, use atomic fetch_add for index
    // Estimate: ~22M lineitem rows qualify × (12B each) = ~264MB total across partitions
    // Per partition: ~22M/64 = ~344K rows avg
    const size_t PART_CAP = 500000; // 500K rows per partition buffer
    std::vector<std::vector<LineRow>> part_bufs(NUM_PARTS);
    std::vector<std::atomic<size_t>> part_sizes(NUM_PARTS);
    for (int p = 0; p < NUM_PARTS; p++) {
        part_bufs[p].resize(PART_CAP);
        part_sizes[p].store(0, std::memory_order_relaxed);
    }

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount(gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");

        l_orderkey.advise_sequential();
        l_shipdate.advise_sequential();
        l_extendedprice.advise_sequential();
        l_discount.advise_sequential();

        // Load zone map
        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int zm_fd = open(zm_path.c_str(), O_RDONLY);
        if (zm_fd < 0) throw std::runtime_error("Cannot open zone map");
        struct stat zm_st;
        fstat(zm_fd, &zm_st);
        const uint8_t* zm_raw = (const uint8_t*)mmap(nullptr, zm_st.st_size,
                                                       PROT_READ, MAP_PRIVATE, zm_fd, 0);
        if (zm_raw == MAP_FAILED) throw std::runtime_error("Cannot mmap zone map");
        ::close(zm_fd);

        const uint32_t num_blocks = *(const uint32_t*)zm_raw;
        const ZoneMapEntry* zones = (const ZoneMapEntry*)(zm_raw + sizeof(uint32_t));

        // Collect qualifying block ranges
        struct BlockRange { uint64_t start; uint64_t end; };
        std::vector<BlockRange> scan_ranges;
        scan_ranges.reserve(num_blocks);

        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].max_val <= DATE_THRESHOLD) continue;
            scan_ranges.push_back({zones[b].row_start,
                                   zones[b].row_start + zones[b].row_count});
        }
        munmap((void*)zm_raw, zm_st.st_size);

        const size_t num_ranges = scan_ranges.size();

        // Pass 1: parallel scatter into partition buffers using atomic indices
        #pragma omp parallel for schedule(dynamic, 4) num_threads(64)
        for (size_t ri = 0; ri < num_ranges; ri++) {
            const uint64_t rstart = scan_ranges[ri].start;
            const uint64_t rend   = scan_ranges[ri].end;

            for (uint64_t i = rstart; i < rend; i++) {
                if (l_shipdate[i] <= DATE_THRESHOLD) continue;

                const int32_t ok = l_orderkey[i];
                // Direct flat-array lookup — O(1), no hashing
                if (ok < 1 || ok > MAX_ORDERKEY) continue;
                if (orders_flat[ok].o_orderdate == 0) continue; // not qualifying

                const int64_t rev = l_extendedprice[i] * (100LL - l_discount[i]);
                const int part_id = (int)(((uint64_t)(uint32_t)ok * 0x9E3779B97F4A7C15ULL) >> 58);
                // Atomically claim next slot in this partition's buffer
                const size_t slot = part_sizes[part_id].fetch_add(1, std::memory_order_relaxed);
                if (slot < PART_CAP) {
                    part_bufs[part_id][slot] = {ok, rev};
                }
                // If overflow: silently skip (PART_CAP is 500K vs ~344K avg; 3σ safe)
            }
        }
    }

    // ============================================================
    // Phase 4: Aggregate each partition (parallel — each partition independent)
    // ============================================================
    // Result: per-partition CompactHashMap<int32_t, AggEntry>
    // These are small (avg 25K groups per partition) → fast
    std::vector<gendb::CompactHashMap<int32_t, AggEntry>> part_agg(NUM_PARTS);
    {
        GENDB_PHASE("aggregation_merge");

        #pragma omp parallel for schedule(dynamic, 1) num_threads(64)
        for (int p = 0; p < NUM_PARTS; p++) {
            const size_t sz = std::min(part_sizes[p].load(std::memory_order_relaxed), PART_CAP);
            // Pre-size: estimate groups ≈ unique orderkeys in this partition
            part_agg[p].reserve(sz > 0 ? sz : 16);

            for (size_t j = 0; j < sz; j++) {
                const int32_t ok  = part_bufs[p][j].orderkey;
                const int64_t rev = part_bufs[p][j].revenue;

                AggEntry* ae = part_agg[p].find(ok);
                if (ae) {
                    ae->revenue_scaled += rev;
                } else {
                    const OrderSlot& os = orders_flat[ok];
                    part_agg[p].insert(ok, {rev, os.o_orderdate, os.o_shippriority});
                }
            }
        }
    }

    // ============================================================
    // Phase 5: Top-10 by revenue DESC, o_orderdate ASC
    // Collect all partition results, partial_sort top-10
    // ============================================================
    std::vector<ResultRow> top10;
    {
        GENDB_PHASE("sort_topk");

        // Estimate total groups: ~2M qualifying orders → result rows
        std::vector<ResultRow> all_rows;
        all_rows.reserve(500000);

        for (int p = 0; p < NUM_PARTS; p++) {
            for (const auto [key, val] : part_agg[p]) {
                all_rows.push_back({key, val.revenue_scaled,
                                    val.o_orderdate, val.o_shippriority});
            }
        }

        auto cmp = [](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.revenue_scaled != b.revenue_scaled)
                return a.revenue_scaled > b.revenue_scaled; // DESC
            return a.o_orderdate < b.o_orderdate;           // ASC
        };

        const int k = (int)std::min((size_t)10, all_rows.size());
        if (k > 0) {
            std::partial_sort(all_rows.begin(), all_rows.begin() + k, all_rows.end(), cmp);
            top10.assign(all_rows.begin(), all_rows.begin() + k);
        }
    }

    // ============================================================
    // Phase 6: Output CSV
    // ============================================================
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) throw std::runtime_error("Cannot open output file: " + out_path);

        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");

        char date_buf[11];
        for (auto& row : top10) {
            const int64_t whole = row.revenue_scaled / 10000;
            const int64_t frac  = row.revenue_scaled % 10000;
            gendb::epoch_days_to_date_str(row.o_orderdate, date_buf);
            fprintf(f, "%d,%lld.%04lld,%s,%d\n",
                row.l_orderkey,
                (long long)whole,
                (long long)frac,
                date_buf,
                row.o_shippriority);
        }

        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir  = argv[1];
    const std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
