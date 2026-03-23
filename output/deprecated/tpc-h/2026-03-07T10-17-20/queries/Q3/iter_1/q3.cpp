// Q3: Shipping Priority — optimized implementation
// Pipeline: dim_filter → parallel_collect_orders → serial_insert_orders_map →
//           parallel_scan_lineitem_aggregate → parallel_merge_agg → topk
//
// Key optimizations (vs previous 288ms):
//   (1) Compact 8-byte orders hash map slots (32MB, fully L3-resident)
//   (2) Per-thread local agg maps (avoid merge bottleneck)
//   (3) Parallel merge via atomic CAS on global agg map
//   (4) Two-phase parallel-collect + serial-insert for orders build

#include <cstdint>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <climits>
#include <cassert>
#include <cmath>
#include <vector>
#include <string>
#include <algorithm>
#include <atomic>
#include <new>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "mmap_utils.h"
#include "date_utils.h"

using namespace gendb;

// ---------------------------------------------------------------------------
// Hash function — must match prebuilt indexes (build_indexes.cpp)
// ---------------------------------------------------------------------------
static inline uint32_t hash32(uint32_t x) {
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = (x >> 16) ^ x;
    return x;
}

// ---------------------------------------------------------------------------
// Zone map reader: actual on-disk format is:
//   int32_t num_blocks, int32_t block_size, int32_t min[N], int32_t max[N]
// (NOT the ZoneMapIndex struct from mmap_utils.h)
// ---------------------------------------------------------------------------
struct ZoneMap {
    int32_t num_blocks = 0;
    int32_t block_size = 0;
    std::vector<int32_t> zm_min;
    std::vector<int32_t> zm_max;

    void open(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open zone map: " + path);
        if (::read(fd, &num_blocks, 4) != 4 || ::read(fd, &block_size, 4) != 4) {
            ::close(fd); throw std::runtime_error("Short read zone map header: " + path);
        }
        zm_min.resize(num_blocks);
        zm_max.resize(num_blocks);
        ssize_t nb4 = (ssize_t)num_blocks * 4;
        if (::read(fd, zm_min.data(), nb4) != nb4 ||
            ::read(fd, zm_max.data(), nb4) != nb4) {
            ::close(fd); throw std::runtime_error("Short read zone map data: " + path);
        }
        ::close(fd);
    }
};

// ---------------------------------------------------------------------------
// Date constants — computed at runtime from canonical date string
// 1995-03-15 = 9204 days since 1970-01-01 (verified against data)
// ---------------------------------------------------------------------------
static constexpr int8_t kBuilding = 1;  // encode_mktsegment('B')

// ---------------------------------------------------------------------------
// Orders hash map: compact 8-byte slots
// Capacity: 2^22 = 4,194,304. Total: 32MB — fits in L3.
// Sentinel: key == -1 (0xFFFFFFFF, can be set with memset 0xFF)
// ---------------------------------------------------------------------------
static constexpr uint32_t ORDERS_CAP  = 4194304u;
static constexpr uint32_t ORDERS_MASK = ORDERS_CAP - 1;
static constexpr int32_t  ORDERS_SENT = -1;

struct alignas(8) OrderSlot {
    int32_t key;          // o_orderkey; sentinel = -1
    int16_t orderdate;    // raw days since epoch (~7900-9300, fits int16)
    int8_t  shippriority;
    uint8_t _pad;
};
static_assert(sizeof(OrderSlot) == 8, "OrderSlot must be 8 bytes");

// ---------------------------------------------------------------------------
// Aggregation map: 24-byte slots (padded for 8-byte revenue alignment)
// Per-thread: capacity 2^17 = 131,072 (~3.1MB — L2/L3 resident)
// Global:     capacity 2^22 = 4,194,304 (96MB)
// Sentinel: key == -1
// ---------------------------------------------------------------------------
static constexpr uint32_t LOCAL_AGG_CAP   = 131072u;
static constexpr uint32_t LOCAL_AGG_MASK  = LOCAL_AGG_CAP - 1;
static constexpr uint32_t GLOBAL_AGG_CAP  = 4194304u;
static constexpr uint32_t GLOBAL_AGG_MASK = GLOBAL_AGG_CAP - 1;
static constexpr int32_t  AGG_SENT        = -1;

struct AggSlot {
    int32_t  key;           // l_orderkey; sentinel = -1
    int32_t  orderdate;     // o_orderdate (days since epoch)
    int32_t  shippriority;
    uint32_t _pad;          // padding to align revenue to offset 16
    int64_t  revenue;       // integer cents×10000 (exact accumulation, no FP rounding)
                            // at offset 16; 8-byte aligned (24 % 8 == 0 → all slots aligned)
};
static_assert(sizeof(AggSlot) == 24, "AggSlot must be 24 bytes");
static_assert(offsetof(AggSlot, revenue) == 16, "revenue must be at offset 16");

// ---------------------------------------------------------------------------
// OrdRow: row buffer for parallel orders collect phase
// ---------------------------------------------------------------------------
struct OrdRow {
    int32_t okey;
    int32_t odate;
    int32_t spri;
};

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    // Create results directory (ignore EEXIST)
    mkdir(results_dir.c_str(), 0755);

    GENDB_PHASE_MS("total", total_ms);

    // Date threshold: 1995-03-15 = 9204 days since epoch
    init_date_tables();
    const int32_t kThreshold    = date_str_to_epoch_days("1995-03-15");
    const int32_t kOrderDateMax = kThreshold;  // o_orderdate < kThreshold
    const int32_t kShipdateMin  = kThreshold;  // l_shipdate  > kThreshold

    const int nthreads = omp_get_max_threads();

    // =========================================================================
    // Open all columns (mmap — zero-copy)
    // =========================================================================
    MmapColumn<int8_t>  c_mktsegment    (gendb_dir + "/customer/c_mktsegment.bin");
    MmapColumn<int32_t> c_custkey       (gendb_dir + "/customer/c_custkey.bin");

    MmapColumn<int32_t> o_orderkey      (gendb_dir + "/orders/o_orderkey.bin");
    MmapColumn<int32_t> o_custkey       (gendb_dir + "/orders/o_custkey.bin");
    MmapColumn<int32_t> o_orderdate     (gendb_dir + "/orders/o_orderdate.bin");
    MmapColumn<int32_t> o_shippriority  (gendb_dir + "/orders/o_shippriority.bin");

    MmapColumn<int32_t> l_orderkey      (gendb_dir + "/lineitem/l_orderkey.bin");
    MmapColumn<int32_t> l_shipdate      (gendb_dir + "/lineitem/l_shipdate.bin");
    MmapColumn<double>  l_extendedprice (gendb_dir + "/lineitem/l_extendedprice.bin");
    MmapColumn<double>  l_discount      (gendb_dir + "/lineitem/l_discount.bin");

    // Zone maps (custom reader matching actual file format)
    ZoneMap orders_zm, lineitem_zm;
    orders_zm.open  (gendb_dir + "/orders/o_orderdate_zone_map.bin");
    lineitem_zm.open(gendb_dir + "/lineitem/l_shipdate_zone_map.bin");

    // =========================================================================
    // Phase: data_loading — fire async prefetch for all hot columns
    // =========================================================================
    {
        GENDB_PHASE("data_loading");
        // Prefetch lineitem columns (HDD: overlap I/O with CPU setup)
        mmap_prefetch_all(l_orderkey, l_shipdate, l_extendedprice, l_discount);
        // Prefetch orders columns
        mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_shippriority);
        // Prefetch customer columns
        mmap_prefetch_all(c_mktsegment, c_custkey);
    }

    // =========================================================================
    // Allocate data structures
    // =========================================================================

    // Customer qualifying bitset: bool[1,500,001] indexed by c_custkey (~1.5MB, L3-resident)
    bool* qualifying_cust = new bool[1500001]();

    // Orders hash map: 4M * 8 bytes = 32MB (L3-resident)
    // memset 0xFF → key field = 0xFFFFFFFF = -1 (sentinel) ✓
    OrderSlot* orders_map = static_cast<OrderSlot*>(
        aligned_alloc(64, ORDERS_CAP * sizeof(OrderSlot)));
    memset(orders_map, 0xFF, ORDERS_CAP * sizeof(OrderSlot));

    // Per-thread agg maps: nthreads × 131072 slots × 24 bytes
    const size_t local_map_bytes = (size_t)nthreads * LOCAL_AGG_CAP * sizeof(AggSlot);
    AggSlot* all_local = static_cast<AggSlot*>(aligned_alloc(64, local_map_bytes));
    // Initialize: revenue = 0.0 (memset 0 = IEEE 754 zero), key = -1
    memset(all_local, 0, local_map_bytes);
    for (int t = 0; t < nthreads; t++) {
        AggSlot* lm = all_local + (size_t)t * LOCAL_AGG_CAP;
        for (uint32_t i = 0; i < LOCAL_AGG_CAP; i++) lm[i].key = AGG_SENT;
    }

    // Global agg map: 4M * 24 bytes = 96MB
    AggSlot* global_map = static_cast<AggSlot*>(
        aligned_alloc(64, GLOBAL_AGG_CAP * sizeof(AggSlot)));
    // Parallel init: key = -1, revenue = 0.0
    memset(global_map, 0, GLOBAL_AGG_CAP * sizeof(AggSlot));
    #pragma omp parallel for schedule(static) num_threads(nthreads)
    for (uint32_t i = 0; i < GLOBAL_AGG_CAP; i++) global_map[i].key = AGG_SENT;

    // Per-thread order collect buffers
    std::vector<std::vector<OrdRow>> thread_orders(nthreads);
    for (int t = 0; t < nthreads; t++) thread_orders[t].reserve(32768);

    // =========================================================================
    // Phase: dim_filter
    // Single-threaded: scan customer, build qualifying_cust bool bitset
    // ~1.5MB data; ~7ms
    // =========================================================================
    {
        GENDB_PHASE("dim_filter");
        const size_t n_cust = c_mktsegment.count;
        for (size_t i = 0; i < n_cust; i++) {
            if (c_mktsegment[i] == kBuilding) {
                qualifying_cust[c_custkey[i]] = true;
            }
        }
    }

    // =========================================================================
    // Phase: build_joins
    // 2a) Parallel collect: all threads scan orders zones, push qualifying rows
    //     to per-thread vectors.
    // 2b) Serial insert: single thread inserts all collected OrdRows into the
    //     compact orders hash map.
    // =========================================================================
    {
        GENDB_PHASE("build_joins");

        // Find the last block to process (orders sorted ascending by o_orderdate;
        // break when zm_min[b] >= kOrderDateMax — all later blocks also fail)
        const int32_t ord_nb = orders_zm.num_blocks;
        const int32_t ord_bs = orders_zm.block_size;
        int last_block = ord_nb;
        for (int b = 0; b < ord_nb; b++) {
            if (orders_zm.zm_min[b] >= kOrderDateMax) {
                last_block = b;
                break;
            }
        }

        // Phase 2a: parallel collect across orders blocks
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& buf = thread_orders[tid];

            #pragma omp for schedule(dynamic, 1)
            for (int b = 0; b < last_block; b++) {
                const uint32_t row_start = (uint32_t)b * (uint32_t)ord_bs;
                const uint32_t row_end   = std::min(row_start + (uint32_t)ord_bs,
                                                    (uint32_t)o_orderdate.count);
                // Partial block: max may meet/exceed threshold → need per-row check
                const bool partial = (orders_zm.zm_max[b] >= kOrderDateMax);

                for (uint32_t i = row_start; i < row_end; i++) {
                    const int32_t odate = o_orderdate[i];
                    if (partial && odate >= kOrderDateMax) continue;
                    const int32_t ckey = o_custkey[i];
                    if (!qualifying_cust[ckey]) continue;
                    buf.push_back({o_orderkey[i], odate, o_shippriority[i]});
                }
            }
        } // implicit barrier — all threads done collecting

        // Phase 2b: serial insert into compact orders hash map
        // ~1.47M inserts into 32MB L3-resident map, ~10ns/insert → ~15ms
        for (int t = 0; t < nthreads; t++) {
            for (const OrdRow& row : thread_orders[t]) {
                uint32_t h = hash32((uint32_t)row.okey) & ORDERS_MASK;
                while (orders_map[h].key != ORDERS_SENT) {
                    h = (h + 1) & ORDERS_MASK;
                }
                orders_map[h].key          = row.okey;
                orders_map[h].orderdate    = (int16_t)row.odate;
                orders_map[h].shippriority = (int8_t)row.spri;
            }
        }
    }

    // Free order collect buffers (release memory before lineitem scan)
    for (int t = 0; t < nthreads; t++) {
        std::vector<OrdRow>().swap(thread_orders[t]);
    }

    // =========================================================================
    // Phase: main_scan
    // 3) Parallel morsel-driven lineitem scan:
    //    - Zone map for l_shipdate to skip/optimize blocks
    //    - Probe compact orders hash map (L3-resident, 32MB)
    //    - Aggregate into per-thread local agg maps (no locking)
    // 4) Parallel merge (overlapped with scan via nowait):
    //    - Each thread merges its local map into global_map using atomic CAS
    //    - Key claim: __atomic_compare_exchange_n on int32 key
    //    - Revenue: std::atomic<double>::compare_exchange_weak spin
    // =========================================================================
    {
        GENDB_PHASE("main_scan");

        const int32_t li_nb = lineitem_zm.num_blocks;
        const int32_t li_bs = lineitem_zm.block_size;

        #pragma omp parallel num_threads(nthreads)
        {
            const int tid = omp_get_thread_num();
            AggSlot* __restrict__ lmap = all_local + (size_t)tid * LOCAL_AGG_CAP;

            // Phase 3: parallel lineitem scan (morsel-driven over blocks)
            #pragma omp for schedule(dynamic, 1) nowait
            for (int b = 0; b < li_nb; b++) {
                // Skip block entirely if all rows are before shipdate threshold
                if (lineitem_zm.zm_max[b] <= kShipdateMin) continue;

                const uint32_t row_start = (uint32_t)b * (uint32_t)li_bs;
                const uint32_t row_end   = std::min(row_start + (uint32_t)li_bs,
                                                    (uint32_t)l_orderkey.count);
                // If entire block is after threshold, skip per-row shipdate check
                const bool all_pass = (lineitem_zm.zm_min[b] > kShipdateMin);

                for (uint32_t i = row_start; i < row_end; i++) {
                    // Per-row shipdate filter (skipped for hot blocks where all_pass)
                    if (!all_pass && l_shipdate[i] <= kShipdateMin) continue;

                    const int32_t lkey = l_orderkey[i];

                    // Probe compact orders hash map (L3-resident, 32MB)
                    uint32_t h = hash32((uint32_t)lkey) & ORDERS_MASK;
                    int32_t odate = 0, spri = 0;
                    bool found = false;
                    while (true) {
                        const int32_t ok = orders_map[h].key;
                        if (ok == ORDERS_SENT) break;   // not in qualifying orders
                        if (ok == lkey) {
                            odate = (int32_t)orders_map[h].orderdate;
                            spri  = (int32_t)orders_map[h].shippriority;
                            found = true;
                            break;
                        }
                        h = (h + 1) & ORDERS_MASK;
                    }
                    if (!found) continue;

                    // revenue = l_extendedprice * (1 - l_discount), scaled ×10000 as int64
                    // llround gives exact integer since values have ≤4 decimal places
                    const int64_t rev_int = llround(
                        l_extendedprice[i] * (1.0 - l_discount[i]) * 10000.0);

                    // Insert/accumulate into per-thread local agg map (no locking)
                    uint32_t ah = hash32((uint32_t)lkey) & LOCAL_AGG_MASK;
                    while (true) {
                        const int32_t ak = lmap[ah].key;
                        if (ak == AGG_SENT) {
                            lmap[ah].key          = lkey;
                            lmap[ah].orderdate    = odate;
                            lmap[ah].shippriority = spri;
                            lmap[ah].revenue      = rev_int;
                            break;
                        }
                        if (ak == lkey) {
                            lmap[ah].revenue += rev_int;
                            break;
                        }
                        ah = (ah + 1) & LOCAL_AGG_MASK;
                    }
                }
            }
            // nowait: each thread immediately proceeds to Phase 4 after finishing
            // its lineitem zones. Global map uses atomics → no data race.

            // Phase 4: parallel merge — each thread merges its own local map
            // into global_map using atomic CAS (concurrent across threads)
            for (uint32_t i = 0; i < LOCAL_AGG_CAP; i++) {
                if (lmap[i].key == AGG_SENT) continue;

                const int32_t lkey  = lmap[i].key;
                const int32_t ldate = lmap[i].orderdate;
                const int32_t lspri = lmap[i].shippriority;
                const int64_t lrev  = lmap[i].revenue;

                // Find or claim slot in global map via linear probing + CAS
                uint32_t gh = hash32((uint32_t)lkey) & GLOBAL_AGG_MASK;
                uint32_t target = UINT32_MAX;

                while (target == UINT32_MAX) {
                    int32_t cur_key = __atomic_load_n(&global_map[gh].key, __ATOMIC_ACQUIRE);
                    if (cur_key == lkey) {
                        // Slot already claimed for our key by another thread
                        target = gh;
                    } else if (cur_key == AGG_SENT) {
                        // Attempt to claim this empty slot
                        int32_t expected = AGG_SENT;
                        if (__atomic_compare_exchange_n(
                                &global_map[gh].key, &expected, lkey,
                                /*weak=*/false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
                            // We claimed the slot — write metadata
                            global_map[gh].orderdate    = ldate;
                            global_map[gh].shippriority = lspri;
                            target = gh;
                        } else {
                            // CAS failed — re-read and check if they put our key
                            cur_key = __atomic_load_n(&global_map[gh].key, __ATOMIC_ACQUIRE);
                            if (cur_key == lkey) {
                                target = gh;
                            } else {
                                // Different key claimed this slot — linear probe
                                gh = (gh + 1) & GLOBAL_AGG_MASK;
                            }
                        }
                    } else {
                        // Different key in this slot — linear probe
                        gh = (gh + 1) & GLOBAL_AGG_MASK;
                    }
                }

                // Atomically accumulate revenue using atomic fetch_add on int64
                // Revenue at offset 16, 8-byte aligned → single LOCK XADD instruction
                // No CAS spin loop needed — much faster than atomic double CAS
                __atomic_fetch_add(&global_map[target].revenue, lrev, __ATOMIC_RELAXED);
            }
        } // implicit barrier — all threads done with scan + merge
    }

    // =========================================================================
    // Phase: output
    // Scan global agg map for non-empty slots, partial_sort top-10, write CSV
    // =========================================================================
    {
        GENDB_PHASE("output");

        struct ResultRow {
            int32_t l_orderkey;
            double  revenue;
            int32_t o_orderdate;
            int32_t o_shippriority;
        };

        // Collect all aggregated groups from global map (~1.47M entries)
        std::vector<ResultRow> results;
        results.reserve(1600000);
        for (uint32_t i = 0; i < GLOBAL_AGG_CAP; i++) {
            if (global_map[i].key == AGG_SENT) continue;
            // Convert int64 revenue (scaled ×10000) back to double for output
            results.push_back({
                global_map[i].key,
                (double)global_map[i].revenue / 10000.0,
                global_map[i].orderdate,
                global_map[i].shippriority
            });
        }

        // Partial sort: top-10 by revenue DESC, o_orderdate ASC
        auto cmp = [](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;  // DESC
            return a.o_orderdate < b.o_orderdate;                       // ASC
        };
        const size_t k = std::min((size_t)10, results.size());
        std::partial_sort(results.begin(), results.begin() + k, results.end(), cmp);

        // Write CSV output
        const std::string outfile = results_dir + "/Q3.csv";
        FILE* fp = fopen(outfile.c_str(), "w");
        if (!fp) { perror("fopen output"); return 1; }

        fprintf(fp, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[16];
        for (size_t i = 0; i < k; i++) {
            const ResultRow& r = results[i];
            epoch_days_to_date_str(r.o_orderdate, date_buf);
            fprintf(fp, "%d,%.2f,%s,%d\n",
                    r.l_orderkey, r.revenue, date_buf, r.o_shippriority);
        }
        fclose(fp);
    }

    // Cleanup
    delete[] qualifying_cust;
    free(orders_map);
    free(all_local);
    free(global_map);

    return 0;
}
