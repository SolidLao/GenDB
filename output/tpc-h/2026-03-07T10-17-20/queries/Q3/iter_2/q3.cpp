// Q3: Shipping Priority — iter 2
// Pipeline: dim_filter → parallel_collect_and_partition_orders →
//           parallel_insert_combined_map → parallel_scan_lineitem_aggregate → topk
//
// Root cause of iter1 regression (288ms):
//   Parallel CAS merge phase operated on a ~100MB global agg map (4M slots × 24B)
//   exceeding the 44MB L3 → every CAS incurred ~100ns RAM latency + coherency storm.
//
// Fix: single combined_map = 32MB (fits in 44MB L3):
//   - 2^21 CombinedSlot entries (64 partitions × 32768 slots, 16B each)
//   - Serves as BOTH join filter AND revenue accumulator (no merge phase)
//   - Partition-scatter collect: thread tid pushes to mini_bufs[tid*64+part]
//   - Thread i exclusively inserts into partition i (zero contention)
//   - Lineitem probe CAS operates at L3 latency (~15-30ns)

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
// Zone map: on-disk format: int32_t num_blocks, block_size, min[N], max[N]
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
// Constants
// c_mktsegment == 1 → BUILDING (derived from encode_mktsegment: case 'B' → 1)
// ---------------------------------------------------------------------------
static constexpr int8_t kBuilding = 1;

// Combined map geometry
static constexpr int      NUM_PARTS    = 64;
static constexpr int      PART_SLOTS   = 32768;                         // 2^15 per partition
static constexpr uint32_t PART_MASK    = (uint32_t)PART_SLOTS - 1u;
static constexpr size_t   COMBINED_CAP = (size_t)NUM_PARTS * PART_SLOTS; // 2^21 = 2,097,152
static constexpr int32_t  SLOT_SENT    = INT32_MIN;                     // 0x80000000

// ---------------------------------------------------------------------------
// CombinedSlot: join filter + aggregation accumulator.
// key = o_orderkey (sentinel = INT32_MIN; TPC-H orderkeys are positive → safe)
// Layout: key(4) + orderdate(2) + shippriority(1) + _pad(1) + revenue(8) = 16 bytes
// alignas(16): 4 slots per 64-byte cache line → good density for probe + output scan
// ---------------------------------------------------------------------------
struct alignas(16) CombinedSlot {
    int32_t key;           // o_orderkey; sentinel = INT32_MIN
    int16_t orderdate;     // o_orderdate days since epoch (fits int16: ~7900-10200)
    int8_t  shippriority;  // o_shippriority (0 in TPC-H; fits int8)
    uint8_t _pad;
    double  revenue;       // accumulated l_extendedprice*(1-l_discount)
};
static_assert(sizeof(CombinedSlot) == 16, "CombinedSlot must be 16 bytes");
static_assert(offsetof(CombinedSlot, revenue) == 8, "revenue must be at offset 8");

// ---------------------------------------------------------------------------
// OrdRow: 8-byte row buffer for partition-scatter collect phase.
// Partition index   = hash32(okey) & 63       (bits 0-5)
// Within-part start = (hash32(okey) >> 6) & 32767  (bits 6-20; different range)
// ---------------------------------------------------------------------------
struct OrdRow {
    int32_t okey;
    int16_t odate;
    int8_t  spri;
    uint8_t _pad;
};
static_assert(sizeof(OrdRow) == 8, "OrdRow must be 8 bytes");

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

    mkdir(results_dir.c_str(), 0755);

    GENDB_PHASE_MS("total", total_ms);

    // Date threshold: 1995-03-15
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

    ZoneMap orders_zm, lineitem_zm;
    orders_zm.open  (gendb_dir + "/orders/o_orderdate_zone_map.bin");
    lineitem_zm.open(gendb_dir + "/lineitem/l_shipdate_zone_map.bin");

    // =========================================================================
    // Phase: data_loading — fire async prefetch for all hot columns
    // =========================================================================
    {
        GENDB_PHASE("data_loading");
        mmap_prefetch_all(l_orderkey, l_shipdate, l_extendedprice, l_discount);
        mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_shippriority);
        mmap_prefetch_all(c_mktsegment, c_custkey);
    }

    // =========================================================================
    // Allocate data structures
    // =========================================================================

    // Customer qualifying bitset: bool[1,500,001] indexed directly by c_custkey (~1.5MB)
    bool* qualifying_cust = new bool[1500001]();

    // Combined map: 2^21 slots * 16 bytes = 32MB (fits in 44MB L3)
    // Initialized per-partition by owner thread in Phase 2b.
    CombinedSlot* combined_map = static_cast<CombinedSlot*>(
        aligned_alloc(64, COMBINED_CAP * sizeof(CombinedSlot)));
    if (!combined_map) { perror("aligned_alloc combined_map"); return 1; }

    // Mini-buffers: mini_bufs[tid * NUM_PARTS + part]
    // Each thread writes only to its own rows (tid dimension) → no inter-thread writes.
    const int mini_count = nthreads * NUM_PARTS;
    std::vector<OrdRow>* mini_bufs = new std::vector<OrdRow>[mini_count];
    // Estimate: ~1.47M qualifying orders / 64 threads / 64 partitions ≈ 360 entries each
    for (int i = 0; i < mini_count; i++) mini_bufs[i].reserve(512);

    // =========================================================================
    // Phase: dim_filter
    // Single-threaded scan of 1.5M customer rows → build bool qualifying bitset
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
    //
    // Phase 2a — parallel_collect_and_partition_orders:
    //   Morsel-driven over orders blocks (zone-map: break when zm_min >= kOrderDateMax).
    //   For qualifying rows: push OrdRow to mini_bufs[tid * 64 + part].
    //   No inter-thread writes — each thread writes exclusively to its tid rows.
    //
    // Phase 2b — parallel_insert_combined_map:
    //   Thread i exclusively owns partition i (schedule(static) divides 64 partitions).
    //   (a) Fill partition with SLOT_SENT / 0.0 via a fill loop.
    //   (b) Insert from mini_bufs[*][i] with within-partition linear probe.
    //       Store key LAST with __ATOMIC_RELEASE so probe-side ACQUIRE sees valid slot.
    // =========================================================================
    {
        GENDB_PHASE("build_joins");

        // Find last block to process (sorted ascending → break on first zone_min ≥ threshold)
        const int32_t ord_nb = orders_zm.num_blocks;
        const int32_t ord_bs = orders_zm.block_size;
        int last_block = ord_nb;
        for (int b = 0; b < ord_nb; b++) {
            if (orders_zm.zm_min[b] >= kOrderDateMax) { last_block = b; break; }
        }

        // Phase 2a: parallel collect with partition scatter
        #pragma omp parallel num_threads(nthreads)
        {
            const int tid = omp_get_thread_num();

            #pragma omp for schedule(dynamic, 1)
            for (int b = 0; b < last_block; b++) {
                const uint32_t row_start = (uint32_t)b * (uint32_t)ord_bs;
                const uint32_t row_end   = std::min(row_start + (uint32_t)ord_bs,
                                                    (uint32_t)o_orderdate.count);
                // Partial block: zone_max may have some rows ≥ threshold → need per-row check
                const bool partial = (orders_zm.zm_max[b] >= kOrderDateMax);

                for (uint32_t i = row_start; i < row_end; i++) {
                    const int32_t odate = o_orderdate[i];
                    if (partial && odate >= kOrderDateMax) continue;
                    if (!qualifying_cust[o_custkey[i]]) continue;

                    const int32_t okey = o_orderkey[i];
                    const uint32_t hv  = hash32((uint32_t)okey);
                    const int part     = (int)(hv & 63u);
                    mini_bufs[tid * NUM_PARTS + part].push_back(
                        OrdRow{okey, (int16_t)odate, (int8_t)o_shippriority[i], 0u});
                }
            }
        } // implicit barrier: all threads done collecting before Phase 2b

        // Phase 2b: parallel insert — schedule(static) gives each thread exclusive partitions
        #pragma omp parallel for num_threads(nthreads) schedule(static)
        for (int part = 0; part < NUM_PARTS; part++) {
            CombinedSlot* pslots = combined_map + (size_t)part * PART_SLOTS;

            // Step (a): initialize this partition
            // Use fill loop — memset(0x80) would corrupt the double revenue field.
            // INT32_MIN = 0x80000000 cannot be set via simple byte-memset without side effects.
            for (int s = 0; s < PART_SLOTS; s++) {
                pslots[s].key          = SLOT_SENT;
                pslots[s].orderdate    = 0;
                pslots[s].shippriority = 0;
                pslots[s]._pad         = 0;
                pslots[s].revenue      = 0.0;
            }

            // Step (b): insert from all threads' buffers for this partition
            for (int t = 0; t < nthreads; t++) {
                for (const OrdRow& row : mini_bufs[t * NUM_PARTS + part]) {
                    // Within-partition probe: use bits 6-20 of hash (bits 0-5 used for partition)
                    uint32_t s = (hash32((uint32_t)row.okey) >> 6) & PART_MASK;
                    while (pslots[s].key != SLOT_SENT) {
                        s = (s + 1) & PART_MASK;
                    }
                    // Write metadata before publishing key
                    pslots[s].orderdate    = row.odate;
                    pslots[s].shippriority = row.spri;
                    pslots[s].revenue      = 0.0;
                    // RELEASE store: ensures probe-side ACQUIRE load sees fully initialized slot
                    __atomic_store_n(&pslots[s].key, row.okey, __ATOMIC_RELEASE);
                }
            }
        } // implicit barrier: all partitions fully populated before Phase 3
    }

    // Free mini-buffers before the ~2GB lineitem scan begins
    delete[] mini_bufs;
    mini_bufs = nullptr;

    // =========================================================================
    // Phase: main_scan
    // Parallel morsel-driven lineitem scan:
    //   - Zone map: skip blocks where zone_max ≤ kShipdateMin;
    //               omit per-row check where zone_min > kShipdateMin.
    //   - Probe combined_map: ACQUIRE load on key field (RELEASE from Phase 2b).
    //   - Atomic double accumulation via CAS loop (union double/uint64_t).
    //   - combined_map (32MB) is L3-resident → CAS at L3 latency (~15-30ns).
    // =========================================================================
    {
        GENDB_PHASE("main_scan");

        const int32_t li_nb = lineitem_zm.num_blocks;
        const int32_t li_bs = lineitem_zm.block_size;

        #pragma omp parallel for num_threads(nthreads) schedule(dynamic, 1)
        for (int b = 0; b < li_nb; b++) {
            if (lineitem_zm.zm_max[b] <= kShipdateMin) continue;  // skip leading blocks

            const uint32_t row_start = (uint32_t)b * (uint32_t)li_bs;
            const uint32_t row_end   = std::min(row_start + (uint32_t)li_bs,
                                                (uint32_t)l_orderkey.count);
            const bool all_pass = (lineitem_zm.zm_min[b] > kShipdateMin);

            for (uint32_t i = row_start; i < row_end; i++) {
                if (!all_pass && l_shipdate[i] <= kShipdateMin) continue;

                const int32_t lkey = l_orderkey[i];
                const uint32_t hv  = hash32((uint32_t)lkey);
                const uint32_t part = hv & 63u;
                uint32_t s          = (hv >> 6) & PART_MASK;

                CombinedSlot* const pslots = combined_map + (size_t)part * PART_SLOTS;
                CombinedSlot* slot = nullptr;

                while (true) {
                    // ACQUIRE load: synchronizes with RELEASE store in Phase 2b
                    const int32_t k = __atomic_load_n(&pslots[s].key, __ATOMIC_ACQUIRE);
                    if (k == SLOT_SENT) break;    // not in qualifying orders
                    if (k == lkey) { slot = &pslots[s]; break; }
                    s = (s + 1) & PART_MASK;
                }
                if (!slot) continue;

                const double revenue = l_extendedprice[i] * (1.0 - l_discount[i]);

                // Atomic double add via CAS loop.
                // On CAS failure old_v.u is updated to current value → old_v.d is correct.
                union { double d; uint64_t u; } old_v, new_v;
                old_v.u = __atomic_load_n((uint64_t*)&slot->revenue, __ATOMIC_RELAXED);
                do {
                    new_v.d = old_v.d + revenue;
                } while (!__atomic_compare_exchange_n(
                    (uint64_t*)&slot->revenue, &old_v.u, new_v.u,
                    /*weak=*/true, __ATOMIC_RELAXED, __ATOMIC_RELAXED));
            }
        }
    }

    // =========================================================================
    // Phase: output
    // Sequential scan of 2M combined_map slots; collect non-empty; partial_sort top-10.
    // =========================================================================
    {
        GENDB_PHASE("output");

        struct ResultRow {
            int32_t l_orderkey;
            double  revenue;
            int32_t o_orderdate;
            int32_t o_shippriority;
        };

        std::vector<ResultRow> results;
        results.reserve(1600000);
        for (size_t i = 0; i < COMBINED_CAP; i++) {
            if (combined_map[i].key == SLOT_SENT) continue;
            results.push_back({
                combined_map[i].key,
                combined_map[i].revenue,
                (int32_t)combined_map[i].orderdate,
                (int32_t)combined_map[i].shippriority
            });
        }

        auto cmp = [](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.o_orderdate < b.o_orderdate;
        };
        const size_t k = std::min((size_t)10, results.size());
        std::partial_sort(results.begin(), results.begin() + (ptrdiff_t)k, results.end(), cmp);

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
    free(combined_map);

    return 0;
}
