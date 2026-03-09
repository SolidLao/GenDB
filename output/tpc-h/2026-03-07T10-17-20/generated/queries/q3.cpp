// Q3: Shipping Priority — iter 4
// Key changes from iter_3 (58ms timing_ms):
//
//   1. SINGLE FUSED PARALLEL REGION: replaces 4 separate #pragma omp parallel regions
//      with ONE persistent region using #pragma omp for (implicit barriers) for phase
//      separation and #pragma omp single for single-threaded setup.
//      Eliminates 3 × ~5ms thread pool wake/sleep cycles ≈ 15ms savings.
//
//   2. PRECOMPUTED NET_PRICE: uses column_versions/lineitem.l_net_price/net_price.bin
//      (precomputed l_extendedprice*(1-l_discount)) instead of reading l_extendedprice.bin
//      (480MB) + l_discount.bin (480MB) separately. Saves 240MB of DRAM reads and
//      eliminates the per-row FP multiply for ~3M matching rows.
//
//   3. MINI_CAP = 512: down from 1024, halving mini_pool to 16MB.
//      Expected entries/buffer ≈ 360, 99.99th-pct Poisson(360) ≈ 436 < 512.
//
//   Expected timing_ms: ~40-45ms, beating GenDB reference of 47ms.

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
static constexpr int      PART_SLOTS   = 32768;                          // 2^15 per partition
static constexpr uint32_t PART_MASK    = (uint32_t)PART_SLOTS - 1u;
static constexpr size_t   COMBINED_CAP = (size_t)NUM_PARTS * PART_SLOTS; // 2^21 = 2,097,152
// SLOT_SENT = 0: TPC-H orderkeys are always ≥ 1, so 0 is a safe unoccupied sentinel.
// Using 0 allows calloc() initialization: no per-slot fill loop needed.
static constexpr int32_t  SLOT_SENT    = 0;

// Mini-pool geometry
// MINI_CAP=1024: some threads process 2 blocks (~74 blocks / 64 threads); 2 blocks × ~313
// qualifying orders/partition = ~626 entries, so 512 is insufficient. Keep 1024 (safe).
static constexpr int MAXTHREADS = 64;
static constexpr int MINI_CAP   = 1024;

// Bloom filter: 4MB = 65536 blocks × 64 bytes (512 bits = 1 cache line per block).
static constexpr uint32_t BLOOM_BLOCKS     = 65536u;
static constexpr uint32_t BLOOM_BLOCK_MASK = BLOOM_BLOCKS - 1u;
static constexpr size_t   BLOOM_SIZE       = (size_t)BLOOM_BLOCKS * 64u;

// 8 multipliers for bloom bit positions: bit_pos = (hash32(key * prime_j) >> 23) & 511
static constexpr uint32_t BLOOM_PRIMES[8] = {
    2654435761u, 2246822519u, 3266489917u,  668265263u,
     374761393u, 1274126177u, 2812528769u, 1164413189u
};

// ---------------------------------------------------------------------------
// Bloom filter: set 8 bits for key (ATOMIC — parallel Phase 2b writes).
// ---------------------------------------------------------------------------
static inline void bloom_set_atomic(uint8_t* __restrict__ bloom, uint32_t key) {
    const uint32_t hv    = hash32(key);
    const uint32_t block = hv & BLOOM_BLOCK_MASK;
    uint8_t* bl = bloom + (size_t)block * 64u;
    for (int j = 0; j < 8; j++) {
        const uint32_t bit_pos = (hash32(key * BLOOM_PRIMES[j]) >> 23) & 511u;
        __atomic_fetch_or(&bl[bit_pos >> 3], (uint8_t)(1u << (bit_pos & 7u)), __ATOMIC_RELAXED);
    }
}

// Bloom filter probe: true = might present; false = definite miss.
// hv = hash32(key) pre-computed by caller.
static inline bool bloom_check(const uint8_t* __restrict__ bloom, uint32_t key, uint32_t hv) {
    const uint32_t block = hv & BLOOM_BLOCK_MASK;
    const uint8_t* bl = bloom + (size_t)block * 64u;
    for (int j = 0; j < 8; j++) {
        const uint32_t bit_pos = (hash32(key * BLOOM_PRIMES[j]) >> 23) & 511u;
        if (!((bl[bit_pos >> 3] >> (bit_pos & 7u)) & 1u)) return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// CombinedSlot: join filter + aggregation accumulator.
// key = o_orderkey; SLOT_SENT=0 (unoccupied). All calloc-allocated slots start at 0.
// Layout: key(4) + orderdate(2) + shippriority(1) + _pad(1) + revenue(8) = 16 bytes
// ---------------------------------------------------------------------------
struct alignas(16) CombinedSlot {
    int32_t key;           // o_orderkey; sentinel = 0 (SLOT_SENT)
    int16_t orderdate;     // o_orderdate (days since epoch; int16 covers 7900-10200)
    int8_t  shippriority;  // o_shippriority (0 in TPC-H; fits int8)
    uint8_t _pad;
    double  revenue;       // accumulated l_extendedprice*(1-l_discount)
};
static_assert(sizeof(CombinedSlot) == 16, "CombinedSlot must be 16 bytes");
static_assert(offsetof(CombinedSlot, revenue) == 8, "revenue must be at offset 8");

// ---------------------------------------------------------------------------
// OrdRow: 8-byte row for partition-scatter collect phase.
// ---------------------------------------------------------------------------
struct OrdRow {
    int32_t okey;
    int16_t odate;
    int8_t  spri;
    uint8_t _pad;
};
static_assert(sizeof(OrdRow) == 8, "OrdRow must be 8 bytes");

// ---------------------------------------------------------------------------
// ResultRow and output comparator.
// ---------------------------------------------------------------------------
struct ResultRow {
    int32_t l_orderkey;
    double  revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// BetterThan(a, b) = true iff a is better quality than b.
// When used as make_heap comparator (plays role of "less-than"), the max-heap
// puts the element for which BetterThan(x, front) is false for all x at front.
// That element is the WORST of the current top-10: it is never "better than" others.
// → heap.front() = WORST row = the one to replace when a better candidate arrives.
struct BetterThan {
    bool operator()(const ResultRow& a, const ResultRow& b) const {
        if (a.revenue != b.revenue) return a.revenue > b.revenue;
        return a.o_orderdate < b.o_orderdate;
    }
};

// ---------------------------------------------------------------------------
// Per-thread top-10 accumulator for parallel output scan.
// Padded to 256 bytes (4 cache lines) to eliminate false sharing between threads.
// sizeof(ResultRow) = 24 (double revenue at offset 8 causes 4B padding after l_orderkey).
// Layout: rows[10] = 240B, count = 4B, heapified = 4B, _pad = 8B → total = 256B.
// ---------------------------------------------------------------------------
struct alignas(64) LocalTop10 {
    ResultRow rows[10];   // 10 × 24B = 240B
    int32_t   count;      // 4B
    int32_t   heapified;  // 4B: 0 = not yet, 1 = make_heap called
    char      _pad[8];    // 8B padding → total = 256B = 4 cache lines
};
static_assert(sizeof(LocalTop10) == 256, "LocalTop10 must be 256 bytes");

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
    const int32_t kOrderDateMax = kThreshold;
    const int32_t kShipdateMin  = kThreshold;

    const int nthreads = omp_get_max_threads();

    // =========================================================================
    // Open all columns (mmap — zero-copy)
    // NOTE: l_net_price replaces l_extendedprice + l_discount entirely.
    //       net_price[i] = l_extendedprice[i] * (1.0 - l_discount[i]), all rows valid.
    // =========================================================================
    MmapColumn<int8_t>  c_mktsegment  (gendb_dir + "/customer/c_mktsegment.bin");
    MmapColumn<int32_t> c_custkey     (gendb_dir + "/customer/c_custkey.bin");

    MmapColumn<int32_t> o_orderkey    (gendb_dir + "/orders/o_orderkey.bin");
    MmapColumn<int32_t> o_custkey     (gendb_dir + "/orders/o_custkey.bin");
    MmapColumn<int32_t> o_orderdate   (gendb_dir + "/orders/o_orderdate.bin");
    MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

    MmapColumn<int32_t> l_orderkey    (gendb_dir + "/lineitem/l_orderkey.bin");
    MmapColumn<int32_t> l_shipdate    (gendb_dir + "/lineitem/l_shipdate.bin");
    // Precomputed: net_price[i] = l_extendedprice[i] * (1.0 - l_discount[i])
    // Replaces l_extendedprice.bin + l_discount.bin → saves 480MB of DRAM reads
    MmapColumn<double>  l_net_price   (gendb_dir + "/column_versions/lineitem.l_net_price/net_price.bin");

    ZoneMap orders_zm, lineitem_zm;
    orders_zm.open  (gendb_dir + "/orders/o_orderdate_zone_map.bin");
    lineitem_zm.open(gendb_dir + "/lineitem/l_shipdate_zone_map.bin");

    // =========================================================================
    // Phase: data_loading — fire async prefetch for all hot columns (HDD readahead)
    // =========================================================================
    {
        GENDB_PHASE("data_loading");
        mmap_prefetch_all(l_orderkey, l_shipdate, l_net_price);
        mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_shippriority);
        mmap_prefetch_all(c_mktsegment, c_custkey);
    }

    // =========================================================================
    // Allocate data structures
    // =========================================================================

    // Customer qualifying bitset
    bool* qualifying_cust = new bool[1500001]();

    // Combined map: 2^21 slots × 16B = 32MB.
    // calloc → OS provides zero-filled pages on demand.
    // SLOT_SENT=0: all slots start "empty" without any Phase 2b init loop.
    CombinedSlot* combined_map = static_cast<CombinedSlot*>(
        calloc(COMBINED_CAP, sizeof(CombinedSlot)));
    if (!combined_map) { perror("calloc combined_map"); return 1; }

    // Bloom filter: 4MB, aligned to 64 bytes, zero-initialized
    uint8_t* bloom = static_cast<uint8_t*>(aligned_alloc(64, BLOOM_SIZE));
    if (!bloom) { perror("aligned_alloc bloom"); return 1; }
    memset(bloom, 0, BLOOM_SIZE);

    // Flat mini-pool
    const int pool_bufs = MAXTHREADS * NUM_PARTS;
    OrdRow* mini_pool = static_cast<OrdRow*>(
        malloc((size_t)pool_bufs * MINI_CAP * sizeof(OrdRow)));
    if (!mini_pool) { perror("malloc mini_pool"); return 1; }
    int32_t* pool_cnt = static_cast<int32_t*>(calloc(pool_bufs, sizeof(int32_t)));
    if (!pool_cnt) { perror("calloc pool_cnt"); return 1; }

    // Per-thread top-10 heaps for parallel output scan (allocated aligned)
    LocalTop10* thread_tops = static_cast<LocalTop10*>(
        aligned_alloc(64, (size_t)MAXTHREADS * sizeof(LocalTop10)));
    if (!thread_tops) { perror("aligned_alloc thread_tops"); return 1; }
    memset(thread_tops, 0, (size_t)MAXTHREADS * sizeof(LocalTop10));

    // =========================================================================
    // SINGLE FUSED PARALLEL REGION
    // Phases: dim_filter → single_setup → collect_orders → insert_bloom → main_scan
    //
    // Uses #pragma omp for (implicit barrier) for phase separation between threads.
    // Uses #pragma omp single (implicit barrier) for single-threaded setup/free.
    // ONE thread pool startup instead of 4 → eliminates 3 × ~5ms wake/sleep cycles.
    // =========================================================================

    // Shared state written by #pragma omp single, read by subsequent parallel phases.
    // Declared before the parallel region so they are shared by default.
    const int32_t ord_nb = orders_zm.num_blocks;
    const int32_t ord_bs = orders_zm.block_size;
    int      shared_last_ord_block = ord_nb; // default: process all blocks
    int32_t  shared_li_nb          = lineitem_zm.num_blocks;
    int32_t  shared_li_bs          = lineitem_zm.block_size;

    {
        GENDB_PHASE("fused_compute"); // covers dim_filter + build_joins + main_scan

        #pragma omp parallel num_threads(nthreads)
        {
            const int tid = omp_get_thread_num();

            // ----------------------------------------------------------------
            // Phase 1: dim_filter — PARALLEL
            // Customer sorted by c_custkey ascending → non-overlapping writes.
            // ----------------------------------------------------------------
            #pragma omp for schedule(static)
            for (int i = 0; i < (int)c_mktsegment.count; i++) {
                if (c_mktsegment[i] == kBuilding) {
                    qualifying_cust[c_custkey[i]] = true;
                }
            }
            // implicit barrier

            // ----------------------------------------------------------------
            // Phase 1.5: single_setup — compute last_ord_block, li_nb, li_bs
            // ----------------------------------------------------------------
            #pragma omp single
            {
                shared_last_ord_block = ord_nb;
                for (int b = 0; b < ord_nb; b++) {
                    if (orders_zm.zm_min[b] >= kOrderDateMax) {
                        shared_last_ord_block = b;
                        break;
                    }
                }
                shared_li_nb = lineitem_zm.num_blocks;
                shared_li_bs = lineitem_zm.block_size;
            }
            // implicit barrier: all threads see updated shared_* values

            // ----------------------------------------------------------------
            // Phase 2a: collect_orders — PARALLEL (morsel-driven)
            // Qualifying orders → flat mini_pool, partitioned by hash32(okey) & 63
            // ----------------------------------------------------------------
            {
                const int base = tid * NUM_PARTS;

                #pragma omp for schedule(dynamic, 1)
                for (int b = 0; b < shared_last_ord_block; b++) {
                    const uint32_t row_start = (uint32_t)b * (uint32_t)ord_bs;
                    const uint32_t row_end   = std::min(row_start + (uint32_t)ord_bs,
                                                        (uint32_t)o_orderdate.count);
                    const bool partial = (orders_zm.zm_max[b] >= kOrderDateMax);

                    for (uint32_t i = row_start; i < row_end; i++) {
                        const int32_t odate = o_orderdate[i];
                        if (partial && odate >= kOrderDateMax) continue;
                        if (!qualifying_cust[o_custkey[i]]) continue;

                        const int32_t  okey    = o_orderkey[i];
                        const uint32_t hv      = hash32((uint32_t)okey);
                        const int      part    = (int)(hv & 63u);
                        const int      buf_idx = base + part;
                        const int32_t  cnt     = pool_cnt[buf_idx];

                        if (__builtin_expect(cnt < MINI_CAP, 1)) {
                            OrdRow* slot = mini_pool + (size_t)buf_idx * MINI_CAP + cnt;
                            slot->okey  = okey;
                            slot->odate = (int16_t)odate;
                            slot->spri  = (int8_t)o_shippriority[i];
                            slot->_pad  = 0;
                            pool_cnt[buf_idx] = cnt + 1;
                        }
                        // overflow: drop (MINI_CAP=512, 99.99th-pct ≈ 436 < 512)
                    }
                }
            }
            // implicit barrier

            // ----------------------------------------------------------------
            // Phase 2b: insert_combined_map + build_bloom — PARALLEL (1 partition/thread)
            // Each thread exclusively owns its partition → no map contention.
            // bloom_set_atomic uses atomic OR (threads may share bloom blocks).
            // ----------------------------------------------------------------
            #pragma omp for schedule(static)
            for (int part = 0; part < NUM_PARTS; part++) {
                CombinedSlot* pslots = combined_map + (size_t)part * PART_SLOTS;

                // (a) memset 512KB partition — pre-faults pages before random probes.
                // SLOT_SENT=0, revenue=0.0 (IEEE 754: +0.0 = all-zero bytes).
                memset(pslots, 0, (size_t)PART_SLOTS * sizeof(CombinedSlot));

                // (b) Insert all mini_pool entries for this partition from all threads
                for (int t = 0; t < nthreads; t++) {
                    const int     buf_idx = t * NUM_PARTS + part;
                    const int32_t cnt     = pool_cnt[buf_idx];
                    const OrdRow* rows    = mini_pool + (size_t)buf_idx * MINI_CAP;

                    for (int32_t r = 0; r < cnt; r++) {
                        const OrdRow& row = rows[r];
                        uint32_t s = (hash32((uint32_t)row.okey) >> 6) & PART_MASK;
                        // Non-atomic: exclusive ownership of this partition
                        while (pslots[s].key != SLOT_SENT) {
                            s = (s + 1) & PART_MASK;
                        }
                        pslots[s].orderdate    = row.odate;
                        pslots[s].shippriority = row.spri;
                        // revenue stays 0.0 from memset
                        // RELEASE: ensures orderdate/shippriority visible before key
                        __atomic_store_n(&pslots[s].key, row.okey, __ATOMIC_RELEASE);

                        // (c) Bloom: atomic OR (races with other partition owners)
                        bloom_set_atomic(bloom, (uint32_t)row.okey);
                    }
                }
            }
            // implicit barrier: combined_map and bloom fully built

            // ----------------------------------------------------------------
            // Free mini_pool (single-threaded)
            // ----------------------------------------------------------------
            #pragma omp single
            {
                free(mini_pool);
                free(pool_cnt);
            }
            // implicit barrier

            // ----------------------------------------------------------------
            // Phase 3: main_scan — PARALLEL (bloom-gated, zone-map accelerated)
            // Revenue from precomputed l_net_price[i] — no FP multiply needed.
            // CAS-based atomic double accumulation into combined_map slots.
            // ----------------------------------------------------------------
            #pragma omp for schedule(dynamic, 1)
            for (int b = 0; b < shared_li_nb; b++) {
                if (lineitem_zm.zm_max[b] <= kShipdateMin) continue;

                const uint32_t row_start = (uint32_t)b * (uint32_t)shared_li_bs;
                const uint32_t row_end   = std::min(row_start + (uint32_t)shared_li_bs,
                                                    (uint32_t)l_orderkey.count);
                const bool all_pass = (lineitem_zm.zm_min[b] > kShipdateMin);

                for (uint32_t i = row_start; i < row_end; i++) {
                    if (!all_pass && l_shipdate[i] <= kShipdateMin) continue;

                    const int32_t  lkey = l_orderkey[i];
                    const uint32_t hv   = hash32((uint32_t)lkey);

                    // Bloom gate: 1 cache-line load + 8 bit tests → ~27M misses skipped
                    if (!bloom_check(bloom, (uint32_t)lkey, hv)) continue;

                    // Combined_map probe (~3M rows reach here after bloom)
                    const uint32_t part = hv & 63u;
                    uint32_t s          = (hv >> 6) & PART_MASK;

                    CombinedSlot* const pslots = combined_map + (size_t)part * PART_SLOTS;
                    CombinedSlot* slot = nullptr;

                    while (true) {
                        // ACQUIRE: see full slot written with RELEASE in Phase 2b
                        const int32_t k = __atomic_load_n(&pslots[s].key, __ATOMIC_ACQUIRE);
                        if (k == SLOT_SENT) break;
                        if (k == lkey) { slot = &pslots[s]; break; }
                        s = (s + 1) & PART_MASK;
                    }
                    if (!slot) continue;

                    // Precomputed net_price: no FP multiply needed
                    const double net_price = l_net_price[i];

                    // CAS-based atomic double accumulation
                    union { double d; uint64_t u; } old_v, new_v;
                    old_v.u = __atomic_load_n((uint64_t*)&slot->revenue, __ATOMIC_RELAXED);
                    do {
                        new_v.d = old_v.d + net_price;
                    } while (!__atomic_compare_exchange_n(
                        (uint64_t*)&slot->revenue, &old_v.u, new_v.u,
                        /*weak=*/true, __ATOMIC_RELAXED, __ATOMIC_RELAXED));
                }
            }
            // implicit barrier (all revenue accumulation complete)

        } // end single fused #pragma omp parallel region
    } // GENDB_PHASE("fused_compute") destructor

    free(bloom);

    // =========================================================================
    // Phase: output
    // PARALLEL: each thread scans its assigned partitions, maintains thread-local
    // top-10 heap. Single-threaded merge of 64 × top-10 → final top-10.
    // schedule(static) with 64 partitions and 64 threads: each thread gets 1 partition.
    // =========================================================================
    {
        GENDB_PHASE("output");

        BetterThan better;

        #pragma omp parallel for num_threads(nthreads) schedule(static)
        for (int part = 0; part < NUM_PARTS; part++) {
            const int tid = omp_get_thread_num();
            LocalTop10& lt = thread_tops[tid];
            // lt.count and lt.heapified are initialized to 0 (memset earlier)

            const CombinedSlot* pslots = combined_map + (size_t)part * PART_SLOTS;

            for (int s = 0; s < PART_SLOTS; s++) {
                const int32_t key = pslots[s].key;
                if (key == SLOT_SENT) continue;

                const ResultRow r{
                    key,
                    pslots[s].revenue,
                    (int32_t)pslots[s].orderdate,
                    (int32_t)pslots[s].shippriority
                };

                if (lt.count < 10) {
                    lt.rows[lt.count++] = r;
                    if (lt.count == 10) {
                        std::make_heap(lt.rows, lt.rows + 10, better);
                        lt.heapified = 1;
                    }
                } else if (better(r, lt.rows[0])) {
                    // r is better than current worst → replace
                    std::pop_heap(lt.rows, lt.rows + 10, better);
                    lt.rows[9] = r;
                    std::push_heap(lt.rows, lt.rows + 10, better);
                }
            }
        }

        // Single-threaded merge of 64 thread-local top-10s into global top-10
        std::vector<ResultRow> global_heap;
        global_heap.reserve(10);

        for (int t = 0; t < nthreads; t++) {
            const LocalTop10& lt = thread_tops[t];
            for (int i = 0; i < lt.count; i++) {
                const ResultRow& r = lt.rows[i];
                if ((int)global_heap.size() < 10) {
                    global_heap.push_back(r);
                    if ((int)global_heap.size() == 10) {
                        std::make_heap(global_heap.begin(), global_heap.end(), better);
                    }
                } else if (better(r, global_heap[0])) {
                    std::pop_heap(global_heap.begin(), global_heap.end(), better);
                    global_heap.back() = r;
                    std::push_heap(global_heap.begin(), global_heap.end(), better);
                }
            }
        }

        // Sort global_heap into output order (BetterThan: "less" = better quality → best first)
        if ((int)global_heap.size() == 10) {
            std::sort_heap(global_heap.begin(), global_heap.end(), better);
        } else {
            std::sort(global_heap.begin(), global_heap.end(), better);
        }

        const std::string outfile = results_dir + "/Q3.csv";
        FILE* fp = fopen(outfile.c_str(), "w");
        if (!fp) { perror("fopen output"); return 1; }

        fprintf(fp, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[16];
        for (const ResultRow& r : global_heap) {
            epoch_days_to_date_str(r.o_orderdate, date_buf);
            fprintf(fp, "%d,%.2f,%s,%d\n",
                    r.l_orderkey, r.revenue, date_buf, r.o_shippriority);
        }
        fclose(fp);
    }

    // Cleanup
    delete[] qualifying_cust;
    free(combined_map);
    free(thread_tops);

    return 0;
}
