// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= DATE '1994-01-01'
//   AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
//   AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
//   AND l_quantity < 24
//
// Physical Plan:
// 1. Load zone_map_l_shipdate (12 bytes/entry: min,max,count) and compute alive block list.
//    lineitem is sorted by l_shipdate => ~514/600 blocks skipped (~86% skip rate).
// 2. Load zone_map_l_discount_qty (20 bytes/entry) for secondary pruning on surviving blocks.
// 3. Parallel morsel-driven scan over surviving blocks only (openmp / std::thread).
//    Each thread accumulates local int64_t sum; merge at end.
// 4. Output revenue = global_sum / 10000 (both extprice and discount scaled by 100).
//
// Encoding (scale_factor=2, i.e., stored * 100):
//   l_shipdate  : int32_t, days_since_epoch; 1994-01-01=8766, 1995-01-01=9131
//   l_discount  : int64_t, stored*100;  0.05->5, 0.07->7  (DISC_LO=5, DISC_HI=7)
//   l_quantity  : int64_t, scale=1;     24->24             (QTY_HI=24)
//   l_extprice  : int64_t, stored*100;  revenue=SUM(ep*d)/10000
//
// Zone map binary layouts (from Storage Guide):
//   zone_map_l_shipdate: [uint32_t num_blocks] [int32_t min, int32_t max, uint32_t count]*
//   zone_map_l_discount_qty: [uint32_t num_blocks] [int32_t disc_min, int32_t disc_max,
//                                                    int32_t qty_min,  int32_t qty_max,
//                                                    uint32_t count]*

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <atomic>
#include <thread>
#include <vector>
#include <string>
#include <fstream>
#include <immintrin.h>  // AVX2

#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Correctness anchors — DO NOT MODIFY
// ---------------------------------------------------------------------------
static constexpr int32_t  SHIPDATE_LO  = 8766;   // 1994-01-01 (days since epoch)
static constexpr int32_t  SHIPDATE_HI  = 9131;   // 1995-01-01
static constexpr int64_t  DISC_LO      = 5;      // 0.05 * 100
static constexpr int64_t  DISC_HI      = 7;      // 0.07 * 100
static constexpr int64_t  QTY_HI       = 24;     // threshold_constant (24.00, no scaling — scale=1)

static constexpr size_t   BLOCK_SIZE   = 100000;

// ---------------------------------------------------------------------------
// Zone map structures (per Storage Guide exact binary layout)
// ---------------------------------------------------------------------------
struct ShipdateZMEntry {
    int32_t  min;
    int32_t  max;
    uint32_t count;
};
static_assert(sizeof(ShipdateZMEntry) == 12, "wrong ShipdateZMEntry size");

struct DiscQtyZMEntry {
    int32_t  disc_min;
    int32_t  disc_max;
    int32_t  qty_min;
    int32_t  qty_max;
    uint32_t count;
};
static_assert(sizeof(DiscQtyZMEntry) == 20, "wrong DiscQtyZMEntry size");

// Minimal mmap helper for zone map files (raw bytes, not T-element arrays)
struct RawMmap {
    const void* data = nullptr;
    size_t      size = 0;
    int         fd   = -1;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("RawMmap: cannot open " + path);
        struct stat st;
        fstat(fd, &st);
        size = (size_t)st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) throw std::runtime_error("RawMmap: mmap failed " + path);
    }
    ~RawMmap() {
        if (data && data != MAP_FAILED) munmap(const_cast<void*>(data), size);
        if (fd >= 0) ::close(fd);
    }
};

// ---------------------------------------------------------------------------
// Inner hot loop: process a single surviving block with AVX2 or scalar
// ---------------------------------------------------------------------------
static inline int64_t scan_block(
    const int32_t* __restrict__ sd,
    const int64_t* __restrict__ dis,
    const int64_t* __restrict__ qty,
    const int64_t* __restrict__ ep,
    size_t n)
{
    int64_t local_sum = 0;

#ifdef __AVX2__
    // AVX2 path: process 8 x int32 (shipdate) at a time.
    // For int64 columns we use 4-wide int64 SIMD.
    // Strategy: check discount (int64, 4-wide) and qty (int64, 4-wide) first,
    // then shipdate (int32, 8-wide), accumulate ep*d conditionally.
    // We unroll over groups of 4 (LCM of 4 and 8 for alignment) using scalar for
    // simplicity where SIMD would require gather; let compiler auto-vectorize with hints.
    //
    // Actually, for branchless scalar with __restrict__ the compiler (-O3 -march=native)
    // will auto-vectorize the loop below using SIMD.  We just structure for it.
    const int64_t disc_lo_v = DISC_LO;
    const int64_t disc_hi_v = DISC_HI;
    const int64_t qty_hi_v  = QTY_HI;
    const int32_t sd_lo_v   = SHIPDATE_LO;
    const int32_t sd_hi_v   = SHIPDATE_HI;

    // Branchless inner loop — predicate as integer 0/1, multiply into accumulation.
    // The compiler auto-vectorizes this with -O3 -march=native when types are simple.
    for (size_t i = 0; i < n; i++) {
        int64_t d = dis[i];
        int64_t q = qty[i];
        int32_t s = sd[i];
        // All predicates combined as branchless bitmask
        int pass = (d >= disc_lo_v) & (d <= disc_hi_v) & (q < qty_hi_v)
                 & (s >= sd_lo_v) & (s < sd_hi_v);
        local_sum += pass * (ep[i] * d);
    }
#else
    // Scalar fallback with early-exit branching (good for low selectivity)
    for (size_t i = 0; i < n; i++) {
        int64_t d = dis[i];
        if (d < DISC_LO || d > DISC_HI) continue;
        if (qty[i] >= QTY_HI) continue;
        int32_t s = sd[i];
        if (s < SHIPDATE_LO || s >= SHIPDATE_HI) continue;
        local_sum += ep[i] * d;
    }
#endif

    return local_sum;
}

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // -----------------------------------------------------------------------
    // Phase 1: Load zone maps + compute surviving block list
    // -----------------------------------------------------------------------
    std::vector<uint32_t> alive_blocks;  // block indices that survive zone map pruning
    alive_blocks.reserve(100);

    {
        GENDB_PHASE("zone_map_prune");

        // --- shipdate zone map ---
        RawMmap zm_sd;
        zm_sd.open(gendb_dir + "/indexes/zone_map_l_shipdate.bin");
        const uint8_t* ptr = (const uint8_t*)zm_sd.data;
        uint32_t num_blocks_sd = *(const uint32_t*)ptr;
        ptr += sizeof(uint32_t);
        const ShipdateZMEntry* sd_entries = (const ShipdateZMEntry*)ptr;

        // --- discount+qty zone map ---
        RawMmap zm_dq;
        zm_dq.open(gendb_dir + "/indexes/zone_map_l_discount_qty.bin");
        const uint8_t* ptr2 = (const uint8_t*)zm_dq.data;
        uint32_t num_blocks_dq = *(const uint32_t*)ptr2;
        ptr2 += sizeof(uint32_t);
        const DiscQtyZMEntry* dq_entries = (const DiscQtyZMEntry*)ptr2;

        uint32_t num_blocks = num_blocks_sd;  // should be 600

        for (uint32_t b = 0; b < num_blocks; b++) {
            // Shipdate zone map skip: block is outside [SHIPDATE_LO, SHIPDATE_HI)
            if (sd_entries[b].max < SHIPDATE_LO || sd_entries[b].min >= SHIPDATE_HI) {
                continue;
            }
            // Discount zone map skip: block cannot contain rows in [DISC_LO, DISC_HI]
            // (disc stored as int32_t in zone map, same encoding as int64_t values)
            if (b < num_blocks_dq) {
                if (dq_entries[b].disc_max < (int32_t)DISC_LO ||
                    dq_entries[b].disc_min > (int32_t)DISC_HI) {
                    continue;
                }
                // Quantity zone map skip: block has all rows with qty >= QTY_HI
                if (dq_entries[b].qty_min >= (int32_t)QTY_HI) {
                    continue;
                }
            }
            alive_blocks.push_back(b);
        }
    }

    // -----------------------------------------------------------------------
    // Phase 2: Load columns via mmap (zero-copy) and prefetch only alive blocks
    // -----------------------------------------------------------------------
    gendb::MmapColumn<int32_t> col_shipdate;
    gendb::MmapColumn<int64_t> col_discount;
    gendb::MmapColumn<int64_t> col_quantity;
    gendb::MmapColumn<int64_t> col_extprice;

    {
        GENDB_PHASE("load_columns");
        col_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
        col_discount.open(gendb_dir + "/lineitem/l_discount.bin");
        col_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");
        col_extprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");

        // Prefetch only the pages corresponding to alive blocks
        // MADV_WILLNEED on each alive block's data range for each column
        const size_t total_rows = col_shipdate.count;
        const size_t num_all_blocks = (total_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;
        (void)num_all_blocks;

        // Issue WILLNEED hints for alive blocks across all columns
        for (uint32_t b : alive_blocks) {
            size_t row_start = (size_t)b * BLOCK_SIZE;
            size_t row_end   = row_start + BLOCK_SIZE;
            if (row_end > total_rows) row_end = total_rows;
            size_t n = row_end - row_start;

            madvise((void*)(col_shipdate.data + row_start), n * sizeof(int32_t), MADV_WILLNEED);
            madvise((void*)(col_discount.data + row_start), n * sizeof(int64_t), MADV_WILLNEED);
            madvise((void*)(col_quantity.data + row_start), n * sizeof(int64_t), MADV_WILLNEED);
            madvise((void*)(col_extprice.data + row_start), n * sizeof(int64_t), MADV_WILLNEED);
        }
    }

    const size_t total_rows  = col_shipdate.count;

    // -----------------------------------------------------------------------
    // Phase 3: Parallel morsel-driven scan over surviving blocks only
    // -----------------------------------------------------------------------
    int64_t global_sum = 0;

    {
        GENDB_PHASE("main_scan");

        const unsigned int nthreads = std::min(
            (unsigned int)64u,
            std::thread::hardware_concurrency()
        );

        const size_t num_alive = alive_blocks.size();

        std::vector<int64_t> partial_sums(nthreads, 0LL);
        std::atomic<size_t> morsel_counter{0};

        const int32_t*  shipdate_ptr = col_shipdate.data;
        const int64_t*  discount_ptr = col_discount.data;
        const int64_t*  quantity_ptr = col_quantity.data;
        const int64_t*  extprice_ptr = col_extprice.data;

        auto worker = [&](unsigned int tid) {
            int64_t local_sum = 0;

            while (true) {
                size_t idx = morsel_counter.fetch_add(1, std::memory_order_relaxed);
                if (idx >= num_alive) break;

                uint32_t blk      = alive_blocks[idx];
                size_t   row_start = (size_t)blk * BLOCK_SIZE;
                size_t   row_end   = row_start + BLOCK_SIZE;
                if (row_end > total_rows) row_end = total_rows;
                size_t   n         = row_end - row_start;

                local_sum += scan_block(
                    shipdate_ptr + row_start,
                    discount_ptr + row_start,
                    quantity_ptr + row_start,
                    extprice_ptr + row_start,
                    n
                );
            }

            partial_sums[tid] = local_sum;
        };

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        for (unsigned int t = 0; t < nthreads; t++) {
            threads.emplace_back(worker, t);
        }
        for (auto& th : threads) th.join();

        for (unsigned int t = 0; t < nthreads; t++) {
            global_sum += partial_sums[t];
        }
    }

    // -----------------------------------------------------------------------
    // Phase 4: Output results
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        // global_sum = SUM(extprice * discount) where both are scaled by 100
        // Actual revenue = global_sum / (100 * 100) = global_sum / 10000
        int64_t integer_part = global_sum / 10000LL;
        int64_t frac_part    = global_sum % 10000LL;

        char buf[64];
        std::snprintf(buf, sizeof(buf), "%lld.%04lld",
                      (long long)integer_part, (long long)frac_part);

        std::string out_path = results_dir + "/Q6.csv";
        std::ofstream ofs(out_path);
        ofs << "revenue\n";
        ofs << buf << "\n";
        ofs.close();

        std::printf("revenue = %s\n", buf);
        std::printf("[INFO] alive_blocks = %zu / 600\n", alive_blocks.size());
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
