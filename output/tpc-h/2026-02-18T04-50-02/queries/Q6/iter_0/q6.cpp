/*
 * Q6: Forecasting Revenue Change
 *
 * SQL:
 *   SELECT SUM(l_extendedprice * l_discount) AS revenue
 *   FROM lineitem
 *   WHERE l_shipdate >= DATE '1994-01-01'
 *     AND l_shipdate < DATE '1995-01-01'
 *     AND l_discount BETWEEN 0.05 AND 0.07
 *     AND l_quantity < 24.00
 *
 * ============================================================
 * LOGICAL PLAN
 * ============================================================
 * Table: lineitem (~60M rows)
 * Predicates (all single-table, pushed to scan):
 *   - l_shipdate >= 8766 AND l_shipdate < 9131  (epoch days, int32_t)
 *   - l_discount BETWEEN 5 AND 7               (scaled x100: 0.05→5, 0.07→7)
 *   - l_quantity < 2400                         (scaled x100: 24.00→2400)
 * Aggregation: SUM(l_extendedprice * l_discount) — single scalar, no GROUP BY
 * No joins required.
 *
 * ============================================================
 * PHYSICAL PLAN
 * ============================================================
 * Scan Strategy:
 *   - Zone map pruning on l_shipdate (sorted column):
 *       Layout: [uint32_t num_blocks] then per block:
 *       [int32_t min, int32_t max, uint64_t row_start, uint64_t row_count] = 24 bytes
 *       ~1 year of ~7 years → eliminates ~85% of blocks
 *       Qualifying: ~9.2M rows in a contiguous range
 *   - Merge qualifying blocks into a single row range [lo, hi)
 *
 * Parallelism:
 *   - OpenMP parallel for over the qualifying row range
 *   - Static chunked scheduling (large morsels fit L3 cache = 88MB)
 *   - Thread-local int64_t accumulators, reduce at end
 *   - 64 cores × ~1.5M rows = fast parallel scan
 *
 * Aggregation:
 *   - Single scalar per thread, summed at end (no hash table)
 *   - Product: ep * disc (both scaled x100) → scale x10000
 *   - Output: global_sum / 10000 with 2dp
 *
 * Precision:
 *   - int64_t safe: max ep ~100M (scaled), max disc=10, product=1B/row
 *     9.2M rows × 1B = 9.2e15 << 9.2e18 (int64_t max) ✓
 *
 * Output: results/Q6.csv with header "revenue" and 2 decimal places
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"
#include "date_utils.h"

// Zone map entry layout (verified from actual binary):
// [uint32_t num_blocks] header
// Per block: [int32_t min, int32_t max, uint64_t row_start, uint64_t row_count] = 24 bytes
struct ZoneEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint64_t row_start;
    uint64_t row_count;
};
static_assert(sizeof(ZoneEntry) == 24, "ZoneEntry must be 24 bytes");

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // Predicate constants (all in scaled integer form)
    const int32_t DATE_LO = 8766;   // 1994-01-01, inclusive
    const int32_t DATE_HI = 9131;   // 1995-01-01, exclusive
    const int64_t DISC_LO = 5;      // 0.05 * 100
    const int64_t DISC_HI = 7;      // 0.07 * 100
    const int64_t QTY_MAX = 2400;   // 24.00 * 100 (exclusive)

    // ----------------------------------------------------------------
    // Phase 1: Load columns via mmap (zero-copy, sequential prefetch)
    // ----------------------------------------------------------------
    gendb::MmapColumn<int32_t> col_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");
    gendb::MmapColumn<int64_t> col_discount(gendb_dir + "/lineitem/l_discount.bin");
    gendb::MmapColumn<int64_t> col_quantity(gendb_dir + "/lineitem/l_quantity.bin");
    gendb::MmapColumn<int64_t> col_extprice(gendb_dir + "/lineitem/l_extendedprice.bin");

    // Issue readahead for all columns concurrently (HDD latency hiding)
    mmap_prefetch_all(col_shipdate, col_discount, col_quantity, col_extprice);

    const size_t total_rows = col_shipdate.count;

    // ----------------------------------------------------------------
    // Phase 2: Zone map pruning → compute qualifying row range
    // ----------------------------------------------------------------
    // The shipdate column is sorted, so qualifying blocks form a
    // contiguous region. We compute the min row_start and max row_end.
    uint64_t scan_lo = 0;
    uint64_t scan_hi = total_rows;

    {
        GENDB_PHASE("zone_map_prune");
        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int fd = ::open(zm_path.c_str(), O_RDONLY);
        if (fd >= 0) {
            struct stat st;
            fstat(fd, &st);
            const uint8_t* raw = (const uint8_t*)mmap(
                nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
            ::close(fd);

            if (raw != MAP_FAILED && (size_t)st.st_size >= sizeof(uint32_t)) {
                uint32_t num_blocks = 0;
                memcpy(&num_blocks, raw, sizeof(uint32_t));

                const ZoneEntry* entries = (const ZoneEntry*)(raw + sizeof(uint32_t));
                size_t n_avail = ((size_t)st.st_size - sizeof(uint32_t)) / sizeof(ZoneEntry);
                uint32_t n = (uint32_t)std::min((size_t)num_blocks, n_avail);

                uint64_t q_lo = UINT64_MAX;
                uint64_t q_hi = 0;

                for (uint32_t i = 0; i < n; i++) {
                    // Skip block if its date range doesn't overlap [DATE_LO, DATE_HI)
                    if (entries[i].max_val < DATE_LO || entries[i].min_val >= DATE_HI)
                        continue;
                    uint64_t rs = entries[i].row_start;
                    uint64_t re = rs + entries[i].row_count;
                    if (rs < q_lo) q_lo = rs;
                    if (re > q_hi) q_hi = re;
                }

                if (q_lo != UINT64_MAX) {
                    scan_lo = q_lo;
                    scan_hi = std::min(q_hi, (uint64_t)total_rows);
                }
                munmap((void*)raw, st.st_size);
            }
        }
    }

    // ----------------------------------------------------------------
    // Phase 3: Parallel morsel scan over [scan_lo, scan_hi)
    // ----------------------------------------------------------------
    int64_t global_revenue = 0;

    {
        GENDB_PHASE("main_scan");

        const int32_t* __restrict__ shipdate = col_shipdate.data;
        const int64_t* __restrict__ discount  = col_discount.data;
        const int64_t* __restrict__ quantity  = col_quantity.data;
        const int64_t* __restrict__ extprice  = col_extprice.data;

        const int64_t n_rows = (int64_t)(scan_hi - scan_lo);

        // Morsel size: ~1M rows per chunk (balance parallelism vs scheduling overhead)
        // With 64 cores and ~9M rows, ~140K rows/thread for static scheduling
        int64_t lsum = 0;

        #pragma omp parallel for schedule(static) reduction(+:lsum)
        for (int64_t i = 0; i < n_rows; i++) {
            int64_t r = (int64_t)scan_lo + i;

            int32_t sd = shipdate[r];
            if (__builtin_expect(sd < DATE_LO || sd >= DATE_HI, 0)) continue;

            int64_t disc = discount[r];
            if (__builtin_expect(disc < DISC_LO || disc > DISC_HI, 0)) continue;

            int64_t qty = quantity[r];
            if (__builtin_expect(qty >= QTY_MAX, 0)) continue;

            lsum += extprice[r] * disc;
        }

        global_revenue = lsum;
    }

    // ----------------------------------------------------------------
    // Phase 4: Output results
    // ----------------------------------------------------------------
    {
        GENDB_PHASE("output");

        // global_revenue = sum(extprice * disc), scaled by 10000
        // Output as: integer_part.fractional_part (2 decimal places)
        // Use double for final division — only 1 value, no precision issue
        double revenue_d = (double)global_revenue / 10000.0;

        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) {
            fprintf(stderr, "Cannot open output file: %s\n", out_path.c_str());
            return;
        }
        fprintf(f, "revenue\n");
        fprintf(f, "%.2f\n", revenue_d);
        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q6(gendb_dir, results_dir);
    return 0;
}
#endif
