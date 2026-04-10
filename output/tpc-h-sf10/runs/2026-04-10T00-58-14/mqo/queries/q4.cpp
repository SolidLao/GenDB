// Q4 tail — Order Priority Checking
// Consumes: scan_lineitem_full (shared)
// Steps: (1) build semi-join bitset from lineitem, (2) zonemap-pruned orders scan
//        fused with bitset probe + direct-array aggregation, (3) output

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_full.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <omp.h>

namespace mqo::tails {

void run_Q4(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q4_tail");

    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------
    // o_orderdate >= DATE '1993-07-01' (epoch day 8582) AND < DATE '1993-10-01' (epoch day 8674)
    constexpr int32_t DATE_LO = 8582;
    constexpr int32_t DATE_HI = 8674;
    constexpr size_t MAX_ORDERKEY = 60000001;  // orderkeys are 1..60M for SF10
    constexpr size_t BITSET_WORDS = (MAX_ORDERKEY + 63) / 64;

    // -----------------------------------------------------------------------
    // Step 1: Build semi-join bitset from shared lineitem scan
    //         Set bit[l_orderkey] if l_commitdate < l_receiptdate
    // -----------------------------------------------------------------------
    // Allocate a single global bitset (7.5 MB — fits in L3)
    std::vector<uint64_t> bitset(BITSET_WORDS, 0);

    {
        MQO_TIME_PHASE("Q4_build_bitset");
        const auto& li = mqo::shared::scan_lineitem_full::get_columns(ctx);
        const size_t n = li.n_rows;
        const int32_t* __restrict__ commitdate  = li.l_commitdate;
        const int32_t* __restrict__ receiptdate = li.l_receiptdate;
        const int32_t* __restrict__ orderkey    = li.l_orderkey;

        // Parallel: thread-local bitsets, OR-merged
        const int nthreads = omp_get_max_threads();

        // For large thread counts, use thread-local bitsets and merge
        // Each bitset is ~7.5MB; with 64 threads that's 480MB — acceptable
        // But to save memory, use atomic OR on the global bitset instead
        // since contention is low (different orderkeys spread across words)

        #pragma omp parallel
        {
            // Thread-local bitset to avoid false sharing / atomic overhead
            std::vector<uint64_t> local_bs(BITSET_WORDS, 0);

            #pragma omp for schedule(static) nowait
            for (size_t i = 0; i < n; ++i) {
                if (commitdate[i] < receiptdate[i]) {
                    uint32_t key = static_cast<uint32_t>(orderkey[i]);
                    local_bs[key >> 6] |= (1ULL << (key & 63));
                }
            }

            // Merge into global bitset
            #pragma omp critical
            {
                for (size_t w = 0; w < BITSET_WORDS; ++w) {
                    bitset[w] |= local_bs[w];
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 2-4 (fused): Scan orders with zonemap pruning, date filter,
    //                    bitset semi-join probe, direct-array aggregation
    // -----------------------------------------------------------------------
    // Read orders dictionary for o_orderpriority
    const std::string orders_dir = ctx.gendb_dir + "/orders/";
    std::vector<std::string> prio_dict = mqo::io::read_dictionary(orders_dir + "o_orderpriority_dict.bin");

    // Global count array (5 priorities)
    const size_t NUM_PRIOS = prio_dict.size();
    std::vector<int64_t> counts(NUM_PRIOS, 0);

    {
        MQO_TIME_PHASE("Q4_orders_scan_agg");

        const size_t n_orders = mqo::io::read_row_count(orders_dir + "meta.txt");

        // mmap orders columns
        const int32_t* __restrict__ o_orderkey      = mqo::io::mmap_column<int32_t>(orders_dir + "o_orderkey.bin", n_orders);
        const int32_t* __restrict__ o_orderdate     = mqo::io::mmap_column<int32_t>(orders_dir + "o_orderdate.bin", n_orders);
        const uint8_t* __restrict__ o_orderpriority = mqo::io::mmap_column<uint8_t>(orders_dir + "o_orderpriority.bin", n_orders);

        // Read zonemap
        constexpr size_t BLOCK_SIZE = 65536;
        const size_t num_blocks = (n_orders + BLOCK_SIZE - 1) / BLOCK_SIZE;

        struct ZoneEntry { int32_t min_val; int32_t max_val; };
        const std::string zm_path = ctx.gendb_dir + "/indexes/orders_o_orderdate_zonemap.bin";
        const ZoneEntry* zonemap = mqo::io::mmap_column<ZoneEntry>(zm_path, num_blocks);

        // Parallel scan with thread-local count arrays
        #pragma omp parallel
        {
            int64_t local_counts[5] = {0, 0, 0, 0, 0};

            #pragma omp for schedule(dynamic, 1) nowait
            for (size_t blk = 0; blk < num_blocks; ++blk) {
                // Zonemap pruning: skip block if no overlap with [DATE_LO, DATE_HI)
                if (zonemap[blk].max_val < DATE_LO || zonemap[blk].min_val >= DATE_HI) {
                    continue;
                }

                const size_t row_start = blk * BLOCK_SIZE;
                const size_t row_end = std::min(row_start + BLOCK_SIZE, n_orders);

                for (size_t i = row_start; i < row_end; ++i) {
                    int32_t dt = o_orderdate[i];
                    if (dt >= DATE_LO && dt < DATE_HI) {
                        uint32_t key = static_cast<uint32_t>(o_orderkey[i]);
                        // Semi-join: check bitset
                        if (bitset[key >> 6] & (1ULL << (key & 63))) {
                            local_counts[o_orderpriority[i]]++;
                        }
                    }
                }
            }

            // Merge
            #pragma omp critical
            {
                for (size_t p = 0; p < NUM_PRIOS; ++p) {
                    counts[p] += local_counts[p];
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 5: Sort by o_orderpriority string and output
    // -----------------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q4_output");

        // Build (priority_string, count) pairs and sort
        std::vector<std::pair<std::string, int64_t>> results;
        results.reserve(NUM_PRIOS);
        for (size_t p = 0; p < NUM_PRIOS; ++p) {
            results.emplace_back(prio_dict[p], counts[p]);
        }
        std::sort(results.begin(), results.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });

        // Write CSV
        std::string out_path = ctx.output_dir + "/q4.csv";
        FILE* f = std::fopen(out_path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "[Q4] Cannot open output: %s\n", out_path.c_str());
            return;
        }
        std::fprintf(f, "o_orderpriority,order_count\n");
        for (const auto& [prio, cnt] : results) {
            std::fprintf(f, "%s,%ld\n", prio.c_str(), cnt);
        }
        std::fclose(f);
    }
}

}  // namespace mqo::tails
