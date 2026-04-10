// Q4 tail — Order Priority Checking
// Shared input: scan_orders_full (materialized orders columns)
// Independent: lineitem scan (l_orderkey, l_commitdate, l_receiptdate)
// Strategy: bitset semi-join with dual bitsets (qualifying + found)

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_orders_full.hpp"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#ifdef _OPENMP
#include <omp.h>
#endif

namespace mqo { namespace tails {

void run_Q4(const Context& ctx) {
    MQO_TIME_TAIL("Q4_tail");

    // Date filter constants: 1993-07-01 to 1993-10-01 (days since epoch)
    constexpr int32_t DATE_LO = 8582;  // 1993-07-01
    constexpr int32_t DATE_HI = 8674;  // 1993-10-01

    // Dictionary: code -> string (from o_orderpriority_dict.bin)
    // code 0 = "5-LOW", 1 = "1-URGENT", 2 = "4-NOT SPECIFIED", 3 = "2-HIGH", 4 = "3-MEDIUM"
    // Sorted alphabetically: 1-URGENT(1), 2-HIGH(3), 3-MEDIUM(4), 4-NOT SPECIFIED(2), 5-LOW(0)
    static const char* dict_strings[5] = {
        "5-LOW", "1-URGENT", "4-NOT SPECIFIED", "2-HIGH", "3-MEDIUM"
    };
    // Sort order for output (alphabetical by priority string)
    static const int sorted_codes[5] = {1, 3, 4, 2, 0};

    // ---- Step 1: Get shared orders scan, apply date filter ----
    const auto& orders = mqo::shared::scan_orders_full::get();
    const size_t n_orders = orders.n_rows;

    // Find max orderkey for bitset sizing
    // Orders are ordered by o_orderkey, so max is last element
    const int32_t max_ok = orders.o_orderkey[n_orders - 1];
    const size_t bitset_words = (static_cast<size_t>(max_ok) + 64) / 64;

    // Allocate bitsets and priority array
    std::vector<uint64_t> qualifying(bitset_words, 0);
    std::vector<uint64_t> found(bitset_words, 0);
    std::vector<uint8_t> priority_by_ok(static_cast<size_t>(max_ok) + 1, 0);

    {
        MQO_TIME_PHASE("Q4_orders_filter");
        // Scan orders, filter by date, populate qualifying bitset + priority array
        for (size_t i = 0; i < n_orders; ++i) {
            int32_t d = orders.o_orderdate[i];
            if (d >= DATE_LO && d < DATE_HI) {
                int32_t ok = orders.o_orderkey[i];
                qualifying[static_cast<size_t>(ok) >> 6] |=
                    (uint64_t(1) << (ok & 63));
                priority_by_ok[static_cast<size_t>(ok)] = orders.o_orderpriority[i];
            }
        }
    }

    // ---- Step 2+3: Independent lineitem scan + semi-join probe ----
    {
        MQO_TIME_PHASE("Q4_lineitem_probe");
        std::string li_dir = ctx.gendb_dir + "/lineitem";
        size_t n_li = mqo::io::read_row_count(li_dir);

        const int32_t* l_orderkey   = mqo::io::mmap_column<int32_t>(li_dir + "/l_orderkey.bin", n_li);
        const int32_t* l_commitdate = mqo::io::mmap_column<int32_t>(li_dir + "/l_commitdate.bin", n_li);
        const int32_t* l_receiptdate= mqo::io::mmap_column<int32_t>(li_dir + "/l_receiptdate.bin", n_li);

        // Parallel scan with thread-local found bitsets
        const int n_threads = 16; // bounded to avoid oversubscription in batch mode

        // Thread-local found bitsets
        std::vector<std::vector<uint64_t>> tl_found(n_threads, std::vector<uint64_t>(bitset_words, 0));

        #pragma omp parallel num_threads(n_threads)
        {
            int tid = 0;
            #ifdef _OPENMP
            tid = omp_get_thread_num();
            #endif
            auto& my_found = tl_found[tid];

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_li; ++i) {
                int32_t ok = l_orderkey[i];
                size_t word = static_cast<size_t>(ok) >> 6;
                uint64_t bit = uint64_t(1) << (ok & 63);
                // Check qualifying first (cheap bitset test), then date predicate
                if ((qualifying[word] & bit) && l_commitdate[i] < l_receiptdate[i]) {
                    my_found[word] |= bit;
                }
            }
        }

        // Merge thread-local found bitsets
        for (int t = 0; t < n_threads; ++t) {
            for (size_t w = 0; w < bitset_words; ++w) {
                found[w] |= tl_found[t][w];
            }
        }
    }

    // ---- Step 4: Aggregate — iterate qualifying, check found, count by priority ----
    int64_t counts[5] = {0, 0, 0, 0, 0};
    {
        MQO_TIME_PHASE("Q4_aggregate");
        for (size_t w = 0; w < bitset_words; ++w) {
            uint64_t q = qualifying[w] & found[w];
            while (q) {
                int bit = __builtin_ctzll(q);
                int32_t ok = static_cast<int32_t>((w << 6) | bit);
                counts[priority_by_ok[static_cast<size_t>(ok)]]++;
                q &= q - 1; // clear lowest set bit
            }
        }
    }

    // ---- Step 5: Output sorted by o_orderpriority ASC (alphabetical) ----
    {
        MQO_TIME_PHASE("Q4_output");
        std::string outpath = ctx.output_dir + "/q4.csv";
        FILE* fp = std::fopen(outpath.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", outpath.c_str());
            return;
        }
        std::fprintf(fp, "o_orderpriority,order_count\n");
        for (int i = 0; i < 5; ++i) {
            int code = sorted_codes[i];
            std::fprintf(fp, "%s,%ld\n", dict_strings[code], counts[code]);
        }
        std::fclose(fp);
    }
}

}} // namespace mqo::tails
