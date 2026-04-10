// Q13 tail — Customer Distribution
// Consumes: scan_orders_full (o_custkey, o_comment), hash_customer_by_custkey (custkey range)
// Operators: parallel scan+filter+aggregate, merge, histogram, sort, output

#include "mqo_profile.hpp"
#include "../shared/scan_orders_full.hpp"
#include "../shared/hash_customer_by_custkey.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <omp.h>

namespace mqo { namespace tails {

void run_Q13(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q13_tail");

    // Fetch shared inputs
    const auto& orders   = mqo::shared::scan_orders_full::get();
    const auto& customer = mqo::shared::hash_customer_by_custkey::get();

    const size_t n_orders = orders.n_rows;
    const int32_t max_custkey = customer.max_key;  // 1500000
    const size_t arr_size = static_cast<size_t>(max_custkey) + 1;

    // Step 1: Parallel scan — filter o_comment NOT LIKE '%special%requests%',
    // increment thread-local count_array[o_custkey] for passing rows.
    const int n_threads = omp_get_max_threads();

    // Allocate thread-local count arrays (uint16_t, ~3MB each)
    std::vector<std::vector<uint16_t>> tl_counts(n_threads);

    {
        MQO_TIME_PHASE("Q13_scan_aggregate");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            tl_counts[tid].resize(arr_size, 0);
            uint16_t* my_counts = tl_counts[tid].data();

            const int32_t* custkeys = orders.o_custkey;
            const mqo::io::VarlenColumn& comment = orders.o_comment;

            #pragma omp for schedule(dynamic, 65536)
            for (size_t i = 0; i < n_orders; ++i) {
                // Check NOT LIKE '%special%requests%'
                // Match means: find "special" then "requests" after it
                uint32_t off_s = comment.offsets[i];
                uint32_t off_e = comment.offsets[i + 1];
                const char* p = comment.data + off_s;
                uint32_t len = off_e - off_s;

                // Find "special" (7 chars)
                bool matches_pattern = false;
                if (len >= 15) {  // minimum: "special" + "requests" = 15 chars
                    const char* end = p + len;
                    const char* found = static_cast<const char*>(
                        ::memmem(p, len, "special", 7));
                    if (found) {
                        const char* after = found + 7;
                        size_t remaining = static_cast<size_t>(end - after);
                        if (::memmem(after, remaining, "requests", 8) != nullptr) {
                            matches_pattern = true;
                        }
                    }
                }

                if (!matches_pattern) {
                    int32_t ck = custkeys[i];
                    my_counts[ck]++;
                }
            }
        }
    }

    // Step 2: Merge thread-local arrays into single global array
    std::vector<uint32_t> merged(arr_size, 0);
    {
        MQO_TIME_PHASE("Q13_merge");

        #pragma omp parallel for schedule(static)
        for (size_t k = 0; k < arr_size; ++k) {
            uint32_t sum = 0;
            for (int t = 0; t < n_threads; ++t) {
                sum += tl_counts[t][k];
            }
            merged[k] = sum;
        }

        // Free thread-local arrays
        for (auto& v : tl_counts) {
            std::vector<uint16_t>().swap(v);
        }
    }

    // Step 3: Histogram sweep — count distribution of c_count values
    // Use pk_index to identify valid custkeys
    std::vector<uint32_t> histogram(256, 0);  // generous upper bound
    {
        MQO_TIME_PHASE("Q13_histogram");

        const int32_t* pk_index = customer.pk_index;

        for (int32_t ck = 1; ck <= max_custkey; ++ck) {
            if (pk_index[ck] >= 0) {  // valid customer
                uint32_t c_count = merged[ck];
                if (c_count >= histogram.size()) {
                    histogram.resize(c_count + 1, 0);
                }
                histogram[c_count]++;
            }
        }
    }

    // Step 4: Materialize and sort
    std::vector<std::pair<int32_t, int64_t>> results;
    {
        MQO_TIME_PHASE("Q13_sort");

        for (size_t i = 0; i < histogram.size(); ++i) {
            if (histogram[i] > 0) {
                results.emplace_back(static_cast<int32_t>(i),
                                     static_cast<int64_t>(histogram[i]));
            }
        }

        std::sort(results.begin(), results.end(),
            [](const auto& a, const auto& b) {
                if (a.second != b.second) return a.second > b.second;
                return a.first > b.first;
            });
    }

    // Output
    {
        MQO_TIME_PHASE("Q13_output");

        std::string path = ctx.output_dir + "/q13.csv";
        FILE* f = std::fopen(path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", path.c_str());
            return;
        }
        std::fprintf(f, "c_count,custdist\n");
        for (const auto& [c_count, custdist] : results) {
            std::fprintf(f, "%d,%ld\n", c_count, custdist);
        }
        std::fclose(f);
    }
}

}}  // namespace mqo::tails
