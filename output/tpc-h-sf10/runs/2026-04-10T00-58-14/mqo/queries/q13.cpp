// Q13 tail — Customer Distribution
// Shared input: hash_customer_by_custkey (used only for customer count verification;
//   dense PK 1..1500000 means we iterate the range directly)
// Own work: scan orders(o_custkey, o_comment), filter NOT LIKE '%special%requests%',
//   count per custkey, distribution aggregation, sort, output.

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/hash_customer_by_custkey.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <omp.h>

namespace mqo::tails {

// Check if a string matches the pattern '%special%requests%'
// i.e., contains "special" followed (anywhere after) by "requests"
static inline bool matches_special_requests(const char* s, size_t len) {
    // Find "special" first
    static constexpr char needle1[] = "special";
    static constexpr size_t n1 = 7;
    static constexpr char needle2[] = "requests";
    static constexpr size_t n2 = 8;

    if (len < n1 + n2) return false;

    const char* end = s + len;
    const char* p = static_cast<const char*>(::memmem(s, len, needle1, n1));
    if (!p) return false;

    // Now find "requests" after "special"
    const char* after = p + n1;
    size_t remaining = static_cast<size_t>(end - after);
    if (remaining < n2) return false;

    return ::memmem(after, remaining, needle2, n2) != nullptr;
}

void run_Q13(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q13_tail");

    static constexpr int32_t MAX_CUSTKEY = 1500000;
    static constexpr int32_t DIST_SIZE = 64;  // max possible c_count value + margin

    // --- Step 1: Load orders columns ---
    size_t n_orders;
    const int32_t* o_custkey;
    mqo::io::VarlenAccessor va_comment;
    {
        MQO_TIME_PHASE("Q13_data_loading");
        const std::string base = ctx.gendb_dir + "/orders/";
        n_orders = mqo::io::read_row_count(base + "meta.txt");
        o_custkey = mqo::io::mmap_column<int32_t>(base + "o_custkey.bin", n_orders);
        va_comment = mqo::io::mmap_varlen(base, "o_comment");
    }

    // --- Steps 2-3: Parallel scan + filter + per-thread count aggregation ---
    const int n_threads = omp_get_max_threads();

    // Allocate thread-local count arrays
    // Each array: int32_t[MAX_CUSTKEY+1], indexed by custkey (1-based)
    std::vector<int32_t*> tl_counts(n_threads);
    for (int t = 0; t < n_threads; ++t) {
        tl_counts[t] = static_cast<int32_t*>(
            std::calloc(static_cast<size_t>(MAX_CUSTKEY) + 1, sizeof(int32_t)));
    }

    {
        MQO_TIME_PHASE("Q13_scan_filter_agg");
        #pragma omp parallel
        {
            const int tid = omp_get_thread_num();
            int32_t* my_counts = tl_counts[tid];

            #pragma omp for schedule(dynamic, 65536)
            for (size_t i = 0; i < n_orders; ++i) {
                // Check if comment matches '%special%requests%'
                std::string_view comment = va_comment.get(i);
                if (!matches_special_requests(comment.data(), comment.size())) {
                    // This order passes the NOT LIKE filter
                    my_counts[o_custkey[i]]++;
                }
            }
        }
    }

    // --- Step 4: Merge thread-local arrays ---
    // Use tl_counts[0] as the global result
    {
        MQO_TIME_PHASE("Q13_merge");
        #pragma omp parallel for schedule(static)
        for (int32_t k = 1; k <= MAX_CUSTKEY; ++k) {
            int32_t sum = tl_counts[0][k];
            for (int t = 1; t < n_threads; ++t) {
                sum += tl_counts[t][k];
            }
            tl_counts[0][k] = sum;
        }
    }
    int32_t* global_counts = tl_counts[0];

    // Free other thread-local arrays
    for (int t = 1; t < n_threads; ++t) {
        std::free(tl_counts[t]);
    }

    // --- Steps 5-6: Distribution aggregation ---
    // c_count -> custdist (how many customers have that order count)
    int64_t custdist[DIST_SIZE] = {};
    {
        MQO_TIME_PHASE("Q13_distribution");
        for (int32_t k = 1; k <= MAX_CUSTKEY; ++k) {
            int32_t c_count = global_counts[k];
            if (c_count < DIST_SIZE) {
                custdist[c_count]++;
            }
        }
    }

    // --- Step 7: Collect non-zero entries and sort ---
    struct Result {
        int32_t c_count;
        int64_t custdist;
    };
    std::vector<Result> results;
    results.reserve(DIST_SIZE);
    for (int32_t i = 0; i < DIST_SIZE; ++i) {
        if (custdist[i] > 0) {
            results.push_back({i, custdist[i]});
        }
    }

    std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        if (a.custdist != b.custdist) return a.custdist > b.custdist;
        return a.c_count > b.c_count;
    });

    // --- Output ---
    {
        MQO_TIME_PHASE("Q13_output");
        std::string out_path = ctx.output_dir + "/q13.csv";
        FILE* fp = std::fopen(out_path.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "[MQO] Cannot write: %s\n", out_path.c_str());
            std::exit(1);
        }
        std::fprintf(fp, "c_count,custdist\n");
        for (const auto& r : results) {
            std::fprintf(fp, "%d,%ld\n", r.c_count, r.custdist);
        }
        std::fclose(fp);
    }

    std::free(global_counts);
}

}  // namespace mqo::tails
