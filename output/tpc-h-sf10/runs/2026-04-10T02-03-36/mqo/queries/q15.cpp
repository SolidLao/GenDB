// Q15 tail — Top Supplier
// Consumes: scan_lineitem_full, hash_supplier_by_suppkey
// Operators: residual_filter → parallel_direct_array_agg → max+filter → hash_probe → sort → project

#include "mqo_profile.hpp"
#include "../shared/scan_lineitem_full.hpp"
#include "../shared/hash_supplier_by_suppkey.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace mqo { namespace tails {

void run_Q15(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q15_tail");

    // --- Fetch shared inputs ---
    const auto& li  = mqo::shared::scan_lineitem_full::get();
    const auto& sup = mqo::shared::hash_supplier_by_suppkey::get();

    constexpr int32_t DATE_LO = 9497;   // 1996-01-01
    constexpr int32_t DATE_HI = 9588;   // 1996-04-01
    constexpr int ARRAY_SIZE = 100001;   // suppkey range [0, 100000]

    // --- Step 1+2: Parallel filter + aggregate into direct arrays ---
    // Each thread gets a local double array indexed by l_suppkey.
    const size_t n_rows = li.n_rows;
    const int32_t* shipdate      = li.l_shipdate;
    const int32_t* suppkey       = li.l_suppkey;
    const double*  extendedprice = li.l_extendedprice;
    const double*  discount      = li.l_discount;

    // Determine thread count (bounded to avoid over-subscription in MQO)
    int n_threads = 1;
#ifdef _OPENMP
    n_threads = omp_get_max_threads();
    if (n_threads > 16) n_threads = 16;  // bound for MQO co-execution
#endif

    // Allocate per-thread arrays (each ~800 KB — fits comfortably in cache)
    std::vector<std::vector<double>> thr_rev(n_threads, std::vector<double>(ARRAY_SIZE, 0.0));

    {
        MQO_TIME_PHASE("Q15_filter_agg");
#ifdef _OPENMP
        #pragma omp parallel num_threads(n_threads)
#endif
        {
            int tid = 0;
#ifdef _OPENMP
            tid = omp_get_thread_num();
#endif
            double* local = thr_rev[tid].data();

#ifdef _OPENMP
            #pragma omp for schedule(static)
#endif
            for (size_t i = 0; i < n_rows; ++i) {
                int32_t sd = shipdate[i];
                if (sd >= DATE_LO && sd < DATE_HI) {
                    int32_t sk = suppkey[i];
                    local[sk] += extendedprice[i] * (1.0 - discount[i]);
                }
            }
        }
    }

    // --- Merge per-thread arrays ---
    std::vector<double> revenue(ARRAY_SIZE, 0.0);
    {
        MQO_TIME_PHASE("Q15_merge");
        double* dst = revenue.data();
        for (int t = 0; t < n_threads; ++t) {
            const double* src = thr_rev[t].data();
            for (int i = 0; i < ARRAY_SIZE; ++i) {
                dst[i] += src[i];
            }
        }
        // Free thread-local arrays
        thr_rev.clear();
        thr_rev.shrink_to_fit();
    }

    // --- Steps 3+4: Find max revenue and matching suppkeys (fused) ---
    double max_rev = 0.0;
    std::vector<int32_t> matching_suppkeys;
    {
        MQO_TIME_PHASE("Q15_max_filter");
        for (int i = 1; i < ARRAY_SIZE; ++i) {
            double r = revenue[i];
            if (r > max_rev) {
                max_rev = r;
                matching_suppkeys.clear();
                matching_suppkeys.push_back(i);
            } else if (r == max_rev && r > 0.0) {
                matching_suppkeys.push_back(i);
            }
        }
    }

    // --- Step 5: Probe shared supplier hash for matching suppkeys ---
    // Result struct
    struct Result {
        int32_t s_suppkey;
        std::string_view s_name;
        std::string_view s_address;
        std::string_view s_phone;
        double total_revenue;
    };
    std::vector<Result> results;
    results.reserve(matching_suppkeys.size());

    {
        MQO_TIME_PHASE("Q15_probe");
        for (int32_t sk : matching_suppkeys) {
            if (sk <= sup.max_key) {
                int32_t row_id = sup.pk_index[sk];
                if (row_id >= 0) {
                    results.push_back({
                        sk,
                        sup.s_name.get(row_id),
                        sup.s_address.get(row_id),
                        sup.s_phone.get(row_id),
                        max_rev
                    });
                }
            }
        }
    }

    // --- Step 6: Sort by s_suppkey ASC (typically 1 row, effectively no-op) ---
    std::sort(results.begin(), results.end(),
              [](const Result& a, const Result& b) { return a.s_suppkey < b.s_suppkey; });

    // --- Step 7: Output ---
    {
        MQO_TIME_PHASE("Q15_output");
        std::string outpath = ctx.output_dir + "/q15.csv";
        FILE* f = std::fopen(outpath.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", outpath.c_str());
            return;
        }
        std::fprintf(f, "s_suppkey,s_name,s_address,s_phone,total_revenue\n");
        for (const auto& r : results) {
            std::fprintf(f, "%d,%.*s,%.*s,%.*s,%.2f\n",
                         r.s_suppkey,
                         (int)r.s_name.size(), r.s_name.data(),
                         (int)r.s_address.size(), r.s_address.data(),
                         (int)r.s_phone.size(), r.s_phone.data(),
                         r.total_revenue);
        }
        std::fclose(f);
    }
}

}}  // namespace mqo::tails
