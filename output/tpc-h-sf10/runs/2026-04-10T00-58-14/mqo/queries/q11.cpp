// Q11 tail — Important Stock Identification
// Shared input: hash_supplier_by_suppkey (iterate to find German suppliers)
// Own work: scan nation → build German suppkey bitset → scan partsupp (8M rows)
//           → fused per-partkey agg + grand total → HAVING filter → sort DESC

#include "mqo_profile.hpp"
#include "../shared/mqo_io.hpp"
#include "../shared/hash_supplier_by_suppkey.hpp"

#include <algorithm>
#include <bitset>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <omp.h>

namespace mqo::tails {

void run_Q11(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q11_tail");

    // ---------------------------------------------------------------
    // Step 1: Find GERMANY nationkey from nation table
    // ---------------------------------------------------------------
    int32_t germany_nk = -1;
    {
        MQO_TIME_PHASE("Q11_nation_filter");
        const std::string nb = ctx.gendb_dir + "/nation/";
        const size_t n_nation = mqo::io::read_row_count(nb + "meta.txt");
        // n_name is dict-encoded as uint8; GERMANY = dict code 7
        const uint8_t* n_name_codes = mqo::io::mmap_column<uint8_t>(nb + "n_name.bin", n_nation);
        const int32_t* n_nationkey  = mqo::io::mmap_column<int32_t>(nb + "n_nationkey.bin", n_nation);

        // Read dictionary to find GERMANY code (don't hardcode in case order differs)
        auto dict = mqo::io::read_dictionary(nb + "n_name_dict.bin");
        uint8_t germany_code = 255;
        for (uint32_t i = 0; i < dict.size(); ++i) {
            if (dict[i] == "GERMANY") { germany_code = static_cast<uint8_t>(i); break; }
        }

        for (size_t i = 0; i < n_nation; ++i) {
            if (n_name_codes[i] == germany_code) {
                germany_nk = n_nationkey[i];
                break;
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 2: Build German supplier bitset from shared hash
    // ---------------------------------------------------------------
    static constexpr int32_t MAX_SUPPKEY = 100000;
    // Use a compact bitset — 12.5KB fits in L1
    uint64_t suppkey_bits[(MAX_SUPPKEY + 64) / 64] = {};
    {
        MQO_TIME_PHASE("Q11_build_suppkey_bitset");
        const auto& supp = mqo::shared::hash_supplier_by_suppkey::get();
        for (int32_t k = 1; k <= MAX_SUPPKEY; ++k) {
            const auto& e = supp.entries[k];
            if (e.s_suppkey != 0 && e.s_nationkey == germany_nk) {
                suppkey_bits[k >> 6] |= (1ULL << (k & 63));
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 3: Parallel scan of partsupp — fused join + aggregation
    //         Direct array indexed by ps_partkey (max 2M for SF10)
    //         Each thread has its own 16MB array + local total
    // ---------------------------------------------------------------
    static constexpr int32_t MAX_PARTKEY = 2000000;
    static constexpr int NUM_THREADS = 16;

    // Thread-local aggregation arrays
    std::vector<double*> tl_agg(NUM_THREADS, nullptr);
    std::vector<double>  tl_total(NUM_THREADS, 0.0);
    for (int t = 0; t < NUM_THREADS; ++t) {
        tl_agg[t] = static_cast<double*>(std::calloc(static_cast<size_t>(MAX_PARTKEY) + 1, sizeof(double)));
    }

    {
        MQO_TIME_PHASE("Q11_partsupp_scan");
        const std::string pb = ctx.gendb_dir + "/partsupp/";
        const size_t n_ps = mqo::io::read_row_count(pb + "meta.txt");
        const int32_t* ps_partkey    = mqo::io::mmap_column<int32_t>(pb + "ps_partkey.bin", n_ps);
        const int32_t* ps_suppkey    = mqo::io::mmap_column<int32_t>(pb + "ps_suppkey.bin", n_ps);
        const double*  ps_supplycost = mqo::io::mmap_column<double> (pb + "ps_supplycost.bin", n_ps);
        const int32_t* ps_availqty   = mqo::io::mmap_column<int32_t>(pb + "ps_availqty.bin", n_ps);

        #pragma omp parallel num_threads(NUM_THREADS)
        {
            int tid = omp_get_thread_num();
            double* my_agg = tl_agg[tid];
            double  my_total = 0.0;

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_ps; ++i) {
                int32_t sk = ps_suppkey[i];
                // Bitset probe
                if (suppkey_bits[sk >> 6] & (1ULL << (sk & 63))) {
                    double val = ps_supplycost[i] * static_cast<double>(ps_availqty[i]);
                    my_agg[ps_partkey[i]] += val;
                    my_total += val;
                }
            }

            tl_total[tid] = my_total;
        }
    }

    // ---------------------------------------------------------------
    // Merge thread-local arrays
    // ---------------------------------------------------------------
    double grand_total = 0.0;
    {
        MQO_TIME_PHASE("Q11_merge");
        for (int t = 0; t < NUM_THREADS; ++t)
            grand_total += tl_total[t];

        // Merge into tl_agg[0]
        double* merged = tl_agg[0];
        for (int t = 1; t < NUM_THREADS; ++t) {
            const double* src = tl_agg[t];
            for (int32_t pk = 1; pk <= MAX_PARTKEY; ++pk) {
                merged[pk] += src[pk];
            }
            std::free(tl_agg[t]);
            tl_agg[t] = nullptr;
        }
    }

    // ---------------------------------------------------------------
    // Step 4: HAVING filter
    // ---------------------------------------------------------------
    double threshold = grand_total * 0.0001000000;
    struct Result { int32_t partkey; double value; };
    std::vector<Result> results;
    {
        MQO_TIME_PHASE("Q11_having");
        const double* merged = tl_agg[0];
        results.reserve(2000);
        for (int32_t pk = 1; pk <= MAX_PARTKEY; ++pk) {
            double v = merged[pk];
            if (v > threshold) {
                results.push_back({pk, v});
            }
        }
    }
    std::free(tl_agg[0]);

    // ---------------------------------------------------------------
    // Step 5: Sort by value DESC
    // ---------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q11_sort");
        std::sort(results.begin(), results.end(),
                  [](const Result& a, const Result& b) { return a.value > b.value; });
    }

    // ---------------------------------------------------------------
    // Step 6: Output CSV
    // ---------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q11_output");
        std::string outpath = ctx.output_dir + "/q11.csv";
        FILE* f = std::fopen(outpath.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "[MQO] Cannot open output: %s\n", outpath.c_str());
            return;
        }
        std::fprintf(f, "ps_partkey,value\n");
        for (const auto& r : results) {
            std::fprintf(f, "%d,%.2f\n", r.partkey, r.value);
        }
        std::fclose(f);
    }
}

}  // namespace mqo::tails
