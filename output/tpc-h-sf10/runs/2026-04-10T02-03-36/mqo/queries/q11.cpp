// Q11 tail — Important Stock Identification
// Shared inputs: scan_partsupp_full, hash_supplier_by_suppkey
// Operators: nation scan/filter → supplier bitset build → probe+aggregate → HAVING → sort → output

#include "mqo_profile.hpp"
#include "../shared/mqo_io.hpp"
#include "../shared/scan_partsupp_full.hpp"
#include "../shared/hash_supplier_by_suppkey.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace mqo { namespace tails {

void run_Q11(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q11_tail");

    // ---------------------------------------------------------------
    // Step 1: Find GERMANY nationkey from nation table (25 rows)
    // ---------------------------------------------------------------
    int32_t germany_nkey = -1;
    {
        MQO_TIME_PHASE("Q11_nation_filter");
        std::string ndir = ctx.gendb_dir + "/nation";
        size_t n_nations = mqo::io::read_row_count(ndir);
        const int32_t* n_nationkey = mqo::io::mmap_column<int32_t>(ndir + "/n_nationkey.bin", n_nations);

        // n_name is dict-encoded uint8_t
        mqo::io::Dictionary n_name_dict;
        n_name_dict.load(ndir + "/n_name_dict.bin");
        // Find the dict code for GERMANY
        uint8_t germany_code = 255;
        for (uint32_t i = 0; i < n_name_dict.count; ++i) {
            if (n_name_dict.entries[i] == "GERMANY") {
                germany_code = static_cast<uint8_t>(i);
                break;
            }
        }
        const uint8_t* n_name_codes = mqo::io::mmap_column<uint8_t>(ndir + "/n_name.bin", n_nations);
        for (size_t i = 0; i < n_nations; ++i) {
            if (n_name_codes[i] == germany_code) {
                germany_nkey = n_nationkey[i];
                break;
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 2: Build bitset of German suppliers from shared hash
    // ---------------------------------------------------------------
    // Bitset indexed by s_suppkey (dense PK, max 100000)
    static constexpr int SUPPLIER_MAX = 100001;
    std::vector<bool> german_supp(SUPPLIER_MAX, false);
    {
        MQO_TIME_PHASE("Q11_supplier_filter");
        const auto& sup = mqo::shared::hash_supplier_by_suppkey::get();
        for (size_t i = 0; i < sup.n_rows; ++i) {
            if (sup.s_nationkey[i] == germany_nkey) {
                german_supp[sup.s_suppkey[i]] = true;
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 3: Single-pass probe + aggregate over partsupp
    // ---------------------------------------------------------------
    // Direct array indexed by ps_partkey (dense, max ~2M at SF10)
    static constexpr int PARTKEY_MAX = 2000001;
    // Use a flat double array for per-partkey aggregation
    std::vector<double> agg(PARTKEY_MAX, 0.0);
    double global_sum = 0.0;
    {
        MQO_TIME_PHASE("Q11_main_scan");
        const auto& ps = mqo::shared::scan_partsupp_full::get();
        const size_t n = ps.n_rows;
        const int32_t* pk = ps.ps_partkey;
        const int32_t* sk = ps.ps_suppkey;
        const int32_t* aq = ps.ps_availqty;
        const double*  sc = ps.ps_supplycost;

        // Use a simple uint64_t bitset for faster probing than vector<bool>
        // 100001 bits = 1563 uint64_t words (~12KB, fits in L1)
        static constexpr int BWORDS = (SUPPLIER_MAX + 63) / 64;
        uint64_t bitmask[BWORDS];
        std::memset(bitmask, 0, sizeof(bitmask));
        for (int i = 0; i < SUPPLIER_MAX; ++i) {
            if (german_supp[i]) {
                bitmask[i >> 6] |= (1ULL << (i & 63));
            }
        }

        for (size_t i = 0; i < n; ++i) {
            int32_t s = sk[i];
            if (bitmask[s >> 6] & (1ULL << (s & 63))) {
                double val = sc[i] * static_cast<double>(aq[i]);
                agg[pk[i]] += val;
                global_sum += val;
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 4: HAVING filter + Step 5: Sort
    // ---------------------------------------------------------------
    double threshold = global_sum * 0.0001;
    struct Result {
        int32_t partkey;
        double value;
    };
    std::vector<Result> results;
    {
        MQO_TIME_PHASE("Q11_having_sort");
        results.reserve(2000);
        for (int i = 1; i < PARTKEY_MAX; ++i) {
            if (agg[i] > threshold) {
                results.push_back({i, agg[i]});
            }
        }
        std::sort(results.begin(), results.end(),
                  [](const Result& a, const Result& b) { return a.value > b.value; });
    }

    // ---------------------------------------------------------------
    // Step 6: Output
    // ---------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q11_output");
        std::string path = ctx.output_dir + "/q11.csv";
        FILE* f = std::fopen(path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", path.c_str());
            return;
        }
        std::fprintf(f, "ps_partkey,value\n");
        for (const auto& r : results) {
            std::fprintf(f, "%d,%.2f\n", r.partkey, r.value);
        }
        std::fclose(f);
    }
}

}} // namespace mqo::tails
