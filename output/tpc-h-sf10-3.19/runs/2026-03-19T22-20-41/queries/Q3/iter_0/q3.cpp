#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <thread>
#include <unordered_map>
#include <cmath>
#include <omp.h>

#include "timing_utils.h"
#include "date_utils.h"
#include "cli_params.h"
#include "mmap_utils.h"

// Per-thread aggregation map entry
struct AggEntry {
    int32_t key;
    double  revenue;
};

int main(int argc, char* argv[]) {
    gendb::init_date_tables();

    std::string gendb_dir  = argv[1];
    std::string results_dir = argv[2];

    // Parse parameters
    int32_t o_orderdate_upper = gendb::parse_date_arg(argc, argv, "--o_orderdate_upper", 9204);
    int32_t l_shipdate_lower  = gendb::parse_date_arg(argc, argv, "--l_shipdate_lower", 9204);
    int64_t limit_val         = gendb::parse_int_arg(argc, argv, "--limit", 10);
    std::string c_mktsegment_eq = gendb::parse_string_arg(argc, argv, "--c_mktsegment_eq", "BUILDING");

    GENDB_PHASE("total");

    // =========================================================================
    // Data Loading
    // =========================================================================
    gendb::MmapColumn<int8_t>  c_mktsegment;
    gendb::MmapColumn<int32_t> c_custkey;
    gendb::MmapColumn<int32_t> o_custkey;
    gendb::MmapColumn<int32_t> o_orderkey;
    gendb::MmapColumn<int32_t> o_orderdate;
    gendb::MmapColumn<int32_t> o_shippriority;
    gendb::MmapColumn<int32_t> l_orderkey;
    gendb::MmapColumn<int32_t> l_shipdate;
    gendb::MmapColumn<double>  l_extendedprice;
    gendb::MmapColumn<double>  l_discount;

    {
        GENDB_PHASE("data_loading");
        c_mktsegment.open(gendb_dir + "/customer/c_mktsegment.bin");
        c_custkey.open(gendb_dir + "/customer/c_custkey.bin");
        o_custkey.open(gendb_dir + "/orders/o_custkey.bin");
        o_orderkey.open(gendb_dir + "/orders/o_orderkey.bin");
        o_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");
        o_shippriority.open(gendb_dir + "/orders/o_shippriority.bin");

        // Prefetch lineitem columns (will be accessed via index later)
        l_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
        l_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
        l_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
        l_discount.open(gendb_dir + "/lineitem/l_discount.bin");

        // Advise random for lineitem (index-based access)
        l_shipdate.advise_random();
        l_extendedprice.advise_random();
        l_discount.advise_random();
    }

    const size_t num_customers = c_mktsegment.count;
    const size_t num_orders    = o_orderkey.count;

    // =========================================================================
    // Step 0: Build customer bitmap
    // =========================================================================
    // Find dictionary code for the target segment
    int8_t building_code = -1;
    {
        GENDB_PHASE("build_customer_bitmap");
        std::ifstream df(gendb_dir + "/customer/c_mktsegment_dict.bin", std::ios::binary);
        uint32_t dict_count;
        df.read((char*)&dict_count, 4);
        for (uint32_t d = 0; d < dict_count; d++) {
            uint16_t len;
            df.read((char*)&len, 2);
            std::string val(len, '\0');
            df.read(val.data(), len);
            if (val == c_mktsegment_eq) building_code = (int8_t)d;
        }
    }

    // Find max custkey for bitmap sizing
    // customer keys are 1-based and dense up to ~1.5M
    // Use a bitmap of size max_custkey+1
    int32_t max_custkey = 0;
    for (size_t i = 0; i < num_customers; i++) {
        if (c_custkey[i] > max_custkey) max_custkey = c_custkey[i];
    }

    // Bitmap: 1 bit per custkey
    size_t bitmap_size = (max_custkey + 64) / 64;  // in uint64_t words
    std::vector<uint64_t> cust_bitmap(bitmap_size, 0);

    if (building_code >= 0) {
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < num_customers; i++) {
            if (c_mktsegment[i] == building_code) {
                int32_t ck = c_custkey[i];
                uint64_t word_idx = ck >> 6;
                uint64_t bit_idx  = ck & 63;
                #pragma omp atomic
                cust_bitmap[word_idx] |= (1ULL << bit_idx);
            }
        }
    }

    // =========================================================================
    // Step 1: Scan orders with filter
    // =========================================================================
    // Load lineitem index for max orderkey sizing
    gendb::MmapColumn<uint64_t> li_index_raw;
    li_index_raw.open(gendb_dir + "/indexes/lineitem_l_orderkey_grouped.bin");
    // Header: uint64_t num_entries
    uint64_t li_index_num_entries = li_index_raw.data[0];
    const uint32_t* li_index = reinterpret_cast<const uint32_t*>(li_index_raw.data + 1);

    // order_metadata: direct arrays for o_orderdate and o_shippriority indexed by orderkey
    int32_t max_orderkey = 0;
    for (size_t i = 0; i < num_orders; i++) {
        if (o_orderkey[i] > max_orderkey) max_orderkey = o_orderkey[i];
    }

    // Allocate order metadata arrays
    std::vector<int32_t> om_orderdate(max_orderkey + 1, 0);
    std::vector<int32_t> om_shippriority(max_orderkey + 1, 0);

    // Collect qualifying orderkeys
    int nthreads = std::thread::hardware_concurrency();
    if (nthreads <= 0) nthreads = 1;
    if (nthreads > 64) nthreads = 64;

    std::vector<std::vector<int32_t>> thread_orderkeys(nthreads);

    {
        GENDB_PHASE("scan_orders");
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = 0;
            #ifdef _OPENMP
            tid = omp_get_thread_num();
            #endif
            auto& local_keys = thread_orderkeys[tid];
            local_keys.reserve(num_orders / nthreads / 3);

            #pragma omp for schedule(static)
            for (size_t i = 0; i < num_orders; i++) {
                int32_t odate = o_orderdate[i];
                if (odate >= o_orderdate_upper) continue;  // o_orderdate < upper

                int32_t ck = o_custkey[i];
                uint64_t word_idx = ck >> 6;
                uint64_t bit_idx  = ck & 63;
                if (!(cust_bitmap[word_idx] & (1ULL << bit_idx))) continue;

                int32_t ok = o_orderkey[i];
                local_keys.push_back(ok);
                om_orderdate[ok] = odate;
                om_shippriority[ok] = o_shippriority[i];
            }
        }
    }

    // Merge qualifying orderkeys
    size_t total_qualifying = 0;
    for (int t = 0; t < nthreads; t++) total_qualifying += thread_orderkeys[t].size();

    std::vector<int32_t> qualifying_orderkeys;
    qualifying_orderkeys.reserve(total_qualifying);
    for (int t = 0; t < nthreads; t++) {
        qualifying_orderkeys.insert(qualifying_orderkeys.end(),
            thread_orderkeys[t].begin(), thread_orderkeys[t].end());
        thread_orderkeys[t].clear();
        thread_orderkeys[t].shrink_to_fit();
    }

    // =========================================================================
    // Step 2: Probe lineitem via index and aggregate
    // =========================================================================
    // Per-thread hash maps for aggregation
    // Use a flat array for aggregation since orderkeys are dense
    // This avoids hash map overhead and ensures deterministic accumulation
    std::vector<double> revenue_by_orderkey(max_orderkey + 1, 0.0);
    // Track which orderkeys have revenue
    std::vector<uint8_t> has_revenue(max_orderkey + 1, 0);

    {
        GENDB_PHASE("main_scan");

        const int32_t* shipdate_ptr = l_shipdate.data;
        const double*  extprice_ptr = l_extendedprice.data;
        const double*  discount_ptr = l_discount.data;

        #pragma omp parallel num_threads(nthreads)
        {
            #pragma omp for schedule(dynamic, 4096)
            for (size_t qi = 0; qi < total_qualifying; qi++) {
                int32_t ok = qualifying_orderkeys[qi];
                if ((uint64_t)ok >= li_index_num_entries) continue;

                uint32_t start = li_index[ok * 2];
                uint32_t count = li_index[ok * 2 + 1];
                if (count == 0) continue;

                long double local_sum = 0.0L;
                bool any = false;
                for (uint32_t j = 0; j < count; j++) {
                    uint32_t row = start + j;
                    if (shipdate_ptr[row] <= l_shipdate_lower) continue;
                    local_sum += (long double)extprice_ptr[row] * (1.0L - (long double)discount_ptr[row]);
                    any = true;
                }
                if (any) {
                    revenue_by_orderkey[ok] = (double)local_sum;
                    has_revenue[ok] = 1;
                }
            }
        }
    }

    // =========================================================================
    // Step 3: Merge and Top-K
    // =========================================================================
    struct ResultEntry {
        int32_t orderkey;
        double  revenue;
        int32_t orderdate;
        int32_t shippriority;
    };

    std::vector<ResultEntry> results;
    {
        GENDB_PHASE("aggregation");

        // Collect all orderkeys with revenue into results
        for (size_t qi = 0; qi < total_qualifying; qi++) {
            int32_t ok = qualifying_orderkeys[qi];
            if (has_revenue[ok]) {
                results.push_back({ok, revenue_by_orderkey[ok], om_orderdate[ok], om_shippriority[ok]});
                has_revenue[ok] = 0; // avoid duplicates
            }
        }
    }

    // Top-K
    size_t k = std::min((size_t)limit_val, results.size());
    if (k > 0) {
        std::partial_sort(results.begin(), results.begin() + k, results.end(),
            [](const ResultEntry& a, const ResultEntry& b) {
                if (a.revenue != b.revenue) return a.revenue > b.revenue;
                return a.orderdate < b.orderdate;
            });
    }

    // =========================================================================
    // Output
    // =========================================================================
    {
        GENDB_PHASE("output");
        std::string outpath = results_dir + "/Q3.csv";
        FILE* fp = fopen(outpath.c_str(), "w");
        fprintf(fp, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char datebuf[11];
        for (size_t i = 0; i < k; i++) {
            gendb::epoch_days_to_date_str(results[i].orderdate, datebuf);
            double rev_rounded = std::round(results[i].revenue * 100.0) / 100.0;
            fprintf(fp, "%d,%.2f,%s,%d\n",
                results[i].orderkey,
                rev_rounded,
                datebuf,
                results[i].shippriority);
        }
        fclose(fp);
    }

    return 0;
}
