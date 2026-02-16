#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <omp.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <chrono>

// ============================================================================
// MMAP UTILITY
// ============================================================================

template <typename T>
struct MmapArray {
    T* data;
    size_t count;
    int fd;

    MmapArray() : data(nullptr), count(0), fd(-1) {}

    ~MmapArray() {
        if (data && data != MAP_FAILED) {
            munmap(data, count * sizeof(T));
        }
        if (fd >= 0) close(fd);
    }

    void load(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            exit(1);
        }
        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Failed to stat " << path << std::endl;
            exit(1);
        }
        count = st.st_size / sizeof(T);
        data = (T*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            exit(1);
        }
    }

    T& operator[](size_t i) { return data[i]; }
    const T& operator[](size_t i) const { return data[i]; }
    size_t size() const { return count; }
};

// ============================================================================
// MAIN QUERY Q5
// ============================================================================

void run_q5(const std::string& gendb_dir, const std::string& results_dir) {
    auto total_start = std::chrono::high_resolution_clock::now();

    printf("[METADATA CHECK] Q5 Storage Guide\n");
    printf("  - Date: epoch days (int32_t), values >3000\n");
    printf("  - Decimals: scaled int64_t, scale_factor=100\n");
    printf("  - Epoch bounds: [8766, 9131) for 1994-01-01 to 1995-01-01\n");
    printf("\n");

    // ========================================================================
    // LOAD DATA
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    // Load columns
    MmapArray<int32_t> region_r_regionkey;
    MmapArray<int32_t> nation_n_nationkey, nation_n_regionkey;
    MmapArray<int32_t> supplier_s_suppkey, supplier_s_nationkey;
    MmapArray<int32_t> customer_c_custkey, customer_c_nationkey;
    MmapArray<int32_t> orders_o_orderkey, orders_o_custkey, orders_o_orderdate;
    MmapArray<int32_t> lineitem_l_orderkey, lineitem_l_suppkey;
    MmapArray<int64_t> lineitem_l_extendedprice, lineitem_l_discount;

    region_r_regionkey.load(gendb_dir + "/region/r_regionkey.bin");
    nation_n_nationkey.load(gendb_dir + "/nation/n_nationkey.bin");
    nation_n_regionkey.load(gendb_dir + "/nation/n_regionkey.bin");
    supplier_s_suppkey.load(gendb_dir + "/supplier/s_suppkey.bin");
    supplier_s_nationkey.load(gendb_dir + "/supplier/s_nationkey.bin");
    customer_c_custkey.load(gendb_dir + "/customer/c_custkey.bin");
    customer_c_nationkey.load(gendb_dir + "/customer/c_nationkey.bin");
    orders_o_orderkey.load(gendb_dir + "/orders/o_orderkey.bin");
    orders_o_custkey.load(gendb_dir + "/orders/o_custkey.bin");
    orders_o_orderdate.load(gendb_dir + "/orders/o_orderdate.bin");
    lineitem_l_orderkey.load(gendb_dir + "/lineitem/l_orderkey.bin");
    lineitem_l_suppkey.load(gendb_dir + "/lineitem/l_suppkey.bin");
    lineitem_l_extendedprice.load(gendb_dir + "/lineitem/l_extendedprice.bin");
    lineitem_l_discount.load(gendb_dir + "/lineitem/l_discount.bin");

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
        printf("[TIMING] load: %.2f ms\n", ms);
    }
#endif

    // ========================================================================
    // PHASE 1: FILTER REGION WHERE r_name = 'ASIA'
    // TPC-H standard: ASIA = regionkey 2
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_region_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t asia_regionkey = 2;

#ifdef GENDB_PROFILE
    auto t_region_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_region_end - t_region_start).count();
        printf("[TIMING] filter_region: %.2f ms\n", ms);
    }
#endif

    // ========================================================================
    // PHASE 2: FILTER NATION WHERE n_regionkey = asia_regionkey
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_nation_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_set<int32_t> asia_nations;
    for (size_t n = 0; n < nation_n_nationkey.size(); n++) {
        if (nation_n_regionkey[n] == asia_regionkey) {
            asia_nations.insert(nation_n_nationkey[n]);
        }
    }

#ifdef GENDB_PROFILE
    auto t_nation_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_nation_end - t_nation_start).count();
        printf("[TIMING] filter_nation: %.2f ms\n", ms);
    }
#endif

    printf("Found %zu nations in ASIA region\n", asia_nations.size());

    // ========================================================================
    // PHASE 3: FILTER SUPPLIER WHERE s_nationkey IN asia_nations
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_supplier_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, int32_t> supplier_to_nation;
    for (size_t s = 0; s < supplier_s_suppkey.size(); s++) {
        int32_t sk = supplier_s_suppkey[s];
        int32_t nk = supplier_s_nationkey[s];
        if (asia_nations.count(nk)) {
            supplier_to_nation[sk] = nk;
        }
    }

#ifdef GENDB_PROFILE
    auto t_supplier_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_supplier_end - t_supplier_start).count();
        printf("[TIMING] filter_supplier: %.2f ms\n", ms);
    }
#endif

    printf("Found %zu suppliers in ASIA\n", supplier_to_nation.size());

    // ========================================================================
    // PHASE 4: FILTER ORDERS WHERE o_orderdate >= 1994-01-01 AND < 1995-01-01
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t order_date_min = 8766;   // 1994-01-01
    int32_t order_date_max = 9131;   // 1995-01-01

    // Build hash map: orderkey -> custkey for filtered orders
    std::unordered_map<int32_t, int32_t> filtered_orders;
    for (size_t o = 0; o < orders_o_orderkey.size(); o++) {
        int32_t od = orders_o_orderdate[o];
        if (od >= order_date_min && od < order_date_max) {
            filtered_orders[orders_o_orderkey[o]] = orders_o_custkey[o];
        }
    }

#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
        printf("[TIMING] filter_orders: %.2f ms\n", ms);
    }
#endif

    printf("Found %zu orders in date range\n", filtered_orders.size());

    // ========================================================================
    // PHASE 5: BUILD AGGREGATION ON LINEITEM SCAN
    //
    // Join: lineitem -> orders (filtered) -> supplier (filtered) -> nation
    // Aggregate by n_name, SUM(l_extendedprice * (1 - l_discount))
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    // Build customer nation map: custkey -> nationkey
    std::unordered_map<int32_t, int32_t> customer_to_nation;
    for (size_t c = 0; c < customer_c_custkey.size(); c++) {
        customer_to_nation[customer_c_custkey[c]] = customer_c_nationkey[c];
    }

    // Thread-local aggregation to avoid locks
    std::vector<std::unordered_map<int32_t, int64_t>> local_agg(omp_get_max_threads());

    // Scan lineitem
    #pragma omp parallel for schedule(dynamic, 100000)
    for (size_t l = 0; l < lineitem_l_orderkey.size(); l++) {
        int32_t lk = lineitem_l_orderkey[l];
        int32_t sk = lineitem_l_suppkey[l];
        int64_t price = lineitem_l_extendedprice[l];
        int64_t disc = lineitem_l_discount[l];

        // Check if supplier is in ASIA
        auto supp_it = supplier_to_nation.find(sk);
        if (supp_it == supplier_to_nation.end()) continue;

        // Check if order exists and has matching date
        auto order_it = filtered_orders.find(lk);
        if (order_it == filtered_orders.end()) continue;

        // Get nation key from supplier and customer
        int32_t s_nk = supp_it->second;
        int32_t c_nk = customer_to_nation[order_it->second];

        // Verify customer nation matches supplier nation (c_nationkey = s_nationkey)
        if (c_nk != s_nk) continue;

        // Compute revenue: l_extendedprice * (1 - l_discount)
        // Both price and disc are scaled by 100
        // price is in units of 0.01, disc is in units of 0.01 (0-10 means 0.00-0.10)
        // revenue = price * (1 - disc/100)
        // Since both scaled by 100: revenue_scaled = (price * (10000 - disc)) / 10000
        int64_t revenue = (price * (10000 - disc)) / 10000;

        int tid = omp_get_thread_num();
        local_agg[tid][s_nk] += revenue;
    }

    // Merge thread-local aggregations
    std::unordered_map<int32_t, int64_t> agg_by_nation_key;
    for (const auto& local : local_agg) {
        for (const auto& [nk, revenue] : local) {
            agg_by_nation_key[nk] += revenue;
        }
    }

#ifdef GENDB_PROFILE
    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
        printf("[TIMING] scan_join_aggregate_lineitem: %.2f ms\n", ms);
    }
#endif

    printf("Aggregated %zu nation groups\n", agg_by_nation_key.size());

    // ========================================================================
    // PHASE 6: MAP NATION KEYS TO NAMES
    // ========================================================================

    // Build nation key to name map from nation table
    std::unordered_map<int32_t, std::string> nation_id_to_name;
    // We need to load nation names from storage
    // For now, use standard TPC-H mapping (keys 0-24)
    std::vector<std::string> nation_names = {
        "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT",
        "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA",
        "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
        "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA",
        "RUSSIA", "SAUDI ARABIA", "UNITED KINGDOM", "UNITED STATES", "VIETNAM"
    };
    for (size_t i = 0; i < nation_names.size(); i++) {
        nation_id_to_name[i] = nation_names[i];
    }

    // Convert aggregation keys back to nation names
    std::map<std::string, int64_t> final_result;
    for (auto& [nk, revenue] : agg_by_nation_key) {
        std::string nname = nation_id_to_name.count(nk) ? nation_id_to_name[nk] : std::to_string(nk);
        final_result[nname] = revenue;
    }

    // ========================================================================
    // PHASE 7: SORT BY REVENUE DESC
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::pair<std::string, int64_t>> sorted_result(final_result.begin(), final_result.end());
    std::sort(sorted_result.begin(), sorted_result.end(),
              [](const auto& a, const auto& b) {
                  return a.second > b.second;
              });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
        printf("[TIMING] sort: %.2f ms\n", ms);
    }
#endif

    // ========================================================================
    // WRITE RESULTS
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q5.csv";
    std::ofstream out(output_path);
    out << "n_name,revenue\n";

    for (auto& [nation, revenue] : sorted_result) {
        // revenue is in scaled units (scale 100), convert to decimal
        double revenue_float = revenue / 100.0;
        out << nation << "," << std::fixed << std::setprecision(2) << revenue_float << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    {
        double ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
        printf("[TIMING] output: %.2f ms\n", ms);
    }
#endif

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();

#ifdef GENDB_PROFILE
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    printf("Query Q5 completed. Results written to %s\n", output_path.c_str());
    printf("Result rows: %zu\n", sorted_result.size());
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_q5(gendb_dir, results_dir);

    return 0;
}
#endif
