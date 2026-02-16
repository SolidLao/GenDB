#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <chrono>
#include <cmath>
#include <thread>
#include <iomanip>
#include <omp.h>

// ============================================================================
// CONSTANTS & HELPER FUNCTIONS
// ============================================================================

// Epoch date constants (days since 1970-01-01)
// 1995-03-15 = 9204 days
// 1995-03-15 (exclusive) means l_shipdate > 9204
static constexpr int32_t DATE_1995_03_15 = 9204;
static constexpr int32_t DATE_1995_03_15_EXCLUSIVE = 9205;

// DECIMAL scale factor
static constexpr int64_t SCALE_FACTOR = 100;

struct ResultRow {
    int32_t l_orderkey;
    __int128 revenue;  // in scaled form (scaled by 10000 = SCALE_FACTOR^2)
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// Convert epoch days to YYYY-MM-DD string
std::string format_date(int32_t days) {
    // Based on standard epoch formula
    int32_t day_count = days;

    // Adjust for epoch (1970-01-01)
    int32_t year = 1970;

    // Add years
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (day_count < days_in_year) break;
        day_count -= days_in_year;
        year++;
    }

    // Add months
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[1] = 29;
    }

    int32_t month = 0;
    while (day_count >= days_in_month[month]) {
        day_count -= days_in_month[month];
        month++;
    }

    int32_t day = day_count + 1;  // Days are 1-indexed

    char buf[12];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month + 1, day);
    return std::string(buf);
}

// Load dictionary from file and find code for target string
int8_t find_dict_code(const std::string& dict_path, const std::string& target) {
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "ERROR: Cannot open dictionary file: " << dict_path << std::endl;
        return -1;
    }

    std::string line;
    while (std::getline(f, line)) {
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            std::string value = line.substr(eq_pos + 1);
            if (value == target) {
                return static_cast<int8_t>(std::stoi(line.substr(0, eq_pos)));
            }
        }
    }
    return -1;
}

// Mmap helper
template<typename T>
const T* mmap_file(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "ERROR: Cannot open file: " << path << std::endl;
        return nullptr;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return nullptr;
    }

    count = st.st_size / sizeof(T);
    auto* ptr = (const T*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "ERROR: mmap failed for " << path << std::endl;
        return nullptr;
    }
    return ptr;
}

// ============================================================================
// ZONE MAP STRUCTURES
// ============================================================================

struct ZoneMapEntry {
    int32_t min_value;
    int32_t max_value;
    uint32_t row_count;
};

const ZoneMapEntry* load_zone_map(const std::string& path, size_t& num_zones) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "ERROR: Cannot open zone map: " << path << std::endl;
        return nullptr;
    }

    uint32_t header;
    if (read(fd, &header, sizeof(uint32_t)) != sizeof(uint32_t)) {
        close(fd);
        return nullptr;
    }
    num_zones = header;

    size_t data_size = num_zones * sizeof(ZoneMapEntry);
    auto* zones = (ZoneMapEntry*)mmap(nullptr, data_size, PROT_READ, MAP_PRIVATE, fd, sizeof(uint32_t));
    close(fd);

    if (zones == MAP_FAILED) {
        std::cerr << "ERROR: mmap failed for zone map" << std::endl;
        return nullptr;
    }
    return zones;
}

// ============================================================================
// HASH INDEX STRUCTURES & LOADING
// ============================================================================

struct HashIndexEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

struct HashIndex {
    uint32_t num_unique;
    uint32_t table_size;
    const HashIndexEntry* entries;
    const uint32_t* positions;
};

HashIndex load_hash_index(const std::string& path) {
    HashIndex idx = {};

    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "ERROR: Cannot open hash index: " << path << std::endl;
        return idx;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return idx;
    }

    auto* base = (const uint8_t*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (base == MAP_FAILED) {
        close(fd);
        return idx;
    }
    close(fd);

    const uint32_t* ptr = (const uint32_t*)base;
    idx.num_unique = *ptr++;
    idx.table_size = *ptr++;
    idx.entries = (const HashIndexEntry*)ptr;
    idx.positions = (const uint32_t*)(idx.entries + idx.table_size);

    return idx;
}

// ============================================================================
// MAIN QUERY EXECUTION
// ============================================================================

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // ========================================================================
    // PHASE 1: Load Customer Data
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    size_t num_customers = 0;
    const int32_t* c_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", num_customers);
    const int8_t* c_mktsegment = mmap_file<int8_t>(gendb_dir + "/customer/c_mktsegment.bin", num_customers);

    if (!c_custkey || !c_mktsegment) {
        std::cerr << "ERROR: Failed to load customer data" << std::endl;
        return;
    }

    // Load BUILDING code from dictionary
    int8_t building_code = find_dict_code(gendb_dir + "/customer/c_mktsegment_dict.txt", "BUILDING");
    if (building_code < 0) {
        std::cerr << "ERROR: Cannot find BUILDING code in dictionary" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_customer: %.2f ms\n", ms);
#endif

    // Filter customers: c_mktsegment = 'BUILDING'
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<int32_t> building_customers;
    for (size_t i = 0; i < num_customers; i++) {
        if (c_mktsegment[i] == building_code) {
            building_customers.push_back(c_custkey[i]);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] filter_building_customers: %.2f ms (count=%zu)\n", ms, building_customers.size());
#endif

    // ========================================================================
    // PHASE 2: Load Orders Data
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    size_t num_orders = 0;
    const int32_t* o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", num_orders);
    const int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", num_orders);
    const int32_t* o_orderdate = mmap_file<int32_t>(gendb_dir + "/orders/o_orderdate.bin", num_orders);
    const int32_t* o_shippriority = mmap_file<int32_t>(gendb_dir + "/orders/o_shippriority.bin", num_orders);

    if (!o_orderkey || !o_custkey || !o_orderdate || !o_shippriority) {
        std::cerr << "ERROR: Failed to load orders data" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", ms);
#endif

    // Build set of BUILDING customers for quick lookup
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_set<int32_t> building_set(building_customers.begin(), building_customers.end());

    // Load pre-built hash index for orders by custkey
    HashIndex orders_custkey_idx = load_hash_index(gendb_dir + "/indexes/orders_o_custkey_hash.bin");
    if (orders_custkey_idx.num_unique == 0) {
        std::cerr << "ERROR: Failed to load orders custkey hash index" << std::endl;
        return;
    }

    // Filter orders: c_custkey in BUILDING and o_orderdate < 1995-03-15
    // Use pre-built hash index to find all orders for each building customer
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> orders_map;  // orderkey -> (orderdate, shippriority)
    orders_map.reserve(1500000);  // Pre-size to avoid rehashing

    for (int32_t custkey : building_customers) {
        // Hash the custkey to find its bucket in the index
        uint64_t hash_val = ((uint64_t)custkey * 0x9E3779B97F4A7C15ULL);
        uint32_t bucket = hash_val % orders_custkey_idx.table_size;

        // Linear probe to find the entry
        for (uint32_t probe = 0; probe < orders_custkey_idx.table_size; probe++) {
            uint32_t idx = (bucket + probe) % orders_custkey_idx.table_size;
            const HashIndexEntry& entry = orders_custkey_idx.entries[idx];

            if (entry.key == custkey) {
                // Found the bucket with all orderkeys for this custkey
                const uint32_t* positions = orders_custkey_idx.positions + entry.offset;
                for (uint32_t j = 0; j < entry.count; j++) {
                    uint32_t order_idx = positions[j];
                    if (o_orderdate[order_idx] < DATE_1995_03_15) {
                        orders_map[o_orderkey[order_idx]] = {o_orderdate[order_idx], o_shippriority[order_idx]};
                    }
                }
                break;
            }

            // Stop if we hit an empty slot
            if (entry.key == 0 && entry.count == 0 && entry.offset == 0) break;
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] filter_and_build_orders: %.2f ms (count=%zu)\n", ms, orders_map.size());
#endif

    // ========================================================================
    // PHASE 3: Load Lineitem Data and Zone Map
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    size_t num_lineitem = 0;
    const int32_t* l_orderkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", num_lineitem);
    const int64_t* l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", num_lineitem);
    const int64_t* l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", num_lineitem);
    const int32_t* l_shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", num_lineitem);

    if (!l_orderkey || !l_extendedprice || !l_discount || !l_shipdate) {
        std::cerr << "ERROR: Failed to load lineitem data" << std::endl;
        return;
    }

    // Load zone map for l_shipdate for block pruning
    size_t num_zones = 0;
    const ZoneMapEntry* l_shipdate_zones = load_zone_map(gendb_dir + "/indexes/lineitem_l_shipdate_zone.bin", num_zones);

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_lineitem: %.2f ms\n", ms);
#endif

    // ========================================================================
    // PHASE 4: Join and Aggregate (with zone map pruning and parallelism)
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Hash map: l_orderkey -> (revenue accumulator, o_orderdate, o_shippriority)
    // Use __int128 for intermediate products to avoid overflow and preserve precision
    struct OrderData {
        __int128 total_revenue;  // Accumulated as full-precision product
        int32_t orderdate;
        int32_t shippriority;
    };
    std::unordered_map<int32_t, OrderData> result_groups;
    result_groups.reserve(1500000);  // Pre-size to avoid rehashing

    // Parallel scan with thread-local aggregation
    int num_threads = std::min(64, (int)std::thread::hardware_concurrency());
    std::vector<std::unordered_map<int32_t, OrderData>> thread_local_groups(num_threads);
    for (int t = 0; t < num_threads; t++) {
        thread_local_groups[t].reserve(1500000 / num_threads + 1000);
    }

    // Build zone row offsets for efficient block range calculation
    std::vector<uint32_t> zone_row_offset(num_zones + 1, 0);
    if (l_shipdate_zones && num_zones > 0) {
        for (size_t z = 0; z < num_zones; z++) {
            zone_row_offset[z + 1] = zone_row_offset[z] + l_shipdate_zones[z].row_count;
        }
    }

    // OpenMP parallel scan with zone map pruning
    #pragma omp parallel num_threads(num_threads)
    {
        int thread_id = omp_get_thread_num();
        auto& local_groups = thread_local_groups[thread_id];

        #pragma omp for schedule(dynamic, 1)
        for (size_t z = 0; z < num_zones; z++) {
            // Skip blocks where max_value <= DATE_1995_03_15 (l_shipdate > DATE_1995_03_15 predicate)
            if (l_shipdate_zones[z].max_value <= DATE_1995_03_15) continue;

            uint32_t start_row = zone_row_offset[z];
            uint32_t end_row = zone_row_offset[z + 1];

            // Process rows in this zone
            for (uint32_t i = start_row; i < end_row; i++) {
                int32_t orderkey = l_orderkey[i];

                // Check if orderkey exists in filtered orders
                auto it = orders_map.find(orderkey);
                if (it == orders_map.end()) continue;

                int32_t orderdate = it->second.first;
                int32_t shippriority = it->second.second;

                // Check l_shipdate > 1995-03-15 (double-check even with zone pruning)
                if (l_shipdate[i] <= DATE_1995_03_15) continue;

                // Calculate revenue = l_extendedprice * (1 - l_discount)
                // Both are scaled by 100, so:
                // extended_price * (100 - discount) gives us result scaled by 100^2 = 10000
                __int128 product = (__int128)l_extendedprice[i] * (SCALE_FACTOR - l_discount[i]);

                auto it2 = local_groups.find(orderkey);
                if (it2 == local_groups.end()) {
                    local_groups[orderkey] = {product, orderdate, shippriority};
                } else {
                    it2->second.total_revenue += product;
                }
            }
        }
    }

    // Merge thread-local results into global result_groups
    for (int t = 0; t < num_threads; t++) {
        for (const auto& [orderkey, data] : thread_local_groups[t]) {
            auto it = result_groups.find(orderkey);
            if (it == result_groups.end()) {
                result_groups[orderkey] = data;
            } else {
                it->second.total_revenue += data.total_revenue;
            }
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] join_and_aggregate: %.2f ms\n", ms);
#endif

    // ========================================================================
    // PHASE 5: GROUP BY and SUM
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<ResultRow> results;
    for (const auto& [orderkey, data] : result_groups) {
        results.push_back({orderkey, data.total_revenue, data.orderdate, data.shippriority});
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] group_and_sum: %.2f ms\n", ms);
#endif

    // ========================================================================
    // PHASE 6: ORDER BY revenue DESC, o_orderdate
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.revenue != b.revenue) {
            return a.revenue > b.revenue;  // DESC
        }
        return a.o_orderdate < b.o_orderdate;  // ASC
    });

    // LIMIT 10
    if (results.size() > 10) {
        results.resize(10);
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] sort_and_limit: %.2f ms\n", ms);
#endif

    // ========================================================================
    // PHASE 7: Write Results
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q3.csv";
    std::ofstream out(output_path);
    if (!out.is_open()) {
        std::cerr << "ERROR: Cannot open output file: " << output_path << std::endl;
        return;
    }

    // Write header
    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    // Write results
    for (const auto& row : results) {
        // revenue is scaled by 10000 (SCALE_FACTOR^2), so divide to get original value
        double revenue_double = static_cast<double>(row.revenue) / (SCALE_FACTOR * SCALE_FACTOR);
        std::string orderdate_str = format_date(row.o_orderdate);
        out << row.l_orderkey << ","
            << std::fixed << std::setprecision(4) << revenue_double << ","
            << orderdate_str << ","
            << row.o_shippriority << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
#endif

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();

#ifdef GENDB_PROFILE
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    std::cout << "Query Q3 executed successfully. Results written to " << output_path << std::endl;
    std::cout << "Result count: " << results.size() << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
