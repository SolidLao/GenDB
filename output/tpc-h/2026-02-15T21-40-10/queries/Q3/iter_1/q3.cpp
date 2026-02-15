#include <algorithm>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <fcntl.h>
#include <chrono>
#include <omp.h>
#include <atomic>

// ============================================================================
// [METADATA CHECK] Q3 Storage Configuration
// ============================================================================
// Tables:
//   - customer: 1,500,000 rows, c_custkey (int32_t), c_mktsegment (dict)
//   - orders: 15,000,000 rows, o_orderkey, o_custkey, o_orderdate (int32_t), o_shippriority
//   - lineitem: 59,986,052 rows, l_orderkey, l_shipdate, l_extendedprice, l_discount
//
// Key encodings:
//   - Dates: int32_t epoch days (1995-03-15 = 9204 days)
//   - Decimals: int64_t × scale_factor=100 (e.g., 0.05 stored as 5)
//   - c_mktsegment: dictionary-encoded (0=BUILDING, 1=AUTOMOBILE, etc.)
//
// Predicates:
//   - c_mktsegment = 'BUILDING' (code 0)
//   - o_orderdate < 1995-03-15 (epoch day 9204)
//   - l_shipdate > 1995-03-15 (epoch day 9204)
//
// Joins: customer.c_custkey = orders.o_custkey = lineitem via orders.o_orderkey
// Group by: l_orderkey, o_orderdate, o_shippriority
// Order by: revenue DESC, o_orderdate ASC (LIMIT 10)
// ============================================================================

// Memory-mapped file handling
struct MMapFile {
    int fd;
    void* ptr;
    size_t size;

    MMapFile(const std::string& path) : fd(-1), ptr(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "ERROR: Cannot open " << path << std::endl;
            return;
        }
        struct stat st;
        if (fstat(fd, &st) < 0) {
            close(fd);
            fd = -1;
            return;
        }
        size = st.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) {
            ptr = nullptr;
            close(fd);
            fd = -1;
        }
    }

    ~MMapFile() {
        if (ptr) munmap(ptr, size);
        if (fd >= 0) close(fd);
    }

    bool is_valid() const { return fd >= 0 && ptr != nullptr; }
};

// Multi-value hash index structure (from pre-built indexes)
struct HashIndexEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

// Load multi-value hash index from mmap file
struct MultiValueHashIndex {
    uint32_t num_unique;
    uint32_t table_size;
    const HashIndexEntry* entries;
    const uint32_t* positions;

    MultiValueHashIndex(const void* ptr, size_t file_size)
        : num_unique(0), table_size(0), entries(nullptr), positions(nullptr) {
        const uint8_t* data = (const uint8_t*)ptr;
        num_unique = *(const uint32_t*)data;
        data += 4;
        table_size = *(const uint32_t*)data;
        data += 4;
        entries = (const HashIndexEntry*)data;
        positions = (const uint32_t*)(data + (size_t)table_size * 12);
    }

    const uint32_t* find_positions(int32_t key, uint32_t& count) const {
        const HashIndexEntry* ent = entries;
        for (uint32_t i = 0; i < table_size; i++) {
            if (ent->key == key) {
                count = ent->count;
                return &positions[ent->offset];
            }
            ent++;
        }
        count = 0;
        return nullptr;
    }
};

// Load dictionary (code -> string mapping)
// Format: "code=value\n"
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> result;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "ERROR: Cannot open dictionary " << dict_path << std::endl;
        return result;
    }
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        int32_t code = std::stoi(line.substr(0, eq));
        std::string value = line.substr(eq + 1);
        result[code] = value;
    }
    f.close();
    return result;
}

// Find dictionary code for a target string value
int32_t find_dict_code(const std::unordered_map<int32_t, std::string>& dict,
                       const std::string& target) {
    for (const auto& [code, value] : dict) {
        if (value == target) return code;
    }
    return -1;  // Not found
}

// Convert epoch days to YYYY-MM-DD format
std::string format_date(int32_t epoch_days) {
    // Convert epoch days (since 1970-01-01) to YYYY-MM-DD
    const int days_per_year = 365;

    int32_t d = epoch_days;
    int year = 1970;
    int month = 1;
    int day = 1;

    // Rough year calculation
    while (d >= days_per_year + (((year % 4 == 0 && year % 100 != 0) || year % 400 == 0) ? 1 : 0)) {
        d -= days_per_year + (((year % 4 == 0 && year % 100 != 0) || year % 400 == 0) ? 1 : 0);
        year++;
    }

    // Month calculation
    const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    for (int m = 1; m <= 12; m++) {
        int dim = days_in_month[m];
        if (m == 2 && is_leap) dim = 29;
        if (d < dim) {
            month = m;
            day = d + 1;
            break;
        }
        d -= dim;
    }

    char buf[12];
    snprintf(buf, 12, "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

// Query result entry
struct ResultEntry {
    int32_t l_orderkey;
    int64_t revenue;  // stored as scaled value (scale_factor=100)
    int32_t o_orderdate;
    int32_t o_shippriority;

    // For sorting: revenue DESC, o_orderdate ASC
    bool operator<(const ResultEntry& other) const {
        if (revenue != other.revenue) return revenue > other.revenue;  // DESC
        return o_orderdate < other.o_orderdate;  // ASC
    }
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    using namespace std;

    auto t_total_start = chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    auto t_start = chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // LOAD DATA
    // ========================================================================

    // Load customer columns
    MMapFile cust_custkey(gendb_dir + "/customer/c_custkey.bin");
    MMapFile cust_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");
    if (!cust_custkey.is_valid() || !cust_mktsegment.is_valid()) {
        cerr << "ERROR: Cannot load customer columns" << endl;
        return;
    }

    auto* c_custkey = (const int32_t*)cust_custkey.ptr;
    auto* c_mktsegment = (const int32_t*)cust_mktsegment.ptr;
    size_t num_customers = cust_custkey.size / sizeof(int32_t);

    // Load orders columns
    MMapFile ord_orderkey(gendb_dir + "/orders/o_orderkey.bin");
    MMapFile ord_custkey(gendb_dir + "/orders/o_custkey.bin");
    MMapFile ord_orderdate(gendb_dir + "/orders/o_orderdate.bin");
    MMapFile ord_shippriority(gendb_dir + "/orders/o_shippriority.bin");
    if (!ord_orderkey.is_valid() || !ord_custkey.is_valid() ||
        !ord_orderdate.is_valid() || !ord_shippriority.is_valid()) {
        cerr << "ERROR: Cannot load orders columns" << endl;
        return;
    }

    auto* o_orderkey = (const int32_t*)ord_orderkey.ptr;
    auto* o_custkey = (const int32_t*)ord_custkey.ptr;
    auto* o_orderdate = (const int32_t*)ord_orderdate.ptr;
    auto* o_shippriority = (const int32_t*)ord_shippriority.ptr;
    size_t num_orders = ord_orderkey.size / sizeof(int32_t);

    // Load lineitem columns
    MMapFile line_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
    MMapFile line_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");
    MMapFile line_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
    MMapFile line_discount(gendb_dir + "/lineitem/l_discount.bin");
    if (!line_orderkey.is_valid() || !line_shipdate.is_valid() ||
        !line_extendedprice.is_valid() || !line_discount.is_valid()) {
        cerr << "ERROR: Cannot load lineitem columns" << endl;
        return;
    }

    auto* l_orderkey = (const int32_t*)line_orderkey.ptr;
    auto* l_shipdate = (const int32_t*)line_shipdate.ptr;
    auto* l_extendedprice = (const int64_t*)line_extendedprice.ptr;
    auto* l_discount = (const int64_t*)line_discount.ptr;
    size_t num_lineitems = line_orderkey.size / sizeof(int32_t);

    // Load dictionary for c_mktsegment
    auto dict_mktsegment = load_dictionary(gendb_dir + "/customer/c_mktsegment_dict.txt");
    int32_t code_building = find_dict_code(dict_mktsegment, "BUILDING");
    if (code_building < 0) {
        cerr << "ERROR: BUILDING code not found in dictionary" << endl;
        return;
    }

#ifdef GENDB_PROFILE
    auto t_load = chrono::high_resolution_clock::now();
    double ms_load = chrono::duration<double, milli>(t_load - t_start).count();
    printf("[TIMING] load: %.2f ms\n", ms_load);
#endif

    // ========================================================================
    // STEP 1: Filter customer table (c_mktsegment = 'BUILDING')
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_cust_filter = chrono::high_resolution_clock::now();
#endif

    unordered_set<int32_t> building_custkeys;
    int num_threads = omp_get_max_threads();

    // Parallel filter with thread-local accumulation
    std::vector<unordered_set<int32_t>> local_sets(num_threads);
    #pragma omp parallel for schedule(static, 100000)
    for (size_t i = 0; i < num_customers; i++) {
        if (c_mktsegment[i] == code_building) {
            int tid = omp_get_thread_num();
            local_sets[tid].insert(c_custkey[i]);
        }
    }

    // Merge local sets
    for (int t = 0; t < num_threads; t++) {
        for (const auto& key : local_sets[t]) {
            building_custkeys.insert(key);
        }
    }

#ifdef GENDB_PROFILE
    auto t_cust_filtered = chrono::high_resolution_clock::now();
    double ms_cust_filter = chrono::duration<double, milli>(t_cust_filtered - t_cust_filter).count();
    printf("[TIMING] customer_filter: %.2f ms\n", ms_cust_filter);
#endif

    // ========================================================================
    // STEP 2: Filter orders table (c_custkey IN building_custkeys AND o_orderdate < 9204)
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_ord_filter = chrono::high_resolution_clock::now();
#endif

    const int32_t DATE_CUTOFF = 9204;  // 1995-03-15

    // Map o_orderkey -> {o_orderdate, o_shippriority}
    unordered_map<int32_t, pair<int32_t, int32_t>> filtered_orders;
    std::vector<unordered_map<int32_t, pair<int32_t, int32_t>>> local_maps(num_threads);

    // Parallel filter with thread-local accumulation
    #pragma omp parallel for schedule(static, 150000)
    for (size_t i = 0; i < num_orders; i++) {
        if (o_orderdate[i] < DATE_CUTOFF && building_custkeys.count(o_custkey[i])) {
            int tid = omp_get_thread_num();
            local_maps[tid][o_orderkey[i]] = {o_orderdate[i], o_shippriority[i]};
        }
    }

    // Merge local maps
    for (int t = 0; t < num_threads; t++) {
        for (const auto& [key, value] : local_maps[t]) {
            filtered_orders[key] = value;
        }
    }

#ifdef GENDB_PROFILE
    auto t_ord_filtered = chrono::high_resolution_clock::now();
    double ms_ord_filter = chrono::duration<double, milli>(t_ord_filtered - t_ord_filter).count();
    printf("[TIMING] orders_filter: %.2f ms\n", ms_ord_filter);
#endif

    // ========================================================================
    // STEP 3: Scan lineitem, join with filtered_orders, compute revenue
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_join_start = chrono::high_resolution_clock::now();
#endif

    // Aggregation: {l_orderkey, o_orderdate, o_shippriority} -> revenue (sum)
    // Key insight: accumulate at full scale_factor^2, divide only at output
    // Thread-local aggregation to avoid synchronization in hot loop
    std::vector<unordered_map<uint64_t, int64_t>> local_revenues(num_threads);
    std::vector<unordered_map<uint64_t, pair<int32_t, int32_t>>> local_metadata(num_threads);

    #pragma omp parallel for schedule(static, 500000)
    for (size_t i = 0; i < num_lineitems; i++) {
        if (l_shipdate[i] <= DATE_CUTOFF) continue;  // Filter: l_shipdate > 9204

        int32_t order_key = l_orderkey[i];
        auto it = filtered_orders.find(order_key);
        if (it == filtered_orders.end()) continue;  // Not in filtered orders

        int32_t o_date = it->second.first;
        int32_t o_priority = it->second.second;

        // Compute revenue: l_extendedprice * (1 - l_discount)
        // Both inputs are scaled by 100:
        //   l_extendedprice_scaled = actual_price × 100
        //   l_discount_scaled = actual_discount × 100
        // revenue = l_extendedprice_scaled * (1 - l_discount_scaled/100) / 100
        //         = l_extendedprice_scaled * (100 - l_discount_scaled) / 10000
        // To maintain precision, accumulate the numerator and divide only once at the end
        int64_t revenue_unscaled = l_extendedprice[i] * (100 - l_discount[i]);

        // Create a unique hash for (l_orderkey, o_date, o_priority)
        uint64_t key = ((uint64_t)order_key << 32) | ((uint32_t)o_date << 8) | (uint8_t)o_priority;

        int tid = omp_get_thread_num();
        local_revenues[tid][key] += revenue_unscaled;
        local_metadata[tid][key] = {o_date, o_priority};
    }

    // Merge local aggregations
    unordered_map<uint64_t, int64_t> agg_revenue;
    unordered_map<uint64_t, pair<int32_t, int32_t>> agg_metadata;
    for (int t = 0; t < num_threads; t++) {
        for (const auto& [key, revenue] : local_revenues[t]) {
            agg_revenue[key] += revenue;
            agg_metadata[key] = local_metadata[t][key];
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = chrono::high_resolution_clock::now();
    double ms_join = chrono::duration<double, milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join_and_aggregate: %.2f ms\n", ms_join);
#endif

    // ========================================================================
    // STEP 4: Prepare results (already grouped by l_orderkey, o_orderdate, o_shippriority)
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_output_prep = chrono::high_resolution_clock::now();
#endif

    vector<ResultEntry> results;
    for (const auto& [hash_key, revenue] : agg_revenue) {
        int32_t l_orderkey_val = (int32_t)(hash_key >> 32);
        auto [o_date, o_priority] = agg_metadata[hash_key];
        results.push_back({l_orderkey_val, revenue, o_date, o_priority});
    }

    // Sort: revenue DESC, o_orderdate ASC
    sort(results.begin(), results.end());

    // Limit to 10
    if (results.size() > 10) {
        results.resize(10);
    }

#ifdef GENDB_PROFILE
    auto t_output_ready = chrono::high_resolution_clock::now();
    double ms_output_prep = chrono::duration<double, milli>(t_output_ready - t_output_prep).count();
    printf("[TIMING] output_prep: %.2f ms\n", ms_output_prep);
#endif

    // ========================================================================
    // STEP 5: Write CSV output
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_write_start = chrono::high_resolution_clock::now();
#endif

    string output_path = results_dir + "/Q3.csv";
    ofstream out(output_path);
    if (!out.is_open()) {
        cerr << "ERROR: Cannot open " << output_path << " for writing" << endl;
        return;
    }

    // Write header
    out << "l_orderkey,revenue,o_orderdate,o_shippriority\r\n";

    // Write rows
    for (const auto& row : results) {
        // revenue is accumulated as scaled_price * (100 - scaled_discount)
        // To get final value, divide by 10000 (100 * 100)
        double revenue_decimal = static_cast<double>(row.revenue) / 10000.0;
        string date_str = format_date(row.o_orderdate);
        out << row.l_orderkey << ","
            << fixed << setprecision(4) << revenue_decimal << ","
            << date_str << ","
            << row.o_shippriority << "\r\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    auto t_write_end = chrono::high_resolution_clock::now();
    double ms_write = chrono::duration<double, milli>(t_write_end - t_write_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_write);
#endif

    auto t_total_end = chrono::high_resolution_clock::now();
    double ms_total = chrono::duration<double, milli>(t_total_end - t_total_start).count();

#ifdef GENDB_PROFILE
    printf("[TIMING] total: %.2f ms\n", ms_total - chrono::duration<double, milli>(t_write_end - t_write_start).count());
#endif

    cout << "Q3 executed successfully. Results written to " << output_path << endl;
    cout << "Rows in result set: " << results.size() << endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    using namespace std;
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << endl;
        return 1;
    }
    string gendb_dir = argv[1];
    string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
