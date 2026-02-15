#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cstdint>
#include <cmath>
#include <iomanip>
#include <omp.h>

namespace {

// Utility: RAII mmap wrapper
class MmapFile {
public:
    void* data = nullptr;
    size_t size = 0;
    int fd = -1;

    MmapFile(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            return;
        }
        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Failed to stat " << path << std::endl;
            close(fd);
            fd = -1;
            return;
        }
        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            data = nullptr;
            close(fd);
            fd = -1;
        }
    }

    ~MmapFile() {
        if (data != nullptr && data != MAP_FAILED) {
            munmap(data, size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    template <typename T>
    const T* as() const { return static_cast<const T*>(data); }
};

// Load dictionary file and find code for target string
int32_t load_dict_code(const std::string& dict_path, const std::string& target) {
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            if (value == target) {
                return code;
            }
        }
    }
    return -1;  // Not found
}

// Convert epoch days to YYYY-MM-DD
std::string format_date(int32_t days) {
    // Use algorithm to convert days since 1970-01-01 to YYYY-MM-DD
    int32_t total_days = days;
    int32_t year = 1970;
    int32_t month = 1;
    int32_t day = 1;

    // Days in each month (non-leap year)
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    // Accumulate days
    int32_t accumulated = 0;

    // Simple iteration (not the most efficient, but correct for small ranges)
    while (accumulated + 365 <= total_days || (year % 4 == 0 && !(year % 100 == 0 && year % 400 != 0) && accumulated + 366 <= total_days)) {
        int days_this_year = (year % 4 == 0 && !(year % 100 == 0 && year % 400 != 0)) ? 366 : 365;
        if (accumulated + days_this_year <= total_days) {
            accumulated += days_this_year;
            year++;
        } else {
            break;
        }
    }

    int remaining = total_days - accumulated;
    int is_leap = (year % 4 == 0 && !(year % 100 == 0 && year % 400 != 0)) ? 1 : 0;

    month = 1;
    for (int m = 0; m < 12; m++) {
        int mdays = days_in_month[m];
        if (m == 1 && is_leap) mdays = 29;
        if (remaining < mdays) {
            day = remaining + 1;
            break;
        }
        remaining -= mdays;
        month++;
    }

    char buf[16];
    snprintf(buf, 16, "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

// Result row structure for aggregation
struct AggResult {
    int32_t l_orderkey;
    int64_t revenue_sum;  // Scaled by 100
    int32_t o_orderdate;
    int32_t o_shippriority;

    double get_revenue() const {
        return revenue_sum / 10000.0;  // revenue_sum is at scale 10000, need scale 100
    }
};

// Compact open-addressing hash table for join lookups
template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(K key) const {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, V value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) { table[idx].value = value; return; }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    V* find(K key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// Structure for pre-built multi-value hash index (lineitem_orderkey_hash)
struct MultiValueHashIndex {
    struct Entry {
        int32_t key;
        uint32_t offset;
        uint32_t count;
    };

    std::vector<Entry> entries;
    std::vector<uint32_t> positions;

    // Load from binary file
    bool load(const std::string& index_path) {
        int fd = open(index_path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open index " << index_path << std::endl;
            return false;
        }

        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Failed to stat index " << index_path << std::endl;
            close(fd);
            return false;
        }

        void* data = mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap index " << index_path << std::endl;
            close(fd);
            return false;
        }

        const uint8_t* ptr = (const uint8_t*)data;
        uint32_t num_unique = *(const uint32_t*)ptr;
        ptr += sizeof(uint32_t);

        entries.resize(num_unique);
        memcpy(entries.data(), ptr, num_unique * sizeof(Entry));
        ptr += num_unique * sizeof(Entry);

        uint32_t pos_count = *(const uint32_t*)ptr;
        ptr += sizeof(uint32_t);

        positions.resize(pos_count);
        memcpy(positions.data(), ptr, pos_count * sizeof(uint32_t));

        munmap(data, st.st_size);
        close(fd);

        return true;
    }

    // Binary search for key in hash table entries
    const Entry* find_entry(int32_t key) const {
        // Since entries are sorted by key, use binary search
        int left = 0, right = (int)entries.size() - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (entries[mid].key == key) {
                return &entries[mid];
            } else if (entries[mid].key < key) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return nullptr;
    }

    // Get all positions for a key
    std::pair<const uint32_t*, uint32_t> find(int32_t key) const {
        const Entry* entry = find_entry(key);
        if (entry == nullptr) {
            return {nullptr, 0};
        }
        return {positions.data() + entry->offset, entry->count};
    }
};

// Zone map structure for block-level pruning
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t start_row;
    uint32_t end_row;
};

// Load zone map file
std::vector<ZoneMapEntry> load_zonemap(const std::string& zonemap_path) {
    std::vector<ZoneMapEntry> zones;
    int fd = open(zonemap_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Warning: failed to open zone map " << zonemap_path << std::endl;
        return zones;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        std::cerr << "Warning: failed to stat zone map" << std::endl;
        close(fd);
        return zones;
    }

    void* data = mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Warning: failed to mmap zone map" << std::endl;
        close(fd);
        return zones;
    }

    size_t num_zones = st.st_size / sizeof(ZoneMapEntry);
    const ZoneMapEntry* zone_data = (const ZoneMapEntry*)data;
    zones.assign(zone_data, zone_data + num_zones);

    munmap(data, st.st_size);
    close(fd);

    return zones;
}

} // end anonymous namespace

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    // Timing instrumentation
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // ----- METADATA CHECK -----
    std::cout << "[METADATA CHECK] Q3 Query" << std::endl;
    std::cout << "  customer.c_custkey: int32_t, no encoding" << std::endl;
    std::cout << "  customer.c_mktsegment: int32_t dictionary-encoded (BUILDING=0)" << std::endl;
    std::cout << "  orders.o_custkey: int32_t, no encoding" << std::endl;
    std::cout << "  orders.o_orderkey: int32_t, no encoding" << std::endl;
    std::cout << "  orders.o_orderdate: int32_t days since 1970-01-01, threshold=9204" << std::endl;
    std::cout << "  orders.o_shippriority: int32_t, no encoding" << std::endl;
    std::cout << "  lineitem.l_orderkey: int32_t, no encoding" << std::endl;
    std::cout << "  lineitem.l_extendedprice: int64_t scaled by 100" << std::endl;
    std::cout << "  lineitem.l_discount: int64_t scaled by 100" << std::endl;
    std::cout << "  lineitem.l_shipdate: int32_t days since 1970-01-01, threshold=9204" << std::endl;
    std::cout << std::endl;

    // ===== LOAD DATA =====
    // Load customer columns
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    MmapFile customer_custkey_f(gendb_dir + "/customer/c_custkey.bin");
    MmapFile customer_mktsegment_f(gendb_dir + "/customer/c_mktsegment.bin");
    const int32_t* customer_custkey = customer_custkey_f.as<int32_t>();
    const int32_t* customer_mktsegment = customer_mktsegment_f.as<int32_t>();
    int32_t num_customers = customer_custkey_f.size / sizeof(int32_t);

    // Load dictionary and find code for 'BUILDING'
    int32_t building_code = load_dict_code(gendb_dir + "/customer/c_mktsegment_dict.txt", "BUILDING");
    std::cout << "[INFO] BUILDING market segment code = " << building_code << std::endl;

    // Load orders columns
    MmapFile orders_orderkey_f(gendb_dir + "/orders/o_orderkey.bin");
    MmapFile orders_custkey_f(gendb_dir + "/orders/o_custkey.bin");
    MmapFile orders_orderdate_f(gendb_dir + "/orders/o_orderdate.bin");
    MmapFile orders_shippriority_f(gendb_dir + "/orders/o_shippriority.bin");
    const int32_t* orders_orderkey = orders_orderkey_f.as<int32_t>();
    const int32_t* orders_custkey = orders_custkey_f.as<int32_t>();
    const int32_t* orders_orderdate = orders_orderdate_f.as<int32_t>();
    const int32_t* orders_shippriority = orders_shippriority_f.as<int32_t>();
    int32_t num_orders = orders_orderkey_f.size / sizeof(int32_t);

    // Load lineitem columns
    MmapFile lineitem_orderkey_f(gendb_dir + "/lineitem/l_orderkey.bin");
    MmapFile lineitem_extendedprice_f(gendb_dir + "/lineitem/l_extendedprice.bin");
    MmapFile lineitem_discount_f(gendb_dir + "/lineitem/l_discount.bin");
    MmapFile lineitem_shipdate_f(gendb_dir + "/lineitem/l_shipdate.bin");
    const int32_t* lineitem_orderkey = lineitem_orderkey_f.as<int32_t>();
    const int64_t* lineitem_extendedprice = lineitem_extendedprice_f.as<int64_t>();
    const int64_t* lineitem_discount = lineitem_discount_f.as<int64_t>();
    const int32_t* lineitem_shipdate = lineitem_shipdate_f.as<int32_t>();
    int32_t num_lineitem = lineitem_orderkey_f.size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] load_data: %.2f ms\n", ms_scan);
#endif

    // ===== FILTER CUSTOMER BY MKTSEGMENT = BUILDING =====
#ifdef GENDB_PROFILE
    auto t_filter_cust_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<int32_t> filtered_customers;
    for (int32_t i = 0; i < num_customers; i++) {
        if (customer_mktsegment[i] == building_code) {
            filtered_customers.push_back(customer_custkey[i]);
        }
    }
    std::cout << "[INFO] Filtered customers (BUILDING): " << filtered_customers.size() << " rows" << std::endl;

#ifdef GENDB_PROFILE
    auto t_filter_cust_end = std::chrono::high_resolution_clock::now();
    double ms_filter_cust = std::chrono::duration<double, std::milli>(t_filter_cust_end - t_filter_cust_start).count();
    printf("[TIMING] filter_customer: %.2f ms\n", ms_filter_cust);
#endif

    // ===== BUILD HASH TABLE ON FILTERED CUSTOMERS =====
#ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_set<int32_t> customer_set(filtered_customers.begin(), filtered_customers.end());

#ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double ms_build = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] build_customer_hash: %.2f ms\n", ms_build);
#endif

    // ===== FILTER ORDERS BY ORDERDATE < 1995-03-15 (9204 days) AND JOIN WITH CUSTOMER =====
#ifdef GENDB_PROFILE
    auto t_filter_orders_start = std::chrono::high_resolution_clock::now();
#endif

    // Structure: (o_orderkey, o_custkey, o_orderdate, o_shippriority)
    struct OrderRow {
        int32_t o_orderkey;
        int32_t o_custkey;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    int32_t order_date_threshold = 9204;  // 1995-03-15

    // Parallel filter with thread-local vectors
    int filter_num_threads = omp_get_max_threads();
    std::vector<std::vector<OrderRow>> local_orders(filter_num_threads);

#pragma omp parallel for schedule(static, 50000)
    for (int32_t i = 0; i < num_orders; i++) {
        if (orders_orderdate[i] < order_date_threshold && customer_set.count(orders_custkey[i])) {
            int tid = omp_get_thread_num();
            local_orders[tid].push_back({
                orders_orderkey[i],
                orders_custkey[i],
                orders_orderdate[i],
                orders_shippriority[i]
            });
        }
    }

    // Merge thread-local order vectors
    std::vector<OrderRow> filtered_orders;
    size_t total_orders = 0;
    for (const auto& local : local_orders) {
        total_orders += local.size();
    }
    filtered_orders.reserve(total_orders);
    for (const auto& local : local_orders) {
        filtered_orders.insert(filtered_orders.end(), local.begin(), local.end());
    }

    std::cout << "[INFO] Filtered orders (orderdate < 9204 AND in BUILDING customers): " << filtered_orders.size() << " rows" << std::endl;

#ifdef GENDB_PROFILE
    auto t_filter_orders_end = std::chrono::high_resolution_clock::now();
    double ms_filter_orders = std::chrono::duration<double, std::milli>(t_filter_orders_end - t_filter_orders_start).count();
    printf("[TIMING] filter_orders: %.2f ms\n", ms_filter_orders);
#endif

    // ===== BUILD ORDER KEY LOOKUP FOR LINEITEM JOIN =====
#ifdef GENDB_PROFILE
    auto t_build_order_start = std::chrono::high_resolution_clock::now();
#endif

    struct OrderInfo {
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    // Build a map of o_orderkey -> OrderInfo from filtered_orders for quick lookup during lineitem join
    // Use compact hash table with open addressing for better performance
    CompactHashTable<int32_t, OrderInfo> order_map(filtered_orders.size());
    for (const auto& ord : filtered_orders) {
        order_map.insert(ord.o_orderkey, {ord.o_orderdate, ord.o_shippriority});
    }

#ifdef GENDB_PROFILE
    auto t_build_order_end = std::chrono::high_resolution_clock::now();
    double ms_build_order = std::chrono::duration<double, std::milli>(t_build_order_end - t_build_order_start).count();
    printf("[TIMING] build_order_map: %.2f ms\n", ms_build_order);
#endif

    // ===== FILTER LINEITEM BY SHIPDATE > 1995-03-15 (9204 days) AND JOIN WITH ORDERS =====
#ifdef GENDB_PROFILE
    auto t_filter_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t shipdate_threshold = 9204;  // 1995-03-15

    struct LineitemOrderJoin {
        int32_t l_orderkey;
        int64_t l_revenue;  // extendedprice * (100 - discount), NOT divided by 100 yet, keeping full precision
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    // Load zone map for l_shipdate to prune blocks
    std::vector<ZoneMapEntry> shipdate_zones = load_zonemap(gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin");

    // Parallel scan and join with thread-local buffers
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<LineitemOrderJoin>> local_results(num_threads);

    // If zone map is available, process blocks in parallel; otherwise scan all rows
    if (!shipdate_zones.empty()) {
        // Process each zone (block) in parallel if its range overlaps with the predicate
#pragma omp parallel for schedule(dynamic, 1)
        for (size_t z = 0; z < shipdate_zones.size(); z++) {
            const ZoneMapEntry& zone = shipdate_zones[z];

            // Skip blocks where max_val <= threshold (all rows violate l_shipdate > threshold)
            if (zone.max_val <= shipdate_threshold) {
                continue;
            }

            int tid = omp_get_thread_num();

            // Process rows in this zone
            for (uint32_t i = zone.start_row; i < zone.end_row; i++) {
                if (lineitem_shipdate[i] > shipdate_threshold) {
                    OrderInfo* order_info = order_map.find(lineitem_orderkey[i]);
                    if (order_info != nullptr) {
                        int64_t discount_complement = 100 - lineitem_discount[i];
                        int64_t revenue = lineitem_extendedprice[i] * discount_complement;

                        local_results[tid].push_back({
                            lineitem_orderkey[i],
                            revenue,
                            order_info->o_orderdate,
                            order_info->o_shippriority
                        });
                    }
                }
            }
        }
    } else {
        // Fallback: scan all rows without zone map
#pragma omp parallel for schedule(static, 100000)
        for (int32_t i = 0; i < num_lineitem; i++) {
            if (lineitem_shipdate[i] > shipdate_threshold) {
                OrderInfo* order_info = order_map.find(lineitem_orderkey[i]);
                if (order_info != nullptr) {
                    int64_t discount_complement = 100 - lineitem_discount[i];
                    int64_t revenue = lineitem_extendedprice[i] * discount_complement;

                    int tid = omp_get_thread_num();
                    local_results[tid].push_back({
                        lineitem_orderkey[i],
                        revenue,
                        order_info->o_orderdate,
                        order_info->o_shippriority
                    });
                }
            }
        }
    }

    // Merge thread-local results
    std::vector<LineitemOrderJoin> lineitem_join_results;
    size_t total_rows = 0;
    for (const auto& local : local_results) {
        total_rows += local.size();
    }
    lineitem_join_results.reserve(total_rows);
    for (const auto& local : local_results) {
        lineitem_join_results.insert(lineitem_join_results.end(), local.begin(), local.end());
    }

    std::cout << "[INFO] Lineitem joined with orders (shipdate > 9204): " << lineitem_join_results.size() << " rows" << std::endl;

#ifdef GENDB_PROFILE
    auto t_filter_lineitem_end = std::chrono::high_resolution_clock::now();
    double ms_filter_lineitem = std::chrono::duration<double, std::milli>(t_filter_lineitem_end - t_filter_lineitem_start).count();
    printf("[TIMING] filter_lineitem_join: %.2f ms\n", ms_filter_lineitem);
#endif

    // ===== GROUP BY AND AGGREGATE =====
#ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
#endif

    struct GroupKey {
        int32_t l_orderkey;
        int32_t o_orderdate;
        int32_t o_shippriority;

        bool operator==(const GroupKey& other) const {
            return l_orderkey == other.l_orderkey && o_orderdate == other.o_orderdate && o_shippriority == other.o_shippriority;
        }
    };

    struct GroupKeyHash {
        size_t operator()(const GroupKey& k) const {
            return std::hash<int64_t>()(((int64_t)k.l_orderkey << 40) | ((int64_t)k.o_orderdate << 20) | k.o_shippriority);
        }
    };

    // Parallel aggregation with thread-local hash maps
    int agg_num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<GroupKey, int64_t, GroupKeyHash>> local_agg(agg_num_threads);
    for (auto& m : local_agg) {
        m.reserve(10000);  // Conservative estimate per thread
    }

#pragma omp parallel for schedule(static)
    for (size_t i = 0; i < lineitem_join_results.size(); i++) {
        const auto& row = lineitem_join_results[i];
        GroupKey key = {row.l_orderkey, row.o_orderdate, row.o_shippriority};
        int tid = omp_get_thread_num();
        local_agg[tid][key] += row.l_revenue;
    }

    // Merge thread-local aggregations
    std::unordered_map<GroupKey, int64_t, GroupKeyHash> agg_map;
    agg_map.reserve(100000);
    for (const auto& local_map : local_agg) {
        for (const auto& [key, revenue] : local_map) {
            agg_map[key] += revenue;
        }
    }

    std::cout << "[INFO] Aggregation groups: " << agg_map.size() << std::endl;

#ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double ms_agg = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", ms_agg);
#endif

    // ===== SORT BY REVENUE DESC, O_ORDERDATE ASC =====
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<AggResult> results;
    for (const auto& [key, revenue_sum] : agg_map) {
        results.push_back({
            key.l_orderkey,
            revenue_sum,
            key.o_orderdate,
            key.o_shippriority
        });
    }

    // Sort: revenue DESC, o_orderdate ASC
    std::sort(results.begin(), results.end(), [](const AggResult& a, const AggResult& b) {
        if (a.revenue_sum != b.revenue_sum) {
            return a.revenue_sum > b.revenue_sum;  // DESC
        }
        return a.o_orderdate < b.o_orderdate;  // ASC
    });

    // Take top 10
    if (results.size() > 10) {
        results.resize(10);
    }

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms_sort);
#endif

    // ===== OUTPUT RESULTS =====
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q3.csv";
    std::ofstream out(output_file);
    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    for (const auto& res : results) {
        out << res.l_orderkey << ","
            << std::fixed << std::setprecision(4) << res.get_revenue() << ","
            << format_date(res.o_orderdate) << ","
            << res.o_shippriority << "\n";  // Already has fixed and setprecision(4) from previous output
    }
    out.close();

    std::cout << "[INFO] Results written to " << output_file << std::endl;

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total - ms_output);  // Exclude output time
#endif
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
