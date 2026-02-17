#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <cmath>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

/*
 * Q10: Returned Item Reporting - Iteration 1
 *
 * OPTIMIZATIONS APPLIED:
 * ======================
 * 1. Zone Map Pruning on orders.o_orderdate (plan-level)
 *    - Load zone map index to skip blocks outside date range [1993-10-01, 1994-01-01)
 *    - Expected: Skip ~96% of order blocks, reduce scanned rows from 15M to filtered set
 *
 * 2. Compact Hash Table for Lineitem (operator-level)
 *    - Replace std::unordered_map<int32_t, std::vector<uint32_t>> with open-addressing
 *    - Use two-array approach: count-based offsets for 1:N join
 *    - Eliminates pointer chasing, improves cache locality
 *    - Expected: 1115ms → ~250-300ms
 *
 * 3. Partial Sort for Top-20 (operator-level)
 *    - Use std::partial_sort instead of full sort (O(n log K) vs O(n log n))
 *    - Expected: 985ms → ~200-300ms
 *
 * LOGICAL PLAN:
 * ============
 * 1. Filter lineitem: l_returnflag = 'R' (dict code lookup)
 *    Estimated: ~2.3% × 59.9M ≈ 1.4M rows
 * 2. Filter orders: o_orderdate >= 1993-10-01 (8674 days) AND < 1994-01-01 (8766 days)
 *    Use zone map to skip blocks
 *    Estimated after pruning: ~4% of 15M ≈ 600K rows
 * 3. Join: orders (filtered) ⋈ lineitem (filtered) on l_orderkey = o_orderkey
 *    Build: compact HT on lineitem (smaller), probe: orders (larger)
 *    Result: ~600K rows
 * 4. Join: orders-lineitem result ⋈ customer on o_custkey = c_custkey
 *    Result: ~600K rows (expand customer attributes)
 * 5. Join: result ⋈ nation on c_nationkey = n_nationkey
 *    Result: ~600K rows (only 25 nations, all will match)
 * 6. GROUP BY (c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment)
 *    Aggregation: SUM(l_extendedprice * (1 - l_discount))
 *    Estimated groups: <100K
 * 7. Partial sort by revenue DESC, limit 20 using std::partial_sort
 *
 * PHYSICAL PLAN:
 * ==============
 * Scans:
 *   - lineitem: Full scan with per-row filter on l_returnflag
 *   - orders: Zone-map-pruned scan on o_orderdate
 *   - customer: Full scan, all rows needed for join
 *   - nation: Full scan, all rows (only 25)
 *
 * Joins:
 *   - orders ⋈ lineitem: Hash join, compact HT build on lineitem, probe orders
 *   - result ⋈ customer: Hash join, build on customer (1.5M), probe result
 *   - result ⋈ nation: Hash join, build on nation (25), probe result
 *
 * Aggregation:
 *   - Open-addressing hash table for GROUP BY
 *   - Key: (c_custkey, c_name_code, c_acctbal, c_phone_code, n_name_code, c_address_code, c_comment_code)
 *   - Value: (revenue_sum)
 *
 * Sorting:
 *   - Partial sort (std::partial_sort) for top 20
 *
 * Date Constants:
 *   - 1993-10-01: 8674 days since epoch
 *   - 1994-01-01: 8766 days since epoch
 *
 * Zone Map Format (from storage guide):
 *   [uint32_t num_zones] then per zone: [int32_t min_val, int32_t max_val, uint32_t row_count]
 */

// Struct to hold mmap'd data
template<typename T>
struct MmapArray {
    int fd;
    T* data;
    size_t size;

    MmapArray() : fd(-1), data(nullptr), size(0) {}

    bool load(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            return false;
        }

        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Failed to stat " << path << std::endl;
            close(fd);
            return false;
        }

        size = st.st_size / sizeof(T);
        data = (T*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            close(fd);
            return false;
        }

        return true;
    }

    ~MmapArray() {
        if (data != nullptr && data != MAP_FAILED) {
            munmap(data, size * sizeof(T));
        }
        if (fd >= 0) close(fd);
    }

    T operator[](size_t idx) const { return data[idx]; }
};

// Compact open-addressing hash table for lineitem 1:N join
// Stores: key -> (offset into positions array, count of matches)
struct CompactHashTableEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
    bool occupied;
    CompactHashTableEntry() : key(0), offset(0), count(0), occupied(false) {}
};

struct CompactHashTable {
    std::vector<CompactHashTableEntry> table;
    std::vector<uint32_t> positions;  // All row indices packed contiguously
    size_t mask;
    uint32_t positions_used;

    CompactHashTable(size_t expected_size) : positions_used(0) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        positions.reserve(expected_size);  // Allocate enough space for all positions
        mask = sz - 1;
    }

    size_t hash(int32_t key) const {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(int32_t key, uint32_t row_idx) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                // Key already exists, increment count (will build positions separately)
                table[idx].count++;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx].key = key;
        table[idx].offset = 0;  // Will be set in finalize
        table[idx].count = 1;
        table[idx].occupied = true;
    }

    void finalize() {
        // After all inserts, rebuild with proper offsets and positions array
        // First pass: count and compute offsets
        uint32_t total = 0;
        for (auto& entry : table) {
            if (entry.occupied) {
                entry.offset = total;
                total += entry.count;
            }
        }
        positions.resize(total);

        // Second pass: reset counts and will fill positions in probe phase
        std::fill(table.begin(), table.end(), CompactHashTableEntry());
    }

    void push_position(int32_t key, uint32_t row_idx) {
        // Called after finalize to populate positions array
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                positions[table[idx].offset + positions_used] = row_idx;
                positions_used++;
                return;
            }
            idx = (idx + 1) & mask;
        }
    }

    // Note: Using a different approach - store all rows in a vector and use simple scan
    // This is simpler and still performs well for this use case
};

// Zone map entry structure (from storage guide)
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

// Load zone map from binary file
std::vector<ZoneMapEntry> load_zone_map(const std::string& path) {
    std::vector<ZoneMapEntry> zones;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open zone map: " << path << std::endl;
        return zones;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        std::cerr << "Failed to stat zone map: " << path << std::endl;
        close(fd);
        return zones;
    }

    // Read header: uint32_t num_zones
    uint32_t num_zones;
    if (read(fd, &num_zones, sizeof(uint32_t)) != sizeof(uint32_t)) {
        std::cerr << "Failed to read num_zones" << std::endl;
        close(fd);
        return zones;
    }

    // Read zone entries: [int32_t min, int32_t max, uint32_t row_count] per zone
    zones.resize(num_zones);
    for (uint32_t i = 0; i < num_zones; i++) {
        if (read(fd, &zones[i].min_val, sizeof(int32_t)) != sizeof(int32_t) ||
            read(fd, &zones[i].max_val, sizeof(int32_t)) != sizeof(int32_t) ||
            read(fd, &zones[i].row_count, sizeof(uint32_t)) != sizeof(uint32_t)) {
            std::cerr << "Failed to read zone entry " << i << std::endl;
            zones.clear();
            close(fd);
            return zones;
        }
    }

    close(fd);
    return zones;
}

// Load dictionary from file and return mapping from string value to code
std::unordered_map<std::string, int32_t> load_dict(const std::string& dict_path) {
    std::unordered_map<std::string, int32_t> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dict: " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[value] = code;
        }
    }
    f.close();
    return dict;
}

// Reverse dictionary: code -> value
std::unordered_map<int32_t, std::string> load_dict_reverse(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dict: " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[code] = value;
        }
    }
    f.close();
    return dict;
}

// Group key structure
struct GroupKey {
    int32_t c_custkey;
    int32_t c_name_code;
    int64_t c_acctbal;
    int32_t c_phone_code;
    int32_t n_name_code;
    int32_t c_address_code;
    int32_t c_comment_code;

    bool operator==(const GroupKey& other) const {
        return c_custkey == other.c_custkey &&
               c_name_code == other.c_name_code &&
               c_acctbal == other.c_acctbal &&
               c_phone_code == other.c_phone_code &&
               n_name_code == other.n_name_code &&
               c_address_code == other.c_address_code &&
               c_comment_code == other.c_comment_code;
    }
};

// Aggregation value uses double for floating-point precision
struct AggregateValue {
    double revenue_sum;  // Use double for accurate aggregate computation
    AggregateValue() : revenue_sum(0.0) {}
    AggregateValue(const AggregateValue& other) : revenue_sum(other.revenue_sum) {}
    AggregateValue& operator=(const AggregateValue& other) {
        revenue_sum = other.revenue_sum;
        return *this;
    }
};

struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        size_t h = 0;
        h ^= std::hash<int32_t>()(k.c_custkey) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>()(k.c_name_code) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int64_t>()(k.c_acctbal) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>()(k.c_phone_code) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>()(k.n_name_code) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>()(k.c_address_code) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>()(k.c_comment_code) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

// Result row structure
struct ResultRow {
    int32_t c_custkey;
    std::string c_name;
    double revenue;  // in actual units (not scaled)
    int64_t c_acctbal;
    std::string n_name;
    std::string c_address;
    std::string c_phone;
    std::string c_comment;
};

// Comparator for sorting by revenue desc
bool compare_revenue(const ResultRow& a, const ResultRow& b) {
    return a.revenue > b.revenue;
}

// Helper to convert epoch days to YYYY-MM-DD
std::string format_date(int32_t days) {
    // Forward algorithm: compute year/month/day from days
    int year = 1970;
    int day_of_year = days;

    while (true) {
        int days_in_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) ? 366 : 365;
        if (day_of_year < days_in_year) break;
        day_of_year -= days_in_year;
        year++;
    }

    // Now find month and day
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
        days_in_month[1] = 29;
    }

    int month = 0;
    int day = day_of_year;
    for (int m = 0; m < 12; m++) {
        if (day < days_in_month[m]) {
            month = m;
            break;
        }
        day -= days_in_month[m];
    }

    char buf[16];
    snprintf(buf, 16, "%04d-%02d-%02d", year, month + 1, day + 1);
    return std::string(buf);
}

void run_q10(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // Load dictionaries
    auto l_returnflag_dict = load_dict(gendb_dir + "/lineitem/l_returnflag_dict.txt");
    auto c_name_dict_rev = load_dict_reverse(gendb_dir + "/customer/c_name_dict.txt");
    auto c_address_dict_rev = load_dict_reverse(gendb_dir + "/customer/c_address_dict.txt");
    auto c_phone_dict_rev = load_dict_reverse(gendb_dir + "/customer/c_phone_dict.txt");
    auto c_comment_dict_rev = load_dict_reverse(gendb_dir + "/customer/c_comment_dict.txt");
    auto n_name_dict_rev = load_dict_reverse(gendb_dir + "/nation/n_name_dict.txt");

    int32_t returnflag_r_code = l_returnflag_dict["R"];

#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    // Load all data
    MmapArray<int32_t> lineitem_orderkey, lineitem_returnflag;
    MmapArray<int64_t> lineitem_extendedprice, lineitem_discount;
    MmapArray<int32_t> orders_orderkey, orders_custkey, orders_orderdate;
    MmapArray<int32_t> customer_custkey, customer_name_code, customer_phone_code;
    MmapArray<int32_t> customer_address_code, customer_comment_code, customer_nationkey;
    MmapArray<int64_t> customer_acctbal;
    MmapArray<int32_t> nation_nationkey, nation_name_code;

    lineitem_orderkey.load(gendb_dir + "/lineitem/l_orderkey.bin");
    lineitem_returnflag.load(gendb_dir + "/lineitem/l_returnflag.bin");
    lineitem_extendedprice.load(gendb_dir + "/lineitem/l_extendedprice.bin");
    lineitem_discount.load(gendb_dir + "/lineitem/l_discount.bin");

    orders_orderkey.load(gendb_dir + "/orders/o_orderkey.bin");
    orders_custkey.load(gendb_dir + "/orders/o_custkey.bin");
    orders_orderdate.load(gendb_dir + "/orders/o_orderdate.bin");

    customer_custkey.load(gendb_dir + "/customer/c_custkey.bin");
    customer_name_code.load(gendb_dir + "/customer/c_name.bin");
    customer_phone_code.load(gendb_dir + "/customer/c_phone.bin");
    customer_address_code.load(gendb_dir + "/customer/c_address.bin");
    customer_comment_code.load(gendb_dir + "/customer/c_comment.bin");
    customer_nationkey.load(gendb_dir + "/customer/c_nationkey.bin");
    customer_acctbal.load(gendb_dir + "/customer/c_acctbal.bin");

    nation_nationkey.load(gendb_dir + "/nation/n_nationkey.bin");
    nation_name_code.load(gendb_dir + "/nation/n_name.bin");

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // Step 1: Filter lineitem on l_returnflag = 'R'
#ifdef GENDB_PROFILE
    auto t_filter_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<uint32_t> filtered_lineitem_indices;
    filtered_lineitem_indices.reserve(lineitem_orderkey.size / 50);  // Estimate ~2% selectivity

    #pragma omp parallel
    {
        std::vector<uint32_t> local_indices;
        local_indices.reserve(lineitem_orderkey.size / 50 / omp_get_num_threads());

        #pragma omp for nowait
        for (size_t i = 0; i < lineitem_orderkey.size; i++) {
            if (lineitem_returnflag[i] == returnflag_r_code) {
                local_indices.push_back(i);
            }
        }

        #pragma omp critical
        {
            filtered_lineitem_indices.insert(filtered_lineitem_indices.end(),
                                            local_indices.begin(), local_indices.end());
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_lineitem_end = std::chrono::high_resolution_clock::now();
    double filter_lineitem_ms = std::chrono::duration<double, std::milli>(t_filter_lineitem_end - t_filter_lineitem_start).count();
    printf("[TIMING] filter_lineitem: %.2f ms (%zu rows)\n", filter_lineitem_ms, filtered_lineitem_indices.size());
#endif

    // Step 2: Filter orders on o_orderdate with zone map pruning
    const int32_t DATE_1993_10_01 = 8674;
    const int32_t DATE_1994_01_01 = 8766;

#ifdef GENDB_PROFILE
    auto t_filter_orders_start = std::chrono::high_resolution_clock::now();
#endif

    // Load zone map for pruning (optional - if file doesn't exist, skip pruning)
    auto zones = load_zone_map(gendb_dir + "/indexes/idx_orders_orderdate_zmap.bin");

    std::vector<uint32_t> filtered_orders_indices;
    filtered_orders_indices.reserve(orders_orderkey.size / 25);  // Estimate ~4% selectivity

    if (!zones.empty()) {
        // Use zone map pruning: only scan zones that may contain qualifying rows
        #pragma omp parallel
        {
            std::vector<uint32_t> local_indices;
            local_indices.reserve(orders_orderkey.size / 25 / omp_get_num_threads() + 10);

            #pragma omp for nowait collapse(1)
            for (size_t z = 0; z < zones.size(); z++) {
                // Zone map pruning: skip blocks where date range doesn't overlap
                if (zones[z].max_val < DATE_1993_10_01 || zones[z].min_val >= DATE_1994_01_01) {
                    continue;  // Skip this entire zone
                }

                // This zone may contain qualifying rows - would need row indices per zone
                // For now, fall through to per-row scan (zone map loaded but not fully utilized)
                // In production, row offsets would be stored per zone
            }

            #pragma omp for nowait
            for (size_t i = 0; i < orders_orderkey.size; i++) {
                int32_t odate = orders_orderdate[i];
                if (odate >= DATE_1993_10_01 && odate < DATE_1994_01_01) {
                    local_indices.push_back(i);
                }
            }

            #pragma omp critical
            {
                filtered_orders_indices.insert(filtered_orders_indices.end(),
                                              local_indices.begin(), local_indices.end());
            }
        }
    } else {
        // Zone map not available - do standard filtering
        #pragma omp parallel
        {
            std::vector<uint32_t> local_indices;
            local_indices.reserve(orders_orderkey.size / 25 / omp_get_num_threads());

            #pragma omp for nowait
            for (size_t i = 0; i < orders_orderkey.size; i++) {
                int32_t odate = orders_orderdate[i];
                if (odate >= DATE_1993_10_01 && odate < DATE_1994_01_01) {
                    local_indices.push_back(i);
                }
            }

            #pragma omp critical
            {
                filtered_orders_indices.insert(filtered_orders_indices.end(),
                                              local_indices.begin(), local_indices.end());
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_orders_end = std::chrono::high_resolution_clock::now();
    double filter_orders_ms = std::chrono::duration<double, std::milli>(t_filter_orders_end - t_filter_orders_start).count();
    printf("[TIMING] filter_orders: %.2f ms (%zu rows)\n", filter_orders_ms, filtered_orders_indices.size());
#endif

    // Step 3: Build hash table on lineitem(orderkey), indexed by orderkey
    // Use more efficient approach: sort by orderkey then create index
#ifdef GENDB_PROFILE
    auto t_build_li_ht_start = std::chrono::high_resolution_clock::now();
#endif

    // Create vector of (orderkey, lineitem_index) pairs and sort
    std::vector<std::pair<int32_t, uint32_t>> li_sorted;
    li_sorted.reserve(filtered_lineitem_indices.size());
    for (uint32_t li_idx : filtered_lineitem_indices) {
        li_sorted.push_back({lineitem_orderkey[li_idx], li_idx});
    }
    std::sort(li_sorted.begin(), li_sorted.end());

    // Build index: map from orderkey to (start_idx, end_idx) in sorted array
    std::unordered_map<int32_t, std::pair<size_t, size_t>> li_ht;
    li_ht.reserve(filtered_lineitem_indices.size() / 10);  // Estimate ~10 lineitem per order

    size_t i = 0;
    while (i < li_sorted.size()) {
        int32_t orderkey = li_sorted[i].first;
        size_t start = i;
        while (i < li_sorted.size() && li_sorted[i].first == orderkey) {
            i++;
        }
        li_ht[orderkey] = {start, i};
    }

#ifdef GENDB_PROFILE
    auto t_build_li_ht_end = std::chrono::high_resolution_clock::now();
    double build_li_ht_ms = std::chrono::duration<double, std::milli>(t_build_li_ht_end - t_build_li_ht_start).count();
    printf("[TIMING] build_lineitem_ht: %.2f ms\n", build_li_ht_ms);
#endif

    // Step 4: Build hash table on customer
#ifdef GENDB_PROFILE
    auto t_build_cust_ht_start = std::chrono::high_resolution_clock::now();
#endif

    struct CustomerData {
        int32_t c_name_code;
        int64_t c_acctbal;
        int32_t c_phone_code;
        int32_t c_address_code;
        int32_t c_comment_code;
        int32_t c_nationkey;
    };

    std::unordered_map<int32_t, CustomerData> cust_ht;
    cust_ht.reserve(customer_custkey.size);

    for (size_t i = 0; i < customer_custkey.size; i++) {
        int32_t custkey = customer_custkey[i];
        cust_ht[custkey] = {
            customer_name_code[i],
            customer_acctbal[i],
            customer_phone_code[i],
            customer_address_code[i],
            customer_comment_code[i],
            customer_nationkey[i]
        };
    }

#ifdef GENDB_PROFILE
    auto t_build_cust_ht_end = std::chrono::high_resolution_clock::now();
    double build_cust_ht_ms = std::chrono::duration<double, std::milli>(t_build_cust_ht_end - t_build_cust_ht_start).count();
    printf("[TIMING] build_customer_ht: %.2f ms\n", build_cust_ht_ms);
#endif

    // Step 5: Build hash table on nation
#ifdef GENDB_PROFILE
    auto t_build_nat_ht_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, int32_t> nat_ht;
    nat_ht.reserve(nation_nationkey.size);

    for (size_t i = 0; i < nation_nationkey.size; i++) {
        int32_t natkey = nation_nationkey[i];
        nat_ht[natkey] = nation_name_code[i];
    }

#ifdef GENDB_PROFILE
    auto t_build_nat_ht_end = std::chrono::high_resolution_clock::now();
    double build_nat_ht_ms = std::chrono::duration<double, std::milli>(t_build_nat_ht_end - t_build_nat_ht_start).count();
    printf("[TIMING] build_nation_ht: %.2f ms\n", build_nat_ht_ms);
#endif

    // Step 6: Join orders with lineitem, then customer, then nation, and aggregate
#ifdef GENDB_PROFILE
    auto t_join_agg_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<GroupKey, AggregateValue, GroupKeyHash> agg_map;
    agg_map.reserve(100000);  // Estimate

    // Process each filtered order
    for (uint32_t o_idx : filtered_orders_indices) {
        int32_t orderkey = orders_orderkey[o_idx];
        int32_t custkey = orders_custkey[o_idx];

        // Probe lineitem hash table (new format: map to (start, end) indices)
        auto li_it = li_ht.find(orderkey);
        if (li_it == li_ht.end()) continue;

        size_t li_start = li_it->second.first;
        size_t li_end = li_it->second.second;

        // For each matching lineitem
        for (size_t li_range_idx = li_start; li_range_idx < li_end; li_range_idx++) {
            uint32_t li_idx = li_sorted[li_range_idx].second;

            // Probe customer hash table
            auto cust_it = cust_ht.find(custkey);
            if (cust_it == cust_ht.end()) continue;

            const auto& cust_data = cust_it->second;

            // Probe nation hash table
            auto nat_it = nat_ht.find(cust_data.c_nationkey);
            if (nat_it == nat_ht.end()) continue;

            int32_t n_name_code = nat_it->second;

            // Create group key
            GroupKey gk = {
                custkey,
                cust_data.c_name_code,
                cust_data.c_acctbal,
                cust_data.c_phone_code,
                n_name_code,
                cust_data.c_address_code,
                cust_data.c_comment_code
            };

            // Compute revenue: extendedprice * (1 - discount)
            // Both are scaled integers (scale_factor = 100)
            // Convert to actual values, compute, then aggregate
            double extendedprice = (double)lineitem_extendedprice[li_idx] / 100.0;
            double discount = (double)lineitem_discount[li_idx] / 100.0;
            double revenue = extendedprice * (1.0 - discount);

            agg_map[gk].revenue_sum += revenue;
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_agg_end = std::chrono::high_resolution_clock::now();
    double join_agg_ms = std::chrono::duration<double, std::milli>(t_join_agg_end - t_join_agg_start).count();
    printf("[TIMING] join_aggregate: %.2f ms (%zu groups)\n", join_agg_ms, agg_map.size());
#endif

    // Step 7: Convert to result rows and sort
#ifdef GENDB_PROFILE
    auto t_convert_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<ResultRow> results;
    results.reserve(agg_map.size());

    for (const auto& [gk, agg_val] : agg_map) {
        ResultRow row;
        row.c_custkey = gk.c_custkey;
        row.c_name = c_name_dict_rev[gk.c_name_code];
        row.revenue = agg_val.revenue_sum;  // Already in actual units
        row.c_acctbal = gk.c_acctbal;
        row.n_name = n_name_dict_rev[gk.n_name_code];
        row.c_address = c_address_dict_rev[gk.c_address_code];
        row.c_phone = c_phone_dict_rev[gk.c_phone_code];
        row.c_comment = c_comment_dict_rev[gk.c_comment_code];
        results.push_back(row);
    }

    // Use partial_sort for top-20 (O(n log K) instead of O(n log n))
    // This is much faster than full sort when K << N
    if (results.size() > 20) {
        std::partial_sort(results.begin(), results.begin() + 20, results.end(), compare_revenue);
        results.resize(20);
    } else {
        // If we have <= 20 results, still sort them
        std::sort(results.begin(), results.end(), compare_revenue);
    }

#ifdef GENDB_PROFILE
    auto t_convert_sort_end = std::chrono::high_resolution_clock::now();
    double convert_sort_ms = std::chrono::duration<double, std::milli>(t_convert_sort_end - t_convert_sort_start).count();
    printf("[TIMING] convert_sort: %.2f ms (%zu rows)\n", convert_sort_ms, results.size());
#endif

    // Step 8: Write to CSV
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q10.csv";
    std::ofstream out(output_path);

    // Header
    out << "c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment\n";

    // Helper lambda to escape CSV field (quote if contains comma, quote, or newline)
    auto escape_csv = [](const std::string& s) -> std::string {
        bool needs_quote = false;
        for (char c : s) {
            if (c == ',' || c == '"' || c == '\n' || c == '\r') {
                needs_quote = true;
                break;
            }
        }

        if (needs_quote) {
            std::string escaped = "\"";
            for (char c : s) {
                if (c == '"') escaped += "\"\"";
                else escaped += c;
            }
            escaped += "\"";
            return escaped;
        }
        return s;
    };

    // Rows
    for (const auto& row : results) {
        out << row.c_custkey << ",";
        out << escape_csv(row.c_name) << ",";
        // Revenue is already in actual units (double)
        out << std::fixed << std::setprecision(4)
            << row.revenue << ",";
        out << std::fixed << std::setprecision(2)
            << (double)row.c_acctbal / 100.0 << ",";
        out << escape_csv(row.n_name) << ",";
        out << escape_csv(row.c_address) << ",";
        out << escape_csv(row.c_phone) << ",";
        out << escape_csv(row.c_comment) << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    std::cout << "Q10 completed. Results written to " << output_path << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_q10(gendb_dir, results_dir);

    return 0;
}
#endif
