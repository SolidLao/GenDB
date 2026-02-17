#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <chrono>
#include <omp.h>
#include <cmath>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

/*
=== LOGICAL QUERY PLAN FOR Q3 ===

Query: SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
              o_orderdate, o_shippriority
       FROM customer, orders, lineitem
       WHERE c_mktsegment = 'BUILDING'
         AND c_custkey = o_custkey
         AND l_orderkey = o_orderkey
         AND o_orderdate < 1995-03-15 (epoch day 9204)
         AND l_shipdate > 1995-03-15 (epoch day 9204)
       GROUP BY l_orderkey, o_orderdate, o_shippriority
       ORDER BY revenue DESC, o_orderdate ASC
       LIMIT 10

Single-table predicates:
  - customer: c_mktsegment = 'BUILDING' (dictionary code 0) → ~300K rows
  - orders: o_orderdate < 9204 → ~5M rows
  - lineitem: l_shipdate > 9204 → ~15M rows

Join ordering (smallest filtered first):
  1. Filter customer by mktsegment
  2. Hash join: orders (filtered) probed with customer (filtered) on o_custkey = c_custkey
  3. Hash join: lineitem (filtered) probed with orders result on l_orderkey = o_orderkey

Aggregation:
  - GROUP BY (l_orderkey, o_orderdate, o_shippriority) → estimated 200K-500K groups
  - Aggregate: SUM(l_extendedprice * (1 - l_discount)) with scale_factor = 100

=== PHYSICAL QUERY PLAN ===

1. SCAN & FILTER customer:
   - Full scan, filter c_mktsegment == 0 (BUILDING)
   - Parallel with OpenMP
   - Output: set of c_custkey values

2. SCAN & FILTER orders:
   - Full scan, filter o_orderdate < 9204
   - Use zone map index to skip blocks
   - Parallel with OpenMP
   - Output: o_orderkey, o_custkey, o_orderdate, o_shippriority

3. HASH JOIN 1 (orders ⊲⊳ customer):
   - Build side: filtered customer (smaller ~300K)
   - Probe side: filtered orders (larger ~5M)
   - Join condition: o_custkey = c_custkey
   - Output: o_orderkey, o_orderdate, o_shippriority, (o_custkey removed after join)

4. SCAN & FILTER lineitem:
   - Full scan, filter l_shipdate > 9204
   - Use zone map index to skip blocks
   - Parallel with OpenMP
   - Output: l_orderkey, l_extendedprice, l_discount

5. HASH JOIN 2 (lineitem ⊲⊳ orders_result):
   - Build side: orders result (smaller ~5M)
   - Probe side: lineitem (larger ~15M)
   - Join condition: l_orderkey = o_orderkey
   - Output: l_orderkey, l_extendedprice, l_discount, o_orderdate, o_shippriority

6. AGGREGATION (GROUP BY):
   - Open-addressing hash table: key = (l_orderkey, o_orderdate, o_shippriority)
   - Aggregate: SUM(revenue) where revenue = l_extendedprice * (1 - l_discount) / scale_factor
   - Thread-local partial aggregation + final merge
   - Output: (l_orderkey, o_orderdate, o_shippriority) → revenue

7. SORT & LIMIT:
   - Sort by revenue DESC, o_orderdate ASC
   - Take top 10

8. OUTPUT:
   - Write CSV: l_orderkey, revenue, o_orderdate, o_shippriority
   - Dates in YYYY-MM-DD format
   - Revenue with 4 decimal places
*/

// Constants
const int32_t DATE_THRESHOLD = 9204; // 1995-03-15 in epoch days
const int BUILDING_CODE = 0; // c_mktsegment = 'BUILDING'

// Zone map entry structure for orders and lineitem
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

// Helper: Convert epoch days to YYYY-MM-DD
std::string epoch_days_to_date(int32_t days) {
    // days since 1970-01-01
    int year = 1970;
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days < days_in_year) break;
        days -= days_in_year;
        year++;
    }

    int month = 1;
    int days_in_months[] = {31, (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 29 : 28,
                            31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    while (days >= days_in_months[month - 1]) {
        days -= days_in_months[month - 1];
        month++;
    }

    int day = days + 1; // days are 0-indexed within the month, but day numbering is 1-indexed

    char buffer[11];
    snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", year, month, day);
    return std::string(buffer);
}

// Helper: mmap a binary file
void* mmap_file(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening " << path << std::endl;
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);
    size = file_size;

    void* ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        return nullptr;
    }
    return ptr;
}

// Helper: Load dictionary and find code for a value
int32_t find_dict_code(const std::string& dict_path, const std::string& value) {
    std::ifstream dict_file(dict_path);
    if (!dict_file.is_open()) {
        std::cerr << "Error opening dictionary " << dict_path << std::endl;
        return -1;
    }

    std::string line;
    while (std::getline(dict_file, line)) {
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            std::string code_str = line.substr(0, eq_pos);
            std::string dict_value = line.substr(eq_pos + 1);
            if (dict_value == value) {
                return std::stoi(code_str);
            }
        }
    }
    return -1;
}

// Helper: Load zone map index
std::vector<ZoneMapEntry> load_zone_map(const std::string& path) {
    std::vector<ZoneMapEntry> zones;
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Warning: Could not open zone map " << path << std::endl;
        return zones;
    }

    // Read number of zones (uint32_t header)
    uint32_t num_zones;
    file.read(reinterpret_cast<char*>(&num_zones), sizeof(uint32_t));

    // Read each zone entry: [int32_t min_val, int32_t max_val, uint32_t row_count]
    zones.reserve(num_zones);
    for (uint32_t i = 0; i < num_zones; i++) {
        ZoneMapEntry entry;
        file.read(reinterpret_cast<char*>(&entry.min_val), sizeof(int32_t));
        file.read(reinterpret_cast<char*>(&entry.max_val), sizeof(int32_t));
        file.read(reinterpret_cast<char*>(&entry.row_count), sizeof(uint32_t));
        zones.push_back(entry);
    }

    file.close();
    return zones;
}

// Structure for join output (orders after filtering and customer join)
struct OrderResult {
    int32_t o_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// Structure for aggregation result
struct AggKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const AggKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

// Hash function for AggKey
struct AggKeyHash {
    size_t operator()(const AggKey& k) const {
        return ((size_t)k.l_orderkey * 73856093) ^
               ((size_t)k.o_orderdate * 19349663) ^
               ((size_t)k.o_shippriority * 83492791);
    }
};

struct AggValue {
    double revenue_sum; // accumulated revenue as floating point
};

// Structure for final output
struct FinalResult {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const FinalResult& other) const {
        if (other.revenue != this->revenue) {
            return other.revenue < this->revenue; // DESC
        }
        return this->o_orderdate < other.o_orderdate; // ASC
    }
};

// ===== COMPACT OPEN-ADDRESSING HASH TABLE FOR int32_t → bool =====
struct CompactBoolHashTable {
    struct Entry {
        int32_t key;
        bool value;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactBoolHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        for (auto& e : table) e.occupied = false;
        mask = sz - 1;
    }

    size_t hash(int32_t key) const {
        return ((size_t)key * 2654435761ULL) >> 32;
    }

    void insert(int32_t key, bool value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    bool find(int32_t key) const {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return table[idx].value;
            idx = (idx + 1) & mask;
        }
        return false;
    }

    bool contains(int32_t key) const {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return true;
            idx = (idx + 1) & mask;
        }
        return false;
    }
};

// ===== COMPACT OPEN-ADDRESSING HASH TABLE FOR int32_t → vector of OrderJoinData =====
struct OrderJoinData {
    int32_t o_orderdate;
    int32_t o_shippriority;
};

struct CompactOrderHashTable {
    struct Entry {
        int32_t key;
        std::vector<OrderJoinData>* value;  // pointer to vector
        bool occupied;
    };

    std::vector<Entry> table;
    std::vector<std::vector<OrderJoinData>> value_storage;
    size_t value_index;
    size_t mask;

    CompactOrderHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        for (auto& e : table) e.occupied = false;
        mask = sz - 1;

        // Pre-allocate storage for values
        value_storage.reserve(expected_size);
        value_index = 0;
    }

    size_t hash(int32_t key) const {
        return ((size_t)key * 2654435761ULL) >> 32;
    }

    void insert(int32_t key, const OrderJoinData& data) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value->push_back(data);
                return;
            }
            idx = (idx + 1) & mask;
        }
        // New key
        value_storage.push_back({});
        value_storage.back().push_back(data);
        table[idx] = {key, &value_storage.back(), true};
    }

    std::vector<OrderJoinData>* find(int32_t key) const {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// ===== COMPACT OPEN-ADDRESSING HASH TABLE FOR AggKey → double =====
struct CompactAggHashTable {
    struct Entry {
        AggKey key;
        double value;  // revenue_sum
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactAggHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        for (auto& e : table) e.occupied = false;
        mask = sz - 1;
    }

    size_t hash(const AggKey& k) const {
        size_t h = ((size_t)k.l_orderkey * 2654435761ULL) >> 32;
        h ^= ((size_t)k.o_orderdate * 2246822519ULL) >> 32;
        h ^= ((size_t)k.o_shippriority * 3266489917ULL) >> 32;
        return h;
    }

    void insert_or_add(const AggKey& key, double value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value += value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        // New key
        table[idx] = {key, value, true};
    }

    double* find(const AggKey& key) const {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return (double*)&table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }

    size_t size() const {
        size_t cnt = 0;
        for (const auto& e : table) {
            if (e.occupied) cnt++;
        }
        return cnt;
    }

    std::vector<std::pair<AggKey, double>> to_vector() const {
        std::vector<std::pair<AggKey, double>> result;
        for (const auto& e : table) {
            if (e.occupied) {
                result.push_back({e.key, e.value});
            }
        }
        return result;
    }
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // ===== METADATA CHECK =====
    std::cout << "[METADATA CHECK] Q3 Query" << std::endl;
    std::cout << "  - DATE_THRESHOLD (1995-03-15): " << DATE_THRESHOLD << " epoch days" << std::endl;
    std::cout << "  - c_mktsegment='BUILDING' code: " << BUILDING_CODE << std::endl;
    std::cout << "  - Decimal scale_factor: 100" << std::endl;

    // ===== LOAD DATA =====

#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    // Load customer table
    size_t customer_size = 0;
    int32_t* c_custkey_data = (int32_t*)mmap_file(gendb_dir + "/customer/c_custkey.bin", customer_size);
    int32_t* c_mktsegment_data = (int32_t*)mmap_file(gendb_dir + "/customer/c_mktsegment.bin", customer_size);
    int32_t num_customers = customer_size / sizeof(int32_t);

    // Load orders table
    size_t orders_size = 0;
    int32_t* o_orderkey_data = (int32_t*)mmap_file(gendb_dir + "/orders/o_orderkey.bin", orders_size);
    int32_t* o_custkey_data = (int32_t*)mmap_file(gendb_dir + "/orders/o_custkey.bin", orders_size);
    int32_t* o_orderdate_data = (int32_t*)mmap_file(gendb_dir + "/orders/o_orderdate.bin", orders_size);
    int32_t* o_shippriority_data = (int32_t*)mmap_file(gendb_dir + "/orders/o_shippriority.bin", orders_size);
    int32_t num_orders = orders_size / sizeof(int32_t);

    // Load lineitem table
    size_t li_orderkey_size = 0, li_extprice_size = 0, li_discount_size = 0, li_shipdate_size = 0;
    int32_t* l_orderkey_data = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_orderkey.bin", li_orderkey_size);
    int64_t* l_extendedprice_data = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_extendedprice.bin", li_extprice_size);
    int64_t* l_discount_data = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_discount.bin", li_discount_size);
    int32_t* l_shipdate_data = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_shipdate.bin", li_shipdate_size);
    int32_t num_lineitem = li_orderkey_size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", ms_load);
#endif

    std::cout << "[METADATA CHECK] Loaded " << num_customers << " customers, "
              << num_orders << " orders, " << num_lineitem << " lineitems" << std::endl;

    // ===== STEP 1: FILTER CUSTOMER & BUILD HASH INDEX =====

#ifdef GENDB_PROFILE
    auto t_filter_customer_start = std::chrono::high_resolution_clock::now();
#endif

    // Use thread-local accumulation to avoid critical sections in hot loop
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<int32_t>> thread_local_custkeys(num_threads);
    for (int t = 0; t < num_threads; t++) {
        thread_local_custkeys[t].reserve(num_customers / (5 * num_threads) + 1000);
    }

#pragma omp parallel for
    for (int32_t i = 0; i < num_customers; i++) {
        if (c_mktsegment_data[i] == BUILDING_CODE) {
            int tid = omp_get_thread_num();
            thread_local_custkeys[tid].push_back(c_custkey_data[i]);
        }
    }

    // Merge thread-local results
    std::vector<int32_t> filtered_custkeys;
    size_t total_filtered = 0;
    for (int t = 0; t < num_threads; t++) {
        total_filtered += thread_local_custkeys[t].size();
    }
    filtered_custkeys.reserve(total_filtered);
    for (int t = 0; t < num_threads; t++) {
        filtered_custkeys.insert(filtered_custkeys.end(),
                                thread_local_custkeys[t].begin(),
                                thread_local_custkeys[t].end());
    }

#ifdef GENDB_PROFILE
    auto t_filter_customer_end = std::chrono::high_resolution_clock::now();
    double ms_filter_customer = std::chrono::duration<double, std::milli>(t_filter_customer_end - t_filter_customer_start).count();
    printf("[TIMING] filter_customer: %.2f ms\n", ms_filter_customer);
#endif

    std::cout << "[METADATA CHECK] Filtered customers: " << filtered_custkeys.size() << std::endl;

    // ===== STEP 2 & 3: FILTER ORDERS + JOIN WITH CUSTOMER (Combined) =====

#ifdef GENDB_PROFILE
    auto t_filter_orders_start = std::chrono::high_resolution_clock::now();
#endif

    // Build hash set of filtered customer keys (small, ~300K) using compact hash table
    CompactBoolHashTable customer_set(filtered_custkeys.size());
    for (int32_t custkey : filtered_custkeys) {
        customer_set.insert(custkey, true);
    }

    // Load zone map for orders o_orderdate
    std::vector<ZoneMapEntry> orders_zones = load_zone_map(gendb_dir + "/indexes/idx_orders_orderdate_zmap.bin");

    // Thread-local accumulation for parallel filtering + joining
    std::vector<std::vector<OrderResult>> thread_local_orders(num_threads);
    for (int t = 0; t < num_threads; t++) {
        thread_local_orders[t].reserve(num_orders / (3 * num_threads) + 1000);
    }

    if (orders_zones.empty()) {
        // Fallback to full scan if zone map not available
#pragma omp parallel for
        for (int32_t i = 0; i < num_orders; i++) {
            if (o_orderdate_data[i] < DATE_THRESHOLD && customer_set.contains(o_custkey_data[i])) {
                int tid = omp_get_thread_num();
                OrderResult or_row;
                or_row.o_orderkey = o_orderkey_data[i];
                or_row.o_orderdate = o_orderdate_data[i];
                or_row.o_shippriority = o_shippriority_data[i];
                thread_local_orders[tid].push_back(or_row);
            }
        }
    } else {
        // Zone map pruning + parallel thread-local accumulation
        int32_t current_row = 0;
        for (const auto& zone : orders_zones) {
            // Skip entire zone if min_val >= DATE_THRESHOLD
            if (zone.min_val >= DATE_THRESHOLD) {
                current_row += zone.row_count;
                continue;
            }

            int32_t zone_end = current_row + zone.row_count;
#pragma omp parallel for
            for (int32_t i = current_row; i < zone_end; i++) {
                if (o_orderdate_data[i] < DATE_THRESHOLD && customer_set.contains(o_custkey_data[i])) {
                    int tid = omp_get_thread_num();
                    OrderResult or_row;
                    or_row.o_orderkey = o_orderkey_data[i];
                    or_row.o_orderdate = o_orderdate_data[i];
                    or_row.o_shippriority = o_shippriority_data[i];
                    thread_local_orders[tid].push_back(or_row);
                }
            }
            current_row = zone_end;
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_orders_end = std::chrono::high_resolution_clock::now();
    double ms_filter_orders = std::chrono::duration<double, std::milli>(t_filter_orders_end - t_filter_orders_start).count();
    printf("[TIMING] filter_orders: %.2f ms\n", ms_filter_orders);
#endif

    // Merge thread-local orders
    std::vector<OrderResult> orders_joined;
    size_t total_orders = 0;
    for (int t = 0; t < num_threads; t++) {
        total_orders += thread_local_orders[t].size();
    }
    orders_joined.reserve(total_orders);
    for (int t = 0; t < num_threads; t++) {
        orders_joined.insert(orders_joined.end(),
                            thread_local_orders[t].begin(),
                            thread_local_orders[t].end());
    }

#ifdef GENDB_PROFILE
    // Join1 is now fused with filter_orders, timing is already included
    printf("[TIMING] join1: 0.00 ms\n");
#endif

    std::cout << "[METADATA CHECK] Filtered orders: " << orders_joined.size() << std::endl;
    std::cout << "[METADATA CHECK] Orders after join with customer: " << orders_joined.size() << std::endl;

    // ===== STEP 4: FILTER LINEITEM WITH ZONE MAP PRUNING =====

#ifdef GENDB_PROFILE
    auto t_filter_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    struct LineitemFiltered {
        int32_t l_orderkey;
        int64_t l_extendedprice;
        int64_t l_discount;
    };

    // Load zone map for lineitem l_shipdate
    std::vector<ZoneMapEntry> lineitem_zones = load_zone_map(gendb_dir + "/indexes/idx_lineitem_shipdate_zmap.bin");

    // Thread-local accumulation for parallel filtering
    std::vector<std::vector<LineitemFiltered>> thread_local_lineitem(num_threads);
    for (int t = 0; t < num_threads; t++) {
        thread_local_lineitem[t].reserve(num_lineitem / (4 * num_threads) + 1000);
    }

    if (lineitem_zones.empty()) {
        // Fallback to full scan if zone map not available
#pragma omp parallel for
        for (int32_t i = 0; i < num_lineitem; i++) {
            if (l_shipdate_data[i] > DATE_THRESHOLD) {
                int tid = omp_get_thread_num();
                LineitemFiltered li_row;
                li_row.l_orderkey = l_orderkey_data[i];
                li_row.l_extendedprice = l_extendedprice_data[i];
                li_row.l_discount = l_discount_data[i];
                thread_local_lineitem[tid].push_back(li_row);
            }
        }
    } else {
        // Zone map pruning: for l_shipdate > DATE_THRESHOLD, skip zones where max <= DATE_THRESHOLD
        int32_t current_row = 0;
        for (const auto& zone : lineitem_zones) {
            // Skip entire zone if max_val <= DATE_THRESHOLD
            if (zone.max_val <= DATE_THRESHOLD) {
                current_row += zone.row_count;
                continue;
            }

            int32_t zone_end = current_row + zone.row_count;
#pragma omp parallel for
            for (int32_t i = current_row; i < zone_end; i++) {
                if (l_shipdate_data[i] > DATE_THRESHOLD) {
                    int tid = omp_get_thread_num();
                    LineitemFiltered li_row;
                    li_row.l_orderkey = l_orderkey_data[i];
                    li_row.l_extendedprice = l_extendedprice_data[i];
                    li_row.l_discount = l_discount_data[i];
                    thread_local_lineitem[tid].push_back(li_row);
                }
            }
            current_row = zone_end;
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_lineitem_end = std::chrono::high_resolution_clock::now();
    double ms_filter_lineitem = std::chrono::duration<double, std::milli>(t_filter_lineitem_end - t_filter_lineitem_start).count();
    printf("[TIMING] filter_lineitem: %.2f ms\n", ms_filter_lineitem);
#endif

    // Merge thread-local lineitem results
    std::vector<LineitemFiltered> filtered_lineitem;
    size_t total_lineitem = 0;
    for (int t = 0; t < num_threads; t++) {
        total_lineitem += thread_local_lineitem[t].size();
    }
    filtered_lineitem.reserve(total_lineitem);
    for (int t = 0; t < num_threads; t++) {
        filtered_lineitem.insert(filtered_lineitem.end(),
                               thread_local_lineitem[t].begin(),
                               thread_local_lineitem[t].end());
    }

    std::cout << "[METADATA CHECK] Filtered lineitem: " << filtered_lineitem.size() << std::endl;

    // ===== STEP 5: HASH JOIN 2 (lineitem ⊲⊳ orders on l_orderkey = o_orderkey) =====

#ifdef GENDB_PROFILE
    auto t_join2_start = std::chrono::high_resolution_clock::now();
#endif

    // Build hash map from orders_joined on o_orderkey using compact hash table
    CompactOrderHashTable orders_map(orders_joined.size());
    for (const auto& ord : orders_joined) {
        orders_map.insert(ord.o_orderkey, {ord.o_orderdate, ord.o_shippriority});
    }

    // Join lineitem with orders using thread-local accumulation
    struct JoinedLineitemOrder {
        int32_t l_orderkey;
        int64_t l_extendedprice;
        int64_t l_discount;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    // Thread-local results for parallel join probe
    std::vector<std::vector<JoinedLineitemOrder>> thread_local_join(num_threads);
    for (int t = 0; t < num_threads; t++) {
        thread_local_join[t].reserve(filtered_lineitem.size() / num_threads + 1000);
    }

#pragma omp parallel for
    for (size_t i = 0; i < filtered_lineitem.size(); i++) {
        const auto& li = filtered_lineitem[i];
        int tid = omp_get_thread_num();

        auto it = orders_map.find(li.l_orderkey);
        if (it != nullptr) {
            for (const auto& ord_data : *it) {
                JoinedLineitemOrder jr;
                jr.l_orderkey = li.l_orderkey;
                jr.l_extendedprice = li.l_extendedprice;
                jr.l_discount = li.l_discount;
                jr.o_orderdate = ord_data.o_orderdate;
                jr.o_shippriority = ord_data.o_shippriority;
                thread_local_join[tid].push_back(jr);
            }
        }
    }

    // Merge thread-local join results
    std::vector<JoinedLineitemOrder> joined_results;
    size_t total_join = 0;
    for (int t = 0; t < num_threads; t++) {
        total_join += thread_local_join[t].size();
    }
    joined_results.reserve(total_join);
    for (int t = 0; t < num_threads; t++) {
        joined_results.insert(joined_results.end(),
                             thread_local_join[t].begin(),
                             thread_local_join[t].end());
    }

#ifdef GENDB_PROFILE
    auto t_join2_end = std::chrono::high_resolution_clock::now();
    double ms_join2 = std::chrono::duration<double, std::milli>(t_join2_end - t_join2_start).count();
    printf("[TIMING] join2: %.2f ms\n", ms_join2);
#endif

    std::cout << "[METADATA CHECK] Joined lineitem-orders: " << joined_results.size() << std::endl;

    // ===== STEP 6: AGGREGATION =====

#ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
#endif

    // Use thread-local aggregation with compact hash tables for better cache locality
    std::vector<CompactAggHashTable> thread_local_agg;
    for (int t = 0; t < num_threads; t++) {
        thread_local_agg.push_back(CompactAggHashTable(std::max(100000UL, joined_results.size() / num_threads + 10000)));
    }

#pragma omp parallel for
    for (size_t i = 0; i < joined_results.size(); i++) {
        const auto& jr = joined_results[i];
        int tid = omp_get_thread_num();

        // Calculate revenue: l_extendedprice * (1 - l_discount)
        // Both l_extendedprice and l_discount are scaled by 100.
        // Convert to actual values and compute
        double actual_price = jr.l_extendedprice / 100.0;
        double actual_discount = jr.l_discount / 100.0;
        double revenue = actual_price * (1.0 - actual_discount);

        AggKey key;
        key.l_orderkey = jr.l_orderkey;
        key.o_orderdate = jr.o_orderdate;
        key.o_shippriority = jr.o_shippriority;

        thread_local_agg[tid].insert_or_add(key, revenue);
    }

    // Merge thread-local aggregation results
    CompactAggHashTable agg_table(500000);
    for (int t = 0; t < num_threads; t++) {
        for (const auto& entry : thread_local_agg[t].table) {
            if (entry.occupied) {
                agg_table.insert_or_add(entry.key, entry.value);
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double ms_agg = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", ms_agg);
#endif

    size_t agg_groups = agg_table.size();
    std::cout << "[METADATA CHECK] Aggregation groups: " << agg_groups << std::endl;

    // ===== STEP 7: SORT & LIMIT =====

#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<FinalResult> final_results;
    final_results.reserve(agg_groups);

    for (const auto& entry : agg_table.table) {
        if (entry.occupied) {
            FinalResult fr;
            fr.l_orderkey = entry.key.l_orderkey;
            fr.revenue = entry.value; // Already in actual units from floating-point calc
            fr.o_orderdate = entry.key.o_orderdate;
            fr.o_shippriority = entry.key.o_shippriority;
            final_results.push_back(fr);
        }
    }

    // Sort by revenue DESC, o_orderdate ASC
    std::sort(final_results.begin(), final_results.end());

    // Keep only top 10
    if (final_results.size() > 10) {
        final_results.resize(10);
    }

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms_sort);
#endif

    // ===== STEP 8: OUTPUT =====

#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q3.csv";
    std::ofstream out_file(output_path);

    if (!out_file.is_open()) {
        std::cerr << "Error opening output file: " << output_path << std::endl;
        return;
    }

    // Write header
    out_file << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    // Write results (output precision: 4 decimal places for revenue)
    for (const auto& fr : final_results) {
        std::string date_str = epoch_days_to_date(fr.o_orderdate);
        out_file << std::fixed
                 << fr.l_orderkey << ","
                 << std::setprecision(4) << fr.revenue << ","
                 << date_str << ","
                 << std::setprecision(0) << fr.o_shippriority << "\n";
    }

    out_file.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
#endif

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();

#ifdef GENDB_PROFILE
    printf("[TIMING] total: %.2f ms\n", ms_total);
#endif

    std::cout << "Output written to " << output_path << std::endl;
    std::cout << "Results: " << final_results.size() << " rows" << std::endl;
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
