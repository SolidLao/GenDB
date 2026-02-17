/*
 * Q10: Returned Item Reporting
 *
 * LOGICAL PLAN (REVISED - Iteration 4):
 * 1. Filter orders: o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01' (3-month window)
 *    - Use zone map on o_orderdate to skip blocks
 *    - Estimated: 15M × (3/84) ≈ 535K rows (actual: ~573K)
 * 2. Join filtered_orders → lineitem on o_orderkey = l_orderkey
 *    - Use pre-built lineitem_orderkey_hash multi-value index
 *    - For each filtered order, lookup lineitems via index (no scan of 60M rows!)
 * 3. Filter lineitem: l_returnflag = 'R'
 *    - Dictionary-encoded column, decode during join
 * 4. Join result → customer on o_custkey = c_custkey
 *    - Use pre-built customer_custkey_hash index
 * 5. Join customer → nation on c_nationkey = n_nationkey
 *    - Direct array lookup (25 nations)
 * 6. Aggregate: GROUP BY (c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment)
 *    - SUM(l_extendedprice * (1 - l_discount))
 *    - Use partition-based aggregation to avoid merge bottleneck
 * 7. Top-K: ORDER BY revenue DESC LIMIT 20
 *    - Use std::partial_sort for top 20
 *
 * PHYSICAL PLAN:
 * - Scan orders with zone map filtering → collect filtered orderkeys
 * - Load pre-built lineitem_orderkey_hash multi-value index
 * - For each filtered order: probe index → get lineitem positions → filter by returnflag
 * - Join with customer via pre-built customer_custkey_hash index
 * - Partition-based aggregation (hash-partitioned by custkey to avoid merge)
 * - Partial sort top 20
 *
 * KEY OPTIMIZATION: Reverse join order using pre-built index eliminates 60M lineitem scan
 */

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <omp.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

// ============================================================================
// Date conversion utilities
// ============================================================================

// Convert YYYY-MM-DD string to epoch days (days since 1970-01-01)
int32_t parse_date(const char* date_str) {
    int year, month, day;
    sscanf(date_str, "%d-%d-%d", &year, &month, &day);

    // Days per month
    static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    // Calculate days from 1970 to target date
    int total_days = 0;

    // Add days for complete years from 1970 to year-1
    for (int y = 1970; y < year; y++) {
        bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        total_days += leap ? 366 : 365;
    }

    // Add days for complete months in target year
    bool leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    for (int m = 1; m < month; m++) {
        total_days += days_in_month[m];
        if (m == 2 && leap_year) total_days += 1;
    }

    // Add remaining days
    total_days += (day - 1);

    return total_days;
}

// Convert epoch days to YYYY-MM-DD string
std::string format_date(int32_t days) {
    static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    int year = 1970;
    int remaining = days;

    while (true) {
        bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int year_days = leap ? 366 : 365;
        if (remaining < year_days) break;
        remaining -= year_days;
        year++;
    }

    bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    int month = 1;
    for (; month <= 12; month++) {
        int month_days = days_in_month[month];
        if (month == 2 && leap) month_days = 29;
        if (remaining < month_days) break;
        remaining -= month_days;
    }

    int day = remaining + 1;

    char buf[12];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return buf;
}

// ============================================================================
// mmap column loading
// ============================================================================

template<typename T>
struct Column {
    const T* data;
    size_t size;
    int fd;
    void* mapped;
    size_t mapped_size;

    Column() : data(nullptr), size(0), fd(-1), mapped(nullptr), mapped_size(0) {}

    ~Column() {
        if (mapped) munmap(mapped, mapped_size);
        if (fd >= 0) close(fd);
    }
};

template<typename T>
Column<T> mmap_column(const std::string& gendb_dir, const std::string& table,
                      const std::string& column, size_t row_count) {
    Column<T> col;
    std::string path = gendb_dir + "/" + table + "/" + column + ".bin";

    col.fd = open(path.c_str(), O_RDONLY);
    if (col.fd < 0) {
        fprintf(stderr, "Failed to open %s\n", path.c_str());
        return col;
    }

    struct stat sb;
    fstat(col.fd, &sb);
    col.mapped_size = sb.st_size;

    col.mapped = mmap(nullptr, col.mapped_size, PROT_READ, MAP_PRIVATE, col.fd, 0);
    if (col.mapped == MAP_FAILED) {
        fprintf(stderr, "Failed to mmap %s\n", path.c_str());
        close(col.fd);
        col.fd = -1;
        return col;
    }

    col.data = static_cast<const T*>(col.mapped);
    col.size = row_count;

    return col;
}

// ============================================================================
// String column loading (length-prefixed)
// ============================================================================

std::vector<std::string> load_string_column(const std::string& gendb_dir,
                                           const std::string& table,
                                           const std::string& column,
                                           size_t row_count) {
    std::string path = gendb_dir + "/" + table + "/" + column + ".bin";
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) {
        fprintf(stderr, "Failed to open %s\n", path.c_str());
        return {};
    }

    std::vector<std::string> result;
    result.reserve(row_count);

    for (size_t i = 0; i < row_count; i++) {
        uint32_t len;
        if (fread(&len, sizeof(len), 1, f) != 1) break;
        std::string s(len, '\0');
        if (fread(&s[0], 1, len, f) != len) break;
        result.push_back(std::move(s));
    }

    fclose(f);
    return result;
}

// ============================================================================
// Dictionary loading
// ============================================================================

std::unordered_map<int32_t, std::string> load_dictionary(const std::string& path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(path);
    if (!f) {
        fprintf(stderr, "Failed to open dictionary %s\n", path.c_str());
        return dict;
    }

    std::string line;
    int32_t code = 0;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            // Format: code=value
            int32_t parsed_code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[parsed_code] = value;
        } else {
            // Format: just value (code is line number)
            dict[code] = line;
            code++;
        }
    }

    return dict;
}

// ============================================================================
// Zone map loading and filtering
// ============================================================================

struct ZoneMapEntry {
    int32_t min_value;
    int32_t max_value;
};

std::vector<ZoneMapEntry> load_zone_map(const std::string& index_path) {
    std::vector<ZoneMapEntry> zones;

    int fd = open(index_path.c_str(), O_RDONLY);
    if (fd < 0) return zones;

    uint32_t num_entries;
    if (read(fd, &num_entries, sizeof(num_entries)) != sizeof(num_entries)) {
        close(fd);
        return zones;
    }

    zones.resize(num_entries);
    read(fd, zones.data(), num_entries * sizeof(ZoneMapEntry));
    close(fd);

    return zones;
}

// ============================================================================
// Hash index loading
// ============================================================================

struct HashSingleEntry {
    int32_t key;
    uint32_t position;
};

struct HashSingleIndex {
    uint32_t num_entries;
    uint32_t table_size;
    std::vector<HashSingleEntry> entries;

    const HashSingleEntry* find(int32_t key) const {
        if (entries.empty() || table_size == 0) return nullptr;

        // Hash function: multiplicative hash (matches index builder)
        // table_size is always power of 2, so we can use & instead of %
        size_t idx = (((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & (table_size - 1);

        for (size_t probe = 0; probe < table_size; probe++) {
            const auto& entry = entries[idx];

            // Empty slot detection: key=-1 is sentinel for empty
            if (entry.key == -1) return nullptr;
            if (entry.key == key) return &entry;

            // Linear probing
            idx = (idx + 1) & (table_size - 1);
        }

        return nullptr;
    }
};

HashSingleIndex load_hash_single_index(const std::string& index_path) {
    HashSingleIndex idx;

    int fd = open(index_path.c_str(), O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "Failed to open hash index: %s\n", index_path.c_str());
        return idx;
    }

    if (read(fd, &idx.num_entries, sizeof(idx.num_entries)) != sizeof(idx.num_entries)) {
        fprintf(stderr, "Failed to read num_entries\n");
        close(fd);
        return idx;
    }

    if (read(fd, &idx.table_size, sizeof(idx.table_size)) != sizeof(idx.table_size)) {
        fprintf(stderr, "Failed to read table_size\n");
        close(fd);
        return idx;
    }

    idx.entries.resize(idx.table_size);
    size_t bytes_to_read = idx.table_size * sizeof(HashSingleEntry);
    if (read(fd, idx.entries.data(), bytes_to_read) != (ssize_t)bytes_to_read) {
        fprintf(stderr, "Failed to read hash entries\n");
        idx.entries.clear();
    }

    close(fd);
    return idx;
}

// ============================================================================
// Multi-value hash index loading (for lineitem_orderkey)
// ============================================================================

struct HashMultiValueEntry {
    int32_t key;
    uint32_t offset;  // Offset into positions array
    uint32_t count;   // Number of positions for this key
};

struct HashMultiValueIndex {
    uint32_t num_unique;
    uint32_t table_size;
    std::vector<HashMultiValueEntry> entries;
    std::vector<uint32_t> positions;

    const HashMultiValueEntry* find(int32_t key) const {
        if (entries.empty() || table_size == 0) return nullptr;

        // Hash function: multiplicative hash (matches index builder)
        size_t idx = (((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & (table_size - 1);

        for (size_t probe = 0; probe < table_size; probe++) {
            const auto& entry = entries[idx];

            // Empty slot detection: key=-1 is sentinel for empty
            if (entry.key == -1) return nullptr;
            if (entry.key == key) return &entry;

            // Linear probing
            idx = (idx + 1) & (table_size - 1);
        }

        return nullptr;
    }
};

HashMultiValueIndex load_hash_multi_value_index(const std::string& index_path) {
    HashMultiValueIndex idx;

    int fd = open(index_path.c_str(), O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "Failed to open multi-value hash index: %s\n", index_path.c_str());
        return idx;
    }

    if (read(fd, &idx.num_unique, sizeof(idx.num_unique)) != sizeof(idx.num_unique)) {
        fprintf(stderr, "Failed to read num_unique\n");
        close(fd);
        return idx;
    }

    if (read(fd, &idx.table_size, sizeof(idx.table_size)) != sizeof(idx.table_size)) {
        fprintf(stderr, "Failed to read table_size\n");
        close(fd);
        return idx;
    }

    idx.entries.resize(idx.table_size);
    size_t bytes_to_read = idx.table_size * sizeof(HashMultiValueEntry);
    if (read(fd, idx.entries.data(), bytes_to_read) != (ssize_t)bytes_to_read) {
        fprintf(stderr, "Failed to read multi-value hash entries\n");
        idx.entries.clear();
        close(fd);
        return idx;
    }

    // Read positions array
    uint32_t pos_count;
    if (read(fd, &pos_count, sizeof(pos_count)) != sizeof(pos_count)) {
        fprintf(stderr, "Failed to read pos_count\n");
        close(fd);
        return idx;
    }

    idx.positions.resize(pos_count);
    bytes_to_read = pos_count * sizeof(uint32_t);
    if (read(fd, idx.positions.data(), bytes_to_read) != (ssize_t)bytes_to_read) {
        fprintf(stderr, "Failed to read positions array\n");
        idx.positions.clear();
    }

    close(fd);
    return idx;
}

// ============================================================================
// Filtered orders vector (orderkey, custkey pairs)
// ============================================================================

struct FilteredOrder {
    int32_t orderkey;
    int32_t custkey;
};

// ============================================================================
// Compact hash table for aggregation (simple linear probing)
// ============================================================================

struct AggregateEntry {
    int32_t c_custkey;
    int64_t revenue_scaled;
    int64_t c_acctbal_scaled;  // Store c_acctbal from customer (part of GROUP BY)
    int32_t c_nationkey;       // Store nationkey from customer (needed for n_name in GROUP BY)
};

struct AggregateHashTable {
    std::vector<AggregateEntry> table;
    size_t mask;
    size_t count;
    int32_t empty_key;

    AggregateHashTable(size_t expected) {
        size_t cap = 1;
        while (cap < expected * 4 / 3) cap <<= 1;  // Load factor 0.75
        table.resize(cap);
        mask = cap - 1;
        count = 0;
        empty_key = -1;

        // Initialize all slots as empty
        for (auto& entry : table) {
            entry.c_custkey = empty_key;
            entry.revenue_scaled = 0;
            entry.c_acctbal_scaled = 0;
            entry.c_nationkey = -1;
        }
    }

    size_t hash_key(int32_t key) const {
        return ((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
    }

    void accumulate(int32_t custkey, int64_t revenue, int64_t acctbal, int32_t nationkey) {
        size_t pos = hash_key(custkey) & mask;

        // Linear probing
        while (table[pos].c_custkey != empty_key) {
            if (table[pos].c_custkey == custkey) {
                // Found existing group, accumulate revenue
                table[pos].revenue_scaled += revenue;
                return;
            }
            pos = (pos + 1) & mask;
        }

        // Found empty slot, insert new group
        table[pos].c_custkey = custkey;
        table[pos].revenue_scaled = revenue;
        table[pos].c_acctbal_scaled = acctbal;
        table[pos].c_nationkey = nationkey;
        count++;
    }
};

// ============================================================================
// Result struct for top-K
// ============================================================================

struct ResultRow {
    int32_t c_custkey;
    std::string c_name;
    double revenue;
    double c_acctbal;
    std::string n_name;
    std::string c_address;
    std::string c_phone;
    std::string c_comment;
};

// ============================================================================
// Main query execution
// ============================================================================

void run_q10(const std::string& gendb_dir, const std::string& results_dir) {

#ifdef GENDB_PROFILE
    auto total_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // Step 1: Load nation (small dimension, 25 rows)
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    auto nation_n_nationkey = mmap_column<int32_t>(gendb_dir, "nation", "n_nationkey", 25);
    auto nation_n_name = load_string_column(gendb_dir, "nation", "n_name", 25);

    // Build direct array lookup for nation (nationkey 0-24)
    std::string nation_names[25];
    for (size_t i = 0; i < 25; i++) {
        nation_names[nation_n_nationkey.data[i]] = nation_n_name[i];
    }

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_nation: %.2f ms\n", ms);
#endif

    // ========================================================================
    // Step 2: Filter orders by date range using zone map → collect filtered orders
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    auto orders_o_orderkey = mmap_column<int32_t>(gendb_dir, "orders", "o_orderkey", 15000000);
    auto orders_o_custkey = mmap_column<int32_t>(gendb_dir, "orders", "o_custkey", 15000000);
    auto orders_o_orderdate = mmap_column<int32_t>(gendb_dir, "orders", "o_orderdate", 15000000);

    int32_t date_min = parse_date("1993-10-01");
    int32_t date_max = parse_date("1994-01-01");

    // Load zone map for o_orderdate
    std::string zone_map_path = gendb_dir + "/indexes/orders_orderdate_zone.bin";
    auto zone_map = load_zone_map(zone_map_path);

    const size_t BLOCK_SIZE = 100000;
    std::vector<bool> active_blocks(150, true); // 15M / 100K = 150 blocks

    for (size_t i = 0; i < zone_map.size() && i < active_blocks.size(); i++) {
        // Skip block if zone doesn't overlap [date_min, date_max)
        if (zone_map[i].max_value < date_min || zone_map[i].min_value >= date_max) {
            active_blocks[i] = false;
        }
    }

    // Collect filtered orders (orderkey, custkey pairs)
    std::vector<FilteredOrder> filtered_orders;
    filtered_orders.reserve(600000); // Estimated ~535K

    for (size_t i = 0; i < 15000000; i++) {
        size_t block_id = i / BLOCK_SIZE;
        if (block_id < active_blocks.size() && !active_blocks[block_id]) continue;

        int32_t orderdate = orders_o_orderdate.data[i];
        if (orderdate >= date_min && orderdate < date_max) {
            filtered_orders.push_back({orders_o_orderkey.data[i], orders_o_custkey.data[i]});
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] scan_filter_orders: %.2f ms (filtered to %zu rows)\n", ms, filtered_orders.size());
#endif

    // ========================================================================
    // Step 3: Load customer data and hash index
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Load customer columns
    auto customer_c_custkey = mmap_column<int32_t>(gendb_dir, "customer", "c_custkey", 1500000);
    auto customer_c_nationkey = mmap_column<int32_t>(gendb_dir, "customer", "c_nationkey", 1500000);
    auto customer_c_acctbal = mmap_column<int64_t>(gendb_dir, "customer", "c_acctbal", 1500000);

    // Load pre-built customer hash index
    std::string customer_index_path = gendb_dir + "/indexes/customer_custkey_hash.bin";
    auto customer_index = load_hash_single_index(customer_index_path);

    // Defer loading string columns until needed (late materialization)

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_customer: %.2f ms\n", ms);
#endif

    // ========================================================================
    // Step 4: Load lineitem columns and pre-built index
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    auto lineitem_l_extendedprice = mmap_column<int64_t>(gendb_dir, "lineitem", "l_extendedprice", 59986052);
    auto lineitem_l_discount = mmap_column<int64_t>(gendb_dir, "lineitem", "l_discount", 59986052);
    auto lineitem_l_returnflag = mmap_column<int32_t>(gendb_dir, "lineitem", "l_returnflag", 59986052);

    // Load dictionary for l_returnflag
    std::string dict_path = gendb_dir + "/lineitem/l_returnflag_dict.txt";
    auto returnflag_dict = load_dictionary(dict_path);

    // Find the code for 'R'
    int32_t code_R = -1;
    for (const auto& [code, value] : returnflag_dict) {
        if (value == "R") {
            code_R = code;
            break;
        }
    }

    // Load pre-built lineitem_orderkey multi-value hash index
    std::string lineitem_index_path = gendb_dir + "/indexes/lineitem_orderkey_hash.bin";
    auto lineitem_index = load_hash_multi_value_index(lineitem_index_path);

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_lineitem: %.2f ms (returnflag 'R' code = %d, index entries = %u)\n", ms, code_R, lineitem_index.num_unique);
#endif

    // ========================================================================
    // Step 5: Join orders → lineitem (via index) → customer, aggregate (parallel)
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Partition-based aggregation: divide custkeys into partitions to reduce merge cost
    const int NUM_PARTITIONS = 256;
    std::vector<AggregateHashTable> partition_aggregates;
    for (int p = 0; p < NUM_PARTITIONS; p++) {
        partition_aggregates.emplace_back(2000); // 381K / 256 ≈ 1500 per partition
    }

    #pragma omp parallel for schedule(dynamic, 1000)
    for (size_t oi = 0; oi < filtered_orders.size(); oi++) {
        int32_t orderkey = filtered_orders[oi].orderkey;
        int32_t custkey = filtered_orders[oi].custkey;

        // Join with customer first to get c_acctbal and c_nationkey (part of GROUP BY)
        const HashSingleEntry* cust_entry = customer_index.find(custkey);
        if (!cust_entry) continue;
        size_t cust_idx = cust_entry->position;

        int64_t c_acctbal_scaled = customer_c_acctbal.data[cust_idx];
        int32_t c_nationkey = customer_c_nationkey.data[cust_idx];

        // Probe lineitem index for this orderkey
        const HashMultiValueEntry* lineitem_entry = lineitem_index.find(orderkey);
        if (!lineitem_entry) continue;

        // Iterate over lineitems for this order
        uint32_t offset = lineitem_entry->offset;
        uint32_t count = lineitem_entry->count;

        for (uint32_t j = 0; j < count; j++) {
            uint32_t lineitem_pos = lineitem_index.positions[offset + j];

            // Filter: l_returnflag = 'R'
            if (lineitem_l_returnflag.data[lineitem_pos] != code_R) continue;

            // Calculate revenue (scaled integer arithmetic)
            int64_t price = lineitem_l_extendedprice.data[lineitem_pos];
            int64_t discount = lineitem_l_discount.data[lineitem_pos];
            int64_t revenue = price * (100 - discount); // scaled by 100*100 = 10000

            // Hash partition by custkey
            int partition = (custkey & 0xFF); // Simple modulo 256
            partition_aggregates[partition].accumulate(custkey, revenue, c_acctbal_scaled, c_nationkey);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();

    // Count total groups
    size_t total_groups = 0;
    for (const auto& part : partition_aggregates) {
        total_groups += part.count;
    }
    printf("[TIMING] join_aggregate: %.2f ms (groups: %zu)\n", ms, total_groups);
#endif

    // ========================================================================
    // Step 6: Extract top 20 by revenue (without loading strings yet)
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Create intermediate results with only numeric data from all partitions
    struct IntermediateRow {
        int32_t c_custkey;
        int64_t revenue_scaled;
        int64_t c_acctbal_scaled;
        int32_t c_nationkey;
    };

    std::vector<IntermediateRow> intermediate;
    intermediate.reserve(total_groups);

    for (const auto& partition : partition_aggregates) {
        for (const auto& entry : partition.table) {
            if (entry.c_custkey != partition.empty_key) {
                intermediate.push_back({entry.c_custkey, entry.revenue_scaled,
                                       entry.c_acctbal_scaled, entry.c_nationkey});
            }
        }
    }

    // Partial sort for top 20 (only on numeric data)
    size_t k = std::min<size_t>(20, intermediate.size());
    std::partial_sort(intermediate.begin(), intermediate.begin() + k, intermediate.end(),
                     [](const IntermediateRow& a, const IntermediateRow& b) {
                         return a.revenue_scaled > b.revenue_scaled; // Descending
                     });

    intermediate.resize(k);

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms);
#endif

    // ========================================================================
    // Step 7: Load strings ONLY for top 20 customers (late materialization)
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Load all customer string columns (unavoidable for now, could be optimized further)
    auto customer_c_name = load_string_column(gendb_dir, "customer", "c_name", 1500000);
    auto customer_c_address = load_string_column(gendb_dir, "customer", "c_address", 1500000);
    auto customer_c_phone = load_string_column(gendb_dir, "customer", "c_phone", 1500000);
    auto customer_c_comment = load_string_column(gendb_dir, "customer", "c_comment", 1500000);

    std::vector<ResultRow> results;
    results.reserve(k);

    for (const auto& row : intermediate) {
        int32_t custkey = row.c_custkey;

        // Get customer position from index
        const HashSingleEntry* cust_entry = customer_index.find(custkey);
        if (!cust_entry) continue;
        size_t cust_idx = cust_entry->position;

        ResultRow result;
        result.c_custkey = custkey;
        result.revenue = row.revenue_scaled / 10000.0;
        result.c_acctbal = row.c_acctbal_scaled / 100.0;  // Use aggregated value, not re-loaded
        result.c_name = customer_c_name[cust_idx];
        result.c_address = customer_c_address[cust_idx];
        result.c_phone = customer_c_phone[cust_idx];
        result.c_comment = customer_c_comment[cust_idx];

        result.n_name = nation_names[row.c_nationkey];  // Use aggregated nationkey

        results.push_back(result);
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] decode_strings: %.2f ms\n", ms);
#endif

#ifdef GENDB_PROFILE
    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    // ========================================================================
    // Step 8: Write output
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q10.csv";
    std::ofstream out(output_path);

    out << "c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment\n";

    auto quote_csv = [](const std::string& s) -> std::string {
        if (s.find(',') != std::string::npos || s.find('"') != std::string::npos || s.find('\n') != std::string::npos) {
            std::string result = "\"";
            for (char c : s) {
                if (c == '"') result += "\"\""; // Escape quotes by doubling
                else result += c;
            }
            result += "\"";
            return result;
        }
        return s;
    };

    for (const auto& row : results) {
        out << row.c_custkey << ","
            << quote_csv(row.c_name) << ","
            << std::fixed << std::setprecision(2) << row.revenue << ","
            << std::fixed << std::setprecision(2) << row.c_acctbal << ","
            << quote_csv(row.n_name) << ","
            << quote_csv(row.c_address) << ","
            << quote_csv(row.c_phone) << ","
            << quote_csv(row.c_comment) << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
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

    run_q10(gendb_dir, results_dir);

    return 0;
}
#endif
