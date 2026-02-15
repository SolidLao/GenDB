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
#include <chrono>
#include <iomanip>
#include <sstream>
#include <cmath>
#include <omp.h>

// ============================================================================
// CONSTANTS AND CONFIGURATION
// ============================================================================

constexpr int DATE_THRESHOLD = 9204;  // 1995-03-15 (days since epoch)
constexpr uint8_t BUILDING_CODE = 0;   // Dictionary code for "BUILDING"
constexpr int SCALE_FACTOR = 100;      // For DECIMAL columns
constexpr int RESULT_LIMIT = 10;

// ============================================================================
// HELPER STRUCTURES
// ============================================================================

struct ResultRow {
    int32_t l_orderkey;
    int64_t revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const ResultRow& other) const {
        // Sort by revenue DESC, then o_orderdate ASC
        if (revenue != other.revenue) {
            return revenue > other.revenue;  // DESC
        }
        return o_orderdate < other.o_orderdate;  // ASC
    }
};

struct GroupKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const GroupKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        // Combine hashes using FNV-1a algorithm
        size_t h = 14695981039346656037ULL;
        h ^= k.l_orderkey;
        h *= 1099511628211ULL;
        h ^= k.o_orderdate;
        h *= 1099511628211ULL;
        h ^= k.o_shippriority;
        h *= 1099511628211ULL;
        return h;
    }
};

// ============================================================================
// MMAP UTILITIES
// ============================================================================

template<typename T>
T* mmap_column(const std::string& file_path, size_t& num_rows) {
    int fd = open(file_path.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "Failed to open file: " << file_path << std::endl;
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    if (file_size == -1) {
        std::cerr << "Failed to get file size: " << file_path << std::endl;
        close(fd);
        return nullptr;
    }

    num_rows = file_size / sizeof(T);

    T* data = (T*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap file: " << file_path << std::endl;
        return nullptr;
    }

    return data;
}

void munmap_column(void* data, size_t num_rows, size_t element_size) {
    if (data != nullptr) {
        munmap(data, num_rows * element_size);
    }
}

// ============================================================================
// ZONE MAP LOADING
// ============================================================================

struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
};

std::vector<ZoneMapEntry> load_zone_map(const std::string& file_path) {
    std::vector<ZoneMapEntry> zones;

    int fd = open(file_path.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "Failed to open zone map: " << file_path << std::endl;
        return zones;
    }

    // Read header: num_blocks
    uint32_t num_blocks = 0;
    if (read(fd, &num_blocks, sizeof(uint32_t)) != sizeof(uint32_t)) {
        std::cerr << "Failed to read zone map header" << std::endl;
        close(fd);
        return zones;
    }

    zones.resize(num_blocks);

    // Read zone entries
    for (uint32_t i = 0; i < num_blocks; i++) {
        if (read(fd, &zones[i].min_val, sizeof(int32_t)) != sizeof(int32_t)) {
            std::cerr << "Failed to read zone map min" << std::endl;
            close(fd);
            zones.clear();
            return zones;
        }
        if (read(fd, &zones[i].max_val, sizeof(int32_t)) != sizeof(int32_t)) {
            std::cerr << "Failed to read zone map max" << std::endl;
            close(fd);
            zones.clear();
            return zones;
        }
    }

    close(fd);
    return zones;
}

// ============================================================================
// HASH INDEX LOADING (Multi-value)
// ============================================================================

struct HashIndexEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

struct MultiValueHashIndex {
    std::vector<HashIndexEntry> table;
    std::vector<uint32_t> positions;
    uint32_t table_size;

    bool load(const std::string& file_path) {
        int fd = open(file_path.c_str(), O_RDONLY);
        if (fd == -1) {
            std::cerr << "Failed to open hash index: " << file_path << std::endl;
            return false;
        }

        // Read header
        uint32_t num_unique_keys = 0;
        if (read(fd, &num_unique_keys, sizeof(uint32_t)) != sizeof(uint32_t)) {
            std::cerr << "Failed to read num_unique_keys" << std::endl;
            close(fd);
            return false;
        }

        if (read(fd, &table_size, sizeof(uint32_t)) != sizeof(uint32_t)) {
            std::cerr << "Failed to read table_size" << std::endl;
            close(fd);
            return false;
        }

        // Read hash table
        table.resize(table_size);
        size_t table_bytes = (size_t)table_size * sizeof(HashIndexEntry);
        if (read(fd, table.data(), table_bytes) != (int)table_bytes) {
            std::cerr << "Failed to read hash table entries" << std::endl;
            close(fd);
            return false;
        }

        // Read positions array header (count)
        uint32_t position_count = 0;
        if (read(fd, &position_count, sizeof(uint32_t)) != sizeof(uint32_t)) {
            std::cerr << "Failed to read position_count" << std::endl;
            close(fd);
            return false;
        }

        // Read positions
        positions.resize(position_count);
        size_t pos_bytes = (size_t)position_count * sizeof(uint32_t);
        if (read(fd, positions.data(), pos_bytes) != (int)pos_bytes) {
            std::cerr << "Failed to read positions" << std::endl;
            close(fd);
            return false;
        }

        close(fd);
        return true;
    }

    // Linear probe lookup
    bool lookup(int32_t key, uint32_t& offset, uint32_t& count) {
        // Simple hash function
        size_t h = (size_t)key;
        size_t probe = h & (table_size - 1);

        for (uint32_t i = 0; i < table_size; i++) {
            const HashIndexEntry& entry = table[probe];
            if (entry.key == key) {
                offset = entry.offset;
                count = entry.count;
                return true;
            }
            if (entry.key == 0 && entry.offset == 0 && entry.count == 0) {
                // Empty slot (assuming this is used as marker)
                return false;
            }
            probe = (probe + 1) & (table_size - 1);
        }

        return false;
    }
};

// ============================================================================
// MAIN QUERY IMPLEMENTATION
// ============================================================================

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Q3: Customer->Orders->Lineitem join\n");
    printf("[METADATA CHECK] Customer: c_mktsegment (dict), c_custkey\n");
    printf("[METADATA CHECK] Orders: o_orderkey, o_custkey, o_orderdate (DATE), o_shippriority\n");
    printf("[METADATA CHECK] Lineitem: l_orderkey, l_extendedprice (DECIMAL:100), l_discount (DECIMAL:100), l_shipdate (DATE)\n");
    printf("[METADATA CHECK] Filters: c_mktsegment='BUILDING'(0), o_orderdate<9204, l_shipdate>9204\n");
#endif

    // Load zone maps
#ifdef GENDB_PROFILE
    auto t_zm_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<ZoneMapEntry> o_orderdate_zones =
        load_zone_map(gendb_dir + "/indexes/o_orderdate_zone_map.bin");
    std::vector<ZoneMapEntry> l_shipdate_zones =
        load_zone_map(gendb_dir + "/indexes/l_shipdate_zone_map.bin");

#ifdef GENDB_PROFILE
    auto t_zm_end = std::chrono::high_resolution_clock::now();
    double zm_ms = std::chrono::duration<double, std::milli>(t_zm_end - t_zm_start).count();
    printf("[TIMING] zone_map_load: %.2f ms\n", zm_ms);
#endif

    // Hash indexes will be built on-the-fly from mmap'd columns
#ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Using on-the-fly hash index construction (zone maps pre-loaded)\n");
#endif

    // ========================================================================
    // STEP 1: Load and filter Customer table
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_cust_start = std::chrono::high_resolution_clock::now();
#endif

    size_t customer_rows = 0;
    auto* c_custkey = mmap_column<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_rows);
    auto* c_mktsegment = mmap_column<uint8_t>(gendb_dir + "/customer/c_mktsegment.bin", customer_rows);

    if (!c_custkey || !c_mktsegment) {
        std::cerr << "Failed to load customer columns" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Loaded customer: %zu rows\n", customer_rows);
#endif

    std::vector<int32_t> filtered_custkeys;
    filtered_custkeys.reserve(customer_rows);

    // Filter customers by c_mktsegment = 'BUILDING' (code 0)
#pragma omp parallel for
    for (size_t i = 0; i < customer_rows; i++) {
        if (c_mktsegment[i] == BUILDING_CODE) {
#pragma omp critical
            filtered_custkeys.push_back(c_custkey[i]);
        }
    }

#ifdef GENDB_PROFILE
    auto t_cust_end = std::chrono::high_resolution_clock::now();
    double cust_ms = std::chrono::duration<double, std::milli>(t_cust_end - t_cust_start).count();
    printf("[TIMING] customer_filter: %.2f ms (%zu rows)\n", cust_ms, filtered_custkeys.size());
#endif

    // ========================================================================
    // STEP 2: Load and filter Orders, join with filtered customers
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    size_t orders_rows = 0;
    auto* o_orderkey = mmap_column<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_rows);
    auto* o_custkey = mmap_column<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_rows);
    auto* o_orderdate = mmap_column<int32_t>(gendb_dir + "/orders/o_orderdate.bin", orders_rows);
    auto* o_shippriority = mmap_column<int32_t>(gendb_dir + "/orders/o_shippriority.bin", orders_rows);

    if (!o_orderkey || !o_custkey || !o_orderdate || !o_shippriority) {
        std::cerr << "Failed to load orders columns" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Loaded orders: %zu rows\n", orders_rows);
#endif

    // Create a set of filtered customer keys for quick lookup
    std::unordered_set<int32_t> filtered_cust_set(
        filtered_custkeys.begin(), filtered_custkeys.end());

    struct OrderRecord {
        int32_t o_orderkey;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::vector<OrderRecord> filtered_orders;
    filtered_orders.reserve(orders_rows / 10);

    // Filter orders: c_custkey in filtered set AND o_orderdate < 9204
    for (size_t i = 0; i < orders_rows; i++) {
        if (filtered_cust_set.count(o_custkey[i]) &&
            o_orderdate[i] < DATE_THRESHOLD) {
            filtered_orders.push_back({
                o_orderkey[i],
                o_orderdate[i],
                o_shippriority[i]
            });
        }
    }

#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] orders_filter: %.2f ms (%zu rows)\n", orders_ms, filtered_orders.size());
#endif

    // ========================================================================
    // STEP 3: Build hash table on filtered orders for join with lineitem
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_oh_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, std::vector<size_t>> orders_by_key;
    orders_by_key.reserve(filtered_orders.size());

    for (size_t i = 0; i < filtered_orders.size(); i++) {
        orders_by_key[filtered_orders[i].o_orderkey].push_back(i);
    }

#ifdef GENDB_PROFILE
    auto t_oh_end = std::chrono::high_resolution_clock::now();
    double oh_ms = std::chrono::duration<double, std::milli>(t_oh_end - t_oh_start).count();
    printf("[TIMING] orders_hash_build: %.2f ms\n", oh_ms);
#endif

    // ========================================================================
    // STEP 4: Load and filter Lineitem, join with filtered orders
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_li_start = std::chrono::high_resolution_clock::now();
#endif

    size_t lineitem_rows = 0;
    auto* l_orderkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_rows);
    auto* l_extendedprice = mmap_column<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_rows);
    auto* l_discount = mmap_column<int64_t>(gendb_dir + "/lineitem/l_discount.bin", lineitem_rows);
    auto* l_shipdate = mmap_column<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_rows);

    if (!l_orderkey || !l_extendedprice || !l_discount || !l_shipdate) {
        std::cerr << "Failed to load lineitem columns" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Loaded lineitem: %zu rows\n", lineitem_rows);
#endif

    // Count blocks
    constexpr int BLOCK_SIZE = 131072;
    int num_blocks = (lineitem_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;

#ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Lineitem blocks: %d\n", num_blocks);
#endif

    // Aggregate by (l_orderkey, o_orderdate, o_shippriority)
    std::unordered_map<GroupKey, int64_t, GroupKeyHash> aggregates;
    aggregates.reserve(filtered_orders.size());

    int blocks_scanned = 0;
    int blocks_skipped = 0;
    size_t lineitem_joined = 0;

    // Process lineitem with zone map filtering
    for (int block_idx = 0; block_idx < num_blocks; block_idx++) {
        // Check zone map: skip if l_shipdate.max <= DATE_THRESHOLD
        if (block_idx < (int)l_shipdate_zones.size()) {
            if (l_shipdate_zones[block_idx].max_val <= DATE_THRESHOLD) {
                blocks_skipped++;
                continue;
            }
        }
        blocks_scanned++;

        size_t start_row = (size_t)block_idx * BLOCK_SIZE;
        size_t end_row = std::min(start_row + BLOCK_SIZE, lineitem_rows);

        for (size_t i = start_row; i < end_row; i++) {
            // Filter: l_shipdate > DATE_THRESHOLD
            if (l_shipdate[i] <= DATE_THRESHOLD) {
                continue;
            }

            // Join: lookup order by l_orderkey
            auto it = orders_by_key.find(l_orderkey[i]);
            if (it == orders_by_key.end()) {
                continue;
            }

            // For each matching order, aggregate revenue
            for (size_t order_idx : it->second) {
                const OrderRecord& order = filtered_orders[order_idx];

                // Calculate revenue: l_extendedprice * (1 - l_discount)
                // Both are scaled: price by 100, discount by 100 (as percentage)
                // Using higher precision: compute price * (100 - discount), then divide
                // This minimizes truncation error compared to dividing first
                int64_t price = l_extendedprice[i];
                int64_t discount = l_discount[i];
                // Intermediate result may be large, but int64 can handle it:
                // max price ~= 6.3M (cents) * max adjustment 100 = 630M, well within int64
                int64_t revenue = (price * (SCALE_FACTOR - discount)) / SCALE_FACTOR;

                GroupKey key = {
                    l_orderkey[i],
                    order.o_orderdate,
                    order.o_shippriority
                };

                aggregates[key] += revenue;
                lineitem_joined++;
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_li_end = std::chrono::high_resolution_clock::now();
    double li_ms = std::chrono::duration<double, std::milli>(t_li_end - t_li_start).count();
    printf("[TIMING] lineitem_join: %.2f ms (%zu rows joined, %d blocks scanned, %d skipped)\n",
           li_ms, lineitem_joined, blocks_scanned, blocks_skipped);
#endif

    // ========================================================================
    // STEP 5: Prepare results and sort
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<ResultRow> results;
    results.reserve(aggregates.size());

    for (const auto& [key, revenue] : aggregates) {
        results.push_back({
            key.l_orderkey,
            revenue,
            key.o_orderdate,
            key.o_shippriority
        });
    }

    // Sort by revenue DESC, o_orderdate ASC
    std::sort(results.begin(), results.end());

    // Keep only top 10
    if (results.size() > RESULT_LIMIT) {
        results.resize(RESULT_LIMIT);
    }

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort_limit: %.2f ms\n", sort_ms);
#endif

    // ========================================================================
    // STEP 6: Write results to CSV
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q3.csv";
    std::ofstream out(output_file);

    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_file << std::endl;
        return;
    }

    // Write header
    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    // Convert epoch dates to YYYY-MM-DD and format revenue
    for (const auto& row : results) {
        // Convert o_orderdate from epoch days to YYYY-MM-DD
        // Note: dates are 1-indexed (day 1 = 1970-01-01), so subtract 1
        int32_t days = row.o_orderdate - 1;
        int year = 1970;
        int month = 1;
        int day = 1;

        // Simple date calculation (days since epoch)
        // Days from 1970-01-01
        int total_days = days;

        // Count years (accounting for leap years)
        while (true) {
            int days_in_year = ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0) ? 366 : 365;
            if (total_days < days_in_year) break;
            total_days -= days_in_year;
            year++;
        }

        // Count months
        int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        if ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0) {
            days_in_month[1] = 29;
        }

        month = 1;
        while (month <= 12 && total_days >= days_in_month[month - 1]) {
            total_days -= days_in_month[month - 1];
            month++;
        }
        day = total_days + 1;

        // Convert revenue from cents to dollars
        double revenue_decimal = (double)row.revenue / SCALE_FACTOR;

        // Format output
        out << row.l_orderkey << ",";
        out << std::fixed << std::setprecision(4) << revenue_decimal << ",";
        out << std::setfill('0') << std::setw(4) << year << "-"
            << std::setw(2) << month << "-"
            << std::setw(2) << day << ",";
        out << row.o_shippriority << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

    // ========================================================================
    // Cleanup
    // ========================================================================
    munmap_column(c_custkey, customer_rows, sizeof(int32_t));
    munmap_column(c_mktsegment, customer_rows, sizeof(uint8_t));
    munmap_column(o_orderkey, orders_rows, sizeof(int32_t));
    munmap_column(o_custkey, orders_rows, sizeof(int32_t));
    munmap_column(o_orderdate, orders_rows, sizeof(int32_t));
    munmap_column(o_shippriority, orders_rows, sizeof(int32_t));
    munmap_column(l_orderkey, lineitem_rows, sizeof(int32_t));
    munmap_column(l_extendedprice, lineitem_rows, sizeof(int64_t));
    munmap_column(l_discount, lineitem_rows, sizeof(int64_t));
    munmap_column(l_shipdate, lineitem_rows, sizeof(int32_t));

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();

#ifdef GENDB_PROFILE
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

#ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Results written to %s (%zu rows)\n", output_file.c_str(), results.size());
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
