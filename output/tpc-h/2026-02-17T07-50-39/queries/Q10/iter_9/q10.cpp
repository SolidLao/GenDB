/*
 * Q10: Returned Item Reporting - COMPLETE REWRITE (Iteration 9)
 *
 * ROOT CAUSE OF STALL:
 * - Previous architecture loaded ALL 1.5M customer strings (610ms!) to output 20 rows
 * - Joined in wrong order (orders → lineitem → customer)
 * - Did not exploit returnflag selectivity early
 *
 * NEW ARCHITECTURE:
 * 1. Pre-filter lineitem by l_returnflag='R' (60M → ~3M, highly selective)
 * 2. Filter orders by date (15M → 573K)
 * 3. Join filtered_orders ⋈ filtered_lineitem on orderkey (using hash on smaller side)
 * 4. Join result → customer on custkey (index lookup)
 * 5. Aggregate by custkey (thread-local, partitioned)
 * 6. Top-20 sort on revenue
 * 7. Load customer strings ONLY for top 20 (selective I/O)
 *
 * KEY OPTIMIZATIONS:
 * - Zone map pruning on orders.o_orderdate
 * - Early filtering of lineitem by returnflag (dictionary-encoded)
 * - Hash join on filtered datasets (not full 60M scan)
 * - Late materialization: load strings only for top 20
 * - Parallel aggregation with partitioning
 * - No loading of 1.5M strings!
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

int32_t parse_date(const char* date_str) {
    int year, month, day;
    sscanf(date_str, "%d-%d-%d", &year, &month, &day);

    static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    int total_days = 0;

    for (int y = 1970; y < year; y++) {
        bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        total_days += leap ? 366 : 365;
    }

    bool leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    for (int m = 1; m < month; m++) {
        total_days += days_in_month[m];
        if (m == 2 && leap_year) total_days += 1;
    }

    total_days += (day - 1);

    return total_days;
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
// Selective string loading (load ONLY specified row indices)
// ============================================================================

std::vector<std::string> load_string_column_selective(const std::string& gendb_dir,
                                                       const std::string& table,
                                                       const std::string& column,
                                                       size_t total_rows,
                                                       const std::vector<size_t>& indices) {
    std::string path = gendb_dir + "/" + table + "/" + column + ".bin";
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) {
        fprintf(stderr, "Failed to open %s\n", path.c_str());
        return std::vector<std::string>(indices.size());
    }

    // Build position index: scan once to find byte offset for each row
    std::vector<size_t> byte_offsets(total_rows + 1);
    byte_offsets[0] = 0;

    for (size_t i = 0; i < total_rows; i++) {
        uint32_t len;
        if (fread(&len, sizeof(len), 1, f) != 1) break;
        fseek(f, len, SEEK_CUR);
        byte_offsets[i + 1] = byte_offsets[i] + sizeof(uint32_t) + len;
    }

    // Load only requested indices
    std::vector<std::string> result(indices.size());
    for (size_t i = 0; i < indices.size(); i++) {
        size_t idx = indices[i];
        if (idx >= total_rows) continue;

        fseek(f, byte_offsets[idx], SEEK_SET);
        uint32_t len;
        if (fread(&len, sizeof(len), 1, f) != 1) continue;

        result[i].resize(len);
        fread(&result[i][0], 1, len, f);
    }

    fclose(f);
    return result;
}

// ============================================================================
// Full string column loading (for small tables like nation)
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
            int32_t parsed_code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[parsed_code] = value;
        } else {
            dict[code] = line;
            code++;
        }
    }

    return dict;
}

// ============================================================================
// Zone map loading
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
// Hash index loading (customer_custkey_hash)
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

        size_t idx = (((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & (table_size - 1);

        for (size_t probe = 0; probe < table_size; probe++) {
            const auto& entry = entries[idx];

            if (entry.key == -1) return nullptr;
            if (entry.key == key) return &entry;

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
        close(fd);
        return idx;
    }

    if (read(fd, &idx.table_size, sizeof(idx.table_size)) != sizeof(idx.table_size)) {
        close(fd);
        return idx;
    }

    idx.entries.resize(idx.table_size);
    size_t bytes_to_read = idx.table_size * sizeof(HashSingleEntry);
    if (read(fd, idx.entries.data(), bytes_to_read) != (ssize_t)bytes_to_read) {
        idx.entries.clear();
    }

    close(fd);
    return idx;
}

// ============================================================================
// Open-addressing hash table for aggregation
// ============================================================================

struct AggregateEntry {
    int32_t custkey;
    int64_t revenue_sum;
    int64_t revenue_c;  // Kahan summation compensation
};

struct AggregateHashTable {
    std::vector<AggregateEntry> table;
    size_t mask;
    size_t count;

    AggregateHashTable(size_t expected_size) {
        size_t cap = 1;
        while (cap < expected_size * 2) cap <<= 1;  // Load factor 0.5
        table.resize(cap);
        mask = cap - 1;
        count = 0;

        for (auto& e : table) {
            e.custkey = -1;
            e.revenue_sum = 0;
            e.revenue_c = 0;
        }
    }

    size_t hash(int32_t key) const {
        return ((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
    }

    // Kahan summation to avoid floating-point precision loss
    void accumulate(int32_t custkey, int64_t revenue_scaled) {
        size_t pos = hash(custkey) & mask;

        while (table[pos].custkey != -1) {
            if (table[pos].custkey == custkey) {
                // Kahan summation for numerical stability
                int64_t y = revenue_scaled - table[pos].revenue_c;
                int64_t t = table[pos].revenue_sum + y;
                table[pos].revenue_c = (t - table[pos].revenue_sum) - y;
                table[pos].revenue_sum = t;
                return;
            }
            pos = (pos + 1) & mask;
        }

        // New group
        table[pos].custkey = custkey;
        table[pos].revenue_sum = revenue_scaled;
        table[pos].revenue_c = 0;
        count++;
    }
};

// ============================================================================
// Result struct
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
    // Step 1: Load nation (small, 25 rows)
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    auto nation_n_nationkey = mmap_column<int32_t>(gendb_dir, "nation", "n_nationkey", 25);
    auto nation_n_name = load_string_column(gendb_dir, "nation", "n_name", 25);

    // Direct array lookup for nation
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
    // Step 2: Pre-filter lineitem by l_returnflag = 'R'
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    auto lineitem_l_orderkey = mmap_column<int32_t>(gendb_dir, "lineitem", "l_orderkey", 59986052);
    auto lineitem_l_extendedprice = mmap_column<int64_t>(gendb_dir, "lineitem", "l_extendedprice", 59986052);
    auto lineitem_l_discount = mmap_column<int64_t>(gendb_dir, "lineitem", "l_discount", 59986052);
    auto lineitem_l_returnflag = mmap_column<int32_t>(gendb_dir, "lineitem", "l_returnflag", 59986052);

    // Load returnflag dictionary
    std::string dict_path = gendb_dir + "/lineitem/l_returnflag_dict.txt";
    auto returnflag_dict = load_dictionary(dict_path);

    int32_t code_R = -1;
    for (const auto& [code, value] : returnflag_dict) {
        if (value == "R") {
            code_R = code;
            break;
        }
    }

    // Filter lineitem: collect (orderkey, revenue) for returnflag='R'
    struct LineitemFiltered {
        int32_t orderkey;
        int64_t revenue_scaled;
    };

    std::vector<LineitemFiltered> filtered_lineitems;
    filtered_lineitems.reserve(3000000);  // Estimate ~3M returned items

    #pragma omp parallel
    {
        std::vector<LineitemFiltered> local_filtered;
        local_filtered.reserve(150000);

        #pragma omp for nowait schedule(static)
        for (size_t i = 0; i < 59986052; i++) {
            if (lineitem_l_returnflag.data[i] == code_R) {
                int64_t price = lineitem_l_extendedprice.data[i];
                int64_t discount = lineitem_l_discount.data[i];
                int64_t revenue = price * (100 - discount);  // scaled by 10000

                local_filtered.push_back({lineitem_l_orderkey.data[i], revenue});
            }
        }

        #pragma omp critical
        {
            filtered_lineitems.insert(filtered_lineitems.end(),
                                     local_filtered.begin(),
                                     local_filtered.end());
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] filter_lineitem: %.2f ms (filtered to %zu rows)\n", ms, filtered_lineitems.size());
#endif

    // ========================================================================
    // Step 3: Filter orders by date range (zone map pruning)
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    auto orders_o_orderkey = mmap_column<int32_t>(gendb_dir, "orders", "o_orderkey", 15000000);
    auto orders_o_custkey = mmap_column<int32_t>(gendb_dir, "orders", "o_custkey", 15000000);
    auto orders_o_orderdate = mmap_column<int32_t>(gendb_dir, "orders", "o_orderdate", 15000000);

    int32_t date_min = parse_date("1993-10-01");
    int32_t date_max = parse_date("1994-01-01");

    // Load zone map
    std::string zone_map_path = gendb_dir + "/indexes/orders_orderdate_zone.bin";
    auto zone_map = load_zone_map(zone_map_path);

    const size_t BLOCK_SIZE = 100000;
    std::vector<bool> active_blocks(150, true);

    for (size_t i = 0; i < zone_map.size() && i < active_blocks.size(); i++) {
        if (zone_map[i].max_value < date_min || zone_map[i].min_value >= date_max) {
            active_blocks[i] = false;
        }
    }

    struct OrderFiltered {
        int32_t orderkey;
        int32_t custkey;
    };

    std::vector<OrderFiltered> filtered_orders;
    filtered_orders.reserve(600000);

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
    printf("[TIMING] filter_orders: %.2f ms (filtered to %zu rows)\n", ms, filtered_orders.size());
#endif

    // ========================================================================
    // Step 4: Build hash on filtered_lineitems (smaller side, ~3M rows)
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Build hash table: orderkey → total_revenue
    // Use open-addressing hash table
    size_t hash_cap = 1;
    while (hash_cap < filtered_lineitems.size() * 2) hash_cap <<= 1;
    size_t hash_mask = hash_cap - 1;

    struct LineitemHashEntry {
        int32_t orderkey;
        int64_t revenue_sum;
        int64_t revenue_c;  // Kahan compensation
    };

    std::vector<LineitemHashEntry> lineitem_hash(hash_cap);
    for (auto& e : lineitem_hash) {
        e.orderkey = -1;
        e.revenue_sum = 0;
        e.revenue_c = 0;
    }

    auto hash_func = [](int32_t key) -> size_t {
        return ((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
    };

    for (const auto& li : filtered_lineitems) {
        size_t pos = hash_func(li.orderkey) & hash_mask;

        while (lineitem_hash[pos].orderkey != -1) {
            if (lineitem_hash[pos].orderkey == li.orderkey) {
                // Kahan summation
                int64_t y = li.revenue_scaled - lineitem_hash[pos].revenue_c;
                int64_t t = lineitem_hash[pos].revenue_sum + y;
                lineitem_hash[pos].revenue_c = (t - lineitem_hash[pos].revenue_sum) - y;
                lineitem_hash[pos].revenue_sum = t;
                goto next_lineitem;
            }
            pos = (pos + 1) & hash_mask;
        }

        // New entry
        lineitem_hash[pos].orderkey = li.orderkey;
        lineitem_hash[pos].revenue_sum = li.revenue_scaled;
        lineitem_hash[pos].revenue_c = 0;

        next_lineitem:;
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] build_lineitem_hash: %.2f ms\n", ms);
#endif

    // ========================================================================
    // Step 5: Load customer data (numeric columns only, defer strings)
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    auto customer_c_custkey = mmap_column<int32_t>(gendb_dir, "customer", "c_custkey", 1500000);
    auto customer_c_nationkey = mmap_column<int32_t>(gendb_dir, "customer", "c_nationkey", 1500000);
    auto customer_c_acctbal = mmap_column<int64_t>(gendb_dir, "customer", "c_acctbal", 1500000);

    // Load customer index
    std::string customer_index_path = gendb_dir + "/indexes/customer_custkey_hash.bin";
    auto customer_index = load_hash_single_index(customer_index_path);

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_customer: %.2f ms\n", ms);
#endif

    // ========================================================================
    // Step 6: Join orders ⋈ lineitem ⋈ customer, aggregate by custkey
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    const int NUM_PARTITIONS = 256;
    std::vector<AggregateHashTable> partitions;
    for (int p = 0; p < NUM_PARTITIONS; p++) {
        partitions.emplace_back(2000);
    }

    #pragma omp parallel for schedule(dynamic, 1000)
    for (size_t i = 0; i < filtered_orders.size(); i++) {
        int32_t orderkey = filtered_orders[i].orderkey;
        int32_t custkey = filtered_orders[i].custkey;

        // Probe lineitem hash
        size_t pos = hash_func(orderkey) & hash_mask;

        for (size_t probe = 0; probe < hash_cap; probe++) {
            if (lineitem_hash[pos].orderkey == orderkey) {
                int64_t revenue = lineitem_hash[pos].revenue_sum;

                // Partition by custkey
                int part_id = custkey & 0xFF;
                partitions[part_id].accumulate(custkey, revenue);
                break;
            }
            if (lineitem_hash[pos].orderkey == -1) break;
            pos = (pos + 1) & hash_mask;
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();

    size_t total_groups = 0;
    for (const auto& p : partitions) total_groups += p.count;
    printf("[TIMING] join_aggregate: %.2f ms (%zu groups)\n", ms, total_groups);
#endif

    // ========================================================================
    // Step 7: Extract all groups, partial sort for top 20
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    struct IntermediateRow {
        int32_t custkey;
        int64_t revenue_scaled;
    };

    std::vector<IntermediateRow> intermediate;
    intermediate.reserve(400000);

    for (const auto& part : partitions) {
        for (const auto& entry : part.table) {
            if (entry.custkey != -1) {
                intermediate.push_back({entry.custkey, entry.revenue_sum});
            }
        }
    }

    size_t k = std::min<size_t>(20, intermediate.size());
    std::partial_sort(intermediate.begin(), intermediate.begin() + k, intermediate.end(),
                     [](const IntermediateRow& a, const IntermediateRow& b) {
                         return a.revenue_scaled > b.revenue_scaled;
                     });

    intermediate.resize(k);

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms);
#endif

    // ========================================================================
    // Step 8: Load customer strings ONLY for top 20 (selective I/O)
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Get customer positions for top 20
    std::vector<size_t> customer_indices;
    customer_indices.reserve(k);

    for (const auto& row : intermediate) {
        const HashSingleEntry* cust_entry = customer_index.find(row.custkey);
        if (cust_entry) {
            customer_indices.push_back(cust_entry->position);
        } else {
            customer_indices.push_back(0);  // Shouldn't happen
        }
    }

    // Selective load: ONLY top 20 customers' strings
    auto customer_c_name = load_string_column_selective(gendb_dir, "customer", "c_name", 1500000, customer_indices);
    auto customer_c_address = load_string_column_selective(gendb_dir, "customer", "c_address", 1500000, customer_indices);
    auto customer_c_phone = load_string_column_selective(gendb_dir, "customer", "c_phone", 1500000, customer_indices);
    auto customer_c_comment = load_string_column_selective(gendb_dir, "customer", "c_comment", 1500000, customer_indices);

    std::vector<ResultRow> results;
    results.reserve(k);

    for (size_t i = 0; i < intermediate.size(); i++) {
        const auto& row = intermediate[i];
        size_t cust_idx = customer_indices[i];

        ResultRow result;
        result.c_custkey = row.custkey;
        result.revenue = row.revenue_scaled / 10000.0;
        result.c_acctbal = customer_c_acctbal.data[cust_idx] / 100.0;
        result.c_name = customer_c_name[i];
        result.c_address = customer_c_address[i];
        result.c_phone = customer_c_phone[i];
        result.c_comment = customer_c_comment[i];
        result.n_name = nation_names[customer_c_nationkey.data[cust_idx]];

        results.push_back(result);
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_top20_strings: %.2f ms\n", ms);
#endif

#ifdef GENDB_PROFILE
    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    // ========================================================================
    // Step 9: Write output
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
                if (c == '"') result += "\"\"";
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
