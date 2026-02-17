/*
 * Q12: Shipping Modes and Order Priority
 *
 * LOGICAL PLAN:
 * 1. Filter lineitem (59.9M rows):
 *    - l_shipmode IN ('MAIL', 'SHIP')
 *    - l_commitdate < l_receiptdate
 *    - l_shipdate < l_commitdate
 *    - l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'
 *    → Estimated 1-2M rows after filtering
 * 2. Build hash on filtered lineitem: orderkey → shipmode
 * 3. Probe with orders to get orderpriority
 * 4. Aggregate: GROUP BY l_shipmode, COUNT by priority categories
 *
 * PHYSICAL PLAN:
 * - Parallel scan + filter lineitem → collect (orderkey, shipmode) pairs
 * - Sequential hash build on orders (15M rows)
 * - Probe orders hash with filtered lineitem (1-2M pairs)
 * - Flat array aggregation (only 2 groups: MAIL, SHIP)
 * - Sort by shipmode for output
 *
 * DATE ENCODING:
 * - 1994-01-01 = days since 1970-01-01 = 8766 days
 * - 1995-01-01 = 9131 days
 */

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <omp.h>

// Compact hash table for join using open addressing
template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t capacity;
    size_t count;

    explicit CompactHashTable(size_t expected_size) {
        capacity = 1;
        while (capacity < expected_size * 2) capacity *= 2;
        table.resize(capacity);
        count = 0;
        for (auto& e : table) e.occupied = false;
    }

    // Hash function - use multiply-shift to avoid identity hash
    inline size_t hash(K key) const {
        uint64_t h = static_cast<uint64_t>(key);
        h = h * 0x9E3779B97F4A7C15ULL;
        return (h ^ (h >> 32)) & (capacity - 1);
    }

    void insert(K key, V value) {
        size_t pos = hash(key);
        while (table[pos].occupied) {
            if (table[pos].key == key) {
                table[pos].value = value;
                return;
            }
            pos = (pos + 1) & (capacity - 1);
        }
        table[pos].key = key;
        table[pos].value = value;
        table[pos].occupied = true;
        count++;
    }

    V* find(K key) {
        size_t pos = hash(key);
        while (table[pos].occupied) {
            if (table[pos].key == key) {
                return &table[pos].value;
            }
            pos = (pos + 1) & (capacity - 1);
        }
        return nullptr;
    }
};

// Helper: mmap a binary column file
template<typename T>
T* mmap_column(const std::string& path, size_t expected_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }
    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t expected_size = expected_rows * sizeof(T);
    if (file_size < expected_size) {
        std::cerr << "File too small: " << path << " (expected " << expected_size << ", got " << file_size << ")" << std::endl;
        close(fd);
        return nullptr;
    }
    void* addr = mmap(nullptr, expected_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        std::cerr << "mmap failed for: " << path << std::endl;
        return nullptr;
    }
    return static_cast<T*>(addr);
}

// Load dictionary from text file
std::vector<std::string> load_dictionary(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream ifs(path);
    if (!ifs) {
        std::cerr << "Failed to open dictionary: " << path << std::endl;
        return dict;
    }
    std::string line;
    while (std::getline(ifs, line)) {
        dict.push_back(line);
    }
    return dict;
}

// Find dictionary code for a string
int32_t find_dict_code(const std::vector<std::string>& dict, const std::string& value) {
    for (size_t i = 0; i < dict.size(); i++) {
        if (dict[i] == value) return static_cast<int32_t>(i);
    }
    return -1;
}

// Calculate days since epoch for a date (year, month, day)
int32_t date_to_epoch_days(int year, int month, int day) {
    // Days from 1970-01-01
    int days = 0;
    for (int y = 1970; y < year; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) month_days[2] = 29;
    for (int m = 1; m < month; m++) {
        days += month_days[m];
    }
    days += (day - 1);
    return days;
}

void run_q12(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Table sizes
    const size_t lineitem_rows = 59986052;
    const size_t orders_rows = 15000000;

    // Load lineitem columns
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t* l_orderkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_rows);
    int32_t* l_shipmode = mmap_column<int32_t>(gendb_dir + "/lineitem/l_shipmode.bin", lineitem_rows);
    int32_t* l_commitdate = mmap_column<int32_t>(gendb_dir + "/lineitem/l_commitdate.bin", lineitem_rows);
    int32_t* l_shipdate = mmap_column<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_rows);
    int32_t* l_receiptdate = mmap_column<int32_t>(gendb_dir + "/lineitem/l_receiptdate.bin", lineitem_rows);

    // Load orders columns
    int32_t* o_orderkey = mmap_column<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_rows);
    int32_t* o_orderpriority = mmap_column<int32_t>(gendb_dir + "/orders/o_orderpriority.bin", orders_rows);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // Load dictionaries
#ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::string> shipmode_dict = load_dictionary(gendb_dir + "/lineitem/l_shipmode_dict.txt");
    std::vector<std::string> orderpriority_dict = load_dictionary(gendb_dir + "/orders/o_orderpriority_dict.txt");

    // Find dictionary codes
    int32_t mail_code = find_dict_code(shipmode_dict, "MAIL");
    int32_t ship_code = find_dict_code(shipmode_dict, "SHIP");
    int32_t urgent_code = find_dict_code(orderpriority_dict, "1-URGENT");
    int32_t high_code = find_dict_code(orderpriority_dict, "2-HIGH");

#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double dict_ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] decode: %.2f ms\n", dict_ms);
#endif

    // Date thresholds
    int32_t date_1994_01_01 = date_to_epoch_days(1994, 1, 1);
    int32_t date_1995_01_01 = date_to_epoch_days(1995, 1, 1);

    // Step 1: Parallel scan and filter lineitem
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Thread-local buffers for filtered results
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<std::pair<int32_t, int32_t>>> thread_local_results(num_threads);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        thread_local_results[tid].reserve(lineitem_rows / num_threads / 10);

        #pragma omp for schedule(static)
        for (size_t i = 0; i < lineitem_rows; i++) {
            int32_t sm = l_shipmode[i];
            // Check shipmode first (most selective likely)
            if (sm != mail_code && sm != ship_code) continue;

            int32_t rdate = l_receiptdate[i];
            if (rdate < date_1994_01_01 || rdate >= date_1995_01_01) continue;

            int32_t cdate = l_commitdate[i];
            int32_t sdate = l_shipdate[i];

            // Check date relationships
            if (cdate >= rdate) continue;
            if (sdate >= cdate) continue;

            // This lineitem qualifies
            thread_local_results[tid].emplace_back(l_orderkey[i], sm);
        }
    }

    // Merge thread-local results
    std::vector<std::pair<int32_t, int32_t>> filtered_lineitem;
    size_t total_count = 0;
    for (auto& v : thread_local_results) {
        total_count += v.size();
    }
    filtered_lineitem.reserve(total_count);
    for (auto& v : thread_local_results) {
        filtered_lineitem.insert(filtered_lineitem.end(), v.begin(), v.end());
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms (filtered %zu rows)\n", scan_ms, filtered_lineitem.size());
#endif

    // Step 2: Load pre-built orders hash index
#ifdef GENDB_PROFILE
    auto t_index_start = std::chrono::high_resolution_clock::now();
#endif

    // Load orders_orderkey_hash index
    // Format: [uint32_t num_entries][uint32_t table_size] then [key:int32_t, position:uint32_t] per slot
    std::string index_path = gendb_dir + "/indexes/orders_orderkey_hash.bin";
    int fd = open(index_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open index: " << index_path << std::endl;
        return;
    }
    struct stat sb;
    fstat(fd, &sb);
    size_t index_size = sb.st_size;
    void* index_addr = mmap(nullptr, index_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (index_addr == MAP_FAILED) {
        std::cerr << "mmap failed for index: " << index_path << std::endl;
        return;
    }

    // Parse index header
    uint32_t* index_header = static_cast<uint32_t*>(index_addr);
    uint32_t num_entries = index_header[0];
    uint32_t table_size = index_header[1];

    struct IndexEntry {
        int32_t key;
        uint32_t position;
    };
    IndexEntry* index_table = reinterpret_cast<IndexEntry*>(&index_header[2]);

    // Helper function to find position using the pre-built index
    auto find_order_position = [&](int32_t orderkey) -> int32_t {
        uint64_t h = static_cast<uint64_t>(orderkey);
        h = h * 0x9E3779B97F4A7C15ULL;
        size_t idx = (h ^ (h >> 32)) & (table_size - 1);

        while (true) {
            if (index_table[idx].key == orderkey) {
                return index_table[idx].position;
            }
            if (index_table[idx].key == 0 && index_table[idx].position == 0) {
                // Empty slot
                return -1;
            }
            idx = (idx + 1) & (table_size - 1);
        }
    };

#ifdef GENDB_PROFILE
    auto t_index_end = std::chrono::high_resolution_clock::now();
    double index_ms = std::chrono::duration<double, std::milli>(t_index_end - t_index_start).count();
    printf("[TIMING] index_load: %.2f ms\n", index_ms);
#endif

    // Step 3: Probe orders using index, aggregate
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Aggregation: GROUP BY shipmode (only 2 values: MAIL, SHIP)
    // Use flat arrays indexed by shipmode code
    std::vector<int64_t> high_line_count(shipmode_dict.size(), 0);
    std::vector<int64_t> low_line_count(shipmode_dict.size(), 0);

    for (auto& p : filtered_lineitem) {
        int32_t orderkey = p.first;
        int32_t shipmode = p.second;

        int32_t order_pos = find_order_position(orderkey);
        if (order_pos >= 0) {
            int32_t priority = o_orderpriority[order_pos];

            if (priority == urgent_code || priority == high_code) {
                high_line_count[shipmode]++;
            } else {
                low_line_count[shipmode]++;
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double join_ms = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join: %.2f ms\n", join_ms);
#endif

    // Step 4: Collect and sort results
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    struct Result {
        std::string shipmode;
        int64_t high_count;
        int64_t low_count;
    };
    std::vector<Result> results;

    for (size_t i = 0; i < shipmode_dict.size(); i++) {
        if (high_line_count[i] > 0 || low_line_count[i] > 0) {
            results.push_back({shipmode_dict[i], high_line_count[i], low_line_count[i]});
        }
    }

    // Sort by shipmode
    std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        return a.shipmode < b.shipmode;
    });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    // Step 5: Write output
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q12.csv";
    std::ofstream ofs(output_path);
    if (!ofs) {
        std::cerr << "Failed to open output file: " << output_path << std::endl;
        return;
    }

    ofs << "l_shipmode,high_line_count,low_line_count\n";
    for (const auto& r : results) {
        ofs << r.shipmode << "," << r.high_count << "," << r.low_count << "\n";
    }
    ofs.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
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
    run_q12(gendb_dir, results_dir);
    return 0;
}
#endif
