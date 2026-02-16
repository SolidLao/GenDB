#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <map>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdio>
#include <algorithm>
#include <chrono>
#include <omp.h>

// ============================================================================
// [METADATA CHECK] - Column Encodings & Storage Layout
// ============================================================================
// Q7 uses:
// - lineitem.l_shipdate: int32_t days_since_epoch (values >3000, e.g., 9568)
// - lineitem.l_extendedprice: int64_t scaled by 100 (e.g., 12345 = 123.45)
// - lineitem.l_discount: int64_t scaled by 100 (e.g., 5 = 0.05)
// - All foreign keys: int32_t, no encoding
// - No dictionary encoding needed for Q7
// ============================================================================

struct HashMultiValueIndex {
    uint32_t num_unique;
    uint32_t table_size;
    std::vector<std::pair<int32_t, std::pair<uint32_t, uint32_t>>> entries; // key, {offset, count}
    std::vector<uint32_t> positions;

    static HashMultiValueIndex load(const std::string& filepath) {
        HashMultiValueIndex idx;
        int fd = open(filepath.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << filepath << std::endl;
            exit(1);
        }

        // Read header
        read(fd, &idx.num_unique, sizeof(uint32_t));
        read(fd, &idx.table_size, sizeof(uint32_t));

        // Read hash entries: [key:int32_t, offset:uint32_t, count:uint32_t] * table_size
        std::vector<char> entry_buf(idx.table_size * 12); // 4 + 4 + 4 bytes per entry
        read(fd, entry_buf.data(), entry_buf.size());

        for (uint32_t i = 0; i < idx.table_size; ++i) {
            int32_t key;
            uint32_t offset, count;
            std::memcpy(&key, entry_buf.data() + i * 12, 4);
            std::memcpy(&offset, entry_buf.data() + i * 12 + 4, 4);
            std::memcpy(&count, entry_buf.data() + i * 12 + 8, 4);
            if (count > 0) {
                idx.entries.push_back({key, {offset, count}});
            }
        }

        // Read positions array: [uint32_t pos_count] [positions...]
        uint32_t pos_count;
        read(fd, &pos_count, sizeof(uint32_t));
        idx.positions.resize(pos_count);
        read(fd, idx.positions.data(), pos_count * sizeof(uint32_t));

        close(fd);
        return idx;
    }

    std::vector<uint32_t>* find(int32_t key) {
        for (auto& entry : entries) {
            if (entry.first == key) {
                auto [offset, count] = entry.second;
                static std::vector<uint32_t> result;
                result.clear();
                for (uint32_t i = offset; i < offset + count; ++i) {
                    result.push_back(positions[i]);
                }
                return &result;
            }
        }
        return nullptr;
    }
};

template<typename T>
std::vector<T> mmap_load(const std::string& filepath, size_t count) {
    std::vector<T> data(count);
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << filepath << std::endl;
        exit(1);
    }

    size_t bytes = count * sizeof(T);
    void* ptr = mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) {
        std::cerr << "mmap failed for: " << filepath << std::endl;
        close(fd);
        exit(1);
    }

    std::memcpy(data.data(), ptr, bytes);
    munmap(ptr, bytes);
    close(fd);
    return data;
}

// Date helper: compute epoch days for a given date
int32_t compute_epoch_days(int year, int month, int day) {
    int days = 0;
    // Days in complete years from 1970 to year-1
    for (int y = 1970; y < year; ++y) {
        int is_leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += is_leap ? 366 : 365;
    }
    // Days in complete months in this year
    int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (is_leap) month_days[1] = 29;
    for (int m = 1; m < month; ++m) {
        days += month_days[m - 1];
    }
    // Add days in current month (day is 1-indexed, epoch day 0 = Jan 1, so -1)
    days += (day - 1);
    return days;
}

// Date output helper
std::string format_date_from_epoch(int32_t epoch_days) {
    int year = 1970;
    int day_count = epoch_days;
    while (true) {
        int is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int days_in_year = is_leap ? 366 : 365;
        if (day_count < days_in_year) break;
        day_count -= days_in_year;
        year++;
    }
    int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (is_leap) month_days[1] = 29;

    int month = 1;
    while (day_count >= month_days[month - 1]) {
        day_count -= month_days[month - 1];
        month++;
    }
    int day = day_count + 1;

    char buf[32];
    snprintf(buf, 32, "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

// ============================================================================
// COMPACT OPEN-ADDRESSING HASH TABLE FOR HIGH PERFORMANCE JOINS
// ============================================================================
// Replaces std::unordered_map for 2-5x speedup. Uses linear probing with
// power-of-2 sizing and simple entry marker for occupied slots.
// ============================================================================

template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected_size) {
        // Size to next power of 2 with ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
        // Pre-fill with unoccupied entries
        for (auto& e : table) e.occupied = false;
    }

    size_t hash(K key) const {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, V value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx].key = key;
        table[idx].value = value;
        table[idx].occupied = true;
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

void run_Q7(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // 1. LOAD DATA VIA MMAP
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    const size_t LINEITEM_ROWS = 59986052;
    const size_t ORDERS_ROWS = 15000000;
    const size_t CUSTOMER_ROWS = 1500000;
    const size_t SUPPLIER_ROWS = 100000;
    const size_t NATION_ROWS = 25;

    // Load nation table (tiny)
    std::vector<int32_t> n_nationkey = mmap_load<int32_t>(
        gendb_dir + "/nation/n_nationkey.bin", NATION_ROWS);

    // Load nation names (variable-length strings with offsets)
    std::unordered_map<int32_t, std::string> nation_names;
    std::ifstream nation_file(gendb_dir + "/nation/n_name.bin", std::ios::binary);
    if (!nation_file.is_open()) {
        std::cerr << "Failed to open nation/n_name.bin" << std::endl;
        exit(1);
    }

    // Read count
    uint32_t count;
    nation_file.read(reinterpret_cast<char*>(&count), sizeof(uint32_t));

    // Read all offsets
    std::vector<uint32_t> offsets(count);
    nation_file.read(reinterpret_cast<char*>(offsets.data()), count * sizeof(uint32_t));

    // Read all strings
    std::vector<char> strings;
    nation_file.seekg(0, std::ios::end);
    size_t file_size = nation_file.tellg();
    strings.resize(file_size - (count + 1) * 4);
    nation_file.seekg((count + 1) * 4);
    nation_file.read(strings.data(), strings.size());
    nation_file.close();

    // Parse strings from offsets
    for (size_t i = 0; i < count; ++i) {
        uint32_t start = offsets[i];
        uint32_t end = (i + 1 < count) ? offsets[i + 1] : strings.size();
        if (start < strings.size() && end <= strings.size()) {
            std::string n(strings.begin() + start, strings.begin() + end);
            nation_names[n_nationkey[i]] = n;
        }
    }

    // Find FRANCE and GERMANY nation keys
    int32_t france_key = -1, germany_key = -1;
    for (auto& [nkey, nname] : nation_names) {
        if (nname == "FRANCE") france_key = nkey;
        if (nname == "GERMANY") germany_key = nkey;
    }

    // Load lineitem
    std::vector<int32_t> l_orderkey = mmap_load<int32_t>(
        gendb_dir + "/lineitem/l_orderkey.bin", LINEITEM_ROWS);
    std::vector<int32_t> l_suppkey = mmap_load<int32_t>(
        gendb_dir + "/lineitem/l_suppkey.bin", LINEITEM_ROWS);
    std::vector<int32_t> l_shipdate = mmap_load<int32_t>(
        gendb_dir + "/lineitem/l_shipdate.bin", LINEITEM_ROWS);
    std::vector<int64_t> l_extendedprice = mmap_load<int64_t>(
        gendb_dir + "/lineitem/l_extendedprice.bin", LINEITEM_ROWS);
    std::vector<int64_t> l_discount = mmap_load<int64_t>(
        gendb_dir + "/lineitem/l_discount.bin", LINEITEM_ROWS);

    // Load orders
    std::vector<int32_t> o_orderkey = mmap_load<int32_t>(
        gendb_dir + "/orders/o_orderkey.bin", ORDERS_ROWS);
    std::vector<int32_t> o_custkey = mmap_load<int32_t>(
        gendb_dir + "/orders/o_custkey.bin", ORDERS_ROWS);

    // Load customer
    std::vector<int32_t> c_custkey = mmap_load<int32_t>(
        gendb_dir + "/customer/c_custkey.bin", CUSTOMER_ROWS);
    std::vector<int32_t> c_nationkey = mmap_load<int32_t>(
        gendb_dir + "/customer/c_nationkey.bin", CUSTOMER_ROWS);

    // Load supplier
    std::vector<int32_t> s_suppkey = mmap_load<int32_t>(
        gendb_dir + "/supplier/s_suppkey.bin", SUPPLIER_ROWS);
    std::vector<int32_t> s_nationkey = mmap_load<int32_t>(
        gendb_dir + "/supplier/s_nationkey.bin", SUPPLIER_ROWS);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // ========================================================================
    // 2. COMPUTE DATE FILTER BOUNDARIES
    // ========================================================================
    // l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
    int32_t date_1995_01_01 = compute_epoch_days(1995, 1, 1);
    int32_t date_1996_12_31 = compute_epoch_days(1996, 12, 31);

    // ========================================================================
    // 3. BUILD HASH TABLES FOR JOINS (OPTIMIZED WITH COMPACT OPEN-ADDRESSING)
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
#endif

    // Use CompactHashTable (open-addressing) instead of std::unordered_map

    // Build supplier→nation1 hash map (100K unique suppliers)
    CompactHashTable<int32_t, std::vector<int32_t>> supplier_to_nation(SUPPLIER_ROWS);
    for (size_t i = 0; i < SUPPLIER_ROWS; ++i) {
        auto* entry = supplier_to_nation.find(s_suppkey[i]);
        if (entry) {
            entry->push_back(s_nationkey[i]);
        } else {
            std::vector<int32_t> vec;
            vec.push_back(s_nationkey[i]);
            supplier_to_nation.insert(s_suppkey[i], vec);
        }
    }

    // Build customer→nation2 hash map (1.5M unique customers)
    CompactHashTable<int32_t, std::vector<int32_t>> customer_to_nation(CUSTOMER_ROWS);
    for (size_t i = 0; i < CUSTOMER_ROWS; ++i) {
        auto* entry = customer_to_nation.find(c_custkey[i]);
        if (entry) {
            entry->push_back(c_nationkey[i]);
        } else {
            std::vector<int32_t> vec;
            vec.push_back(c_nationkey[i]);
            customer_to_nation.insert(c_custkey[i], vec);
        }
    }

    // Build orders hash map (15M unique orders)
    CompactHashTable<int32_t, std::vector<int32_t>> orders_map(ORDERS_ROWS);
    for (size_t i = 0; i < ORDERS_ROWS; ++i) {
        auto* entry = orders_map.find(o_orderkey[i]);
        if (entry) {
            entry->push_back(o_custkey[i]);
        } else {
            std::vector<int32_t> vec;
            vec.push_back(o_custkey[i]);
            orders_map.insert(o_orderkey[i], vec);
        }
    }

#ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double build_ms = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] build: %.2f ms\n", build_ms);
#endif

    // ========================================================================
    // 4. MAIN QUERY EXECUTION: SCAN LINEITEM WITH MULTI-WAY JOINS
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Result aggregation: {supp_nation, cust_nation, l_year} -> revenue
    // Use thread-local aggregation for parallel execution
    std::map<std::tuple<std::string, std::string, int>, int64_t> results;

    // Parallel lineitem scan with thread-local results
    int num_threads = omp_get_max_threads();
    std::vector<std::map<std::tuple<std::string, std::string, int>, int64_t>> local_results(num_threads);

    #pragma omp parallel for schedule(dynamic, 10000)
    for (size_t i = 0; i < LINEITEM_ROWS; ++i) {
        // Filter: l_shipdate in range
        if (l_shipdate[i] < date_1995_01_01 || l_shipdate[i] > date_1996_12_31) {
            continue;
        }

        int32_t l_ok = l_orderkey[i];
        int32_t l_sk = l_suppkey[i];
        int thread_id = omp_get_thread_num();

        // Join with orders to get o_custkey
        auto* orders_entry = orders_map.find(l_ok);
        if (!orders_entry) continue;
        for (int32_t o_ck : *orders_entry) {
            // Join with customer to get c_nationkey
            auto* customer_entry = customer_to_nation.find(o_ck);
            if (!customer_entry) continue;
            for (int32_t c_nk : *customer_entry) {
                // Filter: customer nation must be FRANCE or GERMANY
                if (c_nk != france_key && c_nk != germany_key) continue;

                // Join with supplier to get s_nationkey
                auto* supplier_entry = supplier_to_nation.find(l_sk);
                if (!supplier_entry) continue;
                for (int32_t s_nk : *supplier_entry) {
                    // Filter: supplier nation must be FRANCE or GERMANY
                    if (s_nk != france_key && s_nk != germany_key) continue;

                    // Check nation pair is one of {FRANCE, GERMANY} or {GERMANY, FRANCE}
                    if (!((s_nk == france_key && c_nk == germany_key) ||
                          (s_nk == germany_key && c_nk == france_key))) {
                        continue;
                    }

                    // Compute volume = l_extendedprice * (1 - l_discount)
                    // Keep full precision: don't divide by 100 until final output
                    int64_t volume_unscaled = l_extendedprice[i] * (100 - l_discount[i]);

                    // Extract year from l_shipdate (optimized with fewer branches)
                    int32_t epoch_days = l_shipdate[i];
                    int year = 1970;
                    int day_count = epoch_days;
                    while (day_count >= 365) {
                        int is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
                        int days_in_year = is_leap ? 366 : 365;
                        if (day_count < days_in_year) break;
                        day_count -= days_in_year;
                        year++;
                    }

                    std::string supp_nation = nation_names[s_nk];
                    std::string cust_nation = nation_names[c_nk];
                    auto key = std::make_tuple(supp_nation, cust_nation, year);
                    local_results[thread_id][key] += volume_unscaled;
                }
            }
        }
    }

    // Merge thread-local results into global results
    for (int t = 0; t < num_threads; ++t) {
        for (auto& [key, revenue] : local_results[t]) {
            results[key] += revenue;
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double join_ms = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join_and_aggregate: %.2f ms\n", join_ms);
#endif

    // ========================================================================
    // 5. WRITE RESULTS TO CSV
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q7.csv";
    std::ofstream out(output_path);
    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_path << std::endl;
        exit(1);
    }

    out << "supp_nation,cust_nation,l_year,revenue\n";
    for (auto& [key, revenue] : results) {
        auto [supp_nation, cust_nation, l_year] = key;
        double revenue_double = static_cast<double>(revenue) / 10000.0;  // Unscaled by 100, divide by 10000 for 4 decimals
        char buf[256];
        snprintf(buf, 256, "%s,%s,%d,%.4f\n", supp_nation.c_str(), cust_nation.c_str(),
                 l_year, revenue_double);
        out << buf;
    }
    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    std::cout << "Q7 execution complete. Results written to " << output_path << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q7(gendb_dir, results_dir);
    return 0;
}
#endif
