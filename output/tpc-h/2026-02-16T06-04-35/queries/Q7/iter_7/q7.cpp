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
#include <cmath>

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

// Optimized hash index for single-value lookups (suppkey->nationkey, custkey->nationkey, orderkey->custkey)
struct SimpleHashIndex {
    std::unordered_map<int32_t, int32_t> table;  // key -> value

    static SimpleHashIndex load(const std::string& filepath) {
        SimpleHashIndex idx;
        int fd = open(filepath.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << filepath << std::endl;
            exit(1);
        }

        // Read header
        uint32_t num_unique, table_size;
        if (read(fd, &num_unique, sizeof(uint32_t)) < (ssize_t)sizeof(uint32_t)) {
            std::cerr << "Failed to read num_unique" << std::endl;
            close(fd);
            exit(1);
        }
        if (read(fd, &table_size, sizeof(uint32_t)) < (ssize_t)sizeof(uint32_t)) {
            std::cerr << "Failed to read table_size" << std::endl;
            close(fd);
            exit(1);
        }

        // Pre-size the hash table
        idx.table.reserve(num_unique);

        // Read hash entries: [key:int32_t, offset:uint32_t, count:uint32_t] * table_size
        std::vector<char> entry_buf(table_size * 12);
        if (read(fd, entry_buf.data(), entry_buf.size()) < (ssize_t)entry_buf.size()) {
            std::cerr << "Failed to read hash entries" << std::endl;
            close(fd);
            exit(1);
        }

        // For single-value indexes, extract unique entries (those with count >= 1)
        for (uint32_t i = 0; i < table_size; ++i) {
            int32_t key;
            uint32_t offset, count;
            std::memcpy(&key, entry_buf.data() + i * 12, 4);
            std::memcpy(&offset, entry_buf.data() + i * 12 + 4, 4);
            std::memcpy(&count, entry_buf.data() + i * 12 + 8, 4);
            if (count > 0) {
                // For single-value indexes, store the first position offset
                idx.table[key] = static_cast<int32_t>(offset);
            }
        }

        // Read positions array: [uint32_t pos_count] [positions...]
        uint32_t pos_count;
        if (read(fd, &pos_count, sizeof(uint32_t)) < (ssize_t)sizeof(uint32_t)) {
            std::cerr << "Failed to read pos_count" << std::endl;
            close(fd);
            exit(1);
        }
        // We don't need to read the actual positions for single-value lookups

        close(fd);
        return idx;
    }

    int32_t find_value(int32_t key, int32_t not_found_value = -1) {
        auto it = table.find(key);
        if (it != table.end()) {
            return it->second;
        }
        return not_found_value;
    }

    bool contains(int32_t key) {
        return table.find(key) != table.end();
    }
};

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
        if (read(fd, &idx.num_unique, sizeof(uint32_t)) < (ssize_t)sizeof(uint32_t)) {
            std::cerr << "Failed to read num_unique" << std::endl;
            close(fd);
            exit(1);
        }
        if (read(fd, &idx.table_size, sizeof(uint32_t)) < (ssize_t)sizeof(uint32_t)) {
            std::cerr << "Failed to read table_size" << std::endl;
            close(fd);
            exit(1);
        }

        // Read hash entries: [key:int32_t, offset:uint32_t, count:uint32_t] * table_size
        std::vector<char> entry_buf(idx.table_size * 12); // 4 + 4 + 4 bytes per entry
        if (read(fd, entry_buf.data(), entry_buf.size()) < (ssize_t)entry_buf.size()) {
            std::cerr << "Failed to read hash entries" << std::endl;
            close(fd);
            exit(1);
        }

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
        if (read(fd, &pos_count, sizeof(uint32_t)) < (ssize_t)sizeof(uint32_t)) {
            std::cerr << "Failed to read pos_count" << std::endl;
            close(fd);
            exit(1);
        }
        idx.positions.resize(pos_count);
        if (read(fd, idx.positions.data(), pos_count * sizeof(uint32_t)) < (ssize_t)(pos_count * sizeof(uint32_t))) {
            std::cerr << "Failed to read positions" << std::endl;
            close(fd);
            exit(1);
        }

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

// Structure to hold mmap'd data with file handle
template<typename T>
struct MmappedData {
    T* ptr;
    size_t count;
    int fd;
    void* mmap_ptr;
    size_t mmap_size;

    MmappedData() : ptr(nullptr), count(0), fd(-1), mmap_ptr(nullptr), mmap_size(0) {}

    static MmappedData load(const std::string& filepath, size_t count) {
        MmappedData data;
        data.fd = open(filepath.c_str(), O_RDONLY);
        if (data.fd < 0) {
            std::cerr << "Failed to open: " << filepath << std::endl;
            exit(1);
        }

        data.count = count;
        data.mmap_size = count * sizeof(T);
        data.mmap_ptr = mmap(nullptr, data.mmap_size, PROT_READ, MAP_PRIVATE, data.fd, 0);
        if (data.mmap_ptr == MAP_FAILED) {
            std::cerr << "mmap failed for: " << filepath << std::endl;
            close(data.fd);
            exit(1);
        }

        data.ptr = static_cast<T*>(data.mmap_ptr);
        return data;
    }

    ~MmappedData() {
        if (mmap_ptr != nullptr && mmap_ptr != MAP_FAILED) {
            munmap(mmap_ptr, mmap_size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    T& operator[](size_t idx) {
        return ptr[idx];
    }

    const T& operator[](size_t idx) const {
        return ptr[idx];
    }

    size_t size() const {
        return count;
    }

    T* data() {
        return ptr;
    }

    const T* data() const {
        return ptr;
    }
};

// Legacy interface for compatibility
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
    // 3. LOAD PRE-BUILT HASH INDEXES FOR JOINS
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
#endif

    // Load pre-built hash indexes to skip hash table construction
    SimpleHashIndex supplier_idx = SimpleHashIndex::load(gendb_dir + "/indexes/supplier_s_nationkey_hash.bin");
    SimpleHashIndex orders_idx = SimpleHashIndex::load(gendb_dir + "/indexes/orders_o_custkey_hash.bin");
    SimpleHashIndex customer_idx = SimpleHashIndex::load(gendb_dir + "/indexes/customer_c_nationkey_hash.bin");

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

        // Join with orders to get o_custkey using pre-built hash index
        int32_t o_ck = orders_idx.find_value(l_ok, -1);
        if (o_ck == -1) continue;

        // Join with customer to get c_nationkey using pre-built hash index
        int32_t c_nk = customer_idx.find_value(o_ck, -1);
        if (c_nk == -1) continue;

        // Filter: customer nation must be FRANCE or GERMANY
        if (c_nk != france_key && c_nk != germany_key) continue;

        // Join with supplier to get s_nationkey using pre-built hash index
        int32_t s_nk = supplier_idx.find_value(l_sk, -1);
        if (s_nk == -1) continue;

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
