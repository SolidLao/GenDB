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

// Zone map entry for lineitem.l_shipdate
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t start_row;
    uint32_t end_row;
};
static_assert(sizeof(ZoneMapEntry) == 16);

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

// Load zone map for selective block pruning
std::vector<ZoneMapEntry> load_zone_map(const std::string& filepath) {
    std::vector<ZoneMapEntry> zones;
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Warning: Failed to open zone map: " << filepath << std::endl;
        return zones;
    }

    struct stat st;
    fstat(fd, &st);
    size_t num_zones = st.st_size / sizeof(ZoneMapEntry);

    zones.resize(num_zones);
    ssize_t bytes_read = read(fd, zones.data(), st.st_size);
    close(fd);

    if (bytes_read != st.st_size) {
        std::cerr << "Warning: Zone map read mismatch" << std::endl;
        zones.clear();
    }

    return zones;
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

    // Load lineitem columns in parallel
    std::vector<int32_t> l_orderkey, l_suppkey, l_shipdate;
    std::vector<int64_t> l_extendedprice, l_discount;
    std::vector<ZoneMapEntry> l_shipdate_zones;
    std::vector<bool> l_shipdate_blocks_active;

    #pragma omp parallel sections
    {
        #pragma omp section
        {
            l_shipdate = mmap_load<int32_t>(
                gendb_dir + "/lineitem/l_shipdate.bin", LINEITEM_ROWS);
        }
        #pragma omp section
        {
            l_orderkey = mmap_load<int32_t>(
                gendb_dir + "/lineitem/l_orderkey.bin", LINEITEM_ROWS);
        }
        #pragma omp section
        {
            l_suppkey = mmap_load<int32_t>(
                gendb_dir + "/lineitem/l_suppkey.bin", LINEITEM_ROWS);
        }
        #pragma omp section
        {
            l_extendedprice = mmap_load<int64_t>(
                gendb_dir + "/lineitem/l_extendedprice.bin", LINEITEM_ROWS);
        }
        #pragma omp section
        {
            l_discount = mmap_load<int64_t>(
                gendb_dir + "/lineitem/l_discount.bin", LINEITEM_ROWS);
        }
        #pragma omp section
        {
            l_shipdate_zones = load_zone_map(
                gendb_dir + "/indexes/lineitem_l_shipdate_zone.bin");
        }
    }

    // Compute zone map pruning for date range: [1995-01-01, 1996-12-31]
    int32_t date_1995_01_01_val = compute_epoch_days(1995, 1, 1);
    int32_t date_1996_12_31_val = compute_epoch_days(1996, 12, 31);

    l_shipdate_blocks_active.resize(l_shipdate_zones.size());
    for (size_t z = 0; z < l_shipdate_zones.size(); ++z) {
        // Skip block if entirely outside range [date_1995_01_01, date_1996_12_31]
        if (l_shipdate_zones[z].max_val < date_1995_01_01_val ||
            l_shipdate_zones[z].min_val > date_1996_12_31_val) {
            l_shipdate_blocks_active[z] = false;
        } else {
            l_shipdate_blocks_active[z] = true;
        }
    }

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

    // Hash function for open-addressing (fibonacci hashing)
    auto hash_fn = [](int32_t key) -> size_t {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    };

    // ========================================================================
    // 3. BUILD HASH TABLES FOR JOINS (LOAD PRE-BUILT INDEXES)
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
#endif

    // Build hash maps for lookups using compact open-addressing hash tables
    // These are much faster than std::unordered_map due to better cache locality
    struct SupplierEntry {
        int32_t suppkey;
        int32_t nationkey;
        bool occupied = false;
    };
    struct CustomerEntry {
        int32_t custkey;
        int32_t nationkey;
        bool occupied = false;
    };
    struct OrderEntry {
        int32_t orderkey;
        int32_t custkey;
        bool occupied = false;
    };

    // Size hash tables to 75% load factor
    size_t supplier_size = 1;
    while (supplier_size < SUPPLIER_ROWS * 4 / 3) supplier_size <<= 1;
    std::vector<SupplierEntry> supplier_ht(supplier_size);

    size_t customer_size = 1;
    while (customer_size < CUSTOMER_ROWS * 4 / 3) customer_size <<= 1;
    std::vector<CustomerEntry> customer_ht(customer_size);

    size_t order_size = 1;
    while (order_size < ORDERS_ROWS * 4 / 3) order_size <<= 1;
    std::vector<OrderEntry> order_ht(order_size);

    size_t supplier_mask = supplier_size - 1;
    size_t customer_mask = customer_size - 1;
    size_t order_mask = order_size - 1;

    // Build supplier hash table: suppkey -> nationkey
    for (size_t i = 0; i < SUPPLIER_ROWS; ++i) {
        int32_t key = s_suppkey[i];
        int32_t val = s_nationkey[i];
        size_t idx = hash_fn(key) & supplier_mask;
        while (supplier_ht[idx].occupied && supplier_ht[idx].suppkey != key) {
            idx = (idx + 1) & supplier_mask;
        }
        supplier_ht[idx] = {key, val, true};
    }

    // Build customer hash table: custkey -> nationkey
    for (size_t i = 0; i < CUSTOMER_ROWS; ++i) {
        int32_t key = c_custkey[i];
        int32_t val = c_nationkey[i];
        size_t idx = hash_fn(key) & customer_mask;
        while (customer_ht[idx].occupied && customer_ht[idx].custkey != key) {
            idx = (idx + 1) & customer_mask;
        }
        customer_ht[idx] = {key, val, true};
    }

    // Build orders hash table: orderkey -> custkey
    for (size_t i = 0; i < ORDERS_ROWS; ++i) {
        int32_t key = o_orderkey[i];
        int32_t val = o_custkey[i];
        size_t idx = hash_fn(key) & order_mask;
        while (order_ht[idx].occupied && order_ht[idx].orderkey != key) {
            idx = (idx + 1) & order_mask;
        }
        order_ht[idx] = {key, val, true};
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

    // Process only active blocks identified by zone map
    for (size_t z = 0; z < l_shipdate_zones.size(); ++z) {
        if (!l_shipdate_blocks_active[z]) continue;

        uint32_t block_start = l_shipdate_zones[z].start_row;
        uint32_t block_end = l_shipdate_zones[z].end_row;

        #pragma omp parallel for schedule(dynamic, 10000)
        for (uint32_t i = block_start; i < block_end; ++i) {
            // Filter: l_shipdate in range
            if (l_shipdate[i] < date_1995_01_01_val || l_shipdate[i] > date_1996_12_31_val) {
                continue;
            }

        int thread_id = omp_get_thread_num();

        int32_t l_ok = l_orderkey[i];
        int32_t l_sk = l_suppkey[i];

        // Join with orders to get o_custkey using open-addressing hash table
        size_t order_idx = hash_fn(l_ok) & order_mask;
        int32_t o_ck = -1;
        while (order_ht[order_idx].occupied) {
            if (order_ht[order_idx].orderkey == l_ok) {
                o_ck = order_ht[order_idx].custkey;
                break;
            }
            order_idx = (order_idx + 1) & order_mask;
        }
        if (o_ck == -1) continue;

        // Join with customer to get c_nationkey
        size_t customer_idx = hash_fn(o_ck) & customer_mask;
        int32_t c_nk = -1;
        while (customer_ht[customer_idx].occupied) {
            if (customer_ht[customer_idx].custkey == o_ck) {
                c_nk = customer_ht[customer_idx].nationkey;
                break;
            }
            customer_idx = (customer_idx + 1) & customer_mask;
        }
        if (c_nk == -1) continue;

        // Filter: customer nation must be FRANCE or GERMANY
        if (c_nk != france_key && c_nk != germany_key) continue;

        // Join with supplier to get s_nationkey
        size_t supplier_idx = hash_fn(l_sk) & supplier_mask;
        int32_t s_nk = -1;
        while (supplier_ht[supplier_idx].occupied) {
            if (supplier_ht[supplier_idx].suppkey == l_sk) {
                s_nk = supplier_ht[supplier_idx].nationkey;
                break;
            }
            supplier_idx = (supplier_idx + 1) & supplier_mask;
        }
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
        }  // End of omp parallel for
    }  // End of zone map block loop

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
