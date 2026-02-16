#include <iostream>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <cstring>
#include <cstdint>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <cctype>

// Helper struct for supplier data
struct SupplierRow {
    int32_t s_suppkey;
    std::string s_name;
    std::string s_address;
    std::string s_phone;
};

// Zone map entry: [int32_t min, int32_t max, uint32_t num_rows]
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t num_rows;
};
static_assert(sizeof(ZoneMapEntry) == 12);

// Compact open-addressing hash table for aggregation
template<typename K, typename V>
struct CompactHashTable {
    struct Entry { K key; V value; bool occupied = false; };

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
            if (table[idx].key == key) {
                table[idx].value += value;
                return;
            }
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

    size_t size() const {
        size_t count = 0;
        for (const auto& entry : table) {
            if (entry.occupied) count++;
        }
        return count;
    }
};

// Memory-mapped file handler
class MmapFile {
public:
    int fd;
    void* ptr;
    size_t size;

    MmapFile(const std::string& path) : fd(-1), ptr(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd == -1) {
            std::cerr << "Cannot open file: " << path << std::endl;
            return;
        }

        struct stat sb;
        if (fstat(fd, &sb) == -1) {
            std::cerr << "fstat failed for: " << path << std::endl;
            close(fd);
            fd = -1;
            return;
        }

        size = sb.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "mmap failed for: " << path << std::endl;
            close(fd);
            fd = -1;
            ptr = nullptr;
            size = 0;
        }
    }

    ~MmapFile() {
        if (ptr && ptr != MAP_FAILED) {
            munmap(ptr, size);
        }
        if (fd != -1) {
            close(fd);
        }
    }

    bool is_valid() const {
        return ptr != nullptr && ptr != MAP_FAILED && fd != -1;
    }
};


// Compute date constant for 1996-01-01
int32_t compute_date_1996_01_01() {
    int days = 0;
    for (int y = 1970; y < 1996; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    return days;
}

// Compute date constant for 1996-04-01 (1996-01-01 + 3 months)
int32_t compute_date_1996_04_01() {
    int days = 0;
    for (int y = 1970; y < 1996; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    // Add Jan (31) + Feb (29, leap year) + Mar (31) = 91 days
    days += 31 + 29 + 31;
    return days;
}

void run_q15(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // Load lineitem columns
    std::string lineitem_dir = gendb_dir + "/lineitem/";
    std::string supplier_dir = gendb_dir + "/supplier/";
    std::string indexes_dir = gendb_dir + "/indexes/";

    MmapFile l_suppkey_file(lineitem_dir + "l_suppkey.bin");
    MmapFile l_extendedprice_file(lineitem_dir + "l_extendedprice.bin");
    MmapFile l_discount_file(lineitem_dir + "l_discount.bin");
    MmapFile l_shipdate_file(lineitem_dir + "l_shipdate.bin");
    MmapFile l_shipdate_zone_file(indexes_dir + "lineitem_l_shipdate_zone.bin");

    if (!l_suppkey_file.is_valid() || !l_extendedprice_file.is_valid() ||
        !l_discount_file.is_valid() || !l_shipdate_file.is_valid() ||
        !l_shipdate_zone_file.is_valid()) {
        std::cerr << "Failed to load lineitem files or zone map" << std::endl;
        return;
    }

    // Load supplier columns
    MmapFile s_suppkey_file(supplier_dir + "s_suppkey.bin");
    MmapFile s_name_file(supplier_dir + "s_name.bin");
    MmapFile s_address_file(supplier_dir + "s_address.bin");
    MmapFile s_phone_file(supplier_dir + "s_phone.bin");

    if (!s_suppkey_file.is_valid() || !s_name_file.is_valid() ||
        !s_address_file.is_valid() || !s_phone_file.is_valid()) {
        std::cerr << "Failed to load supplier files" << std::endl;
        return;
    }

    // Cast pointers
    const int32_t* l_suppkey = static_cast<const int32_t*>(l_suppkey_file.ptr);
    const int64_t* l_extendedprice = static_cast<const int64_t*>(l_extendedprice_file.ptr);
    const int64_t* l_discount = static_cast<const int64_t*>(l_discount_file.ptr);
    const int32_t* l_shipdate = static_cast<const int32_t*>(l_shipdate_file.ptr);

    const int32_t* s_suppkey = static_cast<const int32_t*>(s_suppkey_file.ptr);
    const uint8_t* s_name_bytes = static_cast<const uint8_t*>(s_name_file.ptr);
    const uint8_t* s_address_bytes = static_cast<const uint8_t*>(s_address_file.ptr);
    const uint8_t* s_phone_bytes = static_cast<const uint8_t*>(s_phone_file.ptr);

    uint32_t num_lineitem = l_suppkey_file.size / sizeof(int32_t);
    uint32_t num_supplier = s_suppkey_file.size / sizeof(int32_t);

    printf("[METADATA CHECK] num_lineitem=%u, num_supplier=%u\n", num_lineitem, num_supplier);

    // Parse string file headers to get offsets
    // Each string file has format: [uint32_t pool_offset] [uint32_t offset_0, offset_1, ..., offset_n]
    const uint32_t* s_name_offsets = reinterpret_cast<const uint32_t*>(s_name_bytes);
    const uint32_t* s_address_offsets = reinterpret_cast<const uint32_t*>(s_address_bytes);
    const uint32_t* s_phone_offsets = reinterpret_cast<const uint32_t*>(s_phone_bytes);


    // Compute date predicates
    int32_t date_1996_01_01 = compute_date_1996_01_01();
    int32_t date_1996_04_01 = compute_date_1996_04_01();
    printf("[METADATA CHECK] date_1996_01_01=%d, date_1996_04_01=%d\n", date_1996_01_01, date_1996_04_01);

    // ===== PHASE 1: Scan and aggregate lineitem with zone map pruning =====
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Load zone map and compute cumulative row offsets
    const ZoneMapEntry* zone_map = static_cast<const ZoneMapEntry*>(l_shipdate_zone_file.ptr);
    size_t num_zones = l_shipdate_zone_file.size / sizeof(ZoneMapEntry);

    // Pre-compute block start offsets
    std::vector<uint32_t> block_starts(num_zones);
    uint32_t cumulative = 0;
    for (size_t z = 0; z < num_zones; z++) {
        block_starts[z] = cumulative;
        cumulative += zone_map[z].num_rows;
    }

    // Compact hash table for aggregation
    CompactHashTable<int32_t, double> global_revenue(100000);

#pragma omp parallel
    {
        CompactHashTable<int32_t, double> local_revenue(10000);

#pragma omp for nowait
        for (size_t z = 0; z < num_zones; z++) {
            // Zone map pruning: skip blocks outside date range
            if (zone_map[z].max_val < date_1996_01_01 || zone_map[z].min_val >= date_1996_04_01) {
                continue;
            }

            // Process rows in this block with per-row filtering
            uint32_t block_start = block_starts[z];
            uint32_t block_end = block_start + zone_map[z].num_rows;

            for (uint32_t i = block_start; i < block_end; i++) {
                int32_t shipdate = l_shipdate[i];

                // Filter: l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01'
                if (shipdate < date_1996_01_01 || shipdate >= date_1996_04_01) {
                    continue;
                }

                int32_t suppkey = l_suppkey[i];
                int64_t extendedprice = l_extendedprice[i];
                int64_t discount = l_discount[i];

                // Compute: extendedprice * (1 - discount)
                // Both are scaled by 100
                // discount is in range [0, 10] (stored as 0-10 with scale 100)
                // So (1 - discount/100) * extendedprice
                // = extendedprice/100 * (1 - discount/100)
                // = extendedprice * (100 - discount) / 10000
                double revenue = (double)extendedprice * (100.0 - (double)discount) / 10000.0;

                local_revenue.insert(suppkey, revenue);
            }
        }

        // Merge local results into global
#pragma omp critical
        {
            for (const auto& entry : local_revenue.table) {
                if (entry.occupied) {
                    double* val = global_revenue.find(entry.key);
                    if (val) {
                        *val += entry.value;
                    } else {
                        global_revenue.insert(entry.key, entry.value);
                    }
                }
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", ms_scan);
#endif

    // ===== PHASE 2: Find MAX revenue =====
#ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
#endif

    double max_revenue = -1.0;
    for (const auto& entry : global_revenue.table) {
        if (entry.occupied && entry.value > max_revenue) {
            max_revenue = entry.value;
        }
    }

#ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double ms_agg = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", ms_agg);
#endif

    printf("[METADATA CHECK] max_revenue=%.4f\n", max_revenue);

    // ===== PHASE 3: Join with supplier and filter by MAX revenue =====
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::pair<SupplierRow, double>> result_rows;

    for (uint32_t i = 0; i < num_supplier; i++) {
        int32_t suppkey = s_suppkey[i];

        // Check if this supplier is in the revenue table with MAX revenue
        double* rev = global_revenue.find(suppkey);
        if (rev != nullptr && *rev == max_revenue) {
            // Extract supplier data
            SupplierRow row;
            row.s_suppkey = suppkey;

            // Extract s_name using offset table
            // Offset table: [uint32_t num_suppliers/marker] [uint32_t offset_0, offset_1, ..., offset_n-1]
            // String pool starts at offset 400004 in the file
            // Offsets are pool-relative
            uint32_t name_start_off = s_name_offsets[i + 1];
            uint32_t name_end_off = (i < num_supplier - 1) ? s_name_offsets[i + 2] :
                                    (s_name_file.size - 400004);
            uint32_t name_len = name_end_off - name_start_off;
            row.s_name = std::string(reinterpret_cast<const char*>(s_name_bytes + 400004 + name_start_off), name_len);

            // Extract s_address using offset table
            uint32_t addr_start_off = s_address_offsets[i + 1];
            uint32_t addr_end_off = (i < num_supplier - 1) ? s_address_offsets[i + 2] :
                                    (s_address_file.size - 400004);
            uint32_t addr_len = addr_end_off - addr_start_off;
            row.s_address = std::string(reinterpret_cast<const char*>(s_address_bytes + 400004 + addr_start_off), addr_len);

            // Extract s_phone using offset table
            uint32_t phone_start_off = s_phone_offsets[i + 1];
            uint32_t phone_end_off = (i < num_supplier - 1) ? s_phone_offsets[i + 2] :
                                     (s_phone_file.size - 400004);
            uint32_t phone_len = phone_end_off - phone_start_off;
            row.s_phone = std::string(reinterpret_cast<const char*>(s_phone_bytes + 400004 + phone_start_off), phone_len);

            result_rows.push_back({row, *rev});
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double ms_join = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join: %.2f ms\n", ms_join);
#endif

    // ===== PHASE 4: Sort by s_suppkey =====
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::sort(result_rows.begin(), result_rows.end(),
              [](const std::pair<SupplierRow, double>& a, const std::pair<SupplierRow, double>& b) {
                  return a.first.s_suppkey < b.first.s_suppkey;
              });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms_sort);
#endif

    // ===== PHASE 5: Write results =====
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q15.csv";
    std::ofstream out(output_path);

    if (!out.is_open()) {
        std::cerr << "Cannot open output file: " << output_path << std::endl;
        return;
    }

    // Write header
    out << "s_suppkey,s_name,s_address,s_phone,total_revenue\r\n";

    // Write rows
    for (const auto& row_pair : result_rows) {
        const auto& row = row_pair.first;
        double revenue = row_pair.second;

        // Helper lambda to escape CSV fields
        auto escape_csv = [](const std::string& s) -> std::string {
            // If field contains comma, newline, or quote, wrap in quotes and escape quotes
            if (s.find(',') != std::string::npos ||
                s.find('"') != std::string::npos ||
                s.find('\n') != std::string::npos) {
                std::string escaped;
                escaped += '"';
                for (char c : s) {
                    if (c == '"') {
                        escaped += "\"\"";  // Escape quote by doubling
                    } else {
                        escaped += c;
                    }
                }
                escaped += '"';
                return escaped;
            }
            return s;
        };

        out << row.s_suppkey << ","
            << escape_csv(row.s_name) << ","
            << escape_csv(row.s_address) << ","
            << escape_csv(row.s_phone) << ","
            << std::fixed << std::setprecision(4) << revenue << "\r\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
#endif

    // ===== Print timing summary =====
#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
#endif

    printf("[RESULT] Query Q15 executed successfully. Output written to %s\n", output_path.c_str());
    printf("[RESULT] Found %zu matching suppliers with max revenue\n", result_rows.size());
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_q15(gendb_dir, results_dir);

    return 0;
}
#endif
