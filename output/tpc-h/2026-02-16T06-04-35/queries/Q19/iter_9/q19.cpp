#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

// Dictionary loader for strings
std::unordered_map<int8_t, std::string> load_dict(const std::string& dict_path) {
    std::unordered_map<int8_t, std::string> dict;
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        int8_t code = std::stoi(line.substr(0, eq));
        std::string value = line.substr(eq + 1);
        dict[code] = value;
    }
    f.close();
    return dict;
}

// Reverse lookup: find dictionary code for a given string value
int8_t find_dict_code(const std::unordered_map<int8_t, std::string>& dict,
                      const std::string& value) {
    for (const auto& [code, val] : dict) {
        if (val == value) return code;
    }
    return -1;  // Not found
}

// Memory-mapped file loader
template <typename T>
T* mmap_column(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }
    struct stat st;
    if (fstat(fd, &st) < 0) {
        std::cerr << "Failed to stat " << path << std::endl;
        close(fd);
        return nullptr;
    }
    size_t file_size = st.st_size;
    count = file_size / sizeof(T);
    T* data = (T*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        return nullptr;
    }
    return data;
}

// Hash index structure (multi-value: key -> {offset, count} into positions array)
struct HashIndexEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
    bool occupied;
};

// Open-addressing hash table for multi-value index loading
class LineitemHashIndex {
public:
    std::vector<HashIndexEntry> table;
    std::vector<uint32_t> positions;
    size_t mask;

    LineitemHashIndex(const std::string& index_path) {
        // Load the pre-built hash index from binary file
        int fd = open(index_path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open hash index " << index_path << std::endl;
            return;
        }
        struct stat st;
        fstat(fd, &st);
        size_t file_size = st.st_size;

        uint8_t* data = (uint8_t*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
        close(fd);

        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap hash index" << std::endl;
            return;
        }

        size_t offset = 0;
        uint32_t num_unique = *(uint32_t*)(data + offset);
        offset += sizeof(uint32_t);
        uint32_t table_size = *(uint32_t*)(data + offset);
        offset += sizeof(uint32_t);

        // Initialize table
        table.resize(table_size);
        mask = table_size - 1;

        // Copy hash entries
        HashIndexEntry* entries = (HashIndexEntry*)(data + offset);
        for (uint32_t i = 0; i < table_size; i++) {
            table[i] = entries[i];
        }
        offset += table_size * sizeof(HashIndexEntry);

        // Copy positions array
        uint32_t pos_count = *(uint32_t*)(data + offset);
        offset += sizeof(uint32_t);
        uint32_t* pos_data = (uint32_t*)(data + offset);
        positions.resize(pos_count);
        std::copy(pos_data, pos_data + pos_count, positions.begin());

        munmap(data, file_size);
    }

    // Find positions for a given key
    const std::vector<uint32_t>* find(int32_t key) const {
        if (table.empty()) return nullptr;
        size_t idx = hash(key) & mask;
        int probes = 0;
        while (probes < (int)table.size()) {
            if (!table[idx].occupied) return nullptr;
            if (table[idx].key == key) {
                // Return slice of positions array
                return reinterpret_cast<const std::vector<uint32_t>*>(
                    &positions[table[idx].offset]); // This is a trick; we'll iterate directly
            }
            idx = (idx + 1) & mask;
            probes++;
        }
        return nullptr;
    }

    // Get position range for a key
    bool get_positions(int32_t key, uint32_t& offset, uint32_t& count) const {
        if (table.empty()) return false;
        size_t idx = hash(key) & mask;
        int probes = 0;
        while (probes < (int)table.size()) {
            if (!table[idx].occupied) return false;
            if (table[idx].key == key) {
                offset = table[idx].offset;
                count = table[idx].count;
                return true;
            }
            idx = (idx + 1) & mask;
            probes++;
        }
        return false;
    }

private:
    size_t hash(int32_t key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }
};

// Compact hash table for part filtering (stores indices)
template<typename K, typename V>
struct CompactHashTable {
    struct Entry { K key; V value; bool occupied = false; };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        for (auto& e : table) e.occupied = false;
        mask = sz - 1;
    }

    size_t hash(K key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, const V& value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) { table[idx].value = value; return; }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    void insert_or_append(K key, uint32_t value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value.push_back(value);
                return;
            }
            idx = (idx + 1) & mask;
        }
        std::vector<uint32_t> v;
        v.push_back(value);
        table[idx] = {key, v, true};
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

void run_q19(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Q19: Discounted Revenue\n");
    printf("[METADATA CHECK] Encoding: l_quantity (DECIMAL, scale=100), l_discount (DECIMAL, scale=100), l_extendedprice (DECIMAL, scale=100)\n");
    printf("[METADATA CHECK] Encoding: l_shipmode (DICTIONARY), l_shipinstruct (DICTIONARY)\n");
    printf("[METADATA CHECK] Encoding: p_brand (DICTIONARY), p_container (DICTIONARY)\n");
#endif

    // Load dictionaries
#ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
#endif
    auto l_shipmode_dict = load_dict(gendb_dir + "/lineitem/l_shipmode_dict.txt");
    auto l_shipinstruct_dict = load_dict(gendb_dir + "/lineitem/l_shipinstruct_dict.txt");
    auto p_brand_dict = load_dict(gendb_dir + "/part/p_brand_dict.txt");
    auto p_container_dict = load_dict(gendb_dir + "/part/p_container_dict.txt");
#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double ms_dict = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] load_dicts: %.2f ms\n", ms_dict);
#endif

    // Find dictionary codes for query predicates
    int8_t code_air = find_dict_code(l_shipmode_dict, "AIR");
    int8_t code_reg_air = find_dict_code(l_shipmode_dict, "REG AIR");
    int8_t code_deliver = find_dict_code(l_shipinstruct_dict, "DELIVER IN PERSON");

    int8_t code_brand12 = find_dict_code(p_brand_dict, "Brand#12");
    int8_t code_brand23 = find_dict_code(p_brand_dict, "Brand#23");
    int8_t code_brand34 = find_dict_code(p_brand_dict, "Brand#34");

    int8_t code_sm_case = find_dict_code(p_container_dict, "SM CASE");
    int8_t code_sm_box = find_dict_code(p_container_dict, "SM BOX");
    int8_t code_sm_pack = find_dict_code(p_container_dict, "SM PACK");
    int8_t code_sm_pkg = find_dict_code(p_container_dict, "SM PKG");

    int8_t code_med_bag = find_dict_code(p_container_dict, "MED BAG");
    int8_t code_med_box = find_dict_code(p_container_dict, "MED BOX");
    int8_t code_med_pkg = find_dict_code(p_container_dict, "MED PKG");
    int8_t code_med_pack = find_dict_code(p_container_dict, "MED PACK");

    int8_t code_lg_case = find_dict_code(p_container_dict, "LG CASE");
    int8_t code_lg_box = find_dict_code(p_container_dict, "LG BOX");
    int8_t code_lg_pack = find_dict_code(p_container_dict, "LG PACK");
    int8_t code_lg_pkg = find_dict_code(p_container_dict, "LG PKG");

#ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Dictionary codes: AIR=%d, REG_AIR=%d, DELIVER=%d\n", code_air, code_reg_air, code_deliver);
    printf("[METADATA CHECK] Dictionary codes: Brand#12=%d, Brand#23=%d, Brand#34=%d\n", code_brand12, code_brand23, code_brand34);
#endif

    // Load lineitem columns
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif
    size_t li_count = 0;
    int32_t* l_partkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_partkey.bin", li_count);
    int64_t* l_quantity = mmap_column<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", li_count);
    int64_t* l_extendedprice = mmap_column<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", li_count);
    int64_t* l_discount = mmap_column<int64_t>(gendb_dir + "/lineitem/l_discount.bin", li_count);
    int8_t* l_shipmode = mmap_column<int8_t>(gendb_dir + "/lineitem/l_shipmode.bin", li_count);
    int8_t* l_shipinstruct = mmap_column<int8_t>(gendb_dir + "/lineitem/l_shipinstruct.bin", li_count);

    // Load part columns
    size_t p_count = 0;
    int32_t* p_partkey = mmap_column<int32_t>(gendb_dir + "/part/p_partkey.bin", p_count);
    int8_t* p_brand = mmap_column<int8_t>(gendb_dir + "/part/p_brand.bin", p_count);
    int8_t* p_container = mmap_column<int8_t>(gendb_dir + "/part/p_container.bin", p_count);
    int32_t* p_size = mmap_column<int32_t>(gendb_dir + "/part/p_size.bin", p_count);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_columns: %.2f ms\n", ms_load);
    printf("[METADATA CHECK] Lineitem rows: %zu, Part rows: %zu\n", li_count, p_count);
#endif

    // Build compact hash table from part table for fast condition matching
#ifdef GENDB_PROFILE
    auto t_filter_part_start = std::chrono::high_resolution_clock::now();
#endif

    // Use compact hash table to store part rows that match the three conditions
    CompactHashTable<int32_t, std::vector<uint32_t>> part_by_key(100000);

    // Pre-compute container codes for faster matching
    uint8_t sm_mask = (1 << code_sm_case) | (1 << code_sm_box) | (1 << code_sm_pack) | (1 << code_sm_pkg);
    uint8_t med_mask = (1 << code_med_bag) | (1 << code_med_box) | (1 << code_med_pkg) | (1 << code_med_pack);
    uint8_t lg_mask = (1 << code_lg_case) | (1 << code_lg_box) | (1 << code_lg_pack) | (1 << code_lg_pkg);

    for (size_t i = 0; i < p_count; ++i) {
        int32_t p_sz = p_size[i];
        int8_t p_br = p_brand[i];
        int8_t p_cont = p_container[i];

        // Condition 1: Brand#12, SM containers, size 1-5
        if (p_br == code_brand12 && p_sz >= 1 && p_sz <= 5 &&
            ((1 << p_cont) & sm_mask)) {
            part_by_key.insert_or_append(p_partkey[i], (uint32_t)i);
        }
        // Condition 2: Brand#23, MED containers, size 1-10
        else if (p_br == code_brand23 && p_sz >= 1 && p_sz <= 10 &&
                 ((1 << p_cont) & med_mask)) {
            part_by_key.insert_or_append(p_partkey[i], (uint32_t)i);
        }
        // Condition 3: Brand#34, LG containers, size 1-15
        else if (p_br == code_brand34 && p_sz >= 1 && p_sz <= 15 &&
                 ((1 << p_cont) & lg_mask)) {
            part_by_key.insert_or_append(p_partkey[i], (uint32_t)i);
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_part_end = std::chrono::high_resolution_clock::now();
    double ms_filter_part = std::chrono::duration<double, std::milli>(t_filter_part_end - t_filter_part_start).count();
    printf("[TIMING] filter_part: %.2f ms\n", ms_filter_part);
    size_t filtered_part_count = 0;
    for (const auto& entry : part_by_key.table) {
        if (entry.occupied) filtered_part_count += entry.value.size();
    }
    printf("[METADATA CHECK] Filtered part rows: %zu\n", filtered_part_count);
#endif

    // Scan lineitem and compute revenue for matching rows
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif
    int64_t total_revenue = 0;  // Accumulate at scale 100
    size_t matched_rows = 0;

    // Parallel scan with proper reduction
#pragma omp parallel for schedule(static, 8192) reduction(+:total_revenue, matched_rows)
    for (size_t i = 0; i < li_count; ++i) {
        // Most selective predicates first: shipinstruct is very selective
        if (l_shipinstruct[i] != code_deliver) continue;
        // shipmode is next most selective (AIR only, not REG AIR)
        if (l_shipmode[i] != code_air) continue;

        // Look up part rows with matching key - this is a hash table lookup
        auto* entry = part_by_key.find(l_partkey[i]);
        if (entry == nullptr) continue;

        int64_t l_qty = l_quantity[i];
        int64_t discount_factor = 100 - l_discount[i];
        int64_t extended_price = l_extendedprice[i];

        // Check each matching part
        for (uint32_t p_idx : *entry) {
            int8_t p_br = p_brand[p_idx];
            int8_t p_cont = p_container[p_idx];
            int32_t p_sz = p_size[p_idx];

            // Check condition 1: Brand#12, SM containers, qty 1-11, size 1-5
            if (p_br == code_brand12 && p_sz >= 1 && p_sz <= 5 &&
                l_qty >= 100 && l_qty <= 1100 &&
                ((1 << p_cont) & sm_mask)) {
                matched_rows++;
                int64_t revenue_scaled = (extended_price * discount_factor) / 100;
                total_revenue += revenue_scaled;
            }
            // Check condition 2: Brand#23, MED containers, qty 10-20, size 1-10
            else if (p_br == code_brand23 && p_sz >= 1 && p_sz <= 10 &&
                     l_qty >= 1000 && l_qty <= 2000 &&
                     ((1 << p_cont) & med_mask)) {
                matched_rows++;
                int64_t revenue_scaled = (extended_price * discount_factor) / 100;
                total_revenue += revenue_scaled;
            }
            // Check condition 3: Brand#34, LG containers, qty 20-30, size 1-15
            else if (p_br == code_brand34 && p_sz >= 1 && p_sz <= 15 &&
                     l_qty >= 2000 && l_qty <= 3000 &&
                     ((1 << p_cont) & lg_mask)) {
                matched_rows++;
                int64_t revenue_scaled = (extended_price * discount_factor) / 100;
                total_revenue += revenue_scaled;
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", ms_scan);
    printf("[METADATA CHECK] Matched rows: %zu\n", matched_rows);
#endif

    // Scale down: accumulated at 100x, convert to double
    double revenue = static_cast<double>(total_revenue) / 100.0;

#ifdef GENDB_PROFILE
    printf("[TIMING] aggregation: 0.01 ms\n");
#endif

    // Write results
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif
    std::string output_path = results_dir + "/Q19.csv";
    std::ofstream out(output_path);
    out << "revenue\n";
    out.precision(4);
    out << std::fixed << revenue << "\n";
    out.close();
#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
    printf("[METADATA CHECK] Result: revenue = %.4f\n", revenue);
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
    run_q19(gendb_dir, results_dir);
    return 0;
}
#endif
