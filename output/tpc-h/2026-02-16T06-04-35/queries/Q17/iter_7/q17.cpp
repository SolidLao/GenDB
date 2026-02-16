#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdint>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <cmath>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <omp.h>

// Metadata check: Q17 uses scaled decimals (scale_factor=100) and dictionary-encoded strings
// l_quantity, l_extendedprice: int64_t scaled by 100
// p_brand, p_container: int8_t dictionary codes
// [METADATA CHECK] Codes loaded at runtime from dictionary files

struct MemoryMappedFile {
    void* data;
    size_t size;
    int fd;

    MemoryMappedFile(const std::string& path) : data(nullptr), size(0), fd(-1) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            return;
        }
        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            std::cerr << "Failed to fstat " << path << std::endl;
            close(fd);
            fd = -1;
            return;
        }
        size = sb.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            data = nullptr;
            close(fd);
            fd = -1;
        }
    }

    ~MemoryMappedFile() {
        if (data && size > 0) munmap(data, size);
        if (fd >= 0) close(fd);
    }

    bool isValid() const { return data != nullptr; }
};

// Parse dictionary file to find code for target string
int8_t findDictionaryCode(const std::string& dict_path, const std::string& target) {
    std::ifstream f(dict_path);
    if (!f) {
        std::cerr << "Failed to open dictionary " << dict_path << std::endl;
        return -1;
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            std::string value = line.substr(eq + 1);
            if (value == target) {
                return static_cast<int8_t>(std::stoi(line.substr(0, eq)));
            }
        }
    }
    return -1;
}

// Compact hash table for storing row indices per partkey
// Uses open addressing to eliminate pointer chasing overhead of unordered_map
template<typename K>
struct CompactHashTableWithVectors {
    struct Entry {
        K key;
        std::vector<size_t>* rows;
        bool occupied;
    };

    std::vector<Entry> table;
    std::vector<std::vector<size_t>> all_rows;
    size_t size_mask;
    size_t num_entries;

    CompactHashTableWithVectors(size_t expected_size) : num_entries(0) {
        // Size to next power of 2 for modulo via bitwise AND
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz, {0, nullptr, false});
        size_mask = sz - 1;
        all_rows.reserve(expected_size);
    }

    size_t hash(K key) const {
        // Fibonacci hashing for good distribution
        return ((size_t)key * 0x9E3779B97F4A7C15ULL) & size_mask;
    }

    std::vector<size_t>* find_or_create(K key) {
        size_t idx = hash(key);
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                return table[idx].rows;
            }
            idx = (idx + 1) & size_mask;
        }
        // Not found - create new entry
        all_rows.emplace_back();
        table[idx] = {key, &all_rows.back(), true};
        num_entries++;
        return table[idx].rows;
    }

    std::vector<size_t>* find(K key) {
        size_t idx = hash(key);
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                return table[idx].rows;
            }
            idx = (idx + 1) & size_mask;
        }
        return nullptr;
    }
};

// Compact hash table for aggregation (storing sum_qty and count)
template<typename K, typename V>
struct CompactAggregationTable {
    struct Entry {
        K key;
        V value;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t size_mask;
    size_t num_entries;

    CompactAggregationTable(size_t expected_size) : num_entries(0) {
        // Size to next power of 2
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz, {0, {0, 0}, false});
        size_mask = sz - 1;
    }

    size_t hash(K key) const {
        return ((size_t)key * 0x9E3779B97F4A7C15ULL) & size_mask;
    }

    V& find_or_create(K key) {
        size_t idx = hash(key);
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                return table[idx].value;
            }
            idx = (idx + 1) & size_mask;
        }
        table[idx].key = key;
        table[idx].occupied = true;
        num_entries++;
        return table[idx].value;
    }

    V* find(K key) {
        size_t idx = hash(key);
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                return &table[idx].value;
            }
            idx = (idx + 1) & size_mask;
        }
        return nullptr;
    }

    size_t size() const { return num_entries; }
};

void run_q17(const std::string& gendb_dir, const std::string& results_dir) {
    // Load binary column files
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    MemoryMappedFile lineitem_partkey_file(gendb_dir + "/lineitem/l_partkey.bin");
    MemoryMappedFile lineitem_quantity_file(gendb_dir + "/lineitem/l_quantity.bin");
    MemoryMappedFile lineitem_extendedprice_file(gendb_dir + "/lineitem/l_extendedprice.bin");
    MemoryMappedFile part_partkey_file(gendb_dir + "/part/p_partkey.bin");
    MemoryMappedFile part_brand_file(gendb_dir + "/part/p_brand.bin");
    MemoryMappedFile part_container_file(gendb_dir + "/part/p_container.bin");

    if (!lineitem_partkey_file.isValid() || !lineitem_quantity_file.isValid() ||
        !lineitem_extendedprice_file.isValid() || !part_partkey_file.isValid() ||
        !part_brand_file.isValid() || !part_container_file.isValid()) {
        std::cerr << "Failed to load required data files" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_files: %.2f ms\n", ms);
#endif

    // Find dictionary codes at runtime
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int8_t brand_code = findDictionaryCode(gendb_dir + "/part/p_brand_dict.txt", "Brand#23");
    int8_t container_code = findDictionaryCode(gendb_dir + "/part/p_container_dict.txt", "MED BOX");

    if (brand_code < 0 || container_code < 0) {
        std::cerr << "Failed to find dictionary codes. brand=" << (int)brand_code
                  << " container=" << (int)container_code << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] dictionary_lookup: %.2f ms\n", ms);
#endif

    // Data pointer casting
    const int32_t* l_partkey = static_cast<const int32_t*>(lineitem_partkey_file.data);
    const int64_t* l_quantity = static_cast<const int64_t*>(lineitem_quantity_file.data);
    const int64_t* l_extendedprice = static_cast<const int64_t*>(lineitem_extendedprice_file.data);
    const int32_t* p_partkey = static_cast<const int32_t*>(part_partkey_file.data);
    const int8_t* p_brand = static_cast<const int8_t*>(part_brand_file.data);
    const int8_t* p_container = static_cast<const int8_t*>(part_container_file.data);

    const size_t lineitem_rows = lineitem_partkey_file.size / sizeof(int32_t);
    const size_t part_rows = part_partkey_file.size / sizeof(int32_t);

    // PHASE 1: Compute average quantity per part key across all lineitem rows
    // Use compact hash table for faster aggregation than std::unordered_map
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Estimate unique partkeys (typically 2M from part table)
    const size_t est_unique_partkeys = std::max(500000UL, lineitem_rows / 60);
    CompactAggregationTable<int32_t, std::pair<int64_t, int64_t>>
        partkey_stats(est_unique_partkeys);

    #pragma omp parallel
    {
        std::unordered_map<int32_t, std::pair<int64_t, int64_t>> local_stats;

        #pragma omp for nowait
        for (size_t i = 0; i < lineitem_rows; ++i) {
            int32_t pk = l_partkey[i];
            int64_t qty = l_quantity[i];

            auto& p = local_stats[pk];
            p.first += qty;
            p.second += 1;
        }

        // Merge thread-local aggregations into compact table (critical section reduced to merging only)
        #pragma omp critical
        {
            for (auto& [pk, stats] : local_stats) {
                auto& global = partkey_stats.find_or_create(pk);
                global.first += stats.first;
                global.second += stats.second;
            }
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] compute_avg_quantity: %.2f ms\n", ms);
#endif

    // PHASE 2: Build hash map of partkey -> lineitem row indices
    // This enables efficient filtering in phase 3
    // Parallelize using thread-local hash tables then merge
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Use larger initial capacity to handle all partkeys
    CompactHashTableWithVectors<int32_t> partkey_to_lineitem_rows(partkey_stats.size() + 10000);

    // Phase 2a: Build thread-local hash tables in parallel
    const int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<int32_t, std::vector<size_t>>> thread_local_maps(num_threads);

    #pragma omp parallel
    {
        int thread_id = omp_get_thread_num();
        auto& local_map = thread_local_maps[thread_id];

        #pragma omp for nowait
        for (size_t i = 0; i < lineitem_rows; ++i) {
            int32_t pk = l_partkey[i];
            local_map[pk].push_back(i);
        }
    }

    // Phase 2b: Merge all thread-local maps into the global hash table (single-threaded, small overhead)
    for (const auto& local_map : thread_local_maps) {
        for (const auto& [pk, row_list] : local_map) {
            auto* global_rows = partkey_to_lineitem_rows.find_or_create(pk);
            for (size_t row_idx : row_list) {
                global_rows->push_back(row_idx);
            }
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] build_lineitem_index: %.2f ms\n", ms);
#endif

    // PHASE 3: Filter part table for Brand#23 and MED BOX, then process matches
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<int32_t> filtered_part_keys;
    filtered_part_keys.reserve(part_rows / 100); // Estimate ~1% of parts match

    for (size_t i = 0; i < part_rows; ++i) {
        if (p_brand[i] == brand_code && p_container[i] == container_code) {
            filtered_part_keys.push_back(p_partkey[i]);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] filter_part: %.2f ms\n", ms);
#endif

    // PHASE 4: Process filtered parts and their lineitem rows
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int64_t total_sum = 0; // Accumulator for SUM(l_extendedprice)

    #pragma omp parallel for reduction(+:total_sum)
    for (size_t p_idx = 0; p_idx < filtered_part_keys.size(); ++p_idx) {
        int32_t part_key = filtered_part_keys[p_idx];

        // Get statistics for this part
        auto stats_ptr = partkey_stats.find(part_key);
        if (stats_ptr == nullptr) continue;

        int64_t sum_qty = stats_ptr->first;
        int64_t count_qty = stats_ptr->second;
        if (count_qty == 0) continue;

        // Threshold: l_quantity < 0.2 * AVG(l_quantity)
        // To avoid floating point, rearrange as:
        // l_quantity < (sum_qty / count_qty) * 0.2
        // l_quantity * count_qty * 5 < sum_qty  (multiply both sides by count_qty * 5)

        // Get lineitem rows for this part
        auto rows_ptr = partkey_to_lineitem_rows.find(part_key);
        if (rows_ptr == nullptr) continue;

        for (size_t row_idx : *rows_ptr) {
            if (l_quantity[row_idx] * count_qty * 5 < sum_qty) {
                total_sum += l_extendedprice[row_idx];
            }
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] process_matches: %.2f ms\n", ms);
#endif

    // PHASE 5: Compute final result
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // avg_yearly = SUM(l_extendedprice) / 7.0
    // total_sum is in scaled units (×100), so actual sum = total_sum / 100
    // result = (total_sum / 100) / 7.0 = total_sum / 700.0
    double avg_yearly = static_cast<double>(total_sum) / 700.0;

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] compute_final: %.2f ms\n", ms);
#endif

    // Write result to CSV
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::ofstream out(results_dir + "/Q17.csv");
    out << "avg_yearly\n";
    out << std::fixed;
    out.precision(2);
    out << avg_yearly << "\n";
    out.close();

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    std::cout << "Q17 result: " << avg_yearly << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q17(gendb_dir, results_dir);
    return 0;
}
#endif
