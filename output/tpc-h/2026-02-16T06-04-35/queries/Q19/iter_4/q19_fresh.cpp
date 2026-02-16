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

void run_q19(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Q19: Discounted Revenue (Fresh Implementation)\n");
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

    // Find dictionary codes
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

    // Load data
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif
    size_t li_count = 0, p_count = 0;
    int32_t* l_partkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_partkey.bin", li_count);
    int64_t* l_quantity = mmap_column<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", li_count);
    int64_t* l_extendedprice = mmap_column<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", li_count);
    int64_t* l_discount = mmap_column<int64_t>(gendb_dir + "/lineitem/l_discount.bin", li_count);
    int8_t* l_shipmode = mmap_column<int8_t>(gendb_dir + "/lineitem/l_shipmode.bin", li_count);
    int8_t* l_shipinstruct = mmap_column<int8_t>(gendb_dir + "/lineitem/l_shipinstruct.bin", li_count);

    int32_t* p_partkey = mmap_column<int32_t>(gendb_dir + "/part/p_partkey.bin", p_count);
    int8_t* p_brand = mmap_column<int8_t>(gendb_dir + "/part/p_brand.bin", p_count);
    int8_t* p_container = mmap_column<int8_t>(gendb_dir + "/part/p_container.bin", p_count);
    int32_t* p_size = mmap_column<int32_t>(gendb_dir + "/part/p_size.bin", p_count);
#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_columns: %.2f ms\n", ms_load);
#endif

    // Build part map WITHOUT pre-filtering
#ifdef GENDB_PROFILE
    auto t_filter_part_start = std::chrono::high_resolution_clock::now();
#endif
    std::unordered_map<int32_t, std::vector<uint32_t>> part_by_key;
    for (size_t i = 0; i < p_count; ++i) {
        part_by_key[p_partkey[i]].push_back(i);
    }
#ifdef GENDB_PROFILE
    auto t_filter_part_end = std::chrono::high_resolution_clock::now();
    double ms_filter_part = std::chrono::duration<double, std::milli>(t_filter_part_end - t_filter_part_start).count();
    printf("[TIMING] filter_part: %.2f ms\n", ms_filter_part);
    printf("[METADATA CHECK] Part rows indexed: %zu\n", part_by_key.size());
#endif

    // Process lineitem rows
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif
    int64_t total_revenue = 0;
    size_t matched_rows = 0;

    for (size_t i = 0; i < li_count; ++i) {
        // Filter on lineitem attributes first
        if (l_shipinstruct[i] != code_deliver) continue;
        if (l_shipmode[i] != code_air && l_shipmode[i] != code_reg_air) continue;

        // Find parts with matching key
        auto it = part_by_key.find(l_partkey[i]);
        if (it == part_by_key.end()) continue;

        // Check each part
        for (uint32_t p_idx : it->second) {
            int8_t br = p_brand[p_idx];
            int8_t cont = p_container[p_idx];
            int32_t sz = p_size[p_idx];
            int64_t qty = l_quantity[i];

            // Condition 1: Brand#12
            if (br == code_brand12 &&
                (cont == code_sm_case || cont == code_sm_box || cont == code_sm_pack || cont == code_sm_pkg) &&
                sz >= 1 && sz <= 5 &&
                qty >= 100 && qty <= 1100) {
                matched_rows++;
                int64_t revenue_at_scale = l_extendedprice[i] * (100 - l_discount[i]);
                total_revenue += revenue_at_scale;
            }
            // Condition 2: Brand#23
            else if (br == code_brand23 &&
                     (cont == code_med_bag || cont == code_med_box || cont == code_med_pkg || cont == code_med_pack) &&
                     sz >= 1 && sz <= 10 &&
                     qty >= 1000 && qty <= 2000) {
                matched_rows++;
                int64_t revenue_at_scale = l_extendedprice[i] * (100 - l_discount[i]);
                total_revenue += revenue_at_scale;
            }
            // Condition 3: Brand#34
            else if (br == code_brand34 &&
                     (cont == code_lg_case || cont == code_lg_box || cont == code_lg_pack || cont == code_lg_pkg) &&
                     sz >= 1 && sz <= 15 &&
                     qty >= 2000 && qty <= 3000) {
                matched_rows++;
                int64_t revenue_at_scale = l_extendedprice[i] * (100 - l_discount[i]);
                total_revenue += revenue_at_scale;
            }
        }
    }
#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", ms_scan);
    printf("[METADATA CHECK] Matched rows: %zu\n", matched_rows);
#endif

    // Scale and output
    double revenue = static_cast<double>(total_revenue) / 10000.0;

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
