#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <cstring>
#include <cmath>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdint>
#include <chrono>
#include <omp.h>

/*
LOGICAL PLAN (Q19):
==================

Step 1: Single-table predicates
- Part: p_brand IN ('Brand#12', 'Brand#23', 'Brand#34')
         p_container IN (12 specific containers per brand)
         p_size BETWEEN 1 AND {5,10,15 respectively}
         Estimated cardinality: ~4.7K rows

- Lineitem: l_shipmode = 'AIR'
            l_shipinstruct = 'DELIVER IN PERSON'
            l_quantity in range per OR condition
            Estimated cardinality: ~2.1M rows (after shipmode/instruct)

Step 2: Join graph and ordering
- Smallest filtered result first: Part (~4.7K rows)
- Hash table on part.p_partkey
- Probe with lineitem (~60M rows)
- Join result: ~5K rows (after partkey match)
- Final result: ~1.1K rows (after quantity filters)

Step 3: Aggregation
- Single aggregate: SUM(l_extendedprice * (1 - l_discount))
- No GROUP BY, scalar result (1 row)

PHYSICAL PLAN (Q19):
====================

1. Part scan: Full scan (2M rows), filter in-loop
   - Load p_partkey, p_brand, p_container, p_size
   - Parse dictionaries to get target codes
   - Filter: (brand & container & size) per OR condition
   - Store part condition mask per partkey

2. Build phase: Single-threaded hash lookup structure
   - Use unordered_map<partkey, condition_mask>
   - Size: ~4.7K entries

3. Lineitem scan: Parallel scan with OpenMP (64 threads)
   - Load l_partkey, l_quantity, l_extendedprice, l_discount, l_shipmode, l_shipinstruct
   - Filter: shipmode='AIR', shipinstruct='DELIVER IN PERSON'
   - Hash probe: Find matching partkey
   - Check quantity range matches part condition
   - Compute revenue: extprice * (100 - discount)

4. Aggregation: Thread-local Kahan summation
   - Each thread accumulates with Kahan correction for numerical stability
   - Final merge: sum all thread values
   - Divide by 10000 to get final dollars

5. Output: Single row CSV with SUM (2 decimal places)
*/

// Utility: Load dictionary from file
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return dict;
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq_pos));
            std::string value = line.substr(eq_pos + 1);
            dict[code] = value;
        }
    }
    f.close();
    return dict;
}

// Utility: Find dictionary code for a given value
int32_t find_dict_code(const std::unordered_map<int32_t, std::string>& dict, const std::string& value) {
    for (const auto& [code, val] : dict) {
        if (val == value) return code;
    }
    return -1;
}

// Utility: mmap file and return pointer and size
std::pair<void*, size_t> mmap_file(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open file: " << path << std::endl;
        return {nullptr, 0};
    }
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        std::cerr << "Failed to stat file: " << path << std::endl;
        close(fd);
        return {nullptr, 0};
    }
    void* ptr = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        std::cerr << "Failed to mmap file: " << path << std::endl;
        return {nullptr, 0};
    }
    return {ptr, (size_t)sb.st_size};
}

void run_q19(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // ========== LOAD DATA ==========
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string lineitem_dir = gendb_dir + "/lineitem/";
    std::string part_dir = gendb_dir + "/part/";

    // Load lineitem
    auto [li_partkey_ptr, li_partkey_size] = mmap_file(lineitem_dir + "l_partkey.bin");
    auto [li_quantity_ptr, li_quantity_size] = mmap_file(lineitem_dir + "l_quantity.bin");
    auto [li_extprice_ptr, li_extprice_size] = mmap_file(lineitem_dir + "l_extendedprice.bin");
    auto [li_discount_ptr, li_discount_size] = mmap_file(lineitem_dir + "l_discount.bin");
    auto [li_shipmode_ptr, li_shipmode_size] = mmap_file(lineitem_dir + "l_shipmode.bin");
    auto [li_shipinst_ptr, li_shipinst_size] = mmap_file(lineitem_dir + "l_shipinstruct.bin");

    if (!li_partkey_ptr || !li_quantity_ptr || !li_extprice_ptr ||
        !li_discount_ptr || !li_shipmode_ptr || !li_shipinst_ptr) {
        std::cerr << "Failed to mmap lineitem files" << std::endl;
        return;
    }

    int32_t* li_partkey = (int32_t*)li_partkey_ptr;
    int64_t* li_quantity = (int64_t*)li_quantity_ptr;
    int64_t* li_extprice = (int64_t*)li_extprice_ptr;
    int64_t* li_discount = (int64_t*)li_discount_ptr;
    int32_t* li_shipmode = (int32_t*)li_shipmode_ptr;
    int32_t* li_shipinst = (int32_t*)li_shipinst_ptr;

    size_t li_rows = li_partkey_size / sizeof(int32_t);

    // Load part
    auto [p_partkey_ptr, p_partkey_size] = mmap_file(part_dir + "p_partkey.bin");
    auto [p_brand_ptr, p_brand_size] = mmap_file(part_dir + "p_brand.bin");
    auto [p_container_ptr, p_container_size] = mmap_file(part_dir + "p_container.bin");
    auto [p_size_ptr, p_size_size] = mmap_file(part_dir + "p_size.bin");

    if (!p_partkey_ptr || !p_brand_ptr || !p_container_ptr || !p_size_ptr) {
        std::cerr << "Failed to mmap part files" << std::endl;
        return;
    }

    int32_t* p_partkey = (int32_t*)p_partkey_ptr;
    int32_t* p_brand = (int32_t*)p_brand_ptr;
    int32_t* p_container = (int32_t*)p_container_ptr;
    int32_t* p_size = (int32_t*)p_size_ptr;

    size_t p_rows = p_partkey_size / sizeof(int32_t);

    // Load dictionaries
    auto li_shipmode_dict = load_dictionary(lineitem_dir + "l_shipmode_dict.txt");
    auto li_shipinst_dict = load_dictionary(lineitem_dir + "l_shipinstruct_dict.txt");
    auto p_brand_dict = load_dictionary(part_dir + "p_brand_dict.txt");
    auto p_container_dict = load_dictionary(part_dir + "p_container_dict.txt");

    // Find dictionary codes
    int32_t shipmode_air = find_dict_code(li_shipmode_dict, "AIR");
    int32_t shipinst_deliver = find_dict_code(li_shipinst_dict, "DELIVER IN PERSON");

    int32_t brand_12 = find_dict_code(p_brand_dict, "Brand#12");
    int32_t brand_23 = find_dict_code(p_brand_dict, "Brand#23");
    int32_t brand_34 = find_dict_code(p_brand_dict, "Brand#34");

    int32_t cont_sm_case = find_dict_code(p_container_dict, "SM CASE");
    int32_t cont_sm_box = find_dict_code(p_container_dict, "SM BOX");
    int32_t cont_sm_pack = find_dict_code(p_container_dict, "SM PACK");
    int32_t cont_sm_pkg = find_dict_code(p_container_dict, "SM PKG");

    int32_t cont_med_bag = find_dict_code(p_container_dict, "MED BAG");
    int32_t cont_med_box = find_dict_code(p_container_dict, "MED BOX");
    int32_t cont_med_pkg = find_dict_code(p_container_dict, "MED PKG");
    int32_t cont_med_pack = find_dict_code(p_container_dict, "MED PACK");

    int32_t cont_lg_case = find_dict_code(p_container_dict, "LG CASE");
    int32_t cont_lg_box = find_dict_code(p_container_dict, "LG BOX");
    int32_t cont_lg_pack = find_dict_code(p_container_dict, "LG PACK");
    int32_t cont_lg_pkg = find_dict_code(p_container_dict, "LG PKG");

    if (shipmode_air < 0 || shipinst_deliver < 0 ||
        brand_12 < 0 || brand_23 < 0 || brand_34 < 0 ||
        cont_sm_case < 0 || cont_sm_box < 0 || cont_sm_pack < 0 || cont_sm_pkg < 0 ||
        cont_med_bag < 0 || cont_med_box < 0 || cont_med_pkg < 0 || cont_med_pack < 0 ||
        cont_lg_case < 0 || cont_lg_box < 0 || cont_lg_pack < 0 || cont_lg_pkg < 0) {
        std::cerr << "Failed to find required dictionary codes" << std::endl;
        return;
    }

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
    #endif

    // ========== FILTER PART AND BUILD HASH TABLE ==========
    #ifdef GENDB_PROFILE
    auto t_part_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<int32_t, uint8_t> part_conditions; // partkey -> condition bitmask

    for (size_t i = 0; i < p_rows; i++) {
        int32_t pk = p_partkey[i];
        int32_t br = p_brand[i];
        int32_t cn = p_container[i];
        int32_t sz = p_size[i];

        uint8_t cond_mask = 0;

        // Condition 1: Brand#12, SM containers, size 1-5
        if (br == brand_12 && sz >= 1 && sz <= 5) {
            if (cn == cont_sm_case || cn == cont_sm_box || cn == cont_sm_pack || cn == cont_sm_pkg) {
                cond_mask |= 1;
            }
        }

        // Condition 2: Brand#23, MED containers, size 1-10
        if (br == brand_23 && sz >= 1 && sz <= 10) {
            if (cn == cont_med_bag || cn == cont_med_box || cn == cont_med_pkg || cn == cont_med_pack) {
                cond_mask |= 2;
            }
        }

        // Condition 3: Brand#34, LG containers, size 1-15
        if (br == brand_34 && sz >= 1 && sz <= 15) {
            if (cn == cont_lg_case || cn == cont_lg_box || cn == cont_lg_pack || cn == cont_lg_pkg) {
                cond_mask |= 4;
            }
        }

        if (cond_mask != 0) {
            part_conditions[pk] = cond_mask;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_part_filter_end = std::chrono::high_resolution_clock::now();
    double part_filter_ms = std::chrono::duration<double, std::milli>(t_part_filter_end - t_part_filter_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", part_filter_ms);
    #endif

    // ========== SCAN LINEITEM AND AGGREGATE ==========
    #ifdef GENDB_PROFILE
    auto t_li_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    int num_threads = omp_get_max_threads();
    std::vector<int64_t> thread_sum(num_threads, 0);
    std::vector<int64_t> thread_c(num_threads, 0);  // Kahan correction

    #pragma omp parallel for schedule(dynamic, 100000)
    for (size_t i = 0; i < li_rows; i++) {
        int32_t partkey = li_partkey[i];
        int32_t shipmode = li_shipmode[i];
        int32_t shipinst = li_shipinst[i];
        int64_t qty = li_quantity[i];
        int64_t extprice = li_extprice[i];
        int64_t discount = li_discount[i];
        int tid = omp_get_thread_num();

        // Check shipmode: must be AIR
        if (shipmode != shipmode_air) continue;

        // Check shipinstruct: must be DELIVER IN PERSON
        if (shipinst != shipinst_deliver) continue;

        // Check if partkey matches any condition
        auto it = part_conditions.find(partkey);
        if (it == part_conditions.end()) continue;

        uint8_t cond_mask = it->second;

        // Check quantity constraints for the matching condition
        bool passes_qty = false;

        // Condition 1 (bit 0): quantity 1-11
        if ((cond_mask & 1) && qty >= 100 && qty <= 1100) {
            passes_qty = true;
        }
        // Condition 2 (bit 1): quantity 10-20
        if ((cond_mask & 2) && qty >= 1000 && qty <= 2000) {
            passes_qty = true;
        }
        // Condition 3 (bit 2): quantity 20-30
        if ((cond_mask & 4) && qty >= 2000 && qty <= 3000) {
            passes_qty = true;
        }

        if (!passes_qty) continue;

        // Compute revenue: l_extendedprice * (1 - l_discount)
        // extprice and discount are both scaled by 100
        // revenue = extprice * (100 - discount)
        int64_t revenue = extprice * (100 - discount);

        // Kahan summation for numerical stability
        int64_t y = revenue - thread_c[tid];
        int64_t t = thread_sum[tid] + y;
        thread_c[tid] = (t - thread_sum[tid]) - y;
        thread_sum[tid] = t;
    }

    #ifdef GENDB_PROFILE
    auto t_li_scan_end = std::chrono::high_resolution_clock::now();
    double li_scan_ms = std::chrono::duration<double, std::milli>(t_li_scan_end - t_li_scan_start).count();
    printf("[TIMING] join_aggregate: %.2f ms\n", li_scan_ms);
    #endif

    // ========== MERGE THREAD-LOCAL SUMS ==========
    #ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t total_sum = 0;
    int64_t total_c = 0;
    for (int t = 0; t < num_threads; t++) {
        int64_t y = thread_sum[t] - total_c;
        int64_t tmp = total_sum + y;
        total_c = (tmp - total_sum) - y;
        total_sum = tmp;
    }

    #ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] sort: %.2f ms\n", merge_ms);
    #endif

    // ========== WRITE OUTPUT ==========
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    // Convert to decimal:
    // total_sum = SUM(extprice * (100 - discount))
    // extprice scaled by 100, (100-discount) unitless from 0-100
    // result scaled by 10000, so divide by 10000 for final dollars
    double revenue_value = (double)total_sum / 10000.0;

    std::string output_path = results_dir + "/Q19.csv";
    std::ofstream out(output_path);
    out << "revenue" << std::endl;
    out.precision(2);
    out << std::fixed << revenue_value << std::endl;
    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    #endif

    // ========== TOTAL TIME ==========
    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
    #endif

    // ========== CLEANUP ==========
    munmap((void*)li_partkey_ptr, li_partkey_size);
    munmap((void*)li_quantity_ptr, li_quantity_size);
    munmap((void*)li_extprice_ptr, li_extprice_size);
    munmap((void*)li_discount_ptr, li_discount_size);
    munmap((void*)li_shipmode_ptr, li_shipmode_size);
    munmap((void*)li_shipinst_ptr, li_shipinst_size);

    munmap((void*)p_partkey_ptr, p_partkey_size);
    munmap((void*)p_brand_ptr, p_brand_size);
    munmap((void*)p_container_ptr, p_container_size);
    munmap((void*)p_size_ptr, p_size_size);
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
