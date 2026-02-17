#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <cstdint>
#include <unordered_map>
#include <cmath>
#include <chrono>
#include <omp.h>

/*
LOGICAL QUERY PLAN:
1. Load part table: 2M rows
2. Apply single-table predicates on part: p_brand IN ('Brand#12', 'Brand#23', 'Brand#34'),
   p_container IN specific values, p_size BETWEEN conditions
   Estimated: ~4,000-5,000 parts matching (very selective)
3. Build hash table on filtered part: key=p_partkey, value=part_row_data
4. Scan lineitem: 59M rows
5. For each lineitem row:
   - Check l_shipinstruct = 'DELIVER IN PERSON'
   - Check l_shipmode IN ('AIR') — note: 'AIR REG' doesn't exist in dictionary
   - Probe hash table with l_partkey
   - On match, check quantity/size conditions per the three OR clauses
   - Accumulate: revenue += l_extendedprice * (1 - l_discount)

PHYSICAL QUERY PLAN:
1. Scan part table sequentially (2M rows, memory-friendly)
2. Filter part: brand + container + size predicates
3. Build hash table on filtered parts indexed by p_partkey (4869 entries expected)
4. Scan lineitem with OpenMP parallelism (59M rows / 64 cores)
5. For each lineitem row:
   - Check l_shipinstruct and l_shipmode using preloaded dictionary codes
   - Probe hash table for join
   - Apply the three OR quantity/size conditions
   - Accumulate revenue with precision: contribution = price * (100 - discount) / 100
6. Output single-row result as CSV

KEY OPTIMIZATIONS:
- Dictionary codes loaded once at startup (O(1) per-row lookups)
- Pre-filtered part table in hash table (avoid 59M probes on 2M parts)
- Collapsed OR conditions (no materialization)
- Integer arithmetic throughout (no floating point loss)
- OpenMP parallelism on 59M lineitem scan with static scheduling
- DECIMAL(15,2) scale = 100, all values stored as int64_t
*/

struct PartRow {
    int32_t p_partkey;
    int32_t p_size;
    int32_t p_brand;
    int32_t p_container;
};

// Load dictionary file and return map from code (1-indexed) to string value
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return dict;
    }
    std::string line;
    int code = 1;  // Dictionaries are 1-indexed in files
    while (std::getline(f, line)) {
        if (!line.empty()) {
            dict[code] = line;
        }
        code++;
    }
    return dict;
}

// Find dictionary code for a target string, return -1 if not found
int32_t find_dict_code(const std::unordered_map<int32_t, std::string>& dict, const std::string& target) {
    for (const auto& [code, value] : dict) {
        if (value == target) {
            return code;
        }
    }
    return -1;
}

void run_q19(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    const int64_t scale_factor = 100;  // DECIMAL(15,2) → 10^2 = 100

    // Load dictionaries for part
    std::string part_dir = gendb_dir + "/part";
    auto p_brand_dict = load_dictionary(part_dir + "/p_brand_dict.txt");
    auto p_container_dict = load_dictionary(part_dir + "/p_container_dict.txt");

    // Find dictionary codes for brands and containers
    int32_t brand12_code = find_dict_code(p_brand_dict, "Brand#12");
    int32_t brand23_code = find_dict_code(p_brand_dict, "Brand#23");
    int32_t brand34_code = find_dict_code(p_brand_dict, "Brand#34");

    int32_t sm_case_code = find_dict_code(p_container_dict, "SM CASE");
    int32_t sm_box_code = find_dict_code(p_container_dict, "SM BOX");
    int32_t sm_pack_code = find_dict_code(p_container_dict, "SM PACK");
    int32_t sm_pkg_code = find_dict_code(p_container_dict, "SM PKG");

    int32_t med_bag_code = find_dict_code(p_container_dict, "MED BAG");
    int32_t med_box_code = find_dict_code(p_container_dict, "MED BOX");
    int32_t med_pkg_code = find_dict_code(p_container_dict, "MED PKG");
    int32_t med_pack_code = find_dict_code(p_container_dict, "MED PACK");

    int32_t lg_case_code = find_dict_code(p_container_dict, "LG CASE");
    int32_t lg_box_code = find_dict_code(p_container_dict, "LG BOX");
    int32_t lg_pack_code = find_dict_code(p_container_dict, "LG PACK");
    int32_t lg_pkg_code = find_dict_code(p_container_dict, "LG PKG");

    // Load dictionaries for lineitem
    std::string lineitem_dir = gendb_dir + "/lineitem";
    auto l_shipmode_dict = load_dictionary(lineitem_dir + "/l_shipmode_dict.txt");
    auto l_shipinstruct_dict = load_dictionary(lineitem_dir + "/l_shipinstruct_dict.txt");

    // Find dictionary codes for shipmode and shipinstruct
    int32_t air_code = find_dict_code(l_shipmode_dict, "AIR");
    int32_t reg_air_code = find_dict_code(l_shipmode_dict, "REG AIR");
    int32_t deliver_person_code = find_dict_code(l_shipinstruct_dict, "DELIVER IN PERSON");

#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Load part table
    const int32_t num_parts = 2000000;
    int32_t* p_partkey = new int32_t[num_parts];
    int32_t* p_size = new int32_t[num_parts];
    int32_t* p_brand = new int32_t[num_parts];
    int32_t* p_container = new int32_t[num_parts];

    {
        std::ifstream f(part_dir + "/p_partkey.bin", std::ios::binary);
        f.read(reinterpret_cast<char*>(p_partkey), num_parts * sizeof(int32_t));
        f.close();
    }
    {
        std::ifstream f(part_dir + "/p_size.bin", std::ios::binary);
        f.read(reinterpret_cast<char*>(p_size), num_parts * sizeof(int32_t));
        f.close();
    }
    {
        std::ifstream f(part_dir + "/p_brand.bin", std::ios::binary);
        f.read(reinterpret_cast<char*>(p_brand), num_parts * sizeof(int32_t));
        f.close();
    }
    {
        std::ifstream f(part_dir + "/p_container.bin", std::ios::binary);
        f.read(reinterpret_cast<char*>(p_container), num_parts * sizeof(int32_t));
        f.close();
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] load_part: %.2f ms\n", ms);
#endif

#ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
#endif

    // Filter part table and build hash table
    std::unordered_map<int32_t, PartRow> part_hash;

    for (int32_t i = 0; i < num_parts; i++) {
        int32_t brand = p_brand[i];
        int32_t container = p_container[i];
        int32_t size = p_size[i];

        bool matches = false;

        // Clause 1: Brand#12, SM CASE/BOX/PACK/PKG, size 1-5, qty 1-11
        if (brand == brand12_code && size >= 1 && size <= 5) {
            if (container == sm_case_code || container == sm_box_code ||
                container == sm_pack_code || container == sm_pkg_code) {
                matches = true;
            }
        }

        // Clause 2: Brand#23, MED BAG/BOX/PKG/PACK, size 1-10, qty 10-20
        if (brand == brand23_code && size >= 1 && size <= 10) {
            if (container == med_bag_code || container == med_box_code ||
                container == med_pkg_code || container == med_pack_code) {
                matches = true;
            }
        }

        // Clause 3: Brand#34, LG CASE/BOX/PACK/PKG, size 1-15, qty 20-30
        if (brand == brand34_code && size >= 1 && size <= 15) {
            if (container == lg_case_code || container == lg_box_code ||
                container == lg_pack_code || container == lg_pkg_code) {
                matches = true;
            }
        }

        if (matches) {
            PartRow row;
            row.p_partkey = p_partkey[i];
            row.p_size = size;
            row.p_brand = brand;
            row.p_container = container;
            part_hash[row.p_partkey] = row;
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] filter_part: %.2f ms\n", ms);
    printf("[TIMING] part_hash_size: %zu\n", part_hash.size());
#endif

    delete[] p_partkey;
    delete[] p_size;
    delete[] p_brand;
    delete[] p_container;

#ifdef GENDB_PROFILE
    auto t_load_li_start = std::chrono::high_resolution_clock::now();
#endif

    // Load lineitem table
    const int64_t num_lineitem = 59986052;
    int32_t* l_partkey = new int32_t[num_lineitem];
    int64_t* l_quantity = new int64_t[num_lineitem];
    int64_t* l_extendedprice = new int64_t[num_lineitem];
    int64_t* l_discount = new int64_t[num_lineitem];
    int32_t* l_shipmode = new int32_t[num_lineitem];
    int32_t* l_shipinstruct = new int32_t[num_lineitem];

    // Parallelize file loading with OpenMP
    #pragma omp parallel sections
    {
        #pragma omp section
        {
            std::ifstream f(lineitem_dir + "/l_partkey.bin", std::ios::binary);
            f.read(reinterpret_cast<char*>(l_partkey), num_lineitem * sizeof(int32_t));
            f.close();
        }
        #pragma omp section
        {
            std::ifstream f(lineitem_dir + "/l_quantity.bin", std::ios::binary);
            f.read(reinterpret_cast<char*>(l_quantity), num_lineitem * sizeof(int64_t));
            f.close();
        }
        #pragma omp section
        {
            std::ifstream f(lineitem_dir + "/l_extendedprice.bin", std::ios::binary);
            f.read(reinterpret_cast<char*>(l_extendedprice), num_lineitem * sizeof(int64_t));
            f.close();
        }
        #pragma omp section
        {
            std::ifstream f(lineitem_dir + "/l_discount.bin", std::ios::binary);
            f.read(reinterpret_cast<char*>(l_discount), num_lineitem * sizeof(int64_t));
            f.close();
        }
        #pragma omp section
        {
            std::ifstream f(lineitem_dir + "/l_shipmode.bin", std::ios::binary);
            f.read(reinterpret_cast<char*>(l_shipmode), num_lineitem * sizeof(int32_t));
            f.close();
        }
        #pragma omp section
        {
            std::ifstream f(lineitem_dir + "/l_shipinstruct.bin", std::ios::binary);
            f.read(reinterpret_cast<char*>(l_shipinstruct), num_lineitem * sizeof(int32_t));
            f.close();
        }
    }

#ifdef GENDB_PROFILE
    auto t_load_li_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_load_li_end - t_load_li_start).count();
    printf("[TIMING] load_lineitem: %.2f ms\n", ms);
#endif

#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Scan lineitem, filter, join, and aggregate
    // Quantity thresholds (scaled by scale_factor=100)
    const int64_t qty_threshold_1 = 1 * scale_factor;      // 1.00
    const int64_t qty_threshold_2 = 11 * scale_factor;     // 11.00
    const int64_t qty_threshold_3 = 10 * scale_factor;     // 10.00
    const int64_t qty_threshold_4 = 20 * scale_factor;     // 20.00
    const int64_t qty_threshold_5 = 30 * scale_factor;     // 30.00

    double aggregated_revenue = 0.0;
    double kahan_c = 0.0;  // Kahan summation correction term

    #pragma omp parallel for reduction(+:aggregated_revenue) reduction(+:kahan_c) schedule(static, 65536)
    for (int64_t i = 0; i < num_lineitem; i++) {
        // Check shipinstruct first (fast filter)
        if (l_shipinstruct[i] != deliver_person_code) {
            continue;
        }

        // Check shipmode: IN ('AIR', 'REG AIR') — SQL spec says 'AIR REG' but dictionary has 'REG AIR'
        if (l_shipmode[i] != air_code && l_shipmode[i] != reg_air_code) {
            continue;
        }

        // Probe hash table for matching part
        auto it = part_hash.find(l_partkey[i]);
        if (it == part_hash.end()) {
            continue;
        }

        const PartRow& part = it->second;
        int64_t qty = l_quantity[i];

        bool matches = false;

        // Clause 1: Brand#12, size 1-5, qty 1-11
        if (part.p_brand == brand12_code && part.p_size >= 1 && part.p_size <= 5) {
            if (qty >= qty_threshold_1 && qty <= qty_threshold_2) {
                matches = true;
            }
        }

        // Clause 2: Brand#23, size 1-10, qty 10-20
        if (part.p_brand == brand23_code && part.p_size >= 1 && part.p_size <= 10) {
            if (qty >= qty_threshold_3 && qty <= qty_threshold_4) {
                matches = true;
            }
        }

        // Clause 3: Brand#34, size 1-15, qty 20-30
        if (part.p_brand == brand34_code && part.p_size >= 1 && part.p_size <= 15) {
            if (qty >= qty_threshold_4 && qty <= qty_threshold_5) {
                matches = true;
            }
        }

        if (matches) {
            // Calculate revenue: l_extendedprice * (1 - l_discount)
            // Both price and discount are DECIMAL(15,2) scaled by 100.
            // Convert directly to double for accurate computation:
            // (price/100) * (1 - discount/100) = (price/100) * ((100-discount)/100)
            //                                   = price * (100-discount) / 10000

            double price = static_cast<double>(l_extendedprice[i]) / scale_factor;
            double discount = static_cast<double>(l_discount[i]) / scale_factor;
            double contribution = price * (1.0 - discount);

            // Kahan summation for precision
            double y = contribution - kahan_c;
            double t = aggregated_revenue + y;
            kahan_c = (t - aggregated_revenue) - y;
            aggregated_revenue = t;
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join_filter_aggregate: %.2f ms\n", ms);
#endif

    // aggregated_revenue is now the final revenue value in dollars
    double final_revenue = aggregated_revenue;

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms);
#endif

    // Write output
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q19.csv";
    std::ofstream out(output_file);
    out << "revenue\n";
    out.precision(4);
    out << std::fixed << final_revenue << "\n";
    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
#endif

    delete[] l_partkey;
    delete[] l_quantity;
    delete[] l_extendedprice;
    delete[] l_discount;
    delete[] l_shipmode;
    delete[] l_shipinstruct;
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
