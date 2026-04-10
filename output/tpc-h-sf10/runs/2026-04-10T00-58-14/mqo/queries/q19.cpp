// Q19 tail — Discounted Revenue
// Consumes: scan_lineitem_full (shared)
// Operators: part scan+filter → hash build → lineitem residual filter → hash probe + compound OR → scalar agg → output

#include "mqo_profile.hpp"
#include "../shared/mqo_io.hpp"
#include "../shared/scan_lineitem_full.hpp"

#include <cstdio>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>

namespace mqo::tails {

void run_Q19(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q19_tail");

    // ---------------------------------------------------------------
    // Step 1: Load part table, resolve dict codes, filter, build hash
    // ---------------------------------------------------------------
    struct PartEntry {
        uint8_t brand_code;
        uint8_t container_code;
        int32_t p_size;
    };

    std::unordered_map<int32_t, PartEntry> part_ht;

    {
        MQO_TIME_PHASE("Q19_part_scan_and_hash_build");

        const std::string pdir = ctx.gendb_dir + "/part/";
        const size_t n_part = mqo::io::read_row_count(pdir + "meta.txt");

        // Mmap part columns
        const int32_t* p_partkey   = mqo::io::mmap_column<int32_t>(pdir + "p_partkey.bin", n_part);
        const uint8_t* p_brand     = mqo::io::mmap_column<uint8_t>(pdir + "p_brand.bin", n_part);
        const uint8_t* p_container = mqo::io::mmap_column<uint8_t>(pdir + "p_container.bin", n_part);
        const int32_t* p_size      = mqo::io::mmap_column<int32_t>(pdir + "p_size.bin", n_part);

        // Read dictionaries to resolve brand/container codes
        auto brand_dict     = mqo::io::read_dictionary(pdir + "p_brand_dict.bin");
        auto container_dict = mqo::io::read_dictionary(pdir + "p_container_dict.bin");

        // Find dict codes for the 3 brands
        uint8_t brand12_code = 255, brand23_code = 255, brand34_code = 255;
        for (uint32_t i = 0; i < brand_dict.size(); i++) {
            if (brand_dict[i] == "Brand#12") brand12_code = (uint8_t)i;
            else if (brand_dict[i] == "Brand#23") brand23_code = (uint8_t)i;
            else if (brand_dict[i] == "Brand#34") brand34_code = (uint8_t)i;
        }

        // Find dict codes for 12 containers
        // Build a lookup: container_code -> which group (0=SM, 1=MED, 2=LG, 255=none)
        std::vector<uint8_t> container_group(container_dict.size(), 255);
        for (uint32_t i = 0; i < container_dict.size(); i++) {
            const auto& s = container_dict[i];
            if (s == "SM CASE" || s == "SM BOX" || s == "SM PACK" || s == "SM PKG")
                container_group[i] = 0;
            else if (s == "MED BAG" || s == "MED BOX" || s == "MED PKG" || s == "MED PACK")
                container_group[i] = 1;
            else if (s == "LG CASE" || s == "LG BOX" || s == "LG PACK" || s == "LG PKG")
                container_group[i] = 2;
        }

        // Build a brand->group mapping for quick OR-branch matching
        // brand12->0, brand23->1, brand34->2
        // We store brand_code and container_code directly; post-join filter will interpret

        // Scan part with union filter: brand in {12,23,34}, container in any of 12, size 1..15
        part_ht.reserve(80000);
        for (size_t i = 0; i < n_part; i++) {
            uint8_t bc = p_brand[i];
            if (bc != brand12_code && bc != brand23_code && bc != brand34_code) continue;

            uint8_t cc = p_container[i];
            if (cc >= container_group.size() || container_group[cc] == 255) continue;

            int32_t sz = p_size[i];
            if (sz < 1 || sz > 15) continue;

            // Further tighten: check brand-container-size consistency for at least one OR branch
            uint8_t cg = container_group[cc];
            bool valid = false;
            if (bc == brand12_code && cg == 0 && sz <= 5) valid = true;
            else if (bc == brand23_code && cg == 1 && sz <= 10) valid = true;
            else if (bc == brand34_code && cg == 2 /* sz <= 15 already checked */) valid = true;

            if (!valid) continue;

            // Encode: brand_group (0,1,2) and container_group (0,1,2) into the entry
            uint8_t brand_group = (bc == brand12_code) ? 0 : (bc == brand23_code ? 1 : 2);
            part_ht[p_partkey[i]] = {brand_group, cg, sz};
        }
    }

    // ---------------------------------------------------------------
    // Step 2: Scan lineitem (shared), residual filter, probe, aggregate
    // ---------------------------------------------------------------
    const auto& cols = mqo::shared::scan_lineitem_full::get_columns(ctx);
    const size_t n = cols.n_rows;

    double revenue = 0.0;

    {
        MQO_TIME_PHASE("Q19_main_scan");

        const uint8_t*  l_shipinstruct  = cols.l_shipinstruct;
        const uint8_t*  l_shipmode      = cols.l_shipmode;
        const int32_t*  l_partkey       = cols.l_partkey;
        const double*   l_quantity      = cols.l_quantity;
        const double*   l_extendedprice = cols.l_extendedprice;
        const double*   l_discount      = cols.l_discount;

        for (size_t i = 0; i < n; i++) {
            // Residual filter: l_shipinstruct = 'DELIVER IN PERSON' (code 0)
            //                  l_shipmode IN ('AIR', 'REG AIR') (codes 2, 3)
            if (l_shipinstruct[i] != 0) continue;
            uint8_t sm = l_shipmode[i];
            if (sm != 2 && sm != 3) continue;

            // Hash probe
            auto it = part_ht.find(l_partkey[i]);
            if (it == part_ht.end()) continue;

            const auto& pe = it->second;
            double qty = l_quantity[i];

            // Compound OR filter on brand_group, container_group, quantity, size
            bool pass = false;
            switch (pe.brand_code) {  // brand_group: 0=Brand#12, 1=Brand#23, 2=Brand#34
                case 0:  // Brand#12, SM containers, qty [1,11], size [1,5]
                    pass = (pe.container_code == 0 && qty >= 1.0 && qty <= 11.0 && pe.p_size <= 5);
                    break;
                case 1:  // Brand#23, MED containers, qty [10,20], size [1,10]
                    pass = (pe.container_code == 1 && qty >= 10.0 && qty <= 20.0 && pe.p_size <= 10);
                    break;
                case 2:  // Brand#34, LG containers, qty [20,30], size [1,15]
                    pass = (pe.container_code == 2 && qty >= 20.0 && qty <= 30.0);
                    break;
            }

            if (pass) {
                revenue += l_extendedprice[i] * (1.0 - l_discount[i]);
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 3: Output
    // ---------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q19_output");
        std::string out_path = ctx.output_dir + "/q19.csv";
        FILE* f = std::fopen(out_path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "[Q19] Cannot open output: %s\n", out_path.c_str());
            return;
        }
        std::fprintf(f, "revenue\n");
        std::fprintf(f, "%.2f\n", revenue);
        std::fclose(f);
    }
}

}  // namespace mqo::tails
