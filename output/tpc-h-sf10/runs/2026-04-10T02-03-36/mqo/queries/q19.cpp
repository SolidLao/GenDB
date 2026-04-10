// Q19 tail: Discounted Revenue
// Shared input: hash_part_by_partkey (part dense array with dict-encoded brand/container)
// Operators: part_filter → index_lookup → gather+filter → scalar aggregation

#include "mqo_profile.hpp"
#include "../shared/mqo_io.hpp"
#include "../shared/hash_part_by_partkey.hpp"

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace mqo { namespace tails {

void run_Q19(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q19_tail");

    const auto& part = mqo::shared::hash_part_by_partkey::get();
    const std::string gendb = ctx.gendb_dir;
    const std::string li_dir = gendb + "/lineitem";
    const std::string idx_dir = gendb + "/indexes";

    // ---------------------------------------------------------------
    // Step 0: Load dictionaries for part (brand, container) and lineitem (shipmode, shipinstruct)
    // ---------------------------------------------------------------
    mqo::io::Dictionary brand_dict, container_dict, shipmode_dict, shipinstruct_dict;
    {
        MQO_TIME_PHASE("Q19_load_dicts");
        brand_dict.load(gendb + "/part/p_brand_dict.bin");
        container_dict.load(gendb + "/part/p_container_dict.bin");
        shipmode_dict.load(li_dir + "/l_shipmode_dict.bin");
        shipinstruct_dict.load(li_dir + "/l_shipinstruct_dict.bin");
    }

    // Resolve dict codes for part predicates
    uint8_t brand12_code = 255, brand23_code = 255, brand34_code = 255;
    for (uint32_t i = 0; i < brand_dict.count; ++i) {
        if (brand_dict.entries[i] == "Brand#12") brand12_code = (uint8_t)i;
        else if (brand_dict.entries[i] == "Brand#23") brand23_code = (uint8_t)i;
        else if (brand_dict.entries[i] == "Brand#34") brand34_code = (uint8_t)i;
    }

    // Container sets per branch
    // Branch 1: SM CASE, SM BOX, SM PACK, SM PKG
    // Branch 2: MED BAG, MED BOX, MED PKG, MED PACK
    // Branch 3: LG CASE, LG BOX, LG PACK, LG PKG
    bool container_branch1[256] = {};
    bool container_branch2[256] = {};
    bool container_branch3[256] = {};
    for (uint32_t i = 0; i < container_dict.count; ++i) {
        const auto& s = container_dict.entries[i];
        if (s == "SM CASE" || s == "SM BOX" || s == "SM PACK" || s == "SM PKG")
            container_branch1[i] = true;
        if (s == "MED BAG" || s == "MED BOX" || s == "MED PKG" || s == "MED PACK")
            container_branch2[i] = true;
        if (s == "LG CASE" || s == "LG BOX" || s == "LG PACK" || s == "LG PKG")
            container_branch3[i] = true;
    }

    // Resolve lineitem dict codes
    uint8_t shipinstruct_dip_code = 255;
    for (uint32_t i = 0; i < shipinstruct_dict.count; ++i) {
        if (shipinstruct_dict.entries[i] == "DELIVER IN PERSON")
            shipinstruct_dip_code = (uint8_t)i;
    }
    bool shipmode_air[256] = {};
    for (uint32_t i = 0; i < shipmode_dict.count; ++i) {
        if (shipmode_dict.entries[i] == "AIR" || shipmode_dict.entries[i] == "AIR REG")
            shipmode_air[i] = true;
    }

    // ---------------------------------------------------------------
    // Step 1: Build branch_tag array from part dense array
    // ---------------------------------------------------------------
    const int32_t max_partkey = part.max_key;
    std::vector<uint8_t> branch_tag(max_partkey + 1, 0);
    size_t qualifying_count = 0;
    std::vector<int32_t> qualifying_partkeys;
    {
        MQO_TIME_PHASE("Q19_part_filter");
        qualifying_partkeys.reserve(5000);

        for (size_t r = 0; r < part.n_rows; ++r) {
            int32_t pk = part.p_partkey[r];
            int32_t sz = part.p_size[r];
            uint8_t br = part.p_brand[r];
            uint8_t ct = part.p_container[r];

            if (sz < 1) continue;  // all branches require p_size >= 1

            uint8_t tag = 0;
            if (br == brand12_code && container_branch1[ct] && sz <= 5)
                tag = 1;
            else if (br == brand23_code && container_branch2[ct] && sz <= 10)
                tag = 2;
            else if (br == brand34_code && container_branch3[ct] && sz <= 15)
                tag = 3;

            if (tag) {
                branch_tag[pk] = tag;
                qualifying_partkeys.push_back(pk);
            }
        }
        qualifying_count = qualifying_partkeys.size();
    }

    // ---------------------------------------------------------------
    // Step 2: Index lookup — gather lineitem row_ids for qualifying partkeys
    // ---------------------------------------------------------------
    const uint32_t* idx_offsets;
    const uint32_t* idx_rows;
    size_t total_li_rows = 0;
    std::vector<uint32_t> gathered_rowids;
    {
        MQO_TIME_PHASE("Q19_index_lookup");
        idx_offsets = mqo::io::mmap_column<uint32_t>(
            idx_dir + "/lineitem_partkey_grouped_offsets.bin",
            static_cast<size_t>(max_partkey) + 2);
        size_t rows_sz = 0;
        idx_rows = reinterpret_cast<const uint32_t*>(
            mqo::io::mmap_file_raw(idx_dir + "/lineitem_partkey_grouped_rows.bin", rows_sz));

        // Count total rows first
        size_t total = 0;
        for (size_t i = 0; i < qualifying_count; ++i) {
            int32_t pk = qualifying_partkeys[i];
            total += idx_offsets[pk + 1] - idx_offsets[pk];
        }
        gathered_rowids.reserve(total);

        for (size_t i = 0; i < qualifying_count; ++i) {
            int32_t pk = qualifying_partkeys[i];
            uint32_t start = idx_offsets[pk];
            uint32_t end = idx_offsets[pk + 1];
            for (uint32_t j = start; j < end; ++j) {
                gathered_rowids.push_back(idx_rows[j]);
            }
        }
        total_li_rows = gathered_rowids.size();
    }

    // ---------------------------------------------------------------
    // Step 3: Gather lineitem columns and filter + aggregate
    // ---------------------------------------------------------------
    double revenue = 0.0;
    {
        MQO_TIME_PHASE("Q19_scan_filter_agg");
        size_t n_li = 59986052;
        const int32_t*  l_partkey       = mqo::io::mmap_column<int32_t>(li_dir + "/l_partkey.bin", n_li);
        const double*   l_quantity      = mqo::io::mmap_column<double>(li_dir + "/l_quantity.bin", n_li);
        const uint8_t*  l_shipmode      = mqo::io::mmap_column<uint8_t>(li_dir + "/l_shipmode.bin", n_li);
        const uint8_t*  l_shipinstruct  = mqo::io::mmap_column<uint8_t>(li_dir + "/l_shipinstruct.bin", n_li);
        const double*   l_extendedprice = mqo::io::mmap_column<double>(li_dir + "/l_extendedprice.bin", n_li);
        const double*   l_discount      = mqo::io::mmap_column<double>(li_dir + "/l_discount.bin", n_li);

        for (size_t i = 0; i < total_li_rows; ++i) {
            uint32_t rid = gathered_rowids[i];

            // Cheapest filter first: shipinstruct (dict code comparison)
            if (l_shipinstruct[rid] != shipinstruct_dip_code) continue;

            // Next: shipmode IN (AIR, AIR REG)
            if (!shipmode_air[l_shipmode[rid]]) continue;

            // Branch-specific quantity filter
            uint8_t tag = branch_tag[l_partkey[rid]];
            double qty = l_quantity[rid];
            bool qty_ok = false;
            switch (tag) {
                case 1: qty_ok = (qty >= 1.0 && qty <= 11.0); break;
                case 2: qty_ok = (qty >= 10.0 && qty <= 20.0); break;
                case 3: qty_ok = (qty >= 20.0 && qty <= 30.0); break;
                default: continue;
            }
            if (!qty_ok) continue;

            revenue += l_extendedprice[rid] * (1.0 - l_discount[rid]);
        }
    }

    // ---------------------------------------------------------------
    // Step 4: Output
    // ---------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q19_output");
        std::string out_path = ctx.output_dir + "/q19.csv";
        FILE* fp = std::fopen(out_path.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", out_path.c_str());
            return;
        }
        std::fprintf(fp, "revenue\n");
        std::fprintf(fp, "%.2f\n", revenue);
        std::fclose(fp);
    }
}

}} // namespace mqo::tails
