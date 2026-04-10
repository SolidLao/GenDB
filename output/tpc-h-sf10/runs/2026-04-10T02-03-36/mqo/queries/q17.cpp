// Q17 tail — Small-Quantity-Order Revenue
// Shared input: hash_part_by_partkey (part table dense array)
// Plan: filter part(Brand#23, MED BOX) → index gather lineitem → per-group correlated subquery → global sum / 7.0

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/hash_part_by_partkey.hpp"

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace mqo { namespace tails {

void run_Q17(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q17_tail");

    // -----------------------------------------------------------------------
    // Step 0: Load dictionaries for p_brand and p_container
    // -----------------------------------------------------------------------
    const auto& part = mqo::shared::hash_part_by_partkey::get();

    mqo::io::Dictionary brand_dict, container_dict;
    {
        MQO_TIME_PHASE("Q17_load_dicts");
        brand_dict.load(ctx.gendb_dir + "/part/p_brand_dict.bin");
        container_dict.load(ctx.gendb_dir + "/part/p_container_dict.bin");
    }

    // Resolve target codes
    uint8_t brand_code = 255, container_code = 255;
    for (uint32_t i = 0; i < brand_dict.count; ++i) {
        if (brand_dict.entries[i] == "Brand#23") { brand_code = static_cast<uint8_t>(i); break; }
    }
    for (uint32_t i = 0; i < container_dict.count; ++i) {
        if (container_dict.entries[i] == "MED BOX") { container_code = static_cast<uint8_t>(i); break; }
    }

    // -----------------------------------------------------------------------
    // Step 1: Filter part table → qualifying partkeys
    // -----------------------------------------------------------------------
    std::vector<int32_t> qualifying_partkeys;
    {
        MQO_TIME_PHASE("Q17_part_filter");
        qualifying_partkeys.reserve(4000);
        for (size_t i = 0; i < part.n_rows; ++i) {
            if (part.p_brand[i] == brand_code && part.p_container[i] == container_code) {
                qualifying_partkeys.push_back(part.p_partkey[i]);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 2 & 3: Index gather + per-group correlated filter aggregation
    // -----------------------------------------------------------------------
    // mmap the lineitem_partkey_grouped index and the l_quantity / l_extendedprice columns
    const uint32_t* idx_offsets;
    const uint32_t* idx_rows;
    const double* l_quantity;
    const double* l_extendedprice;
    {
        MQO_TIME_PHASE("Q17_mmap_lineitem");
        std::string idx_base = ctx.gendb_dir + "/indexes/lineitem_partkey_grouped";
        // offsets: max_key+2 entries (2000002 uint32_t)
        idx_offsets = mqo::io::mmap_column<uint32_t>(idx_base + "_offsets.bin", 2000002);
        // row_ids: 59986052 entries
        size_t num_rows_li = 59986052;
        idx_rows = mqo::io::mmap_column<uint32_t>(idx_base + "_rows.bin", num_rows_li);

        std::string li_dir = ctx.gendb_dir + "/lineitem";
        l_quantity       = mqo::io::mmap_column<double>(li_dir + "/l_quantity.bin", num_rows_li);
        l_extendedprice  = mqo::io::mmap_column<double>(li_dir + "/l_extendedprice.bin", num_rows_li);
    }

    // -----------------------------------------------------------------------
    // Per-partkey two-phase processing (decorrelated correlated subquery)
    // Phase A: compute AVG(l_quantity) → threshold = 0.2 * avg
    // Phase B: filter l_quantity < threshold, accumulate SUM(l_extendedprice)
    // -----------------------------------------------------------------------
    double global_sum = 0.0;
    {
        MQO_TIME_PHASE("Q17_correlated_agg");

        const size_t n_qual = qualifying_partkeys.size();

        #pragma omp parallel for schedule(dynamic, 16) num_threads(16) reduction(+:global_sum)
        for (size_t qi = 0; qi < n_qual; ++qi) {
            int32_t pk = qualifying_partkeys[qi];
            uint32_t start = idx_offsets[pk];
            uint32_t end   = idx_offsets[pk + 1];
            uint32_t cnt   = end - start;
            if (cnt == 0) continue;

            // Phase A: compute average quantity
            double sum_qty = 0.0;
            for (uint32_t j = start; j < end; ++j) {
                uint32_t rid = idx_rows[j];
                sum_qty += l_quantity[rid];
            }
            double threshold = 0.2 * (sum_qty / cnt);

            // Phase B: filter and accumulate
            double partial = 0.0;
            for (uint32_t j = start; j < end; ++j) {
                uint32_t rid = idx_rows[j];
                if (l_quantity[rid] < threshold) {
                    partial += l_extendedprice[rid];
                }
            }
            global_sum += partial;
        }
    }

    // -----------------------------------------------------------------------
    // Step 4: Finalize and output
    // -----------------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q17_output");
        double avg_yearly = global_sum / 7.0;
        std::string out_path = ctx.output_dir + "/q17.csv";
        FILE* f = std::fopen(out_path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", out_path.c_str());
            return;
        }
        std::fprintf(f, "avg_yearly\n");
        std::fprintf(f, "%.2f\n", avg_yearly);
        std::fclose(f);
    }
}

}} // namespace mqo::tails
