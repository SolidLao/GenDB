// Q17 tail — Small-Quantity-Order Revenue
// Shared input: scan_lineitem_full (l_partkey, l_quantity, l_extendedprice)
// Strategy: bitset semi-join on filtered part (Brand#23, MED BOX),
//           single-pass accumulate per-partkey AVG + materialize matches,
//           post-scan threshold filter + scalar SUM.

#include "mqo_profile.hpp"
#include "../shared/mqo_io.hpp"
#include "../shared/scan_lineitem_full.hpp"

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace mqo::tails {

void run_Q17(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q17_tail");

    // -----------------------------------------------------------------------
    // Step 1: Scan part table, filter Brand#23 + MED BOX, build bitset
    // -----------------------------------------------------------------------
    constexpr int MAX_PARTKEY = 2000001;
    std::vector<bool> matching_pk(MAX_PARTKEY, false);

    {
        MQO_TIME_PHASE("Q17_part_filter");
        const std::string pdir = ctx.gendb_dir + "/part/";
        const size_t n_part = mqo::io::read_row_count(pdir + "meta.txt");

        // Read dictionaries to resolve codes
        auto brand_dict = mqo::io::read_dictionary(pdir + "p_brand_dict.bin");
        auto container_dict = mqo::io::read_dictionary(pdir + "p_container_dict.bin");

        uint8_t brand_code = 255;
        uint8_t container_code = 255;
        for (size_t i = 0; i < brand_dict.size(); ++i) {
            if (brand_dict[i] == "Brand#23") { brand_code = static_cast<uint8_t>(i); break; }
        }
        for (size_t i = 0; i < container_dict.size(); ++i) {
            if (container_dict[i] == "MED BOX") { container_code = static_cast<uint8_t>(i); break; }
        }

        // mmap part columns
        const auto* p_partkey   = mqo::io::mmap_column<int32_t>(pdir + "p_partkey.bin", n_part);
        const auto* p_brand     = mqo::io::mmap_column<uint8_t>(pdir + "p_brand.bin", n_part);
        const auto* p_container = mqo::io::mmap_column<uint8_t>(pdir + "p_container.bin", n_part);

        for (size_t i = 0; i < n_part; ++i) {
            if (p_brand[i] == brand_code && p_container[i] == container_code) {
                int32_t pk = p_partkey[i];
                if (pk >= 0 && pk < MAX_PARTKEY) {
                    matching_pk[static_cast<size_t>(pk)] = true;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 2: Single pass over shared lineitem scan — accumulate per-partkey
    //         (sum_qty, count) and materialize matching rows
    // -----------------------------------------------------------------------
    struct PerPKAgg {
        double sum_qty = 0.0;
        int64_t count  = 0;
    };
    std::vector<PerPKAgg> agg(MAX_PARTKEY);

    struct MatRow {
        int32_t partkey;
        double  quantity;
        double  extendedprice;
    };
    std::vector<MatRow> mat_buf;
    mat_buf.reserve(65536);

    {
        MQO_TIME_PHASE("Q17_lineitem_scan");
        const auto& cols = mqo::shared::scan_lineitem_full::get_columns(ctx);
        const size_t n = cols.n_rows;
        const int32_t* l_partkey       = cols.l_partkey;
        const double*  l_quantity      = cols.l_quantity;
        const double*  l_extendedprice = cols.l_extendedprice;

        for (size_t i = 0; i < n; ++i) {
            int32_t pk = l_partkey[i];
            if (pk >= 0 && pk < MAX_PARTKEY && matching_pk[static_cast<size_t>(pk)]) {
                double qty = l_quantity[i];
                agg[static_cast<size_t>(pk)].sum_qty += qty;
                agg[static_cast<size_t>(pk)].count += 1;
                mat_buf.push_back({pk, qty, l_extendedprice[i]});
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 3: Compute per-partkey threshold = 0.2 * AVG(l_quantity)
    // -----------------------------------------------------------------------
    std::vector<double> threshold(MAX_PARTKEY, 0.0);
    {
        MQO_TIME_PHASE("Q17_thresholds");
        for (int pk = 0; pk < MAX_PARTKEY; ++pk) {
            if (agg[static_cast<size_t>(pk)].count > 0) {
                const auto& a = agg[static_cast<size_t>(pk)];
                threshold[static_cast<size_t>(pk)] = 0.2 * (a.sum_qty / static_cast<double>(a.count));
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 4: Final filter + aggregate over materialized buffer
    // -----------------------------------------------------------------------
    double total_extprice = 0.0;
    {
        MQO_TIME_PHASE("Q17_final_agg");
        for (const auto& r : mat_buf) {
            if (r.quantity < threshold[static_cast<size_t>(r.partkey)]) {
                total_extprice += r.extendedprice;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 5: Output
    // -----------------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q17_output");
        double avg_yearly = total_extprice / 7.0;
        std::string outpath = ctx.output_dir + "/q17.csv";
        FILE* f = std::fopen(outpath.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "[Q17] Cannot open output: %s\n", outpath.c_str());
            return;
        }
        std::fprintf(f, "avg_yearly\n");
        std::fprintf(f, "%.2f\n", avg_yearly);
        std::fclose(f);
    }
}

}  // namespace mqo::tails
