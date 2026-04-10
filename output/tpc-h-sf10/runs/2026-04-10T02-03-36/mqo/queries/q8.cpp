// Q8 tail: National Market Share
// Shared inputs: scan_lineitem_full (callback), scan_orders_full, hash_customer_by_custkey,
//                hash_part_by_partkey, hash_supplier_by_suppkey
// Local tables: nation (25 rows), region (5 rows)

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_full.hpp"
#include "shared/scan_orders_full.hpp"
#include "shared/hash_customer_by_custkey.hpp"
#include "shared/hash_part_by_partkey.hpp"
#include "shared/hash_supplier_by_suppkey.hpp"

#include <bitset>
#include <cstdio>
#include <cstring>
#include <string>

namespace mqo { namespace tails {

void run_Q8(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q8_tail");

    // ---------------------------------------------------------------
    // Step 1: Load region, find AMERICA regionkey
    // ---------------------------------------------------------------
    int32_t america_regionkey = -1;
    {
        MQO_TIME_PHASE("Q8_load_dims");
        std::string rdir = ctx.gendb_dir + "/region";
        size_t r_n = mqo::io::read_row_count(rdir);
        const int32_t* r_regionkey = mqo::io::mmap_column<int32_t>(rdir + "/r_regionkey.bin", r_n);
        const uint8_t* r_name_codes = mqo::io::mmap_column<uint8_t>(rdir + "/r_name.bin", r_n);
        mqo::io::Dictionary r_name_dict;
        r_name_dict.load(rdir + "/r_name_dict.bin");

        for (size_t i = 0; i < r_n; ++i) {
            if (r_name_dict.get(r_name_codes[i]) == "AMERICA") {
                america_regionkey = r_regionkey[i];
                break;
            }
        }

        // Step 2: Load nation, build america_nations bitset and find brazil_nationkey
        std::string ndir = ctx.gendb_dir + "/nation";
        size_t n_n = mqo::io::read_row_count(ndir);
        const int32_t* n_nationkey = mqo::io::mmap_column<int32_t>(ndir + "/n_nationkey.bin", n_n);
        const int32_t* n_regionkey = mqo::io::mmap_column<int32_t>(ndir + "/n_regionkey.bin", n_n);
        const uint8_t* n_name_codes = mqo::io::mmap_column<uint8_t>(ndir + "/n_name.bin", n_n);
        mqo::io::Dictionary n_name_dict;
        n_name_dict.load(ndir + "/n_name_dict.bin");

        // These will be captured below — declare at wider scope
        // Actually we need them outside this block. Let's restructure.
        (void)n_nationkey; (void)n_regionkey; (void)n_name_codes; (void)n_name_dict;
    }

    // Re-load nation (tiny, negligible cost) to keep variables in scope for the main pipeline
    std::string ndir = ctx.gendb_dir + "/nation";
    size_t n_n = mqo::io::read_row_count(ndir);
    const int32_t* n_nationkey = mqo::io::mmap_column<int32_t>(ndir + "/n_nationkey.bin", n_n);
    const int32_t* n_regionkey = mqo::io::mmap_column<int32_t>(ndir + "/n_regionkey.bin", n_n);
    const uint8_t* n_name_codes = mqo::io::mmap_column<uint8_t>(ndir + "/n_name.bin", n_n);
    mqo::io::Dictionary n_name_dict;
    n_name_dict.load(ndir + "/n_name_dict.bin");

    // Re-find america_regionkey if needed
    if (america_regionkey < 0) {
        std::string rdir = ctx.gendb_dir + "/region";
        size_t r_n = mqo::io::read_row_count(rdir);
        const int32_t* r_regionkey = mqo::io::mmap_column<int32_t>(rdir + "/r_regionkey.bin", r_n);
        const uint8_t* r_name_codes = mqo::io::mmap_column<uint8_t>(rdir + "/r_name.bin", r_n);
        mqo::io::Dictionary r_name_dict;
        r_name_dict.load(rdir + "/r_name_dict.bin");
        for (size_t i = 0; i < r_n; ++i) {
            if (r_name_dict.get(r_name_codes[i]) == "AMERICA") {
                america_regionkey = r_regionkey[i];
                break;
            }
        }
    }

    // Build america_nations bitset (nationkeys in AMERICA region)
    bool america_nations[25] = {};
    int32_t brazil_nationkey = -1;
    for (size_t i = 0; i < n_n; ++i) {
        int32_t nk = n_nationkey[i];
        if (n_regionkey[i] == america_regionkey) {
            if (nk >= 0 && nk < 25) america_nations[nk] = true;
        }
        if (n_name_dict.get(n_name_codes[i]) == "BRAZIL") {
            brazil_nationkey = nk;
        }
    }

    // ---------------------------------------------------------------
    // Step 3: Build part filter bitset (p_type = 'ECONOMY ANODIZED STEEL')
    // ---------------------------------------------------------------
    const auto& part = mqo::shared::hash_part_by_partkey::get();
    // p_type is dict-encoded uint8_t — find the code for 'ECONOMY ANODIZED STEEL'
    mqo::io::Dictionary p_type_dict;
    p_type_dict.load(ctx.gendb_dir + "/part/p_type_dict.bin");

    uint8_t target_p_type_code = 255;
    for (uint32_t i = 0; i < p_type_dict.count; ++i) {
        if (p_type_dict.entries[i] == "ECONOMY ANODIZED STEEL") {
            target_p_type_code = static_cast<uint8_t>(i);
            break;
        }
    }

    // Build bitset over partkey space (max_key = 2000000)
    // Use a vector<bool> style bitset — 2000001 bits = ~244KB, fits L3
    static constexpr size_t PART_BITSET_SIZE = 2000001;
    std::vector<uint64_t> part_bitset((PART_BITSET_SIZE + 63) / 64, 0);

    {
        MQO_TIME_PHASE("Q8_build_part_filter");
        for (size_t i = 0; i < part.n_rows; ++i) {
            if (part.p_type[i] == target_p_type_code) {
                int32_t pk = part.p_partkey[i];
                if (pk >= 0 && static_cast<size_t>(pk) < PART_BITSET_SIZE) {
                    part_bitset[pk >> 6] |= (1ULL << (pk & 63));
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 4: Get shared components for lookups
    // ---------------------------------------------------------------
    const auto& orders = mqo::shared::scan_orders_full::get();
    const auto& customer = mqo::shared::hash_customer_by_custkey::get();
    const auto& supplier = mqo::shared::hash_supplier_by_suppkey::get();

    // Build orders pk index: orderkey -> row_id
    // orders_pk_index.bin: 60000001 int32 entries (orderkey 0..60000000)
    static constexpr size_t ORDERS_PK_SIZE = 60000001;
    const int32_t* orders_pk = mqo::io::mmap_column<int32_t>(
        ctx.gendb_dir + "/indexes/orders_pk_index.bin", ORDERS_PK_SIZE);

    // Date constants (days since epoch 1970-01-01)
    static constexpr int32_t DATE_1995_01_01 = 9131;
    static constexpr int32_t DATE_1996_12_31 = 9861;
    // Year extraction: 1995 = year of DATE_1995_01_01
    // We need extract_year from days_since_epoch. Let's compute year boundaries.
    // 1995-01-01 = 9131, 1996-01-01 = 9496, 1997-01-01 = 9862
    static constexpr int32_t DATE_1996_01_01 = 9496;

    // ---------------------------------------------------------------
    // Step 4-5: Main scan pipeline with thread-local aggregation
    // ---------------------------------------------------------------
    // Aggregation: 2 years (1995, 1996) × 2 accumulators (brazil_vol, total_vol)
    const auto& li = mqo::shared::scan_lineitem_full::get();

    // Global aggregation arrays
    double global_brazil_vol[2] = {0.0, 0.0};
    double global_total_vol[2] = {0.0, 0.0};

    {
        MQO_TIME_PHASE("Q8_main_scan");

        #pragma omp parallel
        {
            double local_brazil_vol[2] = {0.0, 0.0};
            double local_total_vol[2] = {0.0, 0.0};

            #pragma omp for schedule(static)
            for (size_t i = 0; i < li.n_rows; ++i) {
                // 4a: Semi-join filter on part (bitset test)
                int32_t l_pk = li.l_partkey[i];
                if (l_pk < 0 || static_cast<size_t>(l_pk) >= PART_BITSET_SIZE) continue;
                if (!(part_bitset[l_pk >> 6] & (1ULL << (l_pk & 63)))) continue;

                // 4b: Lookup orders via dense_pk on l_orderkey
                int32_t l_ok = li.l_orderkey[i];
                if (l_ok < 0 || static_cast<size_t>(l_ok) >= ORDERS_PK_SIZE) continue;
                int32_t o_row = orders_pk[l_ok];
                if (o_row < 0) continue;

                // 4c: Date filter on o_orderdate
                int32_t odate = orders.o_orderdate[o_row];
                if (odate < DATE_1995_01_01 || odate > DATE_1996_12_31) continue;

                // 4d: Lookup customer via dense_pk on o_custkey
                int32_t o_ck = orders.o_custkey[o_row];
                if (o_ck < 0 || o_ck > customer.max_key) continue;
                int32_t c_row = customer.pk_index[o_ck];
                if (c_row < 0) continue;

                // 4e: Filter customer nation in AMERICA region
                int32_t c_nk = customer.c_nationkey[c_row];
                if (c_nk < 0 || c_nk >= 25 || !america_nations[c_nk]) continue;

                // 4f: Lookup supplier via dense_pk on l_suppkey
                int32_t l_sk = li.l_suppkey[i];
                if (l_sk < 0 || l_sk > supplier.max_key) continue;
                int32_t s_row = supplier.pk_index[l_sk];
                if (s_row < 0) continue;

                // 4g: Compute and aggregate
                int32_t s_nk = supplier.s_nationkey[s_row];
                double volume = li.l_extendedprice[i] * (1.0 - li.l_discount[i]);

                // Determine year index: 0 = 1995, 1 = 1996
                int year_idx = (odate < DATE_1996_01_01) ? 0 : 1;

                local_total_vol[year_idx] += volume;
                if (s_nk == brazil_nationkey) {
                    local_brazil_vol[year_idx] += volume;
                }
            }

            // Merge thread-local into global
            #pragma omp critical
            {
                for (int y = 0; y < 2; ++y) {
                    global_brazil_vol[y] += local_brazil_vol[y];
                    global_total_vol[y] += local_total_vol[y];
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 6-7: Finalize and output (sorted by o_year ASC: 1995, 1996)
    // ---------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q8_output");
        std::string outpath = ctx.output_dir + "/q8.csv";
        FILE* fp = std::fopen(outpath.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", outpath.c_str());
            return;
        }
        std::fprintf(fp, "o_year,mkt_share\n");
        for (int y = 0; y < 2; ++y) {
            int year = 1995 + y;
            double mkt_share = (global_total_vol[y] > 0.0)
                ? global_brazil_vol[y] / global_total_vol[y]
                : 0.0;
            std::fprintf(fp, "%d,%.8f\n", year, mkt_share);
        }
        std::fclose(fp);
    }
}

}} // namespace mqo::tails
