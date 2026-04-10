// Q9 tail: Product Type Profit Measure
// Shared inputs: hash_part_by_partkey, hash_supplier_by_suppkey,
//                scan_partsupp_full, scan_orders_full
// Pipeline: filter part(%green%) -> indexed lineitem gather -> probe supplier
//           -> probe partsupp composite hash -> orders PK lookup -> nation lookup
//           -> compute amount -> hash group-by (nation, o_year) -> sort -> output

#include "mqo_profile.hpp"
#include "../shared/hash_part_by_partkey.hpp"
#include "../shared/hash_supplier_by_suppkey.hpp"
#include "../shared/scan_partsupp_full.hpp"
#include "../shared/scan_orders_full.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#ifdef _OPENMP
#include <omp.h>
#endif

namespace mqo { namespace tails {

// -------------------------------------------------------------------------
// Date helper: days since 1970-01-01 -> year (Howard Hinnant civil_from_days)
// -------------------------------------------------------------------------
static inline int days_to_year(int z) {
    z += 719468;
    int era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = static_cast<unsigned>(z - era * 146097);
    unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int y = static_cast<int>(yoe) + era * 400;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned mp = (5 * doy + 2) / 153;
    unsigned m = mp + (mp < 10 ? 3 : -9);
    return y + (m <= 2);
}

void run_Q9(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q9_tail");

    // =====================================================================
    // 0. Fetch shared components
    // =====================================================================
    const auto& part    = mqo::shared::hash_part_by_partkey::get();
    const auto& supp    = mqo::shared::hash_supplier_by_suppkey::get();
    const auto& ps      = mqo::shared::scan_partsupp_full::get();
    const auto& orders  = mqo::shared::scan_orders_full::get();

    const std::string gendb = ctx.gendb_dir;
    const std::string idx_dir = gendb + "/indexes";

    // =====================================================================
    // 1. Filter part: p_name LIKE '%green%' -> collect partkeys into vector
    // =====================================================================
    std::vector<int32_t> filtered_partkeys;
    {
        MQO_TIME_PHASE("Q9_filter_part");
        filtered_partkeys.reserve(60000);
        for (size_t i = 0; i < part.n_rows; ++i) {
            std::string_view name = part.p_name.get(i);
            if (name.find("green") != std::string_view::npos) {
                filtered_partkeys.push_back(part.p_partkey[i]);
            }
        }
    }

    // =====================================================================
    // 2. Load nation table (25 rows) -> nationkey_to_name[nationkey]
    // =====================================================================
    std::string nation_names[25];
    {
        MQO_TIME_PHASE("Q9_load_nation");
        std::string ndir = gendb + "/nation";
        size_t n_nation = mqo::io::read_row_count(ndir);
        const int32_t* n_nationkey = mqo::io::mmap_column<int32_t>(ndir + "/n_nationkey.bin", n_nation);
        const uint8_t* n_name_codes = mqo::io::mmap_column<uint8_t>(ndir + "/n_name.bin", n_nation);
        mqo::io::Dictionary n_name_dict;
        n_name_dict.load(ndir + "/n_name_dict.bin");
        for (size_t i = 0; i < n_nation; ++i) {
            int32_t nk = n_nationkey[i];
            if (nk >= 0 && nk < 25) {
                nation_names[nk] = n_name_dict.get(n_name_codes[i]);
            }
        }
    }

    // =====================================================================
    // 3. Load indexes: lineitem_partkey_grouped, partsupp_composite_hash,
    //    orders_pk_index
    // =====================================================================
    // lineitem_partkey_grouped: offsets_u32[max_key+2], row_ids_u32[num_rows]
    const uint32_t* li_pk_offsets;
    const uint32_t* li_pk_rows;
    size_t li_num_rows;
    {
        MQO_TIME_PHASE("Q9_load_indexes");
        size_t off_sz = 0;
        li_pk_offsets = reinterpret_cast<const uint32_t*>(
            mqo::io::mmap_file_raw(idx_dir + "/lineitem_partkey_grouped_offsets.bin", off_sz));
        size_t row_sz = 0;
        li_pk_rows = reinterpret_cast<const uint32_t*>(
            mqo::io::mmap_file_raw(idx_dir + "/lineitem_partkey_grouped_rows.bin", row_sz));
        li_num_rows = row_sz / sizeof(uint32_t);
    }

    // partsupp_composite_hash: capacity=16777216, mask=16777215
    // entry = {partkey_i32, suppkey_i32, row_id_i32, pad_i32}
    struct PSHashEntry {
        int32_t partkey;
        int32_t suppkey;
        int32_t row_id;
        int32_t pad;
    };
    const PSHashEntry* ps_hash;
    constexpr uint32_t PS_MASK = 16777215u;
    {
        size_t sz = 0;
        ps_hash = reinterpret_cast<const PSHashEntry*>(
            mqo::io::mmap_file_raw(idx_dir + "/partsupp_composite_hash.bin", sz));
    }

    // orders_pk_index: int32_t[max_orderkey+1], pk_index[orderkey] = row_id
    const int32_t* orders_pk;
    {
        size_t sz = 0;
        orders_pk = reinterpret_cast<const int32_t*>(
            mqo::io::mmap_file_raw(idx_dir + "/orders_pk_index.bin", sz));
    }

    // =====================================================================
    // 4. Load lineitem columns (only those needed: orderkey, partkey, suppkey,
    //    quantity, extendedprice, discount)
    // =====================================================================
    const int32_t* l_orderkey;
    const int32_t* l_partkey;
    const int32_t* l_suppkey;
    const double*  l_quantity;
    const double*  l_extendedprice;
    const double*  l_discount;
    {
        MQO_TIME_PHASE("Q9_load_lineitem_cols");
        std::string ldir = gendb + "/lineitem";
        size_t ln = mqo::io::read_row_count(ldir);
        l_orderkey      = mqo::io::mmap_column<int32_t>(ldir + "/l_orderkey.bin", ln);
        l_partkey       = mqo::io::mmap_column<int32_t>(ldir + "/l_partkey.bin", ln);
        l_suppkey       = mqo::io::mmap_column<int32_t>(ldir + "/l_suppkey.bin", ln);
        l_quantity      = mqo::io::mmap_column<double> (ldir + "/l_quantity.bin", ln);
        l_extendedprice = mqo::io::mmap_column<double> (ldir + "/l_extendedprice.bin", ln);
        l_discount      = mqo::io::mmap_column<double> (ldir + "/l_discount.bin", ln);
    }

    // =====================================================================
    // 5. Main scan: iterate filtered partkeys, gather lineitem rows via
    //    partkey grouped index, probe supplier/partsupp/orders, aggregate
    // =====================================================================
    // Aggregation key: (nationkey * 16 + (year - 1990))
    // 25 nations * ~10 years = ~250 possible groups (fits in small array)
    constexpr int YEAR_BASE = 1990;
    constexpr int MAX_YEARS = 16;
    constexpr int AGG_SIZE = 25 * MAX_YEARS;  // 400

    // Use thread-local aggregation for parallelism
    int max_threads = 1;
    #ifdef _OPENMP
    // Limit threads to avoid oversubscription when running with other tails
    max_threads = std::min(omp_get_max_threads(), 16);
    #endif

    // Allocate aggregation arrays
    std::vector<double> agg_sum(static_cast<size_t>(max_threads) * AGG_SIZE, 0.0);
    std::vector<bool> agg_present(static_cast<size_t>(max_threads) * AGG_SIZE, false);

    {
        MQO_TIME_PHASE("Q9_main_scan");
        int n_parts = static_cast<int>(filtered_partkeys.size());

        #pragma omp parallel for schedule(dynamic, 64) num_threads(max_threads)
        for (int pi = 0; pi < n_parts; ++pi) {
            int tid = 0;
            #ifdef _OPENMP
            tid = omp_get_thread_num();
            #endif
            double* my_sum = agg_sum.data() + static_cast<size_t>(tid) * AGG_SIZE;
            bool* my_present = agg_present.data() + static_cast<size_t>(tid) * AGG_SIZE;

            int32_t pk = filtered_partkeys[pi];
            uint32_t start = li_pk_offsets[pk];
            uint32_t end   = li_pk_offsets[pk + 1];

            for (uint32_t ri = start; ri < end; ++ri) {
                uint32_t row = li_pk_rows[ri];

                int32_t l_sk = l_suppkey[row];
                int32_t l_ok = l_orderkey[row];
                int32_t l_pk = l_partkey[row];

                // Probe supplier -> get s_nationkey
                int32_t s_row = supp.pk_index[l_sk];
                int32_t s_nk = supp.s_nationkey[s_row];

                // Probe partsupp composite hash -> get ps_supplycost
                // hash = pk * 2654435761 ^ sk * 40503
                uint32_t h = static_cast<uint32_t>(l_pk) * 2654435761u
                           ^ static_cast<uint32_t>(l_sk) * 40503u;
                h &= PS_MASK;
                double ps_cost = 0.0;
                while (true) {
                    const auto& e = ps_hash[h];
                    if (e.partkey == l_pk && e.suppkey == l_sk) {
                        ps_cost = ps.ps_supplycost[e.row_id];
                        break;
                    }
                    h = (h + 1) & PS_MASK;
                }

                // Probe orders pk index -> get o_orderdate
                int32_t o_row = orders_pk[l_ok];
                int32_t odate = orders.o_orderdate[o_row];
                int year = days_to_year(odate);

                // Compute amount
                double amount = l_extendedprice[row] * (1.0 - l_discount[row])
                              - ps_cost * l_quantity[row];

                // Aggregate into (nationkey, year)
                int slot = s_nk * MAX_YEARS + (year - YEAR_BASE);
                my_sum[slot] += amount;
                my_present[slot] = true;
            }
        }
    }

    // =====================================================================
    // 6. Merge thread-local aggregates
    // =====================================================================
    struct Result {
        std::string nation;
        int year;
        double sum_profit;
    };
    std::vector<Result> results;
    {
        MQO_TIME_PHASE("Q9_merge_sort");
        // Merge into first slot
        double* base_sum = agg_sum.data();
        bool* base_present = agg_present.data();
        for (int t = 1; t < max_threads; ++t) {
            const double* ts = agg_sum.data() + static_cast<size_t>(t) * AGG_SIZE;
            const bool* tp = agg_present.data() + static_cast<size_t>(t) * AGG_SIZE;
            for (int i = 0; i < AGG_SIZE; ++i) {
                if (tp[i]) {
                    base_sum[i] += ts[i];
                    base_present[i] = true;
                }
            }
        }

        // Collect results
        results.reserve(200);
        for (int nk = 0; nk < 25; ++nk) {
            for (int y = 0; y < MAX_YEARS; ++y) {
                int slot = nk * MAX_YEARS + y;
                if (base_present[slot]) {
                    results.push_back({nation_names[nk], y + YEAR_BASE, base_sum[slot]});
                }
            }
        }

        // Sort by nation ASC, o_year DESC
        std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
            int cmp = a.nation.compare(b.nation);
            if (cmp != 0) return cmp < 0;
            return a.year > b.year;
        });
    }

    // =====================================================================
    // 7. Output CSV
    // =====================================================================
    {
        MQO_TIME_PHASE("Q9_output");
        std::string path = ctx.output_dir + "/q9.csv";
        FILE* f = std::fopen(path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", path.c_str());
            return;
        }
        std::fprintf(f, "nation,o_year,sum_profit\n");
        for (const auto& r : results) {
            std::fprintf(f, "%s,%d,%.2f\n", r.nation.c_str(), r.year, r.sum_profit);
        }
        std::fclose(f);
    }
}

}} // namespace mqo::tails
