// Q9 tail — Product Type Profit Measure
// Shared inputs: scan_lineitem_full, hash_orders_by_orderkey, hash_supplier_by_suppkey
// Tail ops: scan part (filter p_name LIKE '%green%'), build part hash set,
//           load nation direct array, load partsupp composite hash index,
//           fused probe pipeline over lineitem, 2D direct-array aggregation, sort, output

#include "mqo_profile.hpp"
#include "../shared/mqo_io.hpp"
#include "../shared/scan_lineitem_full.hpp"
#include "../shared/hash_orders_by_orderkey.hpp"
#include "../shared/hash_supplier_by_suppkey.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>
#include <unordered_set>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace mqo::tails {

// Partsupp composite hash index entry (matches partsupp_composite_hash.bin format)
struct PSHashEntry {
    int32_t partkey;
    int32_t suppkey;
    int32_t row_id;
    int32_t _pad;
};

void run_Q9(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q9_tail");

    // -----------------------------------------------------------------------
    // Step 1: Scan part, filter p_name LIKE '%green%', collect matching partkeys
    // -----------------------------------------------------------------------
    std::unordered_set<int32_t> part_set;
    {
        MQO_TIME_PHASE("Q9_part_filter");
        const std::string pb = ctx.gendb_dir + "/part/";
        const size_t n_part = mqo::io::read_row_count(pb + "meta.txt");
        const int32_t* p_partkey = mqo::io::mmap_column<int32_t>(pb + "p_partkey.bin", n_part);

        // p_name is varlen
        mqo::io::VarlenAccessor va_pname = mqo::io::mmap_varlen(pb, "p_name");

        part_set.reserve(50000);
        for (size_t i = 0; i < n_part; ++i) {
            std::string_view name = va_pname.get(i);
            if (name.find("green") != std::string_view::npos) {
                part_set.insert(p_partkey[i]);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 2: Load nation direct array (25 entries, indexed by n_nationkey)
    // -----------------------------------------------------------------------
    std::string nation_names[25];
    {
        MQO_TIME_PHASE("Q9_nation_load");
        const std::string nb = ctx.gendb_dir + "/nation/";
        const size_t n_nation = mqo::io::read_row_count(nb + "meta.txt");
        const int32_t* n_nationkey = mqo::io::mmap_column<int32_t>(nb + "n_nationkey.bin", n_nation);
        // n_name is dict-encoded: uint8_t codes + dictionary
        const uint8_t* n_name_codes = mqo::io::mmap_column<uint8_t>(nb + "n_name.bin", n_nation);
        std::vector<std::string> n_name_dict = mqo::io::read_dictionary(nb + "n_name_dict.bin");

        for (size_t i = 0; i < n_nation; ++i) {
            int32_t nk = n_nationkey[i];
            if (nk >= 0 && nk < 25) {
                nation_names[nk] = n_name_dict[n_name_codes[i]];
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 3: Load partsupp composite hash index
    // -----------------------------------------------------------------------
    const PSHashEntry* ps_hash = nullptr;
    const double* ps_supplycost = nullptr;
    uint32_t ps_mask = 0;
    {
        MQO_TIME_PHASE("Q9_partsupp_index_load");
        const std::string idx_path = ctx.gendb_dir + "/indexes/partsupp_composite_hash.bin";
        // Meta: capacity=16777216, mask=16777215, entry_size=16, hash=pk*2654435761^sk*40503
        ps_mask = 16777215u;  // capacity - 1
        size_t sz = 0;
        void* ptr = mqo::io::mmap_file(idx_path, sz);
        ps_hash = static_cast<const PSHashEntry*>(ptr);

        // Also mmap ps_supplycost column for payload lookup
        const std::string psb = ctx.gendb_dir + "/partsupp/";
        const size_t n_ps = mqo::io::read_row_count(psb + "meta.txt");
        ps_supplycost = mqo::io::mmap_column<double>(psb + "ps_supplycost.bin", n_ps);
    }

    // -----------------------------------------------------------------------
    // Step 4: Get shared component references
    // -----------------------------------------------------------------------
    const auto& li_cols = mqo::shared::scan_lineitem_full::get_columns(ctx);
    const auto& orders_ht = mqo::shared::hash_orders_by_orderkey::get();
    const auto& supplier_ht = mqo::shared::hash_supplier_by_suppkey::get();

    // -----------------------------------------------------------------------
    // Step 5: Fused probe pipeline over lineitem with thread-local 2D aggregation
    // Year range: 1992-1998 (7 years), 25 nations => 175 cells
    // -----------------------------------------------------------------------
    static constexpr int YEAR_MIN = 1992;
    static constexpr int YEAR_MAX = 1998;
    static constexpr int N_YEARS = YEAR_MAX - YEAR_MIN + 1;  // 7
    static constexpr int N_NATIONS = 25;

    // Precompute year for each possible o_orderdate epoch day value
    // o_orderdate range: roughly 1992-01-01 to 1998-12-31
    // epoch days for 1992-01-01 = ?
    static constexpr int32_t DATE_MIN = mqo::io::to_epoch_days(1992, 1, 1);  // 8035
    static constexpr int32_t DATE_MAX = mqo::io::to_epoch_days(1998, 12, 31);  // 10591
    static constexpr int DATE_RANGE = DATE_MAX - DATE_MIN + 1;

    // Build a date-to-year lookup table
    int8_t date_to_year_offset[DATE_RANGE];  // year - YEAR_MIN
    {
        for (int i = 0; i < DATE_RANGE; ++i) {
            int y, m, d;
            mqo::io::from_epoch_days(DATE_MIN + i, y, m, d);
            date_to_year_offset[i] = static_cast<int8_t>(y - YEAR_MIN);
        }
    }

    const size_t n_rows = li_cols.n_rows;

    // Determine thread count
    int n_threads = 1;
    #pragma omp parallel
    {
        #pragma omp single
        n_threads = omp_get_num_threads();
    }

    // Allocate thread-local aggregation arrays
    std::vector<double> tl_agg(static_cast<size_t>(n_threads) * N_NATIONS * N_YEARS, 0.0);

    {
        MQO_TIME_PHASE("Q9_main_scan");
        const int32_t* l_orderkey = li_cols.l_orderkey;
        const int32_t* l_partkey = li_cols.l_partkey;
        const int32_t* l_suppkey = li_cols.l_suppkey;
        const double* l_quantity = li_cols.l_quantity;
        const double* l_extendedprice = li_cols.l_extendedprice;
        const double* l_discount = li_cols.l_discount;

        #pragma omp parallel
        {
            const int tid = omp_get_thread_num();
            double* my_agg = tl_agg.data() + static_cast<size_t>(tid) * N_NATIONS * N_YEARS;

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_rows; ++i) {
                // 5a: Semi-join probe on part
                const int32_t pk = l_partkey[i];
                if (part_set.find(pk) == part_set.end()) continue;

                // 5b: Probe supplier hash for s_nationkey
                const int32_t sk = l_suppkey[i];
                const auto* se = supplier_ht.probe(sk);
                if (!se) continue;
                const int32_t s_nationkey = se->s_nationkey;

                // 5c: Probe partsupp composite hash for ps_supplycost
                // Hash: pk * 2654435761 ^ sk * 40503
                uint32_t h = static_cast<uint32_t>(pk) * 2654435761u ^ static_cast<uint32_t>(sk) * 40503u;
                h &= ps_mask;
                double ps_sc = 0.0;
                bool ps_found = false;
                for (;;) {
                    const PSHashEntry& pe = ps_hash[h];
                    if (pe.partkey == 0 && pe.suppkey == 0) break;  // empty slot
                    if (pe.partkey == pk && pe.suppkey == sk) {
                        ps_sc = ps_supplycost[pe.row_id];
                        ps_found = true;
                        break;
                    }
                    h = (h + 1) & ps_mask;
                }
                if (!ps_found) continue;

                // 5d: Probe orders hash for o_orderdate
                const int32_t ok = l_orderkey[i];
                const auto* oe = orders_ht.probe(ok);
                if (!oe) continue;
                const int32_t odate = oe->o_orderdate;

                // 5e: Nation name lookup via s_nationkey (direct array, deferred to output)
                // 5f: Compute amount and year
                const double amount = l_extendedprice[i] * (1.0 - l_discount[i]) - ps_sc * l_quantity[i];

                // Convert date to year offset
                int year_off;
                if (odate >= DATE_MIN && odate <= DATE_MAX) {
                    year_off = date_to_year_offset[odate - DATE_MIN];
                } else {
                    int y, m, d;
                    mqo::io::from_epoch_days(odate, y, m, d);
                    year_off = y - YEAR_MIN;
                }

                if (year_off < 0 || year_off >= N_YEARS) continue;
                if (s_nationkey < 0 || s_nationkey >= N_NATIONS) continue;

                // 5g: Thread-local aggregate
                my_agg[s_nationkey * N_YEARS + year_off] += amount;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 6: Merge thread-local aggregates
    // -----------------------------------------------------------------------
    double global_agg[N_NATIONS * N_YEARS];
    {
        MQO_TIME_PHASE("Q9_merge_agg");
        std::memset(global_agg, 0, sizeof(global_agg));
        for (int t = 0; t < n_threads; ++t) {
            const double* src = tl_agg.data() + static_cast<size_t>(t) * N_NATIONS * N_YEARS;
            for (int j = 0; j < N_NATIONS * N_YEARS; ++j) {
                global_agg[j] += src[j];
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 7: Project, sort, and output
    // -----------------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q9_output");

        struct ResultRow {
            const char* nation;
            int o_year;
            double sum_profit;
        };

        std::vector<ResultRow> results;
        results.reserve(N_NATIONS * N_YEARS);

        for (int n = 0; n < N_NATIONS; ++n) {
            for (int y = 0; y < N_YEARS; ++y) {
                double val = global_agg[n * N_YEARS + y];
                if (val != 0.0) {
                    results.push_back({nation_names[n].c_str(), YEAR_MIN + y, val});
                }
            }
        }

        // Sort by nation ASC, o_year DESC
        std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
            int cmp = std::strcmp(a.nation, b.nation);
            if (cmp != 0) return cmp < 0;
            return a.o_year > b.o_year;
        });

        // Write CSV
        std::string out_path = ctx.output_dir + "/q9.csv";
        FILE* fp = std::fopen(out_path.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "[MQO] Cannot open output: %s\n", out_path.c_str());
            std::exit(1);
        }
        std::fprintf(fp, "nation,o_year,sum_profit\n");
        for (const auto& r : results) {
            std::fprintf(fp, "%s,%d,%.2f\n", r.nation, r.o_year, r.sum_profit);
        }
        std::fclose(fp);
    }
}

}  // namespace mqo::tails
