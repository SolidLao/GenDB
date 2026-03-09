// Q9: Product Type Profit Measure — GenDB iteration 0
// SELECT nation, o_year, SUM(amount) AS sum_profit
// 6-way join: part(filtered) → lineitem → partsupp, orders, supplier, nation
// Strategy: bitset part filter, morsel-driven parallel lineitem scan, direct array aggregation

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <atomic>
#include <thread>
#include <omp.h>
#include "timing_utils.h"
#include "mmap_utils.h"

using namespace gendb;

static inline int year_from_days(int z) {
    z += 719468;
    int era = (z >= 0 ? z : z - 146096) / 146097;
    int doe = z - era * 146097;
    int yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int y = yoe + era * 400;
    int doy = doe - (365*yoe + yoe/4 - yoe/100);
    int mp = (5*doy + 2) / 153;
    int m = mp + (mp < 10 ? 3 : -9);
    return y + (m <= 2);
}

struct PSEntry { uint32_t start; uint32_t count; };

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb(argv[1]);
    std::string results(argv[2]);

    GENDB_PHASE_MS("total", total_ms);
    double output_ms = 0;

    // =========================================================================
    // Phase 1: Load nation names
    // =========================================================================
    std::string nation_names[25];
    {
        GENDB_PHASE("load_nation");
        MmapColumn<int32_t> n_nationkey(gendb + "/nation/n_nationkey.bin");
        MmapColumn<int64_t> n_name_off(gendb + "/nation/n_name_offsets.bin");
        MmapColumn<char> n_name_data(gendb + "/nation/n_name_data.bin");
        for (size_t i = 0; i < 25; i++) {
            int nk = n_nationkey[i];
            int64_t s = n_name_off[i], e = n_name_off[i+1];
            nation_names[nk] = std::string(n_name_data.data + s, e - s);
        }
    }

    // =========================================================================
    // Phase 2: Load supplier → nationkey mapping (direct array, suppkey 1-based)
    // =========================================================================
    std::vector<int32_t> supp_nationkey; // indexed by suppkey
    {
        GENDB_PHASE("load_supplier");
        MmapColumn<int32_t> s_nationkey(gendb + "/supplier/s_nationkey.bin");
        // suppkeys are 1..100000, contiguous, so s_nationkey[suppkey-1] works
        // Build array indexed by suppkey (0-based entry unused)
        size_t n_supp = s_nationkey.count;
        supp_nationkey.resize(n_supp + 1);
        supp_nationkey[0] = 0;
        for (size_t i = 0; i < n_supp; i++) {
            supp_nationkey[i + 1] = s_nationkey[i];
        }
    }

    // =========================================================================
    // Phase 3: Filter parts — build bitset of qualifying partkeys
    // =========================================================================
    std::vector<uint64_t> part_bitset;
    uint32_t max_partkey = 0;
    {
        GENDB_PHASE("filter_part");
        MmapColumn<int32_t> p_partkey(gendb + "/part/p_partkey.bin");
        MmapColumn<int64_t> p_name_off(gendb + "/part/p_name_offsets.bin");
        MmapColumn<char> p_name_data(gendb + "/part/p_name_data.bin");
        size_t n_parts = p_partkey.count;

        // Find max partkey for bitset sizing
        for (size_t i = 0; i < n_parts; i++) {
            uint32_t pk = (uint32_t)p_partkey[i];
            if (pk > max_partkey) max_partkey = pk;
        }
        part_bitset.resize((max_partkey / 64) + 1, 0);

        const char* needle = "green";
        size_t needle_len = 5;
        for (size_t i = 0; i < n_parts; i++) {
            int64_t s = p_name_off[i], e = p_name_off[i+1];
            if (memmem(p_name_data.data + s, e - s, needle, needle_len) != nullptr) {
                uint32_t pk = (uint32_t)p_partkey[i];
                part_bitset[pk >> 6] |= (1ULL << (pk & 63));
            }
        }
    }

    // =========================================================================
    // Phase 4: Load indexes
    // =========================================================================
    // partsupp_pk_index
    MmapColumn<char> ps_index_raw(gendb + "/indexes/partsupp_pk_index.bin");
    uint32_t ps_max_partkey = *(const uint32_t*)ps_index_raw.data;
    const PSEntry* ps_index = (const PSEntry*)(ps_index_raw.data + 4);

    // partsupp columns
    MmapColumn<int32_t> ps_suppkey(gendb + "/partsupp/ps_suppkey.bin");
    MmapColumn<double> ps_supplycost(gendb + "/partsupp/ps_supplycost.bin");

    // orders_orderkey_lookup
    MmapColumn<char> ord_lookup_raw(gendb + "/indexes/orders_orderkey_lookup.bin");
    uint32_t ord_max_key = *(const uint32_t*)ord_lookup_raw.data;
    const int32_t* ord_lookup = (const int32_t*)(ord_lookup_raw.data + 4);

    // orders orderdate
    MmapColumn<int32_t> o_orderdate(gendb + "/orders/o_orderdate.bin");

    // =========================================================================
    // Phase 5: Parallel lineitem scan + join + aggregate
    // =========================================================================
    // mmap lineitem columns
    MmapColumn<int32_t> l_partkey(gendb + "/lineitem/l_partkey.bin");
    MmapColumn<int32_t> l_suppkey(gendb + "/lineitem/l_suppkey.bin");
    MmapColumn<int32_t> l_orderkey(gendb + "/lineitem/l_orderkey.bin");
    MmapColumn<double> l_extendedprice(gendb + "/lineitem/l_extendedprice.bin");
    MmapColumn<double> l_discount(gendb + "/lineitem/l_discount.bin");
    MmapColumn<double> l_quantity(gendb + "/lineitem/l_quantity.bin");

    // Advise random for conditionally-accessed columns
    l_suppkey.advise_random();
    l_orderkey.advise_random();
    l_extendedprice.advise_random();
    l_discount.advise_random();
    l_quantity.advise_random();
    ps_suppkey.advise_random();
    ps_supplycost.advise_random();
    o_orderdate.advise_random();

    size_t n_lineitem = l_partkey.count;
    int n_threads = omp_get_max_threads();

    // Thread-local aggregation using long double for precision
    // sum_profit[thread][nation][year_offset], year_offset = year - 1992, range 0..7
    std::vector<long double> tl_agg(n_threads * 25 * 8, 0.0L);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            long double* my_agg = tl_agg.data() + tid * 25 * 8;

            #pragma omp for schedule(dynamic, 100000)
            for (size_t i = 0; i < n_lineitem; i++) {
                uint32_t pk = (uint32_t)l_partkey[i];
                // Bitset probe
                if (pk > max_partkey) continue;
                if (!(part_bitset[pk >> 6] & (1ULL << (pk & 63)))) continue;

                // Qualifying row — load other columns
                int32_t sk = l_suppkey[i];
                int32_t ok = l_orderkey[i];

                // partsupp lookup: find ps_supplycost for (pk, sk)
                double supplycost = 0.0;
                if (pk <= ps_max_partkey) {
                    auto& entry = ps_index[pk];
                    uint32_t ps_start = entry.start;
                    uint32_t ps_cnt = entry.count;
                    for (uint32_t j = ps_start; j < ps_start + ps_cnt; j++) {
                        if (ps_suppkey[j] == sk) {
                            supplycost = ps_supplycost[j];
                            break;
                        }
                    }
                }

                // orders lookup: orderkey → row → orderdate → year
                int32_t ord_row = ord_lookup[(uint32_t)ok];
                int year = year_from_days(o_orderdate[ord_row]);
                int year_off = year - 1992;

                // supplier → nation
                int32_t nationkey = supp_nationkey[sk];

                // Compute amount
                double amount = l_extendedprice[i] * (1.0 - l_discount[i]) - supplycost * l_quantity[i];

                // Aggregate
                my_agg[nationkey * 8 + year_off] += amount;
            }
        }
    }

    // =========================================================================
    // Phase 6: Merge thread-local aggregations and output
    // =========================================================================
    {
        GENDB_PHASE_MS("output", output_ms);

        // Merge
        long double global_agg[25][8] = {};
        for (int t = 0; t < n_threads; t++) {
            long double* ta = tl_agg.data() + t * 25 * 8;
            for (int n = 0; n < 25; n++) {
                for (int y = 0; y < 8; y++) {
                    global_agg[n][y] += ta[n * 8 + y];
                }
            }
        }

        // Collect non-zero results
        struct Result { std::string nation; int year; double sum_profit; };
        std::vector<Result> out;
        out.reserve(175);
        for (int n = 0; n < 25; n++) {
            for (int y = 0; y < 8; y++) {
                if (global_agg[n][y] != 0.0L) {
                    out.push_back({nation_names[n], 1992 + y, (double)global_agg[n][y]});
                }
            }
        }

        // Sort: nation ASC, o_year DESC
        std::sort(out.begin(), out.end(), [](const Result& a, const Result& b) {
            if (a.nation != b.nation) return a.nation < b.nation;
            return a.year > b.year;
        });

        // Write CSV
        std::string outpath = results + "/Q9.csv";
        FILE* fp = fopen(outpath.c_str(), "w");
        fprintf(fp, "nation,o_year,sum_profit\n");
        for (auto& r : out) {
            fprintf(fp, "%s,%d,%.2f\n", r.nation.c_str(), r.year, r.sum_profit);
        }
        fclose(fp);
    }

    return 0;
}
