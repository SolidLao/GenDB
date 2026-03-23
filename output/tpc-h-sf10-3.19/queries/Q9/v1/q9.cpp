#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <cmath>
#include <thread>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <omp.h>
#include "timing_utils.h"
#include "cli_params.h"
#include "mmap_utils.h"

using namespace gendb;

// Partsupp hash entry layout
struct PSHashEntry {
    int32_t partkey;
    int32_t suppkey;
    int32_t row_id;
    int32_t padding;
};

// Thread-local aggregation: 25 nations × 10 years (1992-2001 range)
static constexpr int MIN_YEAR = 1992;
static constexpr int MAX_YEAR = 1998;
static constexpr int NUM_YEARS = MAX_YEAR - MIN_YEAR + 1;
static constexpr int NUM_NATIONS = 25;

struct ThreadAgg {
    long double sum_profit[NUM_NATIONS][NUM_YEARS];
};

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir> [--p_name_pattern <pattern>]\n", argv[0]);
        return 1;
    }

    init_date_tables();

    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];
    std::string p_name_pattern = parse_string_arg(argc, argv, "--p_name_pattern", "%green%");

    // Extract the substring between % delimiters for strstr matching
    std::string search_str = p_name_pattern;
    if (!search_str.empty() && search_str.front() == '%') search_str = search_str.substr(1);
    if (!search_str.empty() && search_str.back() == '%') search_str.pop_back();

    GENDB_PHASE("total");

    // ============================================================
    // Phase 1: Load dimension data
    // ============================================================
    std::string nation_names[25];
    int32_t* s_nationkey_arr = nullptr; // indexed by suppkey (1-based, allocate 100001)

    {
        GENDB_PHASE("data_loading");

        // Load nation names
        {
            MmapColumn<int32_t> n_nationkey(gendb_dir + "/nation/n_nationkey.bin");
            MmapColumn<uint32_t> n_name_off(gendb_dir + "/nation/n_name_offsets.bin");
            MmapColumn<char> n_name_data(gendb_dir + "/nation/n_name_data.bin");
            for (size_t i = 0; i < n_nationkey.count; i++) {
                int32_t nk = n_nationkey[i];
                uint32_t start = n_name_off[i];
                uint32_t end = n_name_off[i + 1];
                nation_names[nk] = std::string(n_name_data.data + start, end - start);
            }
        }

        // Load supplier nationkey array
        {
            MmapColumn<int32_t> s_suppkey(gendb_dir + "/supplier/s_suppkey.bin");
            MmapColumn<int32_t> s_nk(gendb_dir + "/supplier/s_nationkey.bin");
            s_nationkey_arr = new int32_t[100001]();
            for (size_t i = 0; i < s_suppkey.count; i++) {
                s_nationkey_arr[s_suppkey[i]] = s_nk[i];
            }
        }
    }

    // ============================================================
    // Phase 2: Filter parts — build bitset of qualifying partkeys
    // ============================================================
    // Max partkey for SF10 is 2000000
    static constexpr size_t BITSET_SIZE = 2000001;
    std::vector<uint8_t> part_bitset(BITSET_SIZE / 8 + 1, 0);

    {
        GENDB_PHASE("dim_filter");

        MmapColumn<uint32_t> p_name_off(gendb_dir + "/part/p_name_offsets.bin");
        MmapColumn<char> p_name_data(gendb_dir + "/part/p_name_data.bin");
        MmapColumn<int32_t> p_partkey(gendb_dir + "/part/p_partkey.bin");

        size_t nparts = p_partkey.count;
        const char* search_cstr = search_str.c_str();
        size_t search_len = search_str.size();

        #pragma omp parallel for schedule(dynamic, 65536)
        for (size_t i = 0; i < nparts; i++) {
            uint32_t start = p_name_off[i];
            uint32_t end = p_name_off[i + 1];
            size_t len = end - start;
            if (len >= search_len && memmem(p_name_data.data + start, len, search_cstr, search_len) != nullptr) {
                int32_t pk = p_partkey[i];
                // Set bit atomically (different bytes won't conflict much)
                __sync_fetch_and_or(&part_bitset[pk >> 3], (uint8_t)(1 << (pk & 7)));
            }
        }
    }

    // ============================================================
    // Phase 3: mmap indexes and lineitem columns, then scan
    // ============================================================

    // mmap partsupp hash index
    MmapColumn<char> ps_hash_raw(gendb_dir + "/indexes/partsupp_pk_hash.bin");
    ps_hash_raw.advise_random();
    uint64_t ps_capacity = *reinterpret_cast<const uint64_t*>(ps_hash_raw.data);
    uint64_t ps_mask = ps_capacity - 1;
    const PSHashEntry* ps_table = reinterpret_cast<const PSHashEntry*>(ps_hash_raw.data + 8);

    // mmap ps_supplycost
    MmapColumn<double> ps_supplycost(gendb_dir + "/partsupp/ps_supplycost.bin");
    ps_supplycost.advise_random();

    // mmap orders lookup index
    MmapColumn<char> ord_lookup_raw(gendb_dir + "/indexes/orders_o_orderkey_lookup.bin");
    ord_lookup_raw.advise_random();
    uint64_t ord_num_entries = *reinterpret_cast<const uint64_t*>(ord_lookup_raw.data);
    const int32_t* order_lookup = reinterpret_cast<const int32_t*>(ord_lookup_raw.data + 8);
    (void)ord_num_entries;

    // mmap o_orderdate
    MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
    o_orderdate.advise_random();

    // mmap lineitem columns
    MmapColumn<int32_t> l_partkey(gendb_dir + "/lineitem/l_partkey.bin");
    MmapColumn<int32_t> l_suppkey(gendb_dir + "/lineitem/l_suppkey.bin");
    MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
    MmapColumn<double> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
    MmapColumn<double> l_discount(gendb_dir + "/lineitem/l_discount.bin");
    MmapColumn<double> l_quantity(gendb_dir + "/lineitem/l_quantity.bin");

    size_t num_lineitem = l_partkey.count;

    // Prefetch lineitem columns
    l_partkey.prefetch();
    l_suppkey.prefetch();
    l_orderkey.prefetch();
    l_extendedprice.prefetch();
    l_discount.prefetch();
    l_quantity.prefetch();

    // Determine thread count
    int num_threads = std::thread::hardware_concurrency();
    if (num_threads <= 0) num_threads = 8;

    // Allocate thread-local aggregation
    std::vector<ThreadAgg> thread_aggs(num_threads);
    for (auto& ta : thread_aggs) {
        memset(ta.sum_profit, 0, sizeof(ta.sum_profit));
    }

    {
        GENDB_PHASE("main_scan");

        const uint8_t* bitset_data = part_bitset.data();

        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            long double (&local_agg)[NUM_NATIONS][NUM_YEARS] = thread_aggs[tid].sum_profit;

            #pragma omp for schedule(dynamic, 65536)
            for (size_t i = 0; i < num_lineitem; i++) {
                int32_t pk = l_partkey[i];

                // 1. Check part bitset
                if (!(bitset_data[pk >> 3] & (1 << (pk & 7)))) continue;

                int32_t sk = l_suppkey[i];

                // 2. Get nation index from supplier
                int32_t nation_idx = s_nationkey_arr[sk];

                // 3. Partsupp hash lookup for ps_supplycost
                uint64_t h = ((uint64_t)(uint32_t)pk * 2654435761ULL) ^ ((uint64_t)(uint32_t)sk * 40499ULL);
                uint64_t slot = h & ps_mask;
                double supplycost = 0.0;
                while (ps_table[slot].partkey != -1) {
                    if (ps_table[slot].partkey == pk && ps_table[slot].suppkey == sk) {
                        supplycost = ps_supplycost[ps_table[slot].row_id];
                        break;
                    }
                    slot = (slot + 1) & ps_mask;
                }

                // 4. Orders lookup for o_orderdate → year
                int32_t ok = l_orderkey[i];
                int32_t o_row = order_lookup[ok];
                int32_t odate = o_orderdate[o_row];
                int year = YEAR_TABLE[odate];
                int year_idx = year - MIN_YEAR;

                // 5. Compute amount
                double amount = l_extendedprice[i] * (1.0 - l_discount[i]) - supplycost * l_quantity[i];

                // 6. Aggregate
                if (year_idx >= 0 && year_idx < NUM_YEARS) {
                    local_agg[nation_idx][year_idx] += amount;
                }
            }
        }
    }

    // ============================================================
    // Phase 4: Merge and output
    // ============================================================
    {
        GENDB_PHASE("output");

        // Merge thread-local aggregations
        long double final_agg[NUM_NATIONS][NUM_YEARS];
        memset(final_agg, 0, sizeof(final_agg));
        for (int t = 0; t < num_threads; t++) {
            for (int n = 0; n < NUM_NATIONS; n++) {
                for (int y = 0; y < NUM_YEARS; y++) {
                    final_agg[n][y] += thread_aggs[t].sum_profit[n][y];
                }
            }
        }

        // Collect non-zero results
        struct Result {
            const char* nation;
            int year;
            double sum_profit;
            int nation_idx;
        };
        std::vector<Result> results;
        results.reserve(NUM_NATIONS * NUM_YEARS);
        for (int n = 0; n < NUM_NATIONS; n++) {
            for (int y = 0; y < NUM_YEARS; y++) {
                if (final_agg[n][y] != 0.0L) {
                    results.push_back({nation_names[n].c_str(), MIN_YEAR + y, (double)final_agg[n][y], n});
                }
            }
        }

        // Sort by nation ASC, o_year DESC
        std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
            int cmp = strcmp(a.nation, b.nation);
            if (cmp != 0) return cmp < 0;
            return a.year > b.year;
        });

        // Write CSV output
        std::string outpath = results_dir + "/Q9.csv";
        FILE* fp = fopen(outpath.c_str(), "w");
        if (!fp) {
            fprintf(stderr, "Cannot open output file: %s\n", outpath.c_str());
            return 1;
        }
        fprintf(fp, "nation,o_year,sum_profit\n");
        for (const auto& r : results) {
            fprintf(fp, "%s,%d,%.2f\n", r.nation, r.year, round(r.sum_profit * 100.0) / 100.0);
        }
        fclose(fp);
    }

    delete[] s_nationkey_arr;
    return 0;
}
