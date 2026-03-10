// Q9: Product Type Profit Measure — GenDB iteration 1
// Optimizations vs iter_0:
//   1. Parallel part filter with OpenMP (was single-threaded, 81ms → ~2ms)
//   2. No max_partkey scan (partkeys are contiguous 1..n_parts)
//   3. MADV_SEQUENTIAL for lineitem columns (not MADV_RANDOM)
//   4. Thread-local double[25][8] aggregation

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <omp.h>
#include "timing_utils.h"
#include "mmap_utils.h"

using namespace gendb;

static inline int year_from_days(int z) {
    z += 719468;
    int era = (z >= 0 ? z : z - 146097) / 146097;
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
    // Phase 1: Load nation names (25 rows — trivial)
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
    // Phase 2: Supplier nationkey mapping (suppkey is 1-based contiguous)
    // =========================================================================
    MmapColumn<int32_t> s_nationkey(gendb + "/supplier/s_nationkey.bin");
    // Access: s_nationkey[suppkey - 1] gives nationkey

    // =========================================================================
    // Phase 3: Filter parts — PARALLEL bitset build
    // partkeys are contiguous 1..n_parts, no need to scan for max_partkey
    // =========================================================================
    MmapColumn<int64_t> p_name_off(gendb + "/part/p_name_offsets.bin");
    MmapColumn<char> p_name_data(gendb + "/part/p_name_data.bin");
    size_t n_parts = p_name_off.count - 1;  // 2M

    // Bitset indexed by partkey-1 (0-based), so n_parts bits needed
    size_t bitset_words = (n_parts + 63) / 64;
    std::vector<uint64_t> part_bitset(bitset_words, 0);

    {
        GENDB_PHASE("filter_part");
        const int64_t* offsets = p_name_off.data;
        const char* pdata = p_name_data.data;

        #pragma omp parallel for schedule(dynamic, 10000)
        for (size_t i = 0; i < n_parts; i++) {
            size_t len = (size_t)(offsets[i+1] - offsets[i]);
            if (memmem(pdata + offsets[i], len, "green", 5) != nullptr) {
                // partkey = i+1, bitset index = i
                size_t word = i >> 6;
                uint64_t bit = 1ULL << (i & 63);
                __atomic_or_fetch(&part_bitset[word], bit, __ATOMIC_RELAXED);
            }
        }
    }

    // =========================================================================
    // Phase 4: Load indexes and dimension columns
    // =========================================================================
    // partsupp_pk_index: dense_range index
    MmapColumn<char> ps_index_raw(gendb + "/indexes/partsupp_pk_index.bin");
    const PSEntry* ps_index = (const PSEntry*)(ps_index_raw.data + 4);

    // partsupp columns (random access)
    MmapColumn<int32_t> ps_suppkey(gendb + "/partsupp/ps_suppkey.bin");
    MmapColumn<double> ps_supplycost(gendb + "/partsupp/ps_supplycost.bin");
    ps_suppkey.advise_random();
    ps_supplycost.advise_random();

    // orders_orderkey_lookup: dense lookup
    MmapColumn<char> ord_lookup_raw(gendb + "/indexes/orders_orderkey_lookup.bin");
    const int32_t* ord_lookup = (const int32_t*)(ord_lookup_raw.data + 4);

    // orders orderdate (random access by row index)
    MmapColumn<int32_t> o_orderdate(gendb + "/orders/o_orderdate.bin");
    o_orderdate.advise_random();

    // =========================================================================
    // Phase 5: Parallel lineitem scan + join + aggregate
    // MADV_SEQUENTIAL for all lineitem columns (sequential scan with ~5% density)
    // =========================================================================
    MmapColumn<int32_t> l_partkey(gendb + "/lineitem/l_partkey.bin");
    MmapColumn<int32_t> l_suppkey(gendb + "/lineitem/l_suppkey.bin");
    MmapColumn<int32_t> l_orderkey(gendb + "/lineitem/l_orderkey.bin");
    MmapColumn<double> l_extendedprice(gendb + "/lineitem/l_extendedprice.bin");
    MmapColumn<double> l_discount(gendb + "/lineitem/l_discount.bin");
    MmapColumn<double> l_quantity(gendb + "/lineitem/l_quantity.bin");

    // Sequential advice (default from MmapColumn, but be explicit)
    l_partkey.advise_sequential();
    l_suppkey.advise_sequential();
    l_orderkey.advise_sequential();
    l_extendedprice.advise_sequential();
    l_discount.advise_sequential();
    l_quantity.advise_sequential();

    size_t n_lineitem = l_partkey.count;
    int n_threads = omp_get_max_threads();

    // Thread-local aggregation: double[25][8]
    // year_offset = year - 1992, range 0..7
    std::vector<double> tl_agg(n_threads * 25 * 8, 0.0);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            double* my_agg = tl_agg.data() + tid * 25 * 8;

            const uint64_t* bs = part_bitset.data();
            const int32_t* lpk = l_partkey.data;
            const int32_t* lsk = l_suppkey.data;
            const int32_t* lok = l_orderkey.data;
            const double* lep = l_extendedprice.data;
            const double* ld = l_discount.data;
            const double* lq = l_quantity.data;
            const int32_t* snk = s_nationkey.data;
            const int32_t* odate = o_orderdate.data;
            const int32_t* psskey = ps_suppkey.data;
            const double* pssc = ps_supplycost.data;

            #pragma omp for schedule(dynamic, 100000)
            for (size_t i = 0; i < n_lineitem; i++) {
                uint32_t pk = (uint32_t)lpk[i];
                // Bitset probe: pk is 1-based, index by pk-1
                uint32_t idx = pk - 1;
                if (!(bs[idx >> 6] & (1ULL << (idx & 63)))) continue;

                int32_t sk = lsk[i];
                int32_t ok = lok[i];

                // partsupp lookup: find ps_supplycost for (pk, sk)
                const PSEntry& entry = ps_index[pk];
                uint32_t ps_start = entry.start;
                uint32_t ps_end = ps_start + entry.count;
                double supplycost = 0.0;
                for (uint32_t j = ps_start; j < ps_end; j++) {
                    if (psskey[j] == sk) { supplycost = pssc[j]; break; }
                }

                // orders lookup: orderkey → row → orderdate → year
                int32_t ord_row = ord_lookup[(uint32_t)ok];
                int year = year_from_days(odate[ord_row]);
                int year_off = year - 1992;

                // supplier → nationkey (suppkey 1-based)
                int32_t nk = snk[sk - 1];

                // amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
                double amount = lep[i] * (1.0 - ld[i]) - supplycost * lq[i];

                my_agg[nk * 8 + year_off] += amount;
            }
        }
    }

    // =========================================================================
    // Phase 6: Merge and output
    // =========================================================================
    {
        GENDB_PHASE_MS("output", output_ms);

        // Merge thread-local arrays
        double global_agg[25 * 8] = {};
        for (int t = 0; t < n_threads; t++) {
            const double* ta = tl_agg.data() + t * 25 * 8;
            for (int j = 0; j < 25 * 8; j++) {
                global_agg[j] += ta[j];
            }
        }

        // Collect non-zero results
        struct Result { int nation_idx; int year; double sum_profit; };
        std::vector<Result> out;
        out.reserve(175);
        for (int n = 0; n < 25; n++) {
            for (int y = 0; y < 8; y++) {
                double v = global_agg[n * 8 + y];
                if (v != 0.0) {
                    out.push_back({n, 1992 + y, v});
                }
            }
        }

        // Sort: nation ASC, o_year DESC
        std::sort(out.begin(), out.end(), [&](const Result& a, const Result& b) {
            int cmp = nation_names[a.nation_idx].compare(nation_names[b.nation_idx]);
            if (cmp != 0) return cmp < 0;
            return a.year > b.year;
        });

        // Write CSV
        std::string outpath = results + "/Q9.csv";
        FILE* fp = fopen(outpath.c_str(), "w");
        if (!fp) { perror("fopen"); return 1; }
        fprintf(fp, "nation,o_year,sum_profit\n");
        for (auto& r : out) {
            fprintf(fp, "%s,%d,%.4f\n", nation_names[r.nation_idx].c_str(), r.year, r.sum_profit);
        }
        fclose(fp);
    }

    return 0;
}
