// Q9: Product Type Profit Measure — GenDB iteration 2
// Key optimizations vs iter_1:
//   1. mmap ALL files upfront + prefetch lineitem columns for I/O overlap
//   2. Pre-build year_for_orderkey[] uint8_t array — eliminates 240MB orders_orderkey_lookup,
//      57MB o_orderdate random access, and year_from_days() from hot loop
//   3. Pre-build partsupp_compact[] — one 48-byte struct per partkey lookup
//   4. schedule(static) for filter_part — eliminates OpenMP dynamic overhead
//   5. No advise_random() calls — all pre-builds are sequential scans
//   6. Dense uint8_t nationkey_for_supplier[] array

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

// Compact partsupp: 4 suppkeys + 4 supplycosts per partkey = 48 bytes
struct PSCompact {
    int32_t suppkey[4];
    double supplycost[4];
};

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb(argv[1]);
    std::string results(argv[2]);

    GENDB_PHASE_MS("total", total_ms);
    double output_ms = 0;

    // =========================================================================
    // Phase 0: mmap ALL files upfront
    // =========================================================================
    // Nation
    MmapColumn<int32_t> n_nationkey(gendb + "/nation/n_nationkey.bin");
    MmapColumn<int64_t> n_name_off(gendb + "/nation/n_name_offsets.bin");
    MmapColumn<char> n_name_data(gendb + "/nation/n_name_data.bin");

    // Supplier
    MmapColumn<int32_t> s_nationkey(gendb + "/supplier/s_nationkey.bin");

    // Part (for filter)
    MmapColumn<int64_t> p_name_off(gendb + "/part/p_name_offsets.bin");
    MmapColumn<char> p_name_data(gendb + "/part/p_name_data.bin");

    // Orders (for year pre-build)
    MmapColumn<int32_t> o_orderkey(gendb + "/orders/o_orderkey.bin");
    MmapColumn<int32_t> o_orderdate(gendb + "/orders/o_orderdate.bin");

    // Partsupp (for compact pre-build)
    MmapColumn<int32_t> ps_partkey_col(gendb + "/partsupp/ps_partkey.bin");
    MmapColumn<int32_t> ps_suppkey_col(gendb + "/partsupp/ps_suppkey.bin");
    MmapColumn<double> ps_supplycost_col(gendb + "/partsupp/ps_supplycost.bin");

    // Partsupp index
    MmapColumn<char> ps_index_raw(gendb + "/indexes/partsupp_pk_index.bin");
    uint32_t ps_max_partkey = *(const uint32_t*)ps_index_raw.data;
    const PSEntry* ps_index = (const PSEntry*)(ps_index_raw.data + 4);

    // Lineitem columns
    MmapColumn<int32_t> l_partkey(gendb + "/lineitem/l_partkey.bin");
    MmapColumn<int32_t> l_suppkey(gendb + "/lineitem/l_suppkey.bin");
    MmapColumn<int32_t> l_orderkey(gendb + "/lineitem/l_orderkey.bin");
    MmapColumn<double> l_extendedprice(gendb + "/lineitem/l_extendedprice.bin");
    MmapColumn<double> l_discount(gendb + "/lineitem/l_discount.bin");
    MmapColumn<double> l_quantity(gendb + "/lineitem/l_quantity.bin");

    // Prefetch lineitem columns so OS starts readahead while we do pre-builds
    mmap_prefetch_all(l_partkey, l_suppkey, l_orderkey,
                      l_extendedprice, l_discount, l_quantity);

    size_t n_parts = p_name_off.count - 1;
    size_t n_lineitem = l_partkey.count;
    size_t n_orders = o_orderkey.count;
    size_t n_supplier = s_nationkey.count;

    // =========================================================================
    // Phase 1: Load nation names (25 rows — trivial)
    // =========================================================================
    std::string nation_names[25];
    {
        GENDB_PHASE("load_nation");
        for (size_t i = 0; i < 25; i++) {
            int nk = n_nationkey[i];
            int64_t s = n_name_off[i], e = n_name_off[i+1];
            nation_names[nk] = std::string(n_name_data.data + s, e - s);
        }
    }

    // =========================================================================
    // Phase 2: Build supplier nationkey mapping
    // Dense uint8_t array indexed by suppkey (1-based, so [suppkey] = nationkey)
    // =========================================================================
    std::vector<uint8_t> nationkey_for_supplier(n_supplier + 1, 0);
    {
        GENDB_PHASE("build_supplier");
        for (size_t i = 0; i < n_supplier; i++) {
            // suppkey is 1-based contiguous, so suppkey = i+1
            nationkey_for_supplier[i + 1] = (uint8_t)s_nationkey[i];
        }
    }

    // =========================================================================
    // Phase 3: Filter parts — PARALLEL bitset build with schedule(static)
    // =========================================================================
    size_t bitset_words = (n_parts + 63) / 64;
    std::vector<uint64_t> part_bitset(bitset_words, 0);

    {
        GENDB_PHASE("filter_part");
        const int64_t* offsets = p_name_off.data;
        const char* pdata = p_name_data.data;

        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n_parts; i++) {
            size_t len = (size_t)(offsets[i+1] - offsets[i]);
            if (memmem(pdata + offsets[i], len, "green", 5) != nullptr) {
                size_t word = i >> 6;
                uint64_t bit = 1ULL << (i & 63);
                __atomic_or_fetch(&part_bitset[word], bit, __ATOMIC_RELAXED);
            }
        }
    }

    // =========================================================================
    // Phase 4: Pre-build year_for_orderkey[] dense array
    // Scan orders sequentially: year_for_orderkey[orderkey] = year - 1992
    // =========================================================================
    // Orders are sorted by o_orderkey, so max is last element
    uint32_t max_orderkey = (uint32_t)o_orderkey[n_orders - 1];
    std::vector<uint8_t> year_for_orderkey(max_orderkey + 1, 0);

    {
        GENDB_PHASE("prebuild_orders_year");
        const int32_t* ok_data = o_orderkey.data;
        const int32_t* od_data = o_orderdate.data;
        uint8_t* yfo = year_for_orderkey.data();

        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n_orders; i++) {
            uint32_t key = (uint32_t)ok_data[i];
            int year = year_from_days(od_data[i]);
            yfo[key] = (uint8_t)(year - 1992);
        }
    }

    // =========================================================================
    // Phase 5: Pre-build partsupp_compact[] array
    // 48 bytes per partkey: 4 suppkeys + 4 supplycosts in one struct
    // =========================================================================
    std::vector<PSCompact> partsupp_compact(ps_max_partkey + 1);
    memset(partsupp_compact.data(), 0, partsupp_compact.size() * sizeof(PSCompact));

    {
        GENDB_PHASE("prebuild_partsupp_compact");
        const int32_t* psk = ps_suppkey_col.data;
        const double* psc = ps_supplycost_col.data;

        #pragma omp parallel for schedule(static)
        for (size_t pk = 1; pk <= ps_max_partkey; pk++) {
            const PSEntry& entry = ps_index[pk];
            uint32_t start = entry.start;
            uint32_t cnt = entry.count;
            PSCompact& c = partsupp_compact[pk];
            uint32_t n = cnt < 4 ? cnt : 4;
            for (uint32_t j = 0; j < n; j++) {
                c.suppkey[j] = psk[start + j];
                c.supplycost[j] = psc[start + j];
            }
        }
    }

    // =========================================================================
    // Phase 6: Parallel lineitem scan + join + aggregate
    // =========================================================================
    int n_threads = omp_get_max_threads();
    // Use long double for accumulation precision (80-bit on x86)
    std::vector<long double> tl_agg(n_threads * 25 * 8, 0.0L);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            long double* my_agg = tl_agg.data() + tid * 25 * 8;

            const uint64_t* bs = part_bitset.data();
            const int32_t* lpk = l_partkey.data;
            const int32_t* lsk = l_suppkey.data;
            const int32_t* lok = l_orderkey.data;
            const double* lep = l_extendedprice.data;
            const double* ld = l_discount.data;
            const double* lq = l_quantity.data;
            const uint8_t* yfo = year_for_orderkey.data();
            const uint8_t* nfs = nationkey_for_supplier.data();
            const PSCompact* psc = partsupp_compact.data();

            #pragma omp for schedule(dynamic, 100000)
            for (size_t i = 0; i < n_lineitem; i++) {
                uint32_t pk = (uint32_t)lpk[i];
                // Bitset probe: partkey is 1-based, index by pk-1
                uint32_t idx = pk - 1;
                if (!(bs[idx >> 6] & (1ULL << (idx & 63)))) continue;

                int32_t sk = lsk[i];
                uint32_t ok = (uint32_t)lok[i];

                // Partsupp compact lookup: find supplycost for (pk, sk)
                const PSCompact& ps = psc[pk];
                double supplycost = 0.0;
                // Unrolled linear search over 4 entries
                if (ps.suppkey[0] == sk) supplycost = ps.supplycost[0];
                else if (ps.suppkey[1] == sk) supplycost = ps.supplycost[1];
                else if (ps.suppkey[2] == sk) supplycost = ps.supplycost[2];
                else if (ps.suppkey[3] == sk) supplycost = ps.supplycost[3];

                // Year from pre-built array (no year_from_days in hot loop)
                int year_off = (int)yfo[ok];

                // Nationkey from dense supplier array
                uint8_t nk = nfs[sk];

                // amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
                double amount = lep[i] * (1.0 - ld[i]) - supplycost * lq[i];

                my_agg[nk * 8 + year_off] += (long double)amount;
            }
        }
    }

    // =========================================================================
    // Phase 7: Merge and output
    // =========================================================================
    {
        GENDB_PHASE_MS("output", output_ms);

        // Merge thread-local arrays using long double
        long double global_agg[25 * 8] = {};
        for (int t = 0; t < n_threads; t++) {
            const long double* ta = tl_agg.data() + t * 25 * 8;
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
                long double v = global_agg[n * 8 + y];
                if (v != 0.0L) {
                    out.push_back({n, 1992 + y, (double)v});
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
            fprintf(fp, "%s,%d,%.2f\n", nation_names[r.nation_idx].c_str(), r.year, r.sum_profit);
        }
        fclose(fp);
    }

    return 0;
}
