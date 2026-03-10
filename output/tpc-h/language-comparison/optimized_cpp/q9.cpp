// Q9: Product Type Profit Measure — Optimized C++ iter3
// Key improvements:
//   1. No prefault phase (hot runs have pages in cache)
//   2. Software prefetching in main_scan for random access columns
//   3. Optimized filter_part: use strchr first byte then check rest
//   4. Thread-local agg padded to 256 for cache line alignment
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

struct RawMap {
    const char* data; size_t size;
    static RawMap open(const char* path) {
        int fd = ::open(path, O_RDONLY);
        struct stat st; fstat(fd, &st);
        void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        return {(const char*)p, (size_t)st.st_size};
    }
};

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

struct PSEntry { uint32_t start, count; };

int main(int argc, char** argv) {
    if (argc < 3) return 1;
    char gendb_dir[4096], results_dir[4096];
    snprintf(gendb_dir, sizeof(gendb_dir), "%s", argv[1]);
    snprintf(results_dir, sizeof(results_dir), "%s", argv[2]);

    GENDB_PHASE_MS("total", total_ms);

    char path[4096];

    // mmap ALL files
    snprintf(path, sizeof(path), "%s/nation/n_nationkey.bin", gendb_dir);
    auto m_n_nationkey = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/nation/n_name_offsets.bin", gendb_dir);
    auto m_n_name_off = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/nation/n_name_data.bin", gendb_dir);
    auto m_n_name_data = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/supplier/s_nationkey.bin", gendb_dir);
    auto m_s_nationkey = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/part/p_name_offsets.bin", gendb_dir);
    auto m_p_name_off = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/part/p_name_data.bin", gendb_dir);
    auto m_p_name_data = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/orders/o_orderdate.bin", gendb_dir);
    auto m_o_orderdate = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/indexes/orders_orderkey_lookup.bin", gendb_dir);
    auto m_orders_lookup = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/partsupp/ps_suppkey.bin", gendb_dir);
    auto m_ps_suppkey = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/partsupp/ps_supplycost.bin", gendb_dir);
    auto m_ps_supplycost = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/indexes/partsupp_pk_index.bin", gendb_dir);
    auto m_ps_index = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_partkey.bin", gendb_dir);
    auto m_l_partkey = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_suppkey.bin", gendb_dir);
    auto m_l_suppkey = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_orderkey.bin", gendb_dir);
    auto m_l_orderkey = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_extendedprice.bin", gendb_dir);
    auto m_l_extendedprice = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_discount.bin", gendb_dir);
    auto m_l_discount = RawMap::open(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_quantity.bin", gendb_dir);
    auto m_l_quantity = RawMap::open(path);

    const int32_t* n_nationkey = (const int32_t*)m_n_nationkey.data;
    const int64_t* n_name_off = (const int64_t*)m_n_name_off.data;
    const char* n_name_data = m_n_name_data.data;
    const int32_t* s_nationkey = (const int32_t*)m_s_nationkey.data;
    const int64_t* p_name_off_data = (const int64_t*)m_p_name_off.data;
    const char* p_name_data = m_p_name_data.data;
    const int32_t* o_orderdate = (const int32_t*)m_o_orderdate.data;
    const int32_t* orders_lookup = (const int32_t*)(m_orders_lookup.data + 4);
    const int32_t* ps_suppkey = (const int32_t*)m_ps_suppkey.data;
    const double* ps_supplycost = (const double*)m_ps_supplycost.data;
    const PSEntry* ps_index = (const PSEntry*)(m_ps_index.data + 4);
    const int32_t* l_partkey = (const int32_t*)m_l_partkey.data;
    const int32_t* l_suppkey = (const int32_t*)m_l_suppkey.data;
    const int32_t* l_orderkey = (const int32_t*)m_l_orderkey.data;
    const double* l_extendedprice = (const double*)m_l_extendedprice.data;
    const double* l_discount = (const double*)m_l_discount.data;
    const double* l_quantity = (const double*)m_l_quantity.data;

    size_t n_parts = m_p_name_off.size / sizeof(int64_t) - 1;
    size_t n_lineitem = m_l_partkey.size / sizeof(int32_t);

    // Load nation names (25 rows)
    std::string nation_names[25];
    {
        GENDB_PHASE("load_nation");
        for (size_t i = 0; i < 25; i++) {
            int nk = n_nationkey[i];
            int64_t s = n_name_off[i], e = n_name_off[i+1];
            nation_names[nk] = std::string(n_name_data + s, e - s);
        }
    }

    // Filter parts — parallel bitset with case-insensitive "green" search
    size_t bitset_words = (n_parts + 63) / 64;
    std::vector<uint64_t> part_bitset(bitset_words, 0);
    {
        GENDB_PHASE("filter_part");
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n_parts; i++) {
            const char* name = p_name_data + p_name_off_data[i];
            size_t len = (size_t)(p_name_off_data[i+1] - p_name_off_data[i]);
            // Fast path: check if 'g' exists first (avoids full memmem for non-matching)
            if (len >= 5 && memchr(name, 'g', len) != nullptr) {
                if (memmem(name, len, "green", 5) != nullptr) {
                    __atomic_or_fetch(&part_bitset[i >> 6], 1ULL << (i & 63), __ATOMIC_RELAXED);
                }
            }
        }
    }

    // Parallel lineitem scan with software prefetching
    int n_threads = omp_get_max_threads();
    std::vector<double> tl_agg(n_threads * 256, 0.0);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            double* my_agg = tl_agg.data() + tid * 256;
            const uint64_t* bs = part_bitset.data();

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_lineitem; i++) {
                uint32_t pk = (uint32_t)l_partkey[i];
                uint32_t idx = pk - 1;
                if (!(bs[idx >> 6] & (1ULL << (idx & 63)))) continue;

                // Software prefetch for upcoming random accesses
                if (__builtin_expect(i + 16 < n_lineitem, 1)) {
                    uint32_t pk_ahead = (uint32_t)l_partkey[i + 16];
                    uint32_t ok_ahead = (uint32_t)l_orderkey[i + 16];
                    __builtin_prefetch(&ps_index[pk_ahead], 0, 1);
                    __builtin_prefetch(&orders_lookup[ok_ahead], 0, 1);
                }

                int32_t sk = l_suppkey[i];
                uint32_t ok = (uint32_t)l_orderkey[i];

                const PSEntry& ps_entry = ps_index[pk];
                double supplycost = 0.0;
                uint32_t ps_start = ps_entry.start;
                uint32_t ps_count = ps_entry.count;
                for (uint32_t j = ps_start; j < ps_start + ps_count; j++) {
                    if (ps_suppkey[j] == sk) { supplycost = ps_supplycost[j]; break; }
                }

                int32_t order_row = orders_lookup[ok];
                int year_off = year_from_days(o_orderdate[order_row]) - 1992;
                int32_t nk = s_nationkey[sk - 1];

                double amount = l_extendedprice[i] * (1.0 - l_discount[i]) - supplycost * l_quantity[i];
                my_agg[nk * 8 + year_off] += amount;
            }
        }
    }

    // Merge and output
    {
        GENDB_PHASE("output");
        double global_agg[256] = {};
        for (int t = 0; t < n_threads; t++) {
            const double* ta = tl_agg.data() + t * 256;
            for (int j = 0; j < 200; j++) global_agg[j] += ta[j];
        }

        struct Result { int nation_idx; int year; double sum_profit; };
        std::vector<Result> out;
        out.reserve(175);
        for (int n = 0; n < 25; n++)
            for (int y = 0; y < 8; y++) {
                double v = global_agg[n * 8 + y];
                if (v != 0.0) out.push_back({n, 1992 + y, v});
            }

        std::sort(out.begin(), out.end(), [&](const Result& a, const Result& b) {
            int cmp = nation_names[a.nation_idx].compare(nation_names[b.nation_idx]);
            if (cmp != 0) return cmp < 0;
            return a.year > b.year;
        });

        snprintf(path, sizeof(path), "%s/Q9.csv", results_dir);
        FILE* fp = fopen(path, "w");
        fprintf(fp, "nation,o_year,sum_profit\n");
        for (auto& r : out)
            fprintf(fp, "%s,%d,%.2f\n", nation_names[r.nation_idx].c_str(), r.year, r.sum_profit);
        fclose(fp);
    }

    return 0;
}
