// Q9: Product Type Profit Measure — GenDB iteration 3
// Optimizations:
//   1. mmap ALL files upfront + MADV_WILLNEED prefetch for I/O overlap
//   2. Parallel page pre-fault with OpenMP (pre-populates page tables)
//   3. NO advise_random() on o_orderdate/orders_lookup (sequential via l_orderkey sort)
//   4. advise_random() only on ps_suppkey and ps_supplycost
//   5. schedule(static) for both filter_part and main_scan
//   6. No heap allocations for lookup tables — direct index access

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

using namespace gendb;

// Lightweight mmap — no RAII overhead, just raw pointers
struct RawMap {
    const char* data;
    size_t size;
};

static RawMap raw_mmap(const char* path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { fprintf(stderr, "Cannot open %s\n", path); exit(1); }
    struct stat st;
    fstat(fd, &st);
    size_t sz = st.st_size;
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { fprintf(stderr, "mmap failed %s\n", path); exit(1); }
    close(fd); // fd not needed after mmap
    return {(const char*)p, sz};
}

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

    char gendb_dir[4096], results_dir[4096];
    snprintf(gendb_dir, sizeof(gendb_dir), "%s", argv[1]);
    snprintf(results_dir, sizeof(results_dir), "%s", argv[2]);

    GENDB_PHASE_MS("total", total_ms);

    // =========================================================================
    // Phase 0: mmap ALL files upfront with raw mmap (minimal syscall overhead)
    // =========================================================================
    char path[4096];

    // Nation
    snprintf(path, sizeof(path), "%s/nation/n_nationkey.bin", gendb_dir);
    auto m_n_nationkey = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/nation/n_name_offsets.bin", gendb_dir);
    auto m_n_name_off = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/nation/n_name_data.bin", gendb_dir);
    auto m_n_name_data = raw_mmap(path);

    // Supplier
    snprintf(path, sizeof(path), "%s/supplier/s_nationkey.bin", gendb_dir);
    auto m_s_nationkey = raw_mmap(path);

    // Part
    snprintf(path, sizeof(path), "%s/part/p_name_offsets.bin", gendb_dir);
    auto m_p_name_off = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/part/p_name_data.bin", gendb_dir);
    auto m_p_name_data = raw_mmap(path);

    // Orders
    snprintf(path, sizeof(path), "%s/orders/o_orderdate.bin", gendb_dir);
    auto m_o_orderdate = raw_mmap(path);

    // Orders orderkey lookup index
    snprintf(path, sizeof(path), "%s/indexes/orders_orderkey_lookup.bin", gendb_dir);
    auto m_orders_lookup = raw_mmap(path);

    // Partsupp columns
    snprintf(path, sizeof(path), "%s/partsupp/ps_suppkey.bin", gendb_dir);
    auto m_ps_suppkey = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/partsupp/ps_supplycost.bin", gendb_dir);
    auto m_ps_supplycost = raw_mmap(path);

    // Partsupp index
    snprintf(path, sizeof(path), "%s/indexes/partsupp_pk_index.bin", gendb_dir);
    auto m_ps_index = raw_mmap(path);

    // Lineitem columns
    snprintf(path, sizeof(path), "%s/lineitem/l_partkey.bin", gendb_dir);
    auto m_l_partkey = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_suppkey.bin", gendb_dir);
    auto m_l_suppkey = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_orderkey.bin", gendb_dir);
    auto m_l_orderkey = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_extendedprice.bin", gendb_dir);
    auto m_l_extendedprice = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_discount.bin", gendb_dir);
    auto m_l_discount = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_quantity.bin", gendb_dir);
    auto m_l_quantity = raw_mmap(path);

    // Extract typed pointers
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

    // =========================================================================
    // Phase 0b: Parallel page pre-fault + madvise
    // =========================================================================
    {
        GENDB_PHASE("prefault_pages");

        // Set madvise: random only for truly random access columns
        madvise((void*)ps_suppkey, m_ps_suppkey.size, MADV_RANDOM);
        madvise((void*)ps_supplycost, m_ps_supplycost.size, MADV_RANDOM);
        // orders_lookup and o_orderdate: keep MADV_SEQUENTIAL (access is sequential via l_orderkey)
        // lineitem: keep MADV_SEQUENTIAL (default from mmap for large files)

        // Collect all large mmapped regions for parallel pre-fault
        struct MmapRange2 { const volatile char* base; size_t size; };
        MmapRange2 ranges[] = {
            { (const volatile char*)l_partkey, m_l_partkey.size },
            { (const volatile char*)l_suppkey, m_l_suppkey.size },
            { (const volatile char*)l_orderkey, m_l_orderkey.size },
            { (const volatile char*)l_extendedprice, m_l_extendedprice.size },
            { (const volatile char*)l_discount, m_l_discount.size },
            { (const volatile char*)l_quantity, m_l_quantity.size },
            { (const volatile char*)ps_suppkey, m_ps_suppkey.size },
            { (const volatile char*)ps_supplycost, m_ps_supplycost.size },
            { (const volatile char*)m_orders_lookup.data, m_orders_lookup.size },
            { (const volatile char*)o_orderdate, m_o_orderdate.size },
            { (const volatile char*)m_ps_index.data, m_ps_index.size },
            { (const volatile char*)p_name_off_data, m_p_name_off.size },
            { (const volatile char*)p_name_data, m_p_name_data.size },
        };
        constexpr size_t n_ranges = sizeof(ranges) / sizeof(ranges[0]);

        size_t prefix[n_ranges + 1];
        prefix[0] = 0;
        for (size_t r = 0; r < n_ranges; r++) {
            prefix[r + 1] = prefix[r] + (ranges[r].size + 4095) / 4096;
        }
        size_t total_pages = prefix[n_ranges];

        #pragma omp parallel for schedule(static)
        for (size_t pg = 0; pg < total_pages; pg++) {
            size_t r = 0;
            while (r < n_ranges - 1 && pg >= prefix[r + 1]) r++;
            size_t offset = (pg - prefix[r]) * 4096;
            if (offset < ranges[r].size) {
                volatile char c __attribute__((unused)) = ranges[r].base[offset];
            }
        }
    }

    // =========================================================================
    // Phase 1: Load nation names (25 rows — trivial)
    // =========================================================================
    std::string nation_names[25];
    {
        GENDB_PHASE("load_nation");
        for (size_t i = 0; i < 25; i++) {
            int nk = n_nationkey[i];
            int64_t s = n_name_off[i], e = n_name_off[i+1];
            nation_names[nk] = std::string(n_name_data + s, e - s);
        }
    }

    // =========================================================================
    // Phase 2: Filter parts — PARALLEL bitset with schedule(static)
    // =========================================================================
    size_t bitset_words = (n_parts + 63) / 64;
    std::vector<uint64_t> part_bitset(bitset_words, 0);

    {
        GENDB_PHASE("filter_part");
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n_parts; i++) {
            size_t len = (size_t)(p_name_off_data[i+1] - p_name_off_data[i]);
            if (memmem(p_name_data + p_name_off_data[i], len, "green", 5) != nullptr) {
                size_t word = i >> 6;
                uint64_t bit = 1ULL << (i & 63);
                __atomic_or_fetch(&part_bitset[word], bit, __ATOMIC_RELAXED);
            }
        }
    }

    // =========================================================================
    // Phase 3: Parallel lineitem scan + join + aggregate
    // =========================================================================
    int n_threads = omp_get_max_threads();
    std::vector<double> tl_agg(n_threads * 25 * 8, 0.0);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            double* my_agg = tl_agg.data() + tid * 25 * 8;

            const uint64_t* bs = part_bitset.data();

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_lineitem; i++) {
                uint32_t pk = (uint32_t)l_partkey[i];
                uint32_t idx = pk - 1;
                if (!(bs[idx >> 6] & (1ULL << (idx & 63)))) continue;

                int32_t sk = l_suppkey[i];
                uint32_t ok = (uint32_t)l_orderkey[i];

                // Partsupp: dense_range index scan ~4 entries
                const PSEntry& ps_entry = ps_index[pk];
                double supplycost = 0.0;
                uint32_t ps_start = ps_entry.start;
                uint32_t ps_count = ps_entry.count;
                for (uint32_t j = ps_start; j < ps_start + ps_count; j++) {
                    if (ps_suppkey[j] == sk) { supplycost = ps_supplycost[j]; break; }
                }

                // Orders: orderkey -> row_index -> orderdate -> year
                int32_t order_row = orders_lookup[ok];
                int year_off = year_from_days(o_orderdate[order_row]) - 1992;

                // Supplier: s_nationkey[suppkey - 1]
                int32_t nk = s_nationkey[sk - 1];

                // amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
                double amount = l_extendedprice[i] * (1.0 - l_discount[i]) - supplycost * l_quantity[i];

                my_agg[nk * 8 + year_off] += amount;
            }
        }
    }

    // =========================================================================
    // Phase 4: Merge and output
    // =========================================================================
    {
        GENDB_PHASE("output");

        double global_agg[25 * 8] = {};
        for (int t = 0; t < n_threads; t++) {
            const double* ta = tl_agg.data() + t * 25 * 8;
            for (int j = 0; j < 25 * 8; j++) {
                global_agg[j] += ta[j];
            }
        }

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

        std::sort(out.begin(), out.end(), [&](const Result& a, const Result& b) {
            int cmp = nation_names[a.nation_idx].compare(nation_names[b.nation_idx]);
            if (cmp != 0) return cmp < 0;
            return a.year > b.year;
        });

        snprintf(path, sizeof(path), "%s/Q9.csv", results_dir);
        FILE* fp = fopen(path, "w");
        if (!fp) { perror("fopen"); return 1; }
        fprintf(fp, "nation,o_year,sum_profit\n");
        for (auto& r : out) {
            fprintf(fp, "%s,%d,%.2f\n", nation_names[r.nation_idx].c_str(), r.year, r.sum_profit);
        }
        fclose(fp);
    }

    // No munmap cleanup needed — process exit handles it
    return 0;
}
