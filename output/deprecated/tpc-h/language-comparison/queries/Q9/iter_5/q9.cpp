// Q9: Product Type Profit Measure — GenDB iter_5
// Storage extensions: partsupp.pk_flat.dense_array, orders.orderkey_year.direct_lookup
// Key optimization: PSFlat reduces 3 random cache misses to 1 contiguous 48-byte read
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <algorithm>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

// ── helpers ──────────────────────────────────────────────────────────────────

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
    close(fd);
    return {(const char*)p, sz};
}

struct PSFlat {
    int32_t suppkeys[4];
    double  supplycosts[4];
};

// ── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    const char* gdir = argv[1];
    const char* rdir = argv[2];
    char path[4096];

    GENDB_PHASE("total");

    // ── Phase 0: mmap all files ──────────────────────────────────────────────
    // Nation
    snprintf(path, sizeof(path), "%s/nation/n_nationkey.bin", gdir);
    auto m_n_nkey = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/nation/n_name_offsets.bin", gdir);
    auto m_n_noff = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/nation/n_name_data.bin", gdir);
    auto m_n_ndat = raw_mmap(path);
    // Supplier
    snprintf(path, sizeof(path), "%s/supplier/s_nationkey.bin", gdir);
    auto m_s_nkey = raw_mmap(path);
    // Part
    snprintf(path, sizeof(path), "%s/part/p_name_offsets.bin", gdir);
    auto m_p_off = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/part/p_name_data.bin", gdir);
    auto m_p_dat = raw_mmap(path);
    // Lineitem
    snprintf(path, sizeof(path), "%s/lineitem/l_partkey.bin", gdir);
    auto m_l_pk = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_suppkey.bin", gdir);
    auto m_l_sk = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_orderkey.bin", gdir);
    auto m_l_ok = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_extendedprice.bin", gdir);
    auto m_l_ep = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_discount.bin", gdir);
    auto m_l_di = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/lineitem/l_quantity.bin", gdir);
    auto m_l_qt = raw_mmap(path);
    // Storage extensions
    snprintf(path, sizeof(path), "%s/column_versions/orders.orderkey_year.direct_lookup/year_offset.bin", gdir);
    auto m_year_off = raw_mmap(path);
    snprintf(path, sizeof(path), "%s/column_versions/partsupp.pk_flat.dense_array/flat.bin", gdir);
    auto m_ps_flat = raw_mmap(path);

    // Pointers
    const int32_t* l_partkey  = (const int32_t*)m_l_pk.data;
    const int32_t* l_suppkey  = (const int32_t*)m_l_sk.data;
    const int32_t* l_orderkey = (const int32_t*)m_l_ok.data;
    const double*  l_extprice = (const double*)m_l_ep.data;
    const double*  l_discount = (const double*)m_l_di.data;
    const double*  l_quantity = (const double*)m_l_qt.data;
    size_t n_lineitem = m_l_pk.size / sizeof(int32_t);

    const int32_t* s_nationkey = (const int32_t*)m_s_nkey.data;

    // Year offset: skip 4-byte header
    const uint8_t* year_off_ptr = (const uint8_t*)(m_year_off.data + sizeof(uint32_t));

    // PSFlat: skip 4-byte header
    const PSFlat* ps_flat = (const PSFlat*)(m_ps_flat.data + sizeof(uint32_t));

    const int64_t* p_name_off = (const int64_t*)m_p_off.data;
    const char*    p_name_dat = m_p_dat.data;
    size_t n_parts = (m_p_off.size / sizeof(int64_t)) - 1;

    // ── Phase 1: Parallel prefault all large mmapped regions ─────────────────
    {
        GENDB_PHASE("prefault_pages");
        // Collect all regions to prefault
        struct Region { const char* base; size_t len; };
        Region regions[] = {
            {m_l_pk.data, m_l_pk.size},
            {m_l_sk.data, m_l_sk.size},
            {m_l_ok.data, m_l_ok.size},
            {m_l_ep.data, m_l_ep.size},
            {m_l_di.data, m_l_di.size},
            {m_l_qt.data, m_l_qt.size},
            {m_year_off.data, m_year_off.size},
            {m_ps_flat.data, m_ps_flat.size},
            {m_p_off.data, m_p_off.size},
            {m_p_dat.data, m_p_dat.size},
            {m_s_nkey.data, m_s_nkey.size},
        };
        constexpr int N_REGIONS = sizeof(regions)/sizeof(regions[0]);
        // Issue MADV_WILLNEED on all regions first (non-blocking kernel readahead)
        for (int r = 0; r < N_REGIONS; r++) {
            madvise((void*)regions[r].base, regions[r].len, MADV_WILLNEED);
        }
        // Parallel touch to ensure pages are faulted in
        #pragma omp parallel for schedule(dynamic)
        for (int r = 0; r < N_REGIONS; r++) {
            volatile char sum = 0;
            const char* base = regions[r].base;
            size_t len = regions[r].len;
            for (size_t off = 0; off < len; off += 4096) {
                sum += base[off];
            }
            (void)sum;
        }
    }

    // ── Phase 2: Load nation names ───────────────────────────────────────────
    std::string nation_names[25];
    {
        GENDB_PHASE("load_nation");
        const int32_t* n_keys = (const int32_t*)m_n_nkey.data;
        const int64_t* n_off  = (const int64_t*)m_n_noff.data;
        const char*    n_data = m_n_ndat.data;
        for (int i = 0; i < 25; i++) {
            int k = n_keys[i];
            nation_names[k] = std::string(n_data + n_off[i], n_off[i+1] - n_off[i]);
        }
    }

    // ── Phase 3: Filter parts — build bitset ─────────────────────────────────
    size_t bitset_words = (n_parts + 63) / 64;
    uint64_t* part_bitset = new uint64_t[bitset_words]();
    {
        GENDB_PHASE("filter_part");
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n_parts; i++) {
            const char* s = p_name_dat + p_name_off[i];
            size_t slen = p_name_off[i+1] - p_name_off[i];
            if (memmem(s, slen, "green", 5) != nullptr) {
                uint64_t bit = uint64_t(1) << (i & 63);
                __atomic_or_fetch(&part_bitset[i >> 6], bit, __ATOMIC_RELAXED);
            }
        }
    }

    // ── Phase 4: Main scan — lineitem join + aggregate ───────────────────────
    int n_threads = omp_get_max_threads();
    // Thread-local aggregation: [thread][nation][year_offset]
    // 25 nations × 8 year offsets (1992..1998 → offsets 0..6, slot 7 spare)
    double (*agg)[25][8] = new double[n_threads][25][8]();

    {
        GENDB_PHASE("main_scan");
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            double (*local_agg)[8] = agg[tid];

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_lineitem; i++) {
                int32_t pk = l_partkey[i];
                // Semi-join: check part bitset (pk is 1-based, bitset index = pk-1)
                size_t bidx = (size_t)(pk - 1);
                if ((part_bitset[bidx >> 6] & (uint64_t(1) << (bidx & 63))) == 0)
                    continue;

                int32_t sk = l_suppkey[i];

                // Partsupp flat lookup — single 48-byte contiguous read
                const PSFlat& ps = ps_flat[pk];
                double supplycost = 0.0;
                for (int j = 0; j < 4; j++) {
                    if (ps.suppkeys[j] == sk) {
                        supplycost = ps.supplycosts[j];
                        break;
                    }
                }

                // Year offset — single uint8_t read
                uint8_t yoff = year_off_ptr[l_orderkey[i]];

                // Nation — direct array lookup
                int32_t nk = s_nationkey[sk - 1];

                // Amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
                double amount = l_extprice[i] * (1.0 - l_discount[i]) - supplycost * l_quantity[i];

                local_agg[nk][yoff] += amount;
            }
        }
    }

    // ── Phase 5: Merge + Output ──────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        // Merge thread-local aggregations
        double global_agg[25][8] = {};
        for (int t = 0; t < n_threads; t++) {
            for (int n = 0; n < 25; n++) {
                for (int y = 0; y < 8; y++) {
                    global_agg[n][y] += agg[t][n][y];
                }
            }
        }

        // Build result sorted by nation ASC, o_year DESC
        struct Row { int nk; int year; double sum; };
        std::vector<Row> rows;
        rows.reserve(175);
        for (int n = 0; n < 25; n++) {
            for (int y = 6; y >= 0; y--) {  // year_offset 6→0 = 1998→1992
                if (global_agg[n][y] != 0.0) {
                    rows.push_back({n, 1992 + y, global_agg[n][y]});
                }
            }
        }

        // Write CSV
        snprintf(path, sizeof(path), "%s/Q9.csv", rdir);
        FILE* fp = fopen(path, "w");
        if (!fp) { fprintf(stderr, "Cannot open %s\n", path); return 1; }
        fprintf(fp, "nation,o_year,sum_profit\n");
        for (auto& r : rows) {
            fprintf(fp, "%s,%d,%.4f\n", nation_names[r.nk].c_str(), r.year, r.sum);
        }
        fclose(fp);
    }

    delete[] part_bitset;
    delete[] agg;

    return 0;
}
