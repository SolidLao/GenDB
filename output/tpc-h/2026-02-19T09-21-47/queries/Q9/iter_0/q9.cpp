#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <climits>
#include <omp.h>
#include <iostream>
#include "timing_utils.h"

// Year extraction: TPC-H orders span 1992-1998
// YEAR_DAYS[y-1970] = days from epoch to Jan 1 of year y
static const int32_t YEAR_DAYS[] = {
    0,365,730,1096,1461,1826,2191,2557,2922,3287,3652,
    4018,4383,4748,5113,5479,5844,6209,6574,6940,7305,
    7670,8035,8401,8766,9131,9496,9862,10227,10592,10957
};

static inline int extract_year(int32_t d) {
    // YEAR_DAYS[22]=8035 (1992), YEAR_DAYS[28]=10227 (1998)
    for (int y = 28; y >= 22; y--)
        if (d >= YEAR_DAYS[y]) return 1970 + y;
    return 1992;
}

// Helper: mmap a file, return pointer and size
static const void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "Cannot open: %s\n", path.c_str());
        exit(1);
    }
    struct stat st;
    fstat(fd, &st);
    out_size = (size_t)st.st_size;
    if (out_size == 0) { close(fd); return nullptr; }
    void* ptr = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) {
        fprintf(stderr, "mmap failed: %s\n", path.c_str());
        exit(1);
    }
    close(fd);
    return ptr;
}

// HashSlot for o_orderkey_hash: {int32_t key, uint32_t row_pos}
struct HashSlot32 {
    int32_t  key;
    uint32_t row_pos;
};

// HashSlot64 for ps_key_hash: {int64_t key, uint32_t row_pos, uint32_t _pad}
struct HashSlot64 {
    int64_t  key;
    uint32_t row_pos;
    uint32_t _pad;
};

static inline uint32_t probe_order_hash(const HashSlot32* slots, uint32_t cap,
                                         int32_t key) {
    uint64_t h = ((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> (64 - 25);
    h &= (cap - 1);
    while (true) {
        const HashSlot32& s = slots[h];
        if (s.key == key) return s.row_pos;
        if (s.key == INT32_MIN) return UINT32_MAX;
        h = (h + 1) & (cap - 1);
    }
}

static inline uint32_t probe_ps_hash(const HashSlot64* slots, uint32_t cap,
                                      int64_t key) {
    uint64_t h = ((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> (64 - 24);
    h &= (cap - 1);
    while (true) {
        const HashSlot64& s = slots[h];
        if (s.key == key) return s.row_pos;
        if (s.key == INT64_MIN) return UINT32_MAX;
        h = (h + 1) & (cap - 1);
    }
}

void run_Q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // =========================================================
    // Phase 1: Load dimension tables
    // =========================================================
    std::string nation_dir   = gendb_dir + "/nation";
    std::string supplier_dir = gendb_dir + "/supplier";
    std::string part_dir     = gendb_dir + "/part";
    std::string lineitem_dir = gendb_dir + "/lineitem";
    std::string partsupp_dir = gendb_dir + "/partsupp";
    std::string orders_dir   = gendb_dir + "/orders";

    // --- Nation: direct array[25] of n_name code by n_nationkey ---
    // Also load nation names from dict
    std::vector<std::string> nation_names;
    {
        std::string dict_path = nation_dir + "/n_name_dict.txt";
        FILE* f = fopen(dict_path.c_str(), "r");
        if (!f) { fprintf(stderr, "Cannot open %s\n", dict_path.c_str()); exit(1); }
        char buf[256];
        while (fgets(buf, sizeof(buf), f)) {
            size_t len = strlen(buf);
            while (len > 0 && (buf[len-1] == '\n' || buf[len-1] == '\r')) buf[--len] = 0;
            nation_names.emplace_back(buf);
        }
        fclose(f);
    }

    // n_nationkey -> n_name_code
    size_t n_nkey_sz, n_nname_sz;
    const int32_t* n_nationkey_col = (const int32_t*)mmap_file(nation_dir + "/n_nationkey.bin", n_nkey_sz);
    const int8_t*  n_name_col      = (const int8_t*) mmap_file(nation_dir + "/n_name.bin",      n_nname_sz);
    size_t n_nation = n_nkey_sz / sizeof(int32_t);

    // direct array: nationkey -> n_name string index
    int8_t nation_name_code[25];
    memset(nation_name_code, -1, sizeof(nation_name_code));
    for (size_t i = 0; i < n_nation; i++) {
        int32_t nk = n_nationkey_col[i];
        if (nk >= 0 && nk < 25) nation_name_code[nk] = n_name_col[i];
    }

    // --- Supplier: direct array[100001] of s_nationkey by s_suppkey ---
    size_t s_sk_sz, s_nk_sz;
    const int32_t* s_suppkey_col   = (const int32_t*)mmap_file(supplier_dir + "/s_suppkey.bin",   s_sk_sz);
    const int32_t* s_nationkey_col = (const int32_t*)mmap_file(supplier_dir + "/s_nationkey.bin", s_nk_sz);
    size_t n_supplier = s_sk_sz / sizeof(int32_t);

    std::vector<int8_t> supp_nationcode(100001, -1);
    for (size_t i = 0; i < n_supplier; i++) {
        int32_t sk = s_suppkey_col[i];
        if (sk > 0 && sk <= 100000) {
            int32_t nk = s_nationkey_col[i];
            if (nk >= 0 && nk < 25) supp_nationcode[sk] = (int8_t)nk;
        }
    }

    // --- Part: scan for p_name LIKE '%green%' → bitset ---
    size_t p_pk_sz, p_off_sz, p_data_sz;
    const int32_t*  p_partkey_col = (const int32_t*) mmap_file(part_dir + "/p_partkey.bin",      p_pk_sz);
    const uint32_t* p_name_offsets= (const uint32_t*)mmap_file(part_dir + "/p_name_offsets.bin", p_off_sz);
    const char*     p_name_data   = (const char*)    mmap_file(part_dir + "/p_name_data.bin",    p_data_sz);
    size_t n_part = p_pk_sz / sizeof(int32_t);

    // bitset for qualifying partkeys (max partkey ~2M)
    static const size_t BITSET_SIZE = (2000001 + 63) / 64;
    std::vector<uint64_t> part_bitset(BITSET_SIZE, 0);

    {
        GENDB_PHASE("dim_filter");
        for (size_t i = 0; i < n_part; i++) {
            uint32_t start = p_name_offsets[i];
            uint32_t len   = p_name_offsets[i+1] - start;
            const char* name_ptr = p_name_data + start;
            // substring search for "green"
            bool found = false;
            if (len >= 5) {
                // manual search
                for (uint32_t j = 0; j + 5 <= len; j++) {
                    if (name_ptr[j]   == 'g' &&
                        name_ptr[j+1] == 'r' &&
                        name_ptr[j+2] == 'e' &&
                        name_ptr[j+3] == 'e' &&
                        name_ptr[j+4] == 'n') {
                        found = true;
                        break;
                    }
                }
            }
            if (found) {
                int32_t pk = p_partkey_col[i];
                if (pk > 0 && pk <= 2000000) {
                    part_bitset[pk >> 6] |= (1ULL << (pk & 63));
                }
            }
        }
    }

    // =========================================================
    // Phase 2: mmap hash indexes
    // =========================================================
    size_t ps_hash_sz, o_hash_sz, ps_sc_sz, o_od_sz;
    const uint8_t* ps_hash_raw = (const uint8_t*)mmap_file(
        partsupp_dir + "/indexes/ps_key_hash.bin", ps_hash_sz);
    const uint8_t* o_hash_raw  = (const uint8_t*)mmap_file(
        orders_dir   + "/indexes/o_orderkey_hash.bin", o_hash_sz);
    const double*  ps_supplycost_col = (const double*)mmap_file(
        partsupp_dir + "/ps_supplycost.bin", ps_sc_sz);
    const int32_t* o_orderdate_col   = (const int32_t*)mmap_file(
        orders_dir   + "/o_orderdate.bin", o_od_sz);

    // Parse ps_key_hash header
    uint32_t ps_cap       = ((const uint32_t*)ps_hash_raw)[0];
    // uint32_t ps_num    = ((const uint32_t*)ps_hash_raw)[1];
    const HashSlot64* ps_slots = (const HashSlot64*)(ps_hash_raw + 8);

    // Parse o_orderkey_hash header
    uint32_t o_cap       = ((const uint32_t*)o_hash_raw)[0];
    // uint32_t o_num     = ((const uint32_t*)o_hash_raw)[1];
    const HashSlot32* o_slots = (const HashSlot32*)(o_hash_raw + 8);

    // =========================================================
    // Phase 3: mmap lineitem columns
    // =========================================================
    size_t l_pk_sz, l_sk_sz, l_ok_sz, l_ep_sz, l_disc_sz, l_qty_sz;
    const int32_t* l_partkey_col      = (const int32_t*)mmap_file(lineitem_dir + "/l_partkey.bin",      l_pk_sz);
    const int32_t* l_suppkey_col      = (const int32_t*)mmap_file(lineitem_dir + "/l_suppkey.bin",      l_sk_sz);
    const int32_t* l_orderkey_col     = (const int32_t*)mmap_file(lineitem_dir + "/l_orderkey.bin",     l_ok_sz);
    const double*  l_extprice_col     = (const double*) mmap_file(lineitem_dir + "/l_extendedprice.bin",l_ep_sz);
    const double*  l_discount_col     = (const double*) mmap_file(lineitem_dir + "/l_discount.bin",     l_disc_sz);
    const double*  l_quantity_col     = (const double*) mmap_file(lineitem_dir + "/l_quantity.bin",     l_qty_sz);
    size_t n_lineitem = l_pk_sz / sizeof(int32_t);

    // =========================================================
    // Phase 4: Parallel scan with fused operations
    // =========================================================
    // profit[thread][nation_code][year_idx], year_idx = year - 1992
    int num_threads = omp_get_max_threads();
    // Allocate thread-local profit arrays
    std::vector<std::vector<std::vector<double>>> tl_profit(
        num_threads, std::vector<std::vector<double>>(25, std::vector<double>(7, 0.0)));

    {
        GENDB_PHASE("main_scan");
        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            double local_profit[25][7] = {};

            #pragma omp for schedule(static, 100000)
            for (size_t i = 0; i < n_lineitem; i++) {
                int32_t l_pk = l_partkey_col[i];
                // bitset check
                if (l_pk <= 0 || l_pk > 2000000) continue;
                if (!(part_bitset[l_pk >> 6] & (1ULL << (l_pk & 63)))) continue;

                int32_t l_sk = l_suppkey_col[i];

                // supplier nation lookup
                int8_t nation_idx = (l_sk > 0 && l_sk <= 100000) ? supp_nationcode[l_sk] : -1;
                if (nation_idx < 0) continue;

                // ps_key_hash lookup for ps_supplycost
                int64_t ps_key = ((int64_t)l_pk << 32) | (uint32_t)l_sk;
                uint32_t ps_pos = probe_ps_hash(ps_slots, ps_cap, ps_key);
                if (ps_pos == UINT32_MAX) continue;
                double ps_supplycost = ps_supplycost_col[ps_pos];

                // o_orderkey_hash lookup for o_orderdate
                int32_t l_ok = l_orderkey_col[i];
                uint32_t o_pos = probe_order_hash(o_slots, o_cap, l_ok);
                if (o_pos == UINT32_MAX) continue;
                int32_t o_date = o_orderdate_col[o_pos];

                // year extraction
                int year = extract_year(o_date);
                int year_idx = year - 1992;
                if (year_idx < 0 || year_idx > 6) continue;

                // amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
                double amount = l_extprice_col[i] * (1.0 - l_discount_col[i])
                              - ps_supplycost * l_quantity_col[i];

                local_profit[(int)nation_idx][year_idx] += amount;
            }

            // store to thread-local array
            for (int n = 0; n < 25; n++)
                for (int y = 0; y < 7; y++)
                    tl_profit[tid][n][y] = local_profit[n][y];
        }
    }

    // =========================================================
    // Phase 5: Merge aggregates
    // =========================================================
    double profit[25][7] = {};
    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < num_threads; t++)
            for (int n = 0; n < 25; n++)
                for (int y = 0; y < 7; y++)
                    profit[n][y] += tl_profit[t][n][y];
    }

    // =========================================================
    // Phase 6: Build output rows and sort
    // =========================================================
    struct ResultRow {
        std::string nation;
        int         o_year;
        double      sum_profit;
    };
    std::vector<ResultRow> rows;
    rows.reserve(175);

    for (int n = 0; n < 25; n++) {
        int8_t code = nation_name_code[n];
        if (code < 0 || (size_t)code >= nation_names.size()) continue;
        const std::string& name = nation_names[(int)code];
        for (int y = 0; y < 7; y++) {
            if (profit[n][y] == 0.0) continue; // skip zero groups
            rows.push_back({name, 1992 + y, profit[n][y]});
        }
    }

    // ORDER BY nation ASC, o_year DESC
    std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
        int cmp = a.nation.compare(b.nation);
        if (cmp != 0) return cmp < 0;
        return a.o_year > b.o_year;
    });

    // =========================================================
    // Phase 7: Write CSV
    // =========================================================
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q9.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { fprintf(stderr, "Cannot open output: %s\n", out_path.c_str()); exit(1); }
        fprintf(f, "nation,o_year,sum_profit\n");
        for (const auto& r : rows) {
            fprintf(f, "%s,%d,%.2f\n", r.nation.c_str(), r.o_year, r.sum_profit);
        }
        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q9(gendb_dir, results_dir);
    return 0;
}
#endif
