// Q9: Product Type Profit Measure
// GenDB hand-tuned C++ implementation, iteration 1
// Optimization: parallelize dim_filter (2-pass chunked dict scan + parallel p_name bitmap build)

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <omp.h>
#include <climits>
#include <cmath>

#include "date_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
static constexpr int      BASE_YEAR      = 1992;
static constexpr int      MAX_YEAR_SLOTS = 10;   // covers 1992-2001
static constexpr int      NUM_NATIONS    = 25;
static constexpr uint32_t SUPP_ROWS      = 100000;
static constexpr uint32_t PART_ROWS      = 2000000;
// p_name dict has exactly PART_ROWS distinct entries (TPC-H spec); codes 0..PART_ROWS-1
static constexpr uint32_t MAX_CODE       = PART_ROWS - 1;

// ---------------------------------------------------------------------------
// Index slot structs
// ---------------------------------------------------------------------------
struct PKSlot { int32_t key; uint32_t row_idx; };

struct PSSlot { int32_t partkey; int32_t suppkey; uint32_t row_idx; };

// ---------------------------------------------------------------------------
// Aggregation slot — long double for SUM(ep*(1-disc) - ps_sc*qty)
// C35: int64_t is prohibited for any SUM(expr) involving multi-column products.
// l_extendedprice mandates long double accumulation.
// ---------------------------------------------------------------------------
struct ProfitSlot {
    long double sum_profit = 0.0L;
    bool        used       = false;
};

// ---------------------------------------------------------------------------
// mmap helper
// ---------------------------------------------------------------------------
static const void* mmap_ro(const std::string& path, size_t& sz) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); sz = 0; return nullptr; }
    struct stat st;
    fstat(fd, &st);
    sz = (size_t)st.st_size;
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return p;
}

// ---------------------------------------------------------------------------
// Hash functions (verbatim from build_indexes.cpp)
// ---------------------------------------------------------------------------
static inline uint32_t pk_hash(int32_t key, uint32_t mask) {
    return ((uint32_t)key * 2654435761u) & mask;
}
static inline uint32_t ps_hash(int32_t pk, int32_t sk, uint32_t mask) {
    uint64_t k = ((uint64_t)(uint32_t)pk << 32) | (uint32_t)sk;
    return (uint32_t)((k * 11400714819323198485ull) >> 32) & mask;
}

// ---------------------------------------------------------------------------
// Main query function
// ---------------------------------------------------------------------------
void run_q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();  // C1/C11

    // nthreads needed for dim_filter parallelism — hoist before that phase
    int nthreads = omp_get_max_threads();

    // -----------------------------------------------------------------------
    // DATA LOADING
    // -----------------------------------------------------------------------
    const char* ps_idx_raw  = nullptr;  size_t ps_idx_sz  = 0;
    const char* ord_idx_raw = nullptr;  size_t ord_idx_sz = 0;

    const int32_t* l_orderkey_col     = nullptr;
    const int32_t* l_partkey_col      = nullptr;
    const int32_t* l_suppkey_col      = nullptr;
    const double*  l_quantity_col     = nullptr;
    const double*  l_extendedprice_col= nullptr;
    const double*  l_discount_col     = nullptr;
    size_t lineitem_rows = 0;

    const double*  ps_supplycost_col  = nullptr;
    const int32_t* o_orderdate_col    = nullptr;
    const int32_t* s_suppkey_col      = nullptr;
    const int32_t* s_nationkey_col    = nullptr;
    const int32_t* p_partkey_col      = nullptr;
    const int32_t* p_name_col         = nullptr;
    const int32_t* n_nationkey_col    = nullptr;
    const int16_t* n_name_col         = nullptr;

    {
        GENDB_PHASE("data_loading");

        // P27: concurrent madvise for the two large indexes (192MB + 256MB)
        #pragma omp parallel sections
        {
            #pragma omp section
            {
                ps_idx_raw = (const char*)mmap_ro(
                    gendb_dir + "/partsupp/indexes/partsupp_pk_hash.bin", ps_idx_sz);
                if (ps_idx_raw)
                    madvise((void*)ps_idx_raw, ps_idx_sz, MADV_WILLNEED);
            }
            #pragma omp section
            {
                ord_idx_raw = (const char*)mmap_ro(
                    gendb_dir + "/orders/indexes/orders_pk_hash.bin", ord_idx_sz);
                if (ord_idx_raw)
                    madvise((void*)ord_idx_raw, ord_idx_sz, MADV_WILLNEED);
            }
        }

        // Lineitem columns (sequential scan)
        size_t sz;
        auto mmap_seq = [&](const std::string& path, size_t& out_sz) -> const void* {
            const void* p = mmap_ro(path, out_sz);
            if (p) madvise((void*)p, out_sz, MADV_SEQUENTIAL);
            return p;
        };

        l_orderkey_col      = (const int32_t*)mmap_seq(gendb_dir + "/lineitem/l_orderkey.bin",      sz);
        lineitem_rows       = sz / sizeof(int32_t);
        l_partkey_col       = (const int32_t*)mmap_seq(gendb_dir + "/lineitem/l_partkey.bin",       sz);
        l_suppkey_col       = (const int32_t*)mmap_seq(gendb_dir + "/lineitem/l_suppkey.bin",       sz);
        l_quantity_col      = (const double* )mmap_seq(gendb_dir + "/lineitem/l_quantity.bin",      sz);
        l_extendedprice_col = (const double* )mmap_seq(gendb_dir + "/lineitem/l_extendedprice.bin", sz);
        l_discount_col      = (const double* )mmap_seq(gendb_dir + "/lineitem/l_discount.bin",      sz);

        ps_supplycost_col   = (const double* )mmap_seq(gendb_dir + "/partsupp/ps_supplycost.bin",   sz);
        o_orderdate_col     = (const int32_t*)mmap_seq(gendb_dir + "/orders/o_orderdate.bin",       sz);

        // Small dimension columns
        s_suppkey_col    = (const int32_t*)mmap_ro(gendb_dir + "/supplier/s_suppkey.bin",    sz);
        s_nationkey_col  = (const int32_t*)mmap_ro(gendb_dir + "/supplier/s_nationkey.bin",  sz);
        p_partkey_col    = (const int32_t*)mmap_ro(gendb_dir + "/part/p_partkey.bin",        sz);
        p_name_col       = (const int32_t*)mmap_ro(gendb_dir + "/part/p_name.bin",           sz);
        n_nationkey_col  = (const int32_t*)mmap_ro(gendb_dir + "/nation/n_nationkey.bin",    sz);
        n_name_col       = (const int16_t*)mmap_ro(gendb_dir + "/nation/n_name.bin",         sz);
    }

    // C32: parse index headers at function scope before any probe loop
    uint32_t ps_cap  = *(const uint32_t*)ps_idx_raw;
    uint32_t ps_mask = ps_cap - 1;
    const PSSlot* ps_ht = (const PSSlot*)(ps_idx_raw + 4);

    uint32_t ord_cap  = *(const uint32_t*)ord_idx_raw;
    uint32_t ord_mask = ord_cap - 1;
    const PKSlot* ord_ht = (const PKSlot*)(ord_idx_raw + 4);

    // -----------------------------------------------------------------------
    // Build nation structures
    // -----------------------------------------------------------------------
    std::vector<std::string> nation_dict;
    {
        std::string path = gendb_dir + "/nation/n_name_dict.txt";
        FILE* f = fopen(path.c_str(), "r");
        char line[256];
        while (fgets(line, sizeof(line), f)) {
            size_t len = strlen(line);
            while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r')) --len;
            nation_dict.emplace_back(line, len);
        }
        fclose(f);
    }

    // Build nation_name[nationkey] — indexed by n_nationkey (0-24)
    std::vector<std::string> nation_name(NUM_NATIONS);
    for (int i = 0; i < NUM_NATIONS; i++) {
        int32_t nk = n_nationkey_col[i];
        int16_t nc = n_name_col[i];
        nation_name[nk] = nation_dict[(uint16_t)nc];  // C18
    }

    // Build suppkey_to_nationkey[s_suppkey] — dense direct array [1,100000]
    std::vector<int32_t> suppkey_to_nationkey(SUPP_ROWS + 1, -1);
    for (uint32_t i = 0; i < SUPP_ROWS; i++) {
        suppkey_to_nationkey[s_suppkey_col[i]] = s_nationkey_col[i];
    }

    // -----------------------------------------------------------------------
    // DIM FILTER: part.p_name LIKE '%green%' → part_bitmap_u8
    // -----------------------------------------------------------------------
    // Optimization vs iter_0:
    //   1. Eliminate max_code first-pass over p_name.bin (use fixed MAX_CODE=2M-1)
    //   2. Parallel 2-pass dict scan: count newlines per chunk → prefix sum → parallel memmem
    //   3. Switch to uint8_t for code_qualifies and part_bitmap (thread-safe, no bit masking)
    //   4. Parallel p_name.bin scan to build part_bitmap_u8
    //   5. Add MADV_SEQUENTIAL to dict_raw
    // P32: bitmap fits L3 (2MB), O(1) semi-join test in lineitem scan
    std::vector<uint8_t> part_bitmap_u8(PART_ROWS + 1, 0);
    {
        GENDB_PHASE("dim_filter");

        // Mmap dict with sequential hint
        size_t dict_sz = 0;
        const char* dict_raw = (const char*)mmap_ro(
            gendb_dir + "/part/p_name_dict.txt", dict_sz);
        madvise((void*)dict_raw, dict_sz, MADV_SEQUENTIAL);

        // ---- Compute chunk boundaries (byte-aligned to line boundaries) ----
        // Each chunk_starts[t] points to the first char of a complete line
        std::vector<const char*> chunk_starts(nthreads + 1);
        chunk_starts[0]        = dict_raw;
        chunk_starts[nthreads] = dict_raw + dict_sz;
        {
            size_t chunk_bytes = (dict_sz + (size_t)nthreads - 1) / (size_t)nthreads;
            for (int t = 1; t < nthreads; t++) {
                const char* p = dict_raw + (size_t)t * chunk_bytes;
                if (p >= dict_raw + dict_sz) {
                    chunk_starts[t] = dict_raw + dict_sz;
                } else {
                    // Advance to the start of the next complete line
                    const char* nl = (const char*)memchr(p, '\n',
                                                         (dict_raw + dict_sz) - p);
                    chunk_starts[t] = nl ? nl + 1 : dict_raw + dict_sz;
                }
            }
        }

        // ---- Pass 1: count lines per chunk (parallel) ----
        std::vector<uint32_t> chunk_line_counts(nthreads, 0);
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            const char* start = chunk_starts[tid];
            const char* end   = chunk_starts[tid + 1];
            uint32_t count = 0;
            for (const char* p = start; p < end; ) {
                const char* nl = (const char*)memchr(p, '\n', end - p);
                count++;
                p = nl ? nl + 1 : end;
            }
            chunk_line_counts[tid] = count;
        }

        // Prefix sum → starting line number for each chunk
        std::vector<uint32_t> chunk_line_starts(nthreads + 1, 0);
        for (int t = 0; t < nthreads; t++) {
            chunk_line_starts[t + 1] = chunk_line_starts[t] + chunk_line_counts[t];
        }

        // ---- Pass 2: parallel memmem scan, mark qualifying codes ----
        // uint8_t: one byte per code, thread-safe writes (each line_num unique per thread)
        std::vector<uint8_t> code_qualifies(MAX_CODE + 1, 0);
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            const char* start    = chunk_starts[tid];
            const char* end      = chunk_starts[tid + 1];
            uint32_t    line_num = chunk_line_starts[tid];

            for (const char* p = start; p < end; ) {
                const char* nl       = (const char*)memchr(p, '\n', end - p);
                const char* line_end = nl ? nl : end;
                size_t      line_len = (size_t)(line_end - p);

                if (line_num <= MAX_CODE &&
                    memmem(p, line_len, "green", 5) != nullptr) {
                    code_qualifies[line_num] = 1;
                }

                line_num++;
                p = nl ? nl + 1 : end;
            }
        }
        munmap((void*)dict_raw, dict_sz);

        // ---- Parallel p_name.bin scan → build part_bitmap_u8 ----
        // p_partkey is PK (unique per row) → each part_bitmap_u8 index written by at most
        // one thread → no data race with uint8_t byte-level stores
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (uint32_t r = 0; r < PART_ROWS; r++) {
            int32_t code = p_name_col[r];
            if (code >= 0 && (uint32_t)code <= MAX_CODE && code_qualifies[code]) {
                part_bitmap_u8[p_partkey_col[r]] = 1;
            }
        }
    }

    // -----------------------------------------------------------------------
    // MAIN SCAN: lineitem with morsel-driven parallelism
    // -----------------------------------------------------------------------
    // Thread-local aggregation: agg[nation_idx * MAX_YEAR_SLOTS + year_idx]
    std::vector<ProfitSlot> tl_agg((size_t)nthreads * NUM_NATIONS * MAX_YEAR_SLOTS);
    std::fill(tl_agg.begin(), tl_agg.end(), ProfitSlot{});  // C20

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            ProfitSlot* my_agg = tl_agg.data() + (size_t)tid * NUM_NATIONS * MAX_YEAR_SLOTS;

            #pragma omp for schedule(dynamic, 65536)
            for (int64_t row = 0; row < (int64_t)lineitem_rows; row++) {

                // P32: bitmap semi-join — first filter (cheapest, LLC-resident)
                int32_t lpartkey = l_partkey_col[row];
                if ((uint32_t)lpartkey > PART_ROWS || !part_bitmap_u8[lpartkey]) continue;

                int32_t lsuppkey = l_suppkey_col[row];

                // Supplier → nationkey via direct array (O(1), no hash probe)
                int32_t nationkey = suppkey_to_nationkey[lsuppkey];
                if (nationkey < 0 || nationkey >= NUM_NATIONS) continue;

                int32_t lorderkey = l_orderkey_col[row];

                // Probe orders_pk_hash → o_orderdate → extract year  (C11/C1)
                int32_t o_year = -1;
                {
                    uint32_t h = pk_hash(lorderkey, ord_mask);
                    for (uint32_t pr = 0; pr < ord_cap; pr++) {
                        uint32_t idx = (h + pr) & ord_mask;
                        if (ord_ht[idx].key == INT32_MIN) break;
                        if (ord_ht[idx].key == lorderkey) {
                            uint32_t orow = ord_ht[idx].row_idx;
                            o_year = gendb::extract_year(o_orderdate_col[orow]);
                            break;
                        }
                    }
                }
                if (o_year < 0) continue;

                int year_idx = o_year - BASE_YEAR;
                if (year_idx < 0 || year_idx >= MAX_YEAR_SLOTS) continue;

                // Probe partsupp_pk_hash → ps_supplycost  (C24 bounded)
                double ps_sc    = 0.0;
                bool   ps_found = false;
                {
                    uint32_t h = ps_hash(lpartkey, lsuppkey, ps_mask);
                    for (uint32_t pr = 0; pr < ps_cap; pr++) {
                        uint32_t idx = (h + pr) & ps_mask;
                        if (ps_ht[idx].partkey == INT32_MIN) break;
                        if (ps_ht[idx].partkey == lpartkey && ps_ht[idx].suppkey == lsuppkey) {
                            uint32_t psrow = ps_ht[idx].row_idx;
                            ps_sc    = ps_supplycost_col[psrow];
                            ps_found = true;
                            break;
                        }
                    }
                }
                if (!ps_found) continue;

                // C35: long double accumulation required for multi-column product SUM
                double ep   = l_extendedprice_col[row];
                double disc = l_discount_col[row];
                double qty  = l_quantity_col[row];

                long double amount = (long double)ep * (1.0L - (long double)disc)
                                   - (long double)ps_sc * (long double)qty;

                // Accumulate into thread-local slot  (C15: key = nationkey + year)
                ProfitSlot& slot = my_agg[nationkey * MAX_YEAR_SLOTS + year_idx];
                slot.sum_profit += amount;
                slot.used        = true;
            }
        }
    }

    // -----------------------------------------------------------------------
    // AGGREGATE MERGE — single pass, trivial (nthreads × 250 slots)
    // -----------------------------------------------------------------------
    ProfitSlot global_agg[NUM_NATIONS][MAX_YEAR_SLOTS];
    std::fill(&global_agg[0][0], &global_agg[0][0] + NUM_NATIONS * MAX_YEAR_SLOTS,
              ProfitSlot{});  // C20

    for (int tid = 0; tid < nthreads; tid++) {
        const ProfitSlot* my_agg = tl_agg.data() + (size_t)tid * NUM_NATIONS * MAX_YEAR_SLOTS;
        for (int ni = 0; ni < NUM_NATIONS; ni++) {
            for (int yi = 0; yi < MAX_YEAR_SLOTS; yi++) {
                const ProfitSlot& src = my_agg[ni * MAX_YEAR_SLOTS + yi];
                if (src.used) {
                    global_agg[ni][yi].sum_profit += src.sum_profit;
                    global_agg[ni][yi].used        = true;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // OUTPUT: sort by nation ASC, o_year DESC; write CSV
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        struct Result {
            const std::string* nation;
            int                o_year;
            long double        sum_profit;
        };

        std::vector<Result> results;
        results.reserve(175);

        for (int ni = 0; ni < NUM_NATIONS; ni++) {
            for (int yi = 0; yi < MAX_YEAR_SLOTS; yi++) {
                if (global_agg[ni][yi].used) {
                    results.push_back({&nation_name[ni], BASE_YEAR + yi,
                                        global_agg[ni][yi].sum_profit});
                }
            }
        }

        // ORDER BY nation ASC, o_year DESC
        std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
            int cmp = a.nation->compare(*b.nation);
            if (cmp != 0) return cmp < 0;
            return a.o_year > b.o_year;
        });

        // Write CSV
        std::string outpath = results_dir + "/Q9.csv";
        FILE* out = fopen(outpath.c_str(), "w");
        fprintf(out, "nation,o_year,sum_profit\n");
        for (const auto& r : results) {
            // C31: double-quote nation string
            fprintf(out, "\"%s\",%d,%.2Lf\n",
                    r.nation->c_str(), r.o_year, r.sum_profit);
        }
        fclose(out);
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir    = argv[1];
    std::string results_dir  = argc > 2 ? argv[2] : ".";
    run_q9(gendb_dir, results_dir);
    return 0;
}
#endif
