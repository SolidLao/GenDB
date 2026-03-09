// Q9: Product Type Profit Measure
// Strategy: bitset for green parts, pre-built hash indexes for partsupp+orders,
//           direct array for supplier nationkey, parallel morsel-driven lineitem scan.
// Usage: ./q9 <gendb_dir> <results_dir>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <string>
#include <vector>
#include <algorithm>
#include <atomic>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

// ---------------------------------------------------------------------------
// Hash functions (verbatim from build_indexes.cpp)
// ---------------------------------------------------------------------------
static inline uint32_t hash32(uint32_t x) {
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = (x >> 16) ^ x;
    return x;
}

static inline uint64_t hash64(uint64_t x) {
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    return x ^ (x >> 31);
}

// ---------------------------------------------------------------------------
// JDN year extraction (verbatim from plan)
// ---------------------------------------------------------------------------
static inline int32_t extract_year(int32_t days) {
    int32_t jdn = days + 2440588;
    int32_t a   = jdn + 32044;
    int32_t b2  = (4*a + 3) / 146097;
    int32_t c   = a - (146097*b2) / 4;
    int32_t d2  = (4*c + 3) / 1461;
    int32_t e   = c - (1461*d2) / 4;
    int32_t m   = (5*e + 2) / 153;
    return 100*b2 + d2 - 4800 + (m < 10 ? 0 : 1);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: q9 <gendb_dir> <results_dir>\n");
        return 1;
    }
    GENDB_PHASE("total");

    const std::string gendb = argv[1];
    const std::string rdir  = argv[2];

    // -----------------------------------------------------------------------
    // PHASE: data_loading
    // -----------------------------------------------------------------------
    {
    GENDB_PHASE("data_loading");

    // --- Part: build bitset of green partkeys ----------------------------
    // p_partkey range: 1..2000000, bitset uses 250KB (fits in L2)
    const int MAX_PART = 2000001;
    uint8_t* is_green = (uint8_t*)calloc(MAX_PART, 1);

    {
        MmapColumn<int32_t> partkey(gendb + "/part/p_partkey.bin");
        // p_name.bin: 2,000,000 × 56 bytes
        const std::string pname_path = gendb + "/part/p_name.bin";
        int fd = ::open(pname_path.c_str(), O_RDONLY);
        if (fd < 0) { perror("open p_name.bin"); return 1; }
        struct stat st; fstat(fd, &st);
        const char* pname_buf = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        madvise((void*)pname_buf, st.st_size, MADV_SEQUENTIAL);

        size_t n = partkey.count;
        for (size_t i = 0; i < n; i++) {
            if (strstr(pname_buf + i * 56, "green")) {
                int32_t pk = partkey.data[i];
                if (pk >= 0 && pk < MAX_PART) is_green[pk] = 1;
            }
        }
        munmap((void*)pname_buf, st.st_size);
    }

    // --- Supplier: build direct array suppkey -> nationkey ---------------
    // s_suppkey: 1..100000, so direct array is optimal (~2KB in L1)
    const int MAX_SUPP = 100001;
    uint8_t* supp_nat = (uint8_t*)calloc(MAX_SUPP, 1);
    {
        MmapColumn<int32_t> suppkey  (gendb + "/supplier/s_suppkey.bin");
        MmapColumn<int32_t> nationkey(gendb + "/supplier/s_nationkey.bin");
        size_t n = suppkey.count;
        for (size_t i = 0; i < n; i++) {
            int32_t sk = suppkey.data[i];
            if (sk >= 0 && sk < MAX_SUPP)
                supp_nat[sk] = (uint8_t)nationkey.data[i];
        }
    }

    // --- Nation: load 25 names (650 bytes) --------------------------------
    char nation_names[25][26] = {};
    {
        MmapColumn<int32_t> natkey (gendb + "/nation/n_nationkey.bin");
        MmapColumn<char>    natname(gendb + "/nation/n_name.bin");
        // n_name.bin: 25 × 26 bytes
        size_t n = natkey.count;
        for (size_t i = 0; i < n; i++) {
            int32_t k = natkey.data[i];
            if (k >= 0 && k < 25) {
                strncpy(nation_names[k], natname.data + i*26, 25);
                nation_names[k][25] = '\0';
            }
        }
    }

    // --- Load hash indexes (pre-built) ------------------------------------
    // s_suppkey_hash: capacity 262144, keys int32_t sentinel=-1, values int32_t
    MmapColumn<uint8_t> supp_hash_raw(gendb + "/supplier/s_suppkey_hash.bin");
    const int64_t supp_cap = 262144;
    const int32_t* supp_hkeys = (const int32_t*)(supp_hash_raw.data + 16);
    const int32_t* supp_hvals = supp_hkeys + supp_cap;
    // (We won't actually use supplier hash since direct array is faster)

    // ps_composite_hash: capacity 16777216, keys int64_t sentinel=-1LL, values int32_t
    MmapColumn<uint8_t> ps_hash_raw(gendb + "/partsupp/ps_composite_hash.bin");
    const int64_t ps_cap  = 16777216;
    const int64_t ps_mask = ps_cap - 1;
    const int64_t* ps_hkeys  = (const int64_t*)(ps_hash_raw.data + 16);
    const int32_t* ps_hvals  = (const int32_t*)(ps_hkeys + ps_cap);

    // o_orderkey_hash: capacity 33554432, keys int32_t sentinel=-1, values int32_t
    MmapColumn<uint8_t> ord_hash_raw(gendb + "/orders/o_orderkey_hash.bin");
    const int64_t ord_cap  = 33554432;
    const int64_t ord_mask = ord_cap - 1;
    const int32_t* ord_hkeys = (const int32_t*)(ord_hash_raw.data + 16);
    const int32_t* ord_hvals = ord_hkeys + ord_cap;

    // ps_supplycost and o_orderdate columns (for post-probe value fetch)
    MmapColumn<double>  ps_supplycost(gendb + "/partsupp/ps_supplycost.bin");
    MmapColumn<int32_t> o_orderdate  (gendb + "/orders/o_orderdate.bin");
    ps_supplycost.advise_random();
    o_orderdate.advise_random();

    // --- Load lineitem columns -------------------------------------------
    MmapColumn<int32_t> l_orderkey  (gendb + "/lineitem/l_orderkey.bin");
    MmapColumn<int32_t> l_partkey   (gendb + "/lineitem/l_partkey.bin");
    MmapColumn<int32_t> l_suppkey   (gendb + "/lineitem/l_suppkey.bin");
    MmapColumn<double>  l_quantity  (gendb + "/lineitem/l_quantity.bin");
    MmapColumn<double>  l_extprice  (gendb + "/lineitem/l_extendedprice.bin");
    MmapColumn<double>  l_discount  (gendb + "/lineitem/l_discount.bin");

    mmap_prefetch_all(l_orderkey, l_partkey, l_suppkey, l_quantity, l_extprice, l_discount);

    const size_t total_rows = l_orderkey.count;
    const int64_t MORSEL = 100000;

    // -----------------------------------------------------------------------
    // PHASE: dim_filter (bitset already built)
    // -----------------------------------------------------------------------
    // (already done above)

    // -----------------------------------------------------------------------
    // PHASE: build_joins (indexes already mmap'd)
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // PHASE: main_scan — parallel morsel-driven lineitem scan
    // -----------------------------------------------------------------------
    {
    GENDB_PHASE("main_scan");

    const int nthreads = (int)std::thread::hardware_concurrency();
    std::atomic<int64_t> morsel_counter{0};

    // Per-thread aggregation: agg[nation_idx][year_idx], 25 nations × 7 years
    // Allocate on heap to avoid stack size issues with many threads
    std::vector<std::vector<double>> thread_agg(nthreads, std::vector<double>(25*7, 0.0));

    auto worker = [&](int tid) {
        double* agg = thread_agg[tid].data(); // agg[nation*7 + year]

        const int32_t* ok_col = l_orderkey.data;
        const int32_t* pk_col = l_partkey.data;
        const int32_t* sk_col = l_suppkey.data;
        const double*  qt_col = l_quantity.data;
        const double*  ep_col = l_extprice.data;
        const double*  dc_col = l_discount.data;

        while (true) {
            int64_t start = morsel_counter.fetch_add(MORSEL, std::memory_order_relaxed);
            if (start >= (int64_t)total_rows) break;
            int64_t end = std::min(start + MORSEL, (int64_t)total_rows);

            for (int64_t i = start; i < end; i++) {
                // --- Filter: part must be green -----------------------
                int32_t pk = pk_col[i];
                if (pk < 0 || pk >= MAX_PART || !is_green[pk]) continue;

                // --- Supplier lookup: suppkey -> nationkey -----------
                int32_t sk = sk_col[i];
                if (sk < 0 || sk >= MAX_SUPP) continue;
                uint8_t nat = supp_nat[sk];
                if (nat >= 25) continue;

                // --- Partsupp lookup: (partkey, suppkey) -> supplycost
                int64_t ps_key = ((int64_t)pk << 32) | (uint32_t)sk;
                uint64_t h_ps = hash64((uint64_t)ps_key) & (uint64_t)ps_mask;
                int32_t ps_row = -1;
                while (ps_hkeys[h_ps] != -1LL) {
                    if (ps_hkeys[h_ps] == ps_key) { ps_row = ps_hvals[h_ps]; break; }
                    h_ps = (h_ps + 1) & (uint64_t)ps_mask;
                }
                if (ps_row < 0) continue;
                double supplycost = ps_supplycost.data[ps_row];

                // --- Orders lookup: orderkey -> orderdate -> year ---
                int32_t okey = ok_col[i];
                uint64_t h_o = (uint64_t)hash32((uint32_t)okey) & (uint64_t)ord_mask;
                int32_t ord_row = -1;
                while (ord_hkeys[h_o] != -1) {
                    if (ord_hkeys[h_o] == okey) { ord_row = ord_hvals[h_o]; break; }
                    h_o = (h_o + 1) & (uint64_t)ord_mask;
                }
                if (ord_row < 0) continue;
                int32_t days = o_orderdate.data[ord_row];
                int32_t year = extract_year(days);
                int32_t yr   = year - 1992;
                if (yr < 0 || yr > 6) continue;

                // --- Compute amount and accumulate -------------------
                double amount = ep_col[i] * (1.0 - dc_col[i]) - supplycost * qt_col[i];
                agg[nat * 7 + yr] += amount;
            }
        }
    };

    // Launch threads
    std::vector<std::thread> threads;
    threads.reserve(nthreads);
    for (int t = 0; t < nthreads; t++)
        threads.emplace_back(worker, t);
    for (auto& t : threads) t.join();

    // -----------------------------------------------------------------------
    // PHASE: merge_aggregate — reduce thread-local arrays
    // -----------------------------------------------------------------------
    double final_agg[25][7] = {};
    for (int t = 0; t < nthreads; t++) {
        const double* src = thread_agg[t].data();
        for (int n = 0; n < 25; n++)
            for (int y = 0; y < 7; y++)
                final_agg[n][y] += src[n*7 + y];
    }

    // -----------------------------------------------------------------------
    // PHASE: sort_output
    // -----------------------------------------------------------------------
    struct Row { char nation[26]; int year; double sum_profit; };
    std::vector<Row> rows;
    rows.reserve(175);
    for (int n = 0; n < 25; n++) {
        if (!nation_names[n][0]) continue;
        for (int y = 0; y < 7; y++) {
            if (final_agg[n][y] != 0.0) {
                Row r;
                strncpy(r.nation, nation_names[n], 25);
                r.nation[25] = '\0';
                r.year = 1992 + y;
                r.sum_profit = final_agg[n][y];
                rows.push_back(r);
            }
        }
    }
    std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
        int c = strcmp(a.nation, b.nation);
        if (c != 0) return c < 0;
        return a.year > b.year;  // year DESC
    });

    // -----------------------------------------------------------------------
    // PHASE: output
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");
        const std::string outpath = rdir + "/Q9.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        if (!f) { perror("fopen output"); return 1; }
        fprintf(f, "nation,o_year,sum_profit\n");
        for (const auto& r : rows)
            fprintf(f, "%s,%d,%.4f\n", r.nation, r.year, r.sum_profit);
        fclose(f);
    }

    free(is_green);
    free(supp_nat);
    (void)supp_hkeys; (void)supp_hvals; // suppress unused warning

    } // main_scan phase
    } // data_loading phase (wraps everything except output)

    return 0;
}
