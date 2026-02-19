#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

// ============================================================
// Helper: mmap a binary column file
// ============================================================
template<typename T>
static const T* mmap_col(const std::string& path, size_t& n) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    n = st.st_size / sizeof(T);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return reinterpret_cast<const T*>(p);
}

// ============================================================
// Part bitset: 2M+1 bits — set bit[pk] if p_name contains 'green'
// ============================================================
static constexpr int PART_BITSET_WORDS = (2000001 + 63) / 64;
static uint64_t part_bitset[PART_BITSET_WORDS];

inline void bit_set(uint64_t* bs, int32_t i)        { bs[i>>6] |= 1ULL<<(i&63); }
inline bool bit_test(const uint64_t* bs, int32_t i) { return (bs[i>>6]>>(i&63))&1; }

// ============================================================
// Partsupp composite hash map:
//   key  = ps_partkey * 100001LL + ps_suppkey  (always >= 100002, so 0 = empty)
//   value= ps_supplycost (scaled int64_t)
// Open addressing, linear probing, multiply-shift hash
// ============================================================
struct PSEntry { int64_t key; int64_t cost; };
static PSEntry* ps_ht  = nullptr;
static uint32_t ps_mask = 0;

static inline uint32_t ps_hash(int64_t key) {
    return (uint32_t)((uint64_t)(uint64_t)key * 11400714819323198485ULL >> 32);
}
static void ps_insert(int64_t key, int64_t cost) {
    uint32_t h = ps_hash(key) & ps_mask;
    while (ps_ht[h].key && ps_ht[h].key != key) h = (h+1) & ps_mask;
    ps_ht[h] = {key, cost};
}
static int64_t ps_lookup(int64_t key) {
    uint32_t h = ps_hash(key) & ps_mask;
    while (ps_ht[h].key && ps_ht[h].key != key) h = (h+1) & ps_mask;
    return ps_ht[h].key ? ps_ht[h].cost : INT64_MIN;
}

// ============================================================
// Orders flat array: o_orderkey -> year (int16_t, 0 = absent)
// TPC-H SF10 max orderkey = 60,000,000
// ============================================================
static constexpr int32_t MAX_ORDERKEY = 60000001;
static int16_t* orders_year = nullptr;

// ============================================================
// run_Q9: TPC-H Q9 — Product Type Profit Measure
// ============================================================
void run_Q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // Derive TPC-H raw data dir from gendb_dir:
    // gendb_dir = .../tpc-h/gendb/tpch_sf10.gendb
    // data_dir  = .../tpc-h/data/sf10
    std::string data_dir = gendb_dir + "/../../data/sf10";

    // ============================================================
    // Phase A: Load nation names
    // n_name.bin does not exist in gendb storage (ingest skips raw strings).
    // Read nation.tbl (only 25 rows) for correctness; fallback to TPC-H standard.
    // ============================================================
    std::string nation_names[25];
    {
        GENDB_PHASE("nation_load");
        std::string tbl_path = data_dir + "/nation.tbl";
        FILE* f = fopen(tbl_path.c_str(), "r");
        if (f) {
            char line[512];
            while (fgets(line, sizeof(line), f)) {
                int32_t nk = 0;
                char* p = line;
                while (*p >= '0' && *p <= '9') nk = nk*10 + (*p++ - '0');
                if (*p != '|') continue;
                p++;
                char* name_end = strchr(p, '|');
                if (name_end) *name_end = '\0';
                if (nk >= 0 && nk < 25) nation_names[nk] = p;
            }
            fclose(f);
        } else {
            // Fallback: TPC-H standard nation table (Appendix A of spec)
            const char* TPCH[25] = {
                "ALGERIA","ARGENTINA","BRAZIL","CANADA","EGYPT",
                "ETHIOPIA","FRANCE","GERMANY","INDIA","INDONESIA",
                "IRAN","IRAQ","JAPAN","JORDAN","KENYA",
                "MOROCCO","MOZAMBIQUE","PERU","CHINA","ROMANIA",
                "SAUDI ARABIA","VIETNAM","RUSSIA","UNITED KINGDOM","UNITED STATES"
            };
            for (int i = 0; i < 25; i++) nation_names[i] = TPCH[i];
        }
    }

    // ============================================================
    // Phase B: Build supplier -> nationkey flat array (100KB, fits in L2)
    // s_suppkey is dense 1..100K -> direct array index
    // ============================================================
    static uint8_t suppkey_to_nation[100001];
    memset(suppkey_to_nation, 0, sizeof(suppkey_to_nation));
    {
        GENDB_PHASE("supplier_build");
        size_t n;
        const int32_t* s_sk = mmap_col<int32_t>(gendb_dir + "/supplier/s_suppkey.bin",  n);
        const int32_t* s_nk = mmap_col<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", n);
        for (size_t i = 0; i < n; i++)
            suppkey_to_nation[s_sk[i]] = (uint8_t)s_nk[i];
    }

    // ============================================================
    // Phase C: Build part bitset from part.tbl
    // p_name.bin does not exist in gendb (ingest skips raw strings).
    // mmap part.tbl and scan for "green" substring in p_name field.
    // Format: partkey|p_name|p_mfgr|p_brand|...
    // ============================================================
    memset(part_bitset, 0, sizeof(part_bitset));
    {
        GENDB_PHASE("part_filter");
        std::string part_tbl = data_dir + "/part.tbl";
        int fd = open(part_tbl.c_str(), O_RDONLY);
        if (fd < 0) { perror(part_tbl.c_str()); exit(1); }
        struct stat st; fstat(fd, &st);
        const char* buf = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (buf == MAP_FAILED) { perror("mmap part.tbl"); exit(1); }
        close(fd);
        madvise((void*)buf, st.st_size, MADV_SEQUENTIAL);

        const char* end = buf + st.st_size;
        const char* p   = buf;
        while (p < end) {
            // Parse partkey (integer before first '|')
            int32_t pk = 0;
            while (p < end && (unsigned char)(*p - '0') <= 9u) pk = pk*10 + (*p++ - '0');
            if (p >= end || *p != '|') { while (p < end && *p != '\n') p++; if (p<end) p++; continue; }
            p++; // skip '|'
            // p_name field: up to next '|'
            const char* name = p;
            while (p < end && *p != '|' && *p != '\n') p++;
            size_t nlen = (size_t)(p - name);
            // Search for substring "green"
            if (nlen >= 5) {
                for (size_t i = 0; i + 5 <= nlen; i++) {
                    if (name[i]=='g' && name[i+1]=='r' && name[i+2]=='e' &&
                        name[i+3]=='e' && name[i+4]=='n') {
                        bit_set(part_bitset, pk);
                        break;
                    }
                }
            }
            // Advance to next line
            while (p < end && *p != '\n') p++;
            if (p < end) p++;
        }
        munmap((void*)buf, st.st_size);
    }

    // ============================================================
    // Phase D: Build partsupp composite map
    // Full scan of 8M partsupp rows, filter by part_bitset (~5.5% pass)
    // => ~440K entries; use 1M slot hash table (load ~0.42)
    // ============================================================
    {
        GENDB_PHASE("partsupp_build");
        uint32_t ps_slots = 1u << 20; // 1,048,576
        ps_mask = ps_slots - 1;
        ps_ht = new PSEntry[ps_slots](); // zero-initialized; key=0 is empty sentinel

        size_t n;
        const int32_t* ps_pk   = mmap_col<int32_t>(gendb_dir + "/partsupp/ps_partkey.bin",   n);
        const int32_t* ps_sk   = mmap_col<int32_t>(gendb_dir + "/partsupp/ps_suppkey.bin",   n);
        const int64_t* ps_cost = mmap_col<int64_t>(gendb_dir + "/partsupp/ps_supplycost.bin", n);
        madvise((void*)ps_pk,   n*4, MADV_SEQUENTIAL);
        madvise((void*)ps_sk,   n*4, MADV_SEQUENTIAL);
        madvise((void*)ps_cost, n*8, MADV_SEQUENTIAL);

        for (size_t i = 0; i < n; i++) {
            int32_t pk = ps_pk[i];
            if (!bit_test(part_bitset, pk)) continue;
            int64_t key = (int64_t)pk * 100001LL + ps_sk[i];
            ps_insert(key, ps_cost[i]);
        }
    }

    // ============================================================
    // Phase E: Build orders year flat array
    // o_orderkey -> int16_t year; 0 = absent
    // Flat array of 60M entries = 120MB (TPC-H SF10 max orderkey = 60M)
    // ============================================================
    orders_year = new int16_t[MAX_ORDERKEY](); // zero-initialized
    {
        GENDB_PHASE("orders_build");
        size_t n;
        const int32_t* o_ok   = mmap_col<int32_t>(gendb_dir + "/orders/o_orderkey.bin",  n);
        const int32_t* o_date = mmap_col<int32_t>(gendb_dir + "/orders/o_orderdate.bin", n);
        madvise((void*)o_ok,   n*4, MADV_SEQUENTIAL);
        madvise((void*)o_date, n*4, MADV_SEQUENTIAL);

        for (size_t i = 0; i < n; i++) {
            int32_t ok = o_ok[i];
            if (ok > 0 && ok < MAX_ORDERKEY)
                orders_year[ok] = (int16_t)gendb::extract_year(o_date[i]);
        }
    }

    // ============================================================
    // Phase F: Parallel lineitem scan — fused probe + aggregation
    // agg[nation_idx 0-24][year_offset 0-9]  (year 1992-2001)
    // Thread-local flat 2D arrays, zero contention during scan.
    // ============================================================
    static const int NTHREADS = 64;
    int64_t thread_agg[NTHREADS][25][10];
    memset(thread_agg, 0, sizeof(thread_agg));
    {
        GENDB_PHASE("main_scan");
        size_t n;
        const int32_t* l_ok   = mmap_col<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin",     n);
        const int32_t* l_pk   = mmap_col<int32_t>(gendb_dir + "/lineitem/l_partkey.bin",      n);
        const int32_t* l_sk   = mmap_col<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin",      n);
        const int64_t* l_qty  = mmap_col<int64_t>(gendb_dir + "/lineitem/l_quantity.bin",     n);
        const int64_t* l_ep   = mmap_col<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin",n);
        const int64_t* l_disc = mmap_col<int64_t>(gendb_dir + "/lineitem/l_discount.bin",     n);
        madvise((void*)l_ok,   n*4, MADV_SEQUENTIAL);
        madvise((void*)l_pk,   n*4, MADV_SEQUENTIAL);
        madvise((void*)l_sk,   n*4, MADV_SEQUENTIAL);
        madvise((void*)l_qty,  n*8, MADV_SEQUENTIAL);
        madvise((void*)l_ep,   n*8, MADV_SEQUENTIAL);
        madvise((void*)l_disc, n*8, MADV_SEQUENTIAL);

        #pragma omp parallel for schedule(static, 65536) num_threads(NTHREADS)
        for (size_t i = 0; i < n; i++) {
            int32_t pk = l_pk[i];
            // First gate: part bitset (~94.5% eliminated here)
            if (!bit_test(part_bitset, pk)) continue;

            int32_t sk = l_sk[i];
            // Composite partsupp lookup
            int64_t ps_key = (int64_t)pk * 100001LL + sk;
            int64_t cost   = ps_lookup(ps_key);
            if (cost == INT64_MIN) continue;

            // Year lookup via flat array
            int32_t ok = l_ok[i];
            if (ok <= 0 || ok >= MAX_ORDERKEY) continue;
            int16_t yr = orders_year[ok];
            if (yr < 1992 || yr > 2001) continue;

            // Compute scaled profit:
            // amount = l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity
            // All values are scaled by 100 (stored as actual×100)
            // profit_scaled = ep*(100-disc) - cost*qty  =>  actual_amount * 10000
            int64_t ep   = l_ep[i];
            int64_t disc = l_disc[i];
            int64_t qty  = l_qty[i];
            int64_t profit = ep * (100LL - disc) / 100 - cost * qty / 100;

            int tid = omp_get_thread_num();
            uint8_t ni = suppkey_to_nation[sk];
            int     yi = yr - 1992;
            thread_agg[tid][ni][yi] += profit;
        }
    }

    // ============================================================
    // Phase G: Merge thread-local aggregates and write output CSV
    // ============================================================
    {
        GENDB_PHASE("output");
        int64_t agg[25][10];
        memset(agg, 0, sizeof(agg));
        for (int t = 0; t < NTHREADS; t++)
            for (int ni = 0; ni < 25; ni++)
                for (int yi = 0; yi < 10; yi++)
                    agg[ni][yi] += thread_agg[t][ni][yi];

        struct Row { const std::string* nation; int year; int64_t profit; };
        std::vector<Row> rows;
        rows.reserve(175);
        for (int ni = 0; ni < 25; ni++)
            for (int yi = 0; yi < 10; yi++)
                if (agg[ni][yi] != 0)
                    rows.push_back({&nation_names[ni], 1992 + yi, agg[ni][yi]});

        // ORDER BY nation ASC, o_year DESC
        std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b){
            if (*a.nation != *b.nation) return *a.nation < *b.nation;
            return a.year > b.year;
        });

        std::string outpath = results_dir + "/Q9.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        if (!f) { perror(outpath.c_str()); exit(1); }
        fprintf(f, "nation,o_year,sum_profit\n");
        for (const auto& r : rows)
            fprintf(f, "%s,%d,%.4f\n", r.nation->c_str(), r.year,
                    (double)r.profit / 100.0);
        fclose(f);
    }

    delete[] ps_ht;   ps_ht = nullptr;
    delete[] orders_year; orders_year = nullptr;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q9(gendb_dir, results_dir);
    return 0;
}
#endif
