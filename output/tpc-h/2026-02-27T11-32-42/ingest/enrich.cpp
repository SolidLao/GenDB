// enrich.cpp — Adds pre-joined columns to accelerate Q9, Q18, Q3.
// Reads existing binary columns and produces derived columns.
// Usage: enrich <gendb_dir>
//
// Outputs:
//   li_sup_nat.bin   — int8_t[N]:   nationkey for each lineitem's supplier
//   li_order_year.bin — int16_t[N]: order year for each lineitem
//   li_ps_cost.bin   — double[N]:   supplycost for each lineitem from partsupp
//   li2_orderkey.bin — int32_t[N]:  lineitem orderkeys sorted by orderkey value
//   li2_qty.bin      — int8_t[N]:   quantities in the same sorted order
//   ord_maxkey.bin   — int64_t[1]:  max orderkey value

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <algorithm>
#include <thread>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>

using namespace std;

template<typename T>
static T* mmap_col(const char* dir, const char* name, size_t& count) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); exit(1); }
    struct stat st; fstat(fd, &st);
    count = st.st_size / sizeof(T);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    madvise(p, st.st_size, MADV_SEQUENTIAL);
    close(fd);
    return (T*)p;
}

template<typename T>
static void write_col(const char* dir, const char* name, const T* data, size_t n) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    FILE* f = fopen(path, "wb");
    if (!f) { perror(path); exit(1); }
    fwrite(data, sizeof(T), n, f);
    fclose(f);
}

int main(int argc, char** argv) {
    if (argc < 2) { fprintf(stderr, "Usage: enrich <gendb_dir>\n"); return 1; }
    const char* gendb_dir = argv[1];

    auto t0 = chrono::steady_clock::now();

    // Load lineitem columns
    size_t N;
    size_t tmp;
    int32_t* li_okey   = mmap_col<int32_t>(gendb_dir, "li_orderkey.bin", N);
    int32_t* li_pkey   = mmap_col<int32_t>(gendb_dir, "li_partkey.bin",  tmp);
    int32_t* li_skey   = mmap_col<int32_t>(gendb_dir, "li_suppkey.bin",  tmp);
    int8_t*  li_qty    = mmap_col<int8_t> (gendb_dir, "li_qty.bin",      tmp);
    fprintf(stderr, "Lineitem rows: %zu\n", N);

    // Load supplier nationkey array
    size_t sn_sz;
    int8_t* sup_nat_arr = mmap_col<int8_t>(gendb_dir, "sup_nat_arr.bin", sn_sz);

    // Load orders data
    size_t No;
    int32_t* ord_okey = mmap_col<int32_t>(gendb_dir, "ord_orderkey.bin",  No);
    int32_t* ord_odat = mmap_col<int32_t>(gendb_dir, "ord_orderdate.bin", tmp);
    fprintf(stderr, "Orders rows: %zu\n", No);

    // Load partsupp data
    size_t Nps;
    int32_t* ps_pk   = mmap_col<int32_t>(gendb_dir, "ps_partkey.bin",    Nps);
    int32_t* ps_sk   = mmap_col<int32_t>(gendb_dir, "ps_suppkey.bin",    tmp);
    double*  ps_cost = mmap_col<double> (gendb_dir, "ps_supplycost.bin", tmp);
    fprintf(stderr, "PartSupp rows: %zu\n", Nps);

    // ── 1. Build li_sup_nat.bin ───────────────────────────────────────────────
    {
        fprintf(stderr, "Building li_sup_nat.bin...\n");
        vector<int8_t> li_sup_nat(N);
        int NT = min((int)thread::hardware_concurrency(), 32);
        size_t chunk = (N + NT - 1) / NT;
        vector<thread> threads;
        for (int t = 0; t < NT; ++t) {
            size_t start = t * chunk, end = min(start + chunk, N);
            threads.emplace_back([&, start, end]() {
                for (size_t i = start; i < end; ++i) {
                    int32_t sk = li_skey[i];
                    li_sup_nat[i] = ((size_t)sk < sn_sz) ? sup_nat_arr[sk] : -1;
                }
            });
        }
        for (auto& th : threads) th.join();
        write_col(gendb_dir, "li_sup_nat.bin", li_sup_nat.data(), N);
        fprintf(stderr, "  li_sup_nat.bin done\n");
    }

    // ── 2. Build li_order_year.bin ────────────────────────────────────────────
    {
        fprintf(stderr, "Building li_order_year.bin...\n");
        // Build orderkey → year map (direct array if max orderkey is manageable)
        // Find max orderkey
        int32_t maxok = 0;
        for (size_t i = 0; i < No; ++i) if (ord_okey[i] > maxok) maxok = ord_okey[i];
        fprintf(stderr, "  max orderkey: %d\n", maxok);

        // Write max orderkey
        {
            char path[512]; snprintf(path, sizeof(path), "%s/ord_maxkey.bin", gendb_dir);
            FILE* f = fopen(path, "wb");
            int64_t mk = maxok;
            fwrite(&mk, sizeof(mk), 1, f);
            fclose(f);
        }

        // Build dense year array indexed by orderkey
        // Each year fits in int16_t (1992-1998 for TPC-H SF10)
        vector<int16_t> year_by_ok(maxok + 1, 0);
        // Parse year from orderdate (epoch days)
        // 1992: epoch days ~8035-8400, 1998: epoch days ~10227-10592
        // Year = floor((epoch_day + 719468 - some offset) / 365.25) approx
        // Better: use simple formula since dates are in known range
        // year = 1970 + epoch_day / 365.25 (approximate)
        // Or: store precomputed via simple lookup
        // epoch_day range for SF10: 8035-10592 (1992-1998)
        // year lookup: for epoch_day in 8035-8400: 1992, 8401-8766: 1993, etc.
        // Exact: 1992-01-01 = 8035, 1993-01-01 = 8401, 1994-01-01 = 8766,
        //        1995-01-01 = 9131, 1996-01-01 = 9496, 1997-01-01 = 9862,
        //        1998-01-01 = 10227, 1999-01-01 = 10592
        auto epoch_to_year = [](int32_t d) -> int16_t {
            if (d < 8401) return 1992;
            if (d < 8766) return 1993;
            if (d < 9131) return 1994;
            if (d < 9496) return 1995;
            if (d < 9862) return 1996;
            if (d < 10227) return 1997;
            if (d < 10592) return 1998;
            // More general: iterate
            int y = 1999;
            int ystart = 10592;
            while (d >= ystart + (((y%4==0&&y%100!=0)||(y%400==0)) ? 366 : 365)) {
                ystart += (((y%4==0&&y%100!=0)||(y%400==0)) ? 366 : 365);
                y++;
            }
            return (int16_t)y;
        };

        for (size_t i = 0; i < No; ++i)
            year_by_ok[ord_okey[i]] = epoch_to_year(ord_odat[i]);

        vector<int16_t> li_order_year(N);
        int NT = min((int)thread::hardware_concurrency(), 32);
        size_t chunk = (N + NT - 1) / NT;
        vector<thread> threads;
        for (int t = 0; t < NT; ++t) {
            size_t start = t * chunk, end = min(start + chunk, N);
            threads.emplace_back([&, start, end]() {
                for (size_t i = start; i < end; ++i)
                    li_order_year[i] = year_by_ok[li_okey[i]];
            });
        }
        for (auto& th : threads) th.join();
        write_col(gendb_dir, "li_order_year.bin", li_order_year.data(), N);
        fprintf(stderr, "  li_order_year.bin done\n");
    }

    // ── 3. Build li_ps_cost.bin ───────────────────────────────────────────────
    {
        fprintf(stderr, "Building li_ps_cost.bin...\n");
        // Build hash map: (partkey << 17) | suppkey -> supplycost
        // suppkey max = 100000 < 2^17 = 131072
        // Use open-addressing hash map
        int cap = 1;
        while (cap < (int)Nps * 2) cap <<= 1;
        vector<int64_t> ps_keys(cap, INT64_MIN);
        vector<double>  ps_vals(cap, 0.0);
        for (size_t i = 0; i < Nps; ++i) {
            int64_t k = ((int64_t)ps_pk[i] << 17) | (int64_t)ps_sk[i];
            int h = (int)(((uint64_t)k * 6364136223846793005ULL) >> 32) & (cap - 1);
            while (ps_keys[h] != INT64_MIN) h = (h + 1) & (cap - 1);
            ps_keys[h] = k; ps_vals[h] = ps_cost[i];
        }
        fprintf(stderr, "  PSmap built (cap=%d)\n", cap);

        vector<double> li_ps_cost(N, 0.0);
        int NT = min((int)thread::hardware_concurrency(), 32);
        size_t chunk = (N + NT - 1) / NT;
        vector<thread> threads;
        for (int t = 0; t < NT; ++t) {
            size_t start = t * chunk, end = min(start + chunk, N);
            threads.emplace_back([&, start, end]() {
                for (size_t i = start; i < end; ++i) {
                    int64_t k = ((int64_t)li_pkey[i] << 17) | (int64_t)li_skey[i];
                    int h = (int)(((uint64_t)k * 6364136223846793005ULL) >> 32) & (cap - 1);
                    while (ps_keys[h] != INT64_MIN && ps_keys[h] != k) h = (h + 1) & (cap - 1);
                    if (ps_keys[h] == k) li_ps_cost[i] = ps_vals[h];
                }
            });
        }
        for (auto& th : threads) th.join();
        write_col(gendb_dir, "li_ps_cost.bin", li_ps_cost.data(), N);
        fprintf(stderr, "  li_ps_cost.bin done\n");
    }

    // ── 4. Build li2_orderkey.bin and li2_qty.bin (sorted by orderkey) ────────
    {
        fprintf(stderr, "Building secondary sort by orderkey...\n");
        // Counting sort by orderkey (values in [1, maxok])
        // First, get max orderkey from lineitem
        int32_t maxok_li = 0;
        for (size_t i = 0; i < N; ++i) if (li_okey[i] > maxok_li) maxok_li = li_okey[i];
        fprintf(stderr, "  max li_orderkey: %d\n", maxok_li);

        int range = maxok_li + 1;
        vector<int32_t> cnt(range, 0);
        for (size_t i = 0; i < N; ++i) cnt[li_okey[i]]++;
        // prefix sum
        vector<int32_t> off(range, 0);
        for (int i = 1; i < range; ++i) off[i] = off[i-1] + cnt[i-1];

        // Write sorted output
        vector<int32_t> li2_okey(N);
        vector<int8_t>  li2_qty(N);
        vector<int32_t> pos = off; // working positions
        for (size_t i = 0; i < N; ++i) {
            int32_t ok = li_okey[i];
            int p = pos[ok]++;
            li2_okey[p] = ok;
            li2_qty[p]  = li_qty[i];
        }
        write_col(gendb_dir, "li2_orderkey.bin", li2_okey.data(), N);
        write_col(gendb_dir, "li2_qty.bin",      li2_qty.data(),  N);
        fprintf(stderr, "  li2 (orderkey-sorted) done\n");
    }

    double ms = chrono::duration<double, milli>(
        chrono::steady_clock::now() - t0).count();
    printf("Enrichment complete in %.2f ms\n", ms);
    return 0;
}
