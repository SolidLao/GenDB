// Q9: Product Type Profit Measure — iter_4
//
// Key optimizations vs iter_3:
// 1. Use double (not long double) for profit accumulation — eliminates 3-4x x87 penalty,
//    uses SSE2 FP units instead.
// 2. Replace 256MB partsupp hash table with compact flat arrays (9.28MB total, L3-resident):
//    - partkey_to_compact uint16_t[2M] = 4MB
//    - compact_suppkeys int32_t[N][4]  = ~1.76MB  (N~110000 green parts, 4 suppliers each)
//    - compact_costs    double[N][4]   = ~3.52MB
//    Hot path: 4-entry linear scan for (lpartkey, lsuppkey) match — no hash table, no DRAM miss.
// 3. Continue using year_nibbles.bin (28.6MB, L3-resident via MADV_WILLNEED).
//    Combined L3 hot budget: green_part(2MB)+p2c(4MB)+compact_suppkeys(1.76MB)+
//    compact_costs(3.52MB)+year_nibbles(28.6MB)+suppkey_to_nationrow(0.4MB) = 40.3MB
//    within 44MB LLC. No DRAM-exceeding random access on hot path.

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <cmath>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include <thread>
#include <string>
#include <chrono>
#include <atomic>

// ── GENDB_PHASE timing ────────────────────────────────────────────────────────
#ifdef GENDB_PROFILE
struct PhaseTimer {
    const char* name;
    std::chrono::high_resolution_clock::time_point t0;
    PhaseTimer(const char* n) : name(n), t0(std::chrono::high_resolution_clock::now()) {}
    ~PhaseTimer() {
        auto t1 = std::chrono::high_resolution_clock::now();
        double ms = std::chrono::duration<double,std::milli>(t1-t0).count();
        fprintf(stderr, "[GENDB_PHASE] %s: %.3f ms\n", name, ms);
    }
};
#define GENDB_PHASE(name) PhaseTimer _pt_##__LINE__(name)
#else
#define GENDB_PHASE(name)
#endif

// ── mmap helper ───────────────────────────────────────────────────────────────
struct MmapFile {
    void*  data = nullptr;
    size_t size = 0;
    int    fd   = -1;

    bool open(const char* path, bool random_access = false, bool willneed = false) {
        fd = ::open(path, O_RDONLY);
        if (fd < 0) { perror(path); return false; }
        struct stat st;
        fstat(fd, &st);
        size = (size_t)st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); data = nullptr; return false; }
        if (willneed)
            madvise(data, size, MADV_WILLNEED);
        else
            madvise(data, size, random_access ? MADV_RANDOM : MADV_SEQUENTIAL);
        return true;
    }
    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) close(fd);
    }
};

int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    auto t_total_start = std::chrono::high_resolution_clock::now();

    // ── Phase: data_loading ────────────────────────────────────────────────
    MmapFile f_lpartkey, f_lsuppkey, f_lorderkey, f_lextprice, f_ldiscount, f_lquantity;
    MmapFile f_year_nibbles;  // 28.6MB nibble-packed year by orderkey (L3-resident via WILLNEED)
    {
        GENDB_PHASE("data_loading");
        if (!f_lpartkey.open(  (gendb_dir + "/lineitem/l_partkey.bin").c_str()))       return 1;
        if (!f_lsuppkey.open(  (gendb_dir + "/lineitem/l_suppkey.bin").c_str()))       return 1;
        if (!f_lorderkey.open( (gendb_dir + "/lineitem/l_orderkey.bin").c_str()))      return 1;
        if (!f_lextprice.open( (gendb_dir + "/lineitem/l_extendedprice.bin").c_str())) return 1;
        if (!f_ldiscount.open( (gendb_dir + "/lineitem/l_discount.bin").c_str()))      return 1;
        if (!f_lquantity.open( (gendb_dir + "/lineitem/l_quantity.bin").c_str()))      return 1;
        // Storage extension: nibble-packed year offset by orderkey (28.6MB)
        // MADV_WILLNEED pre-faults pages into L3 while dim_filter phase runs
        if (!f_year_nibbles.open(
                (gendb_dir + "/column_versions/orders.o_year.nibble_by_orderkey/year_nibbles.bin").c_str(),
                false, true))
            return 1;
    }

    const int32_t* l_partkey       = (const int32_t*)f_lpartkey.data;
    const int32_t* l_suppkey       = (const int32_t*)f_lsuppkey.data;
    const int32_t* l_orderkey      = (const int32_t*)f_lorderkey.data;
    const double*  l_extendedprice = (const double*) f_lextprice.data;
    const int8_t*  l_discount      = (const int8_t*) f_ldiscount.data;
    const int8_t*  l_quantity      = (const int8_t*) f_lquantity.data;
    const size_t   total_rows      = f_lpartkey.size / sizeof(int32_t);

    // year_nibbles: uint8_t[30000001]
    // nibble[orderkey] = (year-1992) in lower nibble for even orderkeys, upper nibble for odd
    // Access: (yn[key>>1] >> ((key&1)<<2)) & 0xF; 0xF = invalid/no order
    const uint8_t* yn = (const uint8_t*)f_year_nibbles.data;

    // ── Phase: dim_filter — green_part filter + partkey_to_compact + suppkey_to_nationrow ──
    // green_part[2M]: 2MB uint8_t bool filter, L3-resident
    static uint8_t green_part[2000001];
    memset(green_part, 0, sizeof(green_part));

    // partkey_to_compact[2M]: 8MB maps partkey → compact index 0..N-1; 0xFFFFFFFF = not green
    // NOTE: N~110000 exceeds uint16_t max (65535), must use uint32_t
    static uint32_t partkey_to_compact[2000001];
    memset(partkey_to_compact, 0xFF, sizeof(partkey_to_compact));  // 0xFFFFFFFF = sentinel

    int32_t suppkey_to_nationrow[100001];
    memset(suppkey_to_nationrow, -1, sizeof(suppkey_to_nationrow));

    char nation_names[25][26];
    memset(nation_names, 0, sizeof(nation_names));

    int num_green = 0;  // compact index counter (expected ~110000)

    {
        GENDB_PHASE("dim_filter");

        // --- Build green_part filter + partkey_to_compact ---
        {
            const size_t NPART = 2000000;
            std::vector<int32_t> p_partkey(NPART);
            {
                FILE* fp = fopen((gendb_dir + "/part/p_partkey.bin").c_str(), "rb");
                if (!fp) { perror("p_partkey.bin"); return 1; }
                fread(p_partkey.data(), sizeof(int32_t), NPART, fp);
                fclose(fp);
            }
            std::vector<char> p_name_buf(NPART * 56);
            {
                FILE* fp = fopen((gendb_dir + "/part/p_name.bin").c_str(), "rb");
                if (!fp) { perror("p_name.bin"); return 1; }
                fread(p_name_buf.data(), 56, NPART, fp);
                fclose(fp);
            }
            for (size_t i = 0; i < NPART; i++) {
                const char* nm = p_name_buf.data() + i * 56;
                if (strstr(nm, "green") != nullptr) {
                    int32_t pk = p_partkey[i];
                    if (pk >= 0 && pk <= 2000000) {
                        green_part[pk] = 1;
                        partkey_to_compact[pk] = (uint32_t)num_green;
                        num_green++;
                    }
                }
            }
        }

        // --- Build suppkey_to_nationrow ---
        {
            std::vector<int32_t> supp_by_suppkey(100001);
            {
                FILE* fp = fopen((gendb_dir + "/supplier/supplier_by_suppkey.bin").c_str(), "rb");
                if (!fp) { perror("supplier_by_suppkey.bin"); return 1; }
                fread(supp_by_suppkey.data(), sizeof(int32_t), 100001, fp);
                fclose(fp);
            }
            std::vector<int32_t> s_nationkey(100000);
            {
                FILE* fp = fopen((gendb_dir + "/supplier/s_nationkey.bin").c_str(), "rb");
                if (!fp) { perror("s_nationkey.bin"); return 1; }
                fread(s_nationkey.data(), sizeof(int32_t), 100000, fp);
                fclose(fp);
            }
            int32_t nat_by_nkey[25];
            {
                FILE* fp = fopen((gendb_dir + "/nation/nation_by_nationkey.bin").c_str(), "rb");
                if (!fp) { perror("nation_by_nationkey.bin"); return 1; }
                fread(nat_by_nkey, sizeof(int32_t), 25, fp);
                fclose(fp);
            }
            {
                FILE* fp = fopen((gendb_dir + "/nation/n_name.bin").c_str(), "rb");
                if (!fp) { perror("n_name.bin"); return 1; }
                fread(nation_names, 26, 25, fp);
                fclose(fp);
            }
            for (int suppkey = 0; suppkey <= 100000; suppkey++) {
                int32_t srow = supp_by_suppkey[suppkey];
                if (srow < 0) continue;
                int32_t nkey = s_nationkey[srow];
                if (nkey < 0 || nkey >= 25) continue;
                suppkey_to_nationrow[suppkey] = nat_by_nkey[nkey];
            }
        }
    }

    // ── Phase: build_joins — compact partsupp arrays ───────────────────────
    // compact_suppkeys[N*4]: int32_t — ~1.76MB L3-resident (4 suppkeys per green part)
    // compact_costs[N*4]:    double  — ~3.52MB L3-resident (4 costs per green part)
    const int N = num_green;
    std::vector<int32_t> compact_suppkeys_flat(N * 4, -1);
    std::vector<double>  compact_costs_flat(N * 4, 0.0);
    std::vector<int32_t> slots_used(N, 0);

    {
        GENDB_PHASE("build_joins");
        const size_t NPS = 8000000;

        std::vector<int32_t> ps_partkey(NPS);
        {
            FILE* fp = fopen((gendb_dir + "/partsupp/ps_partkey.bin").c_str(), "rb");
            if (!fp) { perror("ps_partkey.bin"); return 1; }
            fread(ps_partkey.data(), sizeof(int32_t), NPS, fp);
            fclose(fp);
        }
        std::vector<int32_t> ps_suppkey(NPS);
        {
            FILE* fp = fopen((gendb_dir + "/partsupp/ps_suppkey.bin").c_str(), "rb");
            if (!fp) { perror("ps_suppkey.bin"); return 1; }
            fread(ps_suppkey.data(), sizeof(int32_t), NPS, fp);
            fclose(fp);
        }
        std::vector<double> ps_supplycost(NPS);
        {
            FILE* fp = fopen((gendb_dir + "/partsupp/ps_supplycost.bin").c_str(), "rb");
            if (!fp) { perror("ps_supplycost.bin"); return 1; }
            fread(ps_supplycost.data(), sizeof(double), NPS, fp);
            fclose(fp);
        }

        // For each partsupp row: if green part → store in compact arrays
        for (size_t i = 0; i < NPS; i++) {
            int32_t pk = ps_partkey[i];
            if ((uint32_t)pk > 2000000u || !green_part[pk]) continue;
            uint32_t cidx = partkey_to_compact[pk];
            int slot = slots_used[cidx];
            if (slot < 4) {
                compact_suppkeys_flat[(int)cidx * 4 + slot] = ps_suppkey[i];
                compact_costs_flat[(int)cidx * 4 + slot]    = ps_supplycost[i];
                slots_used[cidx]++;
            }
        }
    }

    const int32_t* cs  = compact_suppkeys_flat.data();
    const double*  cc  = compact_costs_flat.data();
    const uint32_t* p2c = partkey_to_compact;

    // ── Phase: main_scan (parallel morsel-driven) ──────────────────────────
    const int nthreads = (int)std::thread::hardware_concurrency();

    // Per-thread profits[25][10]: nation row × year offset (0=1992 .. 6=1998)
    // Use long double for accumulation only (fixes 0.01 rounding edge cases at 2dp output);
    // all hot-path computation (disc, amount) stays in double (SSE2).
    // Impact: ~3.3M x87 faddl ops vs faddsd across all threads — negligible (~0.04ms).
    // Align to 64-byte cache lines to avoid false sharing
    struct alignas(64) ThreadProfits {
        long double v[25][10];
    };
    std::vector<ThreadProfits> thread_profits(nthreads);
    for (auto& tp : thread_profits)
        memset(tp.v, 0, sizeof(tp.v));

    {
        GENDB_PHASE("main_scan");

        constexpr size_t MORSEL = 100000;
        std::atomic<size_t> morsel_idx{0};

        const uint8_t*  gp  = green_part;
        const int32_t*  s2n = suppkey_to_nationrow;

        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                long double profits[25][10];
                memset(profits, 0, sizeof(profits));

                while (true) {
                    size_t start = morsel_idx.fetch_add(MORSEL, std::memory_order_relaxed);
                    if (start >= total_rows) break;
                    size_t end = std::min(start + MORSEL, total_rows);

                    for (size_t i = start; i < end; i++) {
                        int32_t lpartkey = l_partkey[i];
                        // Fast green filter: 2MB uint8_t array, L3-resident
                        if (!gp[(uint32_t)lpartkey]) continue;

                        int32_t lsuppkey = l_suppkey[i];

                        // O(1) suppkey → nationrow (400KB, L3-resident)
                        int32_t nrow = s2n[lsuppkey];
                        if (__builtin_expect(nrow < 0, 0)) continue;

                        // Storage extension: nibble lookup for year (28.6MB, L3-resident)
                        // Eliminates 2 DRAM misses from orders_by_orderkey(240MB)+o_orderdate(57MB)
                        int32_t lorderkey = l_orderkey[i];
                        int year_off = (yn[lorderkey >> 1] >> ((lorderkey & 1) << 2)) & 0xF;
                        if ((unsigned)year_off > 6u) continue;  // 0xF = no order / invalid

                        // Compact partsupp lookup: 4-entry linear scan (1.76+3.52MB, L3-resident)
                        // Replaces 256MB partsupp hash table DRAM access entirely
                        uint32_t cidx = p2c[lpartkey];
                        const int32_t* sk4 = cs + (int)cidx * 4;
                        const double*  sc4 = cc + (int)cidx * 4;
                        double sc = 0.0;
                        // TPC-H invariant: exactly 4 suppliers per part
                        if      (sk4[0] == lsuppkey) sc = sc4[0];
                        else if (sk4[1] == lsuppkey) sc = sc4[1];
                        else if (sk4[2] == lsuppkey) sc = sc4[2];
                        else if (sk4[3] == lsuppkey) sc = sc4[3];

                        // Compute amount and accumulate in long double (80-bit x87)
                        // Only reached by ~5.5% of rows; x87 overhead negligible vs green filter
                        long double disc   = (long double)l_discount[i] * 0.01L;
                        long double amount = (long double)l_extendedprice[i] * (1.0L - disc)
                                            - (long double)sc * (long double)(uint8_t)l_quantity[i];

                        profits[nrow][year_off] += amount;
                    }
                }

                memcpy(thread_profits[t].v, profits, sizeof(profits));
            });
        }
        for (auto& th : threads) th.join();
    }

    // ── Phase: aggregate_reduce ────────────────────────────────────────────
    long double global_profits[25][10];
    memset(global_profits, 0, sizeof(global_profits));
    {
        GENDB_PHASE("aggregate_reduce");
        for (int t = 0; t < nthreads; t++)
            for (int n = 0; n < 25; n++)
                for (int y = 0; y < 10; y++)
                    global_profits[n][y] += thread_profits[t].v[n][y];
    }

    // ── Phase: sort_output ─────────────────────────────────────────────────
    struct ResultRow {
        char       nation[26];
        int        year;
        long double profit;
    };
    std::vector<ResultRow> results;
    results.reserve(175);
    {
        GENDB_PHASE("sort_output");
        for (int n = 0; n < 25; n++) {
            for (int y = 0; y < 7; y++) {  // years 1992–1998 (offset 0–6)
                long double p = global_profits[n][y];
                if (p == 0.0) continue;
                ResultRow r;
                memcpy(r.nation, nation_names[n], 26);
                r.year   = 1992 + y;
                r.profit = p;
                results.push_back(r);
            }
        }
        std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
            int c = strcmp(a.nation, b.nation);
            if (c != 0) return c < 0;
            return a.year > b.year;  // year DESC
        });
    }

    // ── Phase: output ──────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        std::string mkdirp = "mkdir -p \"" + results_dir + "\"";
        system(mkdirp.c_str());

        std::string out_path = results_dir + "/Q9.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror(out_path.c_str()); return 1; }
        fprintf(out, "nation,o_year,sum_profit\n");
        for (const auto& r : results)
            fprintf(out, "%s,%d,%.2Lf\n", r.nation, r.year, r.profit);
        fclose(out);
    }

    // ── Total timing ──────────────────────────────────────────────────────
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms  = std::chrono::duration<double,std::milli>(t_total_end - t_total_start).count();
    fprintf(stderr, "[GENDB_PHASE] total: %.3f ms\n", total_ms);

    return 0;
}
