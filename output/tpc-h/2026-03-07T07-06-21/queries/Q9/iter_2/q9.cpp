// Q9: Product Type Profit Measure — iter_2
// Optimizations vs iter_1:
//   1. Parallel dim_filter: build green_part AND year_by_orderrow using all nthreads
//      (iter_0/1 ran single-threaded fread for p_name.bin: 107MB serial bottleneck)
//   2. year_by_orderrow uint8_t[15000001] indexed by ORDER ROW (15MB, L3-resident)
//      vs iter_1's okey_yearoff[60M] by orderkey (60MB, exceeds 44MB LLC)
//   3. Pre-built partsupp_hash_index mmap'd, zero runtime build cost
//      vs iter_1's compact HT that added ~121MB I/O build overhead
//   4. Software prefetch on orders_by_orderkey (240MB) to hide DRAM latency

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

    bool open(const char* path, bool random_access = false) {
        fd = ::open(path, O_RDONLY);
        if (fd < 0) { perror(path); return false; }
        struct stat st;
        fstat(fd, &st);
        size = (size_t)st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); data = nullptr; return false; }
        madvise(data, size, random_access ? MADV_RANDOM : MADV_SEQUENTIAL);
        return true;
    }
    void close_file() {
        if (data && data != MAP_FAILED) { munmap(data, size); data = nullptr; }
        if (fd >= 0) { ::close(fd); fd = -1; }
        size = 0;
    }
    ~MmapFile() { close_file(); }
};

// ── Pre-built partsupp hash index layout ─────────────────────────────────────
// File: [uint64_t capacity][uint64_t count][HTSlot × capacity]
// Slot: {uint64_t key; int32_t row_idx; int32_t pad;} = 16 bytes
// Hash: Fibonacci hashing, LOG2_CAP=24, FIB_CONST=11400714819323198485ULL
// Empty sentinel: key == 0ULL
#pragma pack(push, 1)
struct HTSlot {
    uint64_t key;
    int32_t  row_idx;
    int32_t  pad;
};
#pragma pack(pop)
static_assert(sizeof(HTSlot) == 16, "HTSlot must be 16 bytes");

static constexpr uint32_t PS_LOG2_CAP = 24;
static constexpr uint64_t PS_CAPACITY = 1ULL << PS_LOG2_CAP;
static constexpr uint64_t PS_MASK     = PS_CAPACITY - 1ULL;
static constexpr uint64_t FIB_CONST   = 11400714819323198485ULL;

inline double lookup_ps(int32_t partkey, int32_t suppkey,
                        const HTSlot* ht, const double* ps_cost) {
    uint64_t key  = (uint64_t)partkey * 200001ULL + (uint64_t)suppkey;
    uint64_t slot = (key * FIB_CONST) >> (64 - PS_LOG2_CAP);
    while (ht[slot].key != 0ULL) {
        if (ht[slot].key == key)
            return ps_cost[ht[slot].row_idx];
        slot = (slot + 1) & PS_MASK;
    }
    return 0.0;
}

// ── days-since-epoch → Gregorian year ────────────────────────────────────────
inline int extract_year(int32_t days) {
    int32_t z   = days + 719468;
    int32_t era = (z >= 0 ? z : z - 146096) / 146097;
    int32_t doe = z - era * 146097;
    int32_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int32_t y   = yoe + era * 400;
    int32_t doy = doe - (365*yoe + yoe/4 - yoe/100);
    if (doy >= 306) y++;
    return (int)y;
}

// ── green_part filter: uint8_t[2000001], 2MB, L3-resident ────────────────────
static uint8_t green_part[2000001];

// ── year_by_orderrow: uint8_t[15000001], 15MB, L3-resident ───────────────────
// Indexed by ORDER ROW index (not by orderkey). Stores (year - 1992) as uint8_t.
// 0xFF = invalid/sentinel (out-of-range year).
static uint8_t year_by_orderrow[15000001];

// ── suppkey_to_nationrow: int32_t[100001], 400KB, L3-resident ────────────────
static int32_t suppkey_to_nationrow[100001];

// ── nation names: char[25][26] ────────────────────────────────────────────────
static char nation_names[25][26];

int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    auto t_total_start = std::chrono::high_resolution_clock::now();

    // ── Phase: data_loading — mmap lineitem columns ────────────────────────
    MmapFile f_lpartkey, f_lsuppkey, f_lorderkey, f_lextprice, f_ldiscount, f_lquantity;
    {
        GENDB_PHASE("data_loading");
        if (!f_lpartkey.open( (gendb_dir + "/lineitem/l_partkey.bin").c_str()))       return 1;
        if (!f_lsuppkey.open( (gendb_dir + "/lineitem/l_suppkey.bin").c_str()))       return 1;
        if (!f_lorderkey.open((gendb_dir + "/lineitem/l_orderkey.bin").c_str()))      return 1;
        if (!f_lextprice.open((gendb_dir + "/lineitem/l_extendedprice.bin").c_str())) return 1;
        if (!f_ldiscount.open((gendb_dir + "/lineitem/l_discount.bin").c_str()))      return 1;
        if (!f_lquantity.open((gendb_dir + "/lineitem/l_quantity.bin").c_str()))      return 1;
    }

    const int32_t* l_partkey       = (const int32_t*)f_lpartkey.data;
    const int32_t* l_suppkey       = (const int32_t*)f_lsuppkey.data;
    const int32_t* l_orderkey      = (const int32_t*)f_lorderkey.data;
    const double*  l_extendedprice = (const double*) f_lextprice.data;
    const int8_t*  l_discount      = (const int8_t*) f_ldiscount.data;
    const int8_t*  l_quantity      = (const int8_t*) f_lquantity.data;
    const size_t   total_rows      = f_lpartkey.size / sizeof(int32_t);

    // ── Phase: dim_filter ─────────────────────────────────────────────────
    // Parallel build of green_part + year_by_orderrow using all threads.
    // Single-threaded: suppkey_to_nationrow, nation_names (trivial size).
    memset(green_part, 0, sizeof(green_part));
    memset(year_by_orderrow, 0xFF, sizeof(year_by_orderrow));
    memset(suppkey_to_nationrow, -1, sizeof(suppkey_to_nationrow));
    memset(nation_names, 0, sizeof(nation_names));

    // orders_by_orderkey is needed in both dim_filter (for year_by_orderrow build)
    // and main_scan. Keep it mmap'd across both phases.
    MmapFile f_orders_by_orderkey;

    {
        GENDB_PHASE("dim_filter");

        // mmap dimension files needed for parallel build
        MmapFile f_p_name, f_p_partkey, f_o_orderdate;
        if (!f_p_name.open(   (gendb_dir + "/part/p_name.bin").c_str()))          return 1;
        if (!f_p_partkey.open((gendb_dir + "/part/p_partkey.bin").c_str()))        return 1;
        if (!f_o_orderdate.open((gendb_dir + "/orders/o_orderdate.bin").c_str()))  return 1;
        if (!f_orders_by_orderkey.open(
                (gendb_dir + "/orders/orders_by_orderkey.bin").c_str(), true))     return 1;

        const char*    p_name_data      = (const char*)   f_p_name.data;
        const int32_t* p_partkey_data   = (const int32_t*)f_p_partkey.data;
        const int32_t* o_orderdate_data = (const int32_t*)f_o_orderdate.data;

        constexpr size_t NPART  = 2000000;
        constexpr size_t NORDER = 15000000;

        const int nthreads = (int)std::thread::hardware_concurrency();

        // --- Parallel: each thread builds its chunk of green_part + year_by_orderrow ---
        // Safe for green_part: p_partkey is PK → unique write indices (no conflicts)
        // Safe for year_by_orderrow: indexed by order row → each row written exactly once
        {
            std::vector<std::thread> threads;
            threads.reserve(nthreads);
            for (int t = 0; t < nthreads; t++) {
                threads.emplace_back([&, t, nthreads]() {
                    // Part chunk: scan p_name for 'green', mark green_part[]
                    const size_t part_begin = (size_t)t * NPART / nthreads;
                    const size_t part_end   = (size_t)(t + 1) * NPART / nthreads;
                    for (size_t i = part_begin; i < part_end; i++) {
                        const char* nm = p_name_data + i * 56;
                        if (strstr(nm, "green") != nullptr) {
                            int32_t pk = p_partkey_data[i];
                            if ((uint32_t)pk <= 2000000u)
                                green_part[pk] = 1;
                        }
                    }
                    // Order chunk: compute year_off, store in year_by_orderrow indexed by row
                    const size_t ord_begin = (size_t)t * NORDER / nthreads;
                    const size_t ord_end   = (size_t)(t + 1) * NORDER / nthreads;
                    for (size_t r = ord_begin; r < ord_end; r++) {
                        int year_off = extract_year(o_orderdate_data[r]) - 1992;
                        if ((unsigned)year_off <= 6u)
                            year_by_orderrow[r] = (uint8_t)year_off;
                        // else: leave as 0xFF sentinel
                    }
                });
            }
            for (auto& th : threads) th.join();
        }

        // --- Single-threaded: suppkey_to_nationrow + nation_names (400KB + 650B) ---
        {
            int32_t supp_by_suppkey[100001];
            {
                FILE* fp = fopen((gendb_dir + "/supplier/supplier_by_suppkey.bin").c_str(), "rb");
                if (!fp) { perror("supplier_by_suppkey.bin"); return 1; }
                fread(supp_by_suppkey, sizeof(int32_t), 100001, fp);
                fclose(fp);
            }
            int32_t s_nationkey_arr[100000];
            {
                FILE* fp = fopen((gendb_dir + "/supplier/s_nationkey.bin").c_str(), "rb");
                if (!fp) { perror("s_nationkey.bin"); return 1; }
                fread(s_nationkey_arr, sizeof(int32_t), 100000, fp);
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
            // suppkey_to_nationrow[sk] = row index into n_name.bin for that suppkey's nation
            for (int suppkey = 0; suppkey <= 100000; suppkey++) {
                int32_t srow = supp_by_suppkey[suppkey];
                if (srow < 0) continue;
                int32_t nkey = s_nationkey_arr[srow];
                if ((unsigned)nkey >= 25u) continue;
                suppkey_to_nationrow[suppkey] = nat_by_nkey[nkey];
            }
        }
        // f_p_name, f_p_partkey, f_o_orderdate unmapped on scope exit
    }

    // ── Phase: build_joins — mmap pre-built partsupp hash index ──────────
    // No runtime HT build. Pre-built file is already on disk.
    MmapFile f_ps_ht, f_ps_supplycost;
    const HTSlot* ps_ht         = nullptr;
    const double* ps_supplycost = nullptr;
    {
        GENDB_PHASE("build_joins");
        if (!f_ps_ht.open((gendb_dir + "/partsupp/partsupp_hash_index.bin").c_str(), true))
            return 1;
        if (!f_ps_supplycost.open((gendb_dir + "/partsupp/ps_supplycost.bin").c_str(), true))
            return 1;
        // File layout: [uint64_t capacity][uint64_t count][HTSlot × capacity]
        const uint8_t* raw = (const uint8_t*)f_ps_ht.data;
        uint64_t ht_cap = *(const uint64_t*)raw;
        if (ht_cap != PS_CAPACITY) {
            fprintf(stderr, "Error: partsupp_hash_index capacity %llu != expected %llu\n",
                    (unsigned long long)ht_cap, (unsigned long long)PS_CAPACITY);
            return 1;
        }
        ps_ht         = (const HTSlot*)(raw + 16);
        ps_supplycost = (const double*)f_ps_supplycost.data;
    }

    // ── Phase: main_scan (parallel morsel-driven over ~60M lineitem rows) ─
    const int nthreads = (int)std::thread::hardware_concurrency();
    const int32_t* oby_okey = (const int32_t*)f_orders_by_orderkey.data;

    // Per-thread profits[25][10]: [nation_row][year_offset]
    // alignas(64) prevents false sharing between threads
    struct alignas(64) ThreadProfits {
        long double v[25][10];
    };
    std::vector<ThreadProfits> thread_profits(nthreads);
    for (auto& tp : thread_profits)
        memset(tp.v, 0, sizeof(tp.v));

    {
        GENDB_PHASE("main_scan");

        constexpr size_t MORSEL        = 100000;
        constexpr int    PREFETCH_DIST = 20;  // prefetch orders_by_orderkey 20 rows ahead
        std::atomic<size_t> morsel_counter{0};

        // Capture L3-resident arrays as raw pointers for thread lambda
        const uint8_t* gp         = green_part;
        const int32_t* s2n        = suppkey_to_nationrow;
        const uint8_t* yr_by_orow = year_by_orderrow;

        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                long double profits[25][10];
                memset(profits, 0, sizeof(profits));

                while (true) {
                    size_t start = morsel_counter.fetch_add(MORSEL, std::memory_order_relaxed);
                    if (start >= total_rows) break;
                    size_t end = std::min(start + MORSEL, total_rows);

                    for (size_t i = start; i < end; i++) {
                        // Prefetch orders_by_orderkey[l_orderkey[i+DIST]] to hide 240MB DRAM latency.
                        // Issued unconditionally (cheap: l_orderkey sequential → L1 hit).
                        if (i + PREFETCH_DIST < end)
                            __builtin_prefetch(&oby_okey[l_orderkey[i + PREFETCH_DIST]], 0, 0);

                        // ── Filter: green-part (2MB, L3-resident) ─────────────────────────
                        int32_t lpartkey = l_partkey[i];
                        if (!gp[(uint32_t)lpartkey]) continue;  // ~94.5% reject

                        // ── Suppkey → nation row (400KB, L3-resident) ─────────────────────
                        int32_t lsuppkey = l_suppkey[i];
                        int32_t nrow = s2n[lsuppkey];
                        if (__builtin_expect(nrow < 0, 0)) continue;

                        // ── l_orderkey → order row (240MB, DRAM, prefetched above) ─────────
                        int32_t orow = oby_okey[l_orderkey[i]];
                        if (__builtin_expect(orow < 0, 0)) continue;

                        // ── Order row → year offset (15MB, L3-resident) ───────────────────
                        int year_off = (int)yr_by_orow[(uint32_t)orow];
                        if ((unsigned)year_off >= 7u) continue;

                        // ── Partsupp lookup (256MB HT + 61MB costs, DRAM random) ──────────
                        double sc = lookup_ps(lpartkey, lsuppkey, ps_ht, ps_supplycost);

                        // ── Compute amount and accumulate ─────────────────────────────────
                        double disc   = (double)l_discount[i] * 0.01;
                        double amount = l_extendedprice[i] * (1.0 - disc)
                                        - sc * (double)(uint8_t)l_quantity[i];
                        profits[nrow][year_off] += (long double)amount;
                    }
                }

                // Copy thread-local results to global array
                memcpy(thread_profits[t].v, profits, sizeof(profits));
            });
        }
        for (auto& th : threads) th.join();
    }

    // ── Phase: aggregate_reduce — sum thread-local profits ────────────────
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
        char   nation[26];
        int    year;
        double profit;
    };
    std::vector<ResultRow> results;
    results.reserve(175);
    {
        GENDB_PHASE("sort_output");
        for (int n = 0; n < 25; n++) {
            for (int y = 0; y < 7; y++) {  // year offsets 0–6 → years 1992–1998
                long double p = global_profits[n][y];
                if (p == 0.0L) continue;
                ResultRow r;
                memcpy(r.nation, nation_names[n], 26);
                r.year   = 1992 + y;
                r.profit = (double)p;
                results.push_back(r);
            }
        }
        std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
            int c = strcmp(a.nation, b.nation);
            if (c != 0) return c < 0;
            return a.year > b.year;  // o_year DESC
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
            fprintf(out, "%s,%d,%.2f\n", r.nation, r.year, r.profit);
        fclose(out);
    }

    // ── Total timing ───────────────────────────────────────────────────────
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms  = std::chrono::duration<double,std::milli>(t_total_end - t_total_start).count();
    fprintf(stderr, "[GENDB_PHASE] total: %.3f ms\n", total_ms);

    return 0;
}
