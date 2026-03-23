// Q9: Product Type Profit Measure — iter_5
//
// Key design vs iter_4:
// 1. Use year_nibbles.bin (28.6MB, MADV_WILLNEED, L3-resident) to replace the 2-DRAM-miss
//    orders chain (orders_by_orderkey 240MB + o_orderdate 57MB). Single nibble access per row.
// 2. Use partsupp_hash_index.bin (256MB, MADV_RANDOM) with software prefetch (PREFETCH_DIST=16)
//    to hide DRAM miss latency. Back to prebuilt hash index — no runtime compact build overhead.
// 3. double (NOT long double) for all profit accumulation — eliminates 3-4x x87 penalty.
// 4. L3 hot budget: green_part(2MB) + suppkey_to_nationrow(0.4MB) + year_nibbles(28.6MB) = 31MB
//    fits within 44MB LLC. partsupp hash (256MB) uses prefetch to hide DRAM latency.

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

    bool open(const char* path, int madv_hint = MADV_SEQUENTIAL) {
        fd = ::open(path, O_RDONLY);
        if (fd < 0) { perror(path); return false; }
        struct stat st;
        fstat(fd, &st);
        size = (size_t)st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); data = nullptr; return false; }
        madvise(data, size, madv_hint);
        return true;
    }
    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) close(fd);
    }
};

// ── partsupp hash index slot ──────────────────────────────────────────────────
#pragma pack(push,1)
struct HTSlot {
    uint64_t key;      // composite key = partkey*200001 + suppkey; 0 = empty
    int32_t  row_idx;
    int32_t  pad;
};
#pragma pack(pop)
static_assert(sizeof(HTSlot) == 16, "HTSlot must be 16 bytes");

static constexpr uint32_t LOG2_CAP  = 24;
static constexpr uint64_t FIB_CONST = 11400714819323198485ULL;
static constexpr uint64_t HT_MASK   = (1ULL << LOG2_CAP) - 1;

inline double lookup_supplycost(int32_t partkey, int32_t suppkey,
                                 const HTSlot* __restrict__ ht,
                                 const double* __restrict__ ps_supplycost) {
    uint64_t key  = (uint64_t)partkey * 200001ULL + (uint64_t)suppkey;
    uint64_t slot = (key * FIB_CONST) >> (64 - LOG2_CAP);
    while (ht[slot].key != 0ULL) {
        if (ht[slot].key == key)
            return ps_supplycost[ht[slot].row_idx];
        slot = (slot + 1) & HT_MASK;
    }
    return 0.0;
}

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
    MmapFile f_year_nibbles;   // 28.6MB nibble-packed year by orderkey — MADV_WILLNEED → L3
    MmapFile f_ps_ht;          // 256MB partsupp hash index — MADV_RANDOM + software prefetch
    MmapFile f_ps_supplycost;  // 61MB ps_supplycost — MADV_RANDOM (accessed via hash index)
    {
        GENDB_PHASE("data_loading");
        if (!f_lpartkey.open(     (gendb_dir + "/lineitem/l_partkey.bin").c_str()))       return 1;
        if (!f_lsuppkey.open(     (gendb_dir + "/lineitem/l_suppkey.bin").c_str()))       return 1;
        if (!f_lorderkey.open(    (gendb_dir + "/lineitem/l_orderkey.bin").c_str()))      return 1;
        if (!f_lextprice.open(    (gendb_dir + "/lineitem/l_extendedprice.bin").c_str())) return 1;
        if (!f_ldiscount.open(    (gendb_dir + "/lineitem/l_discount.bin").c_str()))      return 1;
        if (!f_lquantity.open(    (gendb_dir + "/lineitem/l_quantity.bin").c_str()))      return 1;

        // Storage extension: nibble-packed year offset — MADV_WILLNEED pre-faults 28.6MB into L3
        // while dim_filter phase runs below, so scan phase finds it fully cached.
        // Do NOT open orders_by_orderkey.bin or o_orderdate.bin (plan requirement).
        if (!f_year_nibbles.open(
                (gendb_dir + "/column_versions/orders.o_year.nibble_by_orderkey/year_nibbles.bin").c_str(),
                MADV_WILLNEED))
            return 1;

        // partsupp hash: 256MB, random access → MADV_RANDOM + software prefetch on hot path
        if (!f_ps_ht.open((gendb_dir + "/partsupp/partsupp_hash_index.bin").c_str(), MADV_RANDOM))
            return 1;
        if (!f_ps_supplycost.open((gendb_dir + "/partsupp/ps_supplycost.bin").c_str(), MADV_RANDOM))
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

    // partsupp hash table: 16-byte header (capacity + count), then slots
    const HTSlot* ps_ht         = (const HTSlot*)((const char*)f_ps_ht.data + 16);
    const double* ps_supplycost = (const double*) f_ps_supplycost.data;

    // ── Phase: dim_filter — green_part filter + suppkey_to_nationrow ───────
    // green_part[2000001]: 2MB uint8_t bool filter, L3-resident
    static uint8_t green_part[2000001];
    memset(green_part, 0, sizeof(green_part));

    // suppkey_to_nationrow[100001]: 0.4MB int32_t, precomputed O(1) lookup, L3-resident
    int32_t suppkey_to_nationrow[100001];
    memset(suppkey_to_nationrow, -1, sizeof(suppkey_to_nationrow));

    // nation_names[25][26]: indexed by nation row (nat_by_nkey[nkey])
    char nation_names[25][26];
    memset(nation_names, 0, sizeof(nation_names));

    {
        GENDB_PHASE("dim_filter");

        // --- Build green_part filter ---
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
                    if ((uint32_t)pk <= 2000000u)
                        green_part[pk] = 1;
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
                if ((uint32_t)nkey >= 25u) continue;
                suppkey_to_nationrow[suppkey] = nat_by_nkey[nkey];
            }
        }
    }

    // ── Phase: build_joins (all structures pre-built or built above) ────────
    // No runtime hash/compact build needed — partsupp_hash_index.bin is pre-built.
    // year_nibbles.bin is pre-built. All setup done in dim_filter.
    { GENDB_PHASE("build_joins"); }  // intentionally empty, keep phase for timing consistency

    // ── Phase: main_scan (parallel morsel-driven, 64 threads × 100K morsels) ─
    const int nthreads = (int)std::thread::hardware_concurrency();

    // Per-thread profits[25][10]: nation row × year offset (0=1992 .. 6=1998)
    // Use long double for accumulation to avoid 0.01-cent rounding errors at large magnitudes
    // (e.g. FRANCE 1997 ~458M). Hot-path compute (disc, amount) stays in double (SSE2).
    // x87 overhead is negligible: only ~3.3M qualifying rows reach accumulation (~5.5% of 60M).
    // Aligned to 64-byte cache lines to prevent false sharing across threads
    struct alignas(64) ThreadProfits {
        long double v[25][10];
    };
    std::vector<ThreadProfits> thread_profits(nthreads);
    for (auto& tp : thread_profits)
        memset(tp.v, 0, sizeof(tp.v));

    {
        GENDB_PHASE("main_scan");

        constexpr size_t MORSEL       = 100000;
        constexpr size_t PREFETCH_DIST = 16;  // prefetch partsupp hash slot 16 rows ahead

        std::atomic<size_t> morsel_idx{0};

        const uint8_t* gp  = green_part;
        const int32_t* s2n = suppkey_to_nationrow;

        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                // Thread-local profit array (long double for accumulation precision)
                long double profits[25][10];
                memset(profits, 0, sizeof(profits));

                while (true) {
                    size_t start = morsel_idx.fetch_add(MORSEL, std::memory_order_relaxed);
                    if (start >= total_rows) break;
                    size_t end = std::min(start + MORSEL, total_rows);

                    for (size_t i = start; i < end; i++) {
                        // ── Software prefetch: partsupp hash slot 16 rows ahead ──────────
                        // Hides 256MB hash table DRAM miss latency (~100ns).
                        // Unconditionally prefetch for all rows — ~5.5% will actually use it.
                        // Prefetch instruction is cheap (~1 cycle); wasted prefetches are harmless.
                        if (__builtin_expect(i + PREFETCH_DIST < end, 1)) {
                            size_t pf = i + PREFETCH_DIST;
                            int32_t pf_partkey = l_partkey[pf];
                            int32_t pf_suppkey = l_suppkey[pf];
                            uint64_t pf_key  = (uint64_t)pf_partkey * 200001ULL + (uint64_t)pf_suppkey;
                            uint64_t pf_slot = (pf_key * FIB_CONST) >> (64 - LOG2_CAP);
                            __builtin_prefetch(&ps_ht[pf_slot], 0, 1);
                        }

                        // ── Green part filter: 2MB L3-resident uint8_t array ─────────────
                        int32_t lpartkey = l_partkey[i];
                        if (!gp[(uint32_t)lpartkey]) continue;

                        // ── Supplier → nation row: 0.4MB L3-resident precomputed array ───
                        int32_t lsuppkey = l_suppkey[i];
                        int32_t nrow = s2n[lsuppkey];
                        if (__builtin_expect(nrow < 0, 0)) continue;

                        // ── Year from nibble: 28.6MB L3-resident (via MADV_WILLNEED) ─────
                        // Replaces 2-DRAM-miss chain: orders_by_orderkey(240MB) → o_orderdate(57MB)
                        // Formula: lower nibble of byte[key/2] for even keys, upper nibble for odd
                        uint32_t lorderkey = (uint32_t)l_orderkey[i];
                        int year_off = (yn[lorderkey >> 1] >> ((lorderkey & 1) << 2)) & 0xF;
                        if (__builtin_expect((unsigned)year_off > 6u, 0)) continue;  // 0xF = no order

                        // ── Partsupp hash lookup: 256MB, DRAM (prefetch above hides latency) ─
                        double sc = lookup_supplycost(lpartkey, lsuppkey, ps_ht, ps_supplycost);

                        // ── Compute amount in long double to avoid 0.01-cent rounding errors ──
                        // Individual amount values at ~4.58×10^8 aggregate magnitude lose
                        // precision in double; long double (80-bit x87) preserves enough bits.
                        // Only ~3.3M rows (5.5%) reach this path — x87 overhead negligible.
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
    // Reduce per-thread long double arrays into global long double array
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
        char        nation[26];
        int         year;
        long double profit;
    };
    std::vector<ResultRow> results;
    results.reserve(175);
    {
        GENDB_PHASE("sort_output");
        for (int n = 0; n < 25; n++) {
            for (int y = 0; y < 7; y++) {  // years 1992–1998 (year_off 0–6)
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

    // ── Phase: output ─────────────────────────────────────────────────────
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
