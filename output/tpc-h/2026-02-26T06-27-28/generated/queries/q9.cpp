// Q9: Product Type Profit Measure
// Optimizations vs iter_1:
//   - oky_nibble.bin (30MB mmap) replaces 57MB BSS orderkey_year array + 15M-row orders scan
//     → eliminates 57MB BSS memset (~14600 page faults) AND entire orders scan during build_joins
//   - Compact lineitem columns: int32 price, uint8 disc, uint8 qty (840MB vs 1680MB bandwidth)
//   - OMP warmup before any timed phase (~5-6ms thread-spawn eliminated from dim_filter)
//   - cps_ht: NO memset (BSS already zero at startup; key==0 = empty sentinel; ~5ms saved)

#include <cstdint>
#include <cstring>
#include <cstdio>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <climits>

#include "timing_utils.h"

// ── mmap helpers ──────────────────────────────────────────────────────────────
template<typename T>
static const T* mmap_ro(const std::string& path, size_t& n_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    size_t sz = st.st_size;
    void* p = mmap(nullptr, sz, PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) { perror(("mmap: " + path).c_str()); exit(1); }
    madvise(p, sz, MADV_SEQUENTIAL);
    posix_fadvise(fd, 0, sz, POSIX_FADV_SEQUENTIAL);
    close(fd);
    n_out = sz / sizeof(T);
    return reinterpret_cast<const T*>(p);
}

// mmap nibble file as MAP_SHARED (page-cache persistent across runs), no SEQUENTIAL hint
// (will be preloaded into L3 via parallel touch, then randomly probed)
static const uint8_t* mmap_nibble(const std::string& path, size_t& bytes_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open nibble: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    bytes_out = st.st_size;
    void* p = mmap(nullptr, bytes_out, PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) { perror(("mmap nibble: " + path).c_str()); exit(1); }
    close(fd);
    return reinterpret_cast<const uint8_t*>(p);
}

static const char* mmap_bytes(const std::string& path, size_t& bytes_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open bytes: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    bytes_out = st.st_size;
    void* p = mmap(nullptr, bytes_out, PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) { perror(("mmap bytes: " + path).c_str()); exit(1); }
    madvise(p, bytes_out, MADV_SEQUENTIAL);
    close(fd);
    return reinterpret_cast<const char*>(p);
}

// ── Compact partsupp hash: 1M slots × 16B = 16MB (BSS, zero at startup) ──────
// Key = (ps_partkey << 32) | uint32(ps_suppkey); sentinel = key == 0
// Valid keys: pk>=1, sk>=1 → key = (pk<<32)|uint32(sk) >= 1<<32 > 0 always
struct CPSBucket { int64_t key; double supplycost; };
static_assert(sizeof(CPSBucket) == 16, "CPSBucket must be 16 bytes");

constexpr uint64_t CPS_CAP  = 1ULL << 20;  // 1M slots = 16MB
constexpr uint64_t CPS_MASK = CPS_CAP - 1;

static CPSBucket cps_ht[CPS_CAP];   // BSS: zero at process start → key==0 = empty

// ── Part qualifying bitset: 2MB (BSS) ────────────────────────────────────────
static uint8_t part_bitset[2000001];

// ── Direct suppkey→nationkey: 400KB (BSS) ────────────────────────────────────
static int32_t suppkey_nk[100001];

// ── splitmix64 hash for int64 composite key ───────────────────────────────────
static inline uint64_t hash64(int64_t key) {
    uint64_t x = (uint64_t)key;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

// ── Kahan compensated accumulator (precision guard) ───────────────────────────
struct KahanAcc {
    double sum = 0.0;
    double c   = 0.0;
    inline void add(double v) {
        double y = v - c;
        double t = sum + y;
        c = (t - sum) - y;
        sum = t;
    }
};

// ── Thread-local aggregation: [N_THREADS][N_GROUPS] (BSS) ────────────────────
constexpr int N_THREADS = 64;
constexpr int N_NATIONS = 25;
constexpr int N_YEARS   = 8;   // offsets 0-6 valid; slot 7 unused
constexpr int N_GROUPS  = N_NATIONS * N_YEARS;  // 200 slots (175 valid groups)

static KahanAcc thread_agg[N_THREADS][N_GROUPS];  // BSS

// ── Main ──────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string db      = argv[1];
    const std::string out_dir = argv[2];

    GENDB_PHASE("total");

    // ── OMP WARMUP: pre-initialize thread pool before any timed phase ──────────
    // Eliminates ~5-6ms thread-spawn latency from dim_filter (first real parallel phase).
    #pragma omp parallel num_threads(N_THREADS)
    {
        volatile int x = omp_get_thread_num();
        (void)x;
    }

    constexpr int BASE_YEAR = 1992;

    // ── DATA LOADING ──────────────────────────────────────────────────────────
    size_t n_part, n_li, n_sup, n_ps, n_nat, tmp;

    const int32_t* p_partkey;
    const int32_t* p_name_offs;
    const char*    p_name_data;

    const int32_t* l_partkey;
    const int32_t* l_suppkey;
    const int32_t* l_orderkey;
    const int32_t* l_price_i32;  // compact: actual_price  = val * 0.01
    const uint8_t* l_disc_u8;    // compact: actual_disc   = val * 0.01
    const uint8_t* l_qty_u8;     // compact: actual_qty    = (double)val

    const int32_t* s_suppkey;
    const int32_t* s_nationkey;

    const int32_t* ps_partkey_col;
    const int32_t* ps_suppkey_col;
    const double*  ps_supplycost_col;

    const int32_t* n_nationkey;
    const int32_t* n_name_offs;
    const char*    n_name_data;

    // oky_nibble: pre-built nibble-packed orderkey→year_offset (30MB, L3-resident)
    const uint8_t* oky_nibble  = nullptr;
    size_t         oky_nb_size = 0;

    {
        GENDB_PHASE("data_loading");

        p_partkey     = mmap_ro<int32_t>(db + "/part/p_partkey.bin",      n_part);
        p_name_offs   = mmap_ro<int32_t>(db + "/part/p_name.offsets",     tmp);
        size_t pnd;
        p_name_data   = mmap_bytes(db + "/part/p_name.data", pnd);

        l_partkey     = mmap_ro<int32_t>(db + "/lineitem/l_partkey.bin",  n_li);
        l_suppkey     = mmap_ro<int32_t>(db + "/lineitem/l_suppkey.bin",  tmp);
        l_orderkey    = mmap_ro<int32_t>(db + "/lineitem/l_orderkey.bin", tmp);

        // Compact column versions (int32/uint8/uint8 instead of double/double/double)
        // → lineitem bandwidth ~840MB vs ~1680MB for the 6 columns
        l_price_i32   = mmap_ro<int32_t>(
            db + "/column_versions/lineitem.l_extendedprice.int32/price.bin", tmp);
        l_disc_u8     = mmap_ro<uint8_t>(
            db + "/column_versions/lineitem.l_discount.uint8/disc.bin", tmp);
        l_qty_u8      = mmap_ro<uint8_t>(
            db + "/column_versions/lineitem.l_quantity.uint8/qty.bin", tmp);

        s_suppkey     = mmap_ro<int32_t>(db + "/supplier/s_suppkey.bin",   n_sup);
        s_nationkey   = mmap_ro<int32_t>(db + "/supplier/s_nationkey.bin", tmp);

        ps_partkey_col    = mmap_ro<int32_t>(db + "/partsupp/ps_partkey.bin",    n_ps);
        ps_suppkey_col    = mmap_ro<int32_t>(db + "/partsupp/ps_suppkey.bin",    tmp);
        ps_supplycost_col = mmap_ro<double> (db + "/partsupp/ps_supplycost.bin", tmp);

        n_nationkey   = mmap_ro<int32_t>(db + "/nation/n_nationkey.bin",   n_nat);
        n_name_offs   = mmap_ro<int32_t>(db + "/nation/n_name.offsets",    tmp);
        size_t nnd;
        n_name_data   = mmap_bytes(db + "/nation/n_name.data", nnd);

        // mmap nibble file (MAP_SHARED → persists in OS page cache between hot runs)
        oky_nibble = mmap_nibble(
            db + "/column_versions/orders.o_orderkey_to_year_nibble/oky_nibble.bin",
            oky_nb_size);
        // NOTE: No orders table loaded. No orderkey_year BSS array. No 57MB memset.
    }

    // ── PRELOAD NATION: 25-entry string array indexed by n_nationkey ──────────
    std::string nation_name[N_NATIONS];
    for (size_t i = 0; i < n_nat; i++) {
        int32_t nk = n_nationkey[i];
        if ((uint32_t)nk < (uint32_t)N_NATIONS) {
            int32_t off0 = n_name_offs[i], off1 = n_name_offs[i + 1];
            nation_name[nk] = std::string(n_name_data + off0, (size_t)(off1 - off0));
        }
    }

    // ── DIM FILTER: scan p_name for 'green', build part_bitset ───────────────
    {
        GENDB_PHASE("dim_filter");

        #pragma omp parallel for num_threads(N_THREADS) schedule(static)
        for (size_t i = 0; i < n_part; i++) {
            int32_t off0 = p_name_offs[i], off1 = p_name_offs[i + 1];
            int32_t len  = off1 - off0;
            if (len >= 5 && memmem(p_name_data + off0, (size_t)len, "green", 5)) {
                int32_t pk = p_partkey[i];
                if ((uint32_t)pk <= 2000000u)
                    part_bitset[pk] = 1;
            }
        }
    }

    // ── BUILD JOINS ───────────────────────────────────────────────────────────
    {
        GENDB_PHASE("build_joins");

        // 1. Direct suppkey→nationkey (single-threaded, trivial 400KB)
        memset(suppkey_nk, -1, sizeof(suppkey_nk));
        for (size_t i = 0; i < n_sup; i++) {
            int32_t sk = s_suppkey[i];
            if ((uint32_t)sk <= 100000u)
                suppkey_nk[sk] = s_nationkey[i];
        }

        // 2. Compact partsupp hash (16MB BSS, already zero at process start)
        // DO NOT memset — BSS zero = empty sentinel (key==0). Saves ~5ms page-fault cost.
        // Parallel CAS insert: ~440K qualifying rows into 1M slots → low collision rate.
        #pragma omp parallel for num_threads(N_THREADS) schedule(static)
        for (size_t i = 0; i < n_ps; i++) {
            int32_t pk = ps_partkey_col[i];
            if ((uint32_t)pk > 2000000u || !part_bitset[pk]) continue;

            int32_t sk  = ps_suppkey_col[i];
            int64_t key = ((int64_t)pk << 32) | (uint32_t)sk;
            double  sc  = ps_supplycost_col[i];

            uint64_t h = hash64(key) & CPS_MASK;
            for (;;) {
                int64_t expected = 0LL;
                if (__atomic_compare_exchange_n(&cps_ht[h].key, &expected, key,
                        false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
                    cps_ht[h].supplycost = sc;
                    break;
                }
                h = (h + 1) & CPS_MASK;
            }
        }

        // NOTE: No orders scan! oky_nibble.bin provides orderkey→year_offset directly.
    }

    // ── PRELOAD oky_nibble INTO L3 ────────────────────────────────────────────
    // 30MB < 44MB L3 cache. 64-thread parallel touch (one read per cache line = 64B)
    // fills all pages into L3 before main_scan random probes begin.
    {
        const uint8_t* nb  = oky_nibble;
        size_t         nsz = oky_nb_size;
        volatile uint64_t sink = 0;

        #pragma omp parallel num_threads(N_THREADS) reduction(+:sink)
        {
            uint64_t local_sum = 0;
            #pragma omp for schedule(static)
            for (size_t b = 0; b < nsz; b += 64) {
                local_sum += nb[b];
            }
            sink += local_sum;
        }
        (void)sink;
    }

    // ── MAIN SCAN: parallel morsel-driven lineitem scan ───────────────────────
    // Zero thread_agg before use (BSS is zero at startup but zero explicitly for safety)
    for (int t = 0; t < N_THREADS; t++)
        for (int s = 0; s < N_GROUPS; s++)
            thread_agg[t][s] = KahanAcc{};

    {
        GENDB_PHASE("main_scan");

        const uint8_t* nb = oky_nibble;

        #pragma omp parallel num_threads(N_THREADS)
        {
            int tid = omp_get_thread_num();
            KahanAcc* __restrict__ local = thread_agg[tid];

            #pragma omp for schedule(static, 65536)
            for (size_t i = 0; i < n_li; i++) {
                int32_t lpartkey = l_partkey[i];

                // ── Part bitset filter (~95% skip rate — critical path guard) ──
                if ((uint32_t)lpartkey > 2000000u || !part_bitset[lpartkey]) continue;

                // ── Year lookup via nibble-packed file (30MB, L3-resident) ──
                // Probe: byte_idx = ok>>1, shift = (ok&1)<<2, yr_off = (nb[byte_idx]>>shift)&0xF
                int32_t lorderkey = l_orderkey[i];
                if ((uint32_t)lorderkey > 60000000u) continue;

                // Prefetch nibble byte for a future row to overlap latency
                if (__builtin_expect(i + 128 < n_li, 1))
                    __builtin_prefetch(nb + ((uint32_t)l_orderkey[i + 128] >> 1), 0, 1);

                uint32_t byte_idx = (uint32_t)lorderkey >> 1;
                uint32_t shift    = ((uint32_t)lorderkey & 1u) << 2;
                uint32_t yr_off   = (nb[byte_idx] >> shift) & 0xFu;
                if (yr_off == 0xFu) continue;  // 0xF = not found

                // ── Suppkey→nationkey (400KB direct array, L1/L2-resident) ──
                int32_t lsuppkey = l_suppkey[i];
                if ((uint32_t)lsuppkey > 100000u) continue;
                int32_t nk = suppkey_nk[lsuppkey];
                if ((uint32_t)nk >= (uint32_t)N_NATIONS) continue;

                // ── Probe compact partsupp hash (16MB, L3-resident) ──
                int64_t ps_key = ((int64_t)lpartkey << 32) | (uint32_t)lsuppkey;
                uint64_t h     = hash64(ps_key) & CPS_MASK;
                double supplycost = 0.0;
                bool found_ps = false;
                for (uint64_t probe = 0; probe < CPS_CAP; probe++) {
                    uint64_t slot = (h + probe) & CPS_MASK;
                    int64_t k = cps_ht[slot].key;
                    if (k == 0LL) break;     // empty slot → not in table
                    if (k == ps_key) {
                        supplycost = cps_ht[slot].supplycost;
                        found_ps = true;
                        break;
                    }
                }
                if (!found_ps) continue;

                // ── Compute profit amount using compact column types ──
                // revenue = l_extendedprice * (1 - l_discount)  [correctness anchor]
                //         = price_i32 * (100 - disc_u8) * 1e-4  [integer multiply]
                // amount  = revenue - ps_supplycost * l_quantity
                int32_t price_i32 = l_price_i32[i];
                int32_t disc_val  = (int32_t)(unsigned)l_disc_u8[i];
                // int64 intermediate to avoid overflow across scale factors
                double revenue = (double)((int64_t)price_i32 * (int64_t)(100 - disc_val)) * 1e-4;
                double amount  = revenue - supplycost * (double)l_qty_u8[i];

                local[nk * N_YEARS + (int)yr_off].add(amount);
            }
        }
    }

    // ── MERGE thread-local accumulators ───────────────────────────────────────
    double final_agg[N_GROUPS] = {};
    for (int s = 0; s < N_GROUPS; s++) {
        KahanAcc merged{};
        for (int t = 0; t < N_THREADS; t++)
            merged.add(thread_agg[t][s].sum + thread_agg[t][s].c);
        final_agg[s] = merged.sum;
    }

    // ── OUTPUT: sort 175 rows, write CSV ──────────────────────────────────────
    {
        GENDB_PHASE("output");

        struct Row {
            const std::string* nation;
            int year;
            double sum_profit;
        };

        std::vector<Row> rows;
        rows.reserve(N_NATIONS * 7);
        for (int ni = 0; ni < N_NATIONS; ni++)
            for (int yi = 0; yi < 7; yi++)
                rows.push_back({&nation_name[ni], BASE_YEAR + yi,
                                 final_agg[ni * N_YEARS + yi]});

        // ORDER BY nation ASC, o_year DESC
        std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
            if (*a.nation != *b.nation) return *a.nation < *b.nation;
            return a.year > b.year;
        });

        std::string outpath = out_dir + "/Q9.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        if (!f) { perror(("fopen: " + outpath).c_str()); exit(1); }
        fprintf(f, "nation,o_year,sum_profit\n");
        for (const auto& r : rows)
            fprintf(f, "%s,%d,%.2f\n", r.nation->c_str(), r.year, r.sum_profit);
        fclose(f);
    }

    return 0;
}
