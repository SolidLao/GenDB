// Q9: Product Type Profit Measure
// SELECT nation, o_year, SUM(l_extendedprice*(1-l_discount)-ps_supplycost*l_quantity)
// FROM part, supplier, lineitem, partsupp, orders, nation
// WHERE p_name LIKE '%green%' AND s_suppkey=l_suppkey AND ps_suppkey=l_suppkey
//   AND ps_partkey=l_partkey AND p_partkey=l_partkey AND o_orderkey=l_orderkey
//   AND s_nationkey=n_nationkey
// GROUP BY nation, o_year ORDER BY nation ASC, o_year DESC

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

#include "date_utils.h"
#include "timing_utils.h"

// ── mmap helpers ──────────────────────────────────────────────────────────────
template<typename T>
static const T* mmap_seq(const std::string& path, size_t& n_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    size_t sz = st.st_size;
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror(("mmap: " + path).c_str()); exit(1); }
    madvise(p, sz, MADV_SEQUENTIAL);
    posix_fadvise(fd, 0, sz, POSIX_FADV_SEQUENTIAL);
    close(fd);
    n_out = sz / sizeof(T);
    return reinterpret_cast<const T*>(p);
}

static const char* mmap_bytes_seq(const std::string& path, size_t& bytes_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    bytes_out = st.st_size;
    void* p = mmap(nullptr, bytes_out, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror(("mmap: " + path).c_str()); exit(1); }
    madvise(p, bytes_out, MADV_SEQUENTIAL);
    posix_fadvise(fd, 0, bytes_out, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return reinterpret_cast<const char*>(p);
}

// ── Compact partsupp hash: 1M slots, 16MB, fits in L3 cache ──────────────────
// Key encoding: (ps_partkey << 32) | uint32(ps_suppkey)
// Sentinel: key == 0LL  (all valid keys have pk>=1, sk>=1 → key > 0)
struct CPSBucket { int64_t key; double supplycost; };
static_assert(sizeof(CPSBucket) == 16, "CPSBucket must be 16 bytes");

constexpr uint64_t CPS_CAP  = 1ULL << 20;   // 1M slots = 16MB total
constexpr uint64_t CPS_MASK = CPS_CAP - 1;

static CPSBucket cps_ht[CPS_CAP];   // BSS: zero-initialized → key==0 = empty

// ── Direct orderkey→year_offset table: 60MB uint8_t ─────────────────────────
// Maps o_orderkey → (year - BASE_YEAR); 0xFF = not found / out-of-range
constexpr int32_t MAX_ORDERKEY = 60000000;
static uint8_t orderkey_year[MAX_ORDERKEY + 1];  // ~57 MB

// ── Part qualifying bitset: 2MB ───────────────────────────────────────────────
static uint8_t part_bitset[2000001];

// ── Direct suppkey→nationkey table: 400KB ────────────────────────────────────
static int32_t suppkey_nk[100001];

// ── splitmix64 hash for composite int64 key (good avalanche, no clustering) ──
static inline uint64_t hash64(int64_t key) {
    uint64_t x = (uint64_t)key;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

// ── Kahan compensated accumulator ────────────────────────────────────────────
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

// ── Main ──────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string db      = argv[1];
    const std::string out_dir = argv[2];

    GENDB_PHASE("total");
    gendb::init_date_tables();

    constexpr int N_THREADS = 64;
    constexpr int N_NATIONS = 25;
    constexpr int N_YEARS   = 8;    // [25][8]: years 1992-1998 → offsets 0-6; slot 7 unused
    constexpr int BASE_YEAR = 1992;
    constexpr int N_GROUPS  = N_NATIONS * N_YEARS;  // 200 (175 valid groups)

    // ── DATA LOADING ──────────────────────────────────────────────────────────
    size_t n_part, n_li, n_sup, n_ps, n_ord, n_nat, tmp;

    const int32_t* p_partkey;
    const int32_t* p_name_offs;   // int32_t[n_part + 1]
    const char*    p_name_data;   // raw bytes

    const int32_t* l_partkey;
    const int32_t* l_suppkey;
    const int32_t* l_orderkey;
    const double*  l_extprice;
    const double*  l_discount;
    const double*  l_quantity;

    const int32_t* s_suppkey;
    const int32_t* s_nationkey;

    const int32_t* ps_partkey_col;
    const int32_t* ps_suppkey_col;
    const double*  ps_supplycost_col;

    const int32_t* o_orderkey_col;
    const int32_t* o_orderdate_col;

    const int32_t* n_nationkey;
    const int32_t* n_name_offs;   // int32_t[n_nat + 1]
    const char*    n_name_data;   // raw bytes

    {
        GENDB_PHASE("data_loading");

        p_partkey         = mmap_seq<int32_t>(db + "/part/p_partkey.bin",         n_part);
        p_name_offs       = mmap_seq<int32_t>(db + "/part/p_name.offsets",        tmp);
        size_t pnd;
        p_name_data       = mmap_bytes_seq(db + "/part/p_name.data", pnd);

        l_partkey         = mmap_seq<int32_t>(db + "/lineitem/l_partkey.bin",         n_li);
        l_suppkey         = mmap_seq<int32_t>(db + "/lineitem/l_suppkey.bin",         tmp);
        l_orderkey        = mmap_seq<int32_t>(db + "/lineitem/l_orderkey.bin",        tmp);
        l_extprice        = mmap_seq<double> (db + "/lineitem/l_extendedprice.bin",   tmp);
        l_discount        = mmap_seq<double> (db + "/lineitem/l_discount.bin",        tmp);
        l_quantity        = mmap_seq<double> (db + "/lineitem/l_quantity.bin",        tmp);

        s_suppkey         = mmap_seq<int32_t>(db + "/supplier/s_suppkey.bin",         n_sup);
        s_nationkey       = mmap_seq<int32_t>(db + "/supplier/s_nationkey.bin",       tmp);

        ps_partkey_col    = mmap_seq<int32_t>(db + "/partsupp/ps_partkey.bin",        n_ps);
        ps_suppkey_col    = mmap_seq<int32_t>(db + "/partsupp/ps_suppkey.bin",        tmp);
        ps_supplycost_col = mmap_seq<double> (db + "/partsupp/ps_supplycost.bin",     tmp);

        o_orderkey_col    = mmap_seq<int32_t>(db + "/orders/o_orderkey.bin",          n_ord);
        o_orderdate_col   = mmap_seq<int32_t>(db + "/orders/o_orderdate.bin",         tmp);

        n_nationkey       = mmap_seq<int32_t>(db + "/nation/n_nationkey.bin",         n_nat);
        n_name_offs       = mmap_seq<int32_t>(db + "/nation/n_name.offsets",          tmp);
        size_t nnd;
        n_name_data       = mmap_bytes_seq(db + "/nation/n_name.data", nnd);
    }

    // ── PRELOAD NATION: 25-entry string array indexed by n_nationkey ──────────
    std::string nation_name[N_NATIONS];
    {
        for (size_t i = 0; i < n_nat; i++) {
            int32_t nk = n_nationkey[i];
            if ((uint32_t)nk < (uint32_t)N_NATIONS) {
                int32_t off0 = n_name_offs[i];
                int32_t off1 = n_name_offs[i + 1];
                nation_name[nk] = std::string(n_name_data + off0, (size_t)(off1 - off0));
            }
        }
    }

    // ── DIM FILTER: scan p_name for 'green', build part_bitset ───────────────
    {
        GENDB_PHASE("dim_filter");
        memset(part_bitset, 0, sizeof(part_bitset));

        #pragma omp parallel for num_threads(N_THREADS) schedule(static)
        for (size_t i = 0; i < n_part; i++) {
            int32_t off0 = p_name_offs[i];
            int32_t off1 = p_name_offs[i + 1];
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

        // 1. Direct suppkey→nationkey (tiny, single-threaded, 400 KB)
        memset(suppkey_nk, -1, sizeof(suppkey_nk));
        for (size_t i = 0; i < n_sup; i++) {
            int32_t sk = s_suppkey[i];
            if ((uint32_t)sk <= 100000u)
                suppkey_nk[sk] = s_nationkey[i];
        }

        // 2. Direct orderkey→year_offset array (parallel, ~57 MB)
        memset(orderkey_year, 0xFF, sizeof(orderkey_year));
        #pragma omp parallel for num_threads(N_THREADS) schedule(static)
        for (size_t i = 0; i < n_ord; i++) {
            int32_t ok = o_orderkey_col[i];
            if ((uint32_t)ok <= (uint32_t)MAX_ORDERKEY) {
                int yr_off = gendb::extract_year(o_orderdate_col[i]) - BASE_YEAR;
                orderkey_year[ok] = (uint8_t)((unsigned)yr_off < 7u ? (uint8_t)yr_off : 0xFF);
            }
        }

        // 3. Compact partsupp hash: filter by bitset, ~440K entries, 1M slots = 16 MB
        // Zero-init (BSS is already zero, but explicit for clarity after first run)
        memset(cps_ht, 0, sizeof(cps_ht));

        // Parallel insert with CAS (unique keys guaranteed by TPC-H partsupp PK)
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
                    // Claimed slot; write supplycost before barrier at end of parallel for
                    cps_ht[h].supplycost = sc;
                    break;
                }
                // Slot taken by another thread; try next
                h = (h + 1) & CPS_MASK;
            }
        }
    }

    // ── MAIN SCAN: parallel morsel-driven lineitem scan ───────────────────────
    // Thread-local aggregation arrays: [N_THREADS][N_NATIONS * N_YEARS]
    static KahanAcc thread_agg[N_THREADS][N_GROUPS];
    // Zero-initialize
    for (int t = 0; t < N_THREADS; t++)
        for (int s = 0; s < N_GROUPS; s++)
            thread_agg[t][s] = KahanAcc{};

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(N_THREADS)
        {
            int tid = omp_get_thread_num();
            KahanAcc* local = thread_agg[tid];

            #pragma omp for schedule(static, 65536)
            for (size_t i = 0; i < n_li; i++) {
                int32_t lpartkey = l_partkey[i];

                // ── Part filter: ~95% skip rate ──
                if ((uint32_t)lpartkey > 2000000u || !part_bitset[lpartkey]) continue;

                // ── Year lookup: direct array ──
                int32_t lorderkey = l_orderkey[i];
                if ((uint32_t)lorderkey > (uint32_t)MAX_ORDERKEY) continue;

                // Prefetch year for a future row to hide DRAM latency
                if (__builtin_expect((i + 128) < n_li, 1))
                    __builtin_prefetch(orderkey_year + l_orderkey[i + 128], 0, 0);

                uint8_t yr_off = orderkey_year[lorderkey];
                if (yr_off == 0xFF) continue;

                // ── Suppkey→nationkey: direct array ──
                int32_t lsuppkey = l_suppkey[i];
                if ((uint32_t)lsuppkey > 100000u) continue;
                int32_t nk = suppkey_nk[lsuppkey];
                if ((uint32_t)nk >= (uint32_t)N_NATIONS) continue;

                // ── Probe compact partsupp hash (16 MB, L3-resident) ──
                int64_t ps_key = ((int64_t)lpartkey << 32) | (uint32_t)lsuppkey;
                uint64_t h     = hash64(ps_key) & CPS_MASK;
                double supplycost = 0.0;
                bool found_ps = false;
                for (uint64_t probe = 0; probe < CPS_CAP; probe++) {
                    uint64_t slot = (h + probe) & CPS_MASK;
                    int64_t k = cps_ht[slot].key;
                    if (k == 0LL) break;   // empty slot → not found
                    if (k == ps_key) {
                        supplycost = cps_ht[slot].supplycost;
                        found_ps = true;
                        break;
                    }
                }
                if (!found_ps) continue;

                // ── Compute profit and accumulate ──
                double amount = l_extprice[i] * (1.0 - l_discount[i])
                              - supplycost * l_quantity[i];
                local[nk * N_YEARS + (int)yr_off].add(amount);
            }
        }
    }

    // ── MERGE thread-local accumulators ───────────────────────────────────────
    double final_agg[N_GROUPS] = {};
    {
        for (int s = 0; s < N_GROUPS; s++) {
            KahanAcc merged{};
            for (int t = 0; t < N_THREADS; t++)
                merged.add(thread_agg[t][s].sum + thread_agg[t][s].c);
            final_agg[s] = merged.sum;
        }
    }

    // ── OUTPUT: 175 rows sorted nation ASC, o_year DESC ───────────────────────
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

        // Sort: nation ASC, o_year DESC
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
