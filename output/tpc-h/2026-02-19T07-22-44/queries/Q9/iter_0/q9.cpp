// Q9: Product Type Profit Measure
// Strategy:
//   1. Load nation (25 rows) -> dict array
//   2. Load supplier (100K rows) -> flat array suppkey->nationkey
//   3. Scan part, filter p_name LIKE '%green%' -> bitset of qualifying p_partkey
//   4. Build compact open-addressing hash map for partsupp: (partkey<<32|suppkey)->supplycost
//   5. mmap pre-built orders_orderkey_hash index; mmap o_orderdate column
//   6. Parallel lineitem scan: for each qualifying row collect (nk, yr, amount) into per-thread buffer
//   7. Sequential accumulate in thread order (= same as single-threaded row order) -> matches reference FP
//   8. Sort; write CSV

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <algorithm>
#include <string>
#include <vector>
#include <fstream>
#include "date_utils.h"
#include "timing_utils.h"

static constexpr int NUM_THREADS = 32;
static constexpr int NUM_NATIONS = 25;
static constexpr int NUM_YEARS   = 7;  // 1992-1998

// ---------------------------------------------------------------------------
// mmap helper
// ---------------------------------------------------------------------------
template<typename T>
static const T* mmap_col(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { fprintf(stderr, "Cannot open: %s\n", path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    void* ptr = mmap(nullptr, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) { fprintf(stderr, "mmap failed: %s\n", path.c_str()); exit(1); }
    count = (size_t)st.st_size / sizeof(T);
    return reinterpret_cast<const T*>(ptr);
}

// ---------------------------------------------------------------------------
// Bitset for qualifying part keys (p_partkey in [1..2000000])
// ---------------------------------------------------------------------------
struct PartBitset {
    static constexpr uint32_t MAX_KEY = 2000001u;
    static constexpr uint32_t NWORDS  = (MAX_KEY + 63u) / 64u;
    uint64_t words[NWORDS];

    void clear() { memset(words, 0, sizeof(words)); }
    void set(uint32_t k) { words[k >> 6] |= (1ULL << (k & 63u)); }
    bool test(uint32_t k) const { return (words[k >> 6] >> (k & 63u)) & 1u; }
};

// ---------------------------------------------------------------------------
// Compact open-addressing hash map: uint64 key -> double value
// For partsupp: key = (ps_partkey<<32 | ps_suppkey), value = ps_supplycost
// Capacity 2^24 = 16777216; 8M entries => load ~0.48
// ---------------------------------------------------------------------------
struct PartsuppMap {
    static constexpr uint32_t CAP  = 1u << 24; // 16,777,216
    static constexpr uint32_t MASK = CAP - 1u;
    static constexpr uint64_t EMPTY_KEY = UINT64_MAX;

    struct Slot { uint64_t key; double val; };
    Slot* slots = nullptr;

    void init() {
        slots = (Slot*)malloc((size_t)CAP * sizeof(Slot));
        if (!slots) { perror("malloc PartsuppMap"); exit(1); }
        for (uint32_t i = 0; i < CAP; i++) slots[i].key = EMPTY_KEY;
    }

    static uint32_t hash(uint64_t k) {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53ULL;
        k ^= k >> 33;
        return (uint32_t)(k & MASK);
    }

    void insert(uint64_t key, double val) {
        uint32_t h = hash(key);
        while (slots[h].key != EMPTY_KEY) h = (h + 1u) & MASK;
        slots[h] = {key, val};
    }

    bool lookup(uint64_t key, double& out) const {
        uint32_t h = hash(key);
        while (__builtin_expect(slots[h].key != EMPTY_KEY, 1)) {
            if (slots[h].key == key) { out = slots[h].val; return true; }
            h = (h + 1u) & MASK;
        }
        return false;
    }
};

// ---------------------------------------------------------------------------
// Pre-built orders orderkey hash index
// Layout: [uint32_t capacity] [capacity × {int32_t key, uint32_t pos}]
// Empty bucket: key == INT32_MIN
// Hash: ((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & (cap-1)
// ---------------------------------------------------------------------------
struct OrdersIndex {
    struct Bucket { int32_t key; uint32_t pos; };
    uint32_t cap = 0;
    const Bucket* buckets = nullptr;

    void load(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open orders index: %s\n", path.c_str()); exit(1); }
        struct stat st; fstat(fd, &st);
        const char* raw = (const char*)mmap(nullptr, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (raw == MAP_FAILED) { perror("mmap orders index"); exit(1); }
        cap     = *(const uint32_t*)raw;
        buckets = (const Bucket*)(raw + sizeof(uint32_t));
    }

    uint32_t lookup(int32_t key) const {
        uint32_t idx = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & (uint64_t)(cap - 1));
        while (buckets[idx].key != INT32_MIN) {
            if (buckets[idx].key == key) return buckets[idx].pos;
            idx = (idx + 1u) & (cap - 1u);
        }
        return UINT32_MAX;
    }
};

// ---------------------------------------------------------------------------
// Qualifying row: compact struct for collect-then-accumulate approach.
// Each qualifying lineitem row produces one of these.
// nk_yr = nationkey*8 + (year-1992) packed into uint16_t
// ---------------------------------------------------------------------------
struct QRow {
    uint16_t nk_yr;
    double   amount;
};

// ---------------------------------------------------------------------------
// Shared data for parallel threads
// ---------------------------------------------------------------------------
struct SharedData {
    const int32_t* l_partkey;
    const int32_t* l_suppkey;
    const int32_t* l_orderkey;
    const double*  l_extprice;
    const double*  l_discount;
    const double*  l_quantity;
    size_t         n_li;

    const int32_t* o_orderdate;

    const PartBitset*  part_bitset;
    const int8_t*      supp_nk;   // index: suppkey (1..100000) -> nationkey
    const PartsuppMap* psmap;
    const OrdersIndex* oidx;
};

// Thread work: parallel probe phase — collects qualifying rows in row order
struct ThreadWork {
    const SharedData* shared;
    size_t start, end;
    QRow*  results;      // pre-allocated buffer (caller provides)
    size_t result_count; // filled by thread
};

static void* thread_func(void* arg) {
    ThreadWork* w = (ThreadWork*)arg;
    const SharedData& s = *w->shared;
    w->result_count = 0;

    const PartBitset&  bitset = *s.part_bitset;
    const int8_t*      snk    = s.supp_nk;
    const PartsuppMap& psmap  = *s.psmap;
    const OrdersIndex& oidx   = *s.oidx;
    const int32_t*     odate  = s.o_orderdate;
    QRow* out = w->results;
    size_t cnt = 0;

    for (size_t i = w->start; i < w->end; i++) {
        // Gate 1: part bitset — eliminates ~95.2% of rows
        uint32_t pk = (uint32_t)s.l_partkey[i];
        if (!bitset.test(pk)) continue;

        int32_t sk = s.l_suppkey[i];

        // Gate 2: partsupp lookup
        uint64_t ps_key = ((uint64_t)pk << 32) | (uint32_t)sk;
        double supplycost;
        if (!psmap.lookup(ps_key, supplycost)) continue;

        // Gate 3: orders lookup -> get o_year
        int32_t ok = s.l_orderkey[i];
        uint32_t opos = oidx.lookup(ok);
        if (__builtin_expect(opos == UINT32_MAX, 0)) continue;

        int32_t ord = odate[opos];
        int year = gendb::extract_year(ord);
        if (__builtin_expect(year < 1992 || year > 1998, 0)) continue;

        // Compute amount
        double amount = s.l_extprice[i] * (1.0 - s.l_discount[i])
                      - supplycost * s.l_quantity[i];

        uint8_t nk = (uint8_t)snk[sk];
        out[cnt++] = {(uint16_t)((uint16_t)nk * 8u + (uint16_t)(year - 1992)), amount};
    }
    w->result_count = cnt;
    return nullptr;
}

// ---------------------------------------------------------------------------
// Main query function
// ---------------------------------------------------------------------------
void run_Q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // Static structures to avoid stack overflow
    static int8_t      supp_nk[100001]; // 100KB, suppkey->nationkey
    static PartBitset  part_bitset;     // ~31KB
    static PartsuppMap psmap;           // 192MB heap

    std::vector<std::string> nation_names(25);

    // ---- Phase 1: Load dimensions + build bitset ----
    {
        GENDB_PHASE("dim_filter");

        // Load nation name dictionary
        {
            std::string dp = gendb_dir + "/nation/n_name_dict.txt";
            std::ifstream f(dp);
            std::string line;
            int idx = 0;
            while (std::getline(f, line) && idx < 25) nation_names[idx++] = line;
        }

        // Build suppkey -> nationkey flat array
        {
            size_t n;
            const int32_t* s_sk = mmap_col<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", n);
            const int32_t* s_nk = mmap_col<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", n);
            memset(supp_nk, 0, sizeof(supp_nk));
            for (size_t i = 0; i < n; i++) supp_nk[s_sk[i]] = (int8_t)s_nk[i];
        }

        // Filter part on p_name LIKE '%green%' -> bitset
        {
            part_bitset.clear();
            size_t nc;
            const char*    p_nm = mmap_col<char>(gendb_dir + "/part/p_name.bin", nc);
            size_t nk;
            const int32_t* p_pk = mmap_col<int32_t>(gendb_dir + "/part/p_partkey.bin", nk);
            for (size_t i = 0; i < nk; i++) {
                if (strstr(p_nm + i * 56, "green")) {
                    part_bitset.set((uint32_t)p_pk[i]);
                }
            }
        }
    }

    // ---- Phase 2: Build partsupp hash map ----
    {
        GENDB_PHASE("build_joins");
        psmap.init();
        size_t n;
        const int32_t* ps_pk   = mmap_col<int32_t>(gendb_dir + "/partsupp/ps_partkey.bin", n);
        const int32_t* ps_sk   = mmap_col<int32_t>(gendb_dir + "/partsupp/ps_suppkey.bin", n);
        const double*  ps_cost = mmap_col<double>(gendb_dir + "/partsupp/ps_supplycost.bin", n);
        for (size_t i = 0; i < n; i++) {
            uint64_t key = ((uint64_t)(uint32_t)ps_pk[i] << 32) | (uint32_t)ps_sk[i];
            psmap.insert(key, ps_cost[i]);
        }
    }

    // Load pre-built orders hash index and o_orderdate column
    static OrdersIndex oidx;
    size_t n_orders;
    const int32_t* o_orderdate;
    {
        oidx.load(gendb_dir + "/indexes/orders_orderkey_hash.bin");
        o_orderdate = mmap_col<int32_t>(gendb_dir + "/orders/o_orderdate.bin", n_orders);
    }

    // Load lineitem columns
    size_t n_li, nd;
    const int32_t* l_partkey  = mmap_col<int32_t>(gendb_dir + "/lineitem/l_partkey.bin", n_li);
    const int32_t* l_suppkey  = mmap_col<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", n_li);
    const int32_t* l_orderkey = mmap_col<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", n_li);
    const double*  l_extprice = mmap_col<double>(gendb_dir + "/lineitem/l_extendedprice.bin", nd);
    const double*  l_discount = mmap_col<double>(gendb_dir + "/lineitem/l_discount.bin", nd);
    const double*  l_quantity = mmap_col<double>(gendb_dir + "/lineitem/l_quantity.bin", nd);

    // ---- Phase 3: Parallel lineitem scan ----
    // Each thread gets a pre-allocated result buffer (max ~4.8% of its rows)
    SharedData shared;
    shared.l_partkey   = l_partkey;
    shared.l_suppkey   = l_suppkey;
    shared.l_orderkey  = l_orderkey;
    shared.l_extprice  = l_extprice;
    shared.l_discount  = l_discount;
    shared.l_quantity  = l_quantity;
    shared.n_li        = n_li;
    shared.o_orderdate = o_orderdate;
    shared.part_bitset = &part_bitset;
    shared.supp_nk     = supp_nk;
    shared.psmap       = &psmap;
    shared.oidx        = &oidx;

    // Per-thread result buffer: max 120K qualifying rows per thread (expected ~90K)
    static constexpr size_t MAX_RESULTS_PER_THREAD = 150000;
    static QRow result_bufs[NUM_THREADS][MAX_RESULTS_PER_THREAD];
    static ThreadWork workers[NUM_THREADS];
    pthread_t threads[NUM_THREADS];

    {
        GENDB_PHASE("main_scan");
        size_t chunk = (n_li + NUM_THREADS - 1) / NUM_THREADS;
        for (int t = 0; t < NUM_THREADS; t++) {
            workers[t].shared  = &shared;
            workers[t].start   = (size_t)t * chunk;
            workers[t].end     = std::min(workers[t].start + chunk, n_li);
            workers[t].results = result_bufs[t];
            pthread_create(&threads[t], nullptr, thread_func, &workers[t]);
        }
        for (int t = 0; t < NUM_THREADS; t++) pthread_join(threads[t], nullptr);
    }

    // ---- Sequential accumulate in thread order ----
    // This reproduces single-threaded row-order double += to match reference FP results
    {
        GENDB_PHASE("aggregation_merge");
        double agg[NUM_NATIONS][8];
        memset(agg, 0, sizeof(agg));

        for (int t = 0; t < NUM_THREADS; t++) {
            const QRow* rows = result_bufs[t];
            size_t cnt = workers[t].result_count;
            for (size_t i = 0; i < cnt; i++) {
                uint16_t nk_yr = rows[i].nk_yr;
                int nk = nk_yr / 8;
                int yr = nk_yr & 7;
                agg[nk][yr] += rows[i].amount;
            }
        }

        // ---- Phase 4: Output results ----
        {
            GENDB_PHASE("output");

            struct Row { const char* nation; int year; double sum_profit; };
            std::vector<Row> results;
            results.reserve(NUM_NATIONS * NUM_YEARS);

            for (int n = 0; n < NUM_NATIONS; n++) {
                for (int y = 0; y < NUM_YEARS; y++) {
                    if (agg[n][y] != 0.0) {
                        results.push_back({nation_names[n].c_str(), 1992 + y, agg[n][y]});
                    }
                }
            }

            // Sort: nation ASC, o_year DESC
            std::sort(results.begin(), results.end(), [](const Row& a, const Row& b) {
                int cmp = strcmp(a.nation, b.nation);
                if (cmp != 0) return cmp < 0;
                return a.year > b.year;
            });

            // Write CSV
            std::string out_path = results_dir + "/Q9.csv";
            FILE* f = fopen(out_path.c_str(), "w");
            if (!f) { perror("fopen Q9.csv"); exit(1); }
            fprintf(f, "nation,o_year,sum_profit\n");
            for (const auto& r : results) {
                fprintf(f, "%s,%d,%.2f\n", r.nation, r.year, r.sum_profit);
            }
            fclose(f);
        }
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q9(gendb_dir, results_dir);
    return 0;
}
#endif
