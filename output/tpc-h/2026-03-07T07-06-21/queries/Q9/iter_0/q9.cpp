// Q9: Product Type Profit Measure
// SELECT nation, o_year, SUM(amount) FROM ... WHERE p_name LIKE '%green%' GROUP BY nation, o_year ORDER BY nation ASC, o_year DESC

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
#include <array>
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
    MmapFile f_orders_idx, f_orderdate;
    MmapFile f_ps_ht, f_ps_supplycost;
    {
        GENDB_PHASE("data_loading");
        if (!f_lpartkey.open(   (gendb_dir + "/lineitem/l_partkey.bin").c_str()))          return 1;
        if (!f_lsuppkey.open(   (gendb_dir + "/lineitem/l_suppkey.bin").c_str()))          return 1;
        if (!f_lorderkey.open(  (gendb_dir + "/lineitem/l_orderkey.bin").c_str()))         return 1;
        if (!f_lextprice.open(  (gendb_dir + "/lineitem/l_extendedprice.bin").c_str()))    return 1;
        if (!f_ldiscount.open(  (gendb_dir + "/lineitem/l_discount.bin").c_str()))         return 1;
        if (!f_lquantity.open(  (gendb_dir + "/lineitem/l_quantity.bin").c_str()))         return 1;
        // Random-access large indexes
        if (!f_orders_idx.open( (gendb_dir + "/orders/orders_by_orderkey.bin").c_str(), true))   return 1;
        if (!f_orderdate.open(  (gendb_dir + "/orders/o_orderdate.bin").c_str(), true))          return 1;
        if (!f_ps_ht.open(      (gendb_dir + "/partsupp/partsupp_hash_index.bin").c_str(), true)) return 1;
        if (!f_ps_supplycost.open((gendb_dir + "/partsupp/ps_supplycost.bin").c_str(), true))     return 1;
    }

    const int32_t* l_partkey        = (const int32_t*)f_lpartkey.data;
    const int32_t* l_suppkey        = (const int32_t*)f_lsuppkey.data;
    const int32_t* l_orderkey       = (const int32_t*)f_lorderkey.data;
    const double*  l_extendedprice  = (const double*) f_lextprice.data;
    const int8_t*  l_discount       = (const int8_t*) f_ldiscount.data;
    const int8_t*  l_quantity       = (const int8_t*) f_lquantity.data;
    const size_t   total_rows       = f_lpartkey.size / sizeof(int32_t);

    const int32_t* orders_by_orderkey = (const int32_t*)f_orders_idx.data;
    const int32_t* o_orderdate        = (const int32_t*)f_orderdate.data;

    // partsupp hash table: 16-byte header (capacity + count), then slots
    const HTSlot*  ps_ht         = (const HTSlot*)((const char*)f_ps_ht.data + 16);
    const double*  ps_supplycost = (const double*) f_ps_supplycost.data;

    // ── Phase: dim_filter — build green_part + suppkey_to_nationrow ────────
    static uint8_t green_part[2000001];  // 2 MB, L3-friendly
    memset(green_part, 0, sizeof(green_part));

    int32_t suppkey_to_nationrow[100001];
    memset(suppkey_to_nationrow, -1, sizeof(suppkey_to_nationrow));

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
            // p_name is fixed 56-byte records
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
                    if (pk >= 0 && pk <= 2000000)
                        green_part[pk] = 1;
                }
            }
        }

        // --- Build suppkey_to_nationrow ---
        {
            // Load supplier_by_suppkey (100001 int32_t entries)
            std::vector<int32_t> supp_by_suppkey(100001);
            {
                FILE* fp = fopen((gendb_dir + "/supplier/supplier_by_suppkey.bin").c_str(), "rb");
                if (!fp) { perror("supplier_by_suppkey.bin"); return 1; }
                fread(supp_by_suppkey.data(), sizeof(int32_t), 100001, fp);
                fclose(fp);
            }
            // Load s_nationkey (100000 int32_t entries)
            std::vector<int32_t> s_nationkey(100000);
            {
                FILE* fp = fopen((gendb_dir + "/supplier/s_nationkey.bin").c_str(), "rb");
                if (!fp) { perror("s_nationkey.bin"); return 1; }
                fread(s_nationkey.data(), sizeof(int32_t), 100000, fp);
                fclose(fp);
            }
            // Load nation_by_nationkey (25 int32_t entries)
            int32_t nat_by_nkey[25];
            {
                FILE* fp = fopen((gendb_dir + "/nation/nation_by_nationkey.bin").c_str(), "rb");
                if (!fp) { perror("nation_by_nationkey.bin"); return 1; }
                fread(nat_by_nkey, sizeof(int32_t), 25, fp);
                fclose(fp);
            }
            // Load n_name (25 × 26 bytes)
            {
                FILE* fp = fopen((gendb_dir + "/nation/n_name.bin").c_str(), "rb");
                if (!fp) { perror("n_name.bin"); return 1; }
                fread(nation_names, 26, 25, fp);
                fclose(fp);
            }
            // Precompute suppkey → nationrow (nrow = nat_by_nkey[s_nationkey[srow]])
            for (int suppkey = 0; suppkey <= 100000; suppkey++) {
                int32_t srow = supp_by_suppkey[suppkey];
                if (srow < 0) continue;
                int32_t nkey = s_nationkey[srow];
                if (nkey < 0 || nkey >= 25) continue;
                suppkey_to_nationrow[suppkey] = nat_by_nkey[nkey];
            }
        }
    }

    // ── Phase: build_joins (structures built above) ────────────────────────
    // (All lookup structures ready — no additional join phase needed)

    // ── Phase: main_scan (parallel morsel-driven) ──────────────────────────
    const int nthreads = (int)std::thread::hardware_concurrency();
    // Per-thread profits[25][10]: nation row × year offset (1992=0..1998=6)
    // Align to 64-byte cache lines to avoid false sharing
    struct alignas(64) ThreadProfits {
        double v[25][10];
    };
    std::vector<ThreadProfits> thread_profits(nthreads);
    for (auto& tp : thread_profits)
        memset(tp.v, 0, sizeof(tp.v));

    {
        GENDB_PHASE("main_scan");

        constexpr size_t MORSEL = 100000;
        std::atomic<size_t> morsel_idx{0};

        // Snapshot raw pointers for capture
        const uint8_t* gp  = green_part;
        const int32_t* s2n = suppkey_to_nationrow;

        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                double profits[25][10];
                memset(profits, 0, sizeof(profits));

                while (true) {
                    size_t start = morsel_idx.fetch_add(MORSEL, std::memory_order_relaxed);
                    if (start >= total_rows) break;
                    size_t end = std::min(start + MORSEL, total_rows);

                    for (size_t i = start; i < end; i++) {
                        int32_t lpartkey = l_partkey[i];
                        if (!gp[lpartkey]) continue;

                        int32_t lsuppkey = l_suppkey[i];
                        int32_t nrow = s2n[lsuppkey];
                        if (__builtin_expect(nrow < 0, 0)) continue;

                        int32_t orow     = orders_by_orderkey[l_orderkey[i]];
                        int32_t odate    = o_orderdate[orow];
                        int     year_off = extract_year(odate) - 1992;
                        if (__builtin_expect((unsigned)year_off >= 10u, 0)) continue;

                        double sc     = lookup_supplycost(lpartkey, lsuppkey, ps_ht, ps_supplycost);
                        double disc   = (double)l_discount[i] * 0.01;
                        double amount = l_extendedprice[i] * (1.0 - disc)
                                        - sc * (double)(uint8_t)l_quantity[i];

                        profits[nrow][year_off] += amount;
                    }
                }

                // Copy to shared per-thread storage
                memcpy(thread_profits[t].v, profits, sizeof(profits));
            });
        }
        for (auto& th : threads) th.join();
    }

    // ── Phase: aggregate_reduce ────────────────────────────────────────────
    double global_profits[25][10];
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
            for (int y = 0; y < 7; y++) {  // years 1992–1998
                double p = global_profits[n][y];
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
        for (const auto& r : results) {
            fprintf(out, "%s,%d,%.2f\n", r.nation, r.year, r.profit);
        }
        fclose(out);
    }

    // ── Total timing ──────────────────────────────────────────────────────
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms  = std::chrono::duration<double,std::milli>(t_total_end - t_total_start).count();
    fprintf(stderr, "[GENDB_PHASE] total: %.3f ms\n", total_ms);

    return 0;
}
