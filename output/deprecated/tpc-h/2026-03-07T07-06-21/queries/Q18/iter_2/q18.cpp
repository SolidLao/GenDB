// Q18: Large Volume Customer — iter_2
//
// Pipeline (iter_2 optimisations):
//   (A) int16_t thread-local accumulators: 114MB/thread vs 229MB → halves merge bandwidth
//   (B) Direct O(1) orders index lookup: replaces 15M-row orders scan (~300MB) with ~2520 probes
//   (C) Qualifying keys collected inside merge pass → no is_big[] char array (57MB eliminated)
//
// Execution stages:
//   1. Parallel morsel scan of lineitem → per-thread int16_t[60M] qty accumulators (no atomics)
//   2. Parallel element-wise merge into tl_qty[0] (key-space partitioned, no races);
//      collect qualifying orderkeys (merged sum > 300) in same pass
//   3. For each qualifying orderkey: orders_by_orderkey[okey] → orders row in O(1)
//      then customer_by_custkey[o_custkey] → customer row in O(1)
//   4. partial_sort top-100 by o_totalprice DESC, o_orderdate ASC

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include <thread>
#include <string>
#include <chrono>

// ── GENDB_PHASE timing ────────────────────────────────────────────────────
#ifdef GENDB_PROFILE
struct PhaseTimer {
    const char* name;
    std::chrono::high_resolution_clock::time_point t0;
    explicit PhaseTimer(const char* n)
        : name(n), t0(std::chrono::high_resolution_clock::now()) {}
    ~PhaseTimer() {
        auto t1 = std::chrono::high_resolution_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        fprintf(stderr, "[GENDB_PHASE] %s: %.3f ms\n", name, ms);
    }
};
#define GENDB_PHASE(name) PhaseTimer _phase_timer_##__LINE__(name)
#else
#define GENDB_PHASE(name)
#endif

// ── mmap helper ───────────────────────────────────────────────────────────
struct MmapFile {
    void*  data = nullptr;
    size_t size = 0;
    int    fd   = -1;

    bool open(const char* path) {
        fd = ::open(path, O_RDONLY);
        if (fd < 0) { perror(path); return false; }
        struct stat st; fstat(fd, &st);
        size = (size_t)st.st_size;
        if (size == 0) return true;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); data = nullptr; return false; }
        return true;
    }
    void advise_seq()      { if (data && size) madvise(data, size, MADV_SEQUENTIAL); }
    void advise_rand()     { if (data && size) madvise(data, size, MADV_RANDOM); }
    void advise_willneed() { if (data && size) madvise(data, size, MADV_WILLNEED); }
    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) close(fd);
    }
};

// ── Result struct ─────────────────────────────────────────────────────────
struct Q18Result {
    char    c_name[26];
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;   // days since epoch
    double  o_totalprice;
    int32_t sum_qty;
};

// ── Date decode: days-since-epoch → "YYYY-MM-DD" ──────────────────────────
// Howard Hinnant civil-calendar algorithm
static void epoch_days_to_date_str(int32_t z, char* buf) {
    int32_t zz = z + 719468;
    int era = (zz >= 0 ? zz : zz - 146096) / 146097;
    unsigned doe = (unsigned)(zz - era * 146097);
    unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int y = (int)yoe + era * 400;
    unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
    unsigned mp  = (5*doy + 2) / 153;
    unsigned d   = doy - (153*mp + 2)/5 + 1;
    unsigned m   = mp < 10 ? mp + 3 : mp - 9;
    y += (m <= 2);
    snprintf(buf, 11, "%04d-%02d-%02d", y, (int)m, (int)d);
}

int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    auto t_total_start = std::chrono::high_resolution_clock::now();

    // ── Phase: data_loading ───────────────────────────────────────────────
    // lineitem columns (sequential scan)
    MmapFile f_l_orderkey, f_l_quantity;
    // orders dense index (random access, 240MB) + payload columns
    MmapFile f_orders_idx, f_o_custkey, f_o_orderdate, f_o_totalprice;
    // customer dense index (5.7MB, L3-resident) + payload columns
    MmapFile f_cust_idx, f_c_custkey, f_c_name;
    {
        GENDB_PHASE("data_loading");

        auto p = [&](const char* rel) { return gendb_dir + "/" + rel; };

        // Lineitem (large sequential scans)
        if (!f_l_orderkey.open(p("lineitem/l_orderkey.bin").c_str())) return 1;
        if (!f_l_quantity.open(p("lineitem/l_quantity.bin").c_str()))  return 1;
        f_l_orderkey.advise_willneed();
        f_l_quantity.advise_willneed();

        // Orders: dense index for direct lookup; payload accessed randomly via index
        if (!f_orders_idx.open(p("orders/orders_by_orderkey.bin").c_str())) return 1;
        if (!f_o_custkey.open  (p("orders/o_custkey.bin").c_str()))          return 1;
        if (!f_o_orderdate.open(p("orders/o_orderdate.bin").c_str()))        return 1;
        if (!f_o_totalprice.open(p("orders/o_totalprice.bin").c_str()))      return 1;
        f_orders_idx.advise_rand();
        f_o_custkey.advise_rand();
        f_o_orderdate.advise_rand();
        f_o_totalprice.advise_rand();

        // Customer (small; L3-resident index)
        if (!f_cust_idx.open(p("customer/customer_by_custkey.bin").c_str())) return 1;
        if (!f_c_custkey.open(p("customer/c_custkey.bin").c_str()))           return 1;
        if (!f_c_name.open   (p("customer/c_name.bin").c_str()))              return 1;
        f_cust_idx.advise_willneed();
        f_c_custkey.advise_rand();
        f_c_name.advise_rand();
    }

    // Typed pointers
    const int32_t* l_orderkey     = (const int32_t*)f_l_orderkey.data;
    const int8_t*  l_quantity     = (const int8_t*) f_l_quantity.data;
    const int32_t* orders_by_okey = (const int32_t*)f_orders_idx.data;
    const int32_t* o_custkey      = (const int32_t*)f_o_custkey.data;
    const int32_t* o_orderdate    = (const int32_t*)f_o_orderdate.data;
    const double*  o_totalprice   = (const double*) f_o_totalprice.data;
    const int32_t* cust_by_ckey   = (const int32_t*)f_cust_idx.data;
    const int32_t* c_custkey_col  = (const int32_t*)f_c_custkey.data;
    const char*    c_name_base    = (const char*)   f_c_name.data;

    const size_t N_lineitem = f_l_orderkey.size / sizeof(int32_t);

    const int nthreads = (int)std::thread::hardware_concurrency();

    // Key space: orderkeys in [1, 60_000_000]
    constexpr size_t ARR_SIZE = 60000001;

    // ── Phase: pass1_lineitem_scan_aggregate_int16 ────────────────────────
    // Each thread gets its own calloc'd int16_t[ARR_SIZE] accumulator.
    // int16_t is safe: max sum per orderkey across ALL lineitems = 7×50 = 350 < 32767.
    // 114MB per thread vs 229MB (int32_t) → halves merge memory traffic.
    std::vector<int16_t*> tl_qty(nthreads, nullptr);
    {
        GENDB_PHASE("pass1_lineitem_scan_aggregate_int16");

        for (int t = 0; t < nthreads; t++) {
            tl_qty[t] = (int16_t*)calloc(ARR_SIZE, sizeof(int16_t));
            if (!tl_qty[t]) {
                fprintf(stderr, "OOM: calloc failed for thread %d\n", t);
                return 1;
            }
        }

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        const size_t chunk = (N_lineitem + nthreads - 1) / nthreads;

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                const size_t lo = (size_t)t * chunk;
                const size_t hi = std::min(lo + chunk, N_lineitem);
                if (lo >= hi) return;
                int16_t* qty = tl_qty[t];
                // Hot inner loop: two sequential reads + array update
                for (size_t i = lo; i < hi; i++) {
                    qty[(uint32_t)l_orderkey[i]] += (int16_t)l_quantity[i];
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    // ── Phase: merge_thread_local_arrays_collect_qualifying_keys ─────────
    // Parallel element-wise merge into tl_qty[0].
    // Each thread owns a disjoint key-space slice [lo, hi) → no races on tl_qty[0].
    // Qualifying keys (merged sum > 300) collected in the same sequential pass:
    // no separate is_big[] array needed.
    std::vector<std::vector<int32_t>> per_thread_keys(nthreads);
    {
        GENDB_PHASE("merge_thread_local_arrays_collect_qualifying_keys");

        int16_t* merged = tl_qty[0];

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        const size_t kchunk = (ARR_SIZE + nthreads - 1) / nthreads;

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                const size_t lo = (size_t)t * kchunk;
                const size_t hi = std::min(lo + kchunk, ARR_SIZE);
                if (lo >= hi) return;

                // Merge contributions from threads 1..nthreads-1 into tl_qty[0]
                for (int s = 1; s < nthreads; s++) {
                    const int16_t* src = tl_qty[s];
                    for (size_t k = lo; k < hi; k++) {
                        merged[k] += src[k];
                    }
                }

                // Collect qualifying keys in the same pass (HAVING SUM > 300)
                std::vector<int32_t>& local_keys = per_thread_keys[t];
                local_keys.reserve(48);
                for (size_t k = lo; k < hi; k++) {
                    if (merged[k] > 300) {
                        local_keys.push_back((int32_t)k);
                    }
                }
            });
        }
        for (auto& th : threads) th.join();

        // Free thread-local arrays 1..nthreads-1; keep tl_qty[0] for sum_qty output
        for (int t = 1; t < nthreads; t++) {
            free(tl_qty[t]);
            tl_qty[t] = nullptr;
        }
    }

    // Concatenate per-thread qualifying keys (~2520 total)
    std::vector<int32_t> qualifying_keys;
    qualifying_keys.reserve(3000);
    for (int t = 0; t < nthreads; t++) {
        qualifying_keys.insert(qualifying_keys.end(),
                               per_thread_keys[t].begin(),
                               per_thread_keys[t].end());
    }

    int16_t* qty_merged = tl_qty[0]; // used below for sum_qty output

    // ── Phase: direct_index_lookup_orders + join_orders_to_customer ───────
    // For each of the ~2520 qualifying orderkeys:
    //   orders_by_okey[okey] → orders row (O(1), random access into 240MB index)
    //   cust_by_ckey[o_custkey[orow]] → customer row (O(1), 5.7MB L3-resident)
    // Total: ~2520 index probes, negligible cost.
    std::vector<Q18Result> results;
    results.reserve(qualifying_keys.size());
    {
        GENDB_PHASE("direct_index_lookup_orders_join_customer");

        for (int32_t okey : qualifying_keys) {
            // Bounds guard (should never trigger with valid TPC-H data)
            if (__builtin_expect((uint32_t)okey >= (uint32_t)ARR_SIZE, 0)) continue;

            int32_t orow = orders_by_okey[okey];
            if (__builtin_expect(orow < 0, 0)) continue; // sentinel

            int32_t custkey  = o_custkey[orow];
            int32_t cust_row = cust_by_ckey[custkey];
            if (__builtin_expect(cust_row < 0, 0)) continue; // sentinel

            Q18Result r;
            memcpy(r.c_name, c_name_base + (size_t)cust_row * 26, 26);
            r.c_custkey    = c_custkey_col[cust_row];
            r.o_orderkey   = okey;
            r.o_orderdate  = o_orderdate[orow];
            r.o_totalprice = o_totalprice[orow];
            // qty_merged[okey] holds the fully-merged int16_t sum; cast to int32_t
            r.sum_qty      = (int32_t)(uint16_t)qty_merged[okey];
            results.push_back(r);
        }
    }

    // ── Phase: topk_sort ──────────────────────────────────────────────────
    {
        GENDB_PHASE("topk_sort");
        const size_t K = std::min((size_t)100, results.size());
        std::partial_sort(results.begin(), results.begin() + K, results.end(),
            [](const Q18Result& a, const Q18Result& b) {
                if (a.o_totalprice != b.o_totalprice)
                    return a.o_totalprice > b.o_totalprice;  // DESC
                return a.o_orderdate < b.o_orderdate;         // ASC
            });
        results.resize(K);
    }

    // ── Phase: output ─────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        {
            std::string cmd = "mkdir -p \"" + results_dir + "\"";
            (void)system(cmd.c_str());
        }

        std::string out_path = results_dir + "/Q18.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror(out_path.c_str()); return 1; }

        // Header
        fprintf(out, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        char date_buf[11];
        for (const auto& r : results) {
            epoch_days_to_date_str(r.o_orderdate, date_buf);
            // c_name: 26-byte fixed field, null-padded; print up to 25 chars
            fprintf(out, "%.25s,%d,%d,%s,%.2f,%.2f\n",
                    r.c_name,
                    r.c_custkey,
                    r.o_orderkey,
                    date_buf,
                    r.o_totalprice,
                    (double)r.sum_qty);
        }
        fclose(out);
    }

    // Cleanup
    free(qty_merged);

    // ── Total timing ──────────────────────────────────────────────────────
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    fprintf(stderr, "[GENDB_PHASE] total: %.3f ms\n", total_ms);

    return 0;
}
