// Q18: Large Volume Customer
// Two-pass execution:
//   Pass 1: Parallel scan lineitem → thread-local qty accumulators → merge → build is_big[]
//   Pass 2: Parallel scan orders → filter via is_big[], join to customer → collect results
//   Sort top 100 by o_totalprice DESC, o_orderdate ASC

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
    PhaseTimer(const char* n) : name(n), t0(std::chrono::high_resolution_clock::now()) {}
    ~PhaseTimer() {
        auto t1 = std::chrono::high_resolution_clock::now();
        double ms = std::chrono::duration<double,std::milli>(t1-t0).count();
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
    void prefetch_seq() {
        if (data && size) madvise(data, size, MADV_SEQUENTIAL);
    }
    void prefetch_rand() {
        if (data && size) madvise(data, size, MADV_RANDOM);
    }
    void prefetch_willneed() {
        if (data && size) madvise(data, size, MADV_WILLNEED);
    }
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

// ── Date decode (Howard Hinnant civil-calendar algorithm) ─────────────────
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
    MmapFile f_l_orderkey, f_l_quantity;
    MmapFile f_o_orderkey, f_o_custkey, f_o_orderdate, f_o_totalprice;
    MmapFile f_c_custkey, f_c_name;
    MmapFile f_cust_idx;
    {
        GENDB_PHASE("data_loading");
        if (!f_l_orderkey.open((gendb_dir + "/lineitem/l_orderkey.bin").c_str()))  return 1;
        if (!f_l_quantity.open((gendb_dir + "/lineitem/l_quantity.bin").c_str()))  return 1;
        if (!f_o_orderkey.open((gendb_dir + "/orders/o_orderkey.bin").c_str()))    return 1;
        if (!f_o_custkey.open ((gendb_dir + "/orders/o_custkey.bin").c_str()))     return 1;
        if (!f_o_orderdate.open((gendb_dir + "/orders/o_orderdate.bin").c_str())) return 1;
        if (!f_o_totalprice.open((gendb_dir+"/orders/o_totalprice.bin").c_str())) return 1;
        if (!f_c_custkey.open((gendb_dir + "/customer/c_custkey.bin").c_str()))    return 1;
        if (!f_c_name.open   ((gendb_dir + "/customer/c_name.bin").c_str()))       return 1;
        if (!f_cust_idx.open ((gendb_dir + "/customer/customer_by_custkey.bin").c_str())) return 1;

        // Trigger async kernel readahead for all files
        f_l_orderkey.prefetch_willneed();
        f_l_quantity.prefetch_willneed();
        f_o_orderkey.prefetch_willneed();
        f_o_custkey.prefetch_willneed();
        f_o_orderdate.prefetch_willneed();
        f_o_totalprice.prefetch_willneed();
        f_c_custkey.prefetch_rand();
        f_c_name.prefetch_rand();
        f_cust_idx.prefetch_rand();
    }

    const int32_t* l_orderkey   = (const int32_t*)f_l_orderkey.data;
    const int8_t*  l_quantity   = (const int8_t*) f_l_quantity.data;
    const int32_t* o_orderkey   = (const int32_t*)f_o_orderkey.data;
    const int32_t* o_custkey    = (const int32_t*)f_o_custkey.data;
    const int32_t* o_orderdate  = (const int32_t*)f_o_orderdate.data;
    const double*  o_totalprice = (const double*) f_o_totalprice.data;
    const int32_t* c_custkey    = (const int32_t*)f_c_custkey.data;
    const char*    c_name_base  = (const char*)   f_c_name.data;
    const int32_t* cust_idx     = (const int32_t*)f_cust_idx.data;

    const size_t N_lineitem = f_l_orderkey.size / sizeof(int32_t);
    const size_t N_orders   = f_o_orderkey.size / sizeof(int32_t);

    const int nthreads = (int)std::thread::hardware_concurrency();

    // Key space: l_orderkey in [1, 60000000]
    constexpr size_t ARR_SIZE = 60000001;

    // ── Phase: pass1_lineitem_scan_aggregate ──────────────────────────────
    // Each thread owns a calloc'd partial qty array (lazy physical allocation).
    // Physical RAM per thread ≈ (morsels_touched / total_pages) × 229MB — far less than 229MB.
    std::vector<int32_t*> tl_qty(nthreads, nullptr);
    {
        GENDB_PHASE("pass1_lineitem_scan_aggregate");

        for (int t = 0; t < nthreads; t++) {
            tl_qty[t] = (int32_t*)calloc(ARR_SIZE, sizeof(int32_t));
            if (!tl_qty[t]) {
                fprintf(stderr, "OOM: failed to allocate thread-local qty array (thread %d)\n", t);
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
                int32_t* qty = tl_qty[t];
                for (size_t i = lo; i < hi; i++) {
                    qty[l_orderkey[i]] += (int32_t)l_quantity[i];
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    // ── Phase: build_is_big_bool_array ────────────────────────────────────
    // Parallel merge of thread-local arrays into tl_qty[0], then build is_big[].
    // is_big uses char (1 byte per element) for fast cache-friendly lookup.
    std::vector<char> is_big(ARR_SIZE, 0);
    {
        GENDB_PHASE("build_is_big_bool_array");

        int32_t* merged = tl_qty[0];

        // Parallel element-wise merge: each thread handles a key-space slice
        {
            std::vector<std::thread> threads;
            threads.reserve(nthreads);
            const size_t kchunk = (ARR_SIZE + nthreads - 1) / nthreads;

            for (int t = 0; t < nthreads; t++) {
                threads.emplace_back([&, t]() {
                    const size_t lo = (size_t)t * kchunk;
                    const size_t hi = std::min(lo + kchunk, ARR_SIZE);
                    if (lo >= hi) return;

                    // Accumulate contributions from threads 1..nthreads-1
                    for (int s = 1; s < nthreads; s++) {
                        const int32_t* src = tl_qty[s];
                        for (size_t k = lo; k < hi; k++) {
                            merged[k] += src[k];
                        }
                    }

                    // Build is_big[] in the same pass
                    char* ib = is_big.data();
                    for (size_t k = lo; k < hi; k++) {
                        ib[k] = (merged[k] > 300) ? 1 : 0;
                    }
                });
            }
            for (auto& th : threads) th.join();
        }

        // Free thread-local arrays 1..nthreads-1; keep tl_qty[0] for sum_qty lookup
        for (int t = 1; t < nthreads; t++) {
            free(tl_qty[t]);
            tl_qty[t] = nullptr;
        }
    }

    int32_t* qty_merged = tl_qty[0];

    // ── Phase: pass2_orders_scan_filter + join_orders_to_customer ─────────
    std::vector<std::vector<Q18Result>> tl_results(nthreads);
    {
        GENDB_PHASE("pass2_orders_scan_filter");

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        const size_t chunk = (N_orders + nthreads - 1) / nthreads;

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                const size_t lo = (size_t)t * chunk;
                const size_t hi = std::min(lo + chunk, N_orders);
                if (lo >= hi) return;

                auto& local = tl_results[t];
                local.reserve(64);

                for (size_t j = lo; j < hi; j++) {
                    const int32_t okey = o_orderkey[j];
                    if (!is_big[okey]) continue;

                    // Join to customer via customer_by_custkey index
                    const int32_t ckey = o_custkey[j];
                    const int32_t crow = cust_idx[ckey];
                    if (__builtin_expect(crow < 0, 0)) continue; // safety: sentinel

                    Q18Result r;
                    memcpy(r.c_name, c_name_base + (size_t)crow * 26, 26);
                    r.c_custkey    = c_custkey[crow];
                    r.o_orderkey   = okey;
                    r.o_orderdate  = o_orderdate[j];
                    r.o_totalprice = o_totalprice[j];
                    r.sum_qty      = qty_merged[okey];
                    local.push_back(r);
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    // ── Phase: assemble_result_structs ────────────────────────────────────
    std::vector<Q18Result> results;
    {
        GENDB_PHASE("assemble_result_structs");
        size_t total = 0;
        for (const auto& v : tl_results) total += v.size();
        results.reserve(total);
        for (auto& v : tl_results) {
            for (auto& r : v) results.push_back(r);
        }
    }

    // ── Phase: topk_sort ──────────────────────────────────────────────────
    {
        GENDB_PHASE("topk_sort");
        const size_t K = std::min((size_t)100, results.size());
        std::partial_sort(results.begin(), results.begin() + K, results.end(),
            [](const Q18Result& a, const Q18Result& b) {
                if (a.o_totalprice != b.o_totalprice)
                    return a.o_totalprice > b.o_totalprice;   // DESC
                return a.o_orderdate < b.o_orderdate;          // ASC
            });
        results.resize(K);
    }

    // ── Phase: output ─────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        {
            std::string mkdirp = "mkdir -p \"" + results_dir + "\"";
            system(mkdirp.c_str());
        }

        std::string out_path = results_dir + "/Q18.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror(out_path.c_str()); return 1; }

        fprintf(out, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        char date_buf[11];
        for (const auto& r : results) {
            epoch_days_to_date_str(r.o_orderdate, date_buf);
            fprintf(out, "%.25s,%d,%d,%s,%.2f,%d\n",
                r.c_name, r.c_custkey, r.o_orderkey,
                date_buf, r.o_totalprice, r.sum_qty);
        }
        fclose(out);
    }

    free(qty_merged);

    // ── Total timing ──────────────────────────────────────────────────────
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double,std::milli>(t_total_end - t_total_start).count();
    fprintf(stderr, "[GENDB_PHASE] total: %.3f ms\n", total_ms);

    return 0;
}
