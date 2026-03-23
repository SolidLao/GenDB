// Q18: Large Volume Customer — iter_1 (Shared Atomic int16_t Accumulator)
//
// Key optimization over iter_0:
//   iter_0 allocated 64 thread-local int32_t[60000001] arrays (~14.7 GB total),
//   then merged them with a parallel element-wise reduction (~14.4 GB memory traffic).
//   iter_1 replaces all thread-local arrays with a single shared
//   std::atomic<int16_t>[60000001] (~120 MB) mmapped MAP_ANONYMOUS|MAP_PRIVATE.
//   All 64 threads write directly via memory_order_relaxed fetch_add.
//   No merge phase. int16_t is correct: max sum = 7 rows × 50 qty = 350 < 32767.
//
// Pipeline:
//   pass1_lineitem_scan_aggregate_atomic → build_is_big_bool_array
//   → pass2_orders_scan_filter → join_orders_to_customer
//   → assemble_result_structs → topk_sort → output

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <atomic>
#include <vector>
#include <thread>
#include <algorithm>
#include <string>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// ── GENDB_PHASE timing ────────────────────────────────────────────────────
#ifdef GENDB_PROFILE
struct PhaseTimer {
    const char* name;
    std::chrono::high_resolution_clock::time_point t0;
    PhaseTimer(const char* n) : name(n), t0(std::chrono::high_resolution_clock::now()) {}
    ~PhaseTimer() {
        auto t1 = std::chrono::high_resolution_clock::now();
        double ms = std::chrono::duration<double,std::milli>(t1 - t0).count();
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

    bool open(const char* path, bool sequential = true) {
        fd = ::open(path, O_RDONLY);
        if (fd < 0) { perror(path); return false; }
        struct stat st; fstat(fd, &st);
        size = (size_t)st.st_size;
        if (size == 0) return true;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); data = nullptr; return false; }
        if (sequential)
            madvise(data, size, MADV_SEQUENTIAL);
        return true;
    }
    void prefetch() {
        if (data && size) madvise(data, size, MADV_WILLNEED);
    }
    void advise_random() {
        if (data && size) madvise(data, size, MADV_RANDOM);
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

    const int nthreads = (int)std::thread::hardware_concurrency();

    // Key space: l_orderkey in [1, 60000000]
    constexpr size_t ARR_SIZE = 60000001;

    // ── Phase: data_loading ───────────────────────────────────────────────
    MmapFile f_l_orderkey, f_l_quantity;
    MmapFile f_o_orderkey, f_o_custkey, f_o_orderdate, f_o_totalprice;
    MmapFile f_c_custkey, f_c_name;
    MmapFile f_cust_idx;
    {
        GENDB_PHASE("data_loading");
        if (!f_l_orderkey.open((gendb_dir + "/lineitem/l_orderkey.bin").c_str()))   return 1;
        if (!f_l_quantity.open((gendb_dir + "/lineitem/l_quantity.bin").c_str()))   return 1;
        if (!f_o_orderkey.open((gendb_dir + "/orders/o_orderkey.bin").c_str()))     return 1;
        if (!f_o_custkey.open ((gendb_dir + "/orders/o_custkey.bin").c_str()))      return 1;
        if (!f_o_orderdate.open((gendb_dir + "/orders/o_orderdate.bin").c_str()))   return 1;
        if (!f_o_totalprice.open((gendb_dir+"/orders/o_totalprice.bin").c_str()))   return 1;
        if (!f_c_custkey.open((gendb_dir + "/customer/c_custkey.bin").c_str()))     return 1;
        if (!f_c_name.open   ((gendb_dir + "/customer/c_name.bin").c_str()))        return 1;
        if (!f_cust_idx.open ((gendb_dir + "/customer/customer_by_custkey.bin").c_str())) return 1;

        // Fire async kernel readahead for sequential columns
        f_l_orderkey.prefetch();
        f_l_quantity.prefetch();
        f_o_orderkey.prefetch();
        f_o_custkey.prefetch();
        f_o_orderdate.prefetch();
        f_o_totalprice.prefetch();
        // Random-access columns: customer index and name
        f_c_custkey.advise_random();
        f_c_name.advise_random();
        f_cust_idx.advise_random();
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

    // ── Allocate shared atomic int16_t accumulator (MAP_ANONYMOUS → zero-init) ─
    // ~120 MB total. All threads write directly via relaxed fetch_add.
    // int16_t is sufficient: max sum per orderkey = 7 × 50 = 350 < 32767.
    const size_t atomic_bytes = ARR_SIZE * sizeof(std::atomic<int16_t>);
    std::atomic<int16_t>* qty_by_order = (std::atomic<int16_t>*)mmap(
        nullptr, atomic_bytes,
        PROT_READ | PROT_WRITE,
        MAP_ANONYMOUS | MAP_PRIVATE,
        -1, 0);
    if (qty_by_order == MAP_FAILED) {
        fprintf(stderr, "mmap failed for qty_by_order (%zu bytes)\n", atomic_bytes);
        return 1;
    }
    // MAP_ANONYMOUS guarantees zero-initialization (kernel page fault on first touch)

    // ── Phase: pass1_lineitem_scan_aggregate_atomic ───────────────────────
    // All threads share one atomic array; no per-thread copies, no merge phase.
    // Atomic contention negligible: P(collision) ≈ 64²/60M ≈ 0.007%.
    {
        GENDB_PHASE("pass1_lineitem_scan_aggregate_atomic");

        // Morsel-driven: each thread grabs a block index atomically
        const size_t BLOCK_SIZE = 131072;  // ~128K rows per morsel
        const size_t total_blocks = (N_lineitem + BLOCK_SIZE - 1) / BLOCK_SIZE;
        std::atomic<size_t> next_block{0};

        auto pass1_worker = [&](int /*tid*/) {
            while (true) {
                size_t blk = next_block.fetch_add(1, std::memory_order_relaxed);
                if (blk >= total_blocks) break;
                const size_t lo = blk * BLOCK_SIZE;
                const size_t hi = std::min(lo + BLOCK_SIZE, N_lineitem);

                for (size_t i = lo; i < hi; i++) {
                    qty_by_order[l_orderkey[i]].fetch_add(
                        (int16_t)l_quantity[i],
                        std::memory_order_relaxed);
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back(pass1_worker, t);
        }
        for (auto& th : threads) th.join();
    }

    // ── Phase: build_is_big_bool_array ────────────────────────────────────
    // Parallel scan of shared accumulator: is_big[k] = (qty > 300).
    // Each thread handles a key-space slice — zero contention.
    // ~57 MB char array for O(1) membership test in Pass 2.
    const size_t is_big_bytes = ARR_SIZE * sizeof(char);
    char* is_big = (char*)mmap(
        nullptr, is_big_bytes,
        PROT_READ | PROT_WRITE,
        MAP_ANONYMOUS | MAP_PRIVATE,
        -1, 0);
    if (is_big == MAP_FAILED) {
        fprintf(stderr, "mmap failed for is_big\n");
        return 1;
    }
    {
        GENDB_PHASE("build_is_big_bool_array");

        const size_t BLOCK_SIZE = 1048576;  // 1M keys per morsel
        const size_t total_blocks = (ARR_SIZE + BLOCK_SIZE - 1) / BLOCK_SIZE;
        std::atomic<size_t> next_block{0};

        auto build_worker = [&](int /*tid*/) {
            while (true) {
                size_t blk = next_block.fetch_add(1, std::memory_order_relaxed);
                if (blk >= total_blocks) break;
                const size_t lo = blk * BLOCK_SIZE;
                const size_t hi = std::min(lo + BLOCK_SIZE, ARR_SIZE);

                for (size_t k = lo; k < hi; k++) {
                    is_big[k] = (qty_by_order[k].load(std::memory_order_relaxed) > 300) ? 1 : 0;
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back(build_worker, t);
        }
        for (auto& th : threads) th.join();
    }

    // ── Phase: pass2_orders_scan_filter + join_orders_to_customer ─────────
    // Parallel morsel scan of orders; each thread collects a local result vector.
    // ~2,520 rows qualify from 15M — very selective.
    std::vector<std::vector<Q18Result>> tl_results(nthreads);
    {
        GENDB_PHASE("pass2_orders_scan_filter");

        const size_t BLOCK_SIZE = 131072;
        const size_t total_blocks = (N_orders + BLOCK_SIZE - 1) / BLOCK_SIZE;
        std::atomic<size_t> next_block{0};

        auto pass2_worker = [&](int tid) {
            auto& local = tl_results[tid];
            local.reserve(64);

            while (true) {
                size_t blk = next_block.fetch_add(1, std::memory_order_relaxed);
                if (blk >= total_blocks) break;
                const size_t lo = blk * BLOCK_SIZE;
                const size_t hi = std::min(lo + BLOCK_SIZE, N_orders);

                for (size_t j = lo; j < hi; j++) {
                    const int32_t okey = o_orderkey[j];
                    // O(1) membership test via dense char array
                    if (!is_big[okey]) continue;

                    // Join to customer via dense index (only ~2,520 lookups total)
                    const int32_t ckey = o_custkey[j];
                    const int32_t crow = cust_idx[ckey];
                    if (__builtin_expect(crow < 0, 0)) continue;  // sentinel guard

                    Q18Result r;
                    memcpy(r.c_name, c_name_base + (size_t)crow * 26, 26);
                    r.c_custkey    = c_custkey[crow];
                    r.o_orderkey   = okey;
                    r.o_orderdate  = o_orderdate[j];
                    r.o_totalprice = o_totalprice[j];
                    // Reuse Pass 1 accumulator — no second lineitem scan needed
                    r.sum_qty = (int32_t)qty_by_order[okey].load(std::memory_order_relaxed);
                    local.push_back(r);
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back(pass2_worker, t);
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
            for (auto& r : v) results.push_back(std::move(r));
            v.clear();
            v.shrink_to_fit();
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

        // Ensure output directory exists
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

    // ── Cleanup ───────────────────────────────────────────────────────────
    munmap(qty_by_order, atomic_bytes);
    munmap(is_big, is_big_bytes);

    // ── Total timing ──────────────────────────────────────────────────────
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double,std::milli>(t_total_end - t_total_start).count();
    fprintf(stderr, "[GENDB_PHASE] total: %.3f ms\n", total_ms);

    return 0;
}
