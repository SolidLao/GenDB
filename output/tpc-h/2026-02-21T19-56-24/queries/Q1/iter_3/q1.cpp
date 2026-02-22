#include <cstdint>
#include <cstring>
#include <algorithm>
#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <omp.h>

#include "timing_utils.h"

// ─── mmap helper (async: fadvise + madvise, NO MAP_POPULATE) ────────────────
// Removes the blocking MAP_POPULATE cost. Instead:
//   1. posix_fadvise(SEQUENTIAL) instructs the kernel to issue aggressive readahead.
//   2. madvise(MADV_WILLNEED) triggers async prefetch without blocking.
//   3. madvise(MADV_SEQUENTIAL) keeps kernel readahead active during scan.
// Page faults are resolved lazily during the OMP scan (soft faults when data is
// already in page cache, or async HDD reads driven by readahead). This pipelines
// I/O with computation instead of serializing load→scan.
template<typename T>
static const T* mmap_col_parallel(const std::string& path, size_t& n_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); std::exit(1); }
    struct stat st; fstat(fd, &st);
    n_rows = st.st_size / sizeof(T);
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); std::exit(1); }
    madvise(p, st.st_size, MADV_WILLNEED);
    madvise(p, st.st_size, MADV_SEQUENTIAL);
    close(fd);
    return reinterpret_cast<const T*>(p);
}

// ─── mmap helper (zone map — tiny file, MAP_POPULATE OK) ────────────────────
template<typename T>
static const T* mmap_col(const std::string& path, size_t& n_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); std::exit(1); }
    struct stat st; fstat(fd, &st);
    n_rows = st.st_size / sizeof(T);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); std::exit(1); }
    close(fd);
    return reinterpret_cast<const T*>(p);
}

// ─── Accumulator ─────────────────────────────────────────────────────────────
// Padded to exactly 64 bytes (one cache line) to prevent false sharing between
// adjacent threads' accumulator slots when stored in a flat aligned array.
struct alignas(64) Accum {
    double sum_qty        = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge     = 0.0;
    double sum_disc       = 0.0;
    int64_t count         = 0;
    uint8_t _pad[16];   // pad to 64 bytes: 5*8 + 8 + 16 = 64
};

static const int32_t SHIPDATE_THRESHOLD = 10471;

void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ── Phase 0: Load columns ─────────────────────────────────────────────
    size_t n_shipdate = 0, n_rf = 0, n_ls = 0, n_qty = 0, n_price = 0, n_disc = 0, n_tax = 0;
    const int32_t* shipdate    = nullptr;
    const uint8_t* returnflag  = nullptr;
    const uint8_t* linestatus  = nullptr;
    const double*  quantity    = nullptr;
    const double*  extprice    = nullptr;
    const double*  discount    = nullptr;
    const double*  tax         = nullptr;

    // Zone map
    uint32_t num_blocks = 0;
    const int32_t* zone_min = nullptr;
    const int32_t* zone_max = nullptr;

    {
        GENDB_PHASE("data_loading");

        // Load zone map first (tiny — synchronous, needed before selective prefetch)
        {
            std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
            int fd = open(zm_path.c_str(), O_RDONLY);
            if (fd >= 0) {
                struct stat st; fstat(fd, &st);
                void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
                close(fd);
                if (p != MAP_FAILED) {
                    const char* ptr = reinterpret_cast<const char*>(p);
                    num_blocks = *reinterpret_cast<const uint32_t*>(ptr);
                    const int32_t* pairs = reinterpret_cast<const int32_t*>(ptr + 4);
                    zone_min = pairs;
                    zone_max = pairs + 1;
                }
            }
        }

        // Parallel column loading: mmap all 7 columns concurrently in separate threads.
        // Each thread issues fadvise(SEQUENTIAL) then MAP_POPULATE, blocking until its
        // column is fully loaded. All 7 columns load in parallel, maximizing disk bandwidth.
        // Wall-clock I/O time = max(individual column load times) instead of their sum.
        std::thread t_sd([&]()   { shipdate   = mmap_col_parallel<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin",     n_shipdate); });
        std::thread t_rf([&]()   { returnflag = mmap_col_parallel<uint8_t>(gendb_dir + "/lineitem/l_returnflag.bin",   n_rf);      });
        std::thread t_ls([&]()   { linestatus = mmap_col_parallel<uint8_t>(gendb_dir + "/lineitem/l_linestatus.bin",   n_ls);      });
        std::thread t_qty([&]()  { quantity   = mmap_col_parallel<double> (gendb_dir + "/lineitem/l_quantity.bin",     n_qty);     });
        std::thread t_ep([&]()   { extprice   = mmap_col_parallel<double> (gendb_dir + "/lineitem/l_extendedprice.bin",n_price);   });
        std::thread t_disc([&]() { discount   = mmap_col_parallel<double> (gendb_dir + "/lineitem/l_discount.bin",     n_disc);    });
        std::thread t_tax([&]()  { tax        = mmap_col_parallel<double> (gendb_dir + "/lineitem/l_tax.bin",          n_tax);     });

        // Block until all 7 columns are fully loaded into page cache
        t_sd.join(); t_rf.join(); t_ls.join(); t_qty.join();
        t_ep.join(); t_disc.join(); t_tax.join();
    }

    const size_t N = n_shipdate;
    const uint32_t BLOCK = 100000;
    if (num_blocks == 0) num_blocks = (uint32_t)((N + BLOCK - 1) / BLOCK);

    // ── Phase 1: Load dictionaries ────────────────────────────────────────
    std::vector<std::string> rf_lookup, ls_lookup;
    {
        GENDB_PHASE("dim_filter");
        auto load_lookup = [&](const std::string& path) {
            std::vector<std::string> v;
            std::ifstream f(path);
            std::string line;
            while (std::getline(f, line)) if (!line.empty()) v.push_back(line);
            return v;
        };
        rf_lookup = load_lookup(gendb_dir + "/lineitem/l_returnflag_lookup.txt");
        ls_lookup = load_lookup(gendb_dir + "/lineitem/l_linestatus_lookup.txt");
    }

    // ── Phase 2: Parallel scan ────────────────────────────────────────────
    // 6 groups: returnflag_code(0-2) * 2 + linestatus_code(0-1)
    static const int NGROUPS = 6;
    const int nthreads = omp_get_max_threads();
    // Flat aligned allocation: tl_accum[tid * NGROUPS + g].
    // Each Accum is 64 bytes (one cache line), so adjacent threads never share
    // a cache line → zero false sharing during parallel accumulation.
    std::vector<Accum> tl_accum((size_t)nthreads * NGROUPS);

    {
        GENDB_PHASE("main_scan");
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            // Pointer to this thread's 6 cache-line-aligned accumulators.
            // No adjacent thread shares a cache line.
            Accum* __restrict__ local = &tl_accum[(size_t)tid * NGROUPS];

            // static schedule: pre-assign blocks evenly, zero dispatch overhead.
            #pragma omp for schedule(static)
            for (uint32_t b = 0; b < num_blocks; b++) {
                // Zone map prune
                if (zone_min != nullptr) {
                    int32_t bmin = zone_min[b * 2];
                    if (bmin > SHIPDATE_THRESHOLD) continue;
                }

                uint32_t row_start = b * BLOCK;
                uint32_t row_end   = std::min((uint64_t)row_start + BLOCK, (uint64_t)N);

                // Check if entire block passes (zone max <= threshold → no per-row filter needed)
                bool all_pass = false;
                if (zone_max != nullptr) {
                    int32_t bmax = zone_max[b * 2];
                    all_pass = (bmax <= SHIPDATE_THRESHOLD);
                }

                // Prefetch distance: 32 rows × 8 bytes = 256 bytes = 4 cache lines ahead.
                // Helps pipeline memory loads for the 4 double columns.
                static constexpr int PF = 32;

                if (all_pass) {
                    for (uint32_t i = row_start; i < row_end; i++) {
                        if (i + PF < row_end) {
                            __builtin_prefetch(quantity + i + PF, 0, 1);
                            __builtin_prefetch(extprice + i + PF, 0, 1);
                            __builtin_prefetch(discount + i + PF, 0, 1);
                            __builtin_prefetch(tax      + i + PF, 0, 1);
                        }
                        int g = returnflag[i] * 2 + linestatus[i];
                        double qty  = quantity[i];
                        double ep   = extprice[i];
                        double disc = discount[i];
                        double tx   = tax[i];
                        double disc_price  = ep * (1.0 - disc);
                        double charge      = disc_price * (1.0 + tx);
                        local[g].sum_qty        += qty;
                        local[g].sum_base_price += ep;
                        local[g].sum_disc_price += disc_price;
                        local[g].sum_charge     += charge;
                        local[g].sum_disc       += disc;
                        local[g].count          += 1;
                    }
                } else {
                    for (uint32_t i = row_start; i < row_end; i++) {
                        if (shipdate[i] > SHIPDATE_THRESHOLD) continue;
                        if (i + PF < row_end) {
                            __builtin_prefetch(quantity + i + PF, 0, 1);
                            __builtin_prefetch(extprice + i + PF, 0, 1);
                            __builtin_prefetch(discount + i + PF, 0, 1);
                            __builtin_prefetch(tax      + i + PF, 0, 1);
                        }
                        int g = returnflag[i] * 2 + linestatus[i];
                        double qty  = quantity[i];
                        double ep   = extprice[i];
                        double disc = discount[i];
                        double tx   = tax[i];
                        double disc_price  = ep * (1.0 - disc);
                        double charge      = disc_price * (1.0 + tx);
                        local[g].sum_qty        += qty;
                        local[g].sum_base_price += ep;
                        local[g].sum_disc_price += disc_price;
                        local[g].sum_charge     += charge;
                        local[g].sum_disc       += disc;
                        local[g].count          += 1;
                    }
                }
            }
        }
    }

    // ── Merge accumulators ────────────────────────────────────────────────
    std::array<Accum, NGROUPS> global{};
    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < nthreads; t++) {
            for (int g = 0; g < NGROUPS; g++) {
                const Accum& src = tl_accum[(size_t)t * NGROUPS + g];
                global[g].sum_qty        += src.sum_qty;
                global[g].sum_base_price += src.sum_base_price;
                global[g].sum_disc_price += src.sum_disc_price;
                global[g].sum_charge     += src.sum_charge;
                global[g].sum_disc       += src.sum_disc;
                global[g].count          += src.count;
            }
        }
    }

    // ── Sort output ───────────────────────────────────────────────────────
    struct ResultRow {
        std::string returnflag;
        std::string linestatus;
        int rf_code;
        int ls_code;
        double sum_qty;
        double sum_base_price;
        double sum_disc_price;
        double sum_charge;
        double avg_qty;
        double avg_price;
        double avg_disc;
        int64_t count_order;
    };

    std::vector<ResultRow> results;
    {
        GENDB_PHASE("sort_topk");
        for (int g = 0; g < NGROUPS; g++) {
            if (global[g].count == 0) continue;
            int rf_code = g / 2;
            int ls_code = g % 2;
            if (rf_code >= (int)rf_lookup.size()) continue;
            if (ls_code >= (int)ls_lookup.size()) continue;
            ResultRow r;
            r.rf_code       = rf_code;
            r.ls_code       = ls_code;
            r.returnflag    = rf_lookup[rf_code];
            r.linestatus    = ls_lookup[ls_code];
            r.sum_qty        = global[g].sum_qty;
            r.sum_base_price = global[g].sum_base_price;
            r.sum_disc_price = global[g].sum_disc_price;
            r.sum_charge     = global[g].sum_charge;
            r.avg_qty        = global[g].sum_qty   / global[g].count;
            r.avg_price      = global[g].sum_base_price / global[g].count;
            r.avg_disc       = global[g].sum_disc  / global[g].count;
            r.count_order    = global[g].count;
            results.push_back(r);
        }
        std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
            return a.linestatus < b.linestatus;
        });
    }

    // ── Output CSV ────────────────────────────────────────────────────────
    // Build output in a memory buffer then write once to avoid slow ofstream
    // flush/close overhead on HDD (one write() syscall vs. many small ones).
    {
        GENDB_PHASE("output");
        std::ostringstream buf;
        buf << std::fixed << std::setprecision(2);
        buf << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";
        for (const auto& r : results) {
            buf << r.returnflag << ","
                << r.linestatus << ","
                << r.sum_qty << ","
                << r.sum_base_price << ","
                << r.sum_disc_price << ","
                << r.sum_charge << ","
                << r.avg_qty << ","
                << r.avg_price << ","
                << r.avg_disc << ","
                << r.count_order << "\n";
        }
        std::string content = buf.str();
        std::string out_path = results_dir + "/Q1.csv";
        int out_fd = open(out_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (out_fd < 0) { std::cerr << "Cannot open output: " << out_path << "\n"; std::exit(1); }
        const char* ptr = content.data();
        size_t rem = content.size();
        while (rem > 0) {
            ssize_t written = write(out_fd, ptr, rem);
            if (written <= 0) break;
            ptr += written; rem -= written;
        }
        close(out_fd);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q1(gendb_dir, results_dir);
    return 0;
}
#endif
