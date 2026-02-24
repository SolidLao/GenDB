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
struct Accum {
    double sum_qty        = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge     = 0.0;
    double sum_disc       = 0.0;
    int64_t count         = 0;
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
    // Thread-local accumulators: [thread][group]
    std::vector<std::array<Accum, NGROUPS>> tl_accum(nthreads);

    {
        GENDB_PHASE("main_scan");

        // Chunk size: 2048 rows.
        //   group_buf = 2048 bytes → fits in L1 (32KB).
        //   4 double columns × 2048 × 8B = 64KB → fits in L2 (256KB/core).
        // Two-pass strategy per chunk:
        //   Pass 1 (cold→cache): read shipdate+returnflag+linestatus → group_buf[]
        //   Pass 2 × 6 groups (L2-hot): branchless masked FMA → compiler auto-vectorizes
        //   Eliminates scatter writes; enables AVX2 vectorization.
        static const int CHUNK = 2048;

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& local = tl_accum[tid];

            // Stack-allocated chunk buffer (2KB, stays in L1)
            uint8_t group_buf[CHUNK];

            #pragma omp for schedule(static)
            for (uint32_t b = 0; b < num_blocks; b++) {
                // Zone map prune
                if (zone_min != nullptr && zone_min[b * 2] > SHIPDATE_THRESHOLD) continue;

                uint32_t row_start = b * BLOCK;
                uint32_t row_end   = std::min((uint64_t)row_start + BLOCK, (uint64_t)N);

                // all_pass: zone_max <= threshold → no per-row shipdate check needed
                bool all_pass = (zone_max != nullptr && zone_max[b * 2] <= SHIPDATE_THRESHOLD);

                // Process block in 2048-row chunks
                for (uint32_t cs = row_start; cs < row_end; cs += CHUNK) {
                    int csz = (int)std::min((uint32_t)CHUNK, row_end - cs);

                    // ── Pass 1: compute group index for each row in chunk ──────────
                    // Result in group_buf[j]: 0-5 = valid group, 0xFF = filtered out
                    if (all_pass) {
                        for (int j = 0; j < csz; j++) {
                            group_buf[j] = uint8_t(returnflag[cs + j] * 2 + linestatus[cs + j]);
                        }
                    } else {
                        for (int j = 0; j < csz; j++) {
                            uint32_t i = cs + j;
                            group_buf[j] = (shipdate[i] <= SHIPDATE_THRESHOLD)
                                           ? uint8_t(returnflag[i] * 2 + linestatus[i])
                                           : uint8_t(0xFF);
                        }
                    }

                    // ── Pass 2: accumulate per group (branchless, vectorizable) ────
                    // For each group g: scan group_buf with a 0.0/1.0 mask,
                    // multiply all aggregates by mask, and add. No scatter writes.
                    // Compiler vectorizes with AVX2 FMA (4 doubles/cycle).
                    // Data columns are L2-hot from Pass 1 → minimal DRAM traffic.
                    for (int g = 0; g < NGROUPS; g++) {
                        double sq = 0.0, sbp = 0.0, sdp = 0.0, sc = 0.0, sd = 0.0;
                        int64_t cnt = 0;
                        const uint8_t ug = (uint8_t)g;
                        #pragma GCC ivdep
                        for (int j = 0; j < csz; j++) {
                            double m  = (group_buf[j] == ug) ? 1.0 : 0.0;
                            uint32_t i = cs + (uint32_t)j;
                            double qty  = quantity[i];
                            double ep   = extprice[i];
                            double disc = discount[i];
                            double tx   = tax[i];
                            double dp   = ep * (1.0 - disc);
                            sq  += qty * m;
                            sbp += ep  * m;
                            sdp += dp  * m;
                            sc  += dp * (1.0 + tx) * m;
                            sd  += disc * m;
                            cnt += (int64_t)(group_buf[j] == ug);
                        }
                        local[g].sum_qty        += sq;
                        local[g].sum_base_price += sbp;
                        local[g].sum_disc_price += sdp;
                        local[g].sum_charge     += sc;
                        local[g].sum_disc       += sd;
                        local[g].count          += cnt;
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
                global[g].sum_qty        += tl_accum[t][g].sum_qty;
                global[g].sum_base_price += tl_accum[t][g].sum_base_price;
                global[g].sum_disc_price += tl_accum[t][g].sum_disc_price;
                global[g].sum_charge     += tl_accum[t][g].sum_charge;
                global[g].sum_disc       += tl_accum[t][g].sum_disc;
                global[g].count          += tl_accum[t][g].count;
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
