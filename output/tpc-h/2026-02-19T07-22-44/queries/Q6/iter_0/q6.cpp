#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "date_utils.h"
#include "timing_utils.h"

// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= DATE '1994-01-01'
//   AND l_shipdate < DATE '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24;

static const int32_t SHIPDATE_LO = 8766;  // 1994-01-01
static const int32_t SHIPDATE_HI = 9131;  // 1995-01-01 (exclusive)
static const double  DISCOUNT_LO = 0.05;
static const double  DISCOUNT_HI = 0.07;
static const double  QUANTITY_LT = 24.0;

struct ZoneMapEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t block_size;
};

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // -------------------------------------------------------------------------
    // Phase 1: Load zone map and collect qualifying block indices
    // -------------------------------------------------------------------------
    std::vector<uint64_t> qualifying_row_offsets;  // row offset for each qualifying block
    std::vector<uint32_t> qualifying_block_sizes;

    {
        GENDB_PHASE("dim_filter");

        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int fd = open(zm_path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::fprintf(stderr, "Failed to open zone map: %s\n", zm_path.c_str());
            return;
        }
        struct stat st;
        fstat(fd, &st);
        const uint8_t* zm_data = reinterpret_cast<const uint8_t*>(
            mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
        close(fd);

        uint32_t num_blocks = *reinterpret_cast<const uint32_t*>(zm_data);
        const ZoneMapEntry* entries = reinterpret_cast<const ZoneMapEntry*>(zm_data + 4);

        uint64_t row_offset = 0;
        for (uint32_t b = 0; b < num_blocks; b++) {
            const ZoneMapEntry& e = entries[b];
            // Skip if block is entirely outside [SHIPDATE_LO, SHIPDATE_HI)
            if (e.max_val < SHIPDATE_LO || e.min_val >= SHIPDATE_HI) {
                row_offset += e.block_size;
                continue;
            }
            qualifying_row_offsets.push_back(row_offset);
            qualifying_block_sizes.push_back(e.block_size);
            row_offset += e.block_size;
        }

        munmap((void*)zm_data, st.st_size);
    }

    size_t num_qualifying = qualifying_row_offsets.size();

    // -------------------------------------------------------------------------
    // Phase 2: mmap column files
    // -------------------------------------------------------------------------
    auto mmap_col = [&](const std::string& rel_path, size_t& out_n) -> const void* {
        std::string path = gendb_dir + "/" + rel_path;
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::fprintf(stderr, "Failed to open: %s\n", path.c_str());
            return nullptr;
        }
        struct stat st;
        fstat(fd, &st);
        out_n = st.st_size;
        void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        return p;
    };

    size_t sz_shipdate, sz_discount, sz_quantity, sz_extprice;
    const int32_t* col_shipdate   = reinterpret_cast<const int32_t*>(
        mmap_col("lineitem/l_shipdate.bin",      sz_shipdate));
    const double*  col_discount   = reinterpret_cast<const double*>(
        mmap_col("lineitem/l_discount.bin",      sz_discount));
    const double*  col_quantity   = reinterpret_cast<const double*>(
        mmap_col("lineitem/l_quantity.bin",      sz_quantity));
    const double*  col_extprice   = reinterpret_cast<const double*>(
        mmap_col("lineitem/l_extendedprice.bin", sz_extprice));

    if (!col_shipdate || !col_discount || !col_quantity || !col_extprice) {
        std::fprintf(stderr, "Failed to mmap one or more column files\n");
        return;
    }

    // -------------------------------------------------------------------------
    // Phase 3: Parallel fused scan over qualifying blocks
    // -------------------------------------------------------------------------
    double global_revenue = 0.0;
    {
        GENDB_PHASE("main_scan");

        int num_threads = std::min((int)std::thread::hardware_concurrency(), 32);
        if (num_threads < 1) num_threads = 1;

        std::vector<double> thread_sums(num_threads, 0.0);

        std::atomic<size_t> block_cursor(0);

        auto worker = [&](int tid) {
            double local_sum = 0.0;
            // Kahan compensated summation for precision
            double kahan_c = 0.0;

            while (true) {
                size_t bi = block_cursor.fetch_add(1, std::memory_order_relaxed);
                if (bi >= num_qualifying) break;

                uint64_t row_off = qualifying_row_offsets[bi];
                uint32_t bsz    = qualifying_block_sizes[bi];

                for (uint32_t i = 0; i < bsz; i++) {
                    size_t r = row_off + i;
                    int32_t sd = col_shipdate[r];
                    if (sd < SHIPDATE_LO || sd >= SHIPDATE_HI) continue;

                    double disc = col_discount[r];
                    if (disc < DISCOUNT_LO || disc > DISCOUNT_HI) continue;

                    double qty = col_quantity[r];
                    if (qty >= QUANTITY_LT) continue;

                    // Late materialize extendedprice
                    double ep = col_extprice[r];
                    double val = ep * disc;

                    // Kahan summation
                    double y = val - kahan_c;
                    double t = local_sum + y;
                    kahan_c = (t - local_sum) - y;
                    local_sum = t;
                }
            }

            thread_sums[tid] = local_sum;
        };

        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (int t = 0; t < num_threads; t++) {
            threads.emplace_back(worker, t);
        }
        for (auto& th : threads) th.join();

        for (int t = 0; t < num_threads; t++) {
            global_revenue += thread_sums[t];
        }
    }

    // -------------------------------------------------------------------------
    // Phase 4: Output results
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        // Create results directory if needed
        std::string mkdir_cmd = "mkdir -p " + results_dir;
        (void)system(mkdir_cmd.c_str());

        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "Failed to open output: %s\n", out_path.c_str());
            return;
        }
        std::fprintf(f, "revenue\n");
        std::fprintf(f, "%.2f\n", global_revenue);
        fclose(f);
    }

    // Unmap columns
    munmap((void*)col_shipdate, sz_shipdate);
    munmap((void*)col_discount, sz_discount);
    munmap((void*)col_quantity, sz_quantity);
    munmap((void*)col_extprice, sz_extprice);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
