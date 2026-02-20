#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <array>
#include <algorithm>
#include <atomic>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "date_utils.h"
#include <iostream>
#include "timing_utils.h"

static constexpr int32_t SHIP_THRESHOLD = 10471; // epoch days for 1998-09-02
static constexpr int NUM_GROUPS = 6;              // rflag(0-2)*2 + lstatus(0-1), max index=5
static constexpr int NUM_THREADS = 64;
static constexpr size_t MORSEL_SIZE = 100000;

struct AggEntry {
    double sum_qty       = 0.0;
    double sum_price     = 0.0;
    double sum_disc_price= 0.0;
    double sum_charge    = 0.0;
    double sum_disc      = 0.0;
    int64_t count        = 0;
};

// Load dictionary file: format "0=A\n1=N\n2=R\n"
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    FILE* f = fopen(path.c_str(), "r");
    if (!f) return dict;
    char line[256];
    while (fgets(line, sizeof(line), f)) {
        char* eq = strchr(line, '=');
        if (!eq) continue;
        int code = atoi(line);
        std::string val(eq + 1);
        while (!val.empty() && (val.back() == '\n' || val.back() == '\r'))
            val.pop_back();
        if ((int)dict.size() <= code) dict.resize(code + 1);
        dict[code] = val;
    }
    fclose(f);
    return dict;
}

struct ColInfo {
    const void* ptr;
    size_t size;
};

static ColInfo open_col(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); return {nullptr, 0}; }
    struct stat st;
    fstat(fd, &st);
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    // MAP_POPULATE removed: lazy faulting is handled by worker threads during scan.
    // madvise(MADV_SEQUENTIAL) removed: redundant with posix_fadvise above.
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return {ptr, (size_t)st.st_size};
}

void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // -------------------------------------------------------------------------
    // Phase 1: Load zone map, determine scan range
    // Keep zone map alive for block-aware fast path in main_scan
    // -------------------------------------------------------------------------
    struct ZoneBlock {
        int32_t  min_val;
        int32_t  max_val;
        uint32_t row_count;
    };

    const uint8_t* zm_ptr  = nullptr;
    size_t         zm_size = 0;
    const ZoneBlock* blocks = nullptr;
    uint32_t num_blocks  = 0;
    uint32_t cutoff_block = 0;
    size_t   scan_rows    = 0;

    {
        GENDB_PHASE("dim_filter");

        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int fd = open(zm_path.c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st);
        zm_ptr  = reinterpret_cast<const uint8_t*>(
            mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
        zm_size = (size_t)st.st_size;
        close(fd);

        num_blocks = *reinterpret_cast<const uint32_t*>(zm_ptr);
        blocks = reinterpret_cast<const ZoneBlock*>(zm_ptr + 4);

        // Binary search: find first block where min_val > SHIP_THRESHOLD
        cutoff_block = num_blocks;
        uint32_t lo = 0, hi = num_blocks;
        while (lo < hi) {
            uint32_t mid = (lo + hi) / 2;
            if (blocks[mid].min_val > SHIP_THRESHOLD) {
                cutoff_block = mid;
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        // Sum row counts for blocks [0, cutoff_block)
        for (uint32_t b = 0; b < cutoff_block; b++) {
            scan_rows += blocks[b].row_count;
        }
    }

    // -------------------------------------------------------------------------
    // Phase 2: mmap all 7 columns (timed to expose setup overhead)
    // -------------------------------------------------------------------------
    ColInfo ci_shipdate, ci_quantity, ci_extprice, ci_discount, ci_tax, ci_rflag, ci_lstatus;
    const int32_t* col_shipdate;
    const double*  col_quantity;
    const double*  col_extprice;
    const double*  col_discount;
    const double*  col_tax;
    const uint8_t* col_rflag;
    const uint8_t* col_lstatus;
    std::vector<std::string> rflag_dict, lstatus_dict;

    {
        GENDB_PHASE("col_setup");
        ci_shipdate = open_col(gendb_dir + "/lineitem/l_shipdate.bin");
        ci_quantity = open_col(gendb_dir + "/lineitem/l_quantity.bin");
        ci_extprice = open_col(gendb_dir + "/lineitem/l_extendedprice.bin");
        ci_discount = open_col(gendb_dir + "/lineitem/l_discount.bin");
        ci_tax      = open_col(gendb_dir + "/lineitem/l_tax.bin");
        ci_rflag    = open_col(gendb_dir + "/lineitem/l_returnflag.bin");
        ci_lstatus  = open_col(gendb_dir + "/lineitem/l_linestatus.bin");

        col_shipdate = reinterpret_cast<const int32_t*>(ci_shipdate.ptr);
        col_quantity = reinterpret_cast<const double* >(ci_quantity.ptr);
        col_extprice = reinterpret_cast<const double* >(ci_extprice.ptr);
        col_discount = reinterpret_cast<const double* >(ci_discount.ptr);
        col_tax      = reinterpret_cast<const double* >(ci_tax.ptr);
        col_rflag    = reinterpret_cast<const uint8_t*>(ci_rflag.ptr);
        col_lstatus  = reinterpret_cast<const uint8_t*>(ci_lstatus.ptr);

        // Load dictionaries
        rflag_dict  = load_dict(gendb_dir + "/lineitem/l_returnflag_dict.txt");
        lstatus_dict= load_dict(gendb_dir + "/lineitem/l_linestatus_dict.txt");
    }

    // -------------------------------------------------------------------------
    // Phase 3: Parallel block-driven scan with thread-local aggregation
    // Two inner-loop paths: fast (no date check) and guarded (date check).
    // For blocks where max_val <= SHIP_THRESHOLD ALL rows qualify → skip branch.
    // -------------------------------------------------------------------------
    std::vector<std::array<AggEntry, NUM_GROUPS>> thread_agg(NUM_THREADS);
    for (auto& ta : thread_agg)
        for (auto& e : ta) e = AggEntry{};

    std::atomic<uint32_t> next_block{0};

    {
        GENDB_PHASE("main_scan");

        auto worker = [&](int tid) {
            auto& local = thread_agg[tid];

            while (true) {
                uint32_t b = next_block.fetch_add(1, std::memory_order_relaxed);
                if (b >= cutoff_block) break;

                size_t row_start = (size_t)b * MORSEL_SIZE;
                size_t row_end   = row_start + blocks[b].row_count;

                if (blocks[b].max_val <= SHIP_THRESHOLD) {
                    // Fast path: all rows in this block qualify — no date check
                    for (size_t i = row_start; i < row_end; i++) {
                        int g = col_rflag[i] * 2 + col_lstatus[i];
                        double qty        = col_quantity[i];
                        double price      = col_extprice[i];
                        double disc       = col_discount[i];
                        double tx         = col_tax[i];
                        double disc_price = price * (1.0 - disc);
                        local[g].sum_qty        += qty;
                        local[g].sum_price      += price;
                        local[g].sum_disc_price += disc_price;
                        local[g].sum_charge     += disc_price * (1.0 + tx);
                        local[g].sum_disc       += disc;
                        local[g].count++;
                    }
                } else {
                    // Guarded path: partial block — check each row's date
                    for (size_t i = row_start; i < row_end; i++) {
                        if (col_shipdate[i] > SHIP_THRESHOLD) continue;
                        int g = col_rflag[i] * 2 + col_lstatus[i];
                        double qty        = col_quantity[i];
                        double price      = col_extprice[i];
                        double disc       = col_discount[i];
                        double tx         = col_tax[i];
                        double disc_price = price * (1.0 - disc);
                        local[g].sum_qty        += qty;
                        local[g].sum_price      += price;
                        local[g].sum_disc_price += disc_price;
                        local[g].sum_charge     += disc_price * (1.0 + tx);
                        local[g].sum_disc       += disc;
                        local[g].count++;
                    }
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(NUM_THREADS);
        for (int t = 0; t < NUM_THREADS; t++)
            threads.emplace_back(worker, t);
        for (auto& th : threads)
            th.join();
    }

    // Release zone map now that scan is complete
    munmap((void*)zm_ptr, zm_size);

    // -------------------------------------------------------------------------
    // Merge thread-local results → global accumulators
    // -------------------------------------------------------------------------
    AggEntry global[NUM_GROUPS] = {};
    for (int t = 0; t < NUM_THREADS; t++) {
        for (int g = 0; g < NUM_GROUPS; g++) {
            global[g].sum_qty        += thread_agg[t][g].sum_qty;
            global[g].sum_price      += thread_agg[t][g].sum_price;
            global[g].sum_disc_price += thread_agg[t][g].sum_disc_price;
            global[g].sum_charge     += thread_agg[t][g].sum_charge;
            global[g].sum_disc       += thread_agg[t][g].sum_disc;
            global[g].count          += thread_agg[t][g].count;
        }
    }

    // -------------------------------------------------------------------------
    // Phase 4: Output — decode dict codes, sort, write CSV
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        struct Row {
            std::string rflag_str;
            std::string lstatus_str;
            AggEntry    agg;
        };

        std::vector<Row> rows;
        rows.reserve(NUM_GROUPS);
        for (int rf = 0; rf < (int)rflag_dict.size(); rf++) {
            for (int ls = 0; ls < (int)lstatus_dict.size(); ls++) {
                int g = rf * 2 + ls;
                if (global[g].count == 0) continue;
                rows.push_back({rflag_dict[rf], lstatus_dict[ls], global[g]});
            }
        }

        // Sort by l_returnflag ASC, l_linestatus ASC
        std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
            if (a.rflag_str != b.rflag_str) return a.rflag_str < b.rflag_str;
            return a.lstatus_str < b.lstatus_str;
        });

        std::string out_path = results_dir + "/Q1.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }

        fprintf(f, "l_returnflag,l_linestatus,sum_qty,sum_base_price,"
                   "sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (const auto& row : rows) {
            int64_t cnt = row.agg.count;
            fprintf(f,
                "%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%ld\n",
                row.rflag_str.c_str(),
                row.lstatus_str.c_str(),
                row.agg.sum_qty,
                row.agg.sum_price,
                row.agg.sum_disc_price,
                row.agg.sum_charge,
                row.agg.sum_qty        / (double)cnt,
                row.agg.sum_price      / (double)cnt,
                row.agg.sum_disc       / (double)cnt,
                cnt);
        }
        fclose(f);
    }

    // Intentionally skip munmap for the 7 large column files.
    // munmap on ~2.28 GB of worker-touched pages triggers IPI-based TLB shootdown
    // across all 64 CPUs (~558K pages × 64 cores) — empirically 50-70 ms of overhead.
    // Virtual memory is reclaimed by the OS on process exit; safe for benchmark use.
    (void)ci_shipdate; (void)ci_quantity; (void)ci_extprice;
    (void)ci_discount; (void)ci_tax; (void)ci_rflag; (void)ci_lstatus;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q1(gendb_dir, results_dir);
    return 0;
}
#endif
