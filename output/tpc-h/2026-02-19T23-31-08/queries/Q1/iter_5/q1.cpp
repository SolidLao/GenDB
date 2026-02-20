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

// Padded to 64 bytes (one cache line) to eliminate cache-line splits on local[g] access.
// Without padding: local[1] starts at byte 56, spanning two cache lines → 2× cache line
// traffic per update. alignas(64) ensures each entry starts on a cache-line boundary.
struct alignas(64) AggEntry {
    double sum_qty       = 0.0;
    double sum_price     = 0.0;
    double sum_disc_price= 0.0;
    double sum_charge    = 0.0;
    double sum_disc      = 0.0;
    int64_t count        = 0;
    int64_t _pad         = 0;  // pad to 64 bytes (6×8 + 2×8 = 64)
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
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr != MAP_FAILED) {
        // MADV_SEQUENTIAL: tells VM subsystem (not buffered-I/O) to aggressively read-ahead
        // mmap pages. posix_fadvise is NOT redundant here — they target different kernel paths.
        madvise(ptr, st.st_size, MADV_SEQUENTIAL);
        // MADV_HUGEPAGE: request THP promotion. 2.3GB across 7 columns = ~590K 4KB pages.
        // Each thread needs ~63K TLB entries with 4KB pages → TLB thrashing.
        // With 2MB huge pages: ~126 TLB entries per thread → fits in L2 TLB.
        madvise(ptr, st.st_size, MADV_HUGEPAGE);
    }
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
                    // Fast path: all rows qualify — no date check.
                    // Use 4 independent accumulator sets to break loop-carried FP-add
                    // dependency chains. FP add latency = 4-5 cycles; with a single
                    // accumulator, consecutive same-group rows serialize. With 4 sets,
                    // the CPU can pipeline 4 independent chains per field.
                    AggEntry loc0[NUM_GROUPS] = {}, loc1[NUM_GROUPS] = {},
                             loc2[NUM_GROUPS] = {}, loc3[NUM_GROUPS] = {};

                    size_t i = row_start;
                    size_t row_end4 = row_start + ((row_end - row_start) / 4) * 4;
                    for (; i < row_end4; i += 4) {
                        int g0 = col_rflag[i]   * 2 + col_lstatus[i];
                        int g1 = col_rflag[i+1] * 2 + col_lstatus[i+1];
                        int g2 = col_rflag[i+2] * 2 + col_lstatus[i+2];
                        int g3 = col_rflag[i+3] * 2 + col_lstatus[i+3];

                        double qty0  = col_quantity[i],   qty1  = col_quantity[i+1],
                               qty2  = col_quantity[i+2], qty3  = col_quantity[i+3];
                        double pr0   = col_extprice[i],   pr1   = col_extprice[i+1],
                               pr2   = col_extprice[i+2], pr3   = col_extprice[i+3];
                        double di0   = col_discount[i],   di1   = col_discount[i+1],
                               di2   = col_discount[i+2], di3   = col_discount[i+3];
                        double tx0   = col_tax[i],        tx1   = col_tax[i+1],
                               tx2   = col_tax[i+2],      tx3   = col_tax[i+3];

                        double dp0 = pr0 * (1.0 - di0), dp1 = pr1 * (1.0 - di1),
                               dp2 = pr2 * (1.0 - di2), dp3 = pr3 * (1.0 - di3);

                        loc0[g0].sum_qty        += qty0; loc0[g0].sum_price      += pr0;
                        loc0[g0].sum_disc_price += dp0;  loc0[g0].sum_charge     += dp0 * (1.0 + tx0);
                        loc0[g0].sum_disc       += di0;  loc0[g0].count++;

                        loc1[g1].sum_qty        += qty1; loc1[g1].sum_price      += pr1;
                        loc1[g1].sum_disc_price += dp1;  loc1[g1].sum_charge     += dp1 * (1.0 + tx1);
                        loc1[g1].sum_disc       += di1;  loc1[g1].count++;

                        loc2[g2].sum_qty        += qty2; loc2[g2].sum_price      += pr2;
                        loc2[g2].sum_disc_price += dp2;  loc2[g2].sum_charge     += dp2 * (1.0 + tx2);
                        loc2[g2].sum_disc       += di2;  loc2[g2].count++;

                        loc3[g3].sum_qty        += qty3; loc3[g3].sum_price      += pr3;
                        loc3[g3].sum_disc_price += dp3;  loc3[g3].sum_charge     += dp3 * (1.0 + tx3);
                        loc3[g3].sum_disc       += di3;  loc3[g3].count++;
                    }
                    // Scalar tail
                    for (; i < row_end; i++) {
                        int g = col_rflag[i] * 2 + col_lstatus[i];
                        double pr = col_extprice[i], di = col_discount[i];
                        double dp = pr * (1.0 - di);
                        loc0[g].sum_qty        += col_quantity[i];
                        loc0[g].sum_price      += pr;
                        loc0[g].sum_disc_price += dp;
                        loc0[g].sum_charge     += dp * (1.0 + col_tax[i]);
                        loc0[g].sum_disc       += di;
                        loc0[g].count++;
                    }
                    // Reduce 4 sets into thread-local
                    for (int g = 0; g < NUM_GROUPS; g++) {
                        local[g].sum_qty        += loc0[g].sum_qty        + loc1[g].sum_qty
                                                 + loc2[g].sum_qty        + loc3[g].sum_qty;
                        local[g].sum_price      += loc0[g].sum_price      + loc1[g].sum_price
                                                 + loc2[g].sum_price      + loc3[g].sum_price;
                        local[g].sum_disc_price += loc0[g].sum_disc_price + loc1[g].sum_disc_price
                                                 + loc2[g].sum_disc_price + loc3[g].sum_disc_price;
                        local[g].sum_charge     += loc0[g].sum_charge     + loc1[g].sum_charge
                                                 + loc2[g].sum_charge     + loc3[g].sum_charge;
                        local[g].sum_disc       += loc0[g].sum_disc       + loc1[g].sum_disc
                                                 + loc2[g].sum_disc       + loc3[g].sum_disc;
                        local[g].count          += loc0[g].count          + loc1[g].count
                                                 + loc2[g].count          + loc3[g].count;
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
