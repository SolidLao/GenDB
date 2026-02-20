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

// Kahan compensated accumulator for precision over large row counts
struct KahanSum {
    double sum  = 0.0;
    double comp = 0.0;  // running compensation
    inline void add(double v) {
        double y = v - comp;
        double t = sum + y;
        comp = (t - sum) - y;
        sum  = t;
    }
};

struct AggEntry {
    KahanSum sum_qty;
    KahanSum sum_price;
    KahanSum sum_disc_price;
    KahanSum sum_charge;
    KahanSum sum_disc;
    int64_t  count = 0;
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
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    close(fd);
    return {ptr, (size_t)st.st_size};
}

void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // -------------------------------------------------------------------------
    // Phase 1: Load zone map, determine scan range
    // -------------------------------------------------------------------------
    size_t scan_rows = 0;
    {
        GENDB_PHASE("dim_filter");

        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int fd = open(zm_path.c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st);
        const uint8_t* zm = reinterpret_cast<const uint8_t*>(
            mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
        close(fd);

        uint32_t num_blocks = *reinterpret_cast<const uint32_t*>(zm);

        struct ZoneBlock {
            int32_t  min_val;
            int32_t  max_val;
            uint32_t row_count;
        };
        const ZoneBlock* blocks = reinterpret_cast<const ZoneBlock*>(zm + 4);

        // Binary search: find first block where min_val > SHIP_THRESHOLD
        // (data sorted ascending → all subsequent blocks also skip)
        uint32_t cutoff = num_blocks;
        uint32_t lo = 0, hi = num_blocks;
        while (lo < hi) {
            uint32_t mid = (lo + hi) / 2;
            if (blocks[mid].min_val > SHIP_THRESHOLD) {
                cutoff = mid;
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        // Sum row counts for blocks [0, cutoff)
        scan_rows = 0;
        for (uint32_t b = 0; b < cutoff; b++) {
            scan_rows += blocks[b].row_count;
        }

        munmap((void*)zm, st.st_size);
    }

    // -------------------------------------------------------------------------
    // Phase 2: mmap all 7 columns
    // -------------------------------------------------------------------------
    ColInfo ci_shipdate = open_col(gendb_dir + "/lineitem/l_shipdate.bin");
    ColInfo ci_quantity = open_col(gendb_dir + "/lineitem/l_quantity.bin");
    ColInfo ci_extprice = open_col(gendb_dir + "/lineitem/l_extendedprice.bin");
    ColInfo ci_discount = open_col(gendb_dir + "/lineitem/l_discount.bin");
    ColInfo ci_tax      = open_col(gendb_dir + "/lineitem/l_tax.bin");
    ColInfo ci_rflag    = open_col(gendb_dir + "/lineitem/l_returnflag.bin");
    ColInfo ci_lstatus  = open_col(gendb_dir + "/lineitem/l_linestatus.bin");

    const int32_t* col_shipdate = reinterpret_cast<const int32_t*>(ci_shipdate.ptr);
    const double*  col_quantity = reinterpret_cast<const double* >(ci_quantity.ptr);
    const double*  col_extprice = reinterpret_cast<const double* >(ci_extprice.ptr);
    const double*  col_discount = reinterpret_cast<const double* >(ci_discount.ptr);
    const double*  col_tax      = reinterpret_cast<const double* >(ci_tax.ptr);
    const uint8_t* col_rflag    = reinterpret_cast<const uint8_t*>(ci_rflag.ptr);
    const uint8_t* col_lstatus  = reinterpret_cast<const uint8_t*>(ci_lstatus.ptr);

    // Load dictionaries
    std::vector<std::string> rflag_dict  = load_dict(gendb_dir + "/lineitem/l_returnflag_dict.txt");
    std::vector<std::string> lstatus_dict= load_dict(gendb_dir + "/lineitem/l_linestatus_dict.txt");

    // -------------------------------------------------------------------------
    // Phase 3: Parallel morsel-driven scan with thread-local aggregation
    // -------------------------------------------------------------------------
    std::vector<std::array<AggEntry, NUM_GROUPS>> thread_agg(NUM_THREADS);
    for (auto& ta : thread_agg)
        for (auto& e : ta) e = AggEntry{};

    std::atomic<size_t> next_morsel{0};

    {
        GENDB_PHASE("main_scan");

        auto worker = [&](int tid) {
            auto& local = thread_agg[tid];

            while (true) {
                size_t start = next_morsel.fetch_add(MORSEL_SIZE, std::memory_order_relaxed);
                if (start >= scan_rows) break;
                size_t end = std::min(start + MORSEL_SIZE, scan_rows);

                // Fused single-pass: filter + aggregate
                for (size_t i = start; i < end; i++) {
                    if (col_shipdate[i] > SHIP_THRESHOLD) continue;
                    int g = col_rflag[i] * 2 + col_lstatus[i];
                    double qty   = col_quantity[i];
                    double price = col_extprice[i];
                    double disc  = col_discount[i];
                    double tx    = col_tax[i];
                    double disc_price = price * (1.0 - disc);
                    local[g].sum_qty.add(qty);
                    local[g].sum_price.add(price);
                    local[g].sum_disc_price.add(disc_price);
                    local[g].sum_charge.add(disc_price * (1.0 + tx));
                    local[g].sum_disc.add(disc);
                    local[g].count++;
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

    // -------------------------------------------------------------------------
    // Merge thread-local results → global accumulators
    // -------------------------------------------------------------------------
    AggEntry global[NUM_GROUPS] = {};
    for (int t = 0; t < NUM_THREADS; t++) {
        for (int g = 0; g < NUM_GROUPS; g++) {
            global[g].sum_qty.add(thread_agg[t][g].sum_qty.sum);
            global[g].sum_price.add(thread_agg[t][g].sum_price.sum);
            global[g].sum_disc_price.add(thread_agg[t][g].sum_disc_price.sum);
            global[g].sum_charge.add(thread_agg[t][g].sum_charge.sum);
            global[g].sum_disc.add(thread_agg[t][g].sum_disc.sum);
            global[g].count += thread_agg[t][g].count;
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
                row.agg.sum_qty.sum,
                row.agg.sum_price.sum,
                row.agg.sum_disc_price.sum,
                row.agg.sum_charge.sum,
                row.agg.sum_qty.sum   / (double)cnt,
                row.agg.sum_price.sum / (double)cnt,
                row.agg.sum_disc.sum  / (double)cnt,
                cnt);
        }
        fclose(f);
    }

    // Cleanup
    munmap((void*)ci_shipdate.ptr, ci_shipdate.size);
    munmap((void*)ci_quantity.ptr, ci_quantity.size);
    munmap((void*)ci_extprice.ptr, ci_extprice.size);
    munmap((void*)ci_discount.ptr, ci_discount.size);
    munmap((void*)ci_tax.ptr,      ci_tax.size);
    munmap((void*)ci_rflag.ptr,    ci_rflag.size);
    munmap((void*)ci_lstatus.ptr,  ci_lstatus.size);
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
