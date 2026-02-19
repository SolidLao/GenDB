#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>
#include "date_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Kahan compensated summation — prevents floating-point drift in large sums
// ---------------------------------------------------------------------------
struct KahanSum {
    double sum  = 0.0;
    double comp = 0.0;

    inline void add(double v) noexcept {
        double y = v - comp;
        double t = sum + y;
        comp = (t - sum) - y;
        sum  = t;
    }
    inline double value() const noexcept { return sum; }
};

// ---------------------------------------------------------------------------
// Aggregation slot: one per (l_returnflag, l_linestatus) group
// ---------------------------------------------------------------------------
struct AggSlot {
    KahanSum sum_qty;
    KahanSum sum_base_price;
    KahanSum sum_disc_price;
    KahanSum sum_charge;
    KahanSum sum_discount;
    int64_t  count = 0;
};

// ---------------------------------------------------------------------------
// Helper: mmap a binary column file, return typed pointer + row count
// ---------------------------------------------------------------------------
template<typename T>
static const T* mmap_col(const std::string& path, size_t& n_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    n_rows = st.st_size / sizeof(T);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    madvise(p, st.st_size, MADV_SEQUENTIAL);
    return reinterpret_cast<const T*>(p);
}

// ---------------------------------------------------------------------------
// Helper: load dictionary file (one entry per line)
// ---------------------------------------------------------------------------
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    FILE* f = fopen(path.c_str(), "r");
    if (!f) { perror(path.c_str()); exit(1); }
    char buf[256];
    while (fgets(buf, sizeof(buf), f)) {
        size_t len = strlen(buf);
        while (len > 0 && (buf[len-1] == '\n' || buf[len-1] == '\r')) buf[--len] = '\0';
        dict.push_back(std::string(buf));
    }
    fclose(f);
    return dict;
}

// ---------------------------------------------------------------------------
// Zone map block descriptor (matches binary layout in the index file)
// ---------------------------------------------------------------------------
struct BlockMeta {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t block_size;
};

// ---------------------------------------------------------------------------
// Main query function
// ---------------------------------------------------------------------------
void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    static const int32_t SHIPDATE_BOUND = 10471;  // 1998-09-02 in epoch days
    const int NUM_THREADS = 32;

    // -----------------------------------------------------------------------
    // Phase 1: Load zone map
    // -----------------------------------------------------------------------
    const BlockMeta* blocks = nullptr;
    uint32_t num_blocks = 0;
    {
        GENDB_PHASE("dim_filter");
        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int zm_fd = open(zm_path.c_str(), O_RDONLY);
        if (zm_fd < 0) { perror(zm_path.c_str()); exit(1); }
        struct stat zm_st;
        fstat(zm_fd, &zm_st);
        auto* zm_data = reinterpret_cast<const char*>(
            mmap(nullptr, zm_st.st_size, PROT_READ, MAP_PRIVATE, zm_fd, 0));
        close(zm_fd);
        if (zm_data == MAP_FAILED) { perror("mmap zone map"); exit(1); }

        num_blocks = *reinterpret_cast<const uint32_t*>(zm_data);
        blocks     = reinterpret_cast<const BlockMeta*>(zm_data + 4);
    }

    // Precompute cumulative row offsets for each block
    std::vector<uint64_t> block_row_offset(num_blocks + 1, 0);
    for (uint32_t b = 0; b < num_blocks; b++) {
        block_row_offset[b + 1] = block_row_offset[b] + blocks[b].block_size;
    }

    // Build list of active block indices (skip blocks where min > bound)
    std::vector<uint32_t> active_blocks;
    active_blocks.reserve(num_blocks);
    for (uint32_t b = 0; b < num_blocks; b++) {
        if (blocks[b].min_val <= SHIPDATE_BOUND) {
            active_blocks.push_back(b);
        }
    }
    uint32_t n_active = static_cast<uint32_t>(active_blocks.size());

    // -----------------------------------------------------------------------
    // Phase 2: Memory-map the 7 required columns
    // -----------------------------------------------------------------------
    const int32_t* col_shipdate;
    const int8_t*  col_returnflag;
    const int8_t*  col_linestatus;
    const double*  col_quantity;
    const double*  col_extendedprice;
    const double*  col_discount;
    const double*  col_tax;

    {
        GENDB_PHASE("build_joins");
        size_t n;
        col_shipdate      = mmap_col<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin",      n);
        col_returnflag    = mmap_col<int8_t> (gendb_dir + "/lineitem/l_returnflag.bin",    n);
        col_linestatus    = mmap_col<int8_t> (gendb_dir + "/lineitem/l_linestatus.bin",    n);
        col_quantity      = mmap_col<double> (gendb_dir + "/lineitem/l_quantity.bin",      n);
        col_extendedprice = mmap_col<double> (gendb_dir + "/lineitem/l_extendedprice.bin", n);
        col_discount      = mmap_col<double> (gendb_dir + "/lineitem/l_discount.bin",      n);
        col_tax           = mmap_col<double> (gendb_dir + "/lineitem/l_tax.bin",           n);
    }

    // Load dictionaries (needed for output decoding)
    std::vector<std::string> rf_dict = load_dict(gendb_dir + "/lineitem/l_returnflag_dict.txt");
    std::vector<std::string> ls_dict = load_dict(gendb_dir + "/lineitem/l_linestatus_dict.txt");

    // -----------------------------------------------------------------------
    // Phase 3: Parallel morsel-driven scan + fused aggregate
    // Direct 2D array [rf_code 0-2][ls_code 0-1] — no hashing, no branching
    // Each thread maintains its own slots to avoid contention
    // -----------------------------------------------------------------------
    constexpr int SLOTS = 6;  // 3 rf_codes × 2 ls_codes
    std::vector<AggSlot> local_agg(NUM_THREADS * SLOTS);

    {
        GENDB_PHASE("main_scan");

        auto worker = [&](int tid) {
            AggSlot* my_agg = &local_agg[tid * SLOTS];

            uint32_t blk_start = static_cast<uint32_t>( tid      * (uint64_t)n_active / NUM_THREADS);
            uint32_t blk_end   = static_cast<uint32_t>((tid + 1) * (uint64_t)n_active / NUM_THREADS);

            for (uint32_t bi = blk_start; bi < blk_end; bi++) {
                uint32_t b         = active_blocks[bi];
                uint64_t row_start = block_row_offset[b];
                uint32_t bsize     = blocks[b].block_size;

                // Fast path: entire block within bound → skip per-row date check
                if (blocks[b].max_val <= SHIPDATE_BOUND) {
                    for (uint32_t i = 0; i < bsize; i++) {
                        uint64_t r    = row_start + i;
                        int rf        = col_returnflag[r];
                        int ls        = col_linestatus[r];
                        double qty    = col_quantity[r];
                        double price  = col_extendedprice[r];
                        double disc   = col_discount[r];
                        double tax    = col_tax[r];

                        AggSlot& slot = my_agg[rf * 2 + ls];
                        slot.sum_qty.add(qty);
                        slot.sum_base_price.add(price);
                        double disc_price = price * (1.0 - disc);
                        slot.sum_disc_price.add(disc_price);
                        slot.sum_charge.add(disc_price * (1.0 + tax));
                        slot.sum_discount.add(disc);
                        slot.count++;
                    }
                } else {
                    // Boundary block: per-row date check required
                    for (uint32_t i = 0; i < bsize; i++) {
                        uint64_t r = row_start + i;
                        if (col_shipdate[r] > SHIPDATE_BOUND) continue;

                        int rf        = col_returnflag[r];
                        int ls        = col_linestatus[r];
                        double qty    = col_quantity[r];
                        double price  = col_extendedprice[r];
                        double disc   = col_discount[r];
                        double tax    = col_tax[r];

                        AggSlot& slot = my_agg[rf * 2 + ls];
                        slot.sum_qty.add(qty);
                        slot.sum_base_price.add(price);
                        double disc_price = price * (1.0 - disc);
                        slot.sum_disc_price.add(disc_price);
                        slot.sum_charge.add(disc_price * (1.0 + tax));
                        slot.sum_discount.add(disc);
                        slot.count++;
                    }
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(NUM_THREADS);
        for (int t = 0; t < NUM_THREADS; t++) {
            threads.emplace_back(worker, t);
        }
        for (auto& t : threads) t.join();
    }

    // -----------------------------------------------------------------------
    // Merge thread-local aggregates — trivial: 6 slots × 5 accumulators = 30 adds
    // -----------------------------------------------------------------------
    AggSlot final_agg[3][2] = {};
    for (int t = 0; t < NUM_THREADS; t++) {
        for (int rf = 0; rf < 3; rf++) {
            for (int ls = 0; ls < 2; ls++) {
                const AggSlot& src = local_agg[t * SLOTS + rf * 2 + ls];
                AggSlot& dst = final_agg[rf][ls];
                dst.sum_qty.add(src.sum_qty.value());
                dst.sum_base_price.add(src.sum_base_price.value());
                dst.sum_disc_price.add(src.sum_disc_price.value());
                dst.sum_charge.add(src.sum_charge.value());
                dst.sum_discount.add(src.sum_discount.value());
                dst.count += src.count;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 4: Sort result rows and write CSV
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        struct ResultRow {
            std::string returnflag;
            std::string linestatus;
            double sum_qty;
            double sum_base_price;
            double sum_disc_price;
            double sum_charge;
            double avg_qty;
            double avg_price;
            double avg_disc;
            int64_t count;
        };

        std::vector<ResultRow> rows;
        rows.reserve(6);
        for (int rf = 0; rf < static_cast<int>(rf_dict.size()) && rf < 3; rf++) {
            for (int ls = 0; ls < static_cast<int>(ls_dict.size()) && ls < 2; ls++) {
                const AggSlot& s = final_agg[rf][ls];
                if (s.count == 0) continue;
                ResultRow row;
                row.returnflag     = rf_dict[rf];
                row.linestatus     = ls_dict[ls];
                row.sum_qty        = s.sum_qty.value();
                row.sum_base_price = s.sum_base_price.value();
                row.sum_disc_price = s.sum_disc_price.value();
                row.sum_charge     = s.sum_charge.value();
                row.avg_qty        = s.sum_qty.value()        / static_cast<double>(s.count);
                row.avg_price      = s.sum_base_price.value() / static_cast<double>(s.count);
                row.avg_disc       = s.sum_discount.value()   / static_cast<double>(s.count);
                row.count          = s.count;
                rows.push_back(row);
            }
        }

        // Sort by (l_returnflag, l_linestatus) alphabetically
        std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
            return a.linestatus < b.linestatus;
        });

        // Write CSV
        std::string out_path = results_dir + "/Q1.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); exit(1); }

        fprintf(f, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,"
                   "avg_qty,avg_price,avg_disc,count_order\n");
        for (const auto& row : rows) {
            fprintf(f, "%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%ld\n",
                    row.returnflag.c_str(),
                    row.linestatus.c_str(),
                    row.sum_qty,
                    row.sum_base_price,
                    row.sum_disc_price,
                    row.sum_charge,
                    row.avg_qty,
                    row.avg_price,
                    row.avg_disc,
                    (long)row.count);
        }
        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q1(gendb_dir, results_dir);
    return 0;
}
#endif
