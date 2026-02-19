// Q1: Pricing Summary Report — GenDB iter 0
// Strategy: morsel-driven parallel scan over l_shipdate-sorted lineitem with
//           zone-map pruning; flat 6-slot aggregation array per thread;
//           pure integer arithmetic to avoid FP in hot loop.

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <array>
#include <thread>
#include <atomic>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include "date_utils.h"
#include "timing_utils.h"

// DATE '1998-12-01' - INTERVAL '90' DAY = 1998-09-02 = epoch day 10471
static constexpr int32_t SHIPDATE_THRESHOLD = 10471;
static constexpr int      NUM_GROUPS        = 6;  // 3 returnflag * 2 linestatus

// Per-group accumulators (all integer — scale noted per field)
struct AggSlot {
    int64_t sum_qty        = 0;  // scale 1
    int64_t sum_base_price = 0;  // scale 100
    int64_t sum_disc_price = 0;  // scale 10000  = ep*(100-disc)
    int64_t sum_charge     = 0;  // scale 1000000 = ep*(100-disc)*(100+tax)
    int64_t sum_disc       = 0;  // scale 100    (for avg_disc)
    int64_t count          = 0;
};

// Zone map entry (matches binary layout: int32 min, int32 max, uint32 block_size)
struct ZoneMapEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t block_size;
};

// mmap a file, returning pointer and file size
static const void* mmap_file(const std::string& path, size_t& file_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); file_size = 0; return nullptr; }
    struct stat st;
    fstat(fd, &st);
    file_size = (size_t)st.st_size;
    if (file_size == 0) { close(fd); return nullptr; }
    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) { perror("mmap"); file_size = 0; return nullptr; }
    madvise(ptr, file_size, MADV_SEQUENTIAL);
    return ptr;
}

void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // -------------------------------------------------------------------------
    // Phase 1: mmap all lineitem columns we need
    // -------------------------------------------------------------------------
    const int32_t* l_shipdate      = nullptr;
    const int32_t* l_returnflag    = nullptr;
    const int32_t* l_linestatus    = nullptr;
    const int64_t* l_quantity      = nullptr;
    const int64_t* l_extendedprice = nullptr;
    const int64_t* l_discount      = nullptr;
    const int64_t* l_tax           = nullptr;
    size_t n_rows = 0;

    {
        GENDB_PHASE("load_columns");
        size_t fs = 0;
        l_shipdate      = (const int32_t*)mmap_file(gendb_dir + "/lineitem/l_shipdate.bin",      fs);
        n_rows = fs / sizeof(int32_t);
        l_returnflag    = (const int32_t*)mmap_file(gendb_dir + "/lineitem/l_returnflag.bin",    fs);
        l_linestatus    = (const int32_t*)mmap_file(gendb_dir + "/lineitem/l_linestatus.bin",    fs);
        l_quantity      = (const int64_t*)mmap_file(gendb_dir + "/lineitem/l_quantity.bin",      fs);
        l_extendedprice = (const int64_t*)mmap_file(gendb_dir + "/lineitem/l_extendedprice.bin", fs);
        l_discount      = (const int64_t*)mmap_file(gendb_dir + "/lineitem/l_discount.bin",      fs);
        l_tax           = (const int64_t*)mmap_file(gendb_dir + "/lineitem/l_tax.bin",           fs);
    }

    // -------------------------------------------------------------------------
    // Phase 2: load zone map; precompute per-block row offsets
    // -------------------------------------------------------------------------
    std::vector<ZoneMapEntry> zonemap;
    std::vector<size_t>       block_offsets;

    {
        GENDB_PHASE("load_zonemap");
        size_t fs = 0;
        const uint8_t* data = (const uint8_t*)mmap_file(
            gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin", fs);

        if (data && fs >= sizeof(uint32_t)) {
            uint32_t num_blocks = *reinterpret_cast<const uint32_t*>(data);
            const ZoneMapEntry* entries =
                reinterpret_cast<const ZoneMapEntry*>(data + sizeof(uint32_t));

            zonemap.resize(num_blocks);
            block_offsets.resize(num_blocks);
            size_t row_offset = 0;
            for (uint32_t i = 0; i < num_blocks; i++) {
                zonemap[i]       = entries[i];
                block_offsets[i] = row_offset;
                row_offset      += entries[i].block_size;
            }
            // Don't munmap — data is small and we've already copied what we need
        } else {
            // Fallback: single block covering all rows (no pruning)
            ZoneMapEntry fallback;
            fallback.min_val   = 0;
            fallback.max_val   = SHIPDATE_THRESHOLD;  // force full-block fast path
            fallback.block_size = (uint32_t)n_rows;
            zonemap.push_back(fallback);
            block_offsets.push_back(0);
        }
    }

    // Load dictionaries (code → string, one entry per line, code = line index)
    std::vector<std::string> returnflag_dict, linestatus_dict;
    {
        auto load_dict = [](const std::string& path) {
            std::vector<std::string> dict;
            std::ifstream f(path);
            std::string line;
            while (std::getline(f, line)) {
                if (!line.empty()) dict.push_back(line);
            }
            return dict;
        };
        returnflag_dict = load_dict(gendb_dir + "/lineitem/l_returnflag_dict.txt");
        linestatus_dict = load_dict(gendb_dir + "/lineitem/l_linestatus_dict.txt");
    }

    // -------------------------------------------------------------------------
    // Phase 3: parallel morsel-driven scan + fused aggregation
    // Each thread pulls blocks atomically; accumulates into thread-local 6-slot array.
    // -------------------------------------------------------------------------
    int num_threads = (int)std::thread::hardware_concurrency();
    if (num_threads <= 0 || num_threads > 128) num_threads = 64;

    std::vector<std::array<AggSlot, NUM_GROUPS>> thread_agg(num_threads);

    std::atomic<uint32_t> block_counter(0);
    const uint32_t num_blocks = (uint32_t)zonemap.size();

    {
        GENDB_PHASE("main_scan");

        auto worker = [&](int tid) {
            auto& agg = thread_agg[tid];
            for (auto& s : agg) s = AggSlot{};   // zero-init

            while (true) {
                uint32_t blk_idx = block_counter.fetch_add(1, std::memory_order_relaxed);
                if (blk_idx >= num_blocks) break;

                const ZoneMapEntry& zm = zonemap[blk_idx];

                // Skip block if entirely beyond threshold
                if (zm.min_val > SHIPDATE_THRESHOLD) continue;

                const size_t row_start = block_offsets[blk_idx];
                const size_t row_end   = row_start + zm.block_size;
                const bool   full_blk  = (zm.max_val <= SHIPDATE_THRESHOLD);

                if (full_blk) {
                    // Fast path: all rows pass the shipdate filter — no branch per row
                    for (size_t r = row_start; r < row_end; r++) {
                        const int32_t rf   = l_returnflag[r];
                        const int32_t ls   = l_linestatus[r];
                        const int     slot = rf * 2 + ls;

                        const int64_t ep   = l_extendedprice[r];
                        const int64_t disc = l_discount[r];
                        const int64_t tax  = l_tax[r];

                        const int64_t one_minus_disc = 100LL - disc;
                        const int64_t disc_price     = ep * one_minus_disc;
                        const int64_t charge         = disc_price * (100LL + tax);

                        AggSlot& s = agg[slot];
                        s.sum_qty        += l_quantity[r];
                        s.sum_base_price += ep;
                        s.sum_disc_price += disc_price;
                        s.sum_charge     += charge;
                        s.sum_disc       += disc;
                        s.count          += 1;
                    }
                } else {
                    // Transition block: per-row shipdate check required
                    for (size_t r = row_start; r < row_end; r++) {
                        if (l_shipdate[r] > SHIPDATE_THRESHOLD) continue;

                        const int32_t rf   = l_returnflag[r];
                        const int32_t ls   = l_linestatus[r];
                        const int     slot = rf * 2 + ls;

                        const int64_t ep   = l_extendedprice[r];
                        const int64_t disc = l_discount[r];
                        const int64_t tax  = l_tax[r];

                        const int64_t one_minus_disc = 100LL - disc;
                        const int64_t disc_price     = ep * one_minus_disc;
                        const int64_t charge         = disc_price * (100LL + tax);

                        AggSlot& s = agg[slot];
                        s.sum_qty        += l_quantity[r];
                        s.sum_base_price += ep;
                        s.sum_disc_price += disc_price;
                        s.sum_charge     += charge;
                        s.sum_disc       += disc;
                        s.count          += 1;
                    }
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (int t = 0; t < num_threads; t++) {
            threads.emplace_back(worker, t);
        }
        for (auto& t : threads) t.join();
    }

    // -------------------------------------------------------------------------
    // Merge thread-local aggregation arrays (O(64*6) = trivial)
    // -------------------------------------------------------------------------
    std::array<AggSlot, NUM_GROUPS> global_agg{};
    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < num_threads; t++) {
            for (int g = 0; g < NUM_GROUPS; g++) {
                global_agg[g].sum_qty        += thread_agg[t][g].sum_qty;
                global_agg[g].sum_base_price += thread_agg[t][g].sum_base_price;
                global_agg[g].sum_disc_price += thread_agg[t][g].sum_disc_price;
                global_agg[g].sum_charge     += thread_agg[t][g].sum_charge;
                global_agg[g].sum_disc       += thread_agg[t][g].sum_disc;
                global_agg[g].count          += thread_agg[t][g].count;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 4: Write CSV output sorted by l_returnflag, l_linestatus
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        mkdir(results_dir.c_str(), 0755);
        const std::string out_path = results_dir + "/Q1.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }

        // Header
        fprintf(f, "l_returnflag,l_linestatus,sum_qty,sum_base_price,"
                   "sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        // Enumerate non-empty (returnflag, linestatus) combos and sort lexicographically
        struct OutputRow {
            std::string rf_str;
            std::string ls_str;
            int         slot;
        };
        std::vector<OutputRow> output_rows;
        output_rows.reserve(NUM_GROUPS);

        // Dict file stores values in REVERSE code order:
        // dict[0] = value for the highest code, dict[N-1] = value for code 0.
        // So: string for code C = dict[dict.size()-1-C]
        const int rf_n = (int)returnflag_dict.size();
        const int ls_n = (int)linestatus_dict.size();
        for (int rf = 0; rf < rf_n; rf++) {
            const std::string& rf_str = returnflag_dict[rf_n - 1 - rf];
            for (int ls = 0; ls < ls_n; ls++) {
                const std::string& ls_str = linestatus_dict[ls_n - 1 - ls];
                int slot = rf * 2 + ls;
                if (slot >= NUM_GROUPS) continue;
                if (global_agg[slot].count == 0) continue;
                output_rows.push_back({rf_str, ls_str, slot});
            }
        }

        std::sort(output_rows.begin(), output_rows.end(),
            [](const OutputRow& a, const OutputRow& b) {
                if (a.rf_str != b.rf_str) return a.rf_str < b.rf_str;
                return a.ls_str < b.ls_str;
            });

        for (const auto& row : output_rows) {
            const AggSlot& s   = global_agg[row.slot];
            const int64_t  cnt = s.count;

            // Scale down once here for output — no FP in the hot loop
            const double sum_qty        = (double)s.sum_qty;
            const double sum_base_price = (double)s.sum_base_price / 100.0;
            const double sum_disc_price = (double)s.sum_disc_price / 10000.0;
            const double sum_charge     = (double)s.sum_charge     / 1000000.0;
            const double avg_qty        = (double)s.sum_qty        / (double)cnt;
            const double avg_price      = (double)s.sum_base_price / (100.0 * (double)cnt);
            const double avg_disc       = (double)s.sum_disc       / (100.0 * (double)cnt);

            fprintf(f, "%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%ld\n",
                    row.rf_str.c_str(),
                    row.ls_str.c_str(),
                    sum_qty,
                    sum_base_price,
                    sum_disc_price,
                    sum_charge,
                    avg_qty,
                    avg_price,
                    avg_disc,
                    (long)cnt);
        }

        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q1(gendb_dir, results_dir);
    return 0;
}
#endif
