#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "date_utils.h"
#include "timing_utils.h"

// Zone map entry layout for l_shipdate_zone.idx
struct ZoneEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t block_size;
};

// A qualifying row range from zone map pruning
struct BlockRange {
    uint64_t row_start;
    uint64_t row_end;
};

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // -----------------------------------------------------------------------
    // Filter constants (scaled integers)
    // l_shipdate: int32_t epoch days;  1994-01-01 = 8401,  1995-01-01 = 8766
    // l_discount: int64_t, scale=100;  BETWEEN 0.05 AND 0.07  → stored 5..7
    // l_quantity: int64_t, scale=100;  < 24                   → stored < 2400
    // Revenue = SUM(l_extendedprice * l_discount) / 10000  (both scale=100)
    // -----------------------------------------------------------------------
    static constexpr int32_t SD_MIN = 8401;
    static constexpr int32_t SD_MAX = 8766;   // exclusive
    static constexpr int64_t DISC_MIN = 5;
    static constexpr int64_t DISC_MAX = 7;
    static constexpr int64_t QTY_MAX  = 2400; // exclusive

    // -----------------------------------------------------------------------
    // Phase 1: Zone-map pruning on l_shipdate
    // Layout: [uint32_t num_blocks] [ZoneEntry x num_blocks]
    // -----------------------------------------------------------------------
    std::vector<BlockRange> qualifying_blocks;
    qualifying_blocks.reserve(300);

    {
        GENDB_PHASE("zone_map_prune");

        std::string zmap_path = gendb_dir + "/lineitem/l_shipdate_zone.idx";
        int fd = open(zmap_path.c_str(), O_RDONLY);
        if (fd >= 0) {
            struct stat st;
            fstat(fd, &st);
            const uint8_t* data = reinterpret_cast<const uint8_t*>(
                mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
            close(fd);

            uint32_t num_blocks = *reinterpret_cast<const uint32_t*>(data);
            const ZoneEntry* entries = reinterpret_cast<const ZoneEntry*>(data + sizeof(uint32_t));

            uint64_t offset = 0;
            for (uint32_t b = 0; b < num_blocks; b++) {
                int32_t  bmin  = entries[b].min_val;
                int32_t  bmax  = entries[b].max_val;
                uint32_t bsize = entries[b].block_size;

                // Keep block if it can contain rows in [SD_MIN, SD_MAX)
                if (!(bmax < SD_MIN || bmin >= SD_MAX)) {
                    qualifying_blocks.push_back({offset, offset + bsize});
                }
                offset += bsize;
            }

            munmap(const_cast<uint8_t*>(data), st.st_size);
        }
        // If zone map unavailable, qualifying_blocks stays empty → full scan below
    }

    // -----------------------------------------------------------------------
    // Phase 2: mmap columns
    // -----------------------------------------------------------------------
    auto mmap_file = [&](const std::string& rel, size_t elem) -> std::pair<const void*, size_t> {
        std::string path = gendb_dir + "/" + rel;
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) { return {nullptr, 0}; }
        struct stat st;
        fstat(fd, &st);
        void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        return {ptr, st.st_size / elem};
    };

    auto [raw_sd, n_sd] = mmap_file("lineitem/l_shipdate.bin",      sizeof(int32_t));
    auto [raw_di, n_di] = mmap_file("lineitem/l_discount.bin",      sizeof(int64_t));
    auto [raw_qt, n_qt] = mmap_file("lineitem/l_quantity.bin",      sizeof(int64_t));
    auto [raw_ep, n_ep] = mmap_file("lineitem/l_extendedprice.bin", sizeof(int64_t));

    const int32_t* col_shipdate  = reinterpret_cast<const int32_t*>(raw_sd);
    const int64_t* col_discount  = reinterpret_cast<const int64_t*>(raw_di);
    const int64_t* col_quantity  = reinterpret_cast<const int64_t*>(raw_qt);
    const int64_t* col_extprice  = reinterpret_cast<const int64_t*>(raw_ep);

    uint64_t total_rows = static_cast<uint64_t>(n_sd);

    // Fall back to full scan if zone map was absent
    if (qualifying_blocks.empty()) {
        qualifying_blocks.push_back({0, total_rows});
    }

    // -----------------------------------------------------------------------
    // Phase 3: Parallel fused scan – morsel-driven over qualifying blocks
    // -----------------------------------------------------------------------
    int num_threads = (int)std::thread::hardware_concurrency();
    if (num_threads < 1)  num_threads = 1;
    if (num_threads > 64) num_threads = 64;

    // Pad to avoid false sharing (64-byte cache lines)
    struct alignas(64) PaddedSum { int64_t val = 0; };
    std::vector<PaddedSum> thread_sums(num_threads);

    {
        GENDB_PHASE("main_scan");

        std::atomic<uint32_t> next_block{0};
        uint32_t num_qblocks = static_cast<uint32_t>(qualifying_blocks.size());

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; t++) {
            threads.emplace_back([&, t]() {
                int64_t local_sum = 0;
                uint32_t bi;
                while ((bi = next_block.fetch_add(1, std::memory_order_relaxed)) < num_qblocks) {
                    const BlockRange& br = qualifying_blocks[bi];
                    uint64_t row_start = br.row_start;
                    uint64_t row_end   = br.row_end;

                    // Fused branch-free filter + accumulate
                    for (uint64_t i = row_start; i < row_end; i++) {
                        int32_t sd   = col_shipdate[i];
                        int64_t disc = col_discount[i];
                        int64_t qty  = col_quantity[i];

                        // Branch-free filter: all three predicates as bitmask
                        int pass = (int)((sd   >= SD_MIN)  &
                                         (sd   <  SD_MAX)  &
                                         (disc >= DISC_MIN) &
                                         (disc <= DISC_MAX) &
                                         (qty  <  QTY_MAX));

                        // Accumulate raw product: ext_price_code * discount_code
                        // Revenue_SQL = raw_sum / 10000 (both scale=100)
                        local_sum += pass * (col_extprice[i] * disc);
                    }
                }
                thread_sums[t].val = local_sum;
            });
        }

        for (auto& th : threads) th.join();
    }

    // -----------------------------------------------------------------------
    // Phase 4: Reduce + output
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        int64_t total_raw = 0;
        for (int t = 0; t < num_threads; t++) {
            total_raw += thread_sums[t].val;
        }

        // revenue = total_raw / 10000 (both scale factors of 100)
        double revenue = static_cast<double>(total_raw) / 10000.0;

        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) {
            // Try to create parent directories
            std::string mkdir_cmd = "mkdir -p " + results_dir;
            system(mkdir_cmd.c_str());
            f = fopen(out_path.c_str(), "w");
        }
        fprintf(f, "revenue\n");
        fprintf(f, "%.2f\n", revenue);
        fclose(f);
    }
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
