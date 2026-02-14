// q6.cpp - TPC-H Q6: Forecasting Revenue Change
// Self-contained implementation with zone map pruning and parallel execution

#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <atomic>
#include <cstring>
#include <iomanip>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <immintrin.h>

// Zone map header structure
struct ZoneMapHeader {
    uint64_t num_blocks;
    uint64_t block_size;
};

// Zone map entry for int32 columns (8 bytes per entry)
struct ZoneMapEntry_i32 {
    int32_t min_val;
    int32_t max_val;
};

// Zone map entry for int64 columns (16 bytes per entry)
struct ZoneMapEntry_i64 {
    int64_t min_val;
    int64_t max_val;
};

// Block range for parallel processing
struct BlockRange {
    size_t start_row;
    size_t end_row;
};

// Helper: mmap a file with custom madvise hint
void* mmapFile(const std::string& path, size_t& size_out, int madvise_hint = MADV_RANDOM) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return nullptr;
    }
    size_out = sb.st_size;
    void* ptr = mmap(nullptr, size_out, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) return nullptr;
    if (madvise_hint >= 0) {
        madvise(ptr, size_out, madvise_hint);
    }
    return ptr;
}

// Load zone map for int32 columns
std::vector<ZoneMapEntry_i32> loadZoneMap_i32(const std::string& path, uint64_t& block_size) {
    size_t zm_size;
    void* zm_data = mmapFile(path, zm_size, -1);
    if (!zm_data) return {};

    const ZoneMapHeader* header = (const ZoneMapHeader*)zm_data;
    block_size = header->block_size;

    std::vector<ZoneMapEntry_i32> entries;
    entries.reserve(header->num_blocks);

    const ZoneMapEntry_i32* entry_data = (const ZoneMapEntry_i32*)((char*)zm_data + sizeof(ZoneMapHeader));
    for (uint64_t i = 0; i < header->num_blocks; ++i) {
        entries.push_back(entry_data[i]);
    }

    munmap(zm_data, zm_size);
    return entries;
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto total_start = std::chrono::high_resolution_clock::now();

    // Filter constants
    const int32_t date_min = 8766;  // 1994-01-01 in epoch days
    const int32_t date_max = 9131;  // 1995-01-01 in epoch days (exclusive: < 1995-01-01)
    const int64_t discount_min = 5; // 0.05 * 100
    const int64_t discount_max = 7; // 0.07 * 100
    const int64_t quantity_max = 2400; // 24.00 * 100

    // 1. Load zone map for l_shipdate and build qualifying block list
    uint64_t block_size;
    auto zonemap = loadZoneMap_i32(gendb_dir + "/lineitem_shipdate_zonemap.idx", block_size);

    std::vector<BlockRange> qualifying_blocks;
    for (size_t i = 0; i < zonemap.size(); ++i) {
        // Check if block's [min, max] overlaps with query range [date_min, date_max)
        if (zonemap[i].max_val >= date_min && zonemap[i].min_val < date_max) {
            BlockRange br;
            br.start_row = i * block_size;
            br.end_row = (i + 1) * block_size;
            qualifying_blocks.push_back(br);
        }
    }

    // 2. Load columns with HDD-aware madvise (RANDOM to prevent sequential readahead)
    size_t shipdate_size, discount_size, quantity_size, price_size;
    const int32_t* shipdate = (const int32_t*)mmapFile(gendb_dir + "/lineitem_l_shipdate.bin", shipdate_size, MADV_RANDOM);
    const int64_t* discount = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_discount.bin", discount_size, MADV_RANDOM);
    const int64_t* quantity = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_quantity.bin", quantity_size, MADV_RANDOM);
    const int64_t* price = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_extendedprice.bin", price_size, MADV_RANDOM);

    if (!shipdate || !discount || !quantity || !price) {
        std::cerr << "Failed to load lineitem columns\n";
        return;
    }

    const size_t row_count = shipdate_size / sizeof(int32_t);

    // Adjust last block's end_row to actual row count
    if (!qualifying_blocks.empty()) {
        qualifying_blocks.back().end_row = std::min(qualifying_blocks.back().end_row, row_count);
    }

    // 3. Parallel scan and filter on qualifying blocks only
    auto scan_start = std::chrono::high_resolution_clock::now();

    const size_t num_threads = std::thread::hardware_concurrency();

    std::vector<std::thread> threads;
    std::vector<double> partial_sums(num_threads, 0.0);
    std::vector<double> partial_comps(num_threads, 0.0);
    std::atomic<size_t> matched_rows{0};
    std::atomic<size_t> block_idx{0};

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            double local_sum = 0.0;
            double local_comp = 0.0;
            size_t local_matched = 0;

            // Process qualifying blocks in parallel using atomic counter
            while (true) {
                size_t block_id = block_idx.fetch_add(1, std::memory_order_relaxed);
                if (block_id >= qualifying_blocks.size()) break;

                const BlockRange& block = qualifying_blocks[block_id];
                const size_t start = block.start_row;
                const size_t end = block.end_row;

                size_t i = start;

                // SIMD vectorized loop (process 8 rows at a time with AVX2)
                #ifdef __AVX2__
                const size_t simd_end = start + ((end - start) / 8) * 8;

                __m256i date_min_vec = _mm256_set1_epi32(date_min);
                __m256i date_max_vec = _mm256_set1_epi32(date_max);

                for (; i < simd_end; i += 8) {
                    // Load 8 shipdate values (int32)
                    __m256i shipdate_vec = _mm256_loadu_si256((__m256i*)&shipdate[i]);

                    // shipdate >= date_min (using NOT(shipdate < date_min))
                    __m256i cmp1 = _mm256_cmpgt_epi32(date_min_vec, shipdate_vec);  // date_min > shipdate
                    __m256i ge_min = _mm256_andnot_si256(cmp1, _mm256_set1_epi32(-1));  // NOT(date_min > shipdate) = shipdate >= date_min

                    // shipdate < date_max
                    __m256i lt_max = _mm256_cmpgt_epi32(date_max_vec, shipdate_vec);  // date_max > shipdate

                    // Combine date predicates
                    __m256i date_mask = _mm256_and_si256(ge_min, lt_max);
                    int date_bits = _mm256_movemask_epi8(date_mask);

                    // Early exit if no rows pass date filter
                    if (date_bits == 0) continue;

                    // Process each row in the SIMD batch
                    for (size_t j = 0; j < 8; ++j) {
                        size_t idx = i + j;

                        // Check if this row passed date filter
                        if ((date_bits & (0x0F << (j * 4))) == 0) continue;

                        // Check discount (most selective: 0.09 selectivity)
                        if (discount[idx] < discount_min || discount[idx] > discount_max) continue;

                        // Check quantity
                        if (quantity[idx] >= quantity_max) continue;

                        // All filters passed - compute revenue
                        double revenue = (double)(price[idx] * discount[idx]) / 100.0;

                        // Kahan summation for accuracy
                        double y = revenue - local_comp;
                        double t_val = local_sum + y;
                        local_comp = (t_val - local_sum) - y;
                        local_sum = t_val;

                        local_matched++;
                    }
                }
                #else
                const size_t simd_end = start;
                #endif

                // Scalar tail handling
                for (; i < end; ++i) {
                    // Most selective predicate first (discount)
                    if (discount[i] < discount_min || discount[i] > discount_max) continue;

                    // Date range check
                    if (shipdate[i] < date_min || shipdate[i] >= date_max) continue;

                    // Quantity check
                    if (quantity[i] >= quantity_max) continue;

                    // All filters passed - compute revenue
                    double revenue = (double)(price[i] * discount[i]) / 100.0;

                    // Kahan summation for accuracy
                    double y = revenue - local_comp;
                    double t_val = local_sum + y;
                    local_comp = (t_val - local_sum) - y;
                    local_sum = t_val;

                    local_matched++;
                }
            }

            partial_sums[t] = local_sum;
            partial_comps[t] = local_comp;
            matched_rows.fetch_add(local_matched, std::memory_order_relaxed);
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Combine partial sums with Kahan summation
    double total_revenue = 0.0;
    double total_comp = 0.0;
    for (size_t t = 0; t < num_threads; ++t) {
        double y = partial_sums[t] - total_comp;
        double t_val = total_revenue + y;
        total_comp = (t_val - total_revenue) - y;
        total_revenue = t_val;
    }

    auto scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(scan_end - scan_start).count();
    std::cout << "[TIMING] scan_filter: " << std::fixed << std::setprecision(1) << scan_ms << " ms" << std::endl;

    // 3. Write results if results_dir is provided
    if (!results_dir.empty()) {
        auto output_start = std::chrono::high_resolution_clock::now();

        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        // Divide by scale_factor (100) to get final decimal value
        out << std::fixed << std::setprecision(2) << (total_revenue / 100.0) << "\n";
        out.close();

        auto output_end = std::chrono::high_resolution_clock::now();
        double output_ms = std::chrono::duration<double, std::milli>(output_end - output_start).count();
        std::cout << "[TIMING] output: " << std::fixed << std::setprecision(1) << output_ms << " ms" << std::endl;
    }

    // Cleanup
    munmap((void*)shipdate, shipdate_size);
    munmap((void*)discount, discount_size);
    munmap((void*)quantity, quantity_size);
    munmap((void*)price, price_size);

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    std::cout << "[TIMING] total: " << std::fixed << std::setprecision(1) << total_ms << " ms" << std::endl;
    std::cout << "Query returned 1 rows (matched " << matched_rows.load() << " detail rows)" << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    run_q6(argv[1], argc > 2 ? argv[2] : "");
    return 0;
}
#endif
