#include <algorithm>
#include <bitset>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <fcntl.h>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "timing_utils.h"

namespace {

constexpr uint64_t kExpectedRows = 59986052ULL;
constexpr uint64_t kExpectedBlocks = 600ULL;
constexpr uint32_t kExpectedBlockSize = 100000U;

constexpr int32_t kShipdateLo = 8766;
constexpr int32_t kShipdateHi = 9131;
constexpr double kDiscountLo = 0.05;
constexpr double kDiscountHi = 0.07;
constexpr double kQuantityHi = 24.0;

struct MMapFile {
    void* data = nullptr;
    size_t size = 0;

    MMapFile() = default;
    MMapFile(const MMapFile&) = delete;
    MMapFile& operator=(const MMapFile&) = delete;

    MMapFile(MMapFile&& other) noexcept : data(other.data), size(other.size) {
        other.data = nullptr;
        other.size = 0;
    }

    MMapFile& operator=(MMapFile&& other) noexcept {
        if (this != &other) {
            if (data != nullptr && data != MAP_FAILED) {
                munmap(data, size);
            }
            data = other.data;
            size = other.size;
            other.data = nullptr;
            other.size = 0;
        }
        return *this;
    }

    ~MMapFile() {
        if (data != nullptr && data != MAP_FAILED) {
            munmap(data, size);
        }
    }
};

MMapFile mmap_readonly(const std::string& path, int advice) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::perror(("open failed: " + path).c_str());
        std::exit(1);
    }

    struct stat st {};
    if (fstat(fd, &st) != 0) {
        std::perror(("fstat failed: " + path).c_str());
        close(fd);
        std::exit(1);
    }

    MMapFile file;
    file.size = static_cast<size_t>(st.st_size);
    file.data = mmap(nullptr, file.size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (file.data == MAP_FAILED) {
        std::perror(("mmap failed: " + path).c_str());
        std::exit(1);
    }

    if (file.size > (1u << 20)) {
        madvise(file.data, file.size, advice);
    }
    return file;
}

template <typename T>
struct ZoneMapView {
    uint32_t block_size = 0;
    uint64_t n = 0;
    uint64_t blocks = 0;
    const T* mins = nullptr;
    const T* maxs = nullptr;
};

template <typename T>
ZoneMapView<T> parse_zonemap(const MMapFile& zm_file, const char* name) {
    constexpr size_t kHeaderBytes = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t);
    if (zm_file.size < kHeaderBytes) {
        std::fprintf(stderr, "Invalid zonemap %s: header too small\n", name);
        std::exit(1);
    }

    const uint8_t* p = static_cast<const uint8_t*>(zm_file.data);
    ZoneMapView<T> v;

    v.block_size = *reinterpret_cast<const uint32_t*>(p);
    p += sizeof(uint32_t);
    v.n = *reinterpret_cast<const uint64_t*>(p);
    p += sizeof(uint64_t);
    v.blocks = *reinterpret_cast<const uint64_t*>(p);
    p += sizeof(uint64_t);

    const size_t arr_bytes = static_cast<size_t>(v.blocks) * sizeof(T);
    const size_t needed = kHeaderBytes + arr_bytes + arr_bytes;
    if (v.block_size == 0 || zm_file.size < needed) {
        std::fprintf(stderr, "Invalid zonemap %s: malformed body\n", name);
        std::exit(1);
    }

    v.mins = reinterpret_cast<const T*>(p);
    p += arr_bytes;
    v.maxs = reinterpret_cast<const T*>(p);
    return v;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];
    std::filesystem::create_directories(results_dir);

    const std::string shipdate_path = gendb_dir + "/lineitem/l_shipdate.bin";
    const std::string discount_path = gendb_dir + "/lineitem/l_discount.bin";
    const std::string quantity_path = gendb_dir + "/lineitem/l_quantity.bin";
    const std::string extendedprice_path = gendb_dir + "/lineitem/l_extendedprice.bin";

    const std::string shipdate_zm_path = gendb_dir + "/lineitem/lineitem_shipdate_zonemap.idx";
    const std::string discount_zm_path = gendb_dir + "/lineitem/lineitem_discount_zonemap.idx";
    const std::string quantity_zm_path = gendb_dir + "/lineitem/lineitem_quantity_zonemap.idx";

    GENDB_PHASE_MS("total", total_ms);

    MMapFile shipdate_file;
    MMapFile discount_file;
    MMapFile quantity_file;
    MMapFile extendedprice_file;
    MMapFile shipdate_zm_file;
    MMapFile discount_zm_file;
    MMapFile quantity_zm_file;

    {
        GENDB_PHASE("data_loading");
        shipdate_file = mmap_readonly(shipdate_path, MADV_SEQUENTIAL);
        discount_file = mmap_readonly(discount_path, MADV_SEQUENTIAL);
        quantity_file = mmap_readonly(quantity_path, MADV_SEQUENTIAL);
        extendedprice_file = mmap_readonly(extendedprice_path, MADV_SEQUENTIAL);

        shipdate_zm_file = mmap_readonly(shipdate_zm_path, MADV_RANDOM);
        discount_zm_file = mmap_readonly(discount_zm_path, MADV_RANDOM);
        quantity_zm_file = mmap_readonly(quantity_zm_path, MADV_RANDOM);
    }

    const auto* l_shipdate = static_cast<const int32_t*>(shipdate_file.data);
    const auto* l_discount = static_cast<const double*>(discount_file.data);
    const auto* l_quantity = static_cast<const double*>(quantity_file.data);
    const auto* l_extendedprice = static_cast<const double*>(extendedprice_file.data);

    const uint64_t n_rows = shipdate_file.size / sizeof(int32_t);
    if (shipdate_file.size != n_rows * sizeof(int32_t) ||
        discount_file.size != n_rows * sizeof(double) ||
        quantity_file.size != n_rows * sizeof(double) ||
        extendedprice_file.size != n_rows * sizeof(double)) {
        std::fprintf(stderr, "Column size mismatch\n");
        return 1;
    }

    const auto shipdate_zm = parse_zonemap<int32_t>(shipdate_zm_file, "lineitem_shipdate_zonemap.idx");
    const auto discount_zm = parse_zonemap<double>(discount_zm_file, "lineitem_discount_zonemap.idx");
    const auto quantity_zm = parse_zonemap<double>(quantity_zm_file, "lineitem_quantity_zonemap.idx");

    if (n_rows != kExpectedRows || shipdate_zm.n != n_rows || discount_zm.n != n_rows || quantity_zm.n != n_rows ||
        shipdate_zm.block_size != kExpectedBlockSize || discount_zm.block_size != kExpectedBlockSize ||
        quantity_zm.block_size != kExpectedBlockSize || shipdate_zm.blocks != kExpectedBlocks ||
        discount_zm.blocks != kExpectedBlocks || quantity_zm.blocks != kExpectedBlocks) {
        std::fprintf(stderr, "Unexpected metadata\n");
        return 1;
    }

    std::bitset<kExpectedBlocks> candidate_mask;
    {
        GENDB_PHASE("dim_filter");
        candidate_mask.reset();
        for (uint64_t b = 0; b < kExpectedBlocks; ++b) {
            const bool pass_shipdate = (shipdate_zm.maxs[b] >= kShipdateLo) && (shipdate_zm.mins[b] < kShipdateHi);
            const bool pass_discount = (discount_zm.maxs[b] >= kDiscountLo) && (discount_zm.mins[b] <= kDiscountHi);
            const bool pass_quantity = (quantity_zm.mins[b] < kQuantityHi);
            if (pass_shipdate && pass_discount && pass_quantity) {
                candidate_mask.set(static_cast<size_t>(b));
            }
        }
    }

    std::vector<uint32_t> candidate_blocks;
    {
        GENDB_PHASE("build_joins");
        candidate_blocks.reserve(kExpectedBlocks);
        for (uint32_t b = 0; b < kExpectedBlocks; ++b) {
            if (candidate_mask.test(b)) {
                candidate_blocks.push_back(b);
            }
        }
    }

    double revenue = 0.0;
    {
        GENDB_PHASE("main_scan");
        if (!candidate_blocks.empty()) {
            const int thread_count = omp_get_max_threads();
            std::vector<double> partial(static_cast<size_t>(thread_count), 0.0);

#pragma omp parallel num_threads(thread_count)
            {
                const int tid = omp_get_thread_num();
                double local_sum = 0.0;

                std::vector<uint32_t> sel_ship;
                std::vector<uint32_t> sel_final;
                sel_ship.reserve(kExpectedBlockSize / 4);
                sel_final.reserve(kExpectedBlockSize / 16);

#pragma omp for schedule(dynamic, 1)
                for (size_t bi = 0; bi < candidate_blocks.size(); ++bi) {
                    const uint64_t block_id = candidate_blocks[bi];
                    const uint64_t begin = block_id * static_cast<uint64_t>(kExpectedBlockSize);
                    const uint64_t end = std::min(begin + static_cast<uint64_t>(kExpectedBlockSize), n_rows);
                    sel_ship.clear();
                    sel_final.clear();

                    for (uint32_t i = static_cast<uint32_t>(begin); i < static_cast<uint32_t>(end); ++i) {
                        const int32_t sd = l_shipdate[i];
                        if (sd >= kShipdateLo && sd < kShipdateHi) {
                            sel_ship.push_back(i);
                        }
                    }

                    for (uint32_t idx : sel_ship) {
                        const double d = l_discount[idx];
                        if (d >= kDiscountLo && d <= kDiscountHi && l_quantity[idx] < kQuantityHi) {
                            sel_final.push_back(idx);
                        }
                    }

                    for (uint32_t idx : sel_final) {
                        local_sum += l_extendedprice[idx] * l_discount[idx];
                    }
                }

                partial[static_cast<size_t>(tid)] = local_sum;
            }

            for (double v : partial) {
                revenue += v;
            }
        }
    }

    {
        GENDB_PHASE("output");
        const std::string out_path = results_dir + "/Q6.csv";
        std::ofstream out(out_path, std::ios::trunc);
        if (!out) {
            std::fprintf(stderr, "Failed to open output file: %s\n", out_path.c_str());
            return 1;
        }

        out.setf(std::ios::fixed);
        out.precision(2);
        out << "revenue\n";
        out << revenue << '\n';
    }

    (void)total_ms;
    return 0;
}
