#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include <fcntl.h>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "timing_utils.h"

namespace {

constexpr uint64_t kExpectedBlocks = 600;
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

MMapFile mmap_readonly(const std::string& path) {
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

    MMapFile f;
    f.size = static_cast<size_t>(st.st_size);
    f.data = mmap(nullptr, f.size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (f.data == MAP_FAILED) {
        std::perror(("mmap failed: " + path).c_str());
        std::exit(1);
    }
    if (f.size > (1u << 20)) {
        madvise(f.data, f.size, MADV_SEQUENTIAL);
    }
    return f;
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
    ZoneMapView<T> zm;
    zm.block_size = *reinterpret_cast<const uint32_t*>(p);
    p += sizeof(uint32_t);
    zm.n = *reinterpret_cast<const uint64_t*>(p);
    p += sizeof(uint64_t);
    zm.blocks = *reinterpret_cast<const uint64_t*>(p);
    p += sizeof(uint64_t);

    const size_t arr_bytes = static_cast<size_t>(zm.blocks) * sizeof(T);
    const size_t needed = kHeaderBytes + arr_bytes + arr_bytes;
    if (zm.block_size == 0 || zm_file.size < needed) {
        std::fprintf(stderr, "Invalid zonemap %s: malformed body\n", name);
        std::exit(1);
    }

    zm.mins = reinterpret_cast<const T*>(p);
    p += arr_bytes;
    zm.maxs = reinterpret_cast<const T*>(p);
    return zm;
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
        shipdate_file = mmap_readonly(gendb_dir + "/lineitem/l_shipdate.bin");
        discount_file = mmap_readonly(gendb_dir + "/lineitem/l_discount.bin");
        quantity_file = mmap_readonly(gendb_dir + "/lineitem/l_quantity.bin");
        extendedprice_file = mmap_readonly(gendb_dir + "/lineitem/l_extendedprice.bin");

        shipdate_zm_file = mmap_readonly(gendb_dir + "/lineitem/lineitem_shipdate_zonemap.idx");
        discount_zm_file = mmap_readonly(gendb_dir + "/lineitem/lineitem_discount_zonemap.idx");
        quantity_zm_file = mmap_readonly(gendb_dir + "/lineitem/lineitem_quantity_zonemap.idx");
    }

    const auto shipdate_zm = parse_zonemap<int32_t>(shipdate_zm_file, "lineitem_shipdate_zonemap.idx");
    const auto discount_zm = parse_zonemap<double>(discount_zm_file, "lineitem_discount_zonemap.idx");
    const auto quantity_zm = parse_zonemap<double>(quantity_zm_file, "lineitem_quantity_zonemap.idx");

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

    if (shipdate_zm.n != n_rows || discount_zm.n != n_rows || quantity_zm.n != n_rows ||
        shipdate_zm.block_size != discount_zm.block_size ||
        shipdate_zm.block_size != quantity_zm.block_size ||
        shipdate_zm.blocks != discount_zm.blocks ||
        shipdate_zm.blocks != quantity_zm.blocks ||
        shipdate_zm.blocks != kExpectedBlocks) {
        std::fprintf(stderr, "Unexpected zonemap metadata\n");
        return 1;
    }

    std::array<uint8_t, kExpectedBlocks> block_keep {};
    {
        GENDB_PHASE("dim_filter");
        for (uint64_t b = 0; b < kExpectedBlocks; ++b) {
            const bool pass_shipdate =
                (shipdate_zm.maxs[b] >= kShipdateLo) && (shipdate_zm.mins[b] < kShipdateHi);
            const bool pass_discount =
                (discount_zm.maxs[b] >= kDiscountLo) && (discount_zm.mins[b] <= kDiscountHi);
            const bool pass_quantity = quantity_zm.mins[b] < kQuantityHi;
            block_keep[static_cast<size_t>(b)] = static_cast<uint8_t>(pass_shipdate && pass_discount && pass_quantity);
        }
    }

    uint64_t kept_blocks = 0;
    {
        GENDB_PHASE("build_joins");
        for (uint64_t b = 0; b < kExpectedBlocks; ++b) {
            kept_blocks += block_keep[static_cast<size_t>(b)];
        }
    }

    double revenue = 0.0;
    {
        GENDB_PHASE("main_scan");
        if (kept_blocks != 0) {
#pragma omp parallel for schedule(static) reduction(+ : revenue)
            for (uint64_t b = 0; b < kExpectedBlocks; ++b) {
                if (!block_keep[static_cast<size_t>(b)]) {
                    continue;
                }

                const uint64_t begin = b * static_cast<uint64_t>(shipdate_zm.block_size);
                const uint64_t end =
                    std::min(begin + static_cast<uint64_t>(shipdate_zm.block_size), n_rows);

                for (uint64_t i = begin; i < end; ++i) {
                    const int32_t shipdate = l_shipdate[i];
                    if (shipdate < kShipdateLo || shipdate >= kShipdateHi) {
                        continue;
                    }

                    const double discount = l_discount[i];
                    if (discount < kDiscountLo || discount > kDiscountHi) {
                        continue;
                    }

                    if (l_quantity[i] >= kQuantityHi) {
                        continue;
                    }

                    revenue += l_extendedprice[i] * discount;
                }
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
