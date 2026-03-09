#include <algorithm>
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

MMapFile mmap_readonly(const std::string& path, int madvise_hint) {
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
        madvise(file.data, file.size, madvise_hint);
    }
    return file;
}

struct ShipdatePostingsView {
    int32_t min_day = 0;
    int32_t max_day = -1;
    const uint64_t* offsets = nullptr;
    uint64_t offsets_count = 0;
    const uint32_t* row_ids = nullptr;
    uint64_t row_ids_count = 0;
};

ShipdatePostingsView parse_shipdate_postings(const MMapFile& offsets_file,
                                             const MMapFile& row_ids_file) {
    if (offsets_file.size < sizeof(int32_t) * 2 + sizeof(uint64_t)) {
        std::fprintf(stderr, "Invalid offsets.bin: file too small\n");
        std::exit(1);
    }
    if ((row_ids_file.size % sizeof(uint32_t)) != 0) {
        std::fprintf(stderr, "Invalid row_ids.bin: size not divisible by 4\n");
        std::exit(1);
    }

    const uint8_t* p = static_cast<const uint8_t*>(offsets_file.data);
    ShipdatePostingsView view;
    view.min_day = *reinterpret_cast<const int32_t*>(p);
    p += sizeof(int32_t);
    view.max_day = *reinterpret_cast<const int32_t*>(p);
    p += sizeof(int32_t);

    if (view.max_day < view.min_day) {
        std::fprintf(stderr, "Invalid offsets.bin: max_day < min_day\n");
        std::exit(1);
    }

    const int64_t span = static_cast<int64_t>(view.max_day) - static_cast<int64_t>(view.min_day) + 1;
    const uint64_t required_offsets = static_cast<uint64_t>(span) + 1;

    const size_t payload_bytes = offsets_file.size - sizeof(int32_t) * 2;
    if ((payload_bytes % sizeof(uint64_t)) != 0) {
        std::fprintf(stderr, "Invalid offsets.bin: malformed offsets payload\n");
        std::exit(1);
    }

    view.offsets_count = static_cast<uint64_t>(payload_bytes / sizeof(uint64_t));
    view.offsets = reinterpret_cast<const uint64_t*>(p);
    view.row_ids = static_cast<const uint32_t*>(row_ids_file.data);
    view.row_ids_count = static_cast<uint64_t>(row_ids_file.size / sizeof(uint32_t));

    if (view.offsets_count < required_offsets) {
        std::fprintf(stderr, "Invalid offsets.bin: insufficient offsets\n");
        std::exit(1);
    }

    if (view.offsets[0] != 0) {
        std::fprintf(stderr, "Invalid offsets.bin: first offset must be 0\n");
        std::exit(1);
    }
    if (view.offsets[required_offsets - 1] != view.row_ids_count) {
        std::fprintf(stderr, "Invalid postings index: terminal offset mismatch\n");
        std::exit(1);
    }

    return view;
}

struct alignas(64) PaddedDouble {
    double value;
};

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];
    std::filesystem::create_directories(results_dir);

    constexpr int32_t kShipdateLo = 8766;
    constexpr int32_t kShipdateHi = 9131;
    constexpr double kDiscountLo = 0.05;
    constexpr double kDiscountHi = 0.07;
    constexpr double kQuantityHi = 24.0;

    const std::string discount_path = gendb_dir + "/lineitem/l_discount.bin";
    const std::string quantity_path = gendb_dir + "/lineitem/l_quantity.bin";
    const std::string extendedprice_path = gendb_dir + "/lineitem/l_extendedprice.bin";
    const std::string offsets_path =
        gendb_dir + "/column_versions/lineitem.l_shipdate.postings_by_day/offsets.bin";
    const std::string row_ids_path =
        gendb_dir + "/column_versions/lineitem.l_shipdate.postings_by_day/row_ids.bin";

    GENDB_PHASE_MS("total", total_ms);

    MMapFile discount_file;
    MMapFile quantity_file;
    MMapFile extendedprice_file;
    MMapFile offsets_file;
    MMapFile row_ids_file;

    {
        GENDB_PHASE("data_loading");
        discount_file = mmap_readonly(discount_path, MADV_RANDOM);
        quantity_file = mmap_readonly(quantity_path, MADV_RANDOM);
        extendedprice_file = mmap_readonly(extendedprice_path, MADV_RANDOM);
        offsets_file = mmap_readonly(offsets_path, MADV_SEQUENTIAL);
        row_ids_file = mmap_readonly(row_ids_path, MADV_SEQUENTIAL);
    }

    const auto* l_discount = static_cast<const double*>(discount_file.data);
    const auto* l_quantity = static_cast<const double*>(quantity_file.data);
    const auto* l_extendedprice = static_cast<const double*>(extendedprice_file.data);

    const uint64_t n_rows = discount_file.size / sizeof(double);
    if (discount_file.size != n_rows * sizeof(double) ||
        quantity_file.size != n_rows * sizeof(double) ||
        extendedprice_file.size != n_rows * sizeof(double)) {
        std::fprintf(stderr, "Column size mismatch\n");
        return 1;
    }

    ShipdatePostingsView postings;
    int32_t lookup_lo_day = kShipdateLo;
    int32_t lookup_hi_day = kShipdateLo;
    uint64_t postings_begin = 0;
    uint64_t postings_end = 0;

    {
        GENDB_PHASE("dim_filter");
        postings = parse_shipdate_postings(offsets_file, row_ids_file);

        lookup_lo_day = std::max(kShipdateLo, postings.min_day);
        lookup_hi_day = std::min(kShipdateHi, postings.max_day + 1);

        if (lookup_lo_day < lookup_hi_day) {
            const uint64_t lo_idx = static_cast<uint64_t>(lookup_lo_day - postings.min_day);
            const uint64_t hi_idx = static_cast<uint64_t>(lookup_hi_day - postings.min_day);
            postings_begin = postings.offsets[lo_idx];
            postings_end = postings.offsets[hi_idx];
        }
    }

    {
        GENDB_PHASE("build_joins");
        if (postings_end < postings_begin || postings_end > postings.row_ids_count) {
            std::fprintf(stderr, "Invalid postings slice bounds\n");
            return 1;
        }
    }

    double revenue = 0.0;
    {
        GENDB_PHASE("main_scan");
        const uint64_t candidates = postings_end - postings_begin;
        if (candidates > 0) {
            const int max_threads = omp_get_max_threads();
            std::vector<PaddedDouble> partial(static_cast<size_t>(max_threads));
            for (int t = 0; t < max_threads; ++t) {
                partial[static_cast<size_t>(t)].value = 0.0;
            }

#pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                double local_sum = 0.0;

#pragma omp for schedule(static)
                for (uint64_t pos = postings_begin; pos < postings_end; ++pos) {
                    const uint32_t row_id = postings.row_ids[pos];
                    if (row_id >= n_rows) {
                        continue;
                    }

                    const double discount = l_discount[row_id];
                    if (discount < kDiscountLo || discount > kDiscountHi) {
                        continue;
                    }

                    if (l_quantity[row_id] >= kQuantityHi) {
                        continue;
                    }

                    local_sum += l_extendedprice[row_id] * discount;
                }

                partial[static_cast<size_t>(tid)].value = local_sum;
            }

            for (const auto& slot : partial) {
                revenue += slot.value;
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
