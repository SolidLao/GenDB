#include <algorithm>
#include <array>
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

[[noreturn]] void fail(const std::string& msg) {
    std::fprintf(stderr, "%s\n", msg.c_str());
    std::exit(1);
}

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

    MMapFile file;
    file.size = static_cast<size_t>(st.st_size);
    file.data = mmap(nullptr, file.size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (file.data == MAP_FAILED) {
        std::perror(("mmap failed: " + path).c_str());
        std::exit(1);
    }

    if (file.size > (1u << 20)) {
        madvise(file.data, file.size, MADV_SEQUENTIAL);
    }
    return file;
}

void radix_sort_u32(std::vector<uint32_t>& values) {
    if (values.size() < 2) {
        return;
    }

    std::vector<uint32_t> tmp(values.size());
    std::vector<uint32_t>* in = &values;
    std::vector<uint32_t>* out = &tmp;

    constexpr uint32_t kRadixBits = 16;
    constexpr uint32_t kRadix = 1u << kRadixBits;
    constexpr uint32_t kRadixMask = kRadix - 1;
    const int threads = std::max(1, omp_get_max_threads());

    std::vector<uint32_t> local_counts(static_cast<size_t>(threads) * kRadix);
    std::vector<uint32_t> thread_pos(static_cast<size_t>(threads) * kRadix);
    std::array<uint32_t, kRadix> bucket_base{};
    std::array<uint32_t, kRadix> bucket_totals{};

    const size_t n = values.size();
    for (uint32_t shift = 0; shift < 32; shift += kRadixBits) {
        std::fill(local_counts.begin(), local_counts.end(), 0u);
        bucket_totals.fill(0u);

#pragma omp parallel num_threads(threads)
        {
            const int tid = omp_get_thread_num();
            uint32_t* local = local_counts.data() + static_cast<size_t>(tid) * kRadix;
            const size_t chunk = (n + static_cast<size_t>(threads) - 1) / static_cast<size_t>(threads);
            const size_t begin = std::min(n, static_cast<size_t>(tid) * chunk);
            const size_t end = std::min(n, begin + chunk);
            const uint32_t* in_data = in->data();

            for (size_t i = begin; i < end; ++i) {
                ++local[(in_data[i] >> shift) & kRadixMask];
            }
        }

        for (uint32_t b = 0; b < kRadix; ++b) {
            uint32_t total = 0;
            for (int t = 0; t < threads; ++t) {
                total += local_counts[static_cast<size_t>(t) * kRadix + b];
            }
            bucket_totals[b] = total;
        }

        uint32_t running = 0;
        for (uint32_t b = 0; b < kRadix; ++b) {
            bucket_base[b] = running;
            running += bucket_totals[b];
        }

        for (uint32_t b = 0; b < kRadix; ++b) {
            uint32_t pos = bucket_base[b];
            for (int t = 0; t < threads; ++t) {
                const size_t idx = static_cast<size_t>(t) * kRadix + b;
                thread_pos[idx] = pos;
                pos += local_counts[idx];
            }
        }

#pragma omp parallel num_threads(threads)
        {
            const int tid = omp_get_thread_num();
            uint32_t* local_pos = thread_pos.data() + static_cast<size_t>(tid) * kRadix;
            const size_t chunk = (n + static_cast<size_t>(threads) - 1) / static_cast<size_t>(threads);
            const size_t begin = std::min(n, static_cast<size_t>(tid) * chunk);
            const size_t end = std::min(n, begin + chunk);
            const uint32_t* in_data = in->data();
            uint32_t* out_data = out->data();

            for (size_t i = begin; i < end; ++i) {
                const uint32_t v = in_data[i];
                out_data[local_pos[(v >> shift) & kRadixMask]++] = v;
            }
        }

        std::swap(in, out);
    }

    if (in != &values) {
        values.swap(*in);
    }
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

    constexpr int32_t kShipdateLo = 8766;
    constexpr int32_t kShipdateHi = 9131;
    constexpr double kDiscountLo = 0.05;
    constexpr double kDiscountHi = 0.07;
    constexpr double kQuantityHi = 24.0;

    const std::string discount_path = gendb_dir + "/lineitem/l_discount.bin";
    const std::string quantity_path = gendb_dir + "/lineitem/l_quantity.bin";
    const std::string extendedprice_path = gendb_dir + "/lineitem/l_extendedprice.bin";

    const std::string postings_offsets_path =
        gendb_dir + "/column_versions/lineitem.l_shipdate.postings_by_day/offsets.bin";
    const std::string postings_row_ids_path =
        gendb_dir + "/column_versions/lineitem.l_shipdate.postings_by_day/row_ids.bin";

    GENDB_PHASE_MS("total", total_ms);

    MMapFile discount_file;
    MMapFile quantity_file;
    MMapFile extendedprice_file;
    MMapFile offsets_file;
    MMapFile row_ids_file;

    {
        GENDB_PHASE("data_loading");
        discount_file = mmap_readonly(discount_path);
        quantity_file = mmap_readonly(quantity_path);
        extendedprice_file = mmap_readonly(extendedprice_path);
        offsets_file = mmap_readonly(postings_offsets_path);
        row_ids_file = mmap_readonly(postings_row_ids_path);
    }

    const auto* l_discount = static_cast<const double*>(discount_file.data);
    const auto* l_quantity = static_cast<const double*>(quantity_file.data);
    const auto* l_extendedprice = static_cast<const double*>(extendedprice_file.data);
    const auto* row_ids = static_cast<const uint32_t*>(row_ids_file.data);

    const uint64_t n_rows = discount_file.size / sizeof(double);
    if (discount_file.size != n_rows * sizeof(double) || quantity_file.size != n_rows * sizeof(double) ||
        extendedprice_file.size != n_rows * sizeof(double)) {
        fail("Column size mismatch in lineitem measure columns");
    }

    if ((row_ids_file.size % sizeof(uint32_t)) != 0) {
        fail("Invalid row_ids.bin size");
    }
    const uint64_t postings_count = row_ids_file.size / sizeof(uint32_t);
    if (postings_count != n_rows) {
        fail("row_ids.bin row count mismatch");
    }

    if (offsets_file.size < 2 * sizeof(int32_t) + sizeof(uint64_t)) {
        fail("Invalid offsets.bin size");
    }

    const uint8_t* off_ptr = static_cast<const uint8_t*>(offsets_file.data);
    const int32_t min_day = *reinterpret_cast<const int32_t*>(off_ptr);
    off_ptr += sizeof(int32_t);
    const int32_t max_day = *reinterpret_cast<const int32_t*>(off_ptr);
    off_ptr += sizeof(int32_t);

    if (max_day < min_day) {
        fail("Invalid offsets.bin day range");
    }

    const uint64_t offset_count = static_cast<uint64_t>(max_day - min_day) + 2;
    const size_t expected_offsets_size = 2 * sizeof(int32_t) + static_cast<size_t>(offset_count) * sizeof(uint64_t);
    if (offsets_file.size != expected_offsets_size) {
        fail("offsets.bin layout mismatch");
    }

    const auto* offsets = reinterpret_cast<const uint64_t*>(off_ptr);
    if (offsets[offset_count - 1] != postings_count) {
        fail("offsets.bin terminal value mismatch");
    }

    uint64_t begin_offset = 0;
    uint64_t end_offset = 0;
    {
        GENDB_PHASE("dim_filter");
        const int32_t lo = std::max(kShipdateLo, min_day);
        const int32_t hi = std::min(kShipdateHi, max_day + 1);
        if (lo < hi) {
            begin_offset = offsets[static_cast<uint64_t>(lo - min_day)];
            end_offset = offsets[static_cast<uint64_t>(hi - min_day)];
        }
    }

    std::vector<uint32_t> candidates;
    {
        GENDB_PHASE("build_joins");
        if (end_offset > begin_offset) {
            const uint64_t candidate_count = end_offset - begin_offset;
            candidates.resize(static_cast<size_t>(candidate_count));
            const uint32_t* src = row_ids + begin_offset;
            std::copy(src, src + candidate_count, candidates.data());
            radix_sort_u32(candidates);
        }
    }

    double revenue = 0.0;
    {
        GENDB_PHASE("main_scan");
        const uint32_t* ids = candidates.data();
        const size_t n = candidates.size();

#pragma omp parallel for schedule(static) reduction(+ : revenue)
        for (size_t i = 0; i < n; ++i) {
            const uint32_t row_id = ids[i];
            const double discount = l_discount[row_id];
            if (discount < kDiscountLo || discount > kDiscountHi) {
                continue;
            }

            if (l_quantity[row_id] >= kQuantityHi) {
                continue;
            }

            revenue += l_extendedprice[row_id] * discount;
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
