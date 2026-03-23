#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

struct MmapU32 {
    int fd = -1;
    size_t bytes = 0;
    const uint32_t* data = nullptr;

    explicit MmapU32(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("open failed: " + path);
        struct stat st {};
        if (::fstat(fd, &st) != 0) throw std::runtime_error("fstat failed: " + path);
        bytes = static_cast<size_t>(st.st_size);
        if (bytes % sizeof(uint32_t) != 0) throw std::runtime_error("size mismatch: " + path);
        void* ptr = ::mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) throw std::runtime_error("mmap failed: " + path);
        data = static_cast<const uint32_t*>(ptr);
    }

    ~MmapU32() {
        if (data) ::munmap(const_cast<uint32_t*>(data), bytes);
        if (fd >= 0) ::close(fd);
    }

    size_t size() const { return bytes / sizeof(uint32_t); }
};

template <typename T>
void write_binary(const std::filesystem::path& path, const std::vector<T>& values) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("failed to open output: " + path.string());
    out.write(reinterpret_cast<const char*>(values.data()),
              static_cast<std::streamsize>(values.size() * sizeof(T)));
    if (!out) throw std::runtime_error("failed to write output: " + path.string());
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    const auto build_start = std::chrono::steady_clock::now();
    const std::filesystem::path gendb_dir = argv[1];
    const std::filesystem::path input_path = gendb_dir / "num" / "adsh.bin";
    const std::filesystem::path out_dir = gendb_dir / "column_versions" / "num.adsh.postings_dense";

    try {
        MmapU32 adsh(input_path.string());
        const size_t row_count = adsh.size();
        uint32_t max_code = 0;
        for (size_t row = 0; row < row_count; ++row) {
            if (adsh.data[row] > max_code) max_code = adsh.data[row];
        }

        std::vector<uint64_t> offsets(static_cast<size_t>(max_code) + 2, 0);
        size_t unique_values = 0;
        for (size_t row = 0; row < row_count; ++row) {
            const uint32_t code = adsh.data[row];
            if (offsets[static_cast<size_t>(code) + 1] == 0) ++unique_values;
            ++offsets[static_cast<size_t>(code) + 1];
        }
        for (size_t idx = 1; idx < offsets.size(); ++idx) offsets[idx] += offsets[idx - 1];

        std::vector<uint64_t> write_pos = offsets;
        std::vector<uint32_t> rowids(row_count);
        for (uint32_t row = 0; row < row_count; ++row) {
            const uint32_t code = adsh.data[row];
            rowids[write_pos[static_cast<size_t>(code)]++] = row;
        }

        std::filesystem::create_directories(out_dir);
        write_binary(out_dir / "offsets.bin", offsets);
        write_binary(out_dir / "rowids.bin", rowids);

        const auto build_end = std::chrono::steady_clock::now();
        const auto build_ms = std::chrono::duration_cast<std::chrono::milliseconds>(build_end - build_start).count();
        std::cout << "row_count=" << row_count
                  << " unique_values=" << unique_values
                  << " max_code=" << max_code
                  << " build_time_ms=" << build_ms << '\n';
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << '\n';
        return 1;
    }
}
