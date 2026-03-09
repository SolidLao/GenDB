#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace fs = std::filesystem;

struct MmapI64 {
    int fd = -1;
    size_t bytes = 0;
    const int64_t* data = nullptr;
    explicit MmapI64(const fs::path& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("open failed: " + path.string());
        struct stat st {};
        if (fstat(fd, &st) != 0) throw std::runtime_error("fstat failed: " + path.string());
        bytes = static_cast<size_t>(st.st_size);
        data = static_cast<const int64_t*>(mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE, fd, 0));
        if (data == MAP_FAILED) throw std::runtime_error("mmap failed: " + path.string());
    }
    ~MmapI64() {
        if (data && data != MAP_FAILED) munmap(const_cast<int64_t*>(data), bytes);
        if (fd >= 0) close(fd);
    }
    size_t size() const { return bytes / sizeof(int64_t); }
};

template <typename Out>
void write_narrow(const MmapI64& src, const fs::path& out_path, int64_t min_allowed, int64_t max_allowed) {
    std::ofstream out(out_path, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("open for write failed: " + out_path.string());

    int64_t min_seen = std::numeric_limits<int64_t>::max();
    int64_t max_seen = std::numeric_limits<int64_t>::min();
    constexpr size_t kChunk = 1 << 20;
    Out buffer[kChunk];

    const size_t n = src.size();
    for (size_t base = 0; base < n; base += kChunk) {
        const size_t end = std::min(n, base + kChunk);
        for (size_t i = base; i < end; ++i) {
            const int64_t value = src.data[i];
            if (value < min_allowed || value > max_allowed) {
                throw std::runtime_error("value out of target range at row " + std::to_string(i));
            }
            min_seen = std::min(min_seen, value);
            max_seen = std::max(max_seen, value);
            buffer[i - base] = static_cast<Out>(value);
        }
        out.write(reinterpret_cast<const char*>(buffer), static_cast<std::streamsize>((end - base) * sizeof(Out)));
        if (!out) throw std::runtime_error("write failed: " + out_path.string());
    }

    std::cout << out_path.filename().string() << " rows=" << n
              << " min=" << min_seen << " max=" << max_seen << '\n';
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    const auto start = std::chrono::steady_clock::now();
    const fs::path gendb_dir = argv[1];
    const fs::path lineitem_dir = gendb_dir / "lineitem";
    const fs::path versions_dir = gendb_dir / "column_versions";

    fs::create_directories(versions_dir / "lineitem.l_discount.u8");
    fs::create_directories(versions_dir / "lineitem.l_quantity.u16");
    fs::create_directories(versions_dir / "lineitem.l_extendedprice.u32");

    MmapI64 discount(lineitem_dir / "l_discount.bin");
    MmapI64 quantity(lineitem_dir / "l_quantity.bin");
    MmapI64 extendedprice(lineitem_dir / "l_extendedprice.bin");

    const size_t row_count = discount.size();
    if (quantity.size() != row_count || extendedprice.size() != row_count) {
        throw std::runtime_error("lineitem column length mismatch");
    }

    write_narrow<uint8_t>(discount, versions_dir / "lineitem.l_discount.u8" / "values.bin", 0, 255);
    write_narrow<uint16_t>(quantity, versions_dir / "lineitem.l_quantity.u16" / "values.bin", 0, 65535);
    write_narrow<uint32_t>(extendedprice, versions_dir / "lineitem.l_extendedprice.u32" / "values.bin", 0, 4294967295LL);

    const auto end = std::chrono::steady_clock::now();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "row_count=" << row_count << " build_time_ms=" << ms << '\n';
    return 0;
}
