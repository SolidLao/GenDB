#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

template <typename T>
struct MmapReadOnly {
    int fd = -1;
    size_t bytes = 0;
    const T* data = nullptr;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("open failed for " + path + ": " + std::strerror(errno));
        }
        struct stat st {};
        if (::fstat(fd, &st) != 0) {
            throw std::runtime_error("fstat failed for " + path + ": " + std::strerror(errno));
        }
        bytes = static_cast<size_t>(st.st_size);
        if (bytes % sizeof(T) != 0) {
            throw std::runtime_error("size mismatch for " + path);
        }
        void* ptr = ::mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) {
            throw std::runtime_error("mmap failed for " + path + ": " + std::strerror(errno));
        }
        data = static_cast<const T*>(ptr);
    }

    size_t size() const {
        return bytes / sizeof(T);
    }

    ~MmapReadOnly() {
        if (data) {
            ::munmap(const_cast<T*>(data), bytes);
        }
        if (fd >= 0) {
            ::close(fd);
        }
    }
};

template <typename T>
struct MmapWritable {
    int fd = -1;
    size_t bytes = 0;
    T* data = nullptr;

    void create(const std::string& path, size_t count) {
        fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            throw std::runtime_error("open failed for " + path + ": " + std::strerror(errno));
        }
        bytes = count * sizeof(T);
        if (::ftruncate(fd, static_cast<off_t>(bytes)) != 0) {
            throw std::runtime_error("ftruncate failed for " + path + ": " + std::strerror(errno));
        }
        void* ptr = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            throw std::runtime_error("mmap failed for " + path + ": " + std::strerror(errno));
        }
        data = static_cast<T*>(ptr);
    }

    void flush() {
        if (data && bytes > 0) {
            ::msync(data, bytes, MS_SYNC);
        }
    }

    ~MmapWritable() {
        if (data) {
            ::munmap(data, bytes);
        }
        if (fd >= 0) {
            ::close(fd);
        }
    }
};

uint8_t checked_u8(double value, double scale, const char* name) {
    const double scaled = value * scale;
    const double rounded = std::llround(scaled);
    if (std::abs(scaled - rounded) > 1e-9 || rounded < 0.0 || rounded > 255.0) {
        throw std::runtime_error(std::string("out-of-range or non-integral scaled value in ") + name);
    }
    return static_cast<uint8_t>(rounded);
}

uint32_t checked_u32(double value, double scale, const char* name) {
    const double scaled = value * scale;
    const double rounded = std::llround(scaled);
    if (std::abs(scaled - rounded) > 1e-6 || rounded < 0.0 ||
        rounded > static_cast<double>(UINT32_MAX)) {
        throw std::runtime_error(std::string("out-of-range or non-integral scaled value in ") + name);
    }
    return static_cast<uint32_t>(rounded);
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }

    try {
        const std::string gendb_dir = argv[1];
        const std::string lineitem_dir = gendb_dir + "/lineitem";
        const std::string out_dir =
            gendb_dir + "/column_versions/lineitem.q6_shipdate_cover";
        std::filesystem::create_directories(out_dir);

        MmapReadOnly<int32_t> shipdate;
        MmapReadOnly<double> discount;
        MmapReadOnly<double> quantity;
        MmapReadOnly<double> extendedprice;
        shipdate.open(lineitem_dir + "/l_shipdate.bin");
        discount.open(lineitem_dir + "/l_discount.bin");
        quantity.open(lineitem_dir + "/l_quantity.bin");
        extendedprice.open(lineitem_dir + "/l_extendedprice.bin");

        const size_t row_count = shipdate.size();
        if (discount.size() != row_count || quantity.size() != row_count ||
            extendedprice.size() != row_count) {
            throw std::runtime_error("row-count mismatch across lineitem columns");
        }

        int32_t min_ship = INT32_MAX;
        int32_t max_ship = INT32_MIN;
        for (size_t i = 0; i < row_count; ++i) {
            min_ship = std::min(min_ship, shipdate.data[i]);
            max_ship = std::max(max_ship, shipdate.data[i]);
        }
        const size_t unique_days = static_cast<size_t>(max_ship - min_ship + 1);

        std::vector<uint64_t> counts(unique_days, 0);
        for (size_t i = 0; i < row_count; ++i) {
            counts[static_cast<size_t>(shipdate.data[i] - min_ship)]++;
        }

        MmapWritable<uint64_t> offsets;
        offsets.create(out_dir + "/offsets.bin", unique_days + 1);
        offsets.data[0] = 0;
        for (size_t i = 0; i < unique_days; ++i) {
            offsets.data[i + 1] = offsets.data[i] + counts[i];
        }

        std::vector<uint64_t> write_pos(unique_days);
        std::memcpy(write_pos.data(), offsets.data, unique_days * sizeof(uint64_t));

        MmapWritable<uint8_t> discount_pct;
        MmapWritable<uint8_t> quantity_units;
        MmapWritable<uint32_t> extendedprice_cents;
        discount_pct.create(out_dir + "/discount_pct.bin", row_count);
        quantity_units.create(out_dir + "/quantity_units.bin", row_count);
        extendedprice_cents.create(out_dir + "/extendedprice_cents.bin", row_count);

        for (size_t row = 0; row < row_count; ++row) {
            const size_t day_idx = static_cast<size_t>(shipdate.data[row] - min_ship);
            const uint64_t pos = write_pos[day_idx]++;
            discount_pct.data[pos] = checked_u8(discount.data[row], 100.0, "l_discount");
            quantity_units.data[pos] = checked_u8(quantity.data[row], 1.0, "l_quantity");
            extendedprice_cents.data[pos] =
                checked_u32(extendedprice.data[row], 100.0, "l_extendedprice");
        }

        offsets.flush();
        discount_pct.flush();
        quantity_units.flush();
        extendedprice_cents.flush();

        std::printf("row_count=%zu\n", row_count);
        std::printf("shipdate_min=%d\n", min_ship);
        std::printf("shipdate_max=%d\n", max_ship);
        std::printf("unique_days=%zu\n", unique_days);
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "build extension failed: %s\n", e.what());
        return 1;
    }
}
