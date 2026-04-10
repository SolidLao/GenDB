#pragma once
// MQO I/O utilities — generated inline helpers for reading GenDB binary columnar data.
// Do NOT depend on external GenDB utility headers; all I/O is self-contained.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <omp.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace mqo {

struct Context {
    std::string gendb_dir;   // path to storage/ directory
    std::string output_dir;  // path for CSV result files
};

}  // namespace mqo

namespace mqo::io {

// ---------------------------------------------------------------------------
// Memory-mapped file I/O
// ---------------------------------------------------------------------------

inline void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::fprintf(stderr, "[MQO] Cannot open: %s\n", path.c_str());
        std::exit(1);
    }
    struct stat st;
    if (::fstat(fd, &st) != 0) {
        std::fprintf(stderr, "[MQO] fstat failed: %s\n", path.c_str());
        ::close(fd);
        std::exit(1);
    }
    out_size = static_cast<size_t>(st.st_size);
    if (out_size == 0) { ::close(fd); return nullptr; }
    void* ptr = ::mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) {
        std::fprintf(stderr, "[MQO] mmap failed: %s\n", path.c_str());
        ::close(fd);
        std::exit(1);
    }
    ::close(fd);
    ::madvise(ptr, out_size, MADV_SEQUENTIAL);
    return ptr;
}

template <typename T>
inline const T* mmap_column(const std::string& path, size_t expected_rows) {
    size_t sz = 0;
    void* ptr = mmap_file(path, sz);
    if (sz < expected_rows * sizeof(T)) {
        std::fprintf(stderr, "[MQO] Column too small: %s (expected >= %zu, got %zu)\n",
                     path.c_str(), expected_rows * sizeof(T), sz);
        std::exit(1);
    }
    return static_cast<const T*>(ptr);
}

template <typename T>
inline const T* mmap_column_unchecked(const std::string& path, size_t& out_count) {
    size_t sz = 0;
    void* ptr = mmap_file(path, sz);
    out_count = sz / sizeof(T);
    return static_cast<const T*>(ptr);
}

// ---------------------------------------------------------------------------
// Meta file parsing
// ---------------------------------------------------------------------------

inline size_t read_row_count(const std::string& meta_path) {
    FILE* f = std::fopen(meta_path.c_str(), "r");
    if (!f) {
        std::fprintf(stderr, "[MQO] Cannot open meta: %s\n", meta_path.c_str());
        std::exit(1);
    }
    size_t n = 0;
    if (std::fscanf(f, "row_count=%zu", &n) != 1) {
        std::fprintf(stderr, "[MQO] Cannot parse row_count from: %s\n", meta_path.c_str());
        std::fclose(f);
        std::exit(1);
    }
    std::fclose(f);
    return n;
}

// ---------------------------------------------------------------------------
// Date encoding: days since 1970-01-01 (epoch)
// ---------------------------------------------------------------------------

constexpr int32_t to_epoch_days(int y, int m, int d) {
    // Howard Hinnant's civil_from_days algorithm (public domain).
    y -= (m <= 2);
    const int era = (y >= 0 ? y : y - 399) / 400;
    const int yoe = y - era * 400;
    const int doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
    const int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return static_cast<int32_t>(era * 146097 + doe - 719468);
}

// Convert epoch-days back to (year, month, day).
inline void from_epoch_days(int32_t z, int& y, int& m, int& d) {
    z += 719468;
    const int era = (z >= 0 ? z : z - 146096) / 146097;
    const int doe = z - era * 146097;
    const int yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    y = yoe + era * 400;
    const int doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const int mp  = (5 * doy + 2) / 153;
    d = doy - (153 * mp + 2) / 5 + 1;
    m = mp < 10 ? mp + 3 : mp - 9;
    y += (m <= 2);
}

// ---------------------------------------------------------------------------
// Dictionary reader (format: uint32 count, then for each: uint16 len, char[len])
// ---------------------------------------------------------------------------

inline std::vector<std::string> read_dictionary(const std::string& path) {
    size_t sz = 0;
    void* raw = mmap_file(path, sz);
    const uint8_t* p = static_cast<const uint8_t*>(raw);
    uint32_t count = 0;
    std::memcpy(&count, p, 4);
    p += 4;
    std::vector<std::string> dict(count);
    for (uint32_t i = 0; i < count; i++) {
        uint16_t len = 0;
        std::memcpy(&len, p, 2);
        p += 2;
        dict[i].assign(reinterpret_cast<const char*>(p), len);
        p += len;
    }
    return dict;
}

// ---------------------------------------------------------------------------
// Varlen column accessor (offset array + data blob)
// Format: <col>.bin = uint32_t offsets[n_rows+1], <col>_data.bin = raw chars
// ---------------------------------------------------------------------------

struct VarlenAccessor {
    const uint32_t* offsets = nullptr;
    const char* data = nullptr;

    std::string_view get(size_t row) const {
        uint32_t s = offsets[row];
        uint32_t e = offsets[row + 1];
        return {data + s, e - s};
    }
};

inline VarlenAccessor mmap_varlen(const std::string& dir, const std::string& col_name) {
    size_t sz1 = 0, sz2 = 0;
    void* offs = mmap_file(dir + "/" + col_name + ".bin", sz1);
    void* data = mmap_file(dir + "/" + col_name + "_data.bin", sz2);
    return {static_cast<const uint32_t*>(offs), static_cast<const char*>(data)};
}

}  // namespace mqo::io
