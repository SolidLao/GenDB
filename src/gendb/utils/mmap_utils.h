#pragma once
#include <cstdint>
#include <cstddef>
#include <string>
#include <stdexcept>
#include <vector>
#include <utility>
#include <climits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace gendb {

// RAII wrapper for memory-mapped column files.
// Zero-copy access to binary columnar data — no allocation, no memcpy.
// Use instead of reading into std::vector to avoid 500ms+ copy overhead on large tables.
template<typename T>
struct MmapColumn {
    const T* data;
    size_t count;     // number of elements
    size_t file_size; // total file size in bytes

    MmapColumn() : data(nullptr), count(0), file_size(0), fd_(-1) {}

    explicit MmapColumn(const std::string& path) : data(nullptr), count(0), file_size(0), fd_(-1) {
        open(path);
    }

    void open(const std::string& path) {
        close(); // clean up any previous mapping
        fd_ = ::open(path.c_str(), O_RDONLY);
        if (fd_ < 0) {
            throw std::runtime_error("MmapColumn: cannot open " + path);
        }
        struct stat st;
        if (fstat(fd_, &st) < 0) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("MmapColumn: cannot stat " + path);
        }
        file_size = st.st_size;
        count = file_size / sizeof(T);
        if (file_size == 0) {
            data = nullptr;
            return;
        }
        void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd_, 0);
        if (ptr == MAP_FAILED) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("MmapColumn: mmap failed for " + path);
        }
        data = static_cast<const T*>(ptr);
        // Advise sequential access for large files
        if (file_size > 1024 * 1024) {
            madvise(const_cast<void*>(static_cast<const void*>(data)), file_size, MADV_SEQUENTIAL);
        }
    }

    void close() {
        if (data && file_size > 0) {
            munmap(const_cast<void*>(static_cast<const void*>(data)), file_size);
        }
        if (fd_ >= 0) ::close(fd_);
        data = nullptr;
        count = 0;
        file_size = 0;
        fd_ = -1;
    }

    ~MmapColumn() { close(); }

    // No copy
    MmapColumn(const MmapColumn&) = delete;
    MmapColumn& operator=(const MmapColumn&) = delete;

    // Move support
    MmapColumn(MmapColumn&& o) noexcept
        : data(o.data), count(o.count), file_size(o.file_size), fd_(o.fd_) {
        o.data = nullptr; o.count = 0; o.file_size = 0; o.fd_ = -1;
    }
    MmapColumn& operator=(MmapColumn&& o) noexcept {
        if (this != &o) {
            close();
            data = o.data; count = o.count; file_size = o.file_size; fd_ = o.fd_;
            o.data = nullptr; o.count = 0; o.file_size = 0; o.fd_ = -1;
        }
        return *this;
    }

    const T& operator[](size_t i) const { return data[i]; }
    size_t size() const { return count; }
    bool empty() const { return count == 0; }

    // Switch kernel readahead hint to MADV_WILLNEED for random-access patterns
    // (e.g., hash-join probe after building hash table on another column).
    // Call after open(), before starting random lookups.
    void advise_random() {
        if (data && file_size > 0) {
            madvise(const_cast<void*>(static_cast<const void*>(data)), file_size, MADV_RANDOM);
        }
    }

    // Re-issue MADV_SEQUENTIAL for sequential scan (default after open).
    void advise_sequential() {
        if (data && file_size > 0) {
            madvise(const_cast<void*>(static_cast<const void*>(data)), file_size, MADV_SEQUENTIAL);
        }
    }

    // Prefetch the entire file into page cache asynchronously (MADV_WILLNEED).
    // Call on all columns that will be needed before the scan loop begins —
    // especially useful for Q9 (6 tables) on HDD to overlap I/O with CPU setup.
    void prefetch() {
        if (data && file_size > 0) {
            madvise(const_cast<void*>(static_cast<const void*>(data)), file_size, MADV_WILLNEED);
        }
    }

private:
    int fd_;
};

// ---------------------------------------------------------------------------
// Prefetch multiple MmapColumns concurrently (MADV_WILLNEED on all of them).
// Call before the main scan loop when multiple tables are accessed in Q9-style
// star joins. Overlap kernel I/O with CPU-side hash table construction.
//
// Usage:
//   MmapColumn<int32_t> col_a("a.bin"), col_b("b.bin"), col_c("c.bin");
//   mmap_prefetch_all(col_a, col_b, col_c);  // fire all readaheads
//   // ... build hash tables, then scan ...
// ---------------------------------------------------------------------------
template<typename T>
inline void mmap_prefetch_all(MmapColumn<T>& col) {
    col.prefetch();
}

template<typename T, typename... Rest>
inline void mmap_prefetch_all(MmapColumn<T>& col, Rest&... rest) {
    col.prefetch();
    mmap_prefetch_all(rest...);
}

// ---------------------------------------------------------------------------
// ZoneMapIndex — reads the binary zone-map index file for a sorted column.
// Index layout (written by build_indexes.cpp):
//   [uint32_t num_zones]
//   [num_zones × ZoneEntry]:  int32_t min, int32_t max, uint32_t row_count, uint32_t row_offset
//   Each ZoneEntry is 16 bytes. row_offset is the *row* index (not byte offset)
//   of the first row in that zone within the sorted column file.
//
// Usage (Q1/Q6 l_shipdate range filter, Q3/Q9 o_orderdate range filter):
//   ZoneMapIndex zm("lineitem_l_shipdate.idx");
//   int32_t lo = date_str_to_epoch_days("1994-01-01");
//   int32_t hi = date_str_to_epoch_days("1995-01-01");
//   // Iterate qualifying zones
//   for (auto& z : zm.zones) {
//       if (z.max < lo || z.min >= hi) continue;  // entire zone out of range
//       size_t start = z.row_offset;
//       size_t end   = z.row_offset + z.row_count;
//       for (size_t i = start; i < end; i++) { /* process row i */ }
//   }
//
// IMPORTANT: row_offset is a ROW index into the binary column file, not bytes.
// To convert to a byte offset into a column file with element size sizeof(T):
//   byte_offset = z.row_offset * sizeof(T)
// ---------------------------------------------------------------------------
struct ZoneEntry {
    int32_t  min;         // minimum value in this zone
    int32_t  max;         // maximum value in this zone
    uint32_t row_count;   // number of rows in this zone
    uint32_t row_offset;  // row index of the first row in this zone
};

struct ZoneMapIndex {
    std::vector<ZoneEntry> zones;

    ZoneMapIndex() = default;

    explicit ZoneMapIndex(const std::string& path) { open(path); }

    void open(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("ZoneMapIndex: cannot open " + path);
        uint32_t num_zones = 0;
        if (::read(fd, &num_zones, sizeof(num_zones)) != sizeof(num_zones)) {
            ::close(fd);
            throw std::runtime_error("ZoneMapIndex: cannot read num_zones from " + path);
        }
        zones.resize(num_zones);
        size_t bytes = num_zones * sizeof(ZoneEntry);
        if (bytes > 0) {
            ssize_t got = ::read(fd, zones.data(), bytes);
            if (got < 0 || static_cast<size_t>(got) != bytes) {
                ::close(fd);
                throw std::runtime_error("ZoneMapIndex: short read on zones from " + path);
            }
        }
        ::close(fd);
    }

    // Count how many rows are skippable for predicate col >= lo AND col < hi.
    // Returns the set of (start_row, end_row) pairs for zones that overlap the range.
    // Zones where max < lo or min >= hi are skipped entirely.
    // NOTE: For a <= predicate (Q1: l_shipdate <= threshold), pass lo=INT32_MIN, hi=threshold+1.
    void qualifying_ranges(int32_t lo, int32_t hi,
                           std::vector<std::pair<uint32_t,uint32_t>>& out) const {
        out.clear();
        for (const auto& z : zones) {
            if (z.max < lo || z.min >= hi) continue;
            out.push_back({z.row_offset, z.row_offset + z.row_count});
        }
    }
};

} // namespace gendb
