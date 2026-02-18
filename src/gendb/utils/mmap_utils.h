#pragma once
#include <cstdint>
#include <cstddef>
#include <string>
#include <stdexcept>
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

} // namespace gendb
