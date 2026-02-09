# I/O Libraries and Techniques

## What It Is
High-performance I/O techniques for loading data into memory: mmap (memory-mapped files), io_uring (async I/O), readahead hints, and direct I/O. These avoid expensive syscalls and buffer copies.

## When To Use
- Loading large datasets from disk (>1GB)
- Bulk data ingestion (CSV, Parquet, JSON)
- Avoiding page cache pollution for sequential scans
- Overlapping I/O with computation (async I/O)

## Key Implementation Ideas

### mmap for Fast Data Loading
```cpp
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

class MMapFile {
    void* data_ = nullptr;
    size_t size_ = 0;
    int fd_ = -1;

public:
    bool open(const char* path) {
        fd_ = ::open(path, O_RDONLY);
        if (fd_ < 0) return false;

        struct stat sb;
        if (fstat(fd_, &sb) < 0) return false;
        size_ = sb.st_size;

        // Map entire file into memory
        data_ = mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0);
        if (data_ == MAP_FAILED) {
            data_ = nullptr;
            return false;
        }

        // Hint: sequential access pattern
        madvise(data_, size_, MADV_SEQUENTIAL);

        return true;
    }

    const char* data() const { return static_cast<const char*>(data_); }
    size_t size() const { return size_; }

    ~MMapFile() {
        if (data_) munmap(data_, size_);
        if (fd_ >= 0) close(fd_);
    }
};

// Usage: zero-copy CSV parsing
MMapFile file;
file.open("data.csv");
const char* ptr = file.data();
const char* end = ptr + file.size();
// Parse directly from mapped memory (no read() syscalls)
```

### Readahead and Prefetching
```cpp
#include <fcntl.h>

void setup_sequential_read(int fd, size_t file_size) {
    // Tell kernel we'll read sequentially (double readahead window)
    posix_fadvise(fd, 0, file_size, POSIX_FADV_SEQUENTIAL);

    // Prefetch first 1MB
    posix_fadvise(fd, 0, 1 << 20, POSIX_FADV_WILLNEED);
}

// Prefetch next chunk while processing current chunk
void process_with_prefetch(const char* data, size_t size) {
    constexpr size_t CHUNK_SIZE = 1 << 20; // 1MB

    for (size_t offset = 0; offset < size; offset += CHUNK_SIZE) {
        // Prefetch next chunk
        if (offset + CHUNK_SIZE < size) {
            __builtin_prefetch(data + offset + CHUNK_SIZE, 0, 3);
        }

        // Process current chunk
        process_chunk(data + offset, std::min(CHUNK_SIZE, size - offset));
    }
}
```

### io_uring for Async I/O (Linux 5.1+)
```cpp
#include <liburing.h>

class AsyncFileReader {
    io_uring ring_;

public:
    bool init(size_t queue_depth = 256) {
        return io_uring_queue_init(queue_depth, &ring_, 0) == 0;
    }

    // Submit multiple reads without blocking
    void submit_read(int fd, void* buffer, size_t size, off_t offset, void* user_data) {
        io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        io_uring_prep_read(sqe, fd, buffer, size, offset);
        io_uring_sqe_set_data(sqe, user_data);
    }

    void submit_all() {
        io_uring_submit(&ring_);
    }

    // Wait for completions
    void wait_completion() {
        io_uring_cqe* cqe;
        io_uring_wait_cqe(&ring_, &cqe);

        void* user_data = io_uring_cqe_get_data(cqe);
        int result = cqe->res; // bytes read or error

        io_uring_cqe_seen(&ring_, cqe);
    }

    ~AsyncFileReader() {
        io_uring_queue_exit(&ring_);
    }
};

// Overlap I/O with computation
void parallel_load_and_process() {
    AsyncFileReader reader;
    reader.init();

    std::vector<char> buffers[4];
    for (auto& buf : buffers) buf.resize(1 << 20); // 1MB each

    // Submit 4 read requests
    for (int i = 0; i < 4; ++i) {
        reader.submit_read(fd, buffers[i].data(), buffers[i].size(),
                          i * (1 << 20), (void*)(intptr_t)i);
    }
    reader.submit_all();

    // Wait and process as they complete
    for (int i = 0; i < 4; ++i) {
        reader.wait_completion();
        // Process buffer while other reads are in flight
    }
}
```

### Direct I/O (Bypass Page Cache)
```cpp
#include <fcntl.h>

// For large sequential scans (avoid polluting page cache)
int fd = open("large_table.dat", O_RDONLY | O_DIRECT);

// Buffers must be aligned to block size (512 or 4096 bytes)
void* buffer;
posix_memalign(&buffer, 4096, 1 << 20); // 1MB aligned to 4KB

// Read must be block-aligned
off_t offset = 0;
size_t read_size = 1 << 20; // 1MB (multiple of 4096)
ssize_t n = pread(fd, buffer, read_size, offset);
```

### Memory-Mapped I/O with Huge Pages
```cpp
// Use huge pages (2MB) for large mmap regions
void* addr = mmap(nullptr, file_size, PROT_READ,
                  MAP_PRIVATE | MAP_HUGETLB, fd, 0);

if (addr == MAP_FAILED) {
    // Fallback to regular pages
    addr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
}

// Advise: use huge pages if available
madvise(addr, file_size, MADV_HUGEPAGE);
```

### Avoiding Page Faults with MAP_POPULATE
```cpp
// Prefault pages at mmap time (avoids page faults during access)
void* addr = mmap(nullptr, file_size, PROT_READ,
                  MAP_PRIVATE | MAP_POPULATE, fd, 0);

// Or manually fault in pages
char dummy;
for (size_t i = 0; i < file_size; i += 4096) {
    dummy += ((char*)addr)[i]; // Touch each page
}
```

## Performance Characteristics
- **mmap Speedup**: 2-5x faster than read() for sequential scans (zero-copy)
- **io_uring Latency**: ~1us per operation vs ~10us for traditional I/O
- **Readahead**: Doubles effective I/O bandwidth for sequential access
- **Direct I/O**: Saves memory but 10-20% slower (no page cache benefit)
- **Huge Pages**: Reduces TLB misses by 50-90% for large mmap regions

## Real-World Examples
- **PostgreSQL**: Uses mmap for shared buffers (optional), readahead for sequential scans
- **DuckDB**: mmap for Parquet files, parallel decompression
- **ClickHouse**: Direct I/O for large table scans, async I/O for multi-file reads
- **RocksDB**: io_uring support for async I/O (LSM compaction)

## Pitfalls
- **mmap and Signals**: SIGBUS on I/O errors (corrupted disk); must handle signal
- **mmap Memory Usage**: Large mmap counts against virtual memory limit
- **Page Cache Eviction**: mmap can evict useful pages; use MADV_DONTNEED after scan
- **O_DIRECT Alignment**: Buffer and offset must be 512-byte or 4KB aligned (check with `getconf PAGESIZE`)
- **io_uring Complexity**: Requires careful error handling and queue management
- **Huge Pages Availability**: Must configure /proc/sys/vm/nr_hugepages; may fail if fragmented
- **mmap on NFS**: Poor performance and consistency issues; use pread() instead
- **Readahead Thrashing**: Excessive readahead wastes memory on random access patterns
- **Direct I/O + mmap**: Cannot mix O_DIRECT and mmap on same file (undefined behavior)
