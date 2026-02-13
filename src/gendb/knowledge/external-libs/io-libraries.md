# I/O Libraries and Techniques

## What It Is
High-performance I/O techniques for loading data into memory: mmap (memory-mapped files), io_uring (async I/O), readahead hints, and direct I/O. These avoid expensive syscalls and buffer copies.

## Key Implementation Ideas
- **mmap for zero-copy reads**: Map files into virtual memory with mmap(); parse data directly without read() syscalls
- **madvise hints**: Use MADV_SEQUENTIAL for sequential scans, MADV_WILLNEED to prefetch, MADV_DONTNEED to release after scan
- **posix_fadvise readahead**: POSIX_FADV_SEQUENTIAL doubles kernel readahead window; POSIX_FADV_WILLNEED triggers prefetch
- **Software prefetch**: Use __builtin_prefetch on the next chunk while processing the current chunk to hide memory latency
- **io_uring async I/O**: Submit multiple read requests to a submission queue; kernel completes them asynchronously (~1us per op)
- **io_uring batching**: Submit many SQEs before calling io_uring_submit() to amortize syscall overhead
- **Direct I/O (O_DIRECT)**: Bypass page cache for large sequential scans to avoid evicting useful cached pages
- **O_DIRECT alignment**: Buffers and offsets must be aligned to block size (512 or 4096 bytes); use posix_memalign()
- **Huge pages with mmap**: Use MAP_HUGETLB or madvise(MADV_HUGEPAGE) to reduce TLB misses for large mapped regions
- **MAP_POPULATE prefaulting**: Prefault all pages at mmap time to avoid page faults during data access
- **Overlapped I/O + compute**: Use double/quad buffering with io_uring to process one buffer while loading the next
- **pread for random access**: Use pread() instead of mmap for random access patterns or NFS-mounted files
