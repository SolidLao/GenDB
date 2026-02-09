# jemalloc and tcmalloc

## What It Is
High-performance memory allocators that replace system malloc/free. jemalloc (by Facebook) and tcmalloc (by Google) optimize for multi-threaded workloads, reduce fragmentation, and minimize lock contention.

## When To Use
- Multi-threaded query execution with many small allocations
- Workloads with high allocation/deallocation rates (temporary data structures)
- When profiling shows malloc/free as a bottleneck (>5% CPU time)
- Large-scale analytical queries that allocate GBs of temporary data

## Key Implementation Ideas

### Linking jemalloc at Compile Time
```cmake
# CMakeLists.txt
find_package(PkgConfig REQUIRED)
pkg_check_modules(JEMALLOC jemalloc)

target_link_libraries(gendb
    PRIVATE
    ${JEMALLOC_LIBRARIES}
)

# Or link directly
target_link_libraries(gendb PRIVATE jemalloc)
```

```cpp
// main.cpp - Force jemalloc usage
#include <jemalloc/jemalloc.h>

int main() {
    // Verify jemalloc is active
    const char* version;
    size_t len = sizeof(version);
    mallctl("version", &version, &len, nullptr, 0);
    printf("Using jemalloc version: %s\n", version);
}
```

### Dynamic Loading with LD_PRELOAD
```bash
# No code changes needed
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 ./gendb_query

# Or for tcmalloc
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libtcmalloc.so.4 ./gendb_query
```

### tcmalloc Thread-Local Caching
```cpp
// tcmalloc automatically uses thread-local caches
// Configure cache size via environment variable
// TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=512MB ./gendb_query

// In code: allocate normally, tcmalloc optimizes
void* ptr = malloc(64); // Served from thread-local cache (no lock)
free(ptr);              // Returned to thread-local cache
```

### jemalloc Arena per Thread
```cpp
#include <jemalloc/jemalloc.h>

// Create per-thread arena to avoid contention
thread_local unsigned int thread_arena_idx = 0;

void init_thread_arena() {
    size_t sz = sizeof(unsigned int);
    // Create new arena for this thread
    mallctl("arenas.create", &thread_arena_idx, &sz, nullptr, 0);
}

void* thread_local_alloc(size_t size) {
    void* ptr;
    int flags = MALLOCX_ARENA(thread_arena_idx) | MALLOCX_TCACHE_NONE;
    ptr = mallocx(size, flags);
    return ptr;
}
```

### Memory Profiling with jemalloc
```cpp
// Enable profiling at runtime
void enable_jemalloc_profiling() {
    bool prof_active = true;
    mallctl("prof.active", nullptr, nullptr, &prof_active, sizeof(prof_active));

    // Dump heap profile
    const char* filename = "heap_profile.heap";
    mallctl("prof.dump", nullptr, nullptr, &filename, sizeof(filename));
}
```

### Bulk Allocation Optimization
```cpp
// Pre-allocate thread caches to avoid runtime overhead
void warmup_allocator() {
    constexpr size_t WARMUP_SIZE = 1 << 20; // 1MB
    std::vector<void*> warmup_ptrs;

    for (size_t size = 8; size <= 4096; size *= 2) {
        for (int i = 0; i < 100; ++i) {
            warmup_ptrs.push_back(malloc(size));
        }
    }

    for (void* ptr : warmup_ptrs) {
        free(ptr);
    }
}
```

## Performance Characteristics
- **Speedup**: 2-5x faster than glibc malloc for multi-threaded workloads
- **Fragmentation**: 20-40% less memory overhead vs system malloc
- **Scalability**: Near-linear scaling to 64+ threads (vs lock contention in glibc)
- **Latency**: Thread-local caches eliminate lock overhead (~10-50ns per allocation)
- **Memory Usage**: jemalloc uses ~10-20% more memory than tcmalloc (better for throughput)

### Comparison
| Feature | jemalloc | tcmalloc | glibc malloc |
|---------|----------|----------|--------------|
| Thread Scalability | Excellent | Excellent | Poor (>8 threads) |
| Fragmentation | Low | Very Low | Medium |
| Small Allocations | Fast | Very Fast | Slow |
| Memory Overhead | ~15% | ~10% | ~10% |
| Profiling Support | Yes (built-in) | Yes (gperftools) | No |

## Real-World Examples
- **PostgreSQL**: jemalloc used by many deployments for multi-threaded workloads
- **Redis**: jemalloc default allocator (30% memory savings)
- **MariaDB**: tcmalloc reduces memory fragmentation by 40%
- **ClickHouse**: jemalloc improves multi-query throughput by 2-3x

## Pitfalls
- **Incompatibility**: Some libraries assume glibc malloc behavior; test thoroughly
- **Memory Bloat**: Thread-local caches can hold unused memory; tune cache sizes
- **Profiling Overhead**: jemalloc profiling adds 10-20% runtime overhead; disable in production
- **Link Order**: Must link jemalloc/tcmalloc FIRST to override glibc malloc
- **Static Linking**: Requires `-static` and may conflict with other libraries
- **Transparent Huge Pages**: Disable THP on Linux (interferes with jemalloc arenas)
- **Debug Builds**: jemalloc debug mode is 10x slower; use release builds only
- **tcmalloc Central Cache**: Contention on central cache for large allocations (>32KB); use jemalloc for mixed workloads
