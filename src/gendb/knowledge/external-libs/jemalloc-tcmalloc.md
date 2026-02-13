# jemalloc and tcmalloc

## What It Is
High-performance memory allocators that replace system malloc/free. jemalloc (Facebook) and tcmalloc (Google) optimize for multi-threaded workloads, reduce fragmentation, and minimize lock contention.

## Key Implementation Ideas
- **Compile-time linking**: Link jemalloc/tcmalloc via CMake (find_package or direct target_link_libraries)
- **LD_PRELOAD injection**: Swap allocator at runtime with no code changes via LD_PRELOAD
- **Thread-local caching (tcmalloc)**: Each thread has a local cache; small allocations served lock-free (~10-50ns)
- **Per-thread arenas (jemalloc)**: Create dedicated arenas per thread via mallctl("arenas.create") to eliminate contention
- **Arena binding**: Use MALLOCX_ARENA flag to allocate from a specific arena
- **Runtime profiling (jemalloc)**: Enable heap profiling via mallctl("prof.active") and dump with mallctl("prof.dump")
- **Cache warmup**: Pre-allocate and free a range of sizes at startup to prime thread-local caches
- **tcmalloc cache tuning**: Control total thread cache size via TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES env var
- **Link order matters**: jemalloc/tcmalloc must be linked FIRST to properly override glibc malloc
- **Size class optimization**: Both allocators use size classes to reduce fragmentation; align allocation sizes to size class boundaries
