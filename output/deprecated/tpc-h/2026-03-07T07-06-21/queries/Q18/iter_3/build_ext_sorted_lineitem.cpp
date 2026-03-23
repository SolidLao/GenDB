// build_ext_sorted_lineitem.cpp
// Builds a sorted copy of lineitem (l_orderkey, l_quantity) sorted by l_orderkey.
// Output:
//   <gendb_dir>/column_versions/lineitem.l_orderkey_qty.sorted/sorted_orderkey.bin
//   <gendb_dir>/column_versions/lineitem.l_orderkey_qty.sorted/sorted_quantity.bin
//
// This enables Pass 1 of Q18 to be fully sequential (no random writes), eliminating
// the large-array accumulator and merge phase entirely.

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include <string>
#include <chrono>
#include <thread>

struct MmapFile {
    void*  data = nullptr;
    size_t size = 0;
    int    fd   = -1;

    bool open(const char* path) {
        fd = ::open(path, O_RDONLY);
        if (fd < 0) { perror(path); return false; }
        struct stat st; fstat(fd, &st);
        size = (size_t)st.st_size;
        if (size == 0) return true;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); data = nullptr; return false; }
        madvise(data, size, MADV_SEQUENTIAL);
        return true;
    }
    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) close(fd);
    }
};

// Simple parallel radix sort for (int32_t key, int8_t qty) pairs sorted by key.
// Uses counting sort (key range 1..60M, fits in 4 passes of 8-bit radix).
struct Pair {
    int32_t key;
    int8_t  qty;
};

static void radix_sort_pairs(std::vector<Pair>& arr) {
    const size_t N = arr.size();
    std::vector<Pair> tmp(N);

    // 4 passes, 8 bits each (keys are non-negative int32_t in [1, 60M])
    for (int pass = 0; pass < 4; pass++) {
        const int shift = pass * 8;
        uint64_t cnt[256] = {};

        // Count
        for (size_t i = 0; i < N; i++) {
            uint8_t byte = (uint8_t)((uint32_t)arr[i].key >> shift);
            cnt[byte]++;
        }
        // Prefix sum
        uint64_t cum = 0;
        for (int b = 0; b < 256; b++) {
            uint64_t c = cnt[b];
            cnt[b] = cum;
            cum += c;
        }
        // Scatter
        for (size_t i = 0; i < N; i++) {
            uint8_t byte = (uint8_t)((uint32_t)arr[i].key >> shift);
            tmp[cnt[byte]++] = arr[i];
        }
        std::swap(arr, tmp);
    }
}

static bool write_file(const std::string& path, const void* data, size_t bytes) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { perror(path.c_str()); return false; }
    size_t written = fwrite(data, 1, bytes, f);
    fclose(f);
    if (written != bytes) {
        fprintf(stderr, "Write error: expected %zu bytes, wrote %zu\n", bytes, written);
        return false;
    }
    return true;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir = argv[1];

    auto t0 = std::chrono::high_resolution_clock::now();

    // ── Open source files ────────────────────────────────────────────────────
    MmapFile f_okey, f_qty;
    if (!f_okey.open((gendb_dir + "/lineitem/l_orderkey.bin").c_str())) return 1;
    if (!f_qty.open ((gendb_dir + "/lineitem/l_quantity.bin").c_str()))  return 1;

    const int32_t* src_okey = (const int32_t*)f_okey.data;
    const int8_t*  src_qty  = (const int8_t*) f_qty.data;
    const size_t   N        = f_okey.size / sizeof(int32_t);

    if (f_qty.size != N) {
        fprintf(stderr, "Row count mismatch: l_orderkey has %zu rows, l_quantity has %zu rows\n",
                N, f_qty.size);
        return 1;
    }

    printf("Sorting %zu lineitem rows by l_orderkey...\n", N);

    // ── Build pairs array ────────────────────────────────────────────────────
    std::vector<Pair> pairs(N);
    {
        const int nthreads = (int)std::thread::hardware_concurrency();
        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        const size_t chunk = (N + nthreads - 1) / nthreads;
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                const size_t lo = (size_t)t * chunk;
                const size_t hi = std::min(lo + chunk, N);
                for (size_t i = lo; i < hi; i++) {
                    pairs[i] = {src_okey[i], src_qty[i]};
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    printf("  Pairs built: %.1f ms\n",
           std::chrono::duration<double,std::milli>(t1-t0).count());

    // ── Radix sort by l_orderkey ─────────────────────────────────────────────
    radix_sort_pairs(pairs);

    auto t2 = std::chrono::high_resolution_clock::now();
    printf("  Sort complete: %.1f ms\n",
           std::chrono::duration<double,std::milli>(t2-t1).count());

    // Verify: check sorted order and collect stats
    int32_t min_key = pairs[0].key, max_key = pairs[0].key;
    size_t  unique_keys = 1;
    for (size_t i = 1; i < N; i++) {
        if (pairs[i].key < pairs[i-1].key) {
            fprintf(stderr, "Sort error at index %zu: key %d < prev %d\n",
                    i, pairs[i].key, pairs[i-1].key);
            return 1;
        }
        if (pairs[i].key != pairs[i-1].key) unique_keys++;
        if (pairs[i].key > max_key) max_key = pairs[i].key;
    }
    printf("  Verified: key range [%d, %d], unique_keys=%zu\n",
           min_key, max_key, unique_keys);

    // ── Create output directory ──────────────────────────────────────────────
    const std::string out_dir = gendb_dir +
        "/column_versions/lineitem.l_orderkey_qty.sorted";
    {
        std::string cmd = "mkdir -p \"" + out_dir + "\"";
        if (system(cmd.c_str()) != 0) {
            fprintf(stderr, "Failed to create output directory\n");
            return 1;
        }
    }

    // ── Write sorted_orderkey.bin (int32_t[N]) ───────────────────────────────
    {
        std::vector<int32_t> okeys(N);
        for (size_t i = 0; i < N; i++) okeys[i] = pairs[i].key;
        if (!write_file(out_dir + "/sorted_orderkey.bin", okeys.data(),
                        N * sizeof(int32_t))) return 1;
        printf("  Written: sorted_orderkey.bin (%zu bytes)\n", N * sizeof(int32_t));
    }

    // ── Write sorted_quantity.bin (int8_t[N]) ────────────────────────────────
    {
        std::vector<int8_t> qtys(N);
        for (size_t i = 0; i < N; i++) qtys[i] = pairs[i].qty;
        if (!write_file(out_dir + "/sorted_quantity.bin", qtys.data(),
                        N * sizeof(int8_t))) return 1;
        printf("  Written: sorted_quantity.bin (%zu bytes)\n", N * sizeof(int8_t));
    }

    auto t3 = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double,std::milli>(t3-t0).count();

    printf("Done: %zu rows, %zu unique orderkeys, build_time=%.1f ms\n",
           N, unique_keys, total_ms);
    return 0;
}
