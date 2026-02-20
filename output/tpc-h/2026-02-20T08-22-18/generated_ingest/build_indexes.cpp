// TPC-H Index Builder
// Builds multi-value hash indexes from sorted binary column files.
// Compile: g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp
//
// Hash Index File Format:
//   uint32_t num_unique      - number of unique key values
//   uint32_t ht_capacity     - hash table capacity (power of 2)
//   uint32_t num_rows        - total row count (= size of positions array)
//   Slot ht[ht_capacity]     - each: {int32_t key (INT32_MIN=empty), uint32_t offset, uint32_t count}
//   uint32_t positions[num_rows]  - row indices grouped by key (sorted by key value)

#include <algorithm>
#include <cassert>
#include <chrono>
#include <climits>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <omp.h>
#include <parallel/algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace std;

// ============ Mmap Helper ============
struct MCol {
    void*  ptr = MAP_FAILED;
    size_t sz  = 0;
    size_t count = 0;
    MCol() = default;
    MCol(const string& path, size_t elem_size) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); exit(1); }
        struct stat st; fstat(fd, &st);
        sz    = (size_t)st.st_size;
        count = sz / elem_size;
        ptr   = mmap(nullptr, sz, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
        madvise(ptr, sz, MADV_SEQUENTIAL);
        close(fd);
    }
    ~MCol() { if (ptr != MAP_FAILED) munmap(ptr, sz); }
    const int32_t* i32() const { return (const int32_t*)ptr; }
};

// ============ Next power of 2 ============
static uint32_t next_pow2(uint32_t n) {
    if (n == 0) return 1;
    --n; n|=n>>1; n|=n>>2; n|=n>>4; n|=n>>8; n|=n>>16;
    return n+1;
}

// ============ Multiply-shift hash ============
// Maps key → slot in [0, capacity-1] where capacity is power of 2
static inline uint32_t mh(int32_t key, uint32_t cap) {
    // log2(cap): number of trailing zeros since cap is power of 2
    uint32_t shift = (uint32_t)__builtin_ctz(cap);
    return (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> (64u - shift));
}

// ============ Build Multi-Value Hash Index ============
struct Slot { int32_t key; uint32_t offset; uint32_t count; };

static void build_hash_index(const string& col_path, const string& out_path,
                              const string& name) {
    auto t0 = chrono::steady_clock::now();
    MCol col(col_path, 4);
    uint32_t N = (uint32_t)col.count;
    const int32_t* keys = col.i32();
    cout << name << ": N=" << N << " rows\n";

    // Step 1: create positions array [0..N-1]
    vector<uint32_t> positions(N);
    #pragma omp parallel for schedule(static)
    for (uint32_t i = 0; i < N; ++i) positions[i] = i;

    // Step 2: parallel sort positions by key value
    __gnu_parallel::sort(positions.begin(), positions.end(),
                         [&keys](uint32_t a, uint32_t b){ return keys[a] < keys[b]; });

    // Step 3: scan sorted positions to find group boundaries
    // Count unique keys first
    vector<uint32_t> group_starts;
    group_starts.reserve(N / 4);
    group_starts.push_back(0);
    for (uint32_t i = 1; i < N; ++i) {
        if (keys[positions[i]] != keys[positions[i-1]])
            group_starts.push_back(i);
    }
    uint32_t num_unique = (uint32_t)group_starts.size();
    cout << name << ": " << num_unique << " unique keys\n";

    // Step 4: build open-addressing hash table (linear probe, empty=INT32_MIN)
    uint32_t cap = next_pow2(num_unique * 2 + 1);
    // Guard: cap must be >= num_unique (shouldn't happen but be safe)
    while (cap < num_unique) cap <<= 1;
    vector<Slot> ht(cap, {INT32_MIN, 0, 0});

    // Insert each unique key group into hash table
    for (uint32_t g = 0; g < num_unique; ++g) {
        uint32_t start  = group_starts[g];
        uint32_t end_g  = (g + 1 < num_unique) ? group_starts[g+1] : N;
        int32_t  k      = keys[positions[start]];
        uint32_t count  = end_g - start;

        uint32_t slot = mh(k, cap);
        while (ht[slot].key != INT32_MIN) slot = (slot + 1) & (cap - 1);
        ht[slot] = {k, start, count};
    }

    // Step 5: write to file
    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) { perror(out_path.c_str()); exit(1); }

    fwrite(&num_unique, 4, 1, f);
    fwrite(&cap,        4, 1, f);
    fwrite(&N,          4, 1, f);
    fwrite(ht.data(),   sizeof(Slot), cap, f);
    // Write positions in 1MB chunks
    const size_t BLEN = (1<<20) / 4;
    for (size_t off = 0; off < N; off += BLEN) {
        size_t cnt = min(BLEN, (size_t)N - off);
        fwrite(positions.data() + off, 4, cnt, f);
    }
    fclose(f);

    auto t1 = chrono::steady_clock::now();
    size_t fsize = (size_t)3*4 + cap*sizeof(Slot) + N*4;
    cout << name << ": done in "
         << chrono::duration_cast<chrono::seconds>(t1-t0).count()
         << "s, file=" << fsize/(1<<20) << " MB\n";
}

// ============ Main ============
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }
    string db = argv[1];
    string idx = db + "/indexes";
    fs::create_directories(idx);

    omp_set_num_threads(omp_get_max_threads());
    cout << "Using " << omp_get_max_threads() << " OpenMP threads\n";

    auto t0 = chrono::steady_clock::now();

    // Build both hash indexes in parallel (independent)
    thread t1([&](){
        build_hash_index(
            db + "/lineitem/l_orderkey.bin",
            idx + "/lineitem_orderkey_hash.bin",
            "lineitem.l_orderkey");
    });
    thread t2([&](){
        build_hash_index(
            db + "/lineitem/l_partkey.bin",
            idx + "/lineitem_partkey_hash.bin",
            "lineitem.l_partkey");
    });
    t1.join();
    t2.join();

    auto t1e = chrono::steady_clock::now();
    cout << "All indexes built in "
         << chrono::duration_cast<chrono::seconds>(t1e-t0).count() << "s\n";
    return 0;
}
