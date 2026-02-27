// Build pre_atv_fset.bin: flat hash set of pack_key(adsh,tag,ver) from all pre.bin rows.
// Format: uint64_t cap | cap × uint64_t slots (UINT64_MAX = empty sentinel)
// 9.6M entries at ~50% load → 16M slots × 8 bytes = 128MB
// At query time: load via mmap, no hash set build needed → saves ~200ms
#include <vector>
#include <string>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using hrc = chrono::high_resolution_clock;
static inline double ms_since(hrc::time_point t) { return chrono::duration<double,milli>(hrc::now()-t).count(); }

#pragma pack(push, 1)
struct PreRow { uint32_t adsh_id, tag_id, version_id, plabel_id, line; char stmt[3], rfile[2]; uint8_t inpth, negating; };
#pragma pack(pop)

inline uint64_t pack_key(uint32_t a, uint32_t t, uint32_t v) {
    return ((uint64_t)a << 40) | ((uint64_t)(t & 0xFFFFF) << 20) | (v & 0xFFFFF);
}

static const uint64_t FSET_EMPTY = UINT64_MAX;

struct FlatSet64 {
    vector<uint64_t> slots;
    uint64_t cap, mask;

    void init(uint64_t n_entries) {
        cap = 1;
        while (cap < n_entries * 2) cap <<= 1;  // ~50% load factor
        slots.assign(cap, FSET_EMPTY);
        mask = cap - 1;
    }

    inline void insert(uint64_t key) {
        uint64_t h = key ^ (key >> 33); h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
        uint64_t idx = h & mask;
        while (slots[idx] != FSET_EMPTY && slots[idx] != key) idx = (idx+1) & mask;
        slots[idx] = key;
    }

    void write(const string& path) {
        FILE* f = fopen(path.c_str(), "wb");
        fwrite(&cap, 8, 1, f);
        fwrite(slots.data(), 8, cap, f);
        fclose(f);
    }
};

int main(int argc, char* argv[]) {
    if (argc < 2) { fprintf(stderr, "Usage: ingest_q24 <gendb_dir>\n"); return 1; }
    string gendb_dir = argv[1];
    auto t0 = hrc::now();

    // mmap pre.bin
    int fd = open((gendb_dir + "/pre.bin").c_str(), O_RDONLY);
    if (fd < 0) { fprintf(stderr, "ERROR: pre.bin not found\n"); return 1; }
    struct stat st; fstat(fd, &st);
    const char* m = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    madvise((void*)m, st.st_size, MADV_SEQUENTIAL);
    close(fd);

    uint64_t count; memcpy(&count, m, 8);
    const PreRow* rows = reinterpret_cast<const PreRow*>(m + 8);
    fprintf(stderr, "pre.bin: %lu rows\n", (unsigned long)count);

    // Build flat hash set
    FlatSet64 fset;
    fset.init(count);
    for (uint64_t i = 0; i < count; i++) {
        fset.insert(pack_key(rows[i].adsh_id, rows[i].tag_id, rows[i].version_id));
    }
    munmap((void*)m, st.st_size);
    fprintf(stderr, "hash set built: %.1f ms (cap=%lu, size=%.1f MB)\n",
            ms_since(t0), (unsigned long)fset.cap, (double)fset.cap * 8 / (1024*1024));

    // Write pre_atv_fset.bin
    fset.write(gendb_dir + "/pre_atv_fset.bin");
    fprintf(stderr, "written: %.1f ms\n", ms_since(t0));

    return 0;
}
