// Build auxiliary files for Q4:
// 1. tag_fmap.bin: flat open-addressing hash map (tv → tlabel_id) for non-abstract tags
// 2. pre_eq_fmap.bin: flat open-addressing count map (pack3(adsh,tag,ver) → count) for EQ rows
// Format for both: 8 bytes capacity | capacity * {uint64_t key=UINT64_MAX sentinel, uint32_t val}
// Total: 8 + capacity*12 bytes
#include <string>
#include <vector>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
using namespace std;

#pragma pack(push, 1)
struct TagRow { uint32_t tag_id, version_id, tlabel_id; uint8_t custom, abstract_flag, crdr, iord; };
struct PreRow { uint32_t adsh_id, tag_id, version_id, plabel_id, line; char stmt[3], rfile[2]; uint8_t inpth, negating; };
#pragma pack(pop)

struct FlatMap {
    struct Slot { uint64_t key; uint32_t val; };
    vector<Slot> slots;
    uint64_t cap;
    static const uint64_t EMPTY = UINT64_MAX;

    void init(uint64_t n_entries) {
        cap = 1;
        while (cap < n_entries * 4) cap <<= 1;  // ~25% load factor
        slots.assign(cap, {EMPTY, 0});
    }

    void insert(uint64_t key, uint32_t val) {
        uint64_t h = key ^ (key >> 33); h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
        uint64_t idx = h & (cap - 1);
        while (slots[idx].key != EMPTY && slots[idx].key != key) idx = (idx + 1) & (cap - 1);
        slots[idx].key = key;
        slots[idx].val = val;
    }

    void increment(uint64_t key) {
        uint64_t h = key ^ (key >> 33); h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
        uint64_t idx = h & (cap - 1);
        while (slots[idx].key != EMPTY && slots[idx].key != key) idx = (idx + 1) & (cap - 1);
        if (slots[idx].key == EMPTY) slots[idx].key = key;
        slots[idx].val++;
    }

    void write(const string& path) {
        FILE* f = fopen(path.c_str(), "wb");
        fwrite(&cap, 8, 1, f);
        fwrite(slots.data(), sizeof(Slot), cap, f);
        fclose(f);
        fprintf(stderr, "  Written %s: cap=%lu entries, size=%.1f MB\n",
                path.c_str(), (unsigned long)cap, (double)(8 + cap*12)/(1<<20));
    }
};

inline uint64_t pack3(uint32_t a, uint32_t t, uint32_t v) {
    return ((uint64_t)a << 40) | ((uint64_t)(t & 0xFFFFF) << 20) | (v & 0xFFFFF);
}

int main(int argc, char* argv[]) {
    if (argc < 2) { fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]); return 1; }
    string gendb_dir = argv[1];

    // Build tag_fmap.bin
    fprintf(stderr, "Building tag_fmap.bin...\n");
    {
        int fd = open((gendb_dir + "/tag.bin").c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st);
        const char* m = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise((void*)m, st.st_size, MADV_SEQUENTIAL);
        close(fd);

        uint64_t count; memcpy(&count, m, 8);
        const TagRow* rows = reinterpret_cast<const TagRow*>(m + 8);

        // Count non-abstract tags
        uint64_t n_entries = 0;
        for (uint64_t i = 0; i < count; i++) if (rows[i].abstract_flag == 0) n_entries++;

        FlatMap fm;
        fm.init(n_entries);
        for (uint64_t i = 0; i < count; i++) {
            if (rows[i].abstract_flag == 0) {
                uint64_t tv = ((uint64_t)rows[i].tag_id << 20) | rows[i].version_id;
                fm.insert(tv, rows[i].tlabel_id);
            }
        }
        munmap((void*)m, st.st_size);
        fm.write(gendb_dir + "/tag_fmap.bin");
    }

    // Build pre_eq_fmap.bin
    fprintf(stderr, "Building pre_eq_fmap.bin...\n");
    {
        int fd = open((gendb_dir + "/pre.bin").c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st);
        const char* m = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise((void*)m, st.st_size, MADV_SEQUENTIAL);
        close(fd);

        uint64_t count; memcpy(&count, m, 8);
        const PreRow* rows = reinterpret_cast<const PreRow*>(m + 8);

        // Count EQ rows for capacity hint
        uint64_t eq_count = 0;
        for (uint64_t i = 0; i < count; i++) if (rows[i].stmt[0]=='E' && rows[i].stmt[1]=='Q') eq_count++;

        FlatMap fm;
        fm.init(eq_count);
        for (uint64_t i = 0; i < count; i++) {
            if (rows[i].stmt[0]=='E' && rows[i].stmt[1]=='Q') {
                uint64_t k = pack3(rows[i].adsh_id, rows[i].tag_id, rows[i].version_id);
                fm.increment(k);
            }
        }
        munmap((void*)m, st.st_size);
        fm.write(gendb_dir + "/pre_eq_fmap.bin");
    }

    return 0;
}
