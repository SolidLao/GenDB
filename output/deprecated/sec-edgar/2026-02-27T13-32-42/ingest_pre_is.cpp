// Build pre_is.bin: pre-sorted IS rows for fast Q6 execution.
// Format:
//   - 8 bytes: uint64_t max_adsh (dense index size)
//   - 4*max_adsh bytes: adsh_count[adsh_id]
//   - 4*(max_adsh+1) bytes: adsh_start[adsh_id] (prefix sums + sentinel)
//   - N * 12 bytes: PreISEntry { uint64_t tv; uint32_t plabel_id; }
//     sorted by (adsh_id, tv) within each bucket
// This allows Q6 to load a single small file instead of double-reading 248MB pre.bin.
#include <string>
#include <vector>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>

#pragma pack(push, 1)
struct PreRow { uint32_t adsh_id, tag_id, version_id, plabel_id, line; char stmt[3], rfile[2]; uint8_t inpth, negating; };
struct PreISEntry { uint64_t tv; uint32_t plabel_id; };  // must match Q6 reader: 12 bytes with pack(1)
#pragma pack(pop)

int main(int argc, char* argv[]) {
    if (argc < 2) { fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];

    // Load sub to get max_adsh
    uint32_t max_adsh = 0;
    {
        FILE* f = fopen((gendb_dir + "/sub.bin").c_str(), "rb");
        if (!f) { fprintf(stderr, "Cannot open sub.bin\n"); return 1; }
        uint64_t count; fread(&count, 8, 1, f);
        // sub rows: read adsh_id (first field) only
        // But SubRow is complex - just scan for max adsh_id
        // SubRow: adsh_id(4) cik(4) name_id(4) sic(4) fy(4) period(4) filed(4) wksi(1) form(11) fp(3) afs(6) countryba(3) countryinc(4) fye(5)
        // Size = 4+4+4+4+4+4+4+1+11+3+6+3+4+5 = 61 bytes
        const int SUB_ROW_SIZE = 61;
        for (uint64_t i = 0; i < count; i++) {
            uint32_t adsh_id;
            fread(&adsh_id, 4, 1, f);
            if (adsh_id > max_adsh) max_adsh = adsh_id;
            fseek(f, SUB_ROW_SIZE - 4, SEEK_CUR);
        }
        fclose(f);
    }
    max_adsh++;
    fprintf(stderr, "max_adsh: %u\n", max_adsh);

    // Read pre.bin, filter IS rows, collect entries
    std::vector<std::pair<uint32_t, PreISEntry>> is_entries; // (adsh_id, entry)
    {
        int fd = open((gendb_dir + "/pre.bin").c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st);
        const char* m = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise((void*)m, st.st_size, MADV_SEQUENTIAL);
        close(fd);

        uint64_t count; memcpy(&count, m, 8);
        const PreRow* rows = reinterpret_cast<const PreRow*>(m + 8);
        is_entries.reserve(2000000);
        for (uint64_t i = 0; i < count; i++) {
            const PreRow& r = rows[i];
            if (r.stmt[0] != 'I' || r.stmt[1] != 'S') continue;
            if (r.adsh_id >= max_adsh) continue;
            uint64_t tv = ((uint64_t)r.tag_id << 20) | r.version_id;
            is_entries.push_back({r.adsh_id, {tv, r.plabel_id}});
        }
        munmap((void*)m, st.st_size);
    }
    fprintf(stderr, "IS entries: %zu\n", is_entries.size());

    // Sort by (adsh_id, tv)
    // Use parallel sort
    std::sort(is_entries.begin(), is_entries.end(), [](const auto& a, const auto& b) {
        if (a.first != b.first) return a.first < b.first;
        return a.second.tv < b.second.tv;
    });

    // Build dense index
    size_t total_is = is_entries.size();
    std::vector<uint32_t> adsh_count(max_adsh, 0);
    for (auto& [adsh, e] : is_entries) adsh_count[adsh]++;
    std::vector<uint32_t> adsh_start(max_adsh + 1, 0);
    for (uint32_t i = 0; i < max_adsh; i++) adsh_start[i+1] = adsh_start[i] + adsh_count[i];

    // Write output
    FILE* out = fopen((gendb_dir + "/pre_is.bin").c_str(), "wb");
    if (!out) { fprintf(stderr, "Cannot open pre_is.bin for writing\n"); return 1; }
    uint64_t max_a = max_adsh;
    fwrite(&max_a, 8, 1, out);
    fwrite(adsh_count.data(), 4, max_adsh, out);
    fwrite(adsh_start.data(), 4, max_adsh + 1, out);
    // Write entries in sorted order (flat array)
    for (auto& [adsh, e] : is_entries) fwrite(&e, sizeof(PreISEntry), 1, out);
    fclose(out);

    long out_size = (8 + 4*max_adsh + 4*(max_adsh+1) + total_is*12);
    fprintf(stderr, "pre_is.bin written: ~%ld MB\n", out_size / (1024*1024));
    return 0;
}
