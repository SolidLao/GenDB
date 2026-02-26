// Build pre.plabel dict-encoded version
// Reads pre/plabel.offsets and pre/plabel.data
// Produces:
//   column_versions/pre.plabel.dict/codes.bin   — uint32_t[row_count]
//   column_versions/pre.plabel.dict/dict.offsets — uint64_t[unique+1]
//   column_versions/pre.plabel.dict/dict.data    — raw string bytes
//
// Usage: ./build_ext_plabel_dict <gendb_dir>

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>

static const uint8_t* mmap_ro(const std::string& path, size_t& out_sz) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_sz = (size_t)st.st_size;
    if (out_sz == 0) { close(fd); return nullptr; }
    void* p = mmap(nullptr, out_sz, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return (const uint8_t*)p;
}

static void write_file(const std::string& path, const void* data, size_t sz) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { perror(path.c_str()); exit(1); }
    size_t written = fwrite(data, 1, sz, f);
    if (written != sz) { fprintf(stderr, "short write: %zu / %zu\n", written, sz); exit(1); }
    fclose(f);
}

int main(int argc, char* argv[]) {
    if (argc < 2) { fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]); return 1; }
    std::string G = argv[1];
    std::string out_dir = G + "/column_versions/pre.plabel.dict";

    auto t0 = std::chrono::steady_clock::now();

    // Load plabel offsets and data
    size_t off_sz, data_sz;
    const int64_t* offsets = (const int64_t*)mmap_ro(G + "/pre/plabel.offsets", off_sz);
    const char*    pdata   = (const char*)   mmap_ro(G + "/pre/plabel.data",    data_sz);

    // Number of rows = (off_sz / sizeof(int64_t)) - 1
    size_t n_offsets = off_sz / sizeof(int64_t);
    uint32_t row_count = (uint32_t)(n_offsets - 1);
    fprintf(stderr, "[info] row_count=%u\n", row_count);

    // Hint sequential access
    madvise((void*)offsets, off_sz,  MADV_SEQUENTIAL);
    madvise((void*)pdata,   data_sz, MADV_SEQUENTIAL);

    // Build string → code mapping using std::string keys (safe against dict_strings realloc)
    struct StrHash {
        size_t operator()(const std::string& s) const noexcept {
            uint64_t h = 14695981039346656037ULL;
            for (unsigned char c : s) { h ^= c; h *= 1099511628211ULL; }
            return (size_t)h;
        }
    };

    std::unordered_map<std::string, uint32_t, StrHash> str_to_code;
    str_to_code.reserve(1 << 17); // 128K initial

    // codes array
    std::vector<uint32_t> codes(row_count);

    // dict storage: accumulate unique strings in insertion order
    std::vector<std::string> dict_strings;
    dict_strings.reserve(1 << 17);

    for (uint32_t i = 0; i < row_count; i++) {
        int64_t lo = offsets[i];
        int64_t hi = offsets[i + 1];
        std::string sv(pdata + lo, (size_t)(hi - lo));

        auto it = str_to_code.find(sv);
        if (it != str_to_code.end()) {
            codes[i] = it->second;
        } else {
            uint32_t code = (uint32_t)dict_strings.size();
            str_to_code[sv] = code;
            dict_strings.push_back(std::move(sv));
            codes[i] = code;
        }
    }

    uint32_t unique_count = (uint32_t)dict_strings.size();
    fprintf(stderr, "[info] unique_values=%u\n", unique_count);

    // Build dict.offsets and dict.data
    std::vector<uint64_t> dict_offsets(unique_count + 1);
    uint64_t byte_pos = 0;
    for (uint32_t i = 0; i < unique_count; i++) {
        dict_offsets[i] = byte_pos;
        byte_pos += dict_strings[i].size();
    }
    dict_offsets[unique_count] = byte_pos;

    // Collect dict.data bytes
    std::vector<char> dict_data(byte_pos);
    for (uint32_t i = 0; i < unique_count; i++) {
        memcpy(dict_data.data() + dict_offsets[i], dict_strings[i].data(), dict_strings[i].size());
    }

    // Write outputs
    std::string codes_path   = out_dir + "/codes.bin";
    std::string doffsets_path = out_dir + "/dict.offsets";
    std::string ddata_path   = out_dir + "/dict.data";

    write_file(codes_path,    codes.data(),       codes.size() * sizeof(uint32_t));
    write_file(doffsets_path, dict_offsets.data(), dict_offsets.size() * sizeof(uint64_t));
    write_file(dict_data.size() > 0 ? ddata_path : ddata_path,
               dict_data.data(), dict_data.size());

    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    fprintf(stderr, "[done] row_count=%u unique_values=%u build_time_ms=%.0f\n",
            row_count, unique_count, ms);
    fprintf(stderr, "[files] %s (%zu bytes)\n", codes_path.c_str(), codes.size() * sizeof(uint32_t));
    fprintf(stderr, "[files] %s (%zu bytes)\n", doffsets_path.c_str(), dict_offsets.size() * sizeof(uint64_t));
    fprintf(stderr, "[files] %s (%zu bytes)\n", ddata_path.c_str(), dict_data.size());

    printf("row_count=%u unique_values=%u build_time_ms=%.0f\n", row_count, unique_count, ms);
    return 0;
}
