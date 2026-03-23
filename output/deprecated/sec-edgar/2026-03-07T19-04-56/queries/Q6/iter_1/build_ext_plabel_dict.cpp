#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <unordered_map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

int main(int argc, char* argv[]) {
    if (argc < 2) { fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]); return 1; }
    std::string gendb = argv[1];
    std::string pre_dir = gendb + "/pre";
    std::string out_dir = gendb + "/column_versions/pre.plabel.dict";

    // Mmap plabel offsets
    std::string off_path = pre_dir + "/plabel_offsets.bin";
    int fd = open(off_path.c_str(), O_RDONLY);
    struct stat st;
    fstat(fd, &st);
    size_t off_size = st.st_size;
    size_t num_rows = off_size / sizeof(uint64_t) - 1;
    const uint64_t* offsets = (const uint64_t*)mmap(nullptr, off_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    // Mmap plabel data
    std::string data_path = pre_dir + "/plabel_data.bin";
    fd = open(data_path.c_str(), O_RDONLY);
    fstat(fd, &st);
    size_t data_size = st.st_size;
    const char* data = (const char*)mmap(nullptr, data_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    // Build dictionary: string -> code
    std::unordered_map<std::string, uint32_t> str_to_code;
    str_to_code.reserve(100000);
    std::vector<std::string> dict_strings;

    // Allocate codes array
    std::vector<uint32_t> codes(num_rows);

    for (size_t i = 0; i < num_rows; i++) {
        const char* s = data + offsets[i];
        size_t len = offsets[i + 1] - offsets[i];
        std::string key(s, len);
        auto it = str_to_code.find(key);
        if (it == str_to_code.end()) {
            uint32_t code = (uint32_t)dict_strings.size();
            dict_strings.push_back(std::move(key));
            str_to_code[dict_strings.back()] = code;
            codes[i] = code;
        } else {
            codes[i] = it->second;
        }
    }

    size_t unique_count = dict_strings.size();

    // Write codes.bin
    {
        std::string p = out_dir + "/codes.bin";
        FILE* f = fopen(p.c_str(), "wb");
        fwrite(codes.data(), sizeof(uint32_t), num_rows, f);
        fclose(f);
    }

    // Write dict.offsets (uint64_t[unique_count + 1])
    {
        std::vector<uint64_t> doff(unique_count + 1);
        uint64_t pos = 0;
        for (size_t i = 0; i < unique_count; i++) {
            doff[i] = pos;
            pos += dict_strings[i].size();
        }
        doff[unique_count] = pos;

        std::string p = out_dir + "/dict.offsets";
        FILE* f = fopen(p.c_str(), "wb");
        fwrite(doff.data(), sizeof(uint64_t), unique_count + 1, f);
        fclose(f);
    }

    // Write dict.data
    {
        std::string p = out_dir + "/dict.data";
        FILE* f = fopen(p.c_str(), "wb");
        for (auto& s : dict_strings) {
            fwrite(s.data(), 1, s.size(), f);
        }
        fclose(f);
    }

    munmap((void*)offsets, off_size);
    munmap((void*)data, data_size);

    printf("pre.plabel dict-encoding complete:\n");
    printf("  rows: %zu\n", num_rows);
    printf("  unique values: %zu\n", unique_count);
    return 0;
}
