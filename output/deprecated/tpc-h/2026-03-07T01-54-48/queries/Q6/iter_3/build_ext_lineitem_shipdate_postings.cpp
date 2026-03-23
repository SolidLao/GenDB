#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

struct MMapFile {
    void* data = nullptr;
    size_t size = 0;
    ~MMapFile() {
        if (data && data != MAP_FAILED) {
            munmap(data, size);
        }
    }
};

MMapFile mmap_readonly(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::perror(("open failed: " + path).c_str());
        std::exit(1);
    }
    struct stat st {};
    if (fstat(fd, &st) != 0) {
        std::perror(("fstat failed: " + path).c_str());
        close(fd);
        std::exit(1);
    }
    MMapFile f;
    f.size = static_cast<size_t>(st.st_size);
    f.data = mmap(nullptr, f.size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (f.data == MAP_FAILED) {
        std::perror(("mmap failed: " + path).c_str());
        std::exit(1);
    }
    return f;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string col_path = gendb_dir + "/lineitem/l_shipdate.bin";
    const std::string out_dir = gendb_dir + "/column_versions/lineitem.l_shipdate.postings_by_day";

    auto t0 = std::chrono::steady_clock::now();

    MMapFile ship = mmap_readonly(col_path);
    if (ship.size % sizeof(int32_t) != 0) {
        std::cerr << "Invalid l_shipdate size\n";
        return 1;
    }

    const auto* vals = static_cast<const int32_t*>(ship.data);
    const uint64_t n = ship.size / sizeof(int32_t);
    if (n == 0) {
        std::cerr << "Empty l_shipdate\n";
        return 1;
    }

    int32_t min_day = vals[0];
    int32_t max_day = vals[0];
    for (uint64_t i = 1; i < n; ++i) {
        min_day = std::min(min_day, vals[i]);
        max_day = std::max(max_day, vals[i]);
    }

    const uint64_t domain = static_cast<uint64_t>(static_cast<int64_t>(max_day) - static_cast<int64_t>(min_day) + 1);
    std::vector<uint64_t> counts(domain, 0);
    for (uint64_t i = 0; i < n; ++i) {
        counts[static_cast<uint64_t>(vals[i] - min_day)]++;
    }

    std::vector<uint64_t> offsets(domain + 1, 0);
    for (uint64_t d = 0; d < domain; ++d) {
        offsets[d + 1] = offsets[d] + counts[d];
    }

    std::vector<uint64_t> write_pos = offsets;
    std::vector<uint32_t> row_ids(n);
    for (uint32_t i = 0; i < n; ++i) {
        const uint64_t bucket = static_cast<uint64_t>(vals[i] - min_day);
        row_ids[write_pos[bucket]++] = i;
    }

    std::filesystem::create_directories(out_dir);

    {
        std::ofstream out(out_dir + "/row_ids.bin", std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(row_ids.data()), static_cast<std::streamsize>(row_ids.size() * sizeof(uint32_t)));
    }

    {
        std::ofstream out(out_dir + "/offsets.bin", std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(&min_day), sizeof(int32_t));
        out.write(reinterpret_cast<const char*>(&max_day), sizeof(int32_t));
        out.write(reinterpret_cast<const char*>(offsets.data()), static_cast<std::streamsize>(offsets.size() * sizeof(uint64_t)));
    }

    auto t1 = std::chrono::steady_clock::now();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    std::cout << "row_count=" << n << "\n";
    std::cout << "min_day=" << min_day << " max_day=" << max_day << "\n";
    std::cout << "unique_values=" << domain << "\n";
    std::cout << "build_time_ms=" << ms << "\n";
    return 0;
}
