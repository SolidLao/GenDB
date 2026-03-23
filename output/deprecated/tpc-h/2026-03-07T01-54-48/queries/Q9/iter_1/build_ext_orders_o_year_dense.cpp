#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;

struct MMapFile {
    int fd = -1;
    size_t size = 0;
    void* data = nullptr;

    explicit MMapFile(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("open failed: " + path);
        struct stat st {};
        if (::fstat(fd, &st) != 0) throw std::runtime_error("fstat failed: " + path);
        size = static_cast<size_t>(st.st_size);
        if (size == 0) throw std::runtime_error("empty file: " + path);
        data = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) throw std::runtime_error("mmap failed: " + path);
    }

    ~MMapFile() {
        if (data && data != MAP_FAILED) ::munmap(data, size);
        if (fd >= 0) ::close(fd);
    }
};

// Howard Hinnant's civil-from-days conversion specialized for year extraction.
static inline int extract_year_from_days(int32_t z_days_since_1970) {
    int64_t z = static_cast<int64_t>(z_days_since_1970) + 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = static_cast<unsigned>(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int y = static_cast<int>(yoe) + static_cast<int>(era) * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    const unsigned m = mp + (mp < 10 ? 3 : -9);
    y += (m <= 2);
    return y;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    const fs::path gendb_dir(argv[1]);
    const fs::path orderkey_path = gendb_dir / "orders/o_orderkey.bin";
    const fs::path orderdate_path = gendb_dir / "orders/o_orderdate.bin";

    MMapFile ok_file(orderkey_path.string());
    MMapFile od_file(orderdate_path.string());

    if (ok_file.size % sizeof(int32_t) != 0 || od_file.size % sizeof(int32_t) != 0) {
        throw std::runtime_error("orders column size is not multiple of int32_t");
    }

    const size_t n_ok = ok_file.size / sizeof(int32_t);
    const size_t n_od = od_file.size / sizeof(int32_t);
    if (n_ok != n_od) throw std::runtime_error("row count mismatch between o_orderkey and o_orderdate");

    const int32_t* orderkeys = static_cast<const int32_t*>(ok_file.data);
    const int32_t* orderdates = static_cast<const int32_t*>(od_file.data);

    int32_t max_orderkey = 0;
    for (size_t i = 0; i < n_ok; ++i) {
        if (orderkeys[i] > max_orderkey) max_orderkey = orderkeys[i];
    }

    std::vector<int16_t> dense_year(static_cast<size_t>(max_orderkey) + 1, static_cast<int16_t>(-1));
    for (size_t i = 0; i < n_ok; ++i) {
        const int32_t ok = orderkeys[i];
        if (ok < 0) continue;
        dense_year[static_cast<size_t>(ok)] = static_cast<int16_t>(extract_year_from_days(orderdates[i]));
    }

    const fs::path ext_dir = gendb_dir / "column_versions/orders.o_orderdate.year_dense_by_orderkey";
    fs::create_directories(ext_dir);

    const fs::path out_file = ext_dir / "codes.bin";
    std::ofstream out(out_file, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("failed to open output file");
    out.write(reinterpret_cast<const char*>(dense_year.data()), static_cast<std::streamsize>(dense_year.size() * sizeof(int16_t)));
    out.close();

    std::cout << "row_count=" << n_ok << "\n";
    std::cout << "max_orderkey=" << max_orderkey << "\n";
    std::cout << "codes_count=" << dense_year.size() << "\n";
    std::cout << "output=" << out_file.string() << "\n";
    return 0;
}
