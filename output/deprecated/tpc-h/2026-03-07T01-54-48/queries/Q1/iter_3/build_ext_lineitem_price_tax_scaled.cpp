#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

struct MMapFile {
    void* data = nullptr;
    size_t size = 0;

    ~MMapFile() {
        if (data != nullptr && data != MAP_FAILED) {
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

    MMapFile file;
    file.size = static_cast<size_t>(st.st_size);
    file.data = mmap(nullptr, file.size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (file.data == MAP_FAILED) {
        std::perror(("mmap failed: " + path).c_str());
        std::exit(1);
    }
    return file;
}

void write_binary_u32(const std::string& path, const std::vector<uint32_t>& values) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        std::cerr << "Failed to open output file: " << path << "\n";
        std::exit(1);
    }
    out.write(reinterpret_cast<const char*>(values.data()), static_cast<std::streamsize>(values.size() * sizeof(uint32_t)));
    if (!out) {
        std::cerr << "Failed to write output file: " << path << "\n";
        std::exit(1);
    }
}

void write_binary_u16(const std::string& path, const std::vector<uint16_t>& values) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        std::cerr << "Failed to open output file: " << path << "\n";
        std::exit(1);
    }
    out.write(reinterpret_cast<const char*>(values.data()), static_cast<std::streamsize>(values.size() * sizeof(uint16_t)));
    if (!out) {
        std::cerr << "Failed to write output file: " << path << "\n";
        std::exit(1);
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string price_path = gendb_dir + "/lineitem/l_extendedprice.bin";
    const std::string tax_path = gendb_dir + "/lineitem/l_tax.bin";

    const auto price_file = mmap_readonly(price_path);
    const auto tax_file = mmap_readonly(tax_path);

    const uint64_t n_price = price_file.size / sizeof(double);
    const uint64_t n_tax = tax_file.size / sizeof(double);
    if (price_file.size != n_price * sizeof(double) ||
        tax_file.size != n_tax * sizeof(double) ||
        n_price != n_tax) {
        std::cerr << "Column size mismatch\n";
        return 1;
    }

    const auto* price = static_cast<const double*>(price_file.data);
    const auto* tax = static_cast<const double*>(tax_file.data);

    std::vector<uint32_t> price_scaled(n_price);
    std::vector<uint16_t> tax_scaled(n_tax);

    const uint32_t max_price_scaled = 11000000;
    std::vector<uint8_t> price_seen(static_cast<size_t>(max_price_scaled + 1), 0);
    uint32_t tax_seen[65536] = {0};

    uint64_t price_unique = 0;
    uint64_t tax_unique = 0;

    for (uint64_t i = 0; i < n_price; ++i) {
        const long p = std::lround(price[i] * 100.0);
        const long t = std::lround(tax[i] * 100.0);

        if (p < 0 || p > static_cast<long>(max_price_scaled) || t < 0 || t > 65535) {
            std::cerr << "Scaled value out of range at row " << i << "\n";
            return 1;
        }

        const uint32_t ps = static_cast<uint32_t>(p);
        const uint16_t ts = static_cast<uint16_t>(t);
        price_scaled[i] = ps;
        tax_scaled[i] = ts;

        if (price_seen[ps] == 0) {
            price_seen[ps] = 1;
            ++price_unique;
        }
        if (tax_seen[ts] == 0) {
            tax_seen[ts] = 1;
            ++tax_unique;
        }
    }

    const std::string price_dir = gendb_dir + "/column_versions/lineitem.l_extendedprice.int32_scaled_by_100";
    const std::string tax_dir = gendb_dir + "/column_versions/lineitem.l_tax.int16_scaled_by_100";
    std::filesystem::create_directories(price_dir);
    std::filesystem::create_directories(tax_dir);

    write_binary_u32(price_dir + "/codes.bin", price_scaled);
    write_binary_u16(tax_dir + "/codes.bin", tax_scaled);

    std::cout << "row_count=" << n_price
              << " price_unique=" << price_unique
              << " tax_unique=" << tax_unique << "\n";
    return 0;
}
