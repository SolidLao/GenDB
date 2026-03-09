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

void write_binary(const std::string& path, const std::vector<uint16_t>& values) {
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
    const std::string discount_path = gendb_dir + "/lineitem/l_discount.bin";
    const std::string quantity_path = gendb_dir + "/lineitem/l_quantity.bin";

    const auto discount_file = mmap_readonly(discount_path);
    const auto quantity_file = mmap_readonly(quantity_path);

    const uint64_t n_discount = discount_file.size / sizeof(double);
    const uint64_t n_quantity = quantity_file.size / sizeof(double);
    if (discount_file.size != n_discount * sizeof(double) ||
        quantity_file.size != n_quantity * sizeof(double) ||
        n_discount != n_quantity) {
        std::cerr << "Column size mismatch\n";
        return 1;
    }

    const auto* discount = static_cast<const double*>(discount_file.data);
    const auto* quantity = static_cast<const double*>(quantity_file.data);

    std::vector<uint16_t> discount_scaled(n_discount);
    std::vector<uint16_t> quantity_scaled(n_quantity);

    uint32_t discount_unique_flags[65536] = {0};
    uint32_t quantity_unique_flags[65536] = {0};
    uint64_t discount_unique = 0;
    uint64_t quantity_unique = 0;

    for (uint64_t i = 0; i < n_discount; ++i) {
        const long d = std::lround(discount[i] * 100.0);
        const long q = std::lround(quantity[i] * 100.0);
        if (d < 0 || d > 65535 || q < 0 || q > 65535) {
            std::cerr << "Scaled value out of uint16 range at row " << i << "\n";
            return 1;
        }

        const uint16_t ds = static_cast<uint16_t>(d);
        const uint16_t qs = static_cast<uint16_t>(q);
        discount_scaled[i] = ds;
        quantity_scaled[i] = qs;

        if (discount_unique_flags[ds] == 0) {
            discount_unique_flags[ds] = 1;
            ++discount_unique;
        }
        if (quantity_unique_flags[qs] == 0) {
            quantity_unique_flags[qs] = 1;
            ++quantity_unique;
        }
    }

    const std::string discount_dir = gendb_dir + "/column_versions/lineitem.l_discount.int16_scaled_by_100";
    const std::string quantity_dir = gendb_dir + "/column_versions/lineitem.l_quantity.int16_scaled_by_100";
    std::filesystem::create_directories(discount_dir);
    std::filesystem::create_directories(quantity_dir);

    write_binary(discount_dir + "/codes.bin", discount_scaled);
    write_binary(quantity_dir + "/codes.bin", quantity_scaled);

    std::cout << "row_count=" << n_discount
              << " discount_unique=" << discount_unique
              << " quantity_unique=" << quantity_unique << "\n";
    return 0;
}
