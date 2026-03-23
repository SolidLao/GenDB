#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::filesystem::path src = std::filesystem::path(gendb_dir) / "num" / "ddate.bin";
    const std::filesystem::path out_dir = std::filesystem::path(gendb_dir) / "column_versions" / "num.ddate.zm4096";
    const std::filesystem::path out_file = out_dir / "num_ddate_zonemap_4096.bin";

    std::ifstream in(src, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open source ddate.bin");

    in.seekg(0, std::ios::end);
    const std::streamoff bytes = in.tellg();
    in.seekg(0, std::ios::beg);
    if (bytes < 0 || (bytes % static_cast<std::streamoff>(sizeof(int32_t))) != 0) {
        throw std::runtime_error("invalid ddate.bin size");
    }

    const uint64_t row_count = static_cast<uint64_t>(bytes / static_cast<std::streamoff>(sizeof(int32_t)));
    std::vector<int32_t> ddate(static_cast<size_t>(row_count));
    if (row_count > 0) {
        in.read(reinterpret_cast<char*>(ddate.data()), bytes);
        if (!in) throw std::runtime_error("failed reading ddate.bin");
    }

    constexpr uint64_t block_size = 4096;
    const uint64_t blocks = (row_count + block_size - 1) / block_size;

    std::filesystem::create_directories(out_dir);
    std::ofstream out(out_file, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("cannot open output zonemap file");

    out.write(reinterpret_cast<const char*>(&block_size), sizeof(block_size));
    out.write(reinterpret_cast<const char*>(&blocks), sizeof(blocks));

    for (uint64_t b = 0; b < blocks; ++b) {
        const uint64_t lo = b * block_size;
        const uint64_t hi = std::min<uint64_t>(lo + block_size, row_count);

        int32_t mn = ddate[static_cast<size_t>(lo)];
        int32_t mx = mn;
        for (uint64_t i = lo + 1; i < hi; ++i) {
            const int32_t v = ddate[static_cast<size_t>(i)];
            if (v < mn) mn = v;
            if (v > mx) mx = v;
        }

        out.write(reinterpret_cast<const char*>(&mn), sizeof(mn));
        out.write(reinterpret_cast<const char*>(&mx), sizeof(mx));
    }

    if (!out) throw std::runtime_error("failed writing zonemap output");

    std::cout << "row_count=" << row_count << "\n";
    std::cout << "blocks=" << blocks << "\n";
    std::cout << "output=" << out_file.string() << "\n";
    return 0;
}
