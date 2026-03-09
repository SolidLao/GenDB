#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

bool contains_green(const char* s, size_t len) {
    if (len < 5) return false;
    for (size_t i = 0; i + 5 <= len; ++i) {
        if (s[i] == 'g' && s[i + 1] == 'r' && s[i + 2] == 'e' && s[i + 3] == 'e' && s[i + 4] == 'n') {
            return true;
        }
    }
    return false;
}

template <typename T>
std::vector<T> read_vec(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("failed to open " + path);
    in.seekg(0, std::ios::end);
    const std::streamoff sz = in.tellg();
    in.seekg(0, std::ios::beg);
    if (sz < 0 || (sz % static_cast<std::streamoff>(sizeof(T))) != 0) {
        throw std::runtime_error("invalid file size for " + path);
    }
    std::vector<T> out(static_cast<size_t>(sz / static_cast<std::streamoff>(sizeof(T))));
    in.read(reinterpret_cast<char*>(out.data()), sz);
    if (!in) throw std::runtime_error("failed to read " + path);
    return out;
}

std::vector<uint8_t> read_bytes(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("failed to open " + path);
    in.seekg(0, std::ios::end);
    const std::streamoff sz = in.tellg();
    in.seekg(0, std::ios::beg);
    if (sz < 0) throw std::runtime_error("invalid file size for " + path);
    std::vector<uint8_t> out(static_cast<size_t>(sz));
    in.read(reinterpret_cast<char*>(out.data()), sz);
    if (!in) throw std::runtime_error("failed to read " + path);
    return out;
}

void write_bytes(const std::string& path, const std::vector<uint8_t>& data) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("failed to open output " + path);
    out.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
    if (!out) throw std::runtime_error("failed to write output " + path);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    const auto t0 = std::chrono::steady_clock::now();

    const std::string gendb_dir = argv[1];
    const std::string p_partkey_path = gendb_dir + "/part/p_partkey.bin";
    const std::string p_name_off_path = gendb_dir + "/part/p_name.off";
    const std::string p_name_dat_path = gendb_dir + "/part/p_name.dat";

    auto partkey = read_vec<int32_t>(p_partkey_path);
    auto off = read_vec<uint64_t>(p_name_off_path);
    auto dat = read_bytes(p_name_dat_path);

    if (off.size() != partkey.size() + 1) {
        throw std::runtime_error("p_name.off must have row_count+1 entries");
    }

    int32_t max_partkey = 0;
    for (int32_t pk : partkey) max_partkey = std::max(max_partkey, pk);
    if (max_partkey <= 0) throw std::runtime_error("invalid max_partkey");

    std::vector<uint8_t> has_green(static_cast<size_t>(max_partkey) + 1, 0);
    uint64_t true_count = 0;

    for (size_t i = 0; i < partkey.size(); ++i) {
        const int32_t pk = partkey[i];
        if (pk <= 0) continue;

        const uint64_t b = off[i];
        const uint64_t e = off[i + 1];
        if (e < b || e > dat.size()) {
            throw std::runtime_error("offset out of range at row " + std::to_string(i));
        }

        if (contains_green(reinterpret_cast<const char*>(dat.data() + b), static_cast<size_t>(e - b))) {
            if (!has_green[static_cast<size_t>(pk)]) {
                has_green[static_cast<size_t>(pk)] = 1;
                ++true_count;
            }
        }
    }

    const std::string id = "part.p_name.has_green_by_partkey";
    const std::string out_dir = gendb_dir + "/column_versions/" + id;
    std::filesystem::create_directories(out_dir);
    write_bytes(out_dir + "/codes.bin", has_green);

    const auto t1 = std::chrono::steady_clock::now();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    std::cout << "row_count=" << partkey.size() << "\n";
    std::cout << "max_partkey=" << max_partkey << "\n";
    std::cout << "true_values=" << true_count << "\n";
    std::cout << "build_time_ms=" << ms << "\n";
    return 0;
}
