#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

uint64_t file_size_or_throw(const std::string& path) {
    std::error_code ec;
    const auto sz = std::filesystem::file_size(path, ec);
    if (ec) throw std::runtime_error("file_size failed: " + path + " (" + ec.message() + ")");
    return sz;
}

void read_file_exact(const std::string& path, void* dst, size_t bytes) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open: " + path);
    in.read(reinterpret_cast<char*>(dst), static_cast<std::streamsize>(bytes));
    if (!in) throw std::runtime_error("Failed to read bytes from: " + path);
}

void write_file_exact(const std::string& path, const void* src, size_t bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("Failed to open for write: " + path);
    out.write(reinterpret_cast<const char*>(src), static_cast<std::streamsize>(bytes));
    if (!out) throw std::runtime_error("Failed to write bytes to: " + path);
}

}  // namespace

int main(int argc, char** argv) {
    using clock = std::chrono::steady_clock;
    const auto t0 = clock::now();

    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    try {
        const std::string gendb_dir = argv[1];
        const std::string li_dir = gendb_dir + "/lineitem/";
        const std::string rf_path = li_dir + "l_returnflag.bin";
        const std::string ls_path = li_dir + "l_linestatus.bin";

        const uint64_t rf_bytes = file_size_or_throw(rf_path);
        const uint64_t ls_bytes = file_size_or_throw(ls_path);
        if (rf_bytes % sizeof(uint32_t) != 0 || ls_bytes % sizeof(uint32_t) != 0) {
            throw std::runtime_error("Input file sizes are not uint32_t-aligned");
        }
        const uint64_t n = rf_bytes / sizeof(uint32_t);
        if (ls_bytes / sizeof(uint32_t) != n) {
            throw std::runtime_error("Row count mismatch between returnflag and linestatus");
        }
        if (n > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
            throw std::runtime_error("Row count exceeds uint32_t range");
        }

        std::vector<uint32_t> rf(static_cast<size_t>(n));
        std::vector<uint32_t> ls(static_cast<size_t>(n));
        read_file_exact(rf_path, rf.data(), static_cast<size_t>(rf_bytes));
        read_file_exact(ls_path, ls.data(), static_cast<size_t>(ls_bytes));

        std::vector<uint8_t> codes(static_cast<size_t>(n));
        std::unordered_map<uint64_t, uint8_t> key_to_code;
        key_to_code.reserve(32);
        std::vector<std::pair<uint32_t, uint32_t>> code_to_pair;
        code_to_pair.reserve(32);

        for (uint64_t i = 0; i < n; ++i) {
            const uint64_t key = (static_cast<uint64_t>(rf[static_cast<size_t>(i)]) << 32) |
                                 static_cast<uint64_t>(ls[static_cast<size_t>(i)]);
            auto it = key_to_code.find(key);
            if (it == key_to_code.end()) {
                if (code_to_pair.size() >= 255) {
                    throw std::runtime_error("Too many unique (returnflag,linestatus) pairs for uint8_t");
                }
                const uint8_t code = static_cast<uint8_t>(code_to_pair.size());
                key_to_code.emplace(key, code);
                code_to_pair.emplace_back(rf[static_cast<size_t>(i)], ls[static_cast<size_t>(i)]);
                codes[static_cast<size_t>(i)] = code;
            } else {
                codes[static_cast<size_t>(i)] = it->second;
            }
        }

        const std::string out_dir = gendb_dir + "/column_versions/lineitem.rf_ls.combo_u8";
        std::filesystem::create_directories(out_dir);
        write_file_exact(out_dir + "/codes.bin", codes.data(), codes.size() * sizeof(uint8_t));

        const uint32_t k = static_cast<uint32_t>(code_to_pair.size());
        std::vector<uint32_t> keymap;
        keymap.reserve(1 + static_cast<size_t>(k) * 2);
        keymap.push_back(k);
        for (const auto& p : code_to_pair) {
            keymap.push_back(p.first);
            keymap.push_back(p.second);
        }
        write_file_exact(out_dir + "/keymap.bin", keymap.data(), keymap.size() * sizeof(uint32_t));

        const auto t1 = clock::now();
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
        std::cout << "row_count=" << n << " unique_pairs=" << k << " build_time_ms=" << ms << "\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 2;
    }
}
