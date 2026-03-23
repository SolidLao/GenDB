#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mmap_utils.h"

using namespace gendb;

namespace {

template <typename T>
void write_binary(const std::filesystem::path& path, const std::vector<T>& values) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("failed to open output file: " + path.string());
    }
    if (!values.empty()) {
        out.write(reinterpret_cast<const char*>(values.data()),
                  static_cast<std::streamsize>(values.size() * sizeof(T)));
    }
    if (!out) {
        throw std::runtime_error("failed to write output file: " + path.string());
    }
}

void write_binary(const std::filesystem::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("failed to open output file: " + path.string());
    }
    if (!bytes.empty()) {
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    if (!out) {
        throw std::runtime_error("failed to write output file: " + path.string());
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    try {
        const auto start = std::chrono::steady_clock::now();
        const std::filesystem::path gendb_dir = argv[1];

        MmapColumn<int32_t> p_partkey;
        MmapColumn<uint64_t> p_name_offsets;
        MmapColumn<char> p_name_data;

        p_partkey.open((gendb_dir / "part/p_partkey.bin").string());
        p_name_offsets.open((gendb_dir / "part/p_name.offsets.bin").string());
        p_name_data.open((gendb_dir / "part/p_name.data.bin").string());

        if (p_name_offsets.size() != p_partkey.size() + 1) {
            throw std::runtime_error("row count mismatch for part/p_name");
        }
        if (p_partkey.size() == 0) {
            throw std::runtime_error("part table is empty");
        }

        int32_t max_partkey = 0;
        for (size_t row = 0; row < p_partkey.size(); ++row) {
            max_partkey = std::max(max_partkey, p_partkey[row]);
        }
        if (max_partkey < 0) {
            throw std::runtime_error("invalid max partkey");
        }

        const size_t words_per_bitmap = (static_cast<size_t>(max_partkey) + 64) / 64;
        std::unordered_map<std::string, uint32_t> token_to_id;
        std::vector<std::string> tokens;
        std::vector<std::vector<uint64_t>> bitmaps;

        for (size_t row = 0; row < p_partkey.size(); ++row) {
            const int32_t partkey = p_partkey[row];
            if (partkey < 0) {
                continue;
            }
            const uint64_t begin = p_name_offsets[row];
            const uint64_t end = p_name_offsets[row + 1];
            const char* bytes = p_name_data.data + begin;

            uint64_t pos = 0;
            while (pos < end - begin) {
                while (pos < end - begin &&
                       !std::isalnum(static_cast<unsigned char>(bytes[pos]))) {
                    ++pos;
                }
                const uint64_t token_begin = pos;
                while (pos < end - begin &&
                       std::isalnum(static_cast<unsigned char>(bytes[pos]))) {
                    ++pos;
                }
                if (token_begin == pos) {
                    continue;
                }

                std::string token(bytes + token_begin, bytes + pos);
                auto [it, inserted] = token_to_id.emplace(token, static_cast<uint32_t>(tokens.size()));
                if (inserted) {
                    tokens.push_back(token);
                    bitmaps.emplace_back(words_per_bitmap, 0);
                }
                const size_t bitmap_id = static_cast<size_t>(it->second);
                const size_t key = static_cast<size_t>(partkey);
                bitmaps[bitmap_id][key >> 6] |= (uint64_t{1} << (key & 63));
            }
        }

        std::vector<uint32_t> order(tokens.size(), 0);
        for (uint32_t i = 0; i < order.size(); ++i) {
            order[i] = i;
        }
        std::sort(order.begin(), order.end(), [&](uint32_t lhs, uint32_t rhs) {
            return tokens[static_cast<size_t>(lhs)] < tokens[static_cast<size_t>(rhs)];
        });

        std::vector<uint64_t> token_offsets;
        token_offsets.reserve(tokens.size() + 1);
        token_offsets.push_back(0);
        std::string token_data;
        std::vector<uint64_t> bitmap_words;
        bitmap_words.reserve(order.size() * words_per_bitmap);

        for (uint32_t bitmap_id : order) {
            const std::string& token = tokens[static_cast<size_t>(bitmap_id)];
            token_data.append(token);
            token_offsets.push_back(static_cast<uint64_t>(token_data.size()));

            const auto& bitmap = bitmaps[static_cast<size_t>(bitmap_id)];
            bitmap_words.insert(bitmap_words.end(), bitmap.begin(), bitmap.end());
        }

        const std::filesystem::path out_dir = gendb_dir / "column_versions/part.p_name.token_bitmap";
        std::filesystem::create_directories(out_dir);
        write_binary(out_dir / "token_offsets.bin", token_offsets);
        write_binary(out_dir / "token_data.bin", token_data);
        write_binary(out_dir / "bitmaps.bin", bitmap_words);

        const auto end = std::chrono::steady_clock::now();
        const auto build_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        std::cout << "row_count=" << p_partkey.size()
                  << " unique_tokens=" << tokens.size()
                  << " max_partkey=" << max_partkey
                  << " words_per_bitmap=" << words_per_bitmap
                  << " build_time_ms=" << build_ms << "\n";
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << "\n";
        return 1;
    }
}
