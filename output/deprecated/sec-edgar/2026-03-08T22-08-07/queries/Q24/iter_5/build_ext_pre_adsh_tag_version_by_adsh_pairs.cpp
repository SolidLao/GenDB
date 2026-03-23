#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

struct TripleKey {
    uint32_t a;
    uint32_t b;
    uint32_t c;
};

struct PairKey {
    uint32_t tag;
    uint32_t version;
};

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const fs::path input_path = fs::path(gendb_dir) /
                                "column_versions/pre.adsh_tag_version.partitioned_exact_p19/keys.bin";
    const fs::path out_dir = fs::path(gendb_dir) /
                             "column_versions/pre.adsh_tag_version.by_adsh_pairs";
    const fs::path offsets_path = out_dir / "offsets.bin";
    const fs::path pairs_path = out_dir / "pairs.bin";

    std::ifstream in(input_path, std::ios::binary | std::ios::ate);
    if (!in) {
        throw std::runtime_error("cannot open input keys.bin");
    }
    const std::streamsize bytes = in.tellg();
    if (bytes < 0 || bytes % static_cast<std::streamsize>(sizeof(TripleKey)) != 0) {
        throw std::runtime_error("unexpected keys.bin size");
    }
    in.seekg(0);

    const size_t row_count = static_cast<size_t>(bytes / static_cast<std::streamsize>(sizeof(TripleKey)));
    std::vector<TripleKey> triples(row_count);
    if (!in.read(reinterpret_cast<char*>(triples.data()), bytes)) {
        throw std::runtime_error("failed reading keys.bin");
    }

    uint32_t max_adsh = 0;
    for (const TripleKey& key : triples) {
        max_adsh = std::max(max_adsh, key.a);
    }

    std::vector<uint64_t> offsets(static_cast<size_t>(max_adsh) + 2, 0);
    for (const TripleKey& key : triples) {
        offsets[static_cast<size_t>(key.a) + 1] += 1;
    }
    for (size_t i = 1; i < offsets.size(); ++i) {
        offsets[i] += offsets[i - 1];
    }

    std::vector<PairKey> pairs(row_count);
    std::vector<uint64_t> cursor = offsets;
    for (const TripleKey& key : triples) {
        const uint64_t pos = cursor[static_cast<size_t>(key.a)]++;
        pairs[static_cast<size_t>(pos)] = PairKey{key.b, key.c};
    }

    for (size_t adsh = 0; adsh + 1 < offsets.size(); ++adsh) {
        const uint64_t begin = offsets[adsh];
        const uint64_t end = offsets[adsh + 1];
        std::sort(pairs.begin() + static_cast<std::ptrdiff_t>(begin),
                  pairs.begin() + static_cast<std::ptrdiff_t>(end),
                  [](const PairKey& lhs, const PairKey& rhs) {
                      if (lhs.tag != rhs.tag) {
                          return lhs.tag < rhs.tag;
                      }
                      return lhs.version < rhs.version;
                  });
    }

    fs::create_directories(out_dir);
    {
        std::ofstream out(offsets_path, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("cannot open offsets output");
        }
        out.write(reinterpret_cast<const char*>(offsets.data()),
                  static_cast<std::streamsize>(offsets.size() * sizeof(uint64_t)));
    }
    {
        std::ofstream out(pairs_path, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("cannot open pairs output");
        }
        out.write(reinterpret_cast<const char*>(pairs.data()),
                  static_cast<std::streamsize>(pairs.size() * sizeof(PairKey)));
    }

    size_t unique_adsh = 0;
    for (size_t i = 0; i + 1 < offsets.size(); ++i) {
        if (offsets[i] != offsets[i + 1]) {
            unique_adsh += 1;
        }
    }

    std::cout << "row_count=" << row_count
              << " unique_adsh=" << unique_adsh
              << " max_adsh=" << max_adsh << '\n';
    return 0;
}
