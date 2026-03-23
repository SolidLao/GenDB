#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

struct TripleEntry {
    uint32_t adsh;
    uint32_t tag;
    uint32_t version;
    uint64_t start;
    uint32_t count;
};

struct AdshRange {
    uint32_t adsh;
    uint64_t lo;
    uint64_t hi;
};

struct TripleIndexView {
    const uint8_t* entries = nullptr;
    uint64_t entry_count = 0;
    std::vector<uint8_t> bytes;
};

static TripleEntry read_entry(const uint8_t* base, uint64_t idx) {
    const uint8_t* p = base + idx * 24;
    TripleEntry e{};
    std::memcpy(&e.adsh, p + 0, sizeof(uint32_t));
    std::memcpy(&e.tag, p + 4, sizeof(uint32_t));
    std::memcpy(&e.version, p + 8, sizeof(uint32_t));
    std::memcpy(&e.start, p + 12, sizeof(uint64_t));
    std::memcpy(&e.count, p + 20, sizeof(uint32_t));
    return e;
}

static TripleIndexView load_triple_index(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open: " + p.string());
    in.seekg(0, std::ios::end);
    const std::streamoff sz = in.tellg();
    in.seekg(0, std::ios::beg);
    if (sz < 16) throw std::runtime_error("index too small: " + p.string());

    TripleIndexView v;
    v.bytes.resize(static_cast<size_t>(sz));
    in.read(reinterpret_cast<char*>(v.bytes.data()), sz);
    if (!in) throw std::runtime_error("read failed: " + p.string());

    uint64_t entry_count = 0;
    uint64_t rowid_count = 0;
    std::memcpy(&entry_count, v.bytes.data(), sizeof(uint64_t));
    std::memcpy(&rowid_count, v.bytes.data() + 8, sizeof(uint64_t));
    const uint64_t need = 16ULL + entry_count * 24ULL + rowid_count * 4ULL;
    if (static_cast<uint64_t>(sz) < need) throw std::runtime_error("truncated: " + p.string());

    v.entry_count = entry_count;
    v.entries = v.bytes.data() + 16;
    return v;
}

static std::vector<AdshRange> build_ranges(const TripleIndexView& idx) {
    std::vector<AdshRange> out;
    out.reserve(static_cast<size_t>(idx.entry_count / 4));

    uint64_t i = 0;
    while (i < idx.entry_count) {
      const TripleEntry e = read_entry(idx.entries, i);
      const uint32_t adsh = e.adsh;
      const uint64_t lo = i;
      ++i;
      while (i < idx.entry_count) {
        const TripleEntry x = read_entry(idx.entries, i);
        if (x.adsh != adsh) break;
        ++i;
      }
      out.push_back(AdshRange{adsh, lo, i});
    }
    return out;
}

static void write_ranges(const fs::path& out_file, const std::vector<AdshRange>& ranges) {
    fs::create_directories(out_file.parent_path());
    std::ofstream out(out_file, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("cannot write: " + out_file.string());

    const uint64_t n = static_cast<uint64_t>(ranges.size());
    out.write(reinterpret_cast<const char*>(&n), sizeof(uint64_t));
    out.write(reinterpret_cast<const char*>(ranges.data()), static_cast<std::streamsize>(n * sizeof(AdshRange)));
    if (!out) throw std::runtime_error("write failed: " + out_file.string());
}

int main(int argc, char** argv) {
    if (argc != 2) {
      std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
      return 1;
    }

    try {
      const fs::path gendb = argv[1];
      const auto t0 = std::chrono::steady_clock::now();

      const auto pre_idx = load_triple_index(gendb / "pre/indexes/pre_adsh_tag_version_hash.bin");
      const auto num_idx = load_triple_index(gendb / "num/indexes/num_adsh_tag_version_hash.bin");

      const auto pre_ranges = build_ranges(pre_idx);
      const auto num_ranges = build_ranges(num_idx);

      write_ranges(gendb / "column_versions/pre.adsh.triple_ranges/ranges.bin", pre_ranges);
      write_ranges(gendb / "column_versions/num.adsh.triple_ranges/ranges.bin", num_ranges);

      const auto t1 = std::chrono::steady_clock::now();
      const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

      std::cout << "pre_ranges=" << pre_ranges.size() << "\n";
      std::cout << "num_ranges=" << num_ranges.size() << "\n";
      std::cout << "build_time_ms=" << ms << "\n";
      return 0;
    } catch (const std::exception& e) {
      std::cerr << "Error: " << e.what() << "\n";
      return 1;
    }
}
