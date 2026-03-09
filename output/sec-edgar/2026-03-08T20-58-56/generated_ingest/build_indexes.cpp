#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

template <typename T>
static inline void write_pod(std::ofstream& out, T v) {
  out.write(reinterpret_cast<const char*>(&v), sizeof(T));
}

template <typename T>
static std::vector<T> read_vec(const fs::path& path, uint64_t n) {
  std::vector<T> v(n);
  std::ifstream in(path, std::ios::binary);
  in.read(reinterpret_cast<char*>(v.data()), static_cast<std::streamsize>(n * sizeof(T)));
  return v;
}

static uint64_t read_row_count(const fs::path& table_dir) {
  std::ifstream in(table_dir / "row_count.txt");
  uint64_t n = 0;
  in >> n;
  return n;
}

static void build_dense_pk_sub_adsh(const fs::path& gendb) {
  const fs::path tdir = gendb / "sub";
  const uint64_t n = read_row_count(tdir);
  auto adsh = read_vec<uint32_t>(tdir / "adsh.bin", n);
  uint32_t max_code = 0;
  for (uint32_t k : adsh) max_code = std::max(max_code, k);
  std::vector<uint32_t> lut(static_cast<size_t>(max_code) + 1, std::numeric_limits<uint32_t>::max());
  for (uint32_t i = 0; i < n; ++i) lut[adsh[i]] = i;

  fs::create_directories(tdir / "indexes");
  std::ofstream out(tdir / "indexes" / "sub_adsh_pk_hash.bin", std::ios::binary);
  write_pod(out, static_cast<uint64_t>(lut.size()));
  out.write(reinterpret_cast<const char*>(lut.data()), static_cast<std::streamsize>(lut.size() * sizeof(uint32_t)));
  std::cerr << "built sub_adsh_pk_hash" << "\n";
}

static void build_hash_tag_tag_version_pk(const fs::path& gendb) {
  const fs::path tdir = gendb / "tag";
  const uint64_t n = read_row_count(tdir);
  auto tag = read_vec<uint32_t>(tdir / "tag.bin", n);
  auto ver = read_vec<uint32_t>(tdir / "version.bin", n);

  std::unordered_map<uint64_t, uint32_t> m;
  m.reserve(static_cast<size_t>(n * 1.3));
  for (uint32_t i = 0; i < n; ++i) {
    uint64_t k = (static_cast<uint64_t>(tag[i]) << 32) | ver[i];
    m.emplace(k, i);
  }

  fs::create_directories(tdir / "indexes");
  std::ofstream out(tdir / "indexes" / "tag_tag_version_pk_hash.bin", std::ios::binary);
  write_pod(out, static_cast<uint64_t>(m.size()));
  for (const auto& kv : m) {
    write_pod(out, kv.first);
    write_pod(out, kv.second);
  }
  std::cerr << "built tag_tag_version_pk_hash" << "\n";
}

static void build_posting_u32(const fs::path& table_dir,
                              const std::string& col_file,
                              const std::string& out_file) {
  const uint64_t n = read_row_count(table_dir);
  auto key = read_vec<uint32_t>(table_dir / col_file, n);
  std::vector<std::pair<uint32_t, uint32_t>> p;
  p.reserve(n);
  for (uint32_t i = 0; i < n; ++i) p.emplace_back(key[i], i);
  std::sort(p.begin(), p.end(), [](const auto& a, const auto& b) {
    if (a.first != b.first) return a.first < b.first;
    return a.second < b.second;
  });

  std::vector<uint32_t> rowids;
  rowids.reserve(n);
  struct Entry { uint32_t key; uint64_t start; uint32_t count; };
  std::vector<Entry> entries;
  entries.reserve(n / 8 + 1);

  uint64_t i = 0;
  while (i < p.size()) {
    uint32_t k = p[i].first;
    uint64_t start = rowids.size();
    uint32_t cnt = 0;
    while (i < p.size() && p[i].first == k) {
      rowids.push_back(p[i].second);
      ++i;
      ++cnt;
    }
    entries.push_back({k, start, cnt});
  }

  fs::create_directories(table_dir / "indexes");
  std::ofstream out(table_dir / "indexes" / out_file, std::ios::binary);
  write_pod(out, static_cast<uint64_t>(entries.size()));
  write_pod(out, static_cast<uint64_t>(rowids.size()));
  for (const auto& e : entries) {
    write_pod(out, e.key);
    write_pod(out, e.start);
    write_pod(out, e.count);
  }
  out.write(reinterpret_cast<const char*>(rowids.data()), static_cast<std::streamsize>(rowids.size() * sizeof(uint32_t)));
  std::cerr << "built " << out_file << "\n";
}

struct TripleKey {
  uint32_t a;
  uint32_t b;
  uint32_t c;
};

static void build_posting_triple(const fs::path& table_dir,
                                 const std::string& a_file,
                                 const std::string& b_file,
                                 const std::string& c_file,
                                 const std::string& out_file) {
  const uint64_t n = read_row_count(table_dir);
  auto a = read_vec<uint32_t>(table_dir / a_file, n);
  auto b = read_vec<uint32_t>(table_dir / b_file, n);
  auto c = read_vec<uint32_t>(table_dir / c_file, n);

  std::vector<std::pair<TripleKey, uint32_t>> p;
  p.reserve(n);
  for (uint32_t i = 0; i < n; ++i) p.push_back({{a[i], b[i], c[i]}, i});

  std::sort(p.begin(), p.end(), [](const auto& x, const auto& y) {
    if (x.first.a != y.first.a) return x.first.a < y.first.a;
    if (x.first.b != y.first.b) return x.first.b < y.first.b;
    if (x.first.c != y.first.c) return x.first.c < y.first.c;
    return x.second < y.second;
  });

  std::vector<uint32_t> rowids;
  rowids.reserve(n);
  struct Entry { TripleKey k; uint64_t start; uint32_t count; };
  std::vector<Entry> entries;
  entries.reserve(n / 4 + 1);

  uint64_t i = 0;
  while (i < p.size()) {
    TripleKey k = p[i].first;
    uint64_t start = rowids.size();
    uint32_t cnt = 0;
    while (i < p.size() &&
           p[i].first.a == k.a && p[i].first.b == k.b && p[i].first.c == k.c) {
      rowids.push_back(p[i].second);
      ++i;
      ++cnt;
    }
    entries.push_back({k, start, cnt});
  }

  fs::create_directories(table_dir / "indexes");
  std::ofstream out(table_dir / "indexes" / out_file, std::ios::binary);
  write_pod(out, static_cast<uint64_t>(entries.size()));
  write_pod(out, static_cast<uint64_t>(rowids.size()));
  for (const auto& e : entries) {
    write_pod(out, e.k.a);
    write_pod(out, e.k.b);
    write_pod(out, e.k.c);
    write_pod(out, e.start);
    write_pod(out, e.count);
  }
  out.write(reinterpret_cast<const char*>(rowids.data()), static_cast<std::streamsize>(rowids.size() * sizeof(uint32_t)));
  std::cerr << "built " << out_file << "\n";
}

template <typename T>
static void build_zonemap_t(const fs::path& table_dir,
                            const std::string& col_file,
                            const std::string& out_file,
                            uint64_t block_size) {
  const uint64_t n = read_row_count(table_dir);
  auto v = read_vec<T>(table_dir / col_file, n);
  const uint64_t blocks = (n + block_size - 1) / block_size;

  fs::create_directories(table_dir / "indexes");
  std::ofstream out(table_dir / "indexes" / out_file, std::ios::binary);
  write_pod(out, block_size);
  write_pod(out, blocks);

  for (uint64_t b = 0; b < blocks; ++b) {
    uint64_t start = b * block_size;
    uint64_t end = std::min<uint64_t>(n, start + block_size);
    T mn = std::numeric_limits<T>::max();
    T mx = std::numeric_limits<T>::lowest();
    for (uint64_t i = start; i < end; ++i) {
      mn = std::min(mn, v[i]);
      mx = std::max(mx, v[i]);
    }
    write_pod(out, mn);
    write_pod(out, mx);
  }
  std::cerr << "built " << out_file << "\n";
}

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: build_indexes <gendb_dir>\n";
    return 1;
  }
  fs::path gendb = argv[1];
  const uint64_t block = 100000;

  std::vector<std::future<void>> jobs;
  jobs.push_back(std::async(std::launch::async, [&]() { build_dense_pk_sub_adsh(gendb); }));
  jobs.push_back(std::async(std::launch::async, [&]() { build_hash_tag_tag_version_pk(gendb); }));
  jobs.push_back(std::async(std::launch::async, [&]() { build_posting_u32(gendb / "num", "adsh.bin", "num_adsh_fk_hash.bin"); }));
  jobs.push_back(std::async(std::launch::async, [&]() { build_posting_u32(gendb / "pre", "adsh.bin", "pre_adsh_fk_hash.bin"); }));
  jobs.push_back(std::async(std::launch::async, [&]() {
    build_posting_triple(gendb / "num", "adsh.bin", "tag.bin", "version.bin", "num_adsh_tag_version_hash.bin");
  }));
  jobs.push_back(std::async(std::launch::async, [&]() {
    build_posting_triple(gendb / "pre", "adsh.bin", "tag.bin", "version.bin", "pre_adsh_tag_version_hash.bin");
  }));
  jobs.push_back(std::async(std::launch::async, [&]() {
    build_posting_triple(gendb / "num", "tag.bin", "version.bin", "uom.bin", "num_tag_version_uom_hash.bin");
  }));
  jobs.push_back(std::async(std::launch::async, [&]() { build_posting_u32(gendb / "pre", "stmt.bin", "pre_stmt_hash.bin"); }));
  jobs.push_back(std::async(std::launch::async, [&]() { build_posting_u32(gendb / "num", "uom.bin", "num_uom_hash.bin"); }));

  jobs.push_back(std::async(std::launch::async, [&]() { build_zonemap_t<int32_t>(gendb / "num", "ddate.bin", "num_ddate_zonemap.bin", block); }));
  jobs.push_back(std::async(std::launch::async, [&]() { build_zonemap_t<int32_t>(gendb / "sub", "fy.bin", "sub_fy_zonemap.bin", block); }));
  jobs.push_back(std::async(std::launch::async, [&]() { build_zonemap_t<int32_t>(gendb / "sub", "sic.bin", "sub_sic_zonemap.bin", block); }));
  jobs.push_back(std::async(std::launch::async, [&]() { build_zonemap_t<uint8_t>(gendb / "pre", "stmt.bin", "pre_stmt_zonemap.bin", block); }));
  jobs.push_back(std::async(std::launch::async, [&]() { build_zonemap_t<int8_t>(gendb / "tag", "abstract.bin", "tag_abstract_zonemap.bin", block); }));

  for (auto& j : jobs) j.get();
  return 0;
}
