#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

struct SharedDict {
  std::mutex mu;
  std::unordered_map<std::string, uint32_t> codes;
  std::vector<std::string> values;

  uint32_t encode(const std::string& s) {
    std::lock_guard<std::mutex> lock(mu);
    auto it = codes.find(s);
    if (it != codes.end()) return it->second;
    uint32_t id = static_cast<uint32_t>(values.size());
    values.push_back(s);
    codes.emplace(values.back(), id);
    return id;
  }
};

struct LocalDict {
  std::unordered_map<std::string, uint32_t> codes;
  std::vector<std::string> values;

  uint32_t encode(const std::string& s) {
    auto it = codes.find(s);
    if (it != codes.end()) return it->second;
    uint32_t id = static_cast<uint32_t>(values.size());
    values.push_back(s);
    codes.emplace(values.back(), id);
    return id;
  }
};

static std::vector<std::string> parse_csv_line(const std::string& line) {
  std::vector<std::string> out;
  out.reserve(24);
  std::string cur;
  bool in_quotes = false;
  for (size_t i = 0; i < line.size(); ++i) {
    char c = line[i];
    if (c == '"') {
      if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
        cur.push_back('"');
        ++i;
      } else {
        in_quotes = !in_quotes;
      }
    } else if (c == ',' && !in_quotes) {
      out.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  out.push_back(cur);
  return out;
}

template <typename T>
static inline void write_pod(std::ofstream& out, T v) {
  out.write(reinterpret_cast<const char*>(&v), sizeof(T));
}

static int32_t to_i32(const std::string& s) {
  if (s.empty()) return std::numeric_limits<int32_t>::min();
  return static_cast<int32_t>(std::stol(s));
}

static int16_t to_i16(const std::string& s) {
  if (s.empty()) return std::numeric_limits<int16_t>::min();
  return static_cast<int16_t>(std::stoi(s));
}

static int8_t to_i8(const std::string& s) {
  if (s.empty()) return std::numeric_limits<int8_t>::min();
  return static_cast<int8_t>(std::stoi(s));
}

static double to_f64(const std::string& s) {
  if (s.empty()) return std::numeric_limits<double>::quiet_NaN();
  return std::stod(s);
}

static void write_row_count(const fs::path& table_dir, uint64_t rows) {
  std::ofstream rc(table_dir / "row_count.txt");
  rc << rows << "\n";
}

static void write_dict_file(const fs::path& path, const std::vector<std::string>& values) {
  std::ofstream out(path, std::ios::binary);
  uint32_t n = static_cast<uint32_t>(values.size());
  write_pod(out, n);
  for (const auto& s : values) {
    uint32_t len = static_cast<uint32_t>(s.size());
    write_pod(out, len);
    if (!s.empty()) out.write(s.data(), s.size());
  }
}

static void ingest_sub(const fs::path& src_dir, const fs::path& out_dir,
                       SharedDict& adsh_dict) {
  fs::path in_path = src_dir / "sub.csv";
  fs::path table_dir = out_dir / "sub";
  fs::create_directories(table_dir);

  std::ifstream in(in_path);
  std::string line;
  std::getline(in, line);  // header

  std::ofstream adsh_out(table_dir / "adsh.bin", std::ios::binary);
  std::ofstream cik_out(table_dir / "cik.bin", std::ios::binary);
  std::ofstream name_out(table_dir / "name.bin", std::ios::binary);
  std::ofstream sic_out(table_dir / "sic.bin", std::ios::binary);
  std::ofstream fy_out(table_dir / "fy.bin", std::ios::binary);

  LocalDict name_dict;
  uint64_t rows = 0;
  while (std::getline(in, line)) {
    if (line.empty()) continue;
    auto cols = parse_csv_line(line);
    if (cols.size() < 20) continue;

    uint32_t adsh = adsh_dict.encode(cols[0]);
    int32_t cik = to_i32(cols[1]);
    uint32_t name = name_dict.encode(cols[2]);
    int32_t sic = to_i32(cols[3]);
    int32_t fy = to_i32(cols[10]);

    write_pod(adsh_out, adsh);
    write_pod(cik_out, cik);
    write_pod(name_out, name);
    write_pod(sic_out, sic);
    write_pod(fy_out, fy);
    ++rows;
  }

  write_row_count(table_dir, rows);
  write_dict_file(table_dir / "name.dict", name_dict.values);
  std::cerr << "ingested sub rows=" << rows << "\n";
}

static void ingest_num(const fs::path& src_dir, const fs::path& out_dir,
                       SharedDict& adsh_dict,
                       SharedDict& tag_dict,
                       SharedDict& version_dict,
                       SharedDict& uom_dict) {
  fs::path in_path = src_dir / "num.csv";
  fs::path table_dir = out_dir / "num";
  fs::create_directories(table_dir);

  std::ifstream in(in_path);
  std::string line;
  std::getline(in, line);  // header

  std::ofstream adsh_out(table_dir / "adsh.bin", std::ios::binary);
  std::ofstream tag_out(table_dir / "tag.bin", std::ios::binary);
  std::ofstream version_out(table_dir / "version.bin", std::ios::binary);
  std::ofstream ddate_out(table_dir / "ddate.bin", std::ios::binary);
  std::ofstream qtrs_out(table_dir / "qtrs.bin", std::ios::binary);
  std::ofstream uom_out(table_dir / "uom.bin", std::ios::binary);
  std::ofstream value_out(table_dir / "value.bin", std::ios::binary);

  uint64_t rows = 0;
  while (std::getline(in, line)) {
    if (line.empty()) continue;
    auto cols = parse_csv_line(line);
    if (cols.size() < 9) continue;

    uint32_t adsh = adsh_dict.encode(cols[0]);
    uint32_t tag = tag_dict.encode(cols[1]);
    uint32_t version = version_dict.encode(cols[2]);
    int32_t ddate = to_i32(cols[3]);
    int8_t qtrs = to_i8(cols[4]);
    uint32_t uom_code = uom_dict.encode(cols[5]);
    if (uom_code > std::numeric_limits<uint16_t>::max()) {
      std::cerr << "uom dictionary exceeded uint16_t\n";
      std::exit(1);
    }
    uint16_t uom = static_cast<uint16_t>(uom_code);
    double value = to_f64(cols[7]);

    write_pod(adsh_out, adsh);
    write_pod(tag_out, tag);
    write_pod(version_out, version);
    write_pod(ddate_out, ddate);
    write_pod(qtrs_out, qtrs);
    write_pod(uom_out, uom);
    write_pod(value_out, value);
    ++rows;
  }

  write_row_count(table_dir, rows);
  std::cerr << "ingested num rows=" << rows << "\n";
}

static void ingest_tag(const fs::path& src_dir, const fs::path& out_dir,
                       SharedDict& tag_dict,
                       SharedDict& version_dict) {
  fs::path in_path = src_dir / "tag.csv";
  fs::path table_dir = out_dir / "tag";
  fs::create_directories(table_dir);

  std::ifstream in(in_path);
  std::string line;
  std::getline(in, line);  // header

  std::ofstream tag_out(table_dir / "tag.bin", std::ios::binary);
  std::ofstream version_out(table_dir / "version.bin", std::ios::binary);
  std::ofstream abstract_out(table_dir / "abstract.bin", std::ios::binary);
  std::ofstream tlabel_out(table_dir / "tlabel.bin", std::ios::binary);

  LocalDict tlabel_dict;
  uint64_t rows = 0;
  while (std::getline(in, line)) {
    if (line.empty()) continue;
    auto cols = parse_csv_line(line);
    if (cols.size() < 9) continue;

    uint32_t tag = tag_dict.encode(cols[0]);
    uint32_t version = version_dict.encode(cols[1]);
    int8_t abstract = to_i8(cols[3]);
    uint32_t tlabel = tlabel_dict.encode(cols[7]);

    write_pod(tag_out, tag);
    write_pod(version_out, version);
    write_pod(abstract_out, abstract);
    write_pod(tlabel_out, tlabel);
    ++rows;
  }

  write_row_count(table_dir, rows);
  write_dict_file(table_dir / "tlabel.dict", tlabel_dict.values);
  std::cerr << "ingested tag rows=" << rows << "\n";
}

static void ingest_pre(const fs::path& src_dir, const fs::path& out_dir,
                       SharedDict& adsh_dict,
                       SharedDict& tag_dict,
                       SharedDict& version_dict,
                       SharedDict& stmt_dict,
                       SharedDict& rfile_dict) {
  fs::path in_path = src_dir / "pre.csv";
  fs::path table_dir = out_dir / "pre";
  fs::create_directories(table_dir);

  std::ifstream in(in_path);
  std::string line;
  std::getline(in, line);  // header

  std::ofstream adsh_out(table_dir / "adsh.bin", std::ios::binary);
  std::ofstream report_out(table_dir / "report.bin", std::ios::binary);
  std::ofstream line_out(table_dir / "line.bin", std::ios::binary);
  std::ofstream stmt_out(table_dir / "stmt.bin", std::ios::binary);
  std::ofstream rfile_out(table_dir / "rfile.bin", std::ios::binary);
  std::ofstream tag_out(table_dir / "tag.bin", std::ios::binary);
  std::ofstream version_out(table_dir / "version.bin", std::ios::binary);
  std::ofstream plabel_out(table_dir / "plabel.bin", std::ios::binary);

  LocalDict plabel_dict;
  uint64_t rows = 0;
  while (std::getline(in, line)) {
    if (line.empty()) continue;
    auto cols = parse_csv_line(line);
    if (cols.size() < 10) continue;

    uint32_t adsh = adsh_dict.encode(cols[0]);
    int16_t report = to_i16(cols[1]);
    int16_t line_num = to_i16(cols[2]);
    uint32_t stmt_code = stmt_dict.encode(cols[3]);
    uint32_t rfile_code = rfile_dict.encode(cols[5]);
    if (stmt_code > std::numeric_limits<uint8_t>::max() ||
        rfile_code > std::numeric_limits<uint8_t>::max()) {
      std::cerr << "stmt/rfile dictionary exceeded uint8_t\n";
      std::exit(1);
    }
    uint8_t stmt = static_cast<uint8_t>(stmt_code);
    uint8_t rfile = static_cast<uint8_t>(rfile_code);
    uint32_t tag = tag_dict.encode(cols[6]);
    uint32_t version = version_dict.encode(cols[7]);
    uint32_t plabel = plabel_dict.encode(cols[8]);

    write_pod(adsh_out, adsh);
    write_pod(report_out, report);
    write_pod(line_out, line_num);
    write_pod(stmt_out, stmt);
    write_pod(rfile_out, rfile);
    write_pod(tag_out, tag);
    write_pod(version_out, version);
    write_pod(plabel_out, plabel);
    ++rows;
  }

  write_row_count(table_dir, rows);
  write_dict_file(table_dir / "plabel.dict", plabel_dict.values);
  std::cerr << "ingested pre rows=" << rows << "\n";
}

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "usage: ingest <source_data_dir> <gendb_out_dir>\n";
    return 1;
  }

  fs::path src_dir = argv[1];
  fs::path out_dir = argv[2];
  fs::create_directories(out_dir);
  fs::create_directories(out_dir / "dicts");

  SharedDict adsh_dict;
  SharedDict tag_dict;
  SharedDict version_dict;
  SharedDict uom_dict;
  SharedDict stmt_dict;
  SharedDict rfile_dict;

  std::thread t_sub([&]() { ingest_sub(src_dir, out_dir, adsh_dict); });
  std::thread t_num([&]() { ingest_num(src_dir, out_dir, adsh_dict, tag_dict, version_dict, uom_dict); });
  std::thread t_tag([&]() { ingest_tag(src_dir, out_dir, tag_dict, version_dict); });
  std::thread t_pre([&]() { ingest_pre(src_dir, out_dir, adsh_dict, tag_dict, version_dict, stmt_dict, rfile_dict); });

  t_sub.join();
  t_num.join();
  t_tag.join();
  t_pre.join();

  write_dict_file(out_dir / "dicts" / "adsh.dict", adsh_dict.values);
  write_dict_file(out_dir / "dicts" / "tag.dict", tag_dict.values);
  write_dict_file(out_dir / "dicts" / "version.dict", version_dict.values);
  write_dict_file(out_dir / "dicts" / "uom.dict", uom_dict.values);
  write_dict_file(out_dir / "dicts" / "stmt.dict", stmt_dict.values);
  write_dict_file(out_dir / "dicts" / "rfile.dict", rfile_dict.values);

  std::cerr << "dictionaries: adsh=" << adsh_dict.values.size()
            << " tag=" << tag_dict.values.size()
            << " version=" << version_dict.values.size()
            << " uom=" << uom_dict.values.size()
            << " stmt=" << stmt_dict.values.size()
            << " rfile=" << rfile_dict.values.size() << "\n";

  return 0;
}
