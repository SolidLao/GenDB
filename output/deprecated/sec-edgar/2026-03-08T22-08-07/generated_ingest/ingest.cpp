#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr int32_t kInt32Null = std::numeric_limits<int32_t>::min();
constexpr int16_t kInt16Null = std::numeric_limits<int16_t>::min();
constexpr int8_t kInt8Null = std::numeric_limits<int8_t>::min();

struct CsvReader {
    explicit CsvReader(const fs::path& path) : in(path, std::ios::binary) {
        if (!in) {
            throw std::runtime_error("failed to open csv: " + path.string());
        }
    }

    bool next_record(std::vector<std::string>& fields) {
        fields.clear();
        std::string current;
        bool in_quotes = false;
        bool saw_any = false;
        for (;;) {
            int ch_int = in.get();
            if (ch_int == EOF) {
                if (!saw_any && current.empty() && fields.empty()) {
                    return false;
                }
                fields.push_back(std::move(current));
                return true;
            }
            saw_any = true;
            char ch = static_cast<char>(ch_int);
            if (in_quotes) {
                if (ch == '"') {
                    if (in.peek() == '"') {
                        in.get();
                        current.push_back('"');
                    } else {
                        in_quotes = false;
                    }
                } else {
                    current.push_back(ch);
                }
                continue;
            }
            if (ch == '"') {
                in_quotes = true;
            } else if (ch == ',') {
                fields.push_back(std::move(current));
                current.clear();
            } else if (ch == '\n') {
                fields.push_back(std::move(current));
                return true;
            } else if (ch == '\r') {
                if (in.peek() == '\n') {
                    in.get();
                }
                fields.push_back(std::move(current));
                return true;
            } else {
                current.push_back(ch);
            }
        }
    }

    std::ifstream in;
};

int days_from_civil(int year, unsigned month, unsigned day) {
    year -= month <= 2;
    const int era = (year >= 0 ? year : year - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(year - era * 400);
    const unsigned doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int>(doe) - 719468;
}

int32_t parse_date_days(std::string_view text) {
    if (text.empty()) {
        return kInt32Null;
    }
    if (text.size() < 8) {
        throw std::runtime_error("invalid yyyymmdd date: " + std::string(text));
    }
    int year = std::stoi(std::string(text.substr(0, 4)));
    unsigned month = static_cast<unsigned>(std::stoi(std::string(text.substr(4, 2))));
    unsigned day = static_cast<unsigned>(std::stoi(std::string(text.substr(6, 2))));
    return static_cast<int32_t>(days_from_civil(year, month, day));
}

int32_t parse_int32(std::string_view text) {
    if (text.empty()) {
        return kInt32Null;
    }
    return static_cast<int32_t>(std::stol(std::string(text)));
}

int16_t parse_int16(std::string_view text) {
    if (text.empty()) {
        return kInt16Null;
    }
    return static_cast<int16_t>(std::stoi(std::string(text)));
}

int8_t parse_int8(std::string_view text) {
    if (text.empty()) {
        return kInt8Null;
    }
    return static_cast<int8_t>(std::stoi(std::string(text)));
}

double parse_double(std::string_view text) {
    if (text.empty()) {
        return std::numeric_limits<double>::quiet_NaN();
    }
    return std::stod(std::string(text));
}

template <typename CodeT>
CodeT checked_code(uint32_t value, const std::string& column_name) {
    if (value > static_cast<uint32_t>(std::numeric_limits<CodeT>::max())) {
        throw std::runtime_error("dictionary overflow for column " + column_name);
    }
    return static_cast<CodeT>(value);
}

void ensure_dir(const fs::path& path) {
    fs::create_directories(path);
}

template <typename T>
void write_vector_binary(const fs::path& path, const std::vector<T>& values) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("failed to open for write: " + path.string());
    }
    if (!values.empty()) {
        out.write(reinterpret_cast<const char*>(values.data()), static_cast<std::streamsize>(values.size() * sizeof(T)));
    }
}

void write_text_file(const fs::path& path, const std::string& content) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("failed to write text file: " + path.string());
    }
    out << content;
}

struct BlobColumn {
    std::vector<uint64_t> offsets{0};
    std::string data;

    void append(std::string_view text) {
        data.append(text.data(), text.size());
        offsets.push_back(static_cast<uint64_t>(data.size()));
    }
};

void write_blob_column(const fs::path& dir, const std::string& name, const BlobColumn& column) {
    write_vector_binary(dir / (name + ".offsets.bin"), column.offsets);
    std::ofstream out(dir / (name + ".data.bin"), std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("failed to write blob data");
    }
    out.write(column.data.data(), static_cast<std::streamsize>(column.data.size()));
}

struct LocalStringDict {
    std::unordered_map<std::string, uint32_t> codes;
    std::vector<std::string> values{std::string()};

    uint32_t get_or_add(std::string_view value) {
        if (value.empty()) {
            return 0;
        }
        auto it = codes.find(std::string(value));
        if (it != codes.end()) {
            return it->second;
        }
        const uint32_t code = static_cast<uint32_t>(values.size());
        values.emplace_back(value);
        codes.emplace(values.back(), code);
        return code;
    }
};

struct SharedStringDict {
    std::mutex mu;
    std::unordered_map<std::string, uint32_t> codes;
    std::vector<std::string> values{std::string()};

    uint32_t get_or_add(std::string_view value) {
        if (value.empty()) {
            return 0;
        }
        std::lock_guard<std::mutex> guard(mu);
        auto it = codes.find(std::string(value));
        if (it != codes.end()) {
            return it->second;
        }
        const uint32_t code = static_cast<uint32_t>(values.size());
        values.emplace_back(value);
        codes.emplace(values.back(), code);
        return code;
    }

    std::vector<std::string> snapshot() {
        std::lock_guard<std::mutex> guard(mu);
        return values;
    }
};

void write_dictionary(const fs::path& dict_dir, const std::string& name, const std::vector<std::string>& values) {
    ensure_dir(dict_dir);
    std::vector<uint64_t> offsets;
    offsets.reserve(values.size() + 1);
    offsets.push_back(0);
    std::string blob;
    for (const auto& value : values) {
        blob.append(value);
        offsets.push_back(static_cast<uint64_t>(blob.size()));
    }
    write_vector_binary(dict_dir / (name + ".offsets.bin"), offsets);
    std::ofstream out(dict_dir / (name + ".data.bin"), std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("failed to write dictionary data: " + name);
    }
    out.write(blob.data(), static_cast<std::streamsize>(blob.size()));
}

struct SubTable {
    std::vector<uint32_t> adsh;
    std::vector<int32_t> cik;
    std::vector<uint32_t> name;
    std::vector<int32_t> sic;
    std::vector<uint16_t> countryba;
    std::vector<uint16_t> stprba;
    BlobColumn cityba;
    std::vector<uint16_t> countryinc;
    std::vector<uint16_t> form;
    std::vector<int32_t> period;
    std::vector<int16_t> fy;
    std::vector<uint16_t> fp;
    std::vector<int32_t> filed;
    BlobColumn accepted;
    std::vector<int8_t> prevrpt;
    std::vector<int16_t> nciks;
    std::vector<uint16_t> afs;
    std::vector<int8_t> wksi;
    std::vector<uint16_t> fye;
    BlobColumn instance;
    LocalStringDict name_dict;
    LocalStringDict countryba_dict;
    LocalStringDict stprba_dict;
    LocalStringDict countryinc_dict;
    LocalStringDict form_dict;
    LocalStringDict fp_dict;
    LocalStringDict afs_dict;
    LocalStringDict fye_dict;
};

struct NumTable {
    std::vector<uint32_t> adsh;
    std::vector<uint32_t> tag;
    std::vector<uint32_t> version;
    std::vector<int32_t> ddate;
    std::vector<int8_t> qtrs;
    std::vector<uint16_t> uom;
    std::vector<uint32_t> coreg;
    std::vector<double> value;
    BlobColumn footnote;
    LocalStringDict uom_dict;
    LocalStringDict coreg_dict;
};

struct TagTable {
    std::vector<uint32_t> tag;
    std::vector<uint32_t> version;
    std::vector<int8_t> custom;
    std::vector<int8_t> abstract;
    std::vector<uint16_t> datatype;
    std::vector<uint16_t> iord;
    std::vector<uint16_t> crdr;
    std::vector<uint32_t> tlabel;
    BlobColumn doc;
    LocalStringDict datatype_dict;
    LocalStringDict iord_dict;
    LocalStringDict crdr_dict;
    LocalStringDict tlabel_dict;
};

struct PreTable {
    std::vector<uint32_t> adsh;
    std::vector<int32_t> report;
    std::vector<int32_t> line;
    std::vector<uint16_t> stmt;
    std::vector<int8_t> inpth;
    std::vector<uint16_t> rfile;
    std::vector<uint32_t> tag;
    std::vector<uint32_t> version;
    std::vector<uint32_t> plabel;
    std::vector<int8_t> negating;
    LocalStringDict stmt_dict;
    LocalStringDict rfile_dict;
    LocalStringDict plabel_dict;
};

void reserve_sub(SubTable& table, size_t rows) {
    table.adsh.reserve(rows);
    table.cik.reserve(rows);
    table.name.reserve(rows);
    table.sic.reserve(rows);
    table.countryba.reserve(rows);
    table.stprba.reserve(rows);
    table.countryinc.reserve(rows);
    table.form.reserve(rows);
    table.period.reserve(rows);
    table.fy.reserve(rows);
    table.fp.reserve(rows);
    table.filed.reserve(rows);
    table.prevrpt.reserve(rows);
    table.nciks.reserve(rows);
    table.afs.reserve(rows);
    table.wksi.reserve(rows);
    table.fye.reserve(rows);
    table.cityba.offsets.reserve(rows + 1);
    table.accepted.offsets.reserve(rows + 1);
    table.instance.offsets.reserve(rows + 1);
}

void reserve_num(NumTable& table, size_t rows) {
    table.adsh.reserve(rows);
    table.tag.reserve(rows);
    table.version.reserve(rows);
    table.ddate.reserve(rows);
    table.qtrs.reserve(rows);
    table.uom.reserve(rows);
    table.coreg.reserve(rows);
    table.value.reserve(rows);
    table.footnote.offsets.reserve(rows + 1);
}

void reserve_tag(TagTable& table, size_t rows) {
    table.tag.reserve(rows);
    table.version.reserve(rows);
    table.custom.reserve(rows);
    table.abstract.reserve(rows);
    table.datatype.reserve(rows);
    table.iord.reserve(rows);
    table.crdr.reserve(rows);
    table.tlabel.reserve(rows);
    table.doc.offsets.reserve(rows + 1);
}

void reserve_pre(PreTable& table, size_t rows) {
    table.adsh.reserve(rows);
    table.report.reserve(rows);
    table.line.reserve(rows);
    table.stmt.reserve(rows);
    table.inpth.reserve(rows);
    table.rfile.reserve(rows);
    table.tag.reserve(rows);
    table.version.reserve(rows);
    table.plabel.reserve(rows);
    table.negating.reserve(rows);
}

SubTable ingest_sub(const fs::path& path, SharedStringDict& adsh_dict) {
    CsvReader reader(path);
    std::vector<std::string> fields;
    if (!reader.next_record(fields)) {
        throw std::runtime_error("sub csv is empty");
    }
    SubTable table;
    reserve_sub(table, 86135);
    while (reader.next_record(fields)) {
        if (fields.size() != 20) {
            throw std::runtime_error("sub field count mismatch");
        }
        table.adsh.push_back(adsh_dict.get_or_add(fields[0]));
        table.cik.push_back(parse_int32(fields[1]));
        table.name.push_back(table.name_dict.get_or_add(fields[2]));
        table.sic.push_back(parse_int32(fields[3]));
        table.countryba.push_back(checked_code<uint16_t>(table.countryba_dict.get_or_add(fields[4]), "sub.countryba"));
        table.stprba.push_back(checked_code<uint16_t>(table.stprba_dict.get_or_add(fields[5]), "sub.stprba"));
        table.cityba.append(fields[6]);
        table.countryinc.push_back(checked_code<uint16_t>(table.countryinc_dict.get_or_add(fields[7]), "sub.countryinc"));
        table.form.push_back(checked_code<uint16_t>(table.form_dict.get_or_add(fields[8]), "sub.form"));
        table.period.push_back(parse_date_days(fields[9]));
        table.fy.push_back(parse_int16(fields[10]));
        table.fp.push_back(checked_code<uint16_t>(table.fp_dict.get_or_add(fields[11]), "sub.fp"));
        table.filed.push_back(parse_date_days(fields[12]));
        table.accepted.append(fields[13]);
        table.prevrpt.push_back(parse_int8(fields[14]));
        table.nciks.push_back(parse_int16(fields[15]));
        table.afs.push_back(checked_code<uint16_t>(table.afs_dict.get_or_add(fields[16]), "sub.afs"));
        table.wksi.push_back(parse_int8(fields[17]));
        table.fye.push_back(checked_code<uint16_t>(table.fye_dict.get_or_add(fields[18]), "sub.fye"));
        table.instance.append(fields[19]);
    }
    return table;
}

NumTable ingest_num(const fs::path& path, SharedStringDict& adsh_dict, SharedStringDict& tag_dict, SharedStringDict& version_dict) {
    CsvReader reader(path);
    std::vector<std::string> fields;
    if (!reader.next_record(fields)) {
        throw std::runtime_error("num csv is empty");
    }
    NumTable table;
    reserve_num(table, 39401761);
    while (reader.next_record(fields)) {
        if (fields.size() != 9) {
            throw std::runtime_error("num field count mismatch");
        }
        table.adsh.push_back(adsh_dict.get_or_add(fields[0]));
        table.tag.push_back(tag_dict.get_or_add(fields[1]));
        table.version.push_back(version_dict.get_or_add(fields[2]));
        table.ddate.push_back(parse_date_days(fields[3]));
        table.qtrs.push_back(parse_int8(fields[4]));
        table.uom.push_back(checked_code<uint16_t>(table.uom_dict.get_or_add(fields[5]), "num.uom"));
        table.coreg.push_back(table.coreg_dict.get_or_add(fields[6]));
        table.value.push_back(parse_double(fields[7]));
        table.footnote.append(fields[8]);
    }
    return table;
}

TagTable ingest_tag(const fs::path& path, SharedStringDict& tag_dict, SharedStringDict& version_dict) {
    CsvReader reader(path);
    std::vector<std::string> fields;
    if (!reader.next_record(fields)) {
        throw std::runtime_error("tag csv is empty");
    }
    TagTable table;
    reserve_tag(table, 1070662);
    while (reader.next_record(fields)) {
        if (fields.size() != 9) {
            throw std::runtime_error("tag field count mismatch");
        }
        table.tag.push_back(tag_dict.get_or_add(fields[0]));
        table.version.push_back(version_dict.get_or_add(fields[1]));
        table.custom.push_back(parse_int8(fields[2]));
        table.abstract.push_back(parse_int8(fields[3]));
        table.datatype.push_back(checked_code<uint16_t>(table.datatype_dict.get_or_add(fields[4]), "tag.datatype"));
        table.iord.push_back(checked_code<uint16_t>(table.iord_dict.get_or_add(fields[5]), "tag.iord"));
        table.crdr.push_back(checked_code<uint16_t>(table.crdr_dict.get_or_add(fields[6]), "tag.crdr"));
        table.tlabel.push_back(table.tlabel_dict.get_or_add(fields[7]));
        table.doc.append(fields[8]);
    }
    return table;
}

PreTable ingest_pre(const fs::path& path, SharedStringDict& adsh_dict, SharedStringDict& tag_dict, SharedStringDict& version_dict) {
    CsvReader reader(path);
    std::vector<std::string> fields;
    if (!reader.next_record(fields)) {
        throw std::runtime_error("pre csv is empty");
    }
    PreTable table;
    reserve_pre(table, 9600799);
    while (reader.next_record(fields)) {
        if (fields.size() != 10) {
            throw std::runtime_error("pre field count mismatch");
        }
        table.adsh.push_back(adsh_dict.get_or_add(fields[0]));
        table.report.push_back(parse_int32(fields[1]));
        table.line.push_back(parse_int32(fields[2]));
        table.stmt.push_back(checked_code<uint16_t>(table.stmt_dict.get_or_add(fields[3]), "pre.stmt"));
        table.inpth.push_back(parse_int8(fields[4]));
        table.rfile.push_back(checked_code<uint16_t>(table.rfile_dict.get_or_add(fields[5]), "pre.rfile"));
        table.tag.push_back(tag_dict.get_or_add(fields[6]));
        table.version.push_back(version_dict.get_or_add(fields[7]));
        table.plabel.push_back(table.plabel_dict.get_or_add(fields[8]));
        table.negating.push_back(parse_int8(fields[9]));
    }
    return table;
}

void write_sub(const fs::path& out_dir, const SubTable& table) {
    ensure_dir(out_dir);
    const fs::path dict_dir = out_dir.parent_path() / "dicts";
    std::vector<std::future<void>> tasks;
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "adsh.bin", table.adsh); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "cik.bin", table.cik); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "name.bin", table.name); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "sic.bin", table.sic); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "countryba.bin", table.countryba); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "stprba.bin", table.stprba); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_blob_column(out_dir, "cityba", table.cityba); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "countryinc.bin", table.countryinc); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "form.bin", table.form); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "period.bin", table.period); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "fy.bin", table.fy); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "fp.bin", table.fp); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "filed.bin", table.filed); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_blob_column(out_dir, "accepted", table.accepted); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "prevrpt.bin", table.prevrpt); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "nciks.bin", table.nciks); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "afs.bin", table.afs); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "wksi.bin", table.wksi); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "fye.bin", table.fye); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_blob_column(out_dir, "instance", table.instance); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "sub_name", table.name_dict.values); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "sub_countryba", table.countryba_dict.values); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "sub_stprba", table.stprba_dict.values); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "sub_countryinc", table.countryinc_dict.values); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "sub_form", table.form_dict.values); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "sub_fp", table.fp_dict.values); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "sub_afs", table.afs_dict.values); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "sub_fye", table.fye_dict.values); }));
    for (auto& task : tasks) {
        task.get();
    }
    write_text_file(out_dir / "row_count.txt", std::to_string(table.adsh.size()) + "\n");
}

void write_num(const fs::path& out_dir, const NumTable& table) {
    ensure_dir(out_dir);
    const fs::path dict_dir = out_dir.parent_path() / "dicts";
    std::vector<std::future<void>> tasks;
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "adsh.bin", table.adsh); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "tag.bin", table.tag); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "version.bin", table.version); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "ddate.bin", table.ddate); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "qtrs.bin", table.qtrs); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "uom.bin", table.uom); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "coreg.bin", table.coreg); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "value.bin", table.value); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_blob_column(out_dir, "footnote", table.footnote); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "num_uom", table.uom_dict.values); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "num_coreg", table.coreg_dict.values); }));
    for (auto& task : tasks) {
        task.get();
    }
    write_text_file(out_dir / "row_count.txt", std::to_string(table.adsh.size()) + "\n");
}

void write_tag(const fs::path& out_dir, const TagTable& table) {
    ensure_dir(out_dir);
    const fs::path dict_dir = out_dir.parent_path() / "dicts";
    std::vector<std::future<void>> tasks;
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "tag.bin", table.tag); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "version.bin", table.version); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "custom.bin", table.custom); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "abstract.bin", table.abstract); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "datatype.bin", table.datatype); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "iord.bin", table.iord); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "crdr.bin", table.crdr); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "tlabel.bin", table.tlabel); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_blob_column(out_dir, "doc", table.doc); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "tag_datatype", table.datatype_dict.values); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "tag_iord", table.iord_dict.values); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "tag_crdr", table.crdr_dict.values); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "tag_tlabel", table.tlabel_dict.values); }));
    for (auto& task : tasks) {
        task.get();
    }
    write_text_file(out_dir / "row_count.txt", std::to_string(table.tag.size()) + "\n");
}

void write_pre(const fs::path& out_dir, const PreTable& table) {
    ensure_dir(out_dir);
    const fs::path dict_dir = out_dir.parent_path() / "dicts";
    std::vector<std::future<void>> tasks;
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "adsh.bin", table.adsh); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "report.bin", table.report); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "line.bin", table.line); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "stmt.bin", table.stmt); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "inpth.bin", table.inpth); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "rfile.bin", table.rfile); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "tag.bin", table.tag); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "version.bin", table.version); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "plabel.bin", table.plabel); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_vector_binary(out_dir / "negating.bin", table.negating); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "pre_stmt", table.stmt_dict.values); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "pre_rfile", table.rfile_dict.values); }));
    tasks.push_back(std::async(std::launch::async, [&] { write_dictionary(dict_dir, "pre_plabel", table.plabel_dict.values); }));
    for (auto& task : tasks) {
        task.get();
    }
    write_text_file(out_dir / "row_count.txt", std::to_string(table.adsh.size()) + "\n");
}

void write_shared_dicts(const fs::path& root_dir, SharedStringDict& adsh_dict, SharedStringDict& tag_dict, SharedStringDict& version_dict) {
    const fs::path dict_dir = root_dir / "dicts";
    write_dictionary(dict_dir, "global_adsh", adsh_dict.snapshot());
    write_dictionary(dict_dir, "global_tag", tag_dict.snapshot());
    write_dictionary(dict_dir, "global_version", version_dict.snapshot());
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc != 3) {
            std::cerr << "usage: ingest <source_dir> <gendb_dir>\n";
            return 1;
        }
        const fs::path source_dir = argv[1];
        const fs::path out_dir = argv[2];
        ensure_dir(out_dir);
        SharedStringDict adsh_dict;
        SharedStringDict tag_dict;
        SharedStringDict version_dict;

        auto sub_future = std::async(std::launch::async, ingest_sub, source_dir / "sub.csv", std::ref(adsh_dict));
        auto tag_future = std::async(std::launch::async, ingest_tag, source_dir / "tag.csv", std::ref(tag_dict), std::ref(version_dict));
        auto pre_future = std::async(std::launch::async, ingest_pre, source_dir / "pre.csv", std::ref(adsh_dict), std::ref(tag_dict), std::ref(version_dict));
        auto num_future = std::async(std::launch::async, ingest_num, source_dir / "num.csv", std::ref(adsh_dict), std::ref(tag_dict), std::ref(version_dict));

        SubTable sub = sub_future.get();
        TagTable tag = tag_future.get();
        PreTable pre = pre_future.get();
        NumTable num = num_future.get();

        write_shared_dicts(out_dir, adsh_dict, tag_dict, version_dict);

        auto write_sub_future = std::async(std::launch::async, write_sub, out_dir / "sub", std::cref(sub));
        auto write_tag_future = std::async(std::launch::async, write_tag, out_dir / "tag", std::cref(tag));
        auto write_pre_future = std::async(std::launch::async, write_pre, out_dir / "pre", std::cref(pre));
        auto write_num_future = std::async(std::launch::async, write_num, out_dir / "num", std::cref(num));

        write_sub_future.get();
        write_tag_future.get();
        write_pre_future.get();
        write_num_future.get();

        write_text_file(out_dir / "manifest.txt", "format=binary_columnar\nversion=1\n");

        std::cout << "Ingestion complete\n"
                  << "  sub rows: " << sub.adsh.size() << "\n"
                  << "  tag rows: " << tag.tag.size() << "\n"
                  << "  pre rows: " << pre.adsh.size() << "\n"
                  << "  num rows: " << num.adsh.size() << "\n";
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "ingest failed: " << ex.what() << "\n";
        return 1;
    }
}
