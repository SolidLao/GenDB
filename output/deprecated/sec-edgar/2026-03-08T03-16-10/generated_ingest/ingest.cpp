#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <numeric>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

constexpr uint32_t kNull32 = std::numeric_limits<uint32_t>::max();
constexpr int32_t kNullDate = std::numeric_limits<int32_t>::min();

std::vector<std::string> parse_csv_line(const std::string& line) {
    std::vector<std::string> fields;
    fields.reserve(16);
    std::string current;
    current.reserve(64);
    bool in_quotes = false;
    for (size_t i = 0; i < line.size(); ++i) {
        const char ch = line[i];
        if (in_quotes) {
            if (ch == '"') {
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    current.push_back('"');
                    ++i;
                } else {
                    in_quotes = false;
                }
            } else {
                current.push_back(ch);
            }
        } else if (ch == '"') {
            in_quotes = true;
        } else if (ch == ',') {
            fields.push_back(std::move(current));
            current.clear();
        } else {
            current.push_back(ch);
        }
    }
    fields.push_back(std::move(current));
    return fields;
}

int64_t days_from_civil(int y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? static_cast<unsigned>(-3) : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int>(doe) - 719468;
}

int32_t parse_yyyymmdd_days(const std::string& s) {
    if (s.empty()) {
        return kNullDate;
    }
    const int v = std::stoi(s);
    const int year = v / 10000;
    const unsigned month = static_cast<unsigned>((v / 100) % 100);
    const unsigned day = static_cast<unsigned>(v % 100);
    return static_cast<int32_t>(days_from_civil(year, month, day));
}

int64_t parse_timestamp_seconds(const std::string& s) {
    if (s.size() < 19) {
        return 0;
    }
    const int year = std::stoi(s.substr(0, 4));
    const unsigned month = static_cast<unsigned>(std::stoi(s.substr(5, 2)));
    const unsigned day = static_cast<unsigned>(std::stoi(s.substr(8, 2)));
    const int hour = std::stoi(s.substr(11, 2));
    const int minute = std::stoi(s.substr(14, 2));
    const int second = std::stoi(s.substr(17, 2));
    return days_from_civil(year, month, day) * 86400LL + hour * 3600LL + minute * 60LL + second;
}

template <typename T>
void write_vector(const fs::path& path, const std::vector<T>& values) {
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        throw std::runtime_error("failed to open " + path.string());
    }
    if (!values.empty()) {
        out.write(reinterpret_cast<const char*>(values.data()), static_cast<std::streamsize>(values.size() * sizeof(T)));
    }
}

void ensure_dir(const fs::path& path) {
    fs::create_directories(path);
}

struct VarLenColumnWriter {
    std::vector<uint64_t> offsets{0};
    std::ofstream data_out;

    explicit VarLenColumnWriter(const fs::path& data_path) : data_out(data_path, std::ios::binary) {
        if (!data_out) {
            throw std::runtime_error("failed to open " + data_path.string());
        }
    }

    void append(const std::string& value) {
        if (!value.empty()) {
            data_out.write(value.data(), static_cast<std::streamsize>(value.size()));
        }
        offsets.push_back(offsets.back() + value.size());
    }

    void finish(const fs::path& offsets_path) {
        data_out.flush();
        write_vector(offsets_path, offsets);
    }
};

class DictBuilder {
  public:
    uint32_t get_or_add(const std::string& value) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = ids_.find(value);
        if (it != ids_.end()) {
            return it->second;
        }
        const uint32_t id = static_cast<uint32_t>(values_.size());
        values_.push_back(value);
        ids_.emplace(values_.back(), id);
        return id;
    }

    uint32_t size() const {
        std::lock_guard<std::mutex> lock(mu_);
        return static_cast<uint32_t>(values_.size());
    }

    void write(const fs::path& base_path) const {
        std::lock_guard<std::mutex> lock(mu_);
        std::vector<uint64_t> offsets;
        offsets.reserve(values_.size() + 1);
        offsets.push_back(0);
        std::ofstream data_out(base_path.string() + ".data.bin", std::ios::binary);
        if (!data_out) {
            throw std::runtime_error("failed to open dictionary data for " + base_path.string());
        }
        uint64_t total = 0;
        for (const std::string& value : values_) {
            if (!value.empty()) {
                data_out.write(value.data(), static_cast<std::streamsize>(value.size()));
            }
            total += value.size();
            offsets.push_back(total);
        }
        write_vector(base_path.string() + ".offsets.bin", offsets);
    }

  private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, uint32_t> ids_;
    std::vector<std::string> values_;
};

struct SharedDicts {
    DictBuilder adsh;
    DictBuilder tag;
    DictBuilder version;

    void write_all(const fs::path& dir) const {
        ensure_dir(dir);
        adsh.write(dir / "adsh");
        tag.write(dir / "tag");
        version.write(dir / "version");
    }
};

struct SubColumns {
    std::vector<uint32_t> adsh;
    std::vector<int32_t> cik;
    std::vector<uint32_t> name;
    std::vector<int32_t> sic;
    std::vector<uint32_t> countryba;
    std::vector<uint32_t> stprba;
    std::vector<uint32_t> cityba;
    std::vector<uint32_t> countryinc;
    std::vector<uint32_t> form;
    std::vector<int32_t> period;
    std::vector<int32_t> fy;
    std::vector<uint32_t> fp;
    std::vector<int32_t> filed;
    std::vector<int64_t> accepted;
    std::vector<uint8_t> prevrpt;
    std::vector<int32_t> nciks;
    std::vector<uint32_t> afs;
    std::vector<uint8_t> wksi;
    std::vector<uint32_t> fye;
    std::vector<uint32_t> instance;

    void reserve(size_t n) {
        adsh.reserve(n); cik.reserve(n); name.reserve(n); sic.reserve(n); countryba.reserve(n);
        stprba.reserve(n); cityba.reserve(n); countryinc.reserve(n); form.reserve(n); period.reserve(n);
        fy.reserve(n); fp.reserve(n); filed.reserve(n); accepted.reserve(n); prevrpt.reserve(n);
        nciks.reserve(n); afs.reserve(n); wksi.reserve(n); fye.reserve(n); instance.reserve(n);
    }
};

struct NumColumns {
    std::vector<uint32_t> adsh;
    std::vector<uint32_t> tag;
    std::vector<uint32_t> version;
    std::vector<int32_t> ddate;
    std::vector<int32_t> qtrs;
    std::vector<uint16_t> uom;
    std::vector<uint32_t> coreg;
    std::vector<double> value;

    void reserve(size_t n) {
        adsh.reserve(n); tag.reserve(n); version.reserve(n); ddate.reserve(n);
        qtrs.reserve(n); uom.reserve(n); coreg.reserve(n); value.reserve(n);
    }
};

struct TagColumns {
    std::vector<uint32_t> tag;
    std::vector<uint32_t> version;
    std::vector<uint8_t> custom;
    std::vector<uint8_t> abstract_flag;
    std::vector<uint32_t> datatype;
    std::vector<uint32_t> iord;
    std::vector<uint32_t> crdr;
    std::vector<uint32_t> tlabel;

    void reserve(size_t n) {
        tag.reserve(n); version.reserve(n); custom.reserve(n); abstract_flag.reserve(n);
        datatype.reserve(n); iord.reserve(n); crdr.reserve(n); tlabel.reserve(n);
    }
};

struct PreColumns {
    std::vector<uint32_t> adsh;
    std::vector<int32_t> report;
    std::vector<int32_t> line;
    std::vector<uint16_t> stmt;
    std::vector<uint8_t> inpth;
    std::vector<uint16_t> rfile;
    std::vector<uint32_t> tag;
    std::vector<uint32_t> version;
    std::vector<uint32_t> plabel;
    std::vector<uint8_t> negating;

    void reserve(size_t n) {
        adsh.reserve(n); report.reserve(n); line.reserve(n); stmt.reserve(n); inpth.reserve(n);
        rfile.reserve(n); tag.reserve(n); version.reserve(n); plabel.reserve(n); negating.reserve(n);
    }
};

void ingest_sub(const fs::path& input_path, const fs::path& out_dir, SharedDicts& shared) {
    ensure_dir(out_dir);
    DictBuilder name_dict, countryba_dict, stprba_dict, cityba_dict, countryinc_dict, form_dict;
    DictBuilder fp_dict, afs_dict, fye_dict, instance_dict;
    SubColumns cols;
    cols.reserve(86135);

    std::ifstream in(input_path);
    std::string line;
    std::getline(in, line);
    while (std::getline(in, line)) {
        auto fields = parse_csv_line(line);
        cols.adsh.push_back(shared.adsh.get_or_add(fields[0]));
        cols.cik.push_back(fields[1].empty() ? 0 : std::stoi(fields[1]));
        cols.name.push_back(name_dict.get_or_add(fields[2]));
        cols.sic.push_back(fields[3].empty() ? 0 : std::stoi(fields[3]));
        cols.countryba.push_back(countryba_dict.get_or_add(fields[4]));
        cols.stprba.push_back(stprba_dict.get_or_add(fields[5]));
        cols.cityba.push_back(cityba_dict.get_or_add(fields[6]));
        cols.countryinc.push_back(countryinc_dict.get_or_add(fields[7]));
        cols.form.push_back(form_dict.get_or_add(fields[8]));
        cols.period.push_back(parse_yyyymmdd_days(fields[9]));
        cols.fy.push_back(fields[10].empty() ? 0 : std::stoi(fields[10]));
        cols.fp.push_back(fp_dict.get_or_add(fields[11]));
        cols.filed.push_back(parse_yyyymmdd_days(fields[12]));
        cols.accepted.push_back(parse_timestamp_seconds(fields[13]));
        cols.prevrpt.push_back(fields[14].empty() ? 0 : static_cast<uint8_t>(std::stoi(fields[14])));
        cols.nciks.push_back(fields[15].empty() ? 0 : std::stoi(fields[15]));
        cols.afs.push_back(afs_dict.get_or_add(fields[16]));
        cols.wksi.push_back(fields[17].empty() ? 0 : static_cast<uint8_t>(std::stoi(fields[17])));
        cols.fye.push_back(fye_dict.get_or_add(fields[18]));
        cols.instance.push_back(instance_dict.get_or_add(fields[19]));
    }

    write_vector(out_dir / "adsh.bin", cols.adsh);
    write_vector(out_dir / "cik.bin", cols.cik);
    write_vector(out_dir / "name.bin", cols.name);
    write_vector(out_dir / "sic.bin", cols.sic);
    write_vector(out_dir / "countryba.bin", cols.countryba);
    write_vector(out_dir / "stprba.bin", cols.stprba);
    write_vector(out_dir / "cityba.bin", cols.cityba);
    write_vector(out_dir / "countryinc.bin", cols.countryinc);
    write_vector(out_dir / "form.bin", cols.form);
    write_vector(out_dir / "period.bin", cols.period);
    write_vector(out_dir / "fy.bin", cols.fy);
    write_vector(out_dir / "fp.bin", cols.fp);
    write_vector(out_dir / "filed.bin", cols.filed);
    write_vector(out_dir / "accepted.bin", cols.accepted);
    write_vector(out_dir / "prevrpt.bin", cols.prevrpt);
    write_vector(out_dir / "nciks.bin", cols.nciks);
    write_vector(out_dir / "afs.bin", cols.afs);
    write_vector(out_dir / "wksi.bin", cols.wksi);
    write_vector(out_dir / "fye.bin", cols.fye);
    write_vector(out_dir / "instance.bin", cols.instance);

    name_dict.write(out_dir / "dict_name");
    countryba_dict.write(out_dir / "dict_countryba");
    stprba_dict.write(out_dir / "dict_stprba");
    cityba_dict.write(out_dir / "dict_cityba");
    countryinc_dict.write(out_dir / "dict_countryinc");
    form_dict.write(out_dir / "dict_form");
    fp_dict.write(out_dir / "dict_fp");
    afs_dict.write(out_dir / "dict_afs");
    fye_dict.write(out_dir / "dict_fye");
    instance_dict.write(out_dir / "dict_instance");
}

void ingest_tag(const fs::path& input_path, const fs::path& out_dir, SharedDicts& shared) {
    ensure_dir(out_dir);
    DictBuilder datatype_dict, iord_dict, crdr_dict, tlabel_dict;
    TagColumns cols;
    cols.reserve(1070662);
    VarLenColumnWriter doc_writer(out_dir / "doc.data.bin");

    std::ifstream in(input_path);
    std::string line;
    std::getline(in, line);
    while (std::getline(in, line)) {
        auto fields = parse_csv_line(line);
        cols.tag.push_back(shared.tag.get_or_add(fields[0]));
        cols.version.push_back(shared.version.get_or_add(fields[1]));
        cols.custom.push_back(fields[2].empty() ? 0 : static_cast<uint8_t>(std::stoi(fields[2])));
        cols.abstract_flag.push_back(fields[3].empty() ? 0 : static_cast<uint8_t>(std::stoi(fields[3])));
        cols.datatype.push_back(datatype_dict.get_or_add(fields[4]));
        cols.iord.push_back(iord_dict.get_or_add(fields[5]));
        cols.crdr.push_back(crdr_dict.get_or_add(fields[6]));
        cols.tlabel.push_back(tlabel_dict.get_or_add(fields[7]));
        doc_writer.append(fields[8]);
    }

    write_vector(out_dir / "tag.bin", cols.tag);
    write_vector(out_dir / "version.bin", cols.version);
    write_vector(out_dir / "custom.bin", cols.custom);
    write_vector(out_dir / "abstract.bin", cols.abstract_flag);
    write_vector(out_dir / "datatype.bin", cols.datatype);
    write_vector(out_dir / "iord.bin", cols.iord);
    write_vector(out_dir / "crdr.bin", cols.crdr);
    write_vector(out_dir / "tlabel.bin", cols.tlabel);
    doc_writer.finish(out_dir / "doc.offsets.bin");

    datatype_dict.write(out_dir / "dict_datatype");
    iord_dict.write(out_dir / "dict_iord");
    crdr_dict.write(out_dir / "dict_crdr");
    tlabel_dict.write(out_dir / "dict_tlabel");
}

void ingest_num(const fs::path& input_path, const fs::path& out_dir, SharedDicts& shared) {
    ensure_dir(out_dir);
    DictBuilder uom_dict, coreg_dict;
    NumColumns cols;
    cols.reserve(39401761);
    VarLenColumnWriter footnote_writer(out_dir / "footnote.data.bin");

    std::ifstream in(input_path);
    std::string line;
    std::getline(in, line);
    while (std::getline(in, line)) {
        auto fields = parse_csv_line(line);
        cols.adsh.push_back(shared.adsh.get_or_add(fields[0]));
        cols.tag.push_back(shared.tag.get_or_add(fields[1]));
        cols.version.push_back(shared.version.get_or_add(fields[2]));
        cols.ddate.push_back(parse_yyyymmdd_days(fields[3]));
        cols.qtrs.push_back(fields[4].empty() ? 0 : std::stoi(fields[4]));
        cols.uom.push_back(static_cast<uint16_t>(uom_dict.get_or_add(fields[5])));
        cols.coreg.push_back(fields[6].empty() ? kNull32 : coreg_dict.get_or_add(fields[6]));
        cols.value.push_back(fields[7].empty() ? 0.0 : std::stod(fields[7]));
        footnote_writer.append(fields[8]);
    }

    write_vector(out_dir / "adsh.bin", cols.adsh);
    write_vector(out_dir / "tag.bin", cols.tag);
    write_vector(out_dir / "version.bin", cols.version);
    write_vector(out_dir / "ddate.bin", cols.ddate);
    write_vector(out_dir / "qtrs.bin", cols.qtrs);
    write_vector(out_dir / "uom.bin", cols.uom);
    write_vector(out_dir / "coreg.bin", cols.coreg);
    write_vector(out_dir / "value.bin", cols.value);
    footnote_writer.finish(out_dir / "footnote.offsets.bin");

    uom_dict.write(out_dir / "dict_uom");
    coreg_dict.write(out_dir / "dict_coreg");
}

void ingest_pre(const fs::path& input_path, const fs::path& out_dir, SharedDicts& shared) {
    ensure_dir(out_dir);
    DictBuilder stmt_dict, rfile_dict, plabel_dict;
    PreColumns cols;
    cols.reserve(9600799);

    std::ifstream in(input_path);
    std::string line;
    std::getline(in, line);
    while (std::getline(in, line)) {
        auto fields = parse_csv_line(line);
        cols.adsh.push_back(shared.adsh.get_or_add(fields[0]));
        cols.report.push_back(fields[1].empty() ? 0 : std::stoi(fields[1]));
        cols.line.push_back(fields[2].empty() ? 0 : std::stoi(fields[2]));
        cols.stmt.push_back(static_cast<uint16_t>(stmt_dict.get_or_add(fields[3])));
        cols.inpth.push_back(fields[4].empty() ? 0 : static_cast<uint8_t>(std::stoi(fields[4])));
        cols.rfile.push_back(static_cast<uint16_t>(rfile_dict.get_or_add(fields[5])));
        cols.tag.push_back(shared.tag.get_or_add(fields[6]));
        cols.version.push_back(shared.version.get_or_add(fields[7]));
        cols.plabel.push_back(plabel_dict.get_or_add(fields[8]));
        cols.negating.push_back(fields[9].empty() ? 0 : static_cast<uint8_t>(std::stoi(fields[9])));
    }

    write_vector(out_dir / "adsh.bin", cols.adsh);
    write_vector(out_dir / "report.bin", cols.report);
    write_vector(out_dir / "line.bin", cols.line);
    write_vector(out_dir / "stmt.bin", cols.stmt);
    write_vector(out_dir / "inpth.bin", cols.inpth);
    write_vector(out_dir / "rfile.bin", cols.rfile);
    write_vector(out_dir / "tag.bin", cols.tag);
    write_vector(out_dir / "version.bin", cols.version);
    write_vector(out_dir / "plabel.bin", cols.plabel);
    write_vector(out_dir / "negating.bin", cols.negating);

    stmt_dict.write(out_dir / "dict_stmt");
    rfile_dict.write(out_dir / "dict_rfile");
    plabel_dict.write(out_dir / "dict_plabel");
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "usage: ingest <input_dir> <output_dir>\n";
        return 1;
    }
    const fs::path input_dir = argv[1];
    const fs::path output_dir = argv[2];
    ensure_dir(output_dir);
    ensure_dir(output_dir / "shared");

    SharedDicts shared;

    auto start = std::chrono::steady_clock::now();

    std::thread sub_thread(ingest_sub, input_dir / "sub.csv", output_dir / "sub", std::ref(shared));
    std::thread tag_thread(ingest_tag, input_dir / "tag.csv", output_dir / "tag", std::ref(shared));
    sub_thread.join();
    tag_thread.join();
    shared.write_all(output_dir / "shared");

    std::thread num_thread(ingest_num, input_dir / "num.csv", output_dir / "num", std::ref(shared));
    std::thread pre_thread(ingest_pre, input_dir / "pre.csv", output_dir / "pre", std::ref(shared));
    num_thread.join();
    pre_thread.join();
    shared.write_all(output_dir / "shared");

    auto end = std::chrono::steady_clock::now();
    const double seconds = std::chrono::duration<double>(end - start).count();
    std::cout << "ingestion complete in " << seconds << " seconds\n";
    std::cout << "shared adsh=" << shared.adsh.size()
              << " tag=" << shared.tag.size()
              << " version=" << shared.version.size() << "\n";
    return 0;
}
