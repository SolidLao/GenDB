// ingest.cpp — Parse SEC EDGAR CSV files into binary columnar storage with dict encoding
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <cassert>
#include <filesystem>

namespace fs = std::filesystem;

// ============================================================================
// Dictionary class
// ============================================================================
template<typename CodeType>
struct Dictionary {
    std::unordered_map<std::string, CodeType> str_to_code;
    std::vector<std::string> code_to_str;

    CodeType encode(const std::string& s) {
        auto it = str_to_code.find(s);
        if (it != str_to_code.end()) return it->second;
        CodeType code = static_cast<CodeType>(code_to_str.size());
        str_to_code[s] = code;
        code_to_str.push_back(s);
        return code;
    }

    CodeType encode(const char* begin, size_t len) {
        std::string s(begin, len);
        return encode(s);
    }

    size_t size() const { return code_to_str.size(); }

    void write_to_file(const std::string& path) const {
        std::ofstream out(path, std::ios::binary);
        uint32_t n = static_cast<uint32_t>(code_to_str.size());
        out.write(reinterpret_cast<const char*>(&n), 4);
        // Write offsets
        std::vector<uint32_t> offsets(n + 1);
        uint32_t off = 0;
        for (uint32_t i = 0; i < n; i++) {
            offsets[i] = off;
            off += static_cast<uint32_t>(code_to_str[i].size());
        }
        offsets[n] = off;
        out.write(reinterpret_cast<const char*>(offsets.data()), (n + 1) * 4);
        // Write string data
        for (uint32_t i = 0; i < n; i++) {
            out.write(code_to_str[i].data(), code_to_str[i].size());
        }
    }
};

// ============================================================================
// Mmap helper
// ============================================================================
struct MmapFile {
    const char* data = nullptr;
    size_t size = 0;
    int fd = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::cerr << "Cannot open " << path << "\n"; return false; }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        data = static_cast<const char*>(mmap(nullptr, size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
        if (data == MAP_FAILED) { std::cerr << "mmap failed for " << path << "\n"; close(fd); return false; }
        madvise(const_cast<char*>(data), size, MADV_SEQUENTIAL);
        return true;
    }

    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap(const_cast<char*>(data), size);
        if (fd >= 0) close(fd);
    }
};

// ============================================================================
// CSV field parser — handles quoted fields with embedded commas and escaped quotes
// Returns pointer past the delimiter (comma or newline)
// ============================================================================
inline const char* parse_field(const char* p, const char* end, const char*& field_start, size_t& field_len) {
    if (p >= end) { field_start = p; field_len = 0; return p; }
    if (*p == '"') {
        // Quoted field
        p++; // skip opening quote
        field_start = p;
        // Find closing quote (handle escaped "" inside)
        const char* out = p;
        while (p < end) {
            if (*p == '"') {
                if (p + 1 < end && *(p + 1) == '"') {
                    p += 2; // skip escaped quote
                    continue;
                }
                // End of quoted field
                field_len = static_cast<size_t>(p - field_start);
                p++; // skip closing quote
                // Skip delimiter
                if (p < end && (*p == ',' || *p == '\n' || *p == '\r')) {
                    if (*p == '\r' && p + 1 < end && *(p + 1) == '\n') p++;
                    p++;
                }
                return p;
            }
            p++;
        }
        field_len = static_cast<size_t>(p - field_start);
        return p;
    } else {
        // Unquoted field
        field_start = p;
        while (p < end && *p != ',' && *p != '\n' && *p != '\r') p++;
        field_len = static_cast<size_t>(p - field_start);
        if (p < end) {
            if (*p == '\r' && p + 1 < end && *(p + 1) == '\n') p++;
            p++;
        }
        return p;
    }
}

// Skip to end of line
inline const char* skip_line(const char* p, const char* end) {
    while (p < end && *p != '\n') p++;
    if (p < end) p++;
    return p;
}

// Fast integer parsing
inline int32_t parse_int(const char* s, size_t len) {
    if (len == 0) return 0;
    int32_t result = 0;
    bool neg = false;
    size_t i = 0;
    if (s[0] == '-') { neg = true; i = 1; }
    for (; i < len; i++) result = result * 10 + (s[i] - '0');
    return neg ? -result : result;
}

// Fast double parsing
inline double parse_double(const char* s, size_t len) {
    if (len == 0) return 0.0;
    // Use strtod for robustness with scientific notation
    char buf[64];
    size_t clen = len < 63 ? len : 63;
    memcpy(buf, s, clen);
    buf[clen] = '\0';
    return strtod(buf, nullptr);
}

// ============================================================================
// Write binary column helper
// ============================================================================
template<typename T>
void write_column(const std::string& path, const std::vector<T>& data) {
    std::ofstream out(path, std::ios::binary);
    if (!data.empty()) {
        out.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T));
    }
}

// ============================================================================
// Main
// ============================================================================
int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <storage_dir>\n";
        return 1;
    }
    std::string data_dir = argv[1];
    std::string storage_dir = argv[2];

    auto t0 = std::chrono::high_resolution_clock::now();

    // Create directory structure
    for (auto& d : {"sub", "num", "tag", "pre", "dictionaries", "indexes", "metadata"}) {
        fs::create_directories(storage_dir + "/" + d);
    }

    // Shared dictionaries
    Dictionary<uint32_t> dict_adsh, dict_tag, dict_version;
    // Local dictionaries
    Dictionary<uint32_t> dict_name, dict_tlabel, dict_plabel;
    Dictionary<uint16_t> dict_uom;
    Dictionary<uint8_t> dict_stmt, dict_rfile;

    // ========================================================================
    // Parse SUB
    // ========================================================================
    {
        auto ts = std::chrono::high_resolution_clock::now();
        MmapFile mf;
        if (!mf.open(data_dir + "/sub.csv")) return 1;
        const char* p = mf.data;
        const char* end = mf.data + mf.size;

        // Skip header
        p = skip_line(p, end);

        // Columns: adsh,cik,name,sic,countryba,stprba,cityba,countryinc,form,period,fy,fp,filed,accepted,prevrpt,nciks,afs,wksi,fye,instance
        // Need: adsh(0), cik(1), name(2), sic(3), fy(10)
        std::vector<uint32_t> col_adsh, col_name;
        std::vector<int32_t> col_cik;
        std::vector<int16_t> col_sic, col_fy;

        col_adsh.reserve(90000);
        col_cik.reserve(90000);
        col_name.reserve(90000);
        col_sic.reserve(90000);
        col_fy.reserve(90000);

        while (p < end) {
            const char* fs; size_t fl;
            // 0: adsh
            p = parse_field(p, end, fs, fl);
            if (fl == 0 && p >= end) break;
            col_adsh.push_back(dict_adsh.encode(fs, fl));
            // 1: cik
            p = parse_field(p, end, fs, fl);
            col_cik.push_back(parse_int(fs, fl));
            // 2: name
            p = parse_field(p, end, fs, fl);
            col_name.push_back(dict_name.encode(fs, fl));
            // 3: sic
            p = parse_field(p, end, fs, fl);
            col_sic.push_back(static_cast<int16_t>(parse_int(fs, fl)));
            // 4-9: skip countryba, stprba, cityba, countryinc, form, period
            for (int i = 0; i < 6; i++) p = parse_field(p, end, fs, fl);
            // 10: fy
            p = parse_field(p, end, fs, fl);
            col_fy.push_back(static_cast<int16_t>(parse_int(fs, fl)));
            // 11-19: skip remaining
            for (int i = 0; i < 9; i++) p = parse_field(p, end, fs, fl);
        }

        uint32_t nrows = static_cast<uint32_t>(col_adsh.size());
        std::cout << "SUB: " << nrows << " rows, adsh_dict=" << dict_adsh.size()
                  << " name_dict=" << dict_name.size() << "\n";

        // Write columns in parallel
        std::vector<std::thread> threads;
        threads.emplace_back([&]{ write_column(storage_dir + "/sub/adsh.bin", col_adsh); });
        threads.emplace_back([&]{ write_column(storage_dir + "/sub/cik.bin", col_cik); });
        threads.emplace_back([&]{ write_column(storage_dir + "/sub/name.bin", col_name); });
        threads.emplace_back([&]{ write_column(storage_dir + "/sub/sic.bin", col_sic); });
        threads.emplace_back([&]{ write_column(storage_dir + "/sub/fy.bin", col_fy); });
        for (auto& t : threads) t.join();

        // Write row count
        {
            std::ofstream out(storage_dir + "/sub/row_count.bin", std::ios::binary);
            out.write(reinterpret_cast<const char*>(&nrows), 4);
        }

        auto te = std::chrono::high_resolution_clock::now();
        std::cout << "  SUB time: " << std::chrono::duration_cast<std::chrono::milliseconds>(te - ts).count() << "ms\n";
    }

    // ========================================================================
    // Parse TAG
    // ========================================================================
    {
        auto ts = std::chrono::high_resolution_clock::now();
        MmapFile mf;
        if (!mf.open(data_dir + "/tag.csv")) return 1;
        const char* p = mf.data;
        const char* end = mf.data + mf.size;
        p = skip_line(p, end);

        // Columns: tag,version,custom,abstract,datatype,iord,crdr,tlabel,doc
        // Need: tag(0), version(1), abstract(3), tlabel(7)
        std::vector<uint32_t> col_tag, col_version, col_tlabel;
        std::vector<int8_t> col_abstract;

        col_tag.reserve(1100000);
        col_version.reserve(1100000);
        col_abstract.reserve(1100000);
        col_tlabel.reserve(1100000);

        while (p < end) {
            const char* fs; size_t fl;
            // 0: tag
            p = parse_field(p, end, fs, fl);
            if (fl == 0 && p >= end) break;
            col_tag.push_back(dict_tag.encode(fs, fl));
            // 1: version
            p = parse_field(p, end, fs, fl);
            col_version.push_back(dict_version.encode(fs, fl));
            // 2: custom (skip)
            p = parse_field(p, end, fs, fl);
            // 3: abstract
            p = parse_field(p, end, fs, fl);
            col_abstract.push_back(static_cast<int8_t>(parse_int(fs, fl)));
            // 4: datatype (skip)
            p = parse_field(p, end, fs, fl);
            // 5: iord (skip)
            p = parse_field(p, end, fs, fl);
            // 6: crdr (skip)
            p = parse_field(p, end, fs, fl);
            // 7: tlabel
            p = parse_field(p, end, fs, fl);
            col_tlabel.push_back(dict_tlabel.encode(fs, fl));
            // 8: doc (skip - can be very long, possibly quoted)
            p = parse_field(p, end, fs, fl);
        }

        uint32_t nrows = static_cast<uint32_t>(col_tag.size());
        std::cout << "TAG: " << nrows << " rows, tag_dict=" << dict_tag.size()
                  << " version_dict=" << dict_version.size()
                  << " tlabel_dict=" << dict_tlabel.size() << "\n";

        std::vector<std::thread> threads;
        threads.emplace_back([&]{ write_column(storage_dir + "/tag/tag.bin", col_tag); });
        threads.emplace_back([&]{ write_column(storage_dir + "/tag/version.bin", col_version); });
        threads.emplace_back([&]{ write_column(storage_dir + "/tag/abstract.bin", col_abstract); });
        threads.emplace_back([&]{ write_column(storage_dir + "/tag/tlabel.bin", col_tlabel); });
        for (auto& t : threads) t.join();

        {
            std::ofstream out(storage_dir + "/tag/row_count.bin", std::ios::binary);
            out.write(reinterpret_cast<const char*>(&nrows), 4);
        }

        auto te = std::chrono::high_resolution_clock::now();
        std::cout << "  TAG time: " << std::chrono::duration_cast<std::chrono::milliseconds>(te - ts).count() << "ms\n";
    }

    // ========================================================================
    // Parse PRE
    // ========================================================================
    {
        auto ts = std::chrono::high_resolution_clock::now();
        MmapFile mf;
        if (!mf.open(data_dir + "/pre.csv")) return 1;
        const char* p = mf.data;
        const char* end = mf.data + mf.size;
        p = skip_line(p, end);

        // Columns: adsh,report,line,stmt,inpth,rfile,tag,version,plabel,negating
        // Need: adsh(0), line(2), stmt(3), rfile(5), tag(6), version(7), plabel(8)
        std::vector<uint32_t> col_adsh, col_tag, col_version, col_plabel;
        std::vector<uint8_t> col_stmt, col_rfile;
        std::vector<int32_t> col_line;

        size_t est = 9700000;
        col_adsh.reserve(est);
        col_tag.reserve(est);
        col_version.reserve(est);
        col_plabel.reserve(est);
        col_stmt.reserve(est);
        col_rfile.reserve(est);
        col_line.reserve(est);

        while (p < end) {
            const char* fs; size_t fl;
            // 0: adsh
            p = parse_field(p, end, fs, fl);
            if (fl == 0 && p >= end) break;
            col_adsh.push_back(dict_adsh.encode(fs, fl));
            // 1: report (skip)
            p = parse_field(p, end, fs, fl);
            // 2: line
            p = parse_field(p, end, fs, fl);
            col_line.push_back(parse_int(fs, fl));
            // 3: stmt
            p = parse_field(p, end, fs, fl);
            col_stmt.push_back(dict_stmt.encode(fs, fl));
            // 4: inpth (skip)
            p = parse_field(p, end, fs, fl);
            // 5: rfile
            p = parse_field(p, end, fs, fl);
            col_rfile.push_back(dict_rfile.encode(fs, fl));
            // 6: tag
            p = parse_field(p, end, fs, fl);
            col_tag.push_back(dict_tag.encode(fs, fl));
            // 7: version
            p = parse_field(p, end, fs, fl);
            col_version.push_back(dict_version.encode(fs, fl));
            // 8: plabel
            p = parse_field(p, end, fs, fl);
            col_plabel.push_back(dict_plabel.encode(fs, fl));
            // 9: negating
            p = parse_field(p, end, fs, fl);
        }

        uint32_t nrows = static_cast<uint32_t>(col_adsh.size());
        std::cout << "PRE: " << nrows << " rows, stmt_dict=" << dict_stmt.size()
                  << " rfile_dict=" << dict_rfile.size()
                  << " plabel_dict=" << dict_plabel.size() << "\n";

        std::vector<std::thread> threads;
        threads.emplace_back([&]{ write_column(storage_dir + "/pre/adsh.bin", col_adsh); });
        threads.emplace_back([&]{ write_column(storage_dir + "/pre/tag.bin", col_tag); });
        threads.emplace_back([&]{ write_column(storage_dir + "/pre/version.bin", col_version); });
        threads.emplace_back([&]{ write_column(storage_dir + "/pre/stmt.bin", col_stmt); });
        threads.emplace_back([&]{ write_column(storage_dir + "/pre/rfile.bin", col_rfile); });
        threads.emplace_back([&]{ write_column(storage_dir + "/pre/line.bin", col_line); });
        threads.emplace_back([&]{ write_column(storage_dir + "/pre/plabel.bin", col_plabel); });
        for (auto& t : threads) t.join();

        {
            std::ofstream out(storage_dir + "/pre/row_count.bin", std::ios::binary);
            out.write(reinterpret_cast<const char*>(&nrows), 4);
        }

        auto te = std::chrono::high_resolution_clock::now();
        std::cout << "  PRE time: " << std::chrono::duration_cast<std::chrono::milliseconds>(te - ts).count() << "ms\n";
    }

    // ========================================================================
    // Parse NUM (largest table — 39M rows)
    // ========================================================================
    {
        auto ts = std::chrono::high_resolution_clock::now();
        MmapFile mf;
        if (!mf.open(data_dir + "/num.csv")) return 1;
        const char* p = mf.data;
        const char* end = mf.data + mf.size;
        p = skip_line(p, end);

        // Columns: adsh,tag,version,ddate,qtrs,uom,coreg,value,footnote
        // Need: adsh(0), tag(1), version(2), ddate(3), uom(5), value(7)
        std::vector<uint32_t> col_adsh, col_tag, col_version;
        std::vector<int32_t> col_ddate;
        std::vector<uint16_t> col_uom;
        std::vector<double> col_value;
        std::vector<uint8_t> col_value_null;

        size_t est = 40000000;
        col_adsh.reserve(est);
        col_tag.reserve(est);
        col_version.reserve(est);
        col_ddate.reserve(est);
        col_uom.reserve(est);
        col_value.reserve(est);
        col_value_null.reserve(est);

        size_t row_count = 0;
        while (p < end) {
            const char* fs; size_t fl;
            // 0: adsh
            p = parse_field(p, end, fs, fl);
            if (fl == 0 && p >= end) break;
            col_adsh.push_back(dict_adsh.encode(fs, fl));
            // 1: tag
            p = parse_field(p, end, fs, fl);
            col_tag.push_back(dict_tag.encode(fs, fl));
            // 2: version
            p = parse_field(p, end, fs, fl);
            col_version.push_back(dict_version.encode(fs, fl));
            // 3: ddate
            p = parse_field(p, end, fs, fl);
            col_ddate.push_back(parse_int(fs, fl));
            // 4: qtrs (skip)
            p = parse_field(p, end, fs, fl);
            // 5: uom
            p = parse_field(p, end, fs, fl);
            col_uom.push_back(dict_uom.encode(fs, fl));
            // 6: coreg (skip)
            p = parse_field(p, end, fs, fl);
            // 7: value
            p = parse_field(p, end, fs, fl);
            if (fl == 0) {
                col_value.push_back(0.0);
                col_value_null.push_back(1); // null
            } else {
                col_value.push_back(parse_double(fs, fl));
                col_value_null.push_back(0); // not null
            }
            // 8: footnote (skip — may be quoted with commas)
            p = parse_field(p, end, fs, fl);

            row_count++;
            if (row_count % 10000000 == 0) {
                std::cout << "  NUM progress: " << row_count / 1000000 << "M rows\n";
            }
        }

        uint32_t nrows = static_cast<uint32_t>(col_adsh.size());
        std::cout << "NUM: " << nrows << " rows, uom_dict=" << dict_uom.size() << "\n";

        std::vector<std::thread> threads;
        threads.emplace_back([&]{ write_column(storage_dir + "/num/adsh.bin", col_adsh); });
        threads.emplace_back([&]{ write_column(storage_dir + "/num/tag.bin", col_tag); });
        threads.emplace_back([&]{ write_column(storage_dir + "/num/version.bin", col_version); });
        threads.emplace_back([&]{ write_column(storage_dir + "/num/ddate.bin", col_ddate); });
        threads.emplace_back([&]{ write_column(storage_dir + "/num/uom.bin", col_uom); });
        threads.emplace_back([&]{ write_column(storage_dir + "/num/value.bin", col_value); });
        threads.emplace_back([&]{ write_column(storage_dir + "/num/value_null.bin", col_value_null); });
        for (auto& t : threads) t.join();

        {
            std::ofstream out(storage_dir + "/num/row_count.bin", std::ios::binary);
            out.write(reinterpret_cast<const char*>(&nrows), 4);
        }

        auto te = std::chrono::high_resolution_clock::now();
        std::cout << "  NUM time: " << std::chrono::duration_cast<std::chrono::milliseconds>(te - ts).count() << "ms\n";
    }

    // ========================================================================
    // Write dictionaries
    // ========================================================================
    {
        auto ts = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> threads;
        threads.emplace_back([&]{ dict_adsh.write_to_file(storage_dir + "/dictionaries/adsh.dict"); });
        threads.emplace_back([&]{ dict_tag.write_to_file(storage_dir + "/dictionaries/tag.dict"); });
        threads.emplace_back([&]{ dict_version.write_to_file(storage_dir + "/dictionaries/version.dict"); });
        threads.emplace_back([&]{ dict_uom.write_to_file(storage_dir + "/dictionaries/uom.dict"); });
        threads.emplace_back([&]{ dict_name.write_to_file(storage_dir + "/dictionaries/name.dict"); });
        threads.emplace_back([&]{ dict_stmt.write_to_file(storage_dir + "/dictionaries/stmt.dict"); });
        threads.emplace_back([&]{ dict_rfile.write_to_file(storage_dir + "/dictionaries/rfile.dict"); });
        threads.emplace_back([&]{ dict_tlabel.write_to_file(storage_dir + "/dictionaries/tlabel.dict"); });
        threads.emplace_back([&]{ dict_plabel.write_to_file(storage_dir + "/dictionaries/plabel.dict"); });
        for (auto& t : threads) t.join();
        auto te = std::chrono::high_resolution_clock::now();
        std::cout << "Dictionaries written in " << std::chrono::duration_cast<std::chrono::milliseconds>(te - ts).count() << "ms\n";
    }

    // Print summary
    std::cout << "\nDictionary sizes:\n";
    std::cout << "  adsh:    " << dict_adsh.size() << "\n";
    std::cout << "  tag:     " << dict_tag.size() << "\n";
    std::cout << "  version: " << dict_version.size() << "\n";
    std::cout << "  uom:     " << dict_uom.size() << "\n";
    std::cout << "  name:    " << dict_name.size() << "\n";
    std::cout << "  stmt:    " << dict_stmt.size() << "\n";
    std::cout << "  rfile:   " << dict_rfile.size() << "\n";
    std::cout << "  tlabel:  " << dict_tlabel.size() << "\n";
    std::cout << "  plabel:  " << dict_plabel.size() << "\n";

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "\nTotal ingestion time: " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms\n";
    return 0;
}
