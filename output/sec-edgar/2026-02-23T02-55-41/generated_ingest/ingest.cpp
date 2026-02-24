// ingest.cpp — SEC EDGAR SF3 ingestion
// Converts CSV tables to binary columnar format for GenDB.
//
// Key design: adsh/tag/version use SHARED dictionaries across tables.
// Critical: adsh_code == sub_row_index (sub processed first, one row per adsh PK).
// This enables O(1) num->sub and pre->sub joins with no hash lookup.
//
// ddate/period/filed are YYYYMMDD integers — stored as int32_t directly (no epoch conversion).
// num.value is double; NaN = NULL.
// All dict-encoded string columns use int32_t codes.

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <string_view>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <cmath>
#include <limits>
#include <filesystem>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <chrono>

namespace fs = std::filesystem;

// ─── Global shared dictionaries ───────────────────────────────────────────────
// Built incrementally; first-seen order determines code.
// For adsh: scanned from sub first → code_k == sub_row_k (PK property).
std::vector<std::string> adsh_strings;     // code -> string
std::unordered_map<std::string, int32_t> adsh_map;   // string -> code

std::vector<std::string> tag_strings;
std::unordered_map<std::string, int32_t> tag_map;

std::vector<std::string> version_strings;
std::unordered_map<std::string, int32_t> version_map;

// ─── Helpers ──────────────────────────────────────────────────────────────────
static int32_t get_or_add(std::vector<std::string>& strings,
                           std::unordered_map<std::string, int32_t>& map,
                           const char* ptr, size_t len)
{
    // Lookup without constructing std::string when possible
    auto it = map.find(std::string(ptr, len));
    if (it != map.end()) return it->second;
    int32_t code = (int32_t)strings.size();
    strings.emplace_back(ptr, len);
    map.emplace(strings.back(), code);
    return code;
}

static int32_t get_or_add_local(std::vector<std::string>& strings,
                                  std::unordered_map<std::string, int32_t>& map,
                                  const char* ptr, size_t len)
{
    return get_or_add(strings, map, ptr, len);
}

// Write int32_t binary column
static void write_col_i32(const std::string& path, const std::vector<int32_t>& v) {
    std::ofstream f(path, std::ios::binary);
    f.write(reinterpret_cast<const char*>(v.data()), v.size() * sizeof(int32_t));
}

// Write double binary column
static void write_col_dbl(const std::string& path, const std::vector<double>& v) {
    std::ofstream f(path, std::ios::binary);
    f.write(reinterpret_cast<const char*>(v.data()), v.size() * sizeof(double));
}

// Write dict file (one string per line, line index = code)
static void write_dict(const std::string& path, const std::vector<std::string>& strings) {
    std::ofstream f(path);
    for (const auto& s : strings) f << s << '\n';
}

// mmap helper
struct MmapFile {
    const char* data = nullptr;
    size_t size = 0;
    int fd = -1;
    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::cerr << "Cannot open: " << path << "\n"; return false; }
        struct stat sb; fstat(fd, &sb);
        size = sb.st_size;
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); close(fd); return false; }
        madvise((void*)data, size, MADV_SEQUENTIAL);
        return true;
    }
    void close_file() {
        if (data && data != MAP_FAILED) munmap((void*)data, size);
        if (fd >= 0) ::close(fd);
        data = nullptr; fd = -1;
    }
};

// ─── Fast CSV field parser ─────────────────────────────────────────────────────
// Scans from 'pos' in 'data' up to 'end', fills [field_start, field_len].
// Returns position after the delimiter (or end).
// Handles quoted fields ("...") and empty fields.
static inline size_t next_field(const char* data, size_t pos, size_t end,
                                 const char*& field_start, size_t& field_len)
{
    if (pos >= end) { field_start = nullptr; field_len = 0; return pos; }
    if (data[pos] == '"') {
        // Quoted field — not stored as-is; return start after opening quote
        // We need to handle this carefully. Build into a thread_local buffer.
        // For simplicity in this fast path, we detect and handle it.
        // NOTE: We return a pointer into the source for unquoted; quoted needs copy.
        // Signal quoted with field_start = data+pos (the quote char itself).
        field_start = data + pos;  // caller checks data[field_start[0]] == '"'
        field_len = 0;
        // Find closing quote
        size_t i = pos + 1;
        while (i < end && !(data[i] == '"' && (i+1 >= end || data[i+1] == ',' || data[i+1] == '\n' || data[i+1] == '\r'))) {
            ++i;
        }
        field_len = i - (pos + 1);  // length without surrounding quotes
        // field_start points at opening '"', field_start+1 is content
        pos = i;
        if (pos < end && data[pos] == '"') ++pos;  // skip closing quote
        if (pos < end && data[pos] == ',') ++pos;  // skip comma
        return pos;
    } else {
        field_start = data + pos;
        size_t i = pos;
        while (i < end && data[i] != ',' && data[i] != '\n' && data[i] != '\r') ++i;
        field_len = i - pos;
        pos = i;
        if (pos < end && data[pos] == ',') ++pos;
        return pos;
    }
}

// Helper: get field content pointer and length (strips quotes from quoted fields)
// Returns pointer to start of content, sets len.
static inline const char* field_content(const char* fs, size_t fl, size_t& out_len) {
    if (fs && fs[0] == '"') {
        out_len = fl;  // fl already excludes surrounding quotes
        return fs + 1;
    }
    out_len = fl;
    return fs;
}

// Parse int32 from field (empty -> 0)
static inline int32_t parse_i32(const char* fs, size_t fl) {
    size_t len; const char* p = field_content(fs, fl, len);
    if (len == 0) return 0;
    // Fast manual parse
    bool neg = (p[0] == '-');
    size_t i = neg ? 1 : 0;
    int32_t v = 0;
    while (i < len && p[i] >= '0' && p[i] <= '9') v = v * 10 + (p[i++] - '0');
    return neg ? -v : v;
}

// Parse double from field (empty -> NaN)
static inline double parse_dbl(const char* fs, size_t fl) {
    size_t len; const char* p = field_content(fs, fl, len);
    if (len == 0) return std::numeric_limits<double>::quiet_NaN();
    // strtod with manual null-termination via local buffer
    // Use thread_local buffer for performance
    static thread_local char buf[64];
    if (len < 63) {
        memcpy(buf, p, len); buf[len] = '\0';
        return strtod(buf, nullptr);
    }
    return std::stod(std::string(p, len));
}

// ─── SUB ingestion ─────────────────────────────────────────────────────────────
// Schema: adsh,cik,name,sic,countryba,stprba,cityba,countryinc,form,period,fy,fp,filed,accepted,prevrpt,nciks,afs,wksi,fye,instance
// Store: 0=adsh,1=cik,2=name,3=sic,4=countryba,5=stprba,6=cityba,7=countryinc,8=form,9=period,10=fy,11=fp,12=filed,(13=skip),14=prevrpt,15=nciks,16=afs,17=wksi,18=fye,(19=skip)
void ingest_sub(const std::string& csv_path, const std::string& out_dir)
{
    auto t0 = std::chrono::steady_clock::now();
    MmapFile f; if (!f.open(csv_path)) return;
    const char* data = f.data; size_t sz = f.size;

    // Local dicts for non-shared columns
    std::vector<std::string> name_s; std::unordered_map<std::string,int32_t> name_m;
    std::vector<std::string> cba_s;  std::unordered_map<std::string,int32_t> cba_m;
    std::vector<std::string> spb_s;  std::unordered_map<std::string,int32_t> spb_m;
    std::vector<std::string> city_s; std::unordered_map<std::string,int32_t> city_m;
    std::vector<std::string> cinc_s; std::unordered_map<std::string,int32_t> cinc_m;
    std::vector<std::string> form_s; std::unordered_map<std::string,int32_t> form_m;
    std::vector<std::string> fp_s;   std::unordered_map<std::string,int32_t> fp_m;
    std::vector<std::string> afs_s;  std::unordered_map<std::string,int32_t> afs_m;
    std::vector<std::string> fye_s;  std::unordered_map<std::string,int32_t> fye_m;

    std::vector<int32_t> adsh_col, cik_col, name_col, sic_col, cba_col, spb_col, city_col,
                          cinc_col, form_col, period_col, fy_col, fp_col, filed_col,
                          prevrpt_col, nciks_col, afs_col, wksi_col, fye_col;

    const size_t RESERVE = 90000;
    adsh_col.reserve(RESERVE); cik_col.reserve(RESERVE); name_col.reserve(RESERVE);
    sic_col.reserve(RESERVE); cba_col.reserve(RESERVE); spb_col.reserve(RESERVE);
    city_col.reserve(RESERVE); cinc_col.reserve(RESERVE); form_col.reserve(RESERVE);
    period_col.reserve(RESERVE); fy_col.reserve(RESERVE); fp_col.reserve(RESERVE);
    filed_col.reserve(RESERVE); prevrpt_col.reserve(RESERVE); nciks_col.reserve(RESERVE);
    afs_col.reserve(RESERVE); wksi_col.reserve(RESERVE); fye_col.reserve(RESERVE);

    // Skip header line
    size_t pos = 0;
    while (pos < sz && data[pos] != '\n') ++pos;
    ++pos;

    while (pos < sz) {
        // Find end of line
        size_t line_end = pos;
        while (line_end < sz && data[line_end] != '\n') ++line_end;
        if (line_end == pos) { pos = line_end + 1; continue; }

        const char* fs; size_t fl; size_t len;
        // col 0: adsh (shared)
        pos = next_field(data, pos, line_end, fs, fl);
        const char* p = field_content(fs, fl, len);
        int32_t adsh_code = get_or_add(adsh_strings, adsh_map, p, len);
        adsh_col.push_back(adsh_code);

        // col 1: cik
        pos = next_field(data, pos, line_end, fs, fl);
        cik_col.push_back(parse_i32(fs, fl));

        // col 2: name (local dict)
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        name_col.push_back(get_or_add_local(name_s, name_m, p, len));

        // col 3: sic
        pos = next_field(data, pos, line_end, fs, fl);
        sic_col.push_back(parse_i32(fs, fl));

        // col 4: countryba
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        cba_col.push_back(get_or_add_local(cba_s, cba_m, p, len));

        // col 5: stprba
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        spb_col.push_back(get_or_add_local(spb_s, spb_m, p, len));

        // col 6: cityba
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        city_col.push_back(get_or_add_local(city_s, city_m, p, len));

        // col 7: countryinc
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        cinc_col.push_back(get_or_add_local(cinc_s, cinc_m, p, len));

        // col 8: form
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        form_col.push_back(get_or_add_local(form_s, form_m, p, len));

        // col 9: period
        pos = next_field(data, pos, line_end, fs, fl);
        period_col.push_back(parse_i32(fs, fl));

        // col 10: fy
        pos = next_field(data, pos, line_end, fs, fl);
        fy_col.push_back(parse_i32(fs, fl));

        // col 11: fp
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        fp_col.push_back(get_or_add_local(fp_s, fp_m, p, len));

        // col 12: filed
        pos = next_field(data, pos, line_end, fs, fl);
        filed_col.push_back(parse_i32(fs, fl));

        // col 13: accepted (SKIP)
        pos = next_field(data, pos, line_end, fs, fl);

        // col 14: prevrpt
        pos = next_field(data, pos, line_end, fs, fl);
        prevrpt_col.push_back(parse_i32(fs, fl));

        // col 15: nciks
        pos = next_field(data, pos, line_end, fs, fl);
        nciks_col.push_back(parse_i32(fs, fl));

        // col 16: afs
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        afs_col.push_back(get_or_add_local(afs_s, afs_m, p, len));

        // col 17: wksi
        pos = next_field(data, pos, line_end, fs, fl);
        wksi_col.push_back(parse_i32(fs, fl));

        // col 18: fye
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        fye_col.push_back(get_or_add_local(fye_s, fye_m, p, len));

        // col 19: instance (SKIP) - advance to end of line
        // (already at line_end or past it)
        pos = line_end + 1;
    }

    f.close_file();
    size_t nrows = adsh_col.size();

    // Write binary columns
    fs::create_directories(out_dir + "/sub");
    write_col_i32(out_dir+"/sub/adsh.bin",    adsh_col);
    write_col_i32(out_dir+"/sub/cik.bin",     cik_col);
    write_col_i32(out_dir+"/sub/name.bin",    name_col);
    write_col_i32(out_dir+"/sub/sic.bin",     sic_col);
    write_col_i32(out_dir+"/sub/countryba.bin",cba_col);
    write_col_i32(out_dir+"/sub/stprba.bin",  spb_col);
    write_col_i32(out_dir+"/sub/cityba.bin",  city_col);
    write_col_i32(out_dir+"/sub/countryinc.bin",cinc_col);
    write_col_i32(out_dir+"/sub/form.bin",    form_col);
    write_col_i32(out_dir+"/sub/period.bin",  period_col);
    write_col_i32(out_dir+"/sub/fy.bin",      fy_col);
    write_col_i32(out_dir+"/sub/fp.bin",      fp_col);
    write_col_i32(out_dir+"/sub/filed.bin",   filed_col);
    write_col_i32(out_dir+"/sub/prevrpt.bin", prevrpt_col);
    write_col_i32(out_dir+"/sub/nciks.bin",   nciks_col);
    write_col_i32(out_dir+"/sub/afs.bin",     afs_col);
    write_col_i32(out_dir+"/sub/wksi.bin",    wksi_col);
    write_col_i32(out_dir+"/sub/fye.bin",     fye_col);

    // Write local dict files
    write_dict(out_dir+"/sub/name_dict.txt",      name_s);
    write_dict(out_dir+"/sub/countryba_dict.txt", cba_s);
    write_dict(out_dir+"/sub/stprba_dict.txt",    spb_s);
    write_dict(out_dir+"/sub/cityba_dict.txt",    city_s);
    write_dict(out_dir+"/sub/countryinc_dict.txt",cinc_s);
    write_dict(out_dir+"/sub/form_dict.txt",      form_s);
    write_dict(out_dir+"/sub/fp_dict.txt",        fp_s);
    write_dict(out_dir+"/sub/afs_dict.txt",       afs_s);
    write_dict(out_dir+"/sub/fye_dict.txt",       fye_s);

    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double,std::milli>(t1-t0).count();
    std::cout << "[sub] " << nrows << " rows in " << ms << "ms; adsh dict=" << adsh_strings.size() << "\n";
}

// ─── TAG ingestion ─────────────────────────────────────────────────────────────
// Schema: tag,version,custom,abstract,datatype,iord,crdr,tlabel,doc(skip)
void ingest_tag(const std::string& csv_path, const std::string& out_dir)
{
    auto t0 = std::chrono::steady_clock::now();
    MmapFile f; if (!f.open(csv_path)) return;
    const char* data = f.data; size_t sz = f.size;

    std::vector<std::string> dt_s;  std::unordered_map<std::string,int32_t> dt_m;
    std::vector<std::string> io_s;  std::unordered_map<std::string,int32_t> io_m;
    std::vector<std::string> cr_s;  std::unordered_map<std::string,int32_t> cr_m;
    std::vector<std::string> tl_s;  std::unordered_map<std::string,int32_t> tl_m;

    std::vector<int32_t> tag_col, ver_col, custom_col, abstract_col,
                          dt_col, io_col, cr_col, tl_col;
    const size_t RESERVE = 1100000;
    tag_col.reserve(RESERVE); ver_col.reserve(RESERVE); custom_col.reserve(RESERVE);
    abstract_col.reserve(RESERVE); dt_col.reserve(RESERVE); io_col.reserve(RESERVE);
    cr_col.reserve(RESERVE); tl_col.reserve(RESERVE);

    // Skip header
    size_t pos = 0;
    while (pos < sz && data[pos] != '\n') ++pos;
    ++pos;

    while (pos < sz) {
        size_t line_end = pos;
        while (line_end < sz && data[line_end] != '\n') ++line_end;
        if (line_end == pos) { pos = line_end+1; continue; }

        const char* fs; size_t fl; size_t len; const char* p;

        // col 0: tag (shared)
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        tag_col.push_back(get_or_add(tag_strings, tag_map, p, len));

        // col 1: version (shared)
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        ver_col.push_back(get_or_add(version_strings, version_map, p, len));

        // col 2: custom
        pos = next_field(data, pos, line_end, fs, fl);
        custom_col.push_back(parse_i32(fs, fl));

        // col 3: abstract
        pos = next_field(data, pos, line_end, fs, fl);
        abstract_col.push_back(parse_i32(fs, fl));

        // col 4: datatype
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        dt_col.push_back(get_or_add_local(dt_s, dt_m, p, len));

        // col 5: iord
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        io_col.push_back(get_or_add_local(io_s, io_m, p, len));

        // col 6: crdr
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        cr_col.push_back(get_or_add_local(cr_s, cr_m, p, len));

        // col 7: tlabel (local dict)
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        tl_col.push_back(get_or_add_local(tl_s, tl_m, p, len));

        // col 8: doc (SKIP)
        pos = line_end + 1;
    }

    f.close_file();
    size_t nrows = tag_col.size();

    fs::create_directories(out_dir + "/tag");
    write_col_i32(out_dir+"/tag/tag.bin",      tag_col);
    write_col_i32(out_dir+"/tag/version.bin",  ver_col);
    write_col_i32(out_dir+"/tag/custom.bin",   custom_col);
    write_col_i32(out_dir+"/tag/abstract.bin", abstract_col);
    write_col_i32(out_dir+"/tag/datatype.bin", dt_col);
    write_col_i32(out_dir+"/tag/iord.bin",     io_col);
    write_col_i32(out_dir+"/tag/crdr.bin",     cr_col);
    write_col_i32(out_dir+"/tag/tlabel.bin",   tl_col);

    write_dict(out_dir+"/tag/datatype_dict.txt", dt_s);
    write_dict(out_dir+"/tag/iord_dict.txt",     io_s);
    write_dict(out_dir+"/tag/crdr_dict.txt",     cr_s);
    write_dict(out_dir+"/tag/tlabel_dict.txt",   tl_s);

    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double,std::milli>(t1-t0).count();
    std::cout << "[tag] " << nrows << " rows in " << ms << "ms; tag_dict=" << tag_strings.size()
              << " version_dict=" << version_strings.size() << "\n";
}

// ─── NUM ingestion ─────────────────────────────────────────────────────────────
// Schema: adsh,tag,version,ddate,qtrs,uom,coreg,value,footnote(skip)
void ingest_num(const std::string& csv_path, const std::string& out_dir)
{
    auto t0 = std::chrono::steady_clock::now();
    MmapFile f; if (!f.open(csv_path)) return;
    const char* data = f.data; size_t sz = f.size;

    std::vector<std::string> uom_s;  std::unordered_map<std::string,int32_t> uom_m;
    std::vector<std::string> co_s;   std::unordered_map<std::string,int32_t> co_m;

    const size_t RESERVE = 40000000;
    std::vector<int32_t> adsh_col, tag_col, ver_col, ddate_col, qtrs_col, uom_col, coreg_col;
    std::vector<double>  value_col;

    adsh_col.reserve(RESERVE); tag_col.reserve(RESERVE); ver_col.reserve(RESERVE);
    ddate_col.reserve(RESERVE); qtrs_col.reserve(RESERVE);
    uom_col.reserve(RESERVE); coreg_col.reserve(RESERVE); value_col.reserve(RESERVE);

    // Skip header
    size_t pos = 0;
    while (pos < sz && data[pos] != '\n') ++pos;
    ++pos;

    size_t row = 0;
    while (pos < sz) {
        size_t line_end = pos;
        while (line_end < sz && data[line_end] != '\n') ++line_end;
        if (line_end == pos) { pos = line_end+1; continue; }

        const char* fs; size_t fl; size_t len; const char* p;

        // col 0: adsh (shared)
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        adsh_col.push_back(get_or_add(adsh_strings, adsh_map, p, len));

        // col 1: tag (shared)
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        tag_col.push_back(get_or_add(tag_strings, tag_map, p, len));

        // col 2: version (shared)
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        ver_col.push_back(get_or_add(version_strings, version_map, p, len));

        // col 3: ddate (YYYYMMDD integer, no epoch conversion)
        pos = next_field(data, pos, line_end, fs, fl);
        ddate_col.push_back(parse_i32(fs, fl));

        // col 4: qtrs
        pos = next_field(data, pos, line_end, fs, fl);
        qtrs_col.push_back(parse_i32(fs, fl));

        // col 5: uom (local dict)
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        uom_col.push_back(get_or_add_local(uom_s, uom_m, p, len));

        // col 6: coreg (local dict, empty = code 0)
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        coreg_col.push_back(get_or_add_local(co_s, co_m, p, len));

        // col 7: value (double, empty = NaN)
        pos = next_field(data, pos, line_end, fs, fl);
        value_col.push_back(parse_dbl(fs, fl));

        // col 8: footnote (SKIP)
        pos = line_end + 1;
        ++row;

        if (row % 5000000 == 0)
            std::cout << "  [num] " << row << " rows...\n" << std::flush;
    }

    f.close_file();
    size_t nrows = adsh_col.size();

    fs::create_directories(out_dir + "/num");
    write_col_i32(out_dir+"/num/adsh.bin",   adsh_col);
    write_col_i32(out_dir+"/num/tag.bin",    tag_col);
    write_col_i32(out_dir+"/num/version.bin",ver_col);
    write_col_i32(out_dir+"/num/ddate.bin",  ddate_col);
    write_col_i32(out_dir+"/num/qtrs.bin",   qtrs_col);
    write_col_i32(out_dir+"/num/uom.bin",    uom_col);
    write_col_i32(out_dir+"/num/coreg.bin",  coreg_col);
    write_col_dbl(out_dir+"/num/value.bin",  value_col);

    write_dict(out_dir+"/num/uom_dict.txt",   uom_s);
    write_dict(out_dir+"/num/coreg_dict.txt", co_s);

    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double,std::milli>(t1-t0).count();
    std::cout << "[num] " << nrows << " rows in " << ms << "ms\n";
}

// ─── PRE ingestion ─────────────────────────────────────────────────────────────
// Schema: adsh,report,line,stmt,inpth,rfile,tag,version,plabel,negating
void ingest_pre(const std::string& csv_path, const std::string& out_dir)
{
    auto t0 = std::chrono::steady_clock::now();
    MmapFile f; if (!f.open(csv_path)) return;
    const char* data = f.data; size_t sz = f.size;

    std::vector<std::string> stmt_s; std::unordered_map<std::string,int32_t> stmt_m;
    std::vector<std::string> rf_s;   std::unordered_map<std::string,int32_t> rf_m;
    std::vector<std::string> pl_s;   std::unordered_map<std::string,int32_t> pl_m;

    const size_t RESERVE = 10000000;
    std::vector<int32_t> adsh_col, report_col, line_col, stmt_col, inpth_col,
                          rfile_col, tag_col, ver_col, plabel_col, negating_col;

    adsh_col.reserve(RESERVE); report_col.reserve(RESERVE); line_col.reserve(RESERVE);
    stmt_col.reserve(RESERVE); inpth_col.reserve(RESERVE); rfile_col.reserve(RESERVE);
    tag_col.reserve(RESERVE); ver_col.reserve(RESERVE);
    plabel_col.reserve(RESERVE); negating_col.reserve(RESERVE);

    // Skip header
    size_t pos = 0;
    while (pos < sz && data[pos] != '\n') ++pos;
    ++pos;

    size_t row = 0;
    while (pos < sz) {
        size_t line_end = pos;
        while (line_end < sz && data[line_end] != '\n') ++line_end;
        if (line_end == pos) { pos = line_end+1; continue; }

        const char* fs; size_t fl; size_t len; const char* p;

        // col 0: adsh (shared)
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        adsh_col.push_back(get_or_add(adsh_strings, adsh_map, p, len));

        // col 1: report
        pos = next_field(data, pos, line_end, fs, fl);
        report_col.push_back(parse_i32(fs, fl));

        // col 2: line
        pos = next_field(data, pos, line_end, fs, fl);
        line_col.push_back(parse_i32(fs, fl));

        // col 3: stmt (local dict)
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        stmt_col.push_back(get_or_add_local(stmt_s, stmt_m, p, len));

        // col 4: inpth
        pos = next_field(data, pos, line_end, fs, fl);
        inpth_col.push_back(parse_i32(fs, fl));

        // col 5: rfile (local dict)
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        rfile_col.push_back(get_or_add_local(rf_s, rf_m, p, len));

        // col 6: tag (shared)
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        tag_col.push_back(get_or_add(tag_strings, tag_map, p, len));

        // col 7: version (shared)
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        ver_col.push_back(get_or_add(version_strings, version_map, p, len));

        // col 8: plabel (local dict)
        pos = next_field(data, pos, line_end, fs, fl);
        p = field_content(fs, fl, len);
        plabel_col.push_back(get_or_add_local(pl_s, pl_m, p, len));

        // col 9: negating
        pos = next_field(data, pos, line_end, fs, fl);
        negating_col.push_back(parse_i32(fs, fl));

        pos = line_end + 1;
        ++row;

        if (row % 2000000 == 0)
            std::cout << "  [pre] " << row << " rows...\n" << std::flush;
    }

    f.close_file();
    size_t nrows = adsh_col.size();

    fs::create_directories(out_dir + "/pre");
    write_col_i32(out_dir+"/pre/adsh.bin",    adsh_col);
    write_col_i32(out_dir+"/pre/report.bin",  report_col);
    write_col_i32(out_dir+"/pre/line.bin",    line_col);
    write_col_i32(out_dir+"/pre/stmt.bin",    stmt_col);
    write_col_i32(out_dir+"/pre/inpth.bin",   inpth_col);
    write_col_i32(out_dir+"/pre/rfile.bin",   rfile_col);
    write_col_i32(out_dir+"/pre/tag.bin",     tag_col);
    write_col_i32(out_dir+"/pre/version.bin", ver_col);
    write_col_i32(out_dir+"/pre/plabel.bin",  plabel_col);
    write_col_i32(out_dir+"/pre/negating.bin",negating_col);

    write_dict(out_dir+"/pre/stmt_dict.txt",  stmt_s);
    write_dict(out_dir+"/pre/rfile_dict.txt", rf_s);
    write_dict(out_dir+"/pre/plabel_dict.txt",pl_s);

    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double,std::milli>(t1-t0).count();
    std::cout << "[pre] " << nrows << " rows in " << ms << "ms\n";
}

// Write shared dict files to each table directory that uses them
static void write_shared_dicts(const std::string& out_dir) {
    // Write adsh dict to sub, num, pre directories
    for (const auto& tbl : {"sub","num","pre"})
        write_dict(out_dir + "/" + tbl + "/adsh_dict.txt", adsh_strings);
    // Write tag dict to num, pre, tag directories
    for (const auto& tbl : {"num","pre","tag"})
        write_dict(out_dir + "/" + tbl + "/tag_dict.txt", tag_strings);
    // Write version dict to num, pre, tag directories
    for (const auto& tbl : {"num","pre","tag"})
        write_dict(out_dir + "/" + tbl + "/version_dict.txt", version_strings);

    std::cout << "[shared dicts] adsh=" << adsh_strings.size()
              << " tag=" << tag_strings.size()
              << " version=" << version_strings.size() << "\n";
}

// ─── main ──────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: ingest <input_dir> <output_dir>\n";
        return 1;
    }
    std::string in_dir  = argv[1];
    std::string out_dir = argv[2];
    fs::create_directories(out_dir);

    auto t_total = std::chrono::steady_clock::now();

    // Pre-reserve shared dict maps
    adsh_map.reserve(100000);
    tag_map.reserve(60000);
    version_map.reserve(120000);
    adsh_strings.reserve(100000);
    tag_strings.reserve(60000);
    version_strings.reserve(120000);

    // Process in order: sub -> tag -> num -> pre
    // sub first: builds adsh dict so adsh_code == sub_row_index
    // tag second: builds tag/version dicts
    std::cout << "=== Ingesting sub ===\n";
    ingest_sub(in_dir + "/sub.csv", out_dir);

    std::cout << "=== Ingesting tag ===\n";
    ingest_tag(in_dir + "/tag.csv", out_dir);

    std::cout << "=== Ingesting num ===\n";
    ingest_num(in_dir + "/num.csv", out_dir);

    std::cout << "=== Ingesting pre ===\n";
    ingest_pre(in_dir + "/pre.csv", out_dir);

    std::cout << "=== Writing shared dicts ===\n";
    write_shared_dicts(out_dir);

    auto t_end = std::chrono::steady_clock::now();
    double total_ms = std::chrono::duration<double,std::milli>(t_end-t_total).count();
    std::cout << "=== Ingestion complete in " << total_ms/1000.0 << "s ===\n";
    return 0;
}
