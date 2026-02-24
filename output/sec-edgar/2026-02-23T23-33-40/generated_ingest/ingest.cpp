// ingest.cpp — SEC EDGAR Binary Columnar Ingestion
// Usage: ./ingest <data_dir> <out_dir>
//
// Two-phase approach:
//   Phase 1: Scan all CSVs to build global shared dictionaries (adsh, tag, version)
//            and per-table dictionaries for other string columns.
//   Phase 2: Parse each table, write binary column files.
//            Parallel chunked reading for large tables (num, pre).
//
// Output layout under <out_dir>:
//   sub/  num/  tag/  pre/  indexes/
//   Each column: <table>/<column>.bin
//   Dict files:  <table>/<column>_dict.txt  (line N = code N, 0-indexed)
//   Zone map:    indexes/num_ddate_zone_map.bin

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <cmath>
#include <chrono>
#include <limits>
#include <filesystem>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

namespace fs = std::filesystem;
using clk = std::chrono::steady_clock;

static double elapsed_s(clk::time_point t0) {
    return std::chrono::duration<double>(clk::now() - t0).count();
}

// ============================================================
// Dictionary types
// ============================================================

struct Dict32 {
    std::unordered_map<std::string, int32_t> to_code;
    std::vector<std::string>                 to_str;

    int32_t insert(const std::string& s) {
        auto it = to_code.find(s);
        if (it != to_code.end()) return it->second;
        int32_t code = (int32_t)to_str.size();
        to_code[s]   = code;
        to_str.push_back(s);
        return code;
    }
    int32_t lookup(const std::string& s) const {
        auto it = to_code.find(s);
        return (it != to_code.end()) ? it->second : 0;
    }
    void write(const std::string& path) const {
        std::ofstream f(path);
        for (const auto& s : to_str) f << s << "\n";
        std::cerr << "  dict " << path << " (" << to_str.size() << " entries)\n";
    }
};

struct Dict16 {
    std::unordered_map<std::string, int16_t> to_code;
    std::vector<std::string>                 to_str;

    int16_t insert(const std::string& s) {
        auto it = to_code.find(s);
        if (it != to_code.end()) return it->second;
        int16_t code = (int16_t)to_str.size();
        to_code[s]   = code;
        to_str.push_back(s);
        return code;
    }
    int16_t lookup(const std::string& s) const {
        auto it = to_code.find(s);
        return (it != to_code.end()) ? it->second : 0;
    }
    void write(const std::string& path) const {
        std::ofstream f(path);
        for (const auto& s : to_str) f << s << "\n";
        std::cerr << "  dict " << path << " (" << to_str.size() << " entries)\n";
    }
};

// ============================================================
// Global shared dictionaries (cross-table consistency)
// ============================================================
Dict32 g_adsh_dict;    // shared: sub.adsh, num.adsh, pre.adsh
Dict32 g_tag_dict;     // shared: num.tag, pre.tag, tag.tag
Dict32 g_version_dict; // shared: num.version, pre.version, tag.version

// Per-table local dictionaries
Dict32 g_name_dict;
Dict16 g_countryba_dict, g_stprba_dict, g_cityba_dict, g_countryinc_dict;
Dict16 g_form_dict, g_fp_dict, g_afs_dict, g_fye_dict;
Dict16 g_uom_dict;
Dict32 g_coreg_dict;
Dict16 g_datatype_dict, g_iord_dict, g_crdr_dict;
Dict32 g_tlabel_dict;
Dict16 g_stmt_dict, g_rfile_dict;
Dict32 g_plabel_dict;

// ============================================================
// mmap helper
// ============================================================
struct MmapFile {
    char*  data = nullptr;
    size_t size = 0;
    int    fd   = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::cerr << "Cannot open " << path << "\n"; return false; }
        struct stat st; fstat(fd, &st);
        size = st.st_size;
        if (size == 0) return true;
        data = (char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { data = nullptr; perror("mmap"); return false; }
        madvise(data, size, MADV_SEQUENTIAL);
        return true;
    }
    ~MmapFile() {
        if (data && size) munmap(data, size);
        if (fd >= 0) close(fd);
    }
};

// ============================================================
// CSV row parser (handles RFC 4180 quoting)
// Returns pointer just past end of the parsed line
// ============================================================
static const char* parse_csv_row(const char* p, const char* end,
                                  std::vector<std::string>& fields)
{
    fields.clear();
    if (p >= end) return end;

    // skip any leading newlines (shouldn't normally occur mid-chunk)
    while (p < end && (*p == '\r' || *p == '\n')) ++p;
    if (p >= end) return end;

    while (true) {
        std::string field;
        bool done_row = false;

        if (*p == '"') {
            ++p; // skip opening quote
            while (p < end) {
                if (*p == '"') {
                    ++p;
                    if (p < end && *p == '"') { field += '"'; ++p; } // escaped ""
                    else break; // closing quote
                } else { field += *p++; }
            }
            if (p >= end || *p == '\r' || *p == '\n') done_row = true;
            else if (*p == ',') ++p;
        } else {
            while (p < end && *p != ',' && *p != '\r' && *p != '\n') field += *p++;
            if (p >= end || *p == '\r' || *p == '\n') done_row = true;
            else ++p; // skip ','
        }

        fields.push_back(std::move(field));
        if (done_row) break;
    }

    // skip line terminator(s)
    while (p < end && (*p == '\r' || *p == '\n')) ++p;
    return p;
}

// Skip the header line, return pointer to first data row
static const char* skip_header(const char* data, size_t size) {
    const char* p   = data;
    const char* end = data + size;
    while (p < end && *p != '\n') ++p;
    return (p < end) ? p + 1 : end;
}

// Split [data_start, end) into n roughly equal chunks aligned to line boundaries
static std::vector<std::pair<const char*, const char*>>
split_chunks(const char* data_start, const char* end, int n)
{
    std::vector<std::pair<const char*, const char*>> chunks;
    size_t total = end - data_start;
    const char* p = data_start;

    for (int i = 0; i < n; ++i) {
        const char* chunk_start = p;
        if (i == n - 1 || p >= end) {
            chunks.push_back({chunk_start, end});
            break;
        }
        const char* target = data_start + (size_t)(i + 1) * total / n;
        if (target >= end) { chunks.push_back({chunk_start, end}); break; }
        while (target < end && *target != '\n') ++target;
        if (target < end) ++target; // skip the '\n'
        chunks.push_back({chunk_start, target});
        p = target;
    }
    return chunks;
}

// ============================================================
// Binary write helpers
// ============================================================
template<typename T>
static void write_bin(const std::string& path, const std::vector<T>& data) {
    std::ofstream f(path, std::ios::binary);
    if (!data.empty())
        f.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T));
    std::cerr << "  wrote " << path << " (" << data.size() << " rows)\n";
}

template<typename T>
static void merge_and_write(const std::string& path,
                             std::vector<std::vector<T>>& per_thread)
{
    size_t total = 0;
    for (auto& v : per_thread) total += v.size();
    std::vector<T> merged;
    merged.reserve(total);
    for (auto& v : per_thread) {
        merged.insert(merged.end(), v.begin(), v.end());
        std::vector<T>().swap(v); // free immediately
    }
    write_bin(path, merged);
}

// ============================================================
// Phase 1: Build all dictionaries from full CSV scans
// ============================================================
static void phase1_build_dicts(const std::string& data_dir)
{
    auto t0 = clk::now();
    std::cerr << "[Phase 1] Building dictionaries...\n";

    // coreg empty string = NULL, always code 0
    g_coreg_dict.insert("");

    // --- sub.csv ---
    // cols: adsh(0),cik(1),name(2),sic(3),countryba(4),stprba(5),cityba(6),
    //       countryinc(7),form(8),period(9),fy(10),fp(11),filed(12),accepted(13),
    //       prevrpt(14),nciks(15),afs(16),wksi(17),fye(18),instance(19)
    {
        MmapFile mf; mf.open(data_dir + "/sub.csv");
        const char* p   = skip_header(mf.data, mf.size);
        const char* end = mf.data + mf.size;
        std::vector<std::string> flds;
        while (p < end) {
            p = parse_csv_row(p, end, flds);
            if (flds.size() < 19) continue;
            g_adsh_dict.insert(flds[0]);
            g_name_dict.insert(flds[2]);
            g_countryba_dict.insert(flds[4]);
            g_stprba_dict.insert(flds[5]);
            g_cityba_dict.insert(flds[6]);
            g_countryinc_dict.insert(flds[7]);
            g_form_dict.insert(flds[8]);
            g_fp_dict.insert(flds[11]);
            g_afs_dict.insert(flds[16]);
            g_fye_dict.insert(flds[18]);
        }
        std::cerr << "  sub dict: adsh=" << g_adsh_dict.to_str.size()
                  << " (" << elapsed_s(t0) << "s)\n";
    }

    // --- tag.csv ---
    // cols: tag(0),version(1),custom(2),abstract(3),datatype(4),iord(5),crdr(6),tlabel(7),doc(8)
    {
        MmapFile mf; mf.open(data_dir + "/tag.csv");
        const char* p   = skip_header(mf.data, mf.size);
        const char* end = mf.data + mf.size;
        std::vector<std::string> flds;
        while (p < end) {
            p = parse_csv_row(p, end, flds);
            if (flds.size() < 8) continue;
            g_tag_dict.insert(flds[0]);
            g_version_dict.insert(flds[1]);
            g_datatype_dict.insert(flds[4]);
            g_iord_dict.insert(flds[5]);
            g_crdr_dict.insert(flds[6]);
            g_tlabel_dict.insert(flds[7]);
        }
        std::cerr << "  tag dict: tag=" << g_tag_dict.to_str.size()
                  << " version=" << g_version_dict.to_str.size()
                  << " (" << elapsed_s(t0) << "s)\n";
    }

    // --- num.csv: extend global dicts, build uom/coreg ---
    // cols: adsh(0),tag(1),version(2),ddate(3),qtrs(4),uom(5),coreg(6),value(7),footnote(8)
    {
        MmapFile mf; mf.open(data_dir + "/num.csv");
        const char* p   = skip_header(mf.data, mf.size);
        const char* end = mf.data + mf.size;
        std::vector<std::string> flds;
        int64_t cnt = 0;
        while (p < end) {
            p = parse_csv_row(p, end, flds);
            if (flds.size() < 7) continue;
            g_adsh_dict.insert(flds[0]);
            g_tag_dict.insert(flds[1]);
            g_version_dict.insert(flds[2]);
            g_uom_dict.insert(flds[5]);
            g_coreg_dict.insert(flds[6]);
            ++cnt;
            if (cnt % 10000000 == 0)
                std::cerr << "  num dict: " << cnt << " rows (" << elapsed_s(t0) << "s)\n";
        }
        std::cerr << "  num dict done: " << cnt << " rows (" << elapsed_s(t0) << "s)\n";
    }

    // --- pre.csv: extend global dicts, build stmt/rfile/plabel ---
    // cols: adsh(0),report(1),line(2),stmt(3),inpth(4),rfile(5),tag(6),version(7),plabel(8),negating(9)
    {
        MmapFile mf; mf.open(data_dir + "/pre.csv");
        const char* p   = skip_header(mf.data, mf.size);
        const char* end = mf.data + mf.size;
        std::vector<std::string> flds;
        int64_t cnt = 0;
        while (p < end) {
            p = parse_csv_row(p, end, flds);
            if (flds.size() < 9) continue;
            g_adsh_dict.insert(flds[0]);
            g_stmt_dict.insert(flds[3]);
            g_rfile_dict.insert(flds[5]);
            g_tag_dict.insert(flds[6]);
            g_version_dict.insert(flds[7]);
            g_plabel_dict.insert(flds[8]);
            ++cnt;
            if (cnt % 5000000 == 0)
                std::cerr << "  pre dict: " << cnt << " rows (" << elapsed_s(t0) << "s)\n";
        }
        std::cerr << "  pre dict done: " << cnt << " rows (" << elapsed_s(t0) << "s)\n";
    }

    std::cerr << "[Phase 1] done in " << elapsed_s(t0) << "s\n"
              << "  adsh=" << g_adsh_dict.to_str.size()
              << " tag="   << g_tag_dict.to_str.size()
              << " version="<< g_version_dict.to_str.size()
              << " coreg=" << g_coreg_dict.to_str.size()
              << " plabel="<< g_plabel_dict.to_str.size() << "\n\n";
}

// ============================================================
// Phase 2a: ingest sub (small — single-threaded)
// ============================================================
static void ingest_sub(const std::string& data_dir, const std::string& out_dir)
{
    auto t0 = clk::now();
    std::cerr << "[ingest_sub] starting...\n";
    std::string tdir = out_dir + "/sub";
    fs::create_directories(tdir);

    std::vector<int32_t> col_adsh, col_cik, col_name, col_sic;
    std::vector<int32_t> col_period, col_fy, col_filed, col_prevrpt, col_nciks, col_wksi;
    std::vector<int16_t> col_countryba, col_stprba, col_cityba, col_countryinc;
    std::vector<int16_t> col_form, col_fp, col_afs, col_fye;

    MmapFile mf; mf.open(data_dir + "/sub.csv");
    const char* p   = skip_header(mf.data, mf.size);
    const char* end = mf.data + mf.size;
    std::vector<std::string> flds;

    while (p < end) {
        p = parse_csv_row(p, end, flds);
        if (flds.size() < 19) continue;

        col_adsh.push_back(g_adsh_dict.lookup(flds[0]));
        col_cik.push_back(flds[1].empty() ? 0 : std::stoi(flds[1]));
        col_name.push_back(g_name_dict.lookup(flds[2]));
        col_sic.push_back(flds[3].empty() ? 0 : std::stoi(flds[3]));
        col_countryba.push_back(g_countryba_dict.lookup(flds[4]));
        col_stprba.push_back(g_stprba_dict.lookup(flds[5]));
        col_cityba.push_back(g_cityba_dict.lookup(flds[6]));
        col_countryinc.push_back(g_countryinc_dict.lookup(flds[7]));
        col_form.push_back(g_form_dict.lookup(flds[8]));
        col_period.push_back(flds[9].empty() ? 0 : std::stoi(flds[9]));
        col_fy.push_back(flds[10].empty() ? 0 : std::stoi(flds[10]));
        col_fp.push_back(g_fp_dict.lookup(flds[11]));
        col_filed.push_back(flds[12].empty() ? 0 : std::stoi(flds[12]));
        // flds[13] = accepted (skipped)
        col_prevrpt.push_back(flds[14].empty() ? 0 : std::stoi(flds[14]));
        col_nciks.push_back(flds[15].empty() ? 0 : std::stoi(flds[15]));
        col_afs.push_back(g_afs_dict.lookup(flds[16]));
        col_wksi.push_back(flds[17].empty() ? 0 : std::stoi(flds[17]));
        col_fye.push_back(g_fye_dict.lookup(flds[18]));
        // flds[19] = instance (skipped if present)
    }

    write_bin(tdir + "/adsh.bin",       col_adsh);
    write_bin(tdir + "/cik.bin",        col_cik);
    write_bin(tdir + "/name.bin",       col_name);
    write_bin(tdir + "/sic.bin",        col_sic);
    write_bin(tdir + "/countryba.bin",  col_countryba);
    write_bin(tdir + "/stprba.bin",     col_stprba);
    write_bin(tdir + "/cityba.bin",     col_cityba);
    write_bin(tdir + "/countryinc.bin", col_countryinc);
    write_bin(tdir + "/form.bin",       col_form);
    write_bin(tdir + "/period.bin",     col_period);
    write_bin(tdir + "/fy.bin",         col_fy);
    write_bin(tdir + "/fp.bin",         col_fp);
    write_bin(tdir + "/filed.bin",      col_filed);
    write_bin(tdir + "/prevrpt.bin",    col_prevrpt);
    write_bin(tdir + "/nciks.bin",      col_nciks);
    write_bin(tdir + "/afs.bin",        col_afs);
    write_bin(tdir + "/wksi.bin",       col_wksi);
    write_bin(tdir + "/fye.bin",        col_fye);

    g_adsh_dict.write(tdir + "/adsh_dict.txt");
    g_name_dict.write(tdir + "/name_dict.txt");
    g_countryba_dict.write(tdir + "/countryba_dict.txt");
    g_stprba_dict.write(tdir + "/stprba_dict.txt");
    g_cityba_dict.write(tdir + "/cityba_dict.txt");
    g_countryinc_dict.write(tdir + "/countryinc_dict.txt");
    g_form_dict.write(tdir + "/form_dict.txt");
    g_fp_dict.write(tdir + "/fp_dict.txt");
    g_afs_dict.write(tdir + "/afs_dict.txt");
    g_fye_dict.write(tdir + "/fye_dict.txt");

    std::cerr << "[ingest_sub] done: " << col_adsh.size()
              << " rows in " << elapsed_s(t0) << "s\n\n";
}

// ============================================================
// Phase 2b: ingest tag (medium — single-threaded)
// ============================================================
static void ingest_tag(const std::string& data_dir, const std::string& out_dir)
{
    auto t0 = clk::now();
    std::cerr << "[ingest_tag] starting...\n";
    std::string tdir = out_dir + "/tag";
    fs::create_directories(tdir);

    std::vector<int32_t> col_tag, col_version, col_custom, col_abstract, col_tlabel;
    std::vector<int16_t> col_datatype, col_iord, col_crdr;

    MmapFile mf; mf.open(data_dir + "/tag.csv");
    const char* p   = skip_header(mf.data, mf.size);
    const char* end = mf.data + mf.size;
    std::vector<std::string> flds;

    while (p < end) {
        p = parse_csv_row(p, end, flds);
        if (flds.size() < 8) continue;

        col_tag.push_back(g_tag_dict.lookup(flds[0]));
        col_version.push_back(g_version_dict.lookup(flds[1]));
        col_custom.push_back(flds[2].empty() ? 0 : std::stoi(flds[2]));
        col_abstract.push_back(flds[3].empty() ? 0 : std::stoi(flds[3]));
        col_datatype.push_back(g_datatype_dict.lookup(flds[4]));
        col_iord.push_back(g_iord_dict.lookup(flds[5]));
        col_crdr.push_back(g_crdr_dict.lookup(flds[6]));
        col_tlabel.push_back(g_tlabel_dict.lookup(flds[7]));
        // flds[8] = doc (skipped)
    }

    write_bin(tdir + "/tag.bin",      col_tag);
    write_bin(tdir + "/version.bin",  col_version);
    write_bin(tdir + "/custom.bin",   col_custom);
    write_bin(tdir + "/abstract.bin", col_abstract);
    write_bin(tdir + "/datatype.bin", col_datatype);
    write_bin(tdir + "/iord.bin",     col_iord);
    write_bin(tdir + "/crdr.bin",     col_crdr);
    write_bin(tdir + "/tlabel.bin",   col_tlabel);

    g_tag_dict.write(tdir + "/tag_dict.txt");
    g_version_dict.write(tdir + "/version_dict.txt");
    g_datatype_dict.write(tdir + "/datatype_dict.txt");
    g_iord_dict.write(tdir + "/iord_dict.txt");
    g_crdr_dict.write(tdir + "/crdr_dict.txt");
    g_tlabel_dict.write(tdir + "/tlabel_dict.txt");

    std::cerr << "[ingest_tag] done: " << col_tag.size()
              << " rows in " << elapsed_s(t0) << "s\n\n";
}

// ============================================================
// Phase 2c: ingest num (large — parallel chunked)
// ============================================================
static void ingest_num(const std::string& data_dir, const std::string& out_dir)
{
    auto t0 = clk::now();
    std::cerr << "[ingest_num] starting (parallel)...\n";
    std::string tdir = out_dir + "/num";
    fs::create_directories(tdir);
    fs::create_directories(out_dir + "/indexes");

    MmapFile mf; mf.open(data_dir + "/num.csv");
    const char* data_start = skip_header(mf.data, mf.size);
    const char* end_ptr    = mf.data + mf.size;

    int nthreads = omp_get_max_threads();
    auto chunks  = split_chunks(data_start, end_ptr, nthreads);
    int  nc      = (int)chunks.size();

    std::vector<std::vector<int32_t>> t_adsh(nc), t_tag(nc), t_version(nc);
    std::vector<std::vector<int32_t>> t_ddate(nc), t_qtrs(nc), t_coreg(nc);
    std::vector<std::vector<int16_t>> t_uom(nc);
    std::vector<std::vector<double>>  t_value(nc);

    const double NAN_VAL = std::numeric_limits<double>::quiet_NaN();

    #pragma omp parallel for num_threads(nc) schedule(static)
    for (int ti = 0; ti < nc; ++ti) {
        const char* p   = chunks[ti].first;
        const char* end = chunks[ti].second;
        std::vector<std::string> flds;
        flds.reserve(9);

        while (p < end) {
            p = parse_csv_row(p, end, flds);
            if (flds.size() < 7) continue;

            t_adsh[ti].push_back(g_adsh_dict.lookup(flds[0]));
            t_tag[ti].push_back(g_tag_dict.lookup(flds[1]));
            t_version[ti].push_back(g_version_dict.lookup(flds[2]));
            t_ddate[ti].push_back(flds[3].empty() ? 0 : std::stoi(flds[3]));
            t_qtrs[ti].push_back(flds[4].empty() ? 0 : std::stoi(flds[4]));
            t_uom[ti].push_back(g_uom_dict.lookup(flds[5]));
            t_coreg[ti].push_back(g_coreg_dict.lookup(flds[6]));
            double v = flds[7].empty() ? NAN_VAL : std::stod(flds[7]);
            t_value[ti].push_back(v);
            // flds[8] = footnote (skipped)
        }
    }
    std::cerr << "  parallel parse done (" << elapsed_s(t0) << "s)\n";

    // Merge adsh, tag, version, qtrs, uom, coreg, value
    merge_and_write(tdir + "/adsh.bin",    t_adsh);
    merge_and_write(tdir + "/tag.bin",     t_tag);
    merge_and_write(tdir + "/version.bin", t_version);
    merge_and_write(tdir + "/qtrs.bin",    t_qtrs);
    merge_and_write(tdir + "/uom.bin",     t_uom);
    merge_and_write(tdir + "/coreg.bin",   t_coreg);
    merge_and_write(tdir + "/value.bin",   t_value);

    // Merge ddate and build zone map
    {
        size_t total = 0;
        for (auto& v : t_ddate) total += v.size();
        std::vector<int32_t> ddate_all;
        ddate_all.reserve(total);
        for (auto& v : t_ddate) {
            ddate_all.insert(ddate_all.end(), v.begin(), v.end());
            std::vector<int32_t>().swap(v);
        }
        write_bin(tdir + "/ddate.bin", ddate_all);

        // Build zone map: blocks of 65536 rows
        const uint32_t BLOCK = 65536;
        size_t n = ddate_all.size();
        size_t num_blocks = (n + BLOCK - 1) / BLOCK;
        std::string zm_path = out_dir + "/indexes/num_ddate_zone_map.bin";
        std::ofstream zm(zm_path, std::ios::binary);
        uint32_t nb = (uint32_t)num_blocks;
        zm.write(reinterpret_cast<const char*>(&nb), 4);
        for (size_t b = 0; b < num_blocks; ++b) {
            size_t r0  = b * BLOCK;
            size_t r1  = std::min(r0 + BLOCK, n);
            int32_t mn = ddate_all[r0], mx = ddate_all[r0];
            for (size_t r = r0 + 1; r < r1; ++r) {
                if (ddate_all[r] < mn) mn = ddate_all[r];
                if (ddate_all[r] > mx) mx = ddate_all[r];
            }
            uint32_t cnt = (uint32_t)(r1 - r0);
            zm.write(reinterpret_cast<const char*>(&mn),  4);
            zm.write(reinterpret_cast<const char*>(&mx),  4);
            zm.write(reinterpret_cast<const char*>(&cnt), 4);
        }
        std::cerr << "  zone map: " << num_blocks << " blocks -> " << zm_path << "\n";
    }

    // Write shared dict files
    g_adsh_dict.write(tdir + "/adsh_dict.txt");
    g_tag_dict.write(tdir + "/tag_dict.txt");
    g_version_dict.write(tdir + "/version_dict.txt");
    g_uom_dict.write(tdir + "/uom_dict.txt");
    g_coreg_dict.write(tdir + "/coreg_dict.txt");

    std::cerr << "[ingest_num] done in " << elapsed_s(t0) << "s\n\n";
}

// ============================================================
// Phase 2d: ingest pre (large — parallel chunked)
// ============================================================
static void ingest_pre(const std::string& data_dir, const std::string& out_dir)
{
    auto t0 = clk::now();
    std::cerr << "[ingest_pre] starting (parallel)...\n";
    std::string tdir = out_dir + "/pre";
    fs::create_directories(tdir);

    MmapFile mf; mf.open(data_dir + "/pre.csv");
    const char* data_start = skip_header(mf.data, mf.size);
    const char* end_ptr    = mf.data + mf.size;

    int nthreads = omp_get_max_threads();
    auto chunks  = split_chunks(data_start, end_ptr, nthreads);
    int  nc      = (int)chunks.size();

    std::vector<std::vector<int32_t>> t_adsh(nc), t_report(nc), t_line(nc);
    std::vector<std::vector<int32_t>> t_inpth(nc), t_tag(nc), t_version(nc);
    std::vector<std::vector<int32_t>> t_plabel(nc), t_negating(nc);
    std::vector<std::vector<int16_t>> t_stmt(nc), t_rfile(nc);

    #pragma omp parallel for num_threads(nc) schedule(static)
    for (int ti = 0; ti < nc; ++ti) {
        const char* p   = chunks[ti].first;
        const char* end = chunks[ti].second;
        std::vector<std::string> flds;
        flds.reserve(10);

        while (p < end) {
            p = parse_csv_row(p, end, flds);
            if (flds.size() < 10) continue;

            t_adsh[ti].push_back(g_adsh_dict.lookup(flds[0]));
            t_report[ti].push_back(flds[1].empty() ? 0 : std::stoi(flds[1]));
            t_line[ti].push_back(flds[2].empty() ? 0 : std::stoi(flds[2]));
            t_stmt[ti].push_back(g_stmt_dict.lookup(flds[3]));
            t_inpth[ti].push_back(flds[4].empty() ? 0 : std::stoi(flds[4]));
            t_rfile[ti].push_back(g_rfile_dict.lookup(flds[5]));
            t_tag[ti].push_back(g_tag_dict.lookup(flds[6]));
            t_version[ti].push_back(g_version_dict.lookup(flds[7]));
            t_plabel[ti].push_back(g_plabel_dict.lookup(flds[8]));
            t_negating[ti].push_back(flds[9].empty() ? 0 : std::stoi(flds[9]));
        }
    }
    std::cerr << "  parallel parse done (" << elapsed_s(t0) << "s)\n";

    merge_and_write(tdir + "/adsh.bin",     t_adsh);
    merge_and_write(tdir + "/report.bin",   t_report);
    merge_and_write(tdir + "/line.bin",     t_line);
    merge_and_write(tdir + "/stmt.bin",     t_stmt);
    merge_and_write(tdir + "/inpth.bin",    t_inpth);
    merge_and_write(tdir + "/rfile.bin",    t_rfile);
    merge_and_write(tdir + "/tag.bin",      t_tag);
    merge_and_write(tdir + "/version.bin",  t_version);
    merge_and_write(tdir + "/plabel.bin",   t_plabel);
    merge_and_write(tdir + "/negating.bin", t_negating);

    // Write shared dict files (same content as other tables)
    g_adsh_dict.write(tdir + "/adsh_dict.txt");
    g_stmt_dict.write(tdir + "/stmt_dict.txt");
    g_rfile_dict.write(tdir + "/rfile_dict.txt");
    g_tag_dict.write(tdir + "/tag_dict.txt");
    g_version_dict.write(tdir + "/version_dict.txt");
    g_plabel_dict.write(tdir + "/plabel_dict.txt");

    std::cerr << "[ingest_pre] done in " << elapsed_s(t0) << "s\n\n";
}

// ============================================================
// main
// ============================================================
int main(int argc, char** argv)
{
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <out_dir>\n";
        return 1;
    }
    std::string data_dir = argv[1];
    std::string out_dir  = argv[2];

    fs::create_directories(out_dir);
    fs::create_directories(out_dir + "/indexes");

    auto t_global = clk::now();
    std::cerr << "=== SEC EDGAR Ingestion ===\n"
              << "  data_dir = " << data_dir << "\n"
              << "  out_dir  = " << out_dir  << "\n"
              << "  threads  = " << omp_get_max_threads() << "\n\n";

    phase1_build_dicts(data_dir);
    ingest_sub(data_dir, out_dir);
    ingest_tag(data_dir, out_dir);
    ingest_num(data_dir, out_dir);
    ingest_pre(data_dir, out_dir);

    std::cerr << "=== Total ingestion time: " << elapsed_s(t_global) << "s ===\n";
    return 0;
}
