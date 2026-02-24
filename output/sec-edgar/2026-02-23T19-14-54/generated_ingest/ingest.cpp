// ingest.cpp — SEC EDGAR SF3 binary columnar ingestion
// Reads CSV files, writes binary columns to sf3.gendb/
//
// Architecture:
//   Phase 1: Build global dictionaries (adsh, tag, version) by scanning all 4 CSV files
//   Phase 2: Parse each table and write binary columns (parallel across tables via OpenMP)
//
// Storage layout:
//   sf3.gendb/adsh_global_dict.txt   — shared adsh strings (codes = int32_t)
//   sf3.gendb/tag_global_dict.txt    — shared tag strings
//   sf3.gendb/version_global_dict.txt— shared version strings
//   sf3.gendb/num/{adsh,tag,version,ddate,qtrs,uom,value}.bin
//   sf3.gendb/sub/{adsh,cik,name,sic,fy}.bin
//   sf3.gendb/tag/{tag,version,abstract,tlabel}.bin
//   sf3.gendb/pre/{adsh,tag,version,stmt,rfile,line,plabel}.bin

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <sys/stat.h>
#include <omp.h>

// ---- CSV Parser -------------------------------------------------------
// Returns one field from f. Sets eol=true if this was the last field in row.
// Returns false if EOF.
static bool read_csv_field(FILE* f, std::string& out, bool& eol) {
    out.clear();
    int c = fgetc(f);
    if (c == EOF) { eol = true; return false; }
    if (c == '\r') { c = fgetc(f); } // skip CR
    if (c == '\n') { eol = true; return true; } // empty record (shouldn't happen)

    if (c == '"') {
        // Quoted field
        while (true) {
            c = fgetc(f);
            if (c == EOF) { eol = true; return true; }
            if (c == '"') {
                int nxt = fgetc(f);
                if (nxt == '"') { out += '"'; continue; }   // escaped quote
                if (nxt == ',') { eol = false; return true; }
                if (nxt == '\n') { eol = true; return true; }
                if (nxt == '\r') { fgetc(f); eol = true; return true; }
                if (nxt == EOF)  { eol = true; return true; }
                // malformed: closing quote not followed by delimiter
                eol = true; return true;
            }
            out += (char)c;
        }
    } else {
        // Unquoted field — handle empty field: first char is already the next delimiter
        if (c == ',') { eol = false; return true; }   // empty field, comma already read
        out += (char)c;
        while (true) {
            c = fgetc(f);
            if (c == ',')  { eol = false; return true; }
            if (c == '\n') { eol = true;  return true; }
            if (c == '\r') { fgetc(f); eol = true; return true; }
            if (c == EOF)  { eol = true;  return false; }
            out += (char)c;
        }
    }
}

// Skip rest of current CSV row
static void skip_csv_row(FILE* f) {
    std::string tmp; bool eol = false;
    while (!eol) read_csv_field(f, tmp, eol);
}

// ---- Dictionary builder -----------------------------------------------
struct DictBuilder {
    std::unordered_map<std::string, int32_t> map;
    std::vector<std::string> entries;

    int32_t intern(const std::string& s) {
        auto it = map.find(s);
        if (it != map.end()) return it->second;
        int32_t code = (int32_t)entries.size();
        map[s] = code;
        entries.push_back(s);
        return code;
    }

    int32_t get(const std::string& s) const {
        auto it = map.find(s);
        return it != map.end() ? it->second : -1;
    }

    void write_dict(const std::string& path) const {
        FILE* f = fopen(path.c_str(), "w");
        if (!f) { fprintf(stderr, "Cannot write dict %s\n", path.c_str()); return; }
        for (auto& e : entries) fprintf(f, "%s\n", e.c_str());
        fclose(f);
    }
};

// ---- Utilities --------------------------------------------------------
static void mkdir_p(const std::string& path) {
    mkdir(path.c_str(), 0755);
}

template<typename T>
static void write_column(const std::string& path, const std::vector<T>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot write %s\n", path.c_str()); return; }
    if (!data.empty()) fwrite(data.data(), sizeof(T), data.size(), f);
    fclose(f);
    printf("  Wrote %s (%zu rows)\n", path.c_str(), data.size());
}

// ---- Phase 1: Build global dictionaries --------------------------------
// Scans all 4 CSV files, collecting adsh / tag / version strings.
// Returns true on success.
static void build_global_dicts(
    const std::string& data_dir,
    DictBuilder& adsh_dict,
    DictBuilder& tag_dict,
    DictBuilder& ver_dict)
{
    printf("[Phase 1] Building global dictionaries...\n");

    // --- sub.csv: col 0 = adsh
    {
        std::string path = data_dir + "/sub.csv";
        FILE* f = fopen(path.c_str(), "r");
        if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); return; }
        setvbuf(f, nullptr, _IOFBF, 32*1024*1024);
        skip_csv_row(f); // header
        std::string field; bool eol;
        size_t rows = 0;
        while (true) {
            bool ok = read_csv_field(f, field, eol);
            if (!ok && feof(f)) break;
            if (!field.empty()) adsh_dict.intern(field);
            if (!eol) skip_csv_row(f);
            rows++;
        }
        fclose(f);
        printf("  sub.csv: %zu rows, %zu adsh codes\n", rows, adsh_dict.entries.size());
    }

    // --- num.csv: col 0=adsh, col 1=tag, col 2=version
    {
        std::string path = data_dir + "/num.csv";
        FILE* f = fopen(path.c_str(), "r");
        if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); return; }
        setvbuf(f, nullptr, _IOFBF, 64*1024*1024);
        skip_csv_row(f);
        std::string f0, f1, f2; bool eol;
        size_t rows = 0;
        while (true) {
            bool ok = read_csv_field(f, f0, eol);
            if (!ok && feof(f)) break;
            adsh_dict.intern(f0);
            if (eol) { rows++; continue; }
            read_csv_field(f, f1, eol); tag_dict.intern(f1);
            if (eol) { rows++; continue; }
            read_csv_field(f, f2, eol); ver_dict.intern(f2);
            if (!eol) skip_csv_row(f);
            rows++;
            if (rows % 5000000 == 0) printf("  num.csv: %zu rows scanned\n", rows);
        }
        fclose(f);
        printf("  num.csv: %zu rows, %zu tags, %zu versions\n",
               rows, tag_dict.entries.size(), ver_dict.entries.size());
    }

    // --- tag.csv: col 0=tag, col 1=version
    {
        std::string path = data_dir + "/tag.csv";
        FILE* f = fopen(path.c_str(), "r");
        if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); return; }
        setvbuf(f, nullptr, _IOFBF, 32*1024*1024);
        skip_csv_row(f);
        std::string f0, f1; bool eol;
        size_t rows = 0;
        while (true) {
            bool ok = read_csv_field(f, f0, eol);
            if (!ok && feof(f)) break;
            tag_dict.intern(f0);
            if (eol) { rows++; continue; }
            read_csv_field(f, f1, eol); ver_dict.intern(f1);
            if (!eol) skip_csv_row(f);
            rows++;
        }
        fclose(f);
        printf("  tag.csv: %zu rows\n", rows);
    }

    // --- pre.csv: col 0=adsh, col 6=tag, col 7=version
    {
        std::string path = data_dir + "/pre.csv";
        FILE* f = fopen(path.c_str(), "r");
        if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); return; }
        setvbuf(f, nullptr, _IOFBF, 64*1024*1024);
        skip_csv_row(f);
        std::string field; bool eol;
        size_t rows = 0;
        while (true) {
            // col 0: adsh
            bool ok = read_csv_field(f, field, eol);
            if (!ok && feof(f)) break;
            adsh_dict.intern(field);
            if (eol) { rows++; continue; }
            // skip cols 1,2,3,4,5
            for (int i = 1; i <= 5 && !eol; i++) read_csv_field(f, field, eol);
            if (eol) { rows++; continue; }
            // col 6: tag
            read_csv_field(f, field, eol); tag_dict.intern(field);
            if (eol) { rows++; continue; }
            // col 7: version
            read_csv_field(f, field, eol); ver_dict.intern(field);
            if (!eol) skip_csv_row(f);
            rows++;
            if (rows % 2000000 == 0) printf("  pre.csv: %zu rows scanned\n", rows);
        }
        fclose(f);
        printf("  pre.csv: %zu rows\n", rows);
    }

    printf("[Phase 1] Done. adsh=%zu, tag=%zu, version=%zu\n",
           adsh_dict.entries.size(), tag_dict.entries.size(), ver_dict.entries.size());
}

// ---- Phase 2a: Write NUM columns --------------------------------------
static void write_num(const std::string& data_dir, const std::string& out_dir,
                      const DictBuilder& adsh_d, const DictBuilder& tag_d, const DictBuilder& ver_d)
{
    printf("[num] Writing columns...\n");
    std::string path = data_dir + "/num.csv";
    FILE* f = fopen(path.c_str(), "r");
    if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); return; }
    setvbuf(f, nullptr, _IOFBF, 64*1024*1024);
    skip_csv_row(f);

    std::vector<int32_t> col_adsh, col_tag, col_ver, col_ddate;
    std::vector<int8_t>  col_qtrs;
    std::vector<double>  col_value;
    // local uom dict
    DictBuilder uom_d;
    std::vector<int16_t> col_uom;

    col_adsh.reserve(40000000);
    col_tag.reserve(40000000);
    col_ver.reserve(40000000);
    col_ddate.reserve(40000000);
    col_qtrs.reserve(40000000);
    col_uom.reserve(40000000);
    col_value.reserve(40000000);

    std::string fields[9]; bool eol;
    size_t rows = 0;

    while (true) {
        // Read all 9 fields: adsh,tag,version,ddate,qtrs,uom,coreg,value,footnote
        bool ok = read_csv_field(f, fields[0], eol);
        if (!ok && feof(f)) break;
        for (int i = 1; i < 9 && !eol; i++) read_csv_field(f, fields[i], eol);

        int32_t adsh_code = adsh_d.get(fields[0]);
        int32_t tag_code  = tag_d.get(fields[1]);
        int32_t ver_code  = ver_d.get(fields[2]);
        int32_t ddate     = fields[3].empty() ? 0 : (int32_t)atoi(fields[3].c_str());
        int8_t  qtrs      = fields[4].empty() ? 0 : (int8_t)atoi(fields[4].c_str());
        int16_t uom_code  = (int16_t)uom_d.intern(fields[5]);
        double  value     = fields[7].empty() ? 0.0 : atof(fields[7].c_str());

        col_adsh.push_back(adsh_code);
        col_tag.push_back(tag_code);
        col_ver.push_back(ver_code);
        col_ddate.push_back(ddate);
        col_qtrs.push_back(qtrs);
        col_uom.push_back(uom_code);
        col_value.push_back(value);

        rows++;
        if (rows % 5000000 == 0) printf("  [num] %zu rows parsed\n", rows);
    }
    fclose(f);
    printf("  [num] %zu rows total\n", rows);

    write_column(out_dir + "/num/adsh.bin",    col_adsh);
    write_column(out_dir + "/num/tag.bin",     col_tag);
    write_column(out_dir + "/num/version.bin", col_ver);
    write_column(out_dir + "/num/ddate.bin",   col_ddate);
    write_column(out_dir + "/num/qtrs.bin",    col_qtrs);
    write_column(out_dir + "/num/uom.bin",     col_uom);
    write_column(out_dir + "/num/value.bin",   col_value);
    uom_d.write_dict(out_dir + "/num/uom_dict.txt");
}

// ---- Phase 2b: Write SUB columns --------------------------------------
static void write_sub(const std::string& data_dir, const std::string& out_dir,
                      const DictBuilder& adsh_d)
{
    printf("[sub] Writing columns...\n");
    std::string path = data_dir + "/sub.csv";
    FILE* f = fopen(path.c_str(), "r");
    if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); return; }
    setvbuf(f, nullptr, _IOFBF, 8*1024*1024);
    skip_csv_row(f);

    // cols: adsh[0],cik[1],name[2],sic[3],countryba[4],stprba[5],cityba[6],
    //        countryinc[7],form[8],period[9],fy[10],fp[11],filed[12],...(rest skipped)
    DictBuilder name_d;
    std::vector<int32_t> col_adsh, col_cik, col_name, col_sic, col_fy;

    std::string fields[20]; bool eol;
    size_t rows = 0;

    while (true) {
        bool ok = read_csv_field(f, fields[0], eol);
        if (!ok && feof(f)) break;
        for (int i = 1; i < 20 && !eol; i++) read_csv_field(f, fields[i], eol);

        int32_t adsh_code = adsh_d.get(fields[0]);
        int32_t cik       = fields[1].empty() ? 0 : (int32_t)atoi(fields[1].c_str());
        int32_t name_code = (int32_t)name_d.intern(fields[2]);
        int32_t sic       = fields[3].empty() ? 0 : (int32_t)atoi(fields[3].c_str());
        int32_t fy        = fields[10].empty() ? 0 : (int32_t)atoi(fields[10].c_str());

        col_adsh.push_back(adsh_code);
        col_cik.push_back(cik);
        col_name.push_back(name_code);
        col_sic.push_back(sic);
        col_fy.push_back(fy);
        rows++;
    }
    fclose(f);
    printf("  [sub] %zu rows total\n", rows);

    write_column(out_dir + "/sub/adsh.bin", col_adsh);
    write_column(out_dir + "/sub/cik.bin",  col_cik);
    write_column(out_dir + "/sub/name.bin", col_name);
    write_column(out_dir + "/sub/sic.bin",  col_sic);
    write_column(out_dir + "/sub/fy.bin",   col_fy);
    name_d.write_dict(out_dir + "/sub/name_dict.txt");
}

// ---- Phase 2c: Write TAG columns --------------------------------------
static void write_tag(const std::string& data_dir, const std::string& out_dir,
                      const DictBuilder& tag_d, const DictBuilder& ver_d)
{
    printf("[tag] Writing columns...\n");
    std::string path = data_dir + "/tag.csv";
    FILE* f = fopen(path.c_str(), "r");
    if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); return; }
    setvbuf(f, nullptr, _IOFBF, 32*1024*1024);
    skip_csv_row(f);

    // cols: tag[0],version[1],custom[2],abstract[3],datatype[4],iord[5],crdr[6],tlabel[7],doc[8]
    DictBuilder tlabel_d;
    std::vector<int32_t> col_tag, col_ver, col_abstract, col_tlabel;

    col_tag.reserve(1100000);
    col_ver.reserve(1100000);
    col_abstract.reserve(1100000);
    col_tlabel.reserve(1100000);

    std::string field; bool eol;
    size_t rows = 0;

    while (true) {
        // col 0: tag
        bool ok = read_csv_field(f, field, eol);
        if (!ok && feof(f)) break;
        int32_t tag_code = tag_d.get(field);
        if (eol) { col_tag.push_back(tag_code); col_ver.push_back(0); col_abstract.push_back(0); col_tlabel.push_back(0); rows++; continue; }

        // col 1: version
        std::string ver_s; read_csv_field(f, ver_s, eol);
        int32_t ver_code = ver_d.get(ver_s);
        if (eol) { col_tag.push_back(tag_code); col_ver.push_back(ver_code); col_abstract.push_back(0); col_tlabel.push_back(0); rows++; continue; }

        // col 2: custom (skip)
        std::string tmp; read_csv_field(f, tmp, eol);
        if (eol) { col_tag.push_back(tag_code); col_ver.push_back(ver_code); col_abstract.push_back(0); col_tlabel.push_back(0); rows++; continue; }

        // col 3: abstract
        read_csv_field(f, tmp, eol);
        int32_t abstract_val = tmp.empty() ? 0 : (int32_t)atoi(tmp.c_str());
        if (eol) { col_tag.push_back(tag_code); col_ver.push_back(ver_code); col_abstract.push_back(abstract_val); col_tlabel.push_back(0); rows++; continue; }

        // skip cols 4,5,6
        for (int i = 4; i <= 6 && !eol; i++) read_csv_field(f, tmp, eol);
        if (eol) { col_tag.push_back(tag_code); col_ver.push_back(ver_code); col_abstract.push_back(abstract_val); col_tlabel.push_back(0); rows++; continue; }

        // col 7: tlabel
        std::string tlabel_s; read_csv_field(f, tlabel_s, eol);
        int32_t tlabel_code = (int32_t)tlabel_d.intern(tlabel_s);

        // skip rest (doc - col 8, may be multi-line quoted)
        if (!eol) skip_csv_row(f);

        col_tag.push_back(tag_code);
        col_ver.push_back(ver_code);
        col_abstract.push_back(abstract_val);
        col_tlabel.push_back(tlabel_code);
        rows++;
    }
    fclose(f);
    printf("  [tag] %zu rows total\n", rows);

    write_column(out_dir + "/tag/tag.bin",      col_tag);
    write_column(out_dir + "/tag/version.bin",  col_ver);
    write_column(out_dir + "/tag/abstract.bin", col_abstract);
    write_column(out_dir + "/tag/tlabel.bin",   col_tlabel);
    tlabel_d.write_dict(out_dir + "/tag/tlabel_dict.txt");
}

// ---- Phase 2d: Write PRE columns --------------------------------------
static void write_pre(const std::string& data_dir, const std::string& out_dir,
                      const DictBuilder& adsh_d, const DictBuilder& tag_d, const DictBuilder& ver_d)
{
    printf("[pre] Writing columns...\n");
    std::string path = data_dir + "/pre.csv";
    FILE* f = fopen(path.c_str(), "r");
    if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); return; }
    setvbuf(f, nullptr, _IOFBF, 64*1024*1024);
    skip_csv_row(f);

    // cols: adsh[0],report[1],line[2],stmt[3],inpth[4],rfile[5],tag[6],version[7],plabel[8],negating[9]
    DictBuilder stmt_d, rfile_d, plabel_d;
    std::vector<int32_t> col_adsh, col_tag, col_ver, col_line, col_plabel;
    std::vector<int16_t> col_stmt, col_rfile;

    col_adsh.reserve(10000000);
    col_tag.reserve(10000000);
    col_ver.reserve(10000000);
    col_line.reserve(10000000);
    col_plabel.reserve(10000000);
    col_stmt.reserve(10000000);
    col_rfile.reserve(10000000);

    std::string field; bool eol;
    size_t rows = 0;

    while (true) {
        // col 0: adsh
        bool ok = read_csv_field(f, field, eol);
        if (!ok && feof(f)) break;
        int32_t adsh_code = adsh_d.get(field);
        if (eol) {
            col_adsh.push_back(adsh_code); col_tag.push_back(0); col_ver.push_back(0);
            col_stmt.push_back(0); col_rfile.push_back(0); col_line.push_back(0); col_plabel.push_back(0);
            rows++; continue;
        }

        // col 1: report (skip)
        std::string tmp; read_csv_field(f, tmp, eol);
        if (eol) {
            col_adsh.push_back(adsh_code); col_tag.push_back(0); col_ver.push_back(0);
            col_stmt.push_back(0); col_rfile.push_back(0); col_line.push_back(0); col_plabel.push_back(0);
            rows++; continue;
        }

        // col 2: line
        read_csv_field(f, tmp, eol);
        int32_t line_val = tmp.empty() ? 0 : (int32_t)atoi(tmp.c_str());
        if (eol) {
            col_adsh.push_back(adsh_code); col_tag.push_back(0); col_ver.push_back(0);
            col_stmt.push_back(0); col_rfile.push_back(0); col_line.push_back(line_val); col_plabel.push_back(0);
            rows++; continue;
        }

        // col 3: stmt
        std::string stmt_s; read_csv_field(f, stmt_s, eol);
        int16_t stmt_code = (int16_t)stmt_d.intern(stmt_s);
        if (eol) {
            col_adsh.push_back(adsh_code); col_tag.push_back(0); col_ver.push_back(0);
            col_stmt.push_back(stmt_code); col_rfile.push_back(0); col_line.push_back(line_val); col_plabel.push_back(0);
            rows++; continue;
        }

        // col 4: inpth (skip)
        read_csv_field(f, tmp, eol);
        if (eol) {
            col_adsh.push_back(adsh_code); col_tag.push_back(0); col_ver.push_back(0);
            col_stmt.push_back(stmt_code); col_rfile.push_back(0); col_line.push_back(line_val); col_plabel.push_back(0);
            rows++; continue;
        }

        // col 5: rfile
        std::string rfile_s; read_csv_field(f, rfile_s, eol);
        int16_t rfile_code = (int16_t)rfile_d.intern(rfile_s);
        if (eol) {
            col_adsh.push_back(adsh_code); col_tag.push_back(0); col_ver.push_back(0);
            col_stmt.push_back(stmt_code); col_rfile.push_back(rfile_code); col_line.push_back(line_val); col_plabel.push_back(0);
            rows++; continue;
        }

        // col 6: tag
        read_csv_field(f, field, eol);
        int32_t tag_code = tag_d.get(field);
        if (eol) {
            col_adsh.push_back(adsh_code); col_tag.push_back(tag_code); col_ver.push_back(0);
            col_stmt.push_back(stmt_code); col_rfile.push_back(rfile_code); col_line.push_back(line_val); col_plabel.push_back(0);
            rows++; continue;
        }

        // col 7: version
        read_csv_field(f, field, eol);
        int32_t ver_code = ver_d.get(field);
        if (eol) {
            col_adsh.push_back(adsh_code); col_tag.push_back(tag_code); col_ver.push_back(ver_code);
            col_stmt.push_back(stmt_code); col_rfile.push_back(rfile_code); col_line.push_back(line_val); col_plabel.push_back(0);
            rows++; continue;
        }

        // col 8: plabel
        std::string plabel_s; read_csv_field(f, plabel_s, eol);
        int32_t plabel_code = (int32_t)plabel_d.intern(plabel_s);

        // skip rest (negating - col 9)
        if (!eol) skip_csv_row(f);

        col_adsh.push_back(adsh_code);
        col_tag.push_back(tag_code);
        col_ver.push_back(ver_code);
        col_stmt.push_back(stmt_code);
        col_rfile.push_back(rfile_code);
        col_line.push_back(line_val);
        col_plabel.push_back(plabel_code);
        rows++;

        if (rows % 2000000 == 0) printf("  [pre] %zu rows parsed\n", rows);
    }
    fclose(f);
    printf("  [pre] %zu rows total\n", rows);

    write_column(out_dir + "/pre/adsh.bin",    col_adsh);
    write_column(out_dir + "/pre/tag.bin",     col_tag);
    write_column(out_dir + "/pre/version.bin", col_ver);
    write_column(out_dir + "/pre/stmt.bin",    col_stmt);
    write_column(out_dir + "/pre/rfile.bin",   col_rfile);
    write_column(out_dir + "/pre/line.bin",    col_line);
    write_column(out_dir + "/pre/plabel.bin",  col_plabel);
    stmt_d.write_dict(out_dir + "/pre/stmt_dict.txt");
    rfile_d.write_dict(out_dir + "/pre/rfile_dict.txt");
    plabel_d.write_dict(out_dir + "/pre/plabel_dict.txt");
}

// ---- Main -------------------------------------------------------------
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <data_dir> <output_dir>\n", argv[0]);
        return 1;
    }
    std::string data_dir = argv[1];
    std::string out_dir  = argv[2];

    // Create directory structure
    mkdir_p(out_dir);
    mkdir_p(out_dir + "/num");
    mkdir_p(out_dir + "/sub");
    mkdir_p(out_dir + "/tag");
    mkdir_p(out_dir + "/pre");
    mkdir_p(out_dir + "/indexes");

    double t0 = omp_get_wtime();

    // Phase 1: Build global dictionaries
    DictBuilder adsh_d, tag_d, ver_d;
    build_global_dicts(data_dir, adsh_d, tag_d, ver_d);

    // Write global dict files
    adsh_d.write_dict(out_dir + "/adsh_global_dict.txt");
    tag_d.write_dict(out_dir  + "/tag_global_dict.txt");
    ver_d.write_dict(out_dir  + "/version_global_dict.txt");
    printf("[Phase 1] Global dicts written. %.1fs\n", omp_get_wtime() - t0);

    // Phase 2: Write columns — tables processed sequentially (HDD: one reader at a time)
    double t1 = omp_get_wtime();

    // num is the largest table — do it first
    write_num(data_dir, out_dir, adsh_d, tag_d, ver_d);
    printf("  num done: %.1fs\n", omp_get_wtime() - t1);

    double t2 = omp_get_wtime();
    write_sub(data_dir, out_dir, adsh_d);
    printf("  sub done: %.1fs\n", omp_get_wtime() - t2);

    double t3 = omp_get_wtime();
    write_tag(data_dir, out_dir, tag_d, ver_d);
    printf("  tag done: %.1fs\n", omp_get_wtime() - t3);

    double t4 = omp_get_wtime();
    write_pre(data_dir, out_dir, adsh_d, tag_d, ver_d);
    printf("  pre done: %.1fs\n", omp_get_wtime() - t4);

    printf("[DONE] Total ingestion time: %.1fs\n", omp_get_wtime() - t0);
    return 0;
}
