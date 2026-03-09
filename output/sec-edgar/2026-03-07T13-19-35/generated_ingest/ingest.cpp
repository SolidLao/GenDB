// ingest.cpp — SEC EDGAR binary columnar ingestion
// Converts CSV tables to binary columnar format with dict-encoding and sorting.
//
// Phase 1 (parallel): parse sub.csv + tag.csv → build adsh_dict and tagver_dict
// Phase 2 (sequential on HDD): parse num.csv, sort by (uom_code,ddate), write columns+zone_maps
//                               parse pre.csv, sort by (adsh_code,tagver_code), write columns

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

// ─────────────────────── helpers ─────────────────────────────────────────────

static double now_sec() {
    return std::chrono::duration<double>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

static void make_dirs(const std::string& base) {
    for (auto& sub : {"sub","num","tag","pre","indexes"})
        fs::create_directories(base + "/" + sub);
}

// Write a raw binary blob
static void write_file(const std::string& path, const void* data, size_t bytes) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot open for write: " + path);
    if (bytes > 0 && fwrite(data, 1, bytes, f) != bytes)
        throw std::runtime_error("Write failed: " + path);
    fclose(f);
}

template<typename T>
static void write_vec(const std::string& path, const std::vector<T>& v) {
    write_file(path, v.data(), v.size() * sizeof(T));
}

// Write varlen strings: offsets file (uint32_t[N+1]) + data file (char[])
static void write_varlen(const std::string& offsets_path,
                         const std::string& data_path,
                         const std::vector<std::string>& strs) {
    std::vector<uint32_t> offsets(strs.size() + 1);
    offsets[0] = 0;
    for (size_t i = 0; i < strs.size(); ++i)
        offsets[i+1] = offsets[i] + (uint32_t)strs[i].size();
    write_vec(offsets_path, offsets);

    size_t total = offsets.back();
    std::vector<char> data(total);
    for (size_t i = 0; i < strs.size(); ++i)
        memcpy(data.data() + offsets[i], strs[i].data(), strs[i].size());
    write_file(data_path, data.data(), data.size());
}

// ─────────────────────── CSV parser ──────────────────────────────────────────

// Parse one CSV line into fields (handles double-quoted fields with embedded commas).
// Returns number of fields parsed. Populates out[0..max_fields-1].
static int parse_csv_fields(const char* line, size_t len,
                             std::string* out, int max_fields) {
    int fi = 0;
    size_t i = 0;
    while (fi < max_fields) {
        out[fi].clear();
        if (i < len && line[i] == '"') {
            ++i; // skip opening quote
            while (i < len) {
                if (line[i] == '"') {
                    if (i+1 < len && line[i+1] == '"') { // escaped quote
                        out[fi] += '"'; i += 2;
                    } else { ++i; break; } // closing quote
                } else { out[fi] += line[i++]; }
            }
            // skip to next comma or end
            while (i < len && line[i] != ',') ++i;
        } else {
            while (i < len && line[i] != ',') out[fi] += line[i++];
        }
        ++fi;
        if (i >= len) break; // end of line
        ++i; // skip comma
    }
    return fi;
}

// Fast line reader with large buffer
struct CsvReader {
    FILE* fp;
    std::vector<char> buf;
    std::string line;

    explicit CsvReader(const std::string& path, size_t buf_size = 1 << 26 /*64MB*/) {
        fp = fopen(path.c_str(), "rb");
        if (!fp) throw std::runtime_error("Cannot open: " + path);
        buf.resize(buf_size);
        setvbuf(fp, buf.data(), _IOFBF, buf_size);
    }
    ~CsvReader() { if (fp) fclose(fp); }

    bool next_line() {
        line.clear();
        int c;
        while ((c = fgetc(fp)) != EOF) {
            if (c == '\r') continue;
            if (c == '\n') return true;
            line += (char)c;
        }
        return !line.empty();
    }
    // Skip header
    void skip_header() { next_line(); }
};

// ─────────────────────── global dictionaries ─────────────────────────────────

// adsh → int32_t code (0-based, assigned in sorted adsh order)
static std::unordered_map<std::string, int32_t> adsh_to_code;
// (tag + '\x00' + version) → int32_t tagver_code (= tag table row index)
static std::unordered_map<std::string, int32_t> tagver_to_code;

// ─────────────────────── sub ingestion ───────────────────────────────────────

static void ingest_sub(const std::string& csv_path, const std::string& out) {
    double t0 = now_sec();
    std::cout << "[sub] parsing " << csv_path << "\n" << std::flush;

    // Columns to store (indexed into sub CSV columns):
    // adsh(0), cik(1), name(2), sic(3), form(8), period(9), fy(10), filed(12)
    static const int COL_ADSH=0, COL_CIK=1, COL_NAME=2, COL_SIC=3,
                     COL_FORM=8, COL_PERIOD=9, COL_FY=10, COL_FILED=12;
    static const int MAX_COL = 13;

    struct SubRow {
        std::string adsh;
        int32_t cik;
        std::string name;
        int16_t sic;
        int16_t fy;
        int32_t period;
        int32_t filed;
        int8_t  form_code;
    };

    std::unordered_map<std::string,int8_t> form_dict;
    std::vector<SubRow> rows;
    rows.reserve(90000);

    CsvReader rdr(csv_path);
    rdr.skip_header();
    std::string fields[MAX_COL];

    while (rdr.next_line()) {
        int nf = parse_csv_fields(rdr.line.c_str(), rdr.line.size(), fields, MAX_COL);
        if (nf < MAX_COL) continue;

        SubRow r;
        r.adsh   = fields[COL_ADSH];
        r.cik    = (int32_t)atoi(fields[COL_CIK].c_str());
        r.name   = fields[COL_NAME];
        r.sic    = (int16_t)atoi(fields[COL_SIC].c_str());
        r.fy     = (int16_t)atoi(fields[COL_FY].c_str());
        r.period = (int32_t)atoi(fields[COL_PERIOD].c_str());
        r.filed  = (int32_t)atoi(fields[COL_FILED].c_str());

        const std::string& form_str = fields[COL_FORM];
        auto it = form_dict.find(form_str);
        if (it == form_dict.end()) {
            r.form_code = (int8_t)form_dict.size();
            form_dict[form_str] = r.form_code;
        } else r.form_code = it->second;

        rows.push_back(std::move(r));
    }

    // Sort by adsh to assign consistent codes
    std::sort(rows.begin(), rows.end(),
              [](const SubRow& a, const SubRow& b){ return a.adsh < b.adsh; });

    size_t N = rows.size();
    std::cout << "[sub] " << N << " rows\n" << std::flush;

    // Build adsh_to_code (row index = code)
    adsh_to_code.reserve(N * 2);
    for (size_t i = 0; i < N; ++i)
        adsh_to_code[rows[i].adsh] = (int32_t)i;

    // Write columns
    // adsh: fixed char[20] per row
    {
        std::vector<char> adsh_buf(N * 20, 0);
        for (size_t i = 0; i < N; ++i) {
            const std::string& s = rows[i].adsh;
            size_t len = std::min(s.size(), (size_t)20);
            memcpy(adsh_buf.data() + i*20, s.data(), len);
        }
        write_file(out+"/sub/adsh.bin", adsh_buf.data(), adsh_buf.size());
    }

    { std::vector<int32_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].cik;       write_vec(out+"/sub/cik.bin",    v); }
    { std::vector<int16_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].sic;       write_vec(out+"/sub/sic.bin",    v); }
    { std::vector<int16_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].fy;        write_vec(out+"/sub/fy.bin",     v); }
    { std::vector<int32_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].period;    write_vec(out+"/sub/period.bin", v); }
    { std::vector<int32_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].filed;     write_vec(out+"/sub/filed.bin",  v); }
    { std::vector<int8_t>  v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].form_code; write_vec(out+"/sub/form_code.bin", v); }

    // name: varlen
    { std::vector<std::string> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].name;
      write_varlen(out+"/sub/name_offsets.bin", out+"/sub/name_data.bin", v); }

    // Write adsh_dict index: sorted by adsh, {char[20], int32_t}
    {
        struct Entry { char adsh[20]; int32_t code; };
        std::vector<Entry> entries(N);
        for (size_t i = 0; i < N; ++i) {
            memset(entries[i].adsh, 0, 20);
            memcpy(entries[i].adsh, rows[i].adsh.data(),
                   std::min(rows[i].adsh.size(), (size_t)20));
            entries[i].code = (int32_t)i;
        }
        // Already sorted by adsh (rows sorted above)
        uint32_t n32 = (uint32_t)N;
        FILE* f = fopen((out+"/indexes/adsh_dict.bin").c_str(), "wb");
        fwrite(&n32, sizeof(n32), 1, f);
        fwrite(entries.data(), sizeof(Entry), N, f);
        fclose(f);
    }

    // Write form_dict
    {
        uint8_t n = (uint8_t)form_dict.size();
        FILE* f = fopen((out+"/indexes/form_codes.bin").c_str(), "wb");
        fwrite(&n, 1, 1, f);
        for (auto& [str, code] : form_dict) {
            fwrite(&code, 1, 1, f);
            uint8_t slen = (uint8_t)std::min(str.size(), (size_t)15);
            fwrite(&slen, 1, 1, f);
            fwrite(str.data(), 1, slen, f);
        }
        fclose(f);
    }

    printf("[sub] done in %.2fs\n", now_sec()-t0);
}

// ─────────────────────── tag ingestion ───────────────────────────────────────

static void ingest_tag(const std::string& csv_path, const std::string& out) {
    double t0 = now_sec();
    std::cout << "[tag] parsing " << csv_path << "\n" << std::flush;

    // Columns: tag(0),version(1),custom(2),abstract(3),datatype(4),iord(5),crdr(6),tlabel(7),doc(8)
    static const int MAX_COL = 9;

    struct TagRow {
        std::string tag_str;
        std::string version_str;
        std::string tlabel;
        int8_t custom;
        int8_t abstract_flag;
        int8_t datatype_code;
        int8_t iord_code;
        int8_t crdr_code;
    };

    std::unordered_map<std::string,int8_t> datatype_dict;
    std::vector<TagRow> rows;
    rows.reserve(1100000);

    CsvReader rdr(csv_path);
    rdr.skip_header();
    std::string fields[MAX_COL];

    while (rdr.next_line()) {
        int nf = parse_csv_fields(rdr.line.c_str(), rdr.line.size(), fields, MAX_COL);
        if (nf < 8) continue;

        TagRow r;
        r.tag_str  = fields[0];
        r.version_str = fields[1];
        r.custom   = (int8_t)atoi(fields[2].c_str());
        r.abstract_flag = (int8_t)atoi(fields[3].c_str());
        r.tlabel   = (nf > 7) ? fields[7] : "";

        const std::string& dt = fields[4];
        auto it = datatype_dict.find(dt);
        if (it == datatype_dict.end()) {
            r.datatype_code = (int8_t)datatype_dict.size();
            datatype_dict[dt] = r.datatype_code;
        } else r.datatype_code = it->second;

        // iord: I=0, D=1, else=2
        const std::string& iord = fields[5];
        r.iord_code = iord.empty() ? 2 : (iord[0]=='I' ? 0 : (iord[0]=='D' ? 1 : 2));

        // crdr: C=0, D=1, else=2
        const std::string& crdr = fields[6];
        r.crdr_code = crdr.empty() ? 2 : (crdr[0]=='C' ? 0 : (crdr[0]=='D' ? 1 : 2));

        rows.push_back(std::move(r));
    }

    size_t N = rows.size();
    std::cout << "[tag] " << N << " rows\n" << std::flush;

    // Assign tagver_code = row index (in current order)
    // Build tagver_to_code dict
    tagver_to_code.reserve(N * 2);
    for (size_t i = 0; i < N; ++i) {
        std::string key = rows[i].tag_str + '\x00' + rows[i].version_str;
        tagver_to_code[key] = (int32_t)i;
    }

    // Write tag columns
    { std::vector<int8_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].custom;        write_vec(out+"/tag/custom.bin",        v); }
    { std::vector<int8_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].abstract_flag; write_vec(out+"/tag/abstract.bin",       v); }
    { std::vector<int8_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].datatype_code; write_vec(out+"/tag/datatype_code.bin",  v); }
    { std::vector<int8_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].iord_code;     write_vec(out+"/tag/iord.bin",           v); }
    { std::vector<int8_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].crdr_code;     write_vec(out+"/tag/crdr.bin",           v); }

    { std::vector<std::string> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].tag_str;
      write_varlen(out+"/tag/tag_offsets.bin", out+"/tag/tag_data.bin", v); }
    { std::vector<std::string> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].version_str;
      write_varlen(out+"/tag/version_offsets.bin", out+"/tag/version_data.bin", v); }
    { std::vector<std::string> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].tlabel;
      write_varlen(out+"/tag/tlabel_offsets.bin", out+"/tag/tlabel_data.bin", v); }

    // Write tagver_dict: sorted by (tag, version), store {int32_t code, uint16_t tag_len, uint16_t ver_len, tag_bytes, ver_bytes}
    {
        struct Entry { int32_t code; std::string tag; std::string ver; };
        std::vector<Entry> entries(N);
        for (size_t i = 0; i < N; ++i)
            entries[i] = {(int32_t)i, rows[i].tag_str, rows[i].version_str};
        std::sort(entries.begin(), entries.end(),
                  [](const Entry& a, const Entry& b){
                      if (a.tag != b.tag) return a.tag < b.tag;
                      return a.ver < b.ver;
                  });

        uint32_t n32 = (uint32_t)N;
        FILE* f = fopen((out+"/indexes/tagver_dict.bin").c_str(), "wb");
        fwrite(&n32, sizeof(n32), 1, f);
        for (auto& e : entries) {
            fwrite(&e.code, sizeof(int32_t), 1, f);
            uint16_t tl = (uint16_t)e.tag.size();
            uint16_t vl = (uint16_t)e.ver.size();
            fwrite(&tl, sizeof(uint16_t), 1, f);
            fwrite(&vl, sizeof(uint16_t), 1, f);
            fwrite(e.tag.data(), 1, tl, f);
            fwrite(e.ver.data(), 1, vl, f);
        }
        fclose(f);
    }

    printf("[tag] done in %.2fs\n", now_sec()-t0);
}

// ─────────────────────── num ingestion ───────────────────────────────────────

static void ingest_num(const std::string& csv_path, const std::string& out) {
    double t0 = now_sec();
    std::cout << "[num] parsing " << csv_path << " (" << std::flush;

    // Columns: adsh(0),tag(1),version(2),ddate(3),qtrs(4),uom(5),coreg(6),value(7),footnote(8)
    // Need: 0,1,2,3,4,5,7  (skip 6=coreg, 8=footnote)
    static const int MAX_COL = 8; // parse up to and including value(7)

    struct NumRow {
        double   value;
        int32_t  adsh_code;
        int32_t  tagver_code;
        int32_t  ddate;
        int8_t   qtrs;
        int8_t   uom_code;
    };

    std::unordered_map<std::string,int8_t> uom_dict;
    std::vector<NumRow> rows;
    rows.reserve(40000000);

    CsvReader rdr(csv_path);
    rdr.skip_header();
    std::string fields[MAX_COL];

    size_t skipped = 0;
    while (rdr.next_line()) {
        int nf = parse_csv_fields(rdr.line.c_str(), rdr.line.size(), fields, MAX_COL);
        if (nf < 8) { ++skipped; continue; }

        // value: skip if empty (NULL)
        if (fields[7].empty()) { ++skipped; continue; }
        double val = atof(fields[7].c_str());
        if (std::isnan(val) && fields[7] != "NaN") { ++skipped; continue; }

        NumRow r;
        r.value = val;
        r.ddate = (int32_t)atoi(fields[3].c_str());
        r.qtrs  = (int8_t)atoi(fields[4].c_str());

        // adsh_code
        auto ait = adsh_to_code.find(fields[0]);
        if (ait == adsh_to_code.end()) { ++skipped; continue; }
        r.adsh_code = ait->second;

        // tagver_code
        std::string tv_key = fields[1] + '\x00' + fields[2];
        auto tit = tagver_to_code.find(tv_key);
        r.tagver_code = (tit != tagver_to_code.end()) ? tit->second : -1;

        // uom_code
        auto uit = uom_dict.find(fields[5]);
        if (uit == uom_dict.end()) {
            r.uom_code = (int8_t)uom_dict.size();
            uom_dict[fields[5]] = r.uom_code;
        } else r.uom_code = uit->second;

        rows.push_back(r);
    }

    size_t N = rows.size();
    printf("%.1fM rows, %zu skipped)\n", N/1e6, skipped);
    printf("[num] sorting by (uom_code, ddate)...\n"); fflush(stdout);

    std::sort(rows.begin(), rows.end(), [](const NumRow& a, const NumRow& b){
        if (a.uom_code != b.uom_code) return a.uom_code < b.uom_code;
        return a.ddate < b.ddate;
    });

    printf("[num] writing columns...\n"); fflush(stdout);

    // Write columns
    {
        std::vector<int32_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].adsh_code;
        write_vec(out+"/num/adsh_code.bin", v);
    }
    {
        std::vector<int32_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].tagver_code;
        write_vec(out+"/num/tagver_code.bin", v);
    }
    {
        std::vector<int32_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].ddate;
        write_vec(out+"/num/ddate.bin", v);
    }
    {
        std::vector<int8_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].qtrs;
        write_vec(out+"/num/qtrs.bin", v);
    }
    {
        std::vector<int8_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].uom_code;
        write_vec(out+"/num/uom_code.bin", v);
    }
    {
        std::vector<double> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].value;
        write_vec(out+"/num/value.bin", v);
    }

    // Write zone maps: per 100K block, min/max uom_code and min/max ddate
    {
        const size_t BS = 100000;
        size_t n_blocks = (N + BS - 1) / BS;

        // Format: uint32_t n_blocks, then n_blocks × {int8_t min_uom, int8_t max_uom, int32_t min_ddate, int32_t max_ddate}
        struct ZoneMap { int8_t min_uom, max_uom; int32_t min_ddate, max_ddate; };
        std::vector<ZoneMap> zmaps(n_blocks);

        for (size_t b = 0; b < n_blocks; ++b) {
            size_t lo = b * BS, hi = std::min(lo + BS, N);
            int8_t minu=127, maxu=-128; int32_t mind=INT32_MAX, maxd=INT32_MIN;
            for (size_t i = lo; i < hi; ++i) {
                int8_t u = rows[i].uom_code; int32_t d = rows[i].ddate;
                if (u < minu) minu=u; if (u > maxu) maxu=u;
                if (d < mind) mind=d; if (d > maxd) maxd=d;
            }
            zmaps[b] = {minu, maxu, mind, maxd};
        }

        FILE* f = fopen((out+"/indexes/num_zone_maps.bin").c_str(), "wb");
        uint32_t nb = (uint32_t)n_blocks;
        fwrite(&nb, sizeof(nb), 1, f);
        fwrite(zmaps.data(), sizeof(ZoneMap), n_blocks, f);
        fclose(f);
        printf("[num] zone maps: %zu blocks\n", n_blocks);
    }

    // Write uom dict
    {
        uint8_t n = (uint8_t)uom_dict.size();
        FILE* f = fopen((out+"/indexes/uom_codes.bin").c_str(), "wb");
        fwrite(&n, 1, 1, f);
        for (auto& [str, code] : uom_dict) {
            fwrite(&code, 1, 1, f);
            uint8_t slen = (uint8_t)std::min(str.size(), (size_t)23);
            fwrite(&slen, 1, 1, f);
            fwrite(str.data(), 1, slen, f);
        }
        fclose(f);
        printf("[num] uom dict: %d entries\n", (int)n);
    }

    printf("[num] done in %.2fs\n", now_sec()-t0);
}

// ─────────────────────── pre ingestion ───────────────────────────────────────

static void ingest_pre(const std::string& csv_path, const std::string& out) {
    double t0 = now_sec();
    std::cout << "[pre] parsing " << csv_path << "\n" << std::flush;

    // Columns: adsh(0),report(1),line(2),stmt(3),inpth(4),rfile(5),tag(6),version(7),plabel(8),negating(9)
    static const int MAX_COL = 10;

    struct PreRow {
        uint32_t plabel_idx;  // index into plabel_strings
        int32_t  adsh_code;
        int32_t  tagver_code;
        int16_t  report;
        int8_t   stmt_code;
        int8_t   rfile_code;
        int8_t   line;
        int8_t   inpth;
        int8_t   negating;
    };

    std::unordered_map<std::string,int8_t> stmt_dict;
    std::unordered_map<std::string,int8_t> rfile_dict;
    std::vector<PreRow> rows;
    std::vector<std::string> plabel_strings;
    rows.reserve(10000000);
    plabel_strings.reserve(10000000);

    CsvReader rdr(csv_path);
    rdr.skip_header();
    std::string fields[MAX_COL];

    size_t skipped = 0;
    while (rdr.next_line()) {
        int nf = parse_csv_fields(rdr.line.c_str(), rdr.line.size(), fields, MAX_COL);
        if (nf < 9) { ++skipped; continue; }

        PreRow r;

        // adsh_code
        auto ait = adsh_to_code.find(fields[0]);
        if (ait == adsh_to_code.end()) { ++skipped; continue; }
        r.adsh_code = ait->second;

        // tagver_code
        std::string tv_key = fields[6] + '\x00' + fields[7];
        auto tit = tagver_to_code.find(tv_key);
        r.tagver_code = (tit != tagver_to_code.end()) ? tit->second : -1;

        r.report  = (int16_t)atoi(fields[1].c_str());
        r.line    = (int8_t)atoi(fields[2].c_str());
        r.inpth   = (int8_t)atoi(fields[4].c_str());
        r.negating= (nf >= 10) ? (int8_t)atoi(fields[9].c_str()) : 0;

        // stmt_code
        const std::string& stmt = fields[3];
        auto sit = stmt_dict.find(stmt);
        if (sit == stmt_dict.end()) {
            r.stmt_code = (int8_t)stmt_dict.size();
            stmt_dict[stmt] = r.stmt_code;
        } else r.stmt_code = sit->second;

        // rfile_code
        const std::string& rfile = fields[5];
        auto rit = rfile_dict.find(rfile);
        if (rit == rfile_dict.end()) {
            r.rfile_code = (int8_t)rfile_dict.size();
            rfile_dict[rfile] = r.rfile_code;
        } else r.rfile_code = rit->second;

        // plabel
        r.plabel_idx = (uint32_t)plabel_strings.size();
        plabel_strings.push_back(fields[8]);

        rows.push_back(r);
    }

    size_t N = rows.size();
    printf("[pre] %zu rows, %zu skipped\n", N, skipped);
    printf("[pre] sorting by (adsh_code, tagver_code)...\n"); fflush(stdout);

    std::sort(rows.begin(), rows.end(), [](const PreRow& a, const PreRow& b){
        if (a.adsh_code != b.adsh_code) return a.adsh_code < b.adsh_code;
        return a.tagver_code < b.tagver_code;
    });

    printf("[pre] writing columns...\n"); fflush(stdout);

    { std::vector<int32_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].adsh_code;  write_vec(out+"/pre/adsh_code.bin",   v); }
    { std::vector<int32_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].tagver_code; write_vec(out+"/pre/tagver_code.bin", v); }
    { std::vector<int8_t>  v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].stmt_code;   write_vec(out+"/pre/stmt_code.bin",   v); }
    { std::vector<int8_t>  v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].rfile_code;  write_vec(out+"/pre/rfile_code.bin",  v); }
    { std::vector<int16_t> v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].report;      write_vec(out+"/pre/report.bin",      v); }
    { std::vector<int8_t>  v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].line;        write_vec(out+"/pre/line.bin",        v); }
    { std::vector<int8_t>  v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].inpth;       write_vec(out+"/pre/inpth.bin",       v); }
    { std::vector<int8_t>  v(N); for(size_t i=0;i<N;++i) v[i]=rows[i].negating;    write_vec(out+"/pre/negating.bin",    v); }

    // plabel: write in sorted order
    {
        std::vector<std::string> sorted_plabels(N);
        for (size_t i = 0; i < N; ++i)
            sorted_plabels[i] = plabel_strings[rows[i].plabel_idx];
        write_varlen(out+"/pre/plabel_offsets.bin", out+"/pre/plabel_data.bin", sorted_plabels);
    }

    // Write stmt dict
    {
        uint8_t n = (uint8_t)stmt_dict.size();
        FILE* f = fopen((out+"/indexes/stmt_codes.bin").c_str(), "wb");
        fwrite(&n, 1, 1, f);
        for (auto& [str, code] : stmt_dict) {
            fwrite(&code, 1, 1, f);
            uint8_t slen = (uint8_t)std::min(str.size(), (size_t)7);
            fwrite(&slen, 1, 1, f);
            fwrite(str.data(), 1, slen, f);
        }
        fclose(f);
        printf("[pre] stmt dict: %d entries\n", (int)n);
    }

    // Write rfile dict
    {
        uint8_t n = (uint8_t)rfile_dict.size();
        FILE* f = fopen((out+"/indexes/rfile_codes.bin").c_str(), "wb");
        fwrite(&n, 1, 1, f);
        for (auto& [str, code] : rfile_dict) {
            fwrite(&code, 1, 1, f);
            uint8_t slen = (uint8_t)std::min(str.size(), (size_t)3);
            fwrite(&slen, 1, 1, f);
            fwrite(str.data(), 1, slen, f);
        }
        fclose(f);
    }

    printf("[pre] done in %.2fs\n", now_sec()-t0);
}

// ─────────────────────── main ────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <data_dir> <out_dir>\n", argv[0]);
        return 1;
    }
    std::string data_dir = argv[1];
    std::string out_dir  = argv[2];

    double t0 = now_sec();
    make_dirs(out_dir);

    // Phase 1: parse sub and tag in parallel (both are small enough)
    printf("=== Phase 1: parsing sub and tag in parallel ===\n"); fflush(stdout);
    {
        std::exception_ptr ep1, ep2;
        std::thread t1([&]() noexcept {
            try { ingest_sub(data_dir+"/sub.csv", out_dir); }
            catch (...) { ep1 = std::current_exception(); }
        });
        std::thread t2([&]() noexcept {
            try { ingest_tag(data_dir+"/tag.csv", out_dir); }
            catch (...) { ep2 = std::current_exception(); }
        });
        t1.join(); t2.join();
        if (ep1) std::rethrow_exception(ep1);
        if (ep2) std::rethrow_exception(ep2);
    }
    printf("=== Phase 1 done: adsh_dict=%zu, tagver_dict=%zu ===\n",
           adsh_to_code.size(), tagver_to_code.size()); fflush(stdout);

    // Phase 2: parse num and pre sequentially (HDD, avoid seek contention)
    printf("=== Phase 2: parsing num and pre ===\n"); fflush(stdout);
    ingest_num(data_dir+"/num.csv", out_dir);
    ingest_pre(data_dir+"/pre.csv", out_dir);

    printf("=== Ingestion complete in %.2fs ===\n", now_sec()-t0);
    return 0;
}
