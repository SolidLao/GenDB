// ingest.cpp — SEC EDGAR sf3 binary columnar ingestion
// Sort orders: sub by adsh, num by (uom,ddate), pre by (stmt,adsh)
// Shared dicts: adsh (sub→num,pre), tag_numpre (num↔pre), version_numpre (num↔pre)

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <cassert>
#include <climits>
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <numeric>
#include <filesystem>
#include <stdexcept>
#include <limits>

#ifdef _OPENMP
#include <omp.h>
#endif

namespace fs = std::filesystem;

// ============================================================
// Buffered CSV Parser (RFC 4180, handles multi-line quoted fields)
// ============================================================
class CSVParser {
    static const size_t BUF = 1ULL << 26; // 64MB
    FILE* fp;
    std::vector<char> ibuf;
    size_t pos, len;
    bool done;

    int gc() {
        if (pos >= len) {
            len = fread(ibuf.data(), 1, BUF, fp);
            pos = 0;
            if (len == 0) { done = true; return -1; }
        }
        return (unsigned char)ibuf[pos++];
    }
    void unget_one() { if (pos > 0) --pos; }

public:
    explicit CSVParser(const std::string& path)
        : ibuf(BUF), pos(0), len(0), done(false) {
        fp = fopen(path.c_str(), "rb");
        if (!fp) throw std::runtime_error("Cannot open: " + path);
    }
    ~CSVParser() { if (fp) fclose(fp); }
    bool eof() const { return done; }

    bool next_row(std::vector<std::string>& fields) {
        fields.clear();
        int c = gc();
        if (c == -1) return false;

        std::string field;
        bool in_quote = false;

        for (;;) {
            if (c == -1) {
                fields.push_back(std::move(field));
                return true;
            }
            if (in_quote) {
                if (c == '"') {
                    int nx = gc();
                    if (nx == '"') { field += '"'; }
                    else {
                        in_quote = false;
                        if (nx == ',')       { fields.push_back(std::move(field)); field.clear(); }
                        else if (nx == '\n') { fields.push_back(std::move(field)); return true; }
                        else if (nx == '\r') { int nn=gc(); if(nn!='\n'&&nn!=-1) unget_one();
                                               fields.push_back(std::move(field)); return true; }
                        else if (nx == -1)   { fields.push_back(std::move(field)); return true; }
                        else                 { field += (char)nx; }
                    }
                } else { field += (char)c; }
            } else {
                if      (c == '"')  { in_quote = true; }
                else if (c == ',')  { fields.push_back(std::move(field)); field.clear(); }
                else if (c == '\n') { fields.push_back(std::move(field)); return true; }
                else if (c == '\r') { int nx=gc(); if(nx!='\n'&&nx!=-1) unget_one();
                                      fields.push_back(std::move(field)); return true; }
                else                { field += (char)c; }
            }
            c = gc();
        }
    }
};

// ============================================================
// Dictionary types
// ============================================================
struct SmallDict {  // int8_t codes, -1=null
    std::unordered_map<std::string,int8_t> map;
    std::vector<std::string> strs;
    int8_t encode(const std::string& s) {
        if (s.empty()) return -1;
        auto it = map.find(s);
        if (it != map.end()) return it->second;
        int8_t code = (int8_t)strs.size();
        map[s] = code; strs.push_back(s); return code;
    }
};

struct LargeDict {  // int32_t codes, -1=null
    std::unordered_map<std::string,int32_t> map;
    std::vector<std::string> strs;
    LargeDict() { map.reserve(1<<17); }
    int32_t encode(const std::string& s) {
        if (s.empty()) return -1;
        auto it = map.find(s);
        if (it != map.end()) return it->second;
        int32_t code = (int32_t)strs.size();
        map[s] = code; strs.push_back(s); return code;
    }
};

// ============================================================
// Global shared state
// ============================================================
static std::unordered_map<std::string,int32_t> g_adsh_map; // adsh_str -> sorted row_id
static LargeDict g_tag_dict;
static LargeDict g_ver_dict;

// ============================================================
// File writing utilities
// ============================================================
static void write_dict_file(const std::string& path, const std::vector<std::string>& strs) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot write: " + path);
    uint32_t n = (uint32_t)strs.size();
    fwrite(&n, 4, 1, f);
    for (auto& s : strs) {
        uint16_t len = (uint16_t)s.size();
        fwrite(&len, 2, 1, f);
        fwrite(s.data(), 1, len, f);
    }
    fclose(f);
}

template<typename T>
static void write_col(const std::string& path, const std::vector<T>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot write: " + path);
    if (!data.empty()) fwrite(data.data(), sizeof(T), data.size(), f);
    fclose(f);
}

template<typename T>
static void write_col_perm(const std::string& path, const std::vector<T>& data,
                            const std::vector<uint32_t>& perm) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot write: " + path);
    for (uint32_t idx : perm) {
        fwrite(&data[idx], sizeof(T), 1, f);
    }
    fclose(f);
}

static void write_fixed_str_col(const std::string& path,
                                 const std::vector<std::string>& data, int width) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot write: " + path);
    std::vector<char> buf(width, 0);
    for (auto& s : data) {
        memset(buf.data(), 0, width);
        int n = std::min((int)s.size(), width);
        if (n > 0) memcpy(buf.data(), s.data(), n);
        fwrite(buf.data(), 1, width, f);
    }
    fclose(f);
}

static void write_fixed_str_col_perm(const std::string& path,
                                      const std::vector<std::string>& data,
                                      int width,
                                      const std::vector<uint32_t>& perm) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot write: " + path);
    std::vector<char> buf(width, 0);
    for (uint32_t idx : perm) {
        memset(buf.data(), 0, width);
        int n = std::min((int)data[idx].size(), width);
        if (n > 0) memcpy(buf.data(), data[idx].data(), n);
        fwrite(buf.data(), 1, width, f);
    }
    fclose(f);
}

static void write_varlen_col_perm(const std::string& off_path, const std::string& dat_path,
                                   const std::vector<std::string>& data,
                                   const std::vector<uint32_t>& perm) {
    FILE* fo = fopen(off_path.c_str(), "wb");
    FILE* fd = fopen(dat_path.c_str(), "wb");
    if (!fo || !fd) throw std::runtime_error("Cannot write varlen: " + off_path);
    uint32_t N = (uint32_t)perm.size();
    // Write offsets: N+1 int64_t
    int64_t off = 0;
    // First pass: accumulate offsets
    std::vector<int64_t> offsets(N + 1);
    offsets[0] = 0;
    for (uint32_t i = 0; i < N; i++) {
        fwrite(data[perm[i]].data(), 1, data[perm[i]].size(), fd);
        off += (int64_t)data[perm[i]].size();
        offsets[i+1] = off;
    }
    fwrite(offsets.data(), 8, N+1, fo);
    fclose(fo); fclose(fd);
}

static void write_varlen_col(const std::string& off_path, const std::string& dat_path,
                              const std::vector<std::string>& data) {
    FILE* fo = fopen(off_path.c_str(), "wb");
    FILE* fd = fopen(dat_path.c_str(), "wb");
    if (!fo || !fd) throw std::runtime_error("Cannot write varlen: " + off_path);
    uint32_t N = (uint32_t)data.size();
    std::vector<int64_t> offsets(N + 1);
    int64_t off = 0;
    offsets[0] = 0;
    for (uint32_t i = 0; i < N; i++) {
        fwrite(data[i].data(), 1, data[i].size(), fd);
        off += (int64_t)data[i].size();
        offsets[i+1] = off;
    }
    fwrite(offsets.data(), 8, N+1, fo);
    fclose(fo); fclose(fd);
}

// Fast integer parse (no error checking, field guaranteed to be digits with optional leading -)
static int64_t fast_atoi(const std::string& s) {
    if (s.empty()) return 0;
    bool neg = (s[0] == '-');
    int64_t v = 0;
    for (size_t i = neg ? 1 : 0; i < s.size(); i++) v = v*10 + (s[i]-'0');
    return neg ? -v : v;
}

static double fast_atod(const std::string& s) {
    if (s.empty()) return std::numeric_limits<double>::quiet_NaN();
    return std::stod(s);
}

// ============================================================
// parse_sub: read sub.csv, sort by adsh, write binary columns
// Builds g_adsh_map: adsh_str -> sorted_row_id
// ============================================================
static void parse_sub(const std::string& csv, const std::string& db) {
    std::string D = db + "/sub";
    fprintf(stderr, "[sub] parsing %s\n", csv.c_str());

    // Raw storage (CSV order)
    std::vector<std::string> adsh, name_s, cityba, accepted, instance;
    std::vector<int32_t>  cik, period, filed;
    std::vector<int16_t>  sic, fy, nciks;
    std::vector<int8_t>   prevrpt, wksi;
    std::vector<std::string> countryba, stprba, countryinc, form_s, fp_s, fye, afs_s;

    adsh.reserve(90000);

    CSVParser p(csv);
    std::vector<std::string> f;
    p.next_row(f); // skip header

    while (p.next_row(f)) {
        if (f.size() < 20) continue;
        adsh.push_back(f[0]);
        cik.push_back((int32_t)fast_atoi(f[1]));
        name_s.push_back(f[2]);
        sic.push_back((int16_t)fast_atoi(f[3]));
        countryba.push_back(f[4]);
        stprba.push_back(f[5]);
        cityba.push_back(f[6]);
        countryinc.push_back(f[7]);
        form_s.push_back(f[8]);
        period.push_back((int32_t)fast_atoi(f[9]));
        fy.push_back((int16_t)fast_atoi(f[10]));
        fp_s.push_back(f[11]);
        filed.push_back((int32_t)fast_atoi(f[12]));
        accepted.push_back(f[13]);
        prevrpt.push_back((int8_t)fast_atoi(f[14]));
        nciks.push_back((int16_t)fast_atoi(f[15]));
        afs_s.push_back(f[16]);
        wksi.push_back((int8_t)fast_atoi(f[17]));
        fye.push_back(f[18]);
        instance.push_back(f.size() > 19 ? f[19] : "");
    }

    uint32_t N = (uint32_t)adsh.size();
    fprintf(stderr, "[sub] %u rows parsed, sorting by adsh\n", N);

    // Sort by adsh
    std::vector<uint32_t> perm(N);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b){ return adsh[a] < adsh[b]; });

    // Build global adsh map: adsh_str -> sorted_position
    g_adsh_map.reserve(N * 2);
    for (uint32_t i = 0; i < N; i++)
        g_adsh_map[adsh[perm[i]]] = (int32_t)i;

    // Build small dicts
    SmallDict form_dict, fp_dict, afs_dict;
    LargeDict name_dict;

    // Apply permutation and build encoded column vectors
    std::vector<int32_t> cik_c(N), period_c(N), filed_c(N), name_c(N);
    std::vector<int16_t> sic_c(N), fy_c(N), nciks_c(N);
    std::vector<int8_t>  prevrpt_c(N), wksi_c(N), form_c(N), fp_c(N), afs_c(N);
    std::vector<std::string> adsh_sorted(N), cb_s(N), sp_s(N), ci_s(N), cy_s(N), fy_s(N), acc_s(N), ins_s(N);

    for (uint32_t i = 0; i < N; i++) {
        uint32_t j = perm[i];
        adsh_sorted[i] = adsh[j];
        cik_c[i]    = cik[j];
        name_c[i]   = name_dict.encode(name_s[j]);
        sic_c[i]    = sic[j];
        cb_s[i]     = countryba[j];
        sp_s[i]     = stprba[j];
        cy_s[i]     = cityba[j];
        ci_s[i]     = countryinc[j];
        form_c[i]   = form_dict.encode(form_s[j]);
        period_c[i] = period[j];
        fy_c[i]     = fy[j];
        fp_c[i]     = fp_dict.encode(fp_s[j]);
        filed_c[i]  = filed[j];
        acc_s[i]    = accepted[j];
        prevrpt_c[i]= prevrpt[j];
        nciks_c[i]  = nciks[j];
        afs_c[i]    = afs_dict.encode(afs_s[j]);
        wksi_c[i]   = wksi[j];
        fy_s[i]     = fye[j];
        ins_s[i]    = instance[j];
    }

    // Write adsh.bin (fixed20 sorted = master adsh dict)
    write_fixed_str_col(D+"/adsh.bin", adsh_sorted, 20);
    // Write numeric columns
    write_col(D+"/cik.bin",     cik_c);
    write_col(D+"/name.bin",    name_c);
    write_col(D+"/sic.bin",     sic_c);
    write_col(D+"/fy.bin",      fy_c);
    write_col(D+"/form.bin",    form_c);
    write_col(D+"/period.bin",  period_c);
    write_col(D+"/fp.bin",      fp_c);
    write_col(D+"/filed.bin",   filed_c);
    write_col(D+"/prevrpt.bin", prevrpt_c);
    write_col(D+"/nciks.bin",   nciks_c);
    write_col(D+"/afs.bin",     afs_c);
    write_col(D+"/wksi.bin",    wksi_c);
    // Fixed-width string columns
    write_fixed_str_col(D+"/countryba.bin",  cb_s, 2);
    write_fixed_str_col(D+"/stprba.bin",     sp_s, 2);
    write_fixed_str_col(D+"/countryinc.bin", ci_s, 3);
    write_fixed_str_col(D+"/fye.bin",        fy_s, 4);
    // Varlen columns
    write_varlen_col(D+"/cityba.offsets", D+"/cityba.data", cy_s);
    write_varlen_col(D+"/accepted.offsets", D+"/accepted.data", acc_s);
    write_varlen_col(D+"/instance.offsets", D+"/instance.data", ins_s);
    // Dicts
    write_dict_file(D+"/name.dict", name_dict.strs);
    write_dict_file(D+"/form.dict", form_dict.strs);
    write_dict_file(D+"/fp.dict",   fp_dict.strs);
    write_dict_file(D+"/afs.dict",  afs_dict.strs);

    fprintf(stderr, "[sub] done: %u rows\n", N);
}

// ============================================================
// parse_num: read num.csv, sort by (uom,ddate), write columns
// Uses g_adsh_map; builds g_tag_dict, g_ver_dict, uom_dict
// ============================================================
static void parse_num(const std::string& csv, const std::string& db) {
    std::string D = db + "/num";
    fprintf(stderr, "[num] parsing %s\n", csv.c_str());

    const size_t RESERVE = 42000000;
    std::vector<int32_t> adsh_c, tag_c, ver_c, ddate;
    std::vector<int8_t>  qtrs, uom_c;
    std::vector<double>  value;
    std::vector<std::string> coreg, footnote;

    adsh_c.reserve(RESERVE); tag_c.reserve(RESERVE); ver_c.reserve(RESERVE);
    ddate.reserve(RESERVE);  qtrs.reserve(RESERVE);  uom_c.reserve(RESERVE);
    value.reserve(RESERVE);  coreg.reserve(RESERVE); footnote.reserve(RESERVE);

    SmallDict uom_dict;
    // Pre-seed common UOM values for stable codes
    uom_dict.encode("USD"); uom_dict.encode("shares"); uom_dict.encode("pure");

    CSVParser p(csv);
    std::vector<std::string> f;
    p.next_row(f); // skip header

    uint64_t row = 0;
    while (p.next_row(f)) {
        if (f.size() < 8) continue;
        // adsh
        auto it = g_adsh_map.find(f[0]);
        adsh_c.push_back(it != g_adsh_map.end() ? it->second : -1);
        // tag, version: shared dicts
        tag_c.push_back(g_tag_dict.encode(f[1]));
        ver_c.push_back(g_ver_dict.encode(f[2]));
        // ddate
        ddate.push_back((int32_t)fast_atoi(f[3]));
        // qtrs
        qtrs.push_back((int8_t)fast_atoi(f[4]));
        // uom
        uom_c.push_back(uom_dict.encode(f[5]));
        // coreg (f[6])
        coreg.push_back(f[6]);
        // value (f[7])
        value.push_back(f[7].empty() ? std::numeric_limits<double>::quiet_NaN() : fast_atod(f[7]));
        // footnote (f[8] if present)
        footnote.push_back(f.size() > 8 ? f[8] : "");
        ++row;
        if (row % 5000000 == 0) fprintf(stderr, "[num] %lu M rows parsed\n", row/1000000);
    }

    uint32_t N = (uint32_t)adsh_c.size();
    fprintf(stderr, "[num] %u rows parsed. Sorting by (uom, ddate)...\n", N);

    // Counting sort by uom_code -> buckets
    int num_uom = (int)uom_dict.strs.size();
    // uom_c values: -1 for null, 0..num_uom-1 for valid
    // Map to bucket index: -1 -> 0, 0 -> 1, ...
    std::vector<std::vector<uint32_t>> buckets(num_uom + 2); // +2 for null and overflow
    for (int b = 0; b < (int)buckets.size(); b++) buckets[b].reserve(N/(num_uom+2)+1);
    for (uint32_t i = 0; i < N; i++) {
        int b = (int)uom_c[i] + 1; // -1->0, 0->1, ...
        if (b < 0) b = 0;
        if (b >= (int)buckets.size()) b = (int)buckets.size()-1;
        buckets[b].push_back(i);
    }

    // Sort each bucket by ddate in parallel
#pragma omp parallel for schedule(dynamic)
    for (int b = 0; b < (int)buckets.size(); b++) {
        std::sort(buckets[b].begin(), buckets[b].end(),
                  [&](uint32_t a, uint32_t bb){ return ddate[a] < ddate[bb]; });
    }

    // Flatten into permutation
    std::vector<uint32_t> perm;
    perm.reserve(N);
    for (auto& bk : buckets) for (uint32_t idx : bk) perm.push_back(idx);

    fprintf(stderr, "[num] writing binary columns...\n");

    // Write columns in perm order
    write_col_perm(D+"/adsh.bin",    adsh_c, perm);
    write_col_perm(D+"/tag.bin",     tag_c,  perm);
    write_col_perm(D+"/version.bin", ver_c,  perm);
    write_col_perm(D+"/ddate.bin", ddate,  perm);
    write_col_perm(D+"/qtrs.bin",  qtrs,   perm);
    write_col_perm(D+"/uom.bin",   uom_c,  perm);
    write_col_perm(D+"/value.bin", value,  perm);
    write_varlen_col_perm(D+"/coreg.offsets",    D+"/coreg.data",    coreg,    perm);
    write_varlen_col_perm(D+"/footnote.offsets", D+"/footnote.data", footnote, perm);

    // Write UOM dict to shared/
    write_dict_file(db+"/shared/uom.dict", uom_dict.strs);

    fprintf(stderr, "[num] done: %u rows, %u tags, %u versions, %u uoms\n",
            N, (uint32_t)g_tag_dict.strs.size(), (uint32_t)g_ver_dict.strs.size(),
            (uint32_t)uom_dict.strs.size());
}

// ============================================================
// parse_pre: read pre.csv, sort by (stmt,adsh), write columns
// Uses g_adsh_map, g_tag_dict, g_ver_dict; builds stmt_dict, rfile_dict
// ============================================================
static void parse_pre(const std::string& csv, const std::string& db) {
    std::string D = db + "/pre";
    fprintf(stderr, "[pre] parsing %s\n", csv.c_str());

    const size_t RESERVE = 10000000;
    std::vector<int32_t> adsh_c, tag_c, ver_c, line;
    std::vector<int16_t> report;
    std::vector<int8_t>  stmt_c, inpth, rfile_c, negating;
    std::vector<std::string> plabel;

    adsh_c.reserve(RESERVE); tag_c.reserve(RESERVE); ver_c.reserve(RESERVE);
    line.reserve(RESERVE);   report.reserve(RESERVE);
    stmt_c.reserve(RESERVE); inpth.reserve(RESERVE); rfile_c.reserve(RESERVE);
    negating.reserve(RESERVE); plabel.reserve(RESERVE);

    SmallDict stmt_dict, rfile_dict;
    // Pre-seed stmt values for stable codes
    stmt_dict.encode("BS"); stmt_dict.encode("IS"); stmt_dict.encode("CF");
    stmt_dict.encode("EQ"); stmt_dict.encode("CI");

    CSVParser p(csv);
    std::vector<std::string> f;
    p.next_row(f); // skip header

    while (p.next_row(f)) {
        if (f.size() < 10) continue;
        auto it = g_adsh_map.find(f[0]);
        adsh_c.push_back(it != g_adsh_map.end() ? it->second : -1);
        report.push_back((int16_t)fast_atoi(f[1]));
        line.push_back((int32_t)fast_atoi(f[2]));
        stmt_c.push_back(stmt_dict.encode(f[3]));
        inpth.push_back((int8_t)fast_atoi(f[4]));
        rfile_c.push_back(rfile_dict.encode(f[5]));
        tag_c.push_back(g_tag_dict.encode(f[6]));
        ver_c.push_back(g_ver_dict.encode(f[7]));
        plabel.push_back(f[8]);
        negating.push_back((int8_t)fast_atoi(f[9]));
    }

    uint32_t N = (uint32_t)adsh_c.size();
    fprintf(stderr, "[pre] %u rows parsed. Sorting by (stmt, adsh)...\n", N);

    // Counting sort by stmt_code (null=-1 -> bucket 0, codes 0..5 -> buckets 1..6)
    int ns = (int)stmt_dict.strs.size();
    std::vector<std::vector<uint32_t>> buckets(ns + 2);
    for (auto& bk : buckets) bk.reserve(N/(ns+2)+1);
    for (uint32_t i = 0; i < N; i++) {
        int b = (int)stmt_c[i] + 1;
        if (b < 0) b = 0;
        if (b >= (int)buckets.size()) b = (int)buckets.size()-1;
        buckets[b].push_back(i);
    }

#pragma omp parallel for schedule(dynamic)
    for (int b = 0; b < (int)buckets.size(); b++) {
        std::sort(buckets[b].begin(), buckets[b].end(),
                  [&](uint32_t a, uint32_t bb){ return adsh_c[a] < adsh_c[bb]; });
    }

    std::vector<uint32_t> perm;
    perm.reserve(N);
    for (auto& bk : buckets) for (uint32_t idx : bk) perm.push_back(idx);

    fprintf(stderr, "[pre] writing binary columns...\n");

    write_col_perm(D+"/adsh.bin",    adsh_c,  perm);
    write_col_perm(D+"/report.bin",  report,  perm);
    write_col_perm(D+"/line.bin",    line,    perm);
    write_col_perm(D+"/stmt.bin",    stmt_c,  perm);
    write_col_perm(D+"/inpth.bin",   inpth,   perm);
    write_col_perm(D+"/rfile.bin",   rfile_c, perm);
    write_col_perm(D+"/tag.bin",     tag_c,   perm);
    write_col_perm(D+"/version.bin", ver_c,   perm);
    write_col_perm(D+"/negating.bin",negating, perm);
    write_varlen_col_perm(D+"/plabel.offsets", D+"/plabel.data", plabel, perm);

    write_dict_file(db+"/shared/stmt.dict",  stmt_dict.strs);
    write_dict_file(db+"/shared/rfile.dict", rfile_dict.strs);

    fprintf(stderr, "[pre] done: %u rows\n", N);
}

// ============================================================
// parse_tag: read tag.csv, write columns (no sort needed)
// ============================================================
static void parse_tag(const std::string& csv, const std::string& db) {
    std::string D = db + "/tag";
    fprintf(stderr, "[tag] parsing %s\n", csv.c_str());

    std::vector<std::string> tag, version, tlabel, doc;
    std::vector<int8_t> custom, abstract, iord, crdr, datatype_c;
    SmallDict dtype_dict;

    tag.reserve(1100000); version.reserve(1100000);

    CSVParser p(csv);
    std::vector<std::string> f;
    p.next_row(f); // skip header

    while (p.next_row(f)) {
        if (f.size() < 9) continue;
        tag.push_back(f[0]);
        version.push_back(f[1]);
        custom.push_back((int8_t)fast_atoi(f[2]));
        abstract.push_back((int8_t)fast_atoi(f[3]));
        datatype_c.push_back(dtype_dict.encode(f[4]));
        // iord: I=0, D=1, null=-1
        int8_t iv = -1;
        if (f[5] == "I") iv = 0; else if (f[5] == "D") iv = 1;
        iord.push_back(iv);
        // crdr: C=0, D=1, null=-1
        int8_t cv = -1;
        if (f[6] == "C") cv = 0; else if (f[6] == "D") cv = 1;
        crdr.push_back(cv);
        tlabel.push_back(f[7]);
        doc.push_back(f[8]);
    }

    uint32_t N = (uint32_t)tag.size();
    fprintf(stderr, "[tag] %u rows parsed, writing...\n", N);

    write_varlen_col(D+"/tag.offsets",     D+"/tag.data",     tag);
    write_varlen_col(D+"/version.offsets", D+"/version.data", version);
    write_col(D+"/custom.bin",   custom);
    write_col(D+"/abstract.bin", abstract);
    write_col(D+"/datatype.bin", datatype_c);
    write_col(D+"/iord.bin",     iord);
    write_col(D+"/crdr.bin",     crdr);
    write_varlen_col(D+"/tlabel.offsets", D+"/tlabel.data", tlabel);
    write_varlen_col(D+"/doc.offsets",    D+"/doc.data",    doc);
    write_dict_file(D+"/datatype.dict",  dtype_dict.strs);

    fprintf(stderr, "[tag] done: %u rows\n", N);
}

// ============================================================
// main
// ============================================================
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <data_dir> <gendb_dir>\n", argv[0]);
        return 1;
    }
    std::string data = argv[1];
    std::string db   = argv[2];

    // Create directory structure
    for (auto& d : {"sub","num","tag","pre","shared","indexes"})
        fs::create_directories(db + "/" + d);

    try {
        parse_sub(data+"/sub.csv", db);

        // Write shared tag/version dicts after num (will be extended by pre parse)
        parse_num(data+"/num.csv", db);
        parse_pre(data+"/pre.csv", db);

        // Write shared dicts (built incrementally by num then pre)
        write_dict_file(db+"/shared/tag_numpre.dict",     g_tag_dict.strs);
        write_dict_file(db+"/shared/version_numpre.dict", g_ver_dict.strs);

        parse_tag(data+"/tag.csv", db);

    } catch (const std::exception& e) {
        fprintf(stderr, "ERROR: %s\n", e.what());
        return 1;
    }

    fprintf(stderr, "\n=== Ingestion complete ===\n");
    fprintf(stderr, "  adsh dict entries : %zu\n", g_adsh_map.size());
    fprintf(stderr, "  tag  dict entries : %zu\n", g_tag_dict.strs.size());
    fprintf(stderr, "  ver  dict entries : %zu\n", g_ver_dict.strs.size());
    return 0;
}
