// ingest.cpp — SEC EDGAR binary columnar ingestion
// Parallel CSV → binary column files for sf3.gendb
// Sort: sub by (fy,sic), num by (uom,ddate), pre by (stmt)
// Dict files written after all tables parsed (consistent cross-table dicts)

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <numeric>
#include <cstring>
#include <cmath>
#include <limits>
#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <omp.h>
#include <parallel/algorithm>

namespace fs = std::filesystem;
using namespace std;

// ============================================================================
// DictEncoder — thread-safe string → int32_t code map
// code -1 = NULL/empty (not stored in map)
// codes 0, 1, 2, ... for real values
// ============================================================================
struct DictEncoder {
    unordered_map<string,int32_t> s2c;
    vector<string> c2s;
    mutable shared_mutex mu;

    int32_t encode(const string& s) {
        if (s.empty()) return -1;
        { shared_lock lk(mu); auto it = s2c.find(s); if (it != s2c.end()) return it->second; }
        unique_lock lk(mu);
        auto it = s2c.find(s); if (it != s2c.end()) return it->second;
        int32_t c = (int32_t)c2s.size();
        s2c[s] = c; c2s.push_back(s);
        return c;
    }

    // Write dict: line N → code N
    void write_dict(const string& path) const {
        shared_lock lk(mu);
        FILE* f = fopen(path.c_str(), "w");
        if (!f) { perror(path.c_str()); return; }
        for (auto& s : c2s) { fwrite(s.c_str(), 1, s.size(), f); fputc('\n', f); }
        fclose(f);
    }

    size_t size() const { shared_lock lk(mu); return c2s.size(); }
};

// ============================================================================
// CSV field parser — handles quoted fields (RFC 4180), empty fields
// ============================================================================
static void parse_fields(const char* p, const char* end, vector<string>& out) {
    out.clear();
    for (;;) {
        if (p >= end || *p == '\n' || *p == '\r') break;
        string fld;
        if (*p == '"') {
            ++p; // skip opening quote
            const char* qs = p;
            // fast path: find closing quote, no embedded quotes
            while (p < end && *p != '"' && *p != '\n' && *p != '\r') ++p;
            if (p < end && *p == '"') {
                fld.assign(qs, p); ++p; // skip closing quote
            } else {
                // slow path: embedded "" quotes
                fld.assign(qs, p);
                while (p < end && *p != '\n' && *p != '\r') {
                    if (*p == '"') {
                        if (p+1 < end && *(p+1) == '"') { fld += '"'; p += 2; }
                        else { ++p; break; }
                    } else { fld += *p++; }
                }
            }
        } else {
            const char* fs = p;
            while (p < end && *p != ',' && *p != '\n' && *p != '\r') ++p;
            fld.assign(fs, p);
        }
        out.push_back(move(fld));
        if (p < end && *p == ',') ++p; else break;
    }
}

// ============================================================================
// Helpers
// ============================================================================
// Split file body into N chunks aligned to line boundaries
static vector<size_t> find_splits(const char* data, size_t size, int n) {
    vector<size_t> s; s.reserve(n+1); s.push_back(0);
    for (int i = 1; i < n; ++i) {
        size_t pos = (size_t)i * size / n;
        while (pos < size && data[pos] != '\n') ++pos;
        if (pos < size) ++pos;
        s.push_back(pos);
    }
    s.push_back(size);
    return s;
}

// Write binary column to file
template<typename T>
static void write_col(const string& path, const vector<T>& v) {
    int fd = open(path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
    if (fd < 0) { perror(("open:"+path).c_str()); return; }
    const char* ptr = (const char*)v.data();
    size_t total = v.size() * sizeof(T), written = 0;
    while (written < total) {
        ssize_t r = ::write(fd, ptr+written, total-written);
        if (r <= 0) { perror("write"); break; }
        written += r;
    }
    close(fd);
}

// mmap a file for reading
static const char* mmap_file(const string& path, size_t& sz) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); return nullptr; }
    struct stat sb; fstat(fd, &sb); sz = sb.st_size;
    const char* d = (const char*)mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (d == MAP_FAILED) { perror("mmap"); return nullptr; }
    madvise((void*)d, sz, MADV_SEQUENTIAL);
    return d;
}

// Apply sort permutation to a column vector (returns sorted copy)
template<typename T>
static vector<T> apply_perm(const vector<T>& v, const vector<int32_t>& perm) {
    vector<T> tmp(v.size());
    for (size_t i = 0; i < v.size(); ++i) tmp[i] = v[perm[i]];
    return tmp;
}

// ============================================================================
// Global dictionaries — shared across all tables for consistent encoding
// ============================================================================
static DictEncoder adsh_dict;    // sub.adsh / num.adsh / pre.adsh
static DictEncoder tag_dict;     // tag.tag / num.tag / pre.tag
static DictEncoder version_dict; // tag.version / num.version / pre.version
static DictEncoder name_dict;    // sub.name
static DictEncoder tlabel_dict;  // tag.tlabel
static DictEncoder uom_dict;     // num.uom
static DictEncoder stmt_dict;    // pre.stmt
static DictEncoder rfile_dict;   // pre.rfile
static DictEncoder plabel_dict;  // pre.plabel

// ============================================================================
// SUB ingestion — single-threaded (86K rows), sort by (fy, sic)
// CSV cols: adsh(0),cik(1),name(2),sic(3),...,fy(10),...
// ============================================================================
static void ingest_sub(const string& infile, const string& out_dir) {
    cout << "[sub] parsing " << infile << " ..." << flush;
    size_t sz; const char* data = mmap_file(infile, sz);
    if (!data) return;

    vector<int32_t> adsh_col, cik_col, name_col, sic_col, fy_col;
    adsh_col.reserve(90000); cik_col.reserve(90000); name_col.reserve(90000);
    sic_col.reserve(90000);  fy_col.reserve(90000);

    vector<string> fields;
    const char* p = data; const char* end = data + sz;
    // skip header
    while (p < end && *p != '\n') ++p; if (p < end) ++p;

    while (p < end) {
        const char* ls = p;
        while (p < end && *p != '\n') ++p;
        const char* le = p; if (p < end) ++p;
        if (ls >= le) continue;

        parse_fields(ls, le, fields);
        if ((int)fields.size() < 11) continue;

        adsh_col.push_back(adsh_dict.encode(fields[0]));
        cik_col.push_back(fields[1].empty() ? 0 : stoi(fields[1]));
        name_col.push_back(name_dict.encode(fields[2]));
        sic_col.push_back(fields[3].empty() ? 0 : stoi(fields[3]));
        fy_col.push_back(fields[10].empty() ? 0 : stoi(fields[10]));
    }
    munmap((void*)data, sz);

    size_t N = adsh_col.size();
    cout << " " << N << " rows. Sorting..." << flush;

    // Sort by (fy, sic)
    vector<int32_t> perm(N); iota(perm.begin(), perm.end(), 0);
    sort(perm.begin(), perm.end(), [&](int32_t a, int32_t b) {
        if (fy_col[a] != fy_col[b]) return fy_col[a] < fy_col[b];
        return sic_col[a] < sic_col[b];
    });

    adsh_col = apply_perm(adsh_col, perm); cik_col  = apply_perm(cik_col,  perm);
    name_col = apply_perm(name_col, perm); sic_col  = apply_perm(sic_col,  perm);
    fy_col   = apply_perm(fy_col,   perm);

    write_col(out_dir + "/adsh.bin", adsh_col);
    write_col(out_dir + "/cik.bin",  cik_col);
    write_col(out_dir + "/name.bin", name_col);
    write_col(out_dir + "/sic.bin",  sic_col);
    write_col(out_dir + "/fy.bin",   fy_col);
    cout << " done." << endl;
}

// ============================================================================
// TAG ingestion — single-threaded (1M rows), natural order
// CSV cols: tag(0),version(1),custom(2),abstract(3),datatype(4),iord(5),crdr(6),tlabel(7),doc(8)
// ============================================================================
static void ingest_tag(const string& infile, const string& out_dir) {
    cout << "[tag] parsing " << infile << " ..." << flush;
    size_t sz; const char* data = mmap_file(infile, sz);
    if (!data) return;

    vector<int32_t> tag_col, ver_col, abs_col, tlabel_col;
    tag_col.reserve(1100000); ver_col.reserve(1100000);
    abs_col.reserve(1100000); tlabel_col.reserve(1100000);

    vector<string> fields;
    const char* p = data; const char* end = data + sz;
    while (p < end && *p != '\n') ++p; if (p < end) ++p; // skip header

    while (p < end) {
        const char* ls = p;
        while (p < end && *p != '\n') ++p;
        const char* le = p; if (p < end) ++p;
        if (ls >= le) continue;

        parse_fields(ls, le, fields);
        if ((int)fields.size() < 8) continue;

        tag_col.push_back(tag_dict.encode(fields[0]));
        ver_col.push_back(version_dict.encode(fields[1]));
        abs_col.push_back(fields[3].empty() ? 0 : stoi(fields[3]));
        tlabel_col.push_back(tlabel_dict.encode(fields[7]));
    }
    munmap((void*)data, sz);

    cout << " " << tag_col.size() << " rows. Writing..." << flush;
    write_col(out_dir + "/tag.bin",      tag_col);
    write_col(out_dir + "/version.bin",  ver_col);
    write_col(out_dir + "/abstract.bin", abs_col);
    write_col(out_dir + "/tlabel.bin",   tlabel_col);
    cout << " done." << endl;
}

// ============================================================================
// NUM ingestion — parallel (39M rows), sort by (uom_code, ddate)
// CSV cols: adsh(0),tag(1),version(2),ddate(3),qtrs(4),uom(5),coreg(6),value(7),footnote(8)
// ============================================================================
static void ingest_num(const string& infile, const string& out_dir) {
    cout << "[num] mmap " << infile << " ..." << flush;
    size_t sz; const char* data = mmap_file(infile, sz);
    if (!data) return;
    cout << " " << sz/(1024*1024) << " MB" << endl;

    // Skip header
    size_t hdr = 0; while (hdr < sz && data[hdr] != '\n') ++hdr; ++hdr;
    const char* body = data + hdr; size_t bsz = sz - hdr;

    int NT = omp_get_max_threads();
    cout << "[num] parallel parse with " << NT << " threads..." << flush;

    vector<vector<int32_t>> t_adsh(NT), t_tag(NT), t_ver(NT), t_ddate(NT);
    vector<vector<int16_t>> t_uom(NT);
    vector<vector<double>>  t_val(NT);

    size_t est = 39401761 / NT + 100000;
    for (int t = 0; t < NT; ++t) {
        t_adsh[t].reserve(est); t_tag[t].reserve(est);
        t_ver[t].reserve(est);  t_ddate[t].reserve(est);
        t_uom[t].reserve(est);  t_val[t].reserve(est);
    }

    auto splits = find_splits(body, bsz, NT);

    #pragma omp parallel for schedule(static) num_threads(NT)
    for (int t = 0; t < NT; ++t) {
        vector<string> fields;
        const char* p   = body + splits[t];
        const char* end = body + splits[t+1];
        while (p < end) {
            const char* ls = p;
            while (p < end && *p != '\n') ++p;
            const char* le = p; if (p < end) ++p;
            if (ls >= le) continue;

            parse_fields(ls, le, fields);
            if ((int)fields.size() < 8) continue;

            t_adsh[t].push_back(adsh_dict.encode(fields[0]));
            t_tag[t].push_back(tag_dict.encode(fields[1]));
            t_ver[t].push_back(version_dict.encode(fields[2]));
            t_ddate[t].push_back(fields[3].empty() ? 0 : stoi(fields[3]));
            t_uom[t].push_back((int16_t)uom_dict.encode(fields[5]));
            double v = fields[7].empty()
                       ? numeric_limits<double>::quiet_NaN()
                       : stod(fields[7]);
            t_val[t].push_back(v);
        }
    }

    munmap((void*)data, sz);

    // Merge thread-local vectors
    size_t total = 0;
    for (int t = 0; t < NT; ++t) total += t_adsh[t].size();
    cout << " " << total << " rows. Merging..." << flush;

    vector<int32_t> adsh_col, tag_col, ver_col, ddate_col;
    vector<int16_t> uom_col;
    vector<double>  val_col;
    adsh_col.reserve(total); tag_col.reserve(total);  ver_col.reserve(total);
    ddate_col.reserve(total); uom_col.reserve(total); val_col.reserve(total);

    for (int t = 0; t < NT; ++t) {
        adsh_col.insert(adsh_col.end(), t_adsh[t].begin(), t_adsh[t].end());
        tag_col.insert(tag_col.end(),   t_tag[t].begin(),  t_tag[t].end());
        ver_col.insert(ver_col.end(),   t_ver[t].begin(),  t_ver[t].end());
        ddate_col.insert(ddate_col.end(),t_ddate[t].begin(),t_ddate[t].end());
        uom_col.insert(uom_col.end(),   t_uom[t].begin(),  t_uom[t].end());
        val_col.insert(val_col.end(),   t_val[t].begin(),  t_val[t].end());
        // free thread-local memory
        { vector<int32_t>().swap(t_adsh[t]); vector<int32_t>().swap(t_tag[t]); }
        { vector<int32_t>().swap(t_ver[t]);  vector<int32_t>().swap(t_ddate[t]); }
        { vector<int16_t>().swap(t_uom[t]);  vector<double>().swap(t_val[t]); }
    }

    cout << " Sorting..." << flush;
    vector<int32_t> perm(total); iota(perm.begin(), perm.end(), 0);
    __gnu_parallel::sort(perm.begin(), perm.end(), [&](int32_t a, int32_t b) {
        if (uom_col[a] != uom_col[b]) return uom_col[a] < uom_col[b];
        return ddate_col[a] < ddate_col[b];
    });

    cout << " Permuting..." << flush;
    adsh_col  = apply_perm(adsh_col,  perm);
    tag_col   = apply_perm(tag_col,   perm);
    ver_col   = apply_perm(ver_col,   perm);
    ddate_col = apply_perm(ddate_col, perm);
    uom_col   = apply_perm(uom_col,   perm);
    val_col   = apply_perm(val_col,   perm);
    { vector<int32_t>().swap(perm); }

    cout << " Writing..." << flush;
    write_col(out_dir + "/adsh.bin",    adsh_col);
    write_col(out_dir + "/tag.bin",     tag_col);
    write_col(out_dir + "/version.bin", ver_col);
    write_col(out_dir + "/ddate.bin",   ddate_col);
    write_col(out_dir + "/uom.bin",     uom_col);
    write_col(out_dir + "/value.bin",   val_col);
    cout << " done." << endl;
}

// ============================================================================
// PRE ingestion — parallel (9.6M rows), sort by stmt_code
// CSV cols: adsh(0),report(1),line(2),stmt(3),inpth(4),rfile(5),tag(6),version(7),plabel(8),negating(9)
// ============================================================================
static void ingest_pre(const string& infile, const string& out_dir) {
    cout << "[pre] mmap " << infile << " ..." << flush;
    size_t sz; const char* data = mmap_file(infile, sz);
    if (!data) return;
    cout << " " << sz/(1024*1024) << " MB" << endl;

    size_t hdr = 0; while (hdr < sz && data[hdr] != '\n') ++hdr; ++hdr;
    const char* body = data + hdr; size_t bsz = sz - hdr;

    int NT = omp_get_max_threads();
    cout << "[pre] parallel parse with " << NT << " threads..." << flush;

    vector<vector<int32_t>> t_adsh(NT), t_tag(NT), t_ver(NT), t_line(NT), t_plabel(NT);
    vector<vector<int16_t>> t_stmt(NT), t_rfile(NT);

    size_t est = 9600799 / NT + 50000;
    for (int t = 0; t < NT; ++t) {
        t_adsh[t].reserve(est);  t_tag[t].reserve(est);
        t_ver[t].reserve(est);   t_line[t].reserve(est);
        t_plabel[t].reserve(est);t_stmt[t].reserve(est);
        t_rfile[t].reserve(est);
    }

    auto splits = find_splits(body, bsz, NT);

    #pragma omp parallel for schedule(static) num_threads(NT)
    for (int t = 0; t < NT; ++t) {
        vector<string> fields;
        const char* p   = body + splits[t];
        const char* end = body + splits[t+1];
        while (p < end) {
            const char* ls = p;
            while (p < end && *p != '\n') ++p;
            const char* le = p; if (p < end) ++p;
            if (ls >= le) continue;

            parse_fields(ls, le, fields);
            if ((int)fields.size() < 9) continue;

            t_adsh[t].push_back(adsh_dict.encode(fields[0]));
            t_line[t].push_back(fields[2].empty() ? 0 : stoi(fields[2]));
            t_stmt[t].push_back((int16_t)stmt_dict.encode(fields[3]));
            t_rfile[t].push_back((int16_t)rfile_dict.encode(fields[5]));
            t_tag[t].push_back(tag_dict.encode(fields[6]));
            t_ver[t].push_back(version_dict.encode(fields[7]));
            t_plabel[t].push_back(plabel_dict.encode(fields[8]));
        }
    }

    munmap((void*)data, sz);

    size_t total = 0;
    for (int t = 0; t < NT; ++t) total += t_adsh[t].size();
    cout << " " << total << " rows. Merging..." << flush;

    vector<int32_t> adsh_col, tag_col, ver_col, line_col, plabel_col;
    vector<int16_t> stmt_col, rfile_col;
    adsh_col.reserve(total);  tag_col.reserve(total);   ver_col.reserve(total);
    line_col.reserve(total);  plabel_col.reserve(total);
    stmt_col.reserve(total);  rfile_col.reserve(total);

    for (int t = 0; t < NT; ++t) {
        adsh_col.insert(adsh_col.end(),   t_adsh[t].begin(),  t_adsh[t].end());
        tag_col.insert(tag_col.end(),     t_tag[t].begin(),   t_tag[t].end());
        ver_col.insert(ver_col.end(),     t_ver[t].begin(),   t_ver[t].end());
        line_col.insert(line_col.end(),   t_line[t].begin(),  t_line[t].end());
        stmt_col.insert(stmt_col.end(),   t_stmt[t].begin(),  t_stmt[t].end());
        rfile_col.insert(rfile_col.end(), t_rfile[t].begin(), t_rfile[t].end());
        plabel_col.insert(plabel_col.end(),t_plabel[t].begin(),t_plabel[t].end());
        { vector<int32_t>().swap(t_adsh[t]); vector<int32_t>().swap(t_tag[t]); }
        { vector<int32_t>().swap(t_ver[t]);  vector<int32_t>().swap(t_line[t]); }
        { vector<int32_t>().swap(t_plabel[t]);}
        { vector<int16_t>().swap(t_stmt[t]); vector<int16_t>().swap(t_rfile[t]); }
    }

    cout << " Sorting..." << flush;
    vector<int32_t> perm(total); iota(perm.begin(), perm.end(), 0);
    __gnu_parallel::sort(perm.begin(), perm.end(), [&](int32_t a, int32_t b) {
        return stmt_col[a] < stmt_col[b];
    });

    cout << " Permuting..." << flush;
    adsh_col   = apply_perm(adsh_col,   perm);
    tag_col    = apply_perm(tag_col,    perm);
    ver_col    = apply_perm(ver_col,    perm);
    line_col   = apply_perm(line_col,   perm);
    stmt_col   = apply_perm(stmt_col,   perm);
    rfile_col  = apply_perm(rfile_col,  perm);
    plabel_col = apply_perm(plabel_col, perm);
    { vector<int32_t>().swap(perm); }

    cout << " Writing..." << flush;
    write_col(out_dir + "/adsh.bin",    adsh_col);
    write_col(out_dir + "/line.bin",    line_col);
    write_col(out_dir + "/stmt.bin",    stmt_col);
    write_col(out_dir + "/rfile.bin",   rfile_col);
    write_col(out_dir + "/tag.bin",     tag_col);
    write_col(out_dir + "/version.bin", ver_col);
    write_col(out_dir + "/plabel.bin",  plabel_col);
    cout << " done." << endl;
}

// ============================================================================
// main
// ============================================================================
int main(int argc, char* argv[]) {
    if (argc < 3) {
        cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>\n";
        return 1;
    }
    string data_dir = argv[1];
    string db_dir   = argv[2];

    // Verify directories exist
    if (!fs::exists(data_dir)) { cerr << "data_dir not found: " << data_dir << "\n"; return 1; }
    fs::create_directories(db_dir + "/sub");
    fs::create_directories(db_dir + "/num");
    fs::create_directories(db_dir + "/tag");
    fs::create_directories(db_dir + "/pre");
    fs::create_directories(db_dir + "/indexes");

    // Ingest tables in dependency order:
    // sub first  → builds adsh_dict, name_dict
    // tag second → builds tag_dict, version_dict, tlabel_dict
    // num third  → uses adsh/tag/version dicts; builds uom_dict
    // pre fourth → uses adsh/tag/version dicts; builds stmt/rfile/plabel dicts

    ingest_sub(data_dir + "/sub.csv", db_dir + "/sub");
    ingest_tag(data_dir + "/tag.csv", db_dir + "/tag");
    ingest_num(data_dir + "/num.csv", db_dir + "/num");
    ingest_pre(data_dir + "/pre.csv", db_dir + "/pre");

    // Write all dicts after all tables parsed (ensures cross-table consistency)
    cout << "[dicts] writing all dictionary files..." << flush;

    // adsh dict: shared by sub, num, pre
    adsh_dict.write_dict(db_dir + "/sub/adsh_dict.txt");
    adsh_dict.write_dict(db_dir + "/num/adsh_dict.txt");
    adsh_dict.write_dict(db_dir + "/pre/adsh_dict.txt");

    // tag dict: shared by tag, num, pre
    tag_dict.write_dict(db_dir + "/tag/tag_dict.txt");
    tag_dict.write_dict(db_dir + "/num/tag_dict.txt");
    tag_dict.write_dict(db_dir + "/pre/tag_dict.txt");

    // version dict: shared by tag, num, pre
    version_dict.write_dict(db_dir + "/tag/version_dict.txt");
    version_dict.write_dict(db_dir + "/num/version_dict.txt");
    version_dict.write_dict(db_dir + "/pre/version_dict.txt");

    // table-specific dicts
    name_dict.write_dict(db_dir + "/sub/name_dict.txt");
    tlabel_dict.write_dict(db_dir + "/tag/tlabel_dict.txt");
    uom_dict.write_dict(db_dir + "/num/uom_dict.txt");
    stmt_dict.write_dict(db_dir + "/pre/stmt_dict.txt");
    rfile_dict.write_dict(db_dir + "/pre/rfile_dict.txt");
    plabel_dict.write_dict(db_dir + "/pre/plabel_dict.txt");

    cout << " done." << endl;

    // Print dict sizes for verification
    cout << "[summary]"
         << " adsh=" << adsh_dict.size()
         << " tag="  << tag_dict.size()
         << " version=" << version_dict.size()
         << " uom=" << uom_dict.size()
         << " stmt=" << stmt_dict.size()
         << " plabel=" << plabel_dict.size()
         << endl;

    return 0;
}
