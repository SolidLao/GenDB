// SEC EDGAR Data Ingestion
// Usage: ./ingest <data_dir> <gendb_dir>
// Reads sub.csv, tag.csv, pre.csv, num.csv → writes binary columnar data

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <chrono>

using namespace std;
using namespace std::chrono;

// ====== Binary Record Formats ======
// Each binary file starts with uint64_t row_count, then packed records

#pragma pack(push,1)
struct SubRec {
    uint32_t adsh_id;
    uint32_t cik;
    uint32_t name_id;
    int32_t  sic;       // -1 = NULL
    int32_t  fy;        // -1 = NULL
    uint32_t filed;     // YYYYMMDD, 0 = NULL
    uint8_t  wksi;
    uint8_t  _pad[3];
};  // 24 bytes

struct TagRec {
    uint32_t tag_id;
    uint32_t ver_id;
    uint32_t tlabel_id;
    uint8_t  abstract;
    uint8_t  custom;
    uint8_t  crdr;   // 0='C', 1='D', 2=NULL
    uint8_t  iord;   // 0='I', 1='D', 2=NULL
};  // 16 bytes

struct PreRec {
    uint32_t adsh_id;
    uint32_t tag_id;
    uint32_t ver_id;
    uint32_t plabel_id;
    uint32_t line;
    uint8_t  stmt_id;
    uint8_t  rfile_id;
    uint8_t  inpth;
    uint8_t  negating;
};  // 24 bytes

struct NumRec {
    double   value;     // NaN = NULL
    uint32_t adsh_id;
    uint32_t tag_id;
    uint32_t ver_id;
    uint32_t ddate;     // YYYYMMDD
    uint8_t  uom_id;
    uint8_t  qtrs;
    uint8_t  has_value; // 1 = value not NULL
    uint8_t  _pad;
};  // 28 bytes
#pragma pack(pop)

static_assert(sizeof(SubRec) == 28, "SubRec size");
static_assert(sizeof(TagRec) == 16, "TagRec size");
static_assert(sizeof(PreRec) == 24, "PreRec size");
static_assert(sizeof(NumRec) == 28, "NumRec size");

// ====== Dictionary Builder ======
struct DictBuilder {
    unordered_map<string, uint32_t> map;
    vector<string> strings;

    DictBuilder() {
        map[""] = 0;
        strings.push_back(""); // ID 0 = empty/NULL
    }

    uint32_t get_or_add(const char* s, size_t len) {
        // Use string_view-style lookup to avoid allocation on hit
        string key(s, len);
        auto [it, inserted] = map.emplace(key, (uint32_t)strings.size());
        if (inserted) strings.push_back(key);
        return it->second;
    }

    uint32_t get(const char* s, size_t len) const {
        auto it = map.find(string(s, len));
        return it != map.end() ? it->second : 0;
    }
};

// ====== MMap File ======
struct MmapFile {
    const char* data;
    size_t size;
    int fd;

    MmapFile() : data(nullptr), size(0), fd(-1) {}
    MmapFile(const char* path) {
        fd = open(path, O_RDONLY);
        if (fd < 0) { data = nullptr; size = 0; return; }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (data == MAP_FAILED) { data = nullptr; size = 0; }
        else madvise((void*)data, size, MADV_SEQUENTIAL);
    }
    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap((void*)data, size);
        if (fd >= 0) close(fd);
    }
};

// ====== CSV Field Parser ======
static char g_qbuf[65536];

// Parse one CSV field. Returns pointer past field (past separator).
static const char* parse_field(const char* p, const char* end, const char*& fptr, size_t& flen) {
    if (p >= end) { fptr = p; flen = 0; return p; }
    if (*p == '"') {
        p++;
        size_t ql = 0;
        while (p < end) {
            if (*p == '"') {
                if (p+1 < end && p[1] == '"') { if (ql < sizeof(g_qbuf)-1) g_qbuf[ql++] = '"'; p += 2; }
                else { p++; break; }
            } else {
                if (ql < sizeof(g_qbuf)-1) g_qbuf[ql++] = *p;
                p++;
            }
        }
        fptr = g_qbuf; flen = ql;
    } else {
        fptr = p;
        while (p < end && *p != ',' && *p != '\n' && *p != '\r') p++;
        flen = p - fptr;
    }
    if (p < end && *p == ',') { p++; return p; }
    if (p < end && *p == '\r') p++;
    if (p < end && *p == '\n') p++;
    return p;
}

static const char* skip_line(const char* p, const char* end) {
    while (p < end && *p != '\n') p++;
    if (p < end) p++;
    return p;
}

static int64_t fast_int(const char* s, size_t l) {
    if (l == 0) return -1;
    int64_t v = 0; size_t i = 0;
    bool neg = (s[0] == '-'); if (neg) i++;
    for (; i < l; i++) { if (s[i] < '0' || s[i] > '9') return -1; v = v*10+(s[i]-'0'); }
    return neg ? -v : v;
}

static double fast_double(const char* s, size_t l) {
    if (l == 0) return NAN;
    char buf[64]; if (l >= 64) l = 63;
    memcpy(buf, s, l); buf[l] = '\0';
    char* ep; double v = strtod(buf, &ep);
    return (ep == buf) ? NAN : v;
}

// ====== Write Dict File ======
// Format: uint32_t count, uint32_t offsets[count+1], char data[]
static void write_dict(const string& path, const DictBuilder& d) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot write %s\n", path.c_str()); return; }
    uint32_t n = (uint32_t)d.strings.size();
    fwrite(&n, 4, 1, f);
    uint32_t off = 0;
    for (uint32_t i = 0; i < n; i++) { fwrite(&off, 4, 1, f); off += (uint32_t)d.strings[i].size(); }
    fwrite(&off, 4, 1, f);
    for (const auto& s : d.strings) if (!s.empty()) fwrite(s.data(), 1, s.size(), f);
    fclose(f);
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <data_dir> <gendb_dir>\n", argv[0]); return 1; }
    string data_dir = argv[1];
    string gendb_dir = argv[2];

    auto t0 = high_resolution_clock::now();

    DictBuilder adsh_dict, name_dict, tag_dict, ver_dict, tlabel_dict, plabel_dict;
    DictBuilder uom_dict, stmt_dict, rfile_dict;

    // ============================================================
    // Phase 1: sub.csv
    // sub cols: adsh(0),cik(1),name(2),sic(3),countryba(4),stprba(5),
    //           cityba(6),countryinc(7),form(8),period(9),fy(10),fp(11),
    //           filed(12),accepted(13),prevrpt(14),nciks(15),afs(16),
    //           wksi(17),fye(18),instance(19)
    // ============================================================
    {
        string path = data_dir + "/sub.csv";
        MmapFile mf(path.c_str());
        if (!mf.data) { fprintf(stderr, "Cannot open %s\n", path.c_str()); return 1; }

        string op = gendb_dir + "/sub.bin";
        FILE* out = fopen(op.c_str(), "wb");
        uint64_t rc = 0; fwrite(&rc, 8, 1, out);

        const char* p = mf.data, *end = mf.data + mf.size;
        p = skip_line(p, end); // header

        const char* fp; size_t fl;
        while (p < end && p[0] != '\0') {
            if (*p == '\r' || *p == '\n') { p = skip_line(p, end); continue; }
            SubRec rec; memset(&rec, 0, sizeof(rec));

            p = parse_field(p,end,fp,fl); rec.adsh_id = adsh_dict.get_or_add(fp,fl); // 0
            p = parse_field(p,end,fp,fl); rec.cik = (uint32_t)fast_int(fp,fl);       // 1
            p = parse_field(p,end,fp,fl); rec.name_id = name_dict.get_or_add(fp,fl); // 2
            p = parse_field(p,end,fp,fl); rec.sic = (int32_t)fast_int(fp,fl);         // 3
            p = parse_field(p,end,fp,fl); // 4 countryba skip
            p = parse_field(p,end,fp,fl); // 5 stprba skip
            p = parse_field(p,end,fp,fl); // 6 cityba skip
            p = parse_field(p,end,fp,fl); // 7 countryinc skip
            p = parse_field(p,end,fp,fl); // 8 form skip
            p = parse_field(p,end,fp,fl); // 9 period skip
            p = parse_field(p,end,fp,fl); rec.fy = (int32_t)fast_int(fp,fl);          // 10
            p = parse_field(p,end,fp,fl); // 11 fp skip
            p = parse_field(p,end,fp,fl); rec.filed = (uint32_t)fast_int(fp,fl);      // 12
            p = parse_field(p,end,fp,fl); // 13 accepted skip
            p = parse_field(p,end,fp,fl); // 14 prevrpt skip
            p = parse_field(p,end,fp,fl); // 15 nciks skip
            p = parse_field(p,end,fp,fl); // 16 afs skip
            p = parse_field(p,end,fp,fl); rec.wksi = (uint8_t)fast_int(fp,fl);        // 17
            p = skip_line(p, end); // skip 18,19

            fwrite(&rec, sizeof(rec), 1, out);
            rc++;
        }

        fseek(out, 0, SEEK_SET); fwrite(&rc, 8, 1, out); fclose(out);
        fprintf(stderr, "sub: %llu rows\n", (unsigned long long)rc);
    }

    // ============================================================
    // Phase 2: tag.csv
    // tag cols: tag(0),version(1),custom(2),abstract(3),datatype(4),
    //           iord(5),crdr(6),tlabel(7),doc(8)
    // ============================================================
    {
        string path = data_dir + "/tag.csv";
        MmapFile mf(path.c_str());
        if (!mf.data) { fprintf(stderr, "Cannot open %s\n", path.c_str()); return 1; }

        string op = gendb_dir + "/tag.bin";
        FILE* out = fopen(op.c_str(), "wb");
        uint64_t rc = 0; fwrite(&rc, 8, 1, out);

        const char* p = mf.data, *end = mf.data + mf.size;
        p = skip_line(p, end);

        const char* fp; size_t fl;
        while (p < end && p[0] != '\0') {
            if (*p == '\r' || *p == '\n') { p = skip_line(p, end); continue; }
            TagRec rec; memset(&rec, 0, sizeof(rec));

            p = parse_field(p,end,fp,fl); rec.tag_id = tag_dict.get_or_add(fp,fl);     // 0
            p = parse_field(p,end,fp,fl); rec.ver_id = ver_dict.get_or_add(fp,fl);     // 1
            p = parse_field(p,end,fp,fl); rec.custom = (uint8_t)fast_int(fp,fl);        // 2
            p = parse_field(p,end,fp,fl); rec.abstract = (uint8_t)fast_int(fp,fl);      // 3
            p = parse_field(p,end,fp,fl); // 4 datatype skip
            p = parse_field(p,end,fp,fl); // 5 iord
            rec.iord = (fl > 0) ? ((fp[0]=='I') ? 0 : 1) : 2;
            p = parse_field(p,end,fp,fl); // 6 crdr
            rec.crdr = (fl > 0) ? ((fp[0]=='C') ? 0 : 1) : 2;
            p = parse_field(p,end,fp,fl); rec.tlabel_id = tlabel_dict.get_or_add(fp,fl); // 7
            p = skip_line(p, end); // 8 doc skip

            fwrite(&rec, sizeof(rec), 1, out);
            rc++;
        }

        fseek(out, 0, SEEK_SET); fwrite(&rc, 8, 1, out); fclose(out);
        fprintf(stderr, "tag: %llu rows\n", (unsigned long long)rc);
    }

    // ============================================================
    // Phase 3: pre.csv
    // pre cols: adsh(0),report(1),line(2),stmt(3),inpth(4),rfile(5),
    //           tag(6),version(7),plabel(8),negating(9)
    // ============================================================
    {
        string path = data_dir + "/pre.csv";
        MmapFile mf(path.c_str());
        if (!mf.data) { fprintf(stderr, "Cannot open %s\n", path.c_str()); return 1; }

        string op = gendb_dir + "/pre.bin";
        FILE* out = fopen(op.c_str(), "wb");
        uint64_t rc = 0; fwrite(&rc, 8, 1, out);

        const char* p = mf.data, *end = mf.data + mf.size;
        p = skip_line(p, end);

        const char* fp; size_t fl;
        while (p < end && p[0] != '\0') {
            if (*p == '\r' || *p == '\n') { p = skip_line(p, end); continue; }
            PreRec rec; memset(&rec, 0, sizeof(rec));

            p = parse_field(p,end,fp,fl); rec.adsh_id = adsh_dict.get_or_add(fp,fl);  // 0
            p = parse_field(p,end,fp,fl); // 1 report skip
            p = parse_field(p,end,fp,fl); rec.line = (uint32_t)fast_int(fp,fl);        // 2
            p = parse_field(p,end,fp,fl); rec.stmt_id = (uint8_t)stmt_dict.get_or_add(fp,fl); // 3
            p = parse_field(p,end,fp,fl); rec.inpth = (uint8_t)fast_int(fp,fl);        // 4
            p = parse_field(p,end,fp,fl); rec.rfile_id = (uint8_t)rfile_dict.get_or_add(fp,fl); // 5
            p = parse_field(p,end,fp,fl); rec.tag_id = tag_dict.get_or_add(fp,fl);    // 6
            p = parse_field(p,end,fp,fl); rec.ver_id = ver_dict.get_or_add(fp,fl);    // 7
            p = parse_field(p,end,fp,fl); rec.plabel_id = plabel_dict.get_or_add(fp,fl); // 8
            p = parse_field(p,end,fp,fl); rec.negating = (uint8_t)fast_int(fp,fl);    // 9
            // end of line handled by parse_field

            fwrite(&rec, sizeof(rec), 1, out);
            rc++;
        }

        fseek(out, 0, SEEK_SET); fwrite(&rc, 8, 1, out); fclose(out);
        fprintf(stderr, "pre: %llu rows\n", (unsigned long long)rc);
    }

    // ============================================================
    // Phase 4: num.csv
    // num cols: adsh(0),tag(1),version(2),ddate(3),qtrs(4),uom(5),
    //           coreg(6),value(7),footnote(8)
    // ============================================================
    {
        string path = data_dir + "/num.csv";
        MmapFile mf(path.c_str());
        if (!mf.data) { fprintf(stderr, "Cannot open %s\n", path.c_str()); return 1; }

        string op = gendb_dir + "/num.bin";
        FILE* out = fopen(op.c_str(), "wb");
        uint64_t rc = 0; fwrite(&rc, 8, 1, out);

        const char* p = mf.data, *end = mf.data + mf.size;
        p = skip_line(p, end);

        const char* fp; size_t fl;
        uint64_t lines = 0;
        while (p < end && p[0] != '\0') {
            if (*p == '\r' || *p == '\n') { p = skip_line(p, end); continue; }
            NumRec rec; memset(&rec, 0, sizeof(rec));

            p = parse_field(p,end,fp,fl); rec.adsh_id = adsh_dict.get_or_add(fp,fl);  // 0
            p = parse_field(p,end,fp,fl); rec.tag_id = tag_dict.get_or_add(fp,fl);    // 1
            p = parse_field(p,end,fp,fl); rec.ver_id = ver_dict.get_or_add(fp,fl);    // 2
            p = parse_field(p,end,fp,fl); rec.ddate = (uint32_t)fast_int(fp,fl);      // 3
            p = parse_field(p,end,fp,fl); rec.qtrs = (uint8_t)fast_int(fp,fl);        // 4
            p = parse_field(p,end,fp,fl); rec.uom_id = (uint8_t)uom_dict.get_or_add(fp,fl); // 5
            p = parse_field(p,end,fp,fl); // 6 coreg skip
            p = parse_field(p,end,fp,fl); // 7 value
            rec.value = fast_double(fp, fl);
            rec.has_value = !isnan(rec.value) ? 1 : 0;
            if (!rec.has_value) rec.value = 0.0;
            p = skip_line(p, end); // 8 footnote skip

            fwrite(&rec, sizeof(rec), 1, out);
            rc++;
            if (++lines % 5000000 == 0) fprintf(stderr, "num: %llu M rows...\n", lines/1000000);
        }

        fseek(out, 0, SEEK_SET); fwrite(&rc, 8, 1, out); fclose(out);
        fprintf(stderr, "num: %llu rows\n", (unsigned long long)rc);
    }

    // ============================================================
    // Write dictionaries
    // ============================================================
    write_dict(gendb_dir + "/dict_adsh.bin", adsh_dict);
    write_dict(gendb_dir + "/dict_name.bin", name_dict);
    write_dict(gendb_dir + "/dict_tag.bin", tag_dict);
    write_dict(gendb_dir + "/dict_ver.bin", ver_dict);
    write_dict(gendb_dir + "/dict_tlabel.bin", tlabel_dict);
    write_dict(gendb_dir + "/dict_plabel.bin", plabel_dict);
    write_dict(gendb_dir + "/dict_uom.bin", uom_dict);
    write_dict(gendb_dir + "/dict_stmt.bin", stmt_dict);
    write_dict(gendb_dir + "/dict_rfile.bin", rfile_dict);

    // ============================================================
    // Write metadata.json: string → ID mappings for small dicts
    // ============================================================
    {
        string meta_path = gendb_dir + "/metadata.json";
        FILE* f = fopen(meta_path.c_str(), "w");
        fprintf(f, "{\n");

        // UOM ids
        fprintf(f, "  \"uom_ids\": {");
        bool first = true;
        for (uint32_t i = 0; i < (uint32_t)uom_dict.strings.size(); i++) {
            if (!first) fprintf(f, ", ");
            fprintf(f, "\"%s\": %u", uom_dict.strings[i].c_str(), i);
            first = false;
        }
        fprintf(f, "},\n");

        // Stmt ids
        fprintf(f, "  \"stmt_ids\": {");
        first = true;
        for (uint32_t i = 0; i < (uint32_t)stmt_dict.strings.size(); i++) {
            if (!first) fprintf(f, ", ");
            fprintf(f, "\"%s\": %u", stmt_dict.strings[i].c_str(), i);
            first = false;
        }
        fprintf(f, "},\n");

        // Rfile ids
        fprintf(f, "  \"rfile_ids\": {");
        first = true;
        for (uint32_t i = 0; i < (uint32_t)rfile_dict.strings.size(); i++) {
            if (!first) fprintf(f, ", ");
            fprintf(f, "\"%s\": %u", rfile_dict.strings[i].c_str(), i);
            first = false;
        }
        fprintf(f, "}\n");

        fprintf(f, "}\n");
        fclose(f);
    }

    auto t1 = high_resolution_clock::now();
    double ms = duration<double, milli>(t1 - t0).count();
    fprintf(stderr, "\nIngestion complete in %.2f ms\n", ms);

    // Write ingestion timing
    {
        string tp = string(argv[2]) + "/../ingestion_results.json";
        // Find run dir
        string rdir = gendb_dir;
        // go up one level
        size_t pos = rdir.rfind('/');
        if (pos != string::npos) rdir = rdir.substr(0, pos);
        FILE* f = fopen((rdir + "/ingestion_results.json").c_str(), "w");
        if (f) {
            fprintf(f, "{\"ingestion_time_ms\": %.2f}\n", ms);
            fclose(f);
        }
    }

    fprintf(stderr, "Dicts: adsh=%zu name=%zu tag=%zu ver=%zu tlabel=%zu plabel=%zu uom=%zu stmt=%zu rfile=%zu\n",
        adsh_dict.strings.size(), name_dict.strings.size(),
        tag_dict.strings.size(), ver_dict.strings.size(),
        tlabel_dict.strings.size(), plabel_dict.strings.size(),
        uom_dict.strings.size(), stmt_dict.strings.size(), rfile_dict.strings.size());

    return 0;
}
