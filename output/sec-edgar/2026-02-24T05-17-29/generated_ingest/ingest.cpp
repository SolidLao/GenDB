// ingest.cpp — SEC EDGAR SF3 ingestion
// Reads CSV files from data_dir, writes binary columnar files to gendb_dir.
// Shared dictionaries: adsh (int32_t), tag (int32_t), version (int32_t).
// pre table is sorted by (adsh_code, tag_code, version_code) for index efficiency.
// num.value stored as double; C29: SUM must use int64_t cents in query code.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <cassert>
#include <climits>
#include <cfloat>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <numeric>
#include <chrono>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// ─────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────
static constexpr int32_t NULL_CODE32 = -1;
static constexpr int16_t NULL_CODE16 = -1;
static constexpr int32_t NULL_INT32  = INT32_MIN;

static double make_nan() {
    static const double nan_val = std::numeric_limits<double>::quiet_NaN();
    return nan_val;
}

// ─────────────────────────────────────────────────────────────
// Timing
// ─────────────────────────────────────────────────────────────
static double now_sec() {
    auto t = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(t.time_since_epoch()).count();
}

// ─────────────────────────────────────────────────────────────
// Directory helpers
// ─────────────────────────────────────────────────────────────
static void make_dir(const std::string& path) {
    std::string cmd = "mkdir -p \"" + path + "\"";
    if (system(cmd.c_str()) != 0) {
        fprintf(stderr, "Warning: mkdir -p failed for %s\n", path.c_str());
    }
}

// ─────────────────────────────────────────────────────────────
// CSV parser — handles quoted fields, escaped quotes ("")
// ─────────────────────────────────────────────────────────────
static void parse_csv_row(const char* line, std::vector<std::string>& out) {
    out.clear();
    const char* p = line;
    for (;;) {
        if (*p == '"') {
            p++;
            std::string field;
            field.reserve(64);
            while (*p) {
                if (*p == '"') {
                    if (*(p+1) == '"') { field += '"'; p += 2; }
                    else { p++; break; }
                } else if (*p == '\r' || *p == '\n') {
                    break;
                } else {
                    field += *p++;
                }
            }
            out.push_back(std::move(field));
        } else {
            const char* s = p;
            while (*p && *p != ',' && *p != '\r' && *p != '\n') p++;
            out.emplace_back(s, p - s);
        }
        if (*p == ',') { p++; }
        else { break; }
    }
}

// ─────────────────────────────────────────────────────────────
// Dictionary: string → int32_t code
// ─────────────────────────────────────────────────────────────
struct Dict32 {
    std::unordered_map<std::string, int32_t> lookup;
    std::vector<std::string> strings;

    int32_t encode(const std::string& s) {
        auto it = lookup.find(s);
        if (it != lookup.end()) return it->second;
        int32_t code = (int32_t)strings.size();
        lookup[s] = code;
        strings.push_back(s);
        return code;
    }

    int32_t encode(const char* s) {
        auto it = lookup.find(s);
        if (it != lookup.end()) return it->second;
        std::string key(s);
        int32_t code = (int32_t)strings.size();
        lookup[key] = code;
        strings.push_back(std::move(key));
        return code;
    }

    int32_t encode_nullable(const std::string& s) {
        if (s.empty()) return NULL_CODE32;
        return encode(s);
    }

    int32_t find_code(const std::string& s) const {
        auto it = lookup.find(s);
        return (it != lookup.end()) ? it->second : NULL_CODE32;
    }

    size_t size() const { return strings.size(); }

    void save(const std::string& path) const {
        FILE* f = fopen(path.c_str(), "w");
        if (!f) { perror(path.c_str()); exit(1); }
        for (const auto& s : strings) fprintf(f, "%s\n", s.c_str());
        fclose(f);
    }
};

// ─────────────────────────────────────────────────────────────
// Dictionary: string → int16_t code  (< 32767 distinct values)
// ─────────────────────────────────────────────────────────────
struct Dict16 {
    std::unordered_map<std::string, int16_t> lookup;
    std::vector<std::string> strings;

    int16_t encode(const std::string& s) {
        auto it = lookup.find(s);
        if (it != lookup.end()) return it->second;
        if (strings.size() >= 32767) {
            fprintf(stderr, "Dict16 overflow (>32767) adding '%s'\n", s.c_str());
            exit(1);
        }
        int16_t code = (int16_t)strings.size();
        lookup[s] = code;
        strings.push_back(s);
        return code;
    }

    int16_t encode_nullable(const std::string& s) {
        if (s.empty()) return NULL_CODE16;
        return encode(s);
    }

    int16_t find_code(const std::string& s) const {
        auto it = lookup.find(s);
        return (it != lookup.end()) ? it->second : NULL_CODE16;
    }

    size_t size() const { return strings.size(); }

    void save(const std::string& path) const {
        FILE* f = fopen(path.c_str(), "w");
        if (!f) { perror(path.c_str()); exit(1); }
        for (const auto& s : strings) fprintf(f, "%s\n", s.c_str());
        fclose(f);
    }
};

// ─────────────────────────────────────────────────────────────
// Binary column writer
// ─────────────────────────────────────────────────────────────
template<typename T>
static void write_col(const std::string& path, const std::vector<T>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { perror(path.c_str()); exit(1); }
    if (!data.empty()) fwrite(data.data(), sizeof(T), data.size(), f);
    fclose(f);
}

// ─────────────────────────────────────────────────────────────
// Safe integer parse (returns NULL_INT32 if empty)
// ─────────────────────────────────────────────────────────────
static int32_t parse_int32(const std::string& s) {
    if (s.empty()) return NULL_INT32;
    return (int32_t)atoi(s.c_str());
}

static double parse_double(const std::string& s) {
    if (s.empty()) return make_nan();
    return atof(s.c_str());
}

// ─────────────────────────────────────────────────────────────
// Memory-mapped file reader
// ─────────────────────────────────────────────────────────────
struct MmapFile {
    const char* data = nullptr;
    size_t size = 0;
    int fd = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); return false; }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); close(fd); fd=-1; return false; }
        madvise((void*)data, size, MADV_SEQUENTIAL);
        return true;
    }

    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap((void*)data, size);
        if (fd >= 0) close(fd);
    }
};

// ─────────────────────────────────────────────────────────────
// Find all line start offsets in mmap'd file (skip header line)
// Returns offsets of DATA lines only (index 0 = first data line)
// ─────────────────────────────────────────────────────────────
static std::vector<size_t> find_line_starts(const char* data, size_t sz) {
    std::vector<size_t> starts;
    // skip header line
    size_t pos = 0;
    while (pos < sz && data[pos] != '\n') pos++;
    if (pos < sz) pos++; // skip '\n'
    starts.push_back(pos);
    for (size_t i = pos; i < sz; i++) {
        if (data[i] == '\n' && i + 1 < sz) {
            starts.push_back(i + 1);
        }
    }
    return starts;
}

// ─────────────────────────────────────────────────────────────
// parse_sub: parse sub.csv, write sub/ binary columns
// Builds adsh_dict (shared), name_dict, and other local dicts
// ─────────────────────────────────────────────────────────────
static void parse_sub(const std::string& data_dir, const std::string& db_dir,
                      Dict32& adsh_dict)
{
    double t0 = now_sec();
    fprintf(stderr, "[ingest] Parsing sub.csv...\n");

    std::string path = data_dir + "/sub.csv";
    MmapFile mf;
    if (!mf.open(path)) exit(1);

    // Column data
    std::vector<int32_t> c_adsh, c_cik, c_name, c_sic;
    std::vector<int16_t> c_countryba, c_stprba, c_countryinc, c_form, c_fp, c_afs, c_fye;
    std::vector<int32_t> c_cityba, c_period, c_fy, c_filed, c_accepted, c_prevrpt, c_nciks, c_wksi, c_instance;

    Dict32 name_dict, cityba_dict, accepted_dict, instance_dict;
    Dict16 countryba_dict, stprba_dict, countryinc_dict, form_dict, fp_dict, afs_dict, fye_dict;

    auto ls = find_line_starts(mf.data, mf.size);
    std::vector<std::string> fields;
    fields.reserve(22);

    for (size_t li = 0; li < ls.size(); li++) {
        size_t off = ls[li];
        if (off >= mf.size) break;
        parse_csv_row(mf.data + off, fields);
        if (fields.size() < 20) continue;

        // adsh,cik,name,sic,countryba,stprba,cityba,countryinc,form,period,fy,fp,filed,accepted,prevrpt,nciks,afs,wksi,fye,instance
        c_adsh.push_back(adsh_dict.encode(fields[0]));
        c_cik.push_back(parse_int32(fields[1]));
        c_name.push_back(name_dict.encode_nullable(fields[2]));
        c_sic.push_back(fields[3].empty() ? NULL_INT32 : parse_int32(fields[3]));
        c_countryba.push_back(countryba_dict.encode_nullable(fields[4]));
        c_stprba.push_back(stprba_dict.encode_nullable(fields[5]));
        c_cityba.push_back(cityba_dict.encode_nullable(fields[6]));
        c_countryinc.push_back(countryinc_dict.encode_nullable(fields[7]));
        c_form.push_back(form_dict.encode_nullable(fields[8]));
        c_period.push_back(fields[9].empty() ? NULL_INT32 : parse_int32(fields[9]));
        c_fy.push_back(fields[10].empty() ? NULL_INT32 : parse_int32(fields[10]));
        c_fp.push_back(fp_dict.encode_nullable(fields[11]));
        c_filed.push_back(fields[12].empty() ? NULL_INT32 : parse_int32(fields[12]));
        c_accepted.push_back(accepted_dict.encode_nullable(fields[13]));
        c_prevrpt.push_back(parse_int32(fields[14]));
        c_nciks.push_back(parse_int32(fields[15]));
        c_afs.push_back(afs_dict.encode_nullable(fields[16]));
        c_wksi.push_back(parse_int32(fields[17]));
        c_fye.push_back(fye_dict.encode_nullable(fields[18]));
        c_instance.push_back(instance_dict.encode_nullable(fields[19]));
    }

    size_t nrows = c_adsh.size();
    fprintf(stderr, "[ingest] sub: %zu rows parsed\n", nrows);

    // Write columns
    std::string dir = db_dir + "/sub";
    write_col(dir + "/adsh.bin",      c_adsh);
    write_col(dir + "/cik.bin",       c_cik);
    write_col(dir + "/name.bin",      c_name);
    write_col(dir + "/sic.bin",       c_sic);
    write_col(dir + "/countryba.bin", c_countryba);
    write_col(dir + "/stprba.bin",    c_stprba);
    write_col(dir + "/cityba.bin",    c_cityba);
    write_col(dir + "/countryinc.bin",c_countryinc);
    write_col(dir + "/form.bin",      c_form);
    write_col(dir + "/period.bin",    c_period);
    write_col(dir + "/fy.bin",        c_fy);
    write_col(dir + "/fp.bin",        c_fp);
    write_col(dir + "/filed.bin",     c_filed);
    write_col(dir + "/accepted.bin",  c_accepted);
    write_col(dir + "/prevrpt.bin",   c_prevrpt);
    write_col(dir + "/nciks.bin",     c_nciks);
    write_col(dir + "/afs.bin",       c_afs);
    write_col(dir + "/wksi.bin",      c_wksi);
    write_col(dir + "/fye.bin",       c_fye);
    write_col(dir + "/instance.bin",  c_instance);

    // Write local dicts
    name_dict.save(dir + "/name_dict.txt");
    cityba_dict.save(dir + "/cityba_dict.txt");
    accepted_dict.save(dir + "/accepted_dict.txt");
    instance_dict.save(dir + "/instance_dict.txt");
    countryba_dict.save(dir + "/countryba_dict.txt");
    stprba_dict.save(dir + "/stprba_dict.txt");
    countryinc_dict.save(dir + "/countryinc_dict.txt");
    form_dict.save(dir + "/form_dict.txt");
    fp_dict.save(dir + "/fp_dict.txt");
    afs_dict.save(dir + "/afs_dict.txt");
    fye_dict.save(dir + "/fye_dict.txt");

    fprintf(stderr, "[ingest] sub done in %.1fs, adsh_dict=%zu\n",
            now_sec()-t0, adsh_dict.size());
}

// ─────────────────────────────────────────────────────────────
// parse_tag: parse tag.csv, write tag/ binary columns
// Builds tag_dict (shared) and version_dict (shared)
// ─────────────────────────────────────────────────────────────
static void parse_tag(const std::string& data_dir, const std::string& db_dir,
                      Dict32& tag_dict, Dict32& version_dict)
{
    double t0 = now_sec();
    fprintf(stderr, "[ingest] Parsing tag.csv...\n");

    std::string path = data_dir + "/tag.csv";
    MmapFile mf;
    if (!mf.open(path)) exit(1);

    std::vector<int32_t> c_tag, c_version, c_custom, c_abstract, c_tlabel;
    std::vector<int16_t> c_datatype, c_iord, c_crdr;

    Dict32 tlabel_dict;
    Dict16 datatype_dict, iord_dict, crdr_dict;

    auto ls = find_line_starts(mf.data, mf.size);
    std::vector<std::string> fields;
    fields.reserve(10);

    for (size_t li = 0; li < ls.size(); li++) {
        size_t off = ls[li];
        if (off >= mf.size) break;
        parse_csv_row(mf.data + off, fields);
        if (fields.size() < 8) continue;

        // tag,version,custom,abstract,datatype,iord,crdr,tlabel,doc
        c_tag.push_back(tag_dict.encode(fields[0]));
        c_version.push_back(version_dict.encode(fields[1]));
        c_custom.push_back(parse_int32(fields[2]));
        c_abstract.push_back(parse_int32(fields[3]));
        c_datatype.push_back(datatype_dict.encode_nullable(fields[4]));
        c_iord.push_back(iord_dict.encode_nullable(fields[5]));
        c_crdr.push_back(crdr_dict.encode_nullable(fields[6]));
        c_tlabel.push_back(tlabel_dict.encode_nullable(fields[7]));
        // col 8 = doc → skip
    }

    size_t nrows = c_tag.size();
    fprintf(stderr, "[ingest] tag: %zu rows parsed\n", nrows);

    std::string dir = db_dir + "/tag";
    write_col(dir + "/tag.bin",      c_tag);
    write_col(dir + "/version.bin",  c_version);
    write_col(dir + "/custom.bin",   c_custom);
    write_col(dir + "/abstract.bin", c_abstract);
    write_col(dir + "/datatype.bin", c_datatype);
    write_col(dir + "/iord.bin",     c_iord);
    write_col(dir + "/crdr.bin",     c_crdr);
    write_col(dir + "/tlabel.bin",   c_tlabel);

    tlabel_dict.save(dir + "/tlabel_dict.txt");
    datatype_dict.save(dir + "/datatype_dict.txt");
    iord_dict.save(dir + "/iord_dict.txt");
    crdr_dict.save(dir + "/crdr_dict.txt");

    fprintf(stderr, "[ingest] tag done in %.1fs, tag_dict=%zu, version_dict=%zu\n",
            now_sec()-t0, tag_dict.size(), version_dict.size());
}

// ─────────────────────────────────────────────────────────────
// parse_pre: parse pre.csv, sort by (adsh_code, tag_code, version_code),
//            write sorted pre/ binary columns
// ─────────────────────────────────────────────────────────────
static void parse_pre(const std::string& data_dir, const std::string& db_dir,
                      const Dict32& adsh_dict, Dict32& tag_dict, Dict32& version_dict)
{
    double t0 = now_sec();
    fprintf(stderr, "[ingest] Parsing pre.csv...\n");

    std::string path = data_dir + "/pre.csv";
    MmapFile mf;
    if (!mf.open(path)) exit(1);

    // Raw storage for sorting
    struct PreRow {
        int32_t adsh_code, report, line;
        int16_t stmt_code;
        int32_t inpth;
        int16_t rfile_code;
        int32_t tag_code, version_code, plabel_code, negating;
    };

    std::vector<PreRow> rows;
    rows.reserve(9700000);

    Dict16 stmt_dict, rfile_dict;
    Dict32 plabel_dict;

    auto ls = find_line_starts(mf.data, mf.size);
    std::vector<std::string> fields;
    fields.reserve(12);

    for (size_t li = 0; li < ls.size(); li++) {
        size_t off = ls[li];
        if (off >= mf.size) break;
        parse_csv_row(mf.data + off, fields);
        if (fields.size() < 10) continue;

        // adsh,report,line,stmt,inpth,rfile,tag,version,plabel,negating
        PreRow r;
        r.adsh_code    = adsh_dict.find_code(fields[0]);
        r.report       = parse_int32(fields[1]);
        r.line         = parse_int32(fields[2]);
        r.stmt_code    = stmt_dict.encode_nullable(fields[3]);
        r.inpth        = parse_int32(fields[4]);
        r.rfile_code   = rfile_dict.encode_nullable(fields[5]);
        r.tag_code     = tag_dict.encode(fields[6]);
        r.version_code = version_dict.encode(fields[7]);
        r.plabel_code  = plabel_dict.encode_nullable(fields[8]);
        r.negating     = parse_int32(fields[9]);
        rows.push_back(r);
    }

    fprintf(stderr, "[ingest] pre: %zu rows parsed, sorting...\n", rows.size());

    // Sort by (adsh_code, tag_code, version_code)
    double tsort = now_sec();
    std::vector<uint32_t> idx(rows.size());
    std::iota(idx.begin(), idx.end(), 0);
    std::sort(idx.begin(), idx.end(), [&](uint32_t a, uint32_t b){
        const PreRow& ra = rows[a]; const PreRow& rb = rows[b];
        if (ra.adsh_code    != rb.adsh_code)    return ra.adsh_code    < rb.adsh_code;
        if (ra.tag_code     != rb.tag_code)     return ra.tag_code     < rb.tag_code;
        return ra.version_code < rb.version_code;
    });
    fprintf(stderr, "[ingest] pre sort done in %.1fs\n", now_sec()-tsort);

    // Write sorted columns
    size_t N = rows.size();
    std::vector<int32_t> c_adsh(N), c_report(N), c_line(N), c_inpth(N),
                         c_tag(N), c_version(N), c_plabel(N), c_negating(N);
    std::vector<int16_t> c_stmt(N), c_rfile(N);

    for (size_t i = 0; i < N; i++) {
        const PreRow& r = rows[idx[i]];
        c_adsh[i]    = r.adsh_code;
        c_report[i]  = r.report;
        c_line[i]    = r.line;
        c_stmt[i]    = r.stmt_code;
        c_inpth[i]   = r.inpth;
        c_rfile[i]   = r.rfile_code;
        c_tag[i]     = r.tag_code;
        c_version[i] = r.version_code;
        c_plabel[i]  = r.plabel_code;
        c_negating[i]= r.negating;
    }

    std::string dir = db_dir + "/pre";
    write_col(dir + "/adsh.bin",    c_adsh);
    write_col(dir + "/report.bin",  c_report);
    write_col(dir + "/line.bin",    c_line);
    write_col(dir + "/stmt.bin",    c_stmt);
    write_col(dir + "/inpth.bin",   c_inpth);
    write_col(dir + "/rfile.bin",   c_rfile);
    write_col(dir + "/tag.bin",     c_tag);
    write_col(dir + "/version.bin", c_version);
    write_col(dir + "/plabel.bin",  c_plabel);
    write_col(dir + "/negating.bin",c_negating);

    stmt_dict.save(dir + "/stmt_dict.txt");
    rfile_dict.save(dir + "/rfile_dict.txt");
    plabel_dict.save(dir + "/plabel_dict.txt");

    fprintf(stderr, "[ingest] pre done in %.1fs\n", now_sec()-t0);
}

// ─────────────────────────────────────────────────────────────
// parse_num: parse num.csv (39M rows) in parallel chunks
//            write num/ binary columns
// ─────────────────────────────────────────────────────────────
static void parse_num(const std::string& data_dir, const std::string& db_dir,
                      const Dict32& adsh_dict, Dict32& tag_dict, Dict32& version_dict)
{
    double t0 = now_sec();
    fprintf(stderr, "[ingest] Parsing num.csv (39M rows)...\n");

    std::string path = data_dir + "/num.csv";
    MmapFile mf;
    if (!mf.open(path)) exit(1);

    // Pre-allocate output columns
    const size_t EST = 39500000;
    std::vector<int32_t> c_adsh,  c_tag, c_version, c_ddate, c_qtrs, c_coreg;
    std::vector<int16_t> c_uom;
    std::vector<double>  c_value;
    c_adsh.reserve(EST); c_tag.reserve(EST); c_version.reserve(EST);
    c_ddate.reserve(EST); c_qtrs.reserve(EST); c_coreg.reserve(EST);
    c_uom.reserve(EST); c_value.reserve(EST);

    Dict16 uom_dict;
    Dict32 coreg_dict;

    // Find all line starts
    fprintf(stderr, "[ingest] num: scanning line offsets...\n");
    auto ls = find_line_starts(mf.data, mf.size);
    fprintf(stderr, "[ingest] num: %zu data lines found\n", ls.size());

    // Parallel parse using thread-local accumulators, then merge
    int nthreads = omp_get_max_threads();
    if (nthreads > 32) nthreads = 32; // cap threads

    size_t total = ls.size();
    size_t chunk = (total + nthreads - 1) / nthreads;

    // Thread-local storage
    struct TLData {
        std::vector<int32_t> adsh, tag, version, ddate, qtrs, coreg;
        std::vector<int16_t> uom;
        std::vector<double>  value;
        // local dicts for uom and coreg (will be merged)
        std::unordered_map<std::string, int16_t> uom_map;
        std::unordered_map<std::string, int32_t> coreg_map;
        std::vector<std::string> uom_strs, coreg_strs;
    };
    std::vector<TLData> tld(nthreads);
    for (auto& t : tld) {
        t.adsh.reserve(chunk+16); t.tag.reserve(chunk+16);
        t.version.reserve(chunk+16); t.ddate.reserve(chunk+16);
        t.qtrs.reserve(chunk+16); t.coreg.reserve(chunk+16);
        t.uom.reserve(chunk+16); t.value.reserve(chunk+16);
    }

    // Shared mutex for tag_dict/version_dict (rare updates for custom tags not in tag table)
    // Most lookups are reads; new entries are rare.
    // We do a lock-free check first, then lock only if update needed.
    // To keep it simple for correctness: serialize tag/version dict access.
    // (FK constraint: most values already in dict; this path is rarely taken)

    fprintf(stderr, "[ingest] num: parallel parse with %d threads...\n", nthreads);

    // We'll do sequential parse to avoid dict race conditions (tag/version may be extended)
    // For num (39M rows), sequential parse is ~30-60s; acceptable on HDD anyway.
    // Parallel approach used only for uom/coreg which are thread-local.

    // Actually let's do fully sequential for correctness, using pre-allocated vectors.
    std::vector<std::string> fields;
    fields.reserve(12);

    double nan_val = make_nan();
    size_t n_null_value = 0;

    for (size_t li = 0; li < total; li++) {
        size_t off = ls[li];
        if (off >= mf.size) break;
        parse_csv_row(mf.data + off, fields);
        if (fields.size() < 8) continue;

        // adsh,tag,version,ddate,qtrs,uom,coreg,value[,footnote]
        c_adsh.push_back(adsh_dict.find_code(fields[0]));
        c_tag.push_back(tag_dict.encode(fields[1]));
        c_version.push_back(version_dict.encode(fields[2]));
        c_ddate.push_back(parse_int32(fields[3]));
        c_qtrs.push_back(parse_int32(fields[4]));
        c_uom.push_back(uom_dict.encode_nullable(fields[5]));
        c_coreg.push_back(coreg_dict.encode_nullable(fields[6]));

        double v = parse_double(fields[7]);
        c_value.push_back(v);
        if (std::isnan(v)) n_null_value++;

        if ((li & 0xFFFFF) == 0 && li > 0) {
            fprintf(stderr, "[ingest] num: %.1fM rows...\r", li/1e6);
        }
    }
    fprintf(stderr, "\n[ingest] num: %zu rows parsed (%zu NULL values)\n",
            c_adsh.size(), n_null_value);

    std::string dir = db_dir + "/num";
    fprintf(stderr, "[ingest] num: writing binary columns...\n");

    // Write in parallel (one thread per column)
    #pragma omp parallel sections
    {
        #pragma omp section
        write_col(dir + "/adsh.bin",    c_adsh);
        #pragma omp section
        write_col(dir + "/tag.bin",     c_tag);
        #pragma omp section
        write_col(dir + "/version.bin", c_version);
        #pragma omp section
        write_col(dir + "/ddate.bin",   c_ddate);
        #pragma omp section
        write_col(dir + "/qtrs.bin",    c_qtrs);
        #pragma omp section
        write_col(dir + "/uom.bin",     c_uom);
        #pragma omp section
        write_col(dir + "/coreg.bin",   c_coreg);
        #pragma omp section
        write_col(dir + "/value.bin",   c_value);
    }

    uom_dict.save(dir + "/uom_dict.txt");
    coreg_dict.save(dir + "/coreg_dict.txt");

    fprintf(stderr, "[ingest] num done in %.1fs\n", now_sec()-t0);
}

// ─────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <data_dir> <gendb_dir>\n", argv[0]);
        return 1;
    }
    std::string data_dir(argv[1]);
    std::string db_dir(argv[2]);

    double t_start = now_sec();
    fprintf(stderr, "[ingest] Starting SEC EDGAR SF3 ingestion\n");
    fprintf(stderr, "[ingest] data_dir=%s  gendb_dir=%s\n",
            data_dir.c_str(), db_dir.c_str());

    // Create directory structure
    make_dir(db_dir + "/shared");
    make_dir(db_dir + "/sub/indexes");
    make_dir(db_dir + "/num");
    make_dir(db_dir + "/tag/indexes");
    make_dir(db_dir + "/pre/indexes");

    // Shared dictionaries
    Dict32 adsh_dict, tag_dict, version_dict;

    // Phase 1: Parse dimension tables (build shared dicts)
    parse_sub(data_dir, db_dir, adsh_dict);
    parse_tag(data_dir, db_dir, tag_dict, version_dict);

    // Phase 2: Parse fact tables (may extend tag/version dicts with custom tags)
    parse_pre(data_dir, db_dir, adsh_dict, tag_dict, version_dict);
    parse_num(data_dir, db_dir, adsh_dict, tag_dict, version_dict);

    // Write shared dictionaries (after all tables processed)
    fprintf(stderr, "[ingest] Writing shared dictionaries...\n");
    adsh_dict.save(db_dir + "/shared/adsh_dict.txt");
    tag_dict.save(db_dir + "/shared/tag_dict.txt");
    version_dict.save(db_dir + "/shared/version_dict.txt");

    fprintf(stderr, "[ingest] Shared dicts: adsh=%zu, tag=%zu, version=%zu\n",
            adsh_dict.size(), tag_dict.size(), version_dict.size());
    fprintf(stderr, "[ingest] TOTAL time: %.1fs\n", now_sec()-t_start);
    return 0;
}
