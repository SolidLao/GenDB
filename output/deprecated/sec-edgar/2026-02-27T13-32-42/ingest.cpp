// ingest.cpp - SEC EDGAR data ingestion
// Converts CSV files to optimized binary format for fast query processing

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <cstdio>
#include <cassert>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace chrono;

// ============ Binary Row Structures ============
// All files: [uint64_t count][rows...]

#pragma pack(push, 1)

struct SubRow {
    uint32_t adsh_id;
    uint32_t cik;
    uint32_t name_id;
    int32_t  sic;       // -1 if NULL
    int32_t  fy;        // -1 if NULL
    uint32_t period;    // YYYYMMDD, 0 if NULL
    uint32_t filed;     // YYYYMMDD, 0 if NULL
    uint8_t  wksi;
    char     form[11];
    char     fp[3];
    char     afs[6];
    char     countryba[3];
    char     countryinc[4];
    char     fye[5];
}; // 56 bytes

struct NumRow {
    uint32_t adsh_id;
    uint32_t tag_id;
    uint32_t version_id;
    uint32_t ddate;     // YYYYMMDD
    double   value;
}; // 24 bytes

struct PreRow {
    uint32_t adsh_id;
    uint32_t tag_id;
    uint32_t version_id;
    uint32_t plabel_id;
    uint32_t line;
    char     stmt[3];   // actual string e.g. "BS", "IS", etc., null-terminated
    char     rfile[2];  // actual string e.g. "H", "X", null-terminated
    uint8_t  inpth;
    uint8_t  negating;
}; // 22 bytes

struct TagRow {
    uint32_t tag_id;
    uint32_t version_id;
    uint32_t tlabel_id;
    uint8_t  custom;
    uint8_t  abstract_flag;
    uint8_t  crdr;      // 0=C,1=D,255=null
    uint8_t  iord;      // 0=I,1=D,255=null
}; // 16 bytes

#pragma pack(pop)

// ============ String Dictionary ============
struct Dict {
    vector<string> strings;
    unordered_map<string, uint32_t> lookup;

    Dict() {
        // ID 0 = empty/null
        strings.push_back("");
        lookup[""] = 0;
    }

    uint32_t get_or_add(const char* s, int len) {
        if (len == 0) return 0;
        string key(s, len);
        auto it = lookup.find(key);
        if (it != lookup.end()) return it->second;
        uint32_t id = (uint32_t)strings.size();
        strings.push_back(key);
        lookup[key] = id;
        return id;
    }

    uint32_t get(const char* s, int len) const {
        if (len == 0) return 0;
        auto it = lookup.find(string(s, len));
        return (it != lookup.end()) ? it->second : 0;
    }
};

// ============ Memory-mapped file ============
struct MmapFile {
    int fd = -1;
    const char* data = nullptr;
    size_t size = 0;

    bool open(const string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(("open: " + path).c_str()); return false; }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); close(fd); fd = -1; return false; }
        madvise((void*)data, size, MADV_SEQUENTIAL);
        return true;
    }

    ~MmapFile() {
        if (data) munmap((void*)data, size);
        if (fd >= 0) close(fd);
    }
};

// ============ Fast CSV field parser ============
// Returns pointer to start of next field (after delimiter)
// Sets fstart/flen to the field content (excluding quotes)
// For quoted fields with "" escapes, copies to buf and sets fstart=buf
static char g_unescape_buf[2*1024*1024]; // thread-local would be better but this is single-threaded for sub/tag

inline const char* parse_field(const char* p, const char* end,
                                 const char*& fstart, int& flen,
                                 char* unescape_buf = g_unescape_buf) {
    if (p >= end) { fstart = p; flen = 0; return end; }

    if (*p == '"') {
        p++; // skip opening quote
        fstart = p;
        const char* q = p;
        bool has_escape = false;
        while (q < end && !(*q == '"' && (q+1 >= end || *(q+1) != '"'))) {
            if (*q == '"' && q+1 < end && *(q+1) == '"') { has_escape = true; q += 2; }
            else q++;
        }
        if (!has_escape) {
            flen = (int)(q - p);
        } else {
            // Need to unescape
            char* out = unescape_buf;
            const char* r = p;
            while (r < q) {
                if (*r == '"' && r+1 < end && *(r+1) == '"') { *out++ = '"'; r += 2; }
                else *out++ = *r++;
            }
            *out = 0;
            fstart = unescape_buf;
            flen = (int)(out - unescape_buf);
        }
        if (q < end && *q == '"') q++; // skip closing quote
        if (q < end && *q == ',') q++;
        else if (q < end && *q == '\r') { q++; if (q < end && *q == '\n') q++; }
        else if (q < end && *q == '\n') q++;
        return q;
    } else {
        fstart = p;
        while (p < end && *p != ',' && *p != '\r' && *p != '\n') p++;
        flen = (int)(p - fstart);
        if (p < end && *p == ',') p++;
        else if (p < end && *p == '\r') { p++; if (p < end && *p == '\n') p++; }
        else if (p < end && *p == '\n') p++;
        return p;
    }
}

// Skip to end of line
inline const char* skip_line(const char* p, const char* end) {
    while (p < end && *p != '\n') p++;
    if (p < end) p++;
    return p;
}

// Skip N fields
inline const char* skip_fields(const char* p, const char* end, int n) {
    const char* fs; int fl;
    for (int i = 0; i < n; i++) p = parse_field(p, end, fs, fl);
    return p;
}

// Parse integer field
inline int32_t parse_int(const char* s, int len) {
    if (len == 0) return -1;
    int32_t v = 0; bool neg = false;
    int i = 0;
    if (s[0] == '-') { neg = true; i = 1; }
    for (; i < len; i++) v = v * 10 + (s[i] - '0');
    return neg ? -v : v;
}

// Parse double field
inline double parse_double(const char* s, int len) {
    if (len == 0) return 0.0;
    char buf[64]; memcpy(buf, s, len); buf[len] = 0;
    return atof(buf);
}

// Encode stmt
inline uint8_t encode_stmt(const char* s, int len) {
    if (len == 0) return 255;
    if (len == 2) {
        if (s[0]=='B' && s[1]=='S') return 0;
        if (s[0]=='I' && s[1]=='S') return 1;
        if (s[0]=='C' && s[1]=='F') return 2;
        if (s[0]=='E' && s[1]=='Q') return 3;
        if (s[0]=='C' && s[1]=='I') return 4;
    }
    return 5;
}

// Write dictionary file: [count:u32][offsets: (count+1)*u32][data: concatenated strings]
void write_dict(const string& path, const Dict& dict) {
    uint32_t count = (uint32_t)dict.strings.size();

    // Compute string data
    vector<uint32_t> offsets(count + 1);
    uint32_t offset = 0;
    for (uint32_t i = 0; i < count; i++) {
        offsets[i] = offset;
        offset += (uint32_t)dict.strings[i].size() + 1; // +1 for null terminator
    }
    offsets[count] = offset;

    FILE* f = fopen(path.c_str(), "wb");
    fwrite(&count, 4, 1, f);
    fwrite(offsets.data(), 4, count + 1, f);
    for (uint32_t i = 0; i < count; i++) {
        fwrite(dict.strings[i].c_str(), 1, dict.strings[i].size() + 1, f);
    }
    fclose(f);
}

// Write adsh dict (fixed 21-byte strings)
void write_adsh_dict(const string& path, const Dict& dict) {
    uint32_t count = (uint32_t)dict.strings.size();
    FILE* f = fopen(path.c_str(), "wb");
    fwrite(&count, 4, 1, f);
    for (uint32_t i = 0; i < count; i++) {
        char buf[21] = {};
        strncpy(buf, dict.strings[i].c_str(), 20);
        fwrite(buf, 21, 1, f);
    }
    fclose(f);
}

// ============ Main Ingestion Logic ============

int main(int argc, char* argv[]) {
    if (argc < 3) {
        cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>\n";
        return 1;
    }
    string data_dir = argv[1];
    string gendb_dir = argv[2];

    auto t_start = high_resolution_clock::now();

    // Dictionaries (shared)
    Dict adsh_dict, tag_dict, version_dict, name_dict, plabel_dict, tlabel_dict;

    // ============ Parse sub.csv ============
    cerr << "Parsing sub.csv...\n";
    {
        MmapFile mf;
        if (!mf.open(data_dir + "/sub.csv")) return 1;
        const char* p = mf.data, *end = mf.data + mf.size;
        p = skip_line(p, end); // skip header

        vector<SubRow> rows;
        rows.reserve(90000);

        while (p < end) {
            const char* fs; int fl;
            SubRow row = {};

            // adsh
            p = parse_field(p, end, fs, fl);
            if (fl == 0 && p >= end) break;
            row.adsh_id = adsh_dict.get_or_add(fs, fl);

            // cik
            p = parse_field(p, end, fs, fl);
            row.cik = (uint32_t)parse_int(fs, fl);

            // name
            p = parse_field(p, end, fs, fl);
            row.name_id = name_dict.get_or_add(fs, fl);

            // sic
            p = parse_field(p, end, fs, fl);
            row.sic = parse_int(fs, fl);

            // countryba
            p = parse_field(p, end, fs, fl);
            { char buf[3]={}; if(fl>0) { memcpy(buf,fs,min(fl,2)); } memcpy(row.countryba,buf,3); }

            // stprba (skip)
            p = parse_field(p, end, fs, fl);

            // cityba (skip)
            p = parse_field(p, end, fs, fl);

            // countryinc
            p = parse_field(p, end, fs, fl);
            { char buf[4]={}; if(fl>0) { memcpy(buf,fs,min(fl,3)); } memcpy(row.countryinc,buf,4); }

            // form
            p = parse_field(p, end, fs, fl);
            { char buf[11]={}; if(fl>0) { memcpy(buf,fs,min(fl,10)); } memcpy(row.form,buf,11); }

            // period
            p = parse_field(p, end, fs, fl);
            row.period = (uint32_t)parse_int(fs, fl);

            // fy
            p = parse_field(p, end, fs, fl);
            row.fy = parse_int(fs, fl);

            // fp
            p = parse_field(p, end, fs, fl);
            { char buf[3]={}; if(fl>0) { memcpy(buf,fs,min(fl,2)); } memcpy(row.fp,buf,3); }

            // filed
            p = parse_field(p, end, fs, fl);
            row.filed = (uint32_t)parse_int(fs, fl);

            // skip rest of line (accepted, prevrpt, nciks, afs, wksi, fye, instance)
            p = parse_field(p, end, fs, fl); // accepted
            p = parse_field(p, end, fs, fl); // prevrpt
            p = parse_field(p, end, fs, fl); // nciks
            p = parse_field(p, end, fs, fl); // afs
            { char buf[6]={}; if(fl>0) { memcpy(buf,fs,min(fl,5)); } memcpy(row.afs,buf,6); }
            p = parse_field(p, end, fs, fl); // wksi
            row.wksi = (uint8_t)parse_int(fs, fl);
            p = parse_field(p, end, fs, fl); // fye
            { char buf[5]={}; if(fl>0) { memcpy(buf,fs,min(fl,4)); } memcpy(row.fye,buf,5); }
            p = parse_field(p, end, fs, fl); // instance

            rows.push_back(row);
        }

        // Write sub.bin
        FILE* f = fopen((gendb_dir + "/sub.bin").c_str(), "wb");
        uint64_t count = rows.size();
        fwrite(&count, 8, 1, f);
        fwrite(rows.data(), sizeof(SubRow), rows.size(), f);
        fclose(f);
        cerr << "  sub rows: " << rows.size() << "\n";
    }

    // ============ Parse tag.csv ============
    cerr << "Parsing tag.csv...\n";
    {
        MmapFile mf;
        if (!mf.open(data_dir + "/tag.csv")) return 1;
        const char* p = mf.data, *end = mf.data + mf.size;
        p = skip_line(p, end); // skip header

        vector<TagRow> rows;
        rows.reserve(1100000);

        while (p < end) {
            const char* fs; int fl;
            TagRow row = {};

            // tag
            p = parse_field(p, end, fs, fl);
            if (fl == 0 && p >= end) break;
            row.tag_id = tag_dict.get_or_add(fs, fl);

            // version
            p = parse_field(p, end, fs, fl);
            row.version_id = version_dict.get_or_add(fs, fl);

            // custom
            p = parse_field(p, end, fs, fl);
            row.custom = (uint8_t)parse_int(fs, fl);

            // abstract
            p = parse_field(p, end, fs, fl);
            row.abstract_flag = (uint8_t)parse_int(fs, fl);

            // datatype (skip)
            p = parse_field(p, end, fs, fl);

            // iord
            p = parse_field(p, end, fs, fl);
            row.iord = (fl==1) ? (fs[0]=='I' ? 0 : fs[0]=='D' ? 1 : 2) : 255;

            // crdr
            p = parse_field(p, end, fs, fl);
            row.crdr = (fl==1) ? (fs[0]=='C' ? 0 : fs[0]=='D' ? 1 : 2) : 255;

            // tlabel
            p = parse_field(p, end, fs, fl);
            row.tlabel_id = tlabel_dict.get_or_add(fs, fl);

            // doc (skip - rest of line)
            p = skip_line(p, end);

            rows.push_back(row);
        }

        // Write tag.bin
        FILE* f = fopen((gendb_dir + "/tag.bin").c_str(), "wb");
        uint64_t count = rows.size();
        fwrite(&count, 8, 1, f);
        fwrite(rows.data(), sizeof(TagRow), rows.size(), f);
        fclose(f);
        cerr << "  tag rows: " << rows.size() << "\n";
        cerr << "  tag_dict size: " << tag_dict.strings.size() << "\n";
        cerr << "  version_dict size: " << version_dict.strings.size() << "\n";
    }

    // ============ Parse num.csv (parallel) ============
    cerr << "Parsing num.csv...\n";
    {
        MmapFile mf;
        if (!mf.open(data_dir + "/num.csv")) return 1;

        // Skip header line
        const char* header_end = mf.data;
        while (header_end < mf.data + mf.size && *header_end != '\n') header_end++;
        if (header_end < mf.data + mf.size) header_end++;

        // Split into chunks for parallel parsing
        int num_threads = 32;
        size_t data_size = mf.size - (header_end - mf.data);
        size_t chunk_size = data_size / num_threads;

        // Thread-local data: store raw string rows
        // But we need shared dictionaries for tag and version
        // Strategy: use mutex-protected dict for thread safety
        mutex dict_mutex;

        vector<vector<NumRow>> usd_chunks(num_threads);
        vector<vector<NumRow>> pure_chunks(num_threads);

        auto parse_num_chunk = [&](int tid, const char* start, const char* end_ptr) {
            vector<NumRow> usd_rows, pure_rows;
            usd_rows.reserve(chunk_size / 60);
            pure_rows.reserve(chunk_size / 300);

            // Thread-local cache for dict lookups
            struct LRUEntry { uint64_t hash; uint32_t id; const char* str; int len; };
            // Just use local unordered_maps for caching
            unordered_map<string, uint32_t> local_tag_cache, local_ver_cache, local_adsh_cache;
            local_tag_cache.reserve(10000);
            local_ver_cache.reserve(1000);
            local_adsh_cache.reserve(5000);

            const char* p = start;
            char unescape[1024*1024];

            while (p < end_ptr) {
                const char* fs; int fl;
                NumRow row = {};

                // adsh (col 0)
                p = parse_field(p, end_ptr, fs, fl, unescape);
                if (fl == 0 && p >= end_ptr) break;
                {
                    string key(fs, fl);
                    auto it = local_adsh_cache.find(key);
                    if (it != local_adsh_cache.end()) {
                        row.adsh_id = it->second;
                    } else {
                        lock_guard<mutex> lk(dict_mutex);
                        row.adsh_id = adsh_dict.get_or_add(fs, fl);
                        local_adsh_cache[key] = row.adsh_id;
                    }
                }

                // tag (col 1)
                p = parse_field(p, end_ptr, fs, fl, unescape);
                {
                    string key(fs, fl);
                    auto it = local_tag_cache.find(key);
                    if (it != local_tag_cache.end()) {
                        row.tag_id = it->second;
                    } else {
                        lock_guard<mutex> lk(dict_mutex);
                        row.tag_id = tag_dict.get_or_add(fs, fl);
                        local_tag_cache[key] = row.tag_id;
                    }
                }

                // version (col 2)
                p = parse_field(p, end_ptr, fs, fl, unescape);
                {
                    string key(fs, fl);
                    auto it = local_ver_cache.find(key);
                    if (it != local_ver_cache.end()) {
                        row.version_id = it->second;
                    } else {
                        lock_guard<mutex> lk(dict_mutex);
                        row.version_id = version_dict.get_or_add(fs, fl);
                        local_ver_cache[key] = row.version_id;
                    }
                }

                // ddate (col 3)
                p = parse_field(p, end_ptr, fs, fl, unescape);
                row.ddate = (uint32_t)parse_int(fs, fl);

                // qtrs (col 4) - skip
                p = parse_field(p, end_ptr, fs, fl, unescape);

                // uom (col 5) - check for USD or pure
                p = parse_field(p, end_ptr, fs, fl, unescape);
                bool is_usd = (fl == 3 && fs[0]=='U' && fs[1]=='S' && fs[2]=='D');
                bool is_pure = (fl == 4 && fs[0]=='p' && fs[1]=='u' && fs[2]=='r' && fs[3]=='e');

                // coreg (col 6) - skip
                p = parse_field(p, end_ptr, fs, fl, unescape);

                // value (col 7)
                p = parse_field(p, end_ptr, fs, fl, unescape);
                if (fl == 0) {
                    // NULL value - skip this row
                    p = skip_line(p, end_ptr);
                    continue;
                }
                row.value = parse_double(fs, fl);

                // footnote (col 8) - skip to end of line
                p = skip_line(p, end_ptr);

                if (is_usd) usd_rows.push_back(row);
                else if (is_pure) pure_rows.push_back(row);
            }

            usd_chunks[tid] = move(usd_rows);
            pure_chunks[tid] = move(pure_rows);
        };

        // Split data into chunks, finding line boundaries
        vector<const char*> chunk_starts(num_threads + 1);
        chunk_starts[0] = header_end;
        for (int i = 1; i < num_threads; i++) {
            const char* p = header_end + (size_t)i * chunk_size;
            if (p >= mf.data + mf.size) { p = mf.data + mf.size; }
            else { while (p < mf.data + mf.size && *p != '\n') p++; if (p < mf.data + mf.size) p++; }
            chunk_starts[i] = p;
        }
        chunk_starts[num_threads] = mf.data + mf.size;

        vector<thread> threads;
        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back(parse_num_chunk, i, chunk_starts[i], chunk_starts[i+1]);
        }
        for (auto& t : threads) t.join();

        // Write num_usd.bin and num_pure.bin
        {
            FILE* f_usd = fopen((gendb_dir + "/num_usd.bin").c_str(), "wb");
            FILE* f_pure = fopen((gendb_dir + "/num_pure.bin").c_str(), "wb");

            uint64_t total_usd = 0, total_pure = 0;
            for (int i = 0; i < num_threads; i++) {
                total_usd += usd_chunks[i].size();
                total_pure += pure_chunks[i].size();
            }

            fwrite(&total_usd, 8, 1, f_usd);
            fwrite(&total_pure, 8, 1, f_pure);

            for (int i = 0; i < num_threads; i++) {
                fwrite(usd_chunks[i].data(), sizeof(NumRow), usd_chunks[i].size(), f_usd);
                fwrite(pure_chunks[i].data(), sizeof(NumRow), pure_chunks[i].size(), f_pure);
            }

            fclose(f_usd);
            fclose(f_pure);
            cerr << "  num_usd rows: " << total_usd << "\n";
            cerr << "  num_pure rows: " << total_pure << "\n";
        }
    }

    // ============ Parse pre.csv (parallel) ============
    cerr << "Parsing pre.csv...\n";
    {
        MmapFile mf;
        if (!mf.open(data_dir + "/pre.csv")) return 1;

        const char* header_end = mf.data;
        while (header_end < mf.data + mf.size && *header_end != '\n') header_end++;
        if (header_end < mf.data + mf.size) header_end++;

        int num_threads = 16;
        size_t data_size = mf.size - (header_end - mf.data);
        size_t chunk_size = data_size / num_threads;

        mutex dict_mutex;
        vector<vector<PreRow>> pre_chunks(num_threads);

        auto parse_pre_chunk = [&](int tid, const char* start, const char* end_ptr) {
            vector<PreRow> rows;
            rows.reserve(chunk_size / 80);

            unordered_map<string, uint32_t> local_adsh_cache, local_tag_cache, local_ver_cache, local_plabel_cache;
            local_adsh_cache.reserve(3000);
            local_tag_cache.reserve(5000);
            local_ver_cache.reserve(500);
            local_plabel_cache.reserve(10000);

            const char* p = start;
            char unescape[2*1024*1024];

            while (p < end_ptr) {
                const char* fs; int fl;
                PreRow row = {};

                // adsh (col 0)
                p = parse_field(p, end_ptr, fs, fl, unescape);
                if (fl == 0 && p >= end_ptr) break;
                {
                    string key(fs, fl);
                    auto it = local_adsh_cache.find(key);
                    if (it != local_adsh_cache.end()) { row.adsh_id = it->second; }
                    else { lock_guard<mutex> lk(dict_mutex); row.adsh_id = adsh_dict.get_or_add(fs, fl); local_adsh_cache[key] = row.adsh_id; }
                }

                // report (col 1) - skip
                p = parse_field(p, end_ptr, fs, fl, unescape);

                // line (col 2)
                p = parse_field(p, end_ptr, fs, fl, unescape);
                row.line = (uint32_t)parse_int(fs, fl);

                // stmt (col 3)
                p = parse_field(p, end_ptr, fs, fl, unescape);
                { memset(row.stmt, 0, 3); if(fl>0) memcpy(row.stmt, fs, min(fl,2)); }

                // inpth (col 4)
                p = parse_field(p, end_ptr, fs, fl, unescape);
                row.inpth = (uint8_t)parse_int(fs, fl);

                // rfile (col 5)
                p = parse_field(p, end_ptr, fs, fl, unescape);
                { memset(row.rfile, 0, 2); if(fl>0) row.rfile[0] = fs[0]; }

                // tag (col 6)
                p = parse_field(p, end_ptr, fs, fl, unescape);
                {
                    string key(fs, fl);
                    auto it = local_tag_cache.find(key);
                    if (it != local_tag_cache.end()) { row.tag_id = it->second; }
                    else { lock_guard<mutex> lk(dict_mutex); row.tag_id = tag_dict.get_or_add(fs, fl); local_tag_cache[key] = row.tag_id; }
                }

                // version (col 7)
                p = parse_field(p, end_ptr, fs, fl, unescape);
                {
                    string key(fs, fl);
                    auto it = local_ver_cache.find(key);
                    if (it != local_ver_cache.end()) { row.version_id = it->second; }
                    else { lock_guard<mutex> lk(dict_mutex); row.version_id = version_dict.get_or_add(fs, fl); local_ver_cache[key] = row.version_id; }
                }

                // plabel (col 8) - may be quoted
                p = parse_field(p, end_ptr, fs, fl, unescape);
                {
                    string key(fs, fl);
                    auto it = local_plabel_cache.find(key);
                    if (it != local_plabel_cache.end()) { row.plabel_id = it->second; }
                    else { lock_guard<mutex> lk(dict_mutex); row.plabel_id = plabel_dict.get_or_add(fs, fl); local_plabel_cache[key] = row.plabel_id; }
                }

                // negating (col 9)
                p = parse_field(p, end_ptr, fs, fl, unescape);
                row.negating = (uint8_t)parse_int(fs, fl);

                rows.push_back(row);
            }

            pre_chunks[tid] = move(rows);
        };

        vector<const char*> chunk_starts(num_threads + 1);
        chunk_starts[0] = header_end;
        for (int i = 1; i < num_threads; i++) {
            const char* p = header_end + (size_t)i * chunk_size;
            if (p >= mf.data + mf.size) p = mf.data + mf.size;
            else { while (p < mf.data + mf.size && *p != '\n') p++; if (p < mf.data + mf.size) p++; }
            chunk_starts[i] = p;
        }
        chunk_starts[num_threads] = mf.data + mf.size;

        vector<thread> threads;
        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back(parse_pre_chunk, i, chunk_starts[i], chunk_starts[i+1]);
        }
        for (auto& t : threads) t.join();

        // Write pre.bin
        FILE* f = fopen((gendb_dir + "/pre.bin").c_str(), "wb");
        uint64_t total = 0;
        for (auto& c : pre_chunks) total += c.size();
        fwrite(&total, 8, 1, f);
        for (auto& c : pre_chunks) fwrite(c.data(), sizeof(PreRow), c.size(), f);
        fclose(f);
        cerr << "  pre rows: " << total << "\n";
    }

    // ============ Write dictionary files ============
    cerr << "Writing dictionaries...\n";
    write_adsh_dict(gendb_dir + "/adsh.dict", adsh_dict);
    write_dict(gendb_dir + "/tag.dict", tag_dict);
    write_dict(gendb_dir + "/version.dict", version_dict);
    write_dict(gendb_dir + "/name.dict", name_dict);
    write_dict(gendb_dir + "/plabel.dict", plabel_dict);
    write_dict(gendb_dir + "/tlabel.dict", tlabel_dict);

    cerr << "  adsh_dict: " << adsh_dict.strings.size() << "\n";
    cerr << "  tag_dict: " << tag_dict.strings.size() << "\n";
    cerr << "  version_dict: " << version_dict.strings.size() << "\n";
    cerr << "  name_dict: " << name_dict.strings.size() << "\n";
    cerr << "  plabel_dict: " << plabel_dict.strings.size() << "\n";
    cerr << "  tlabel_dict: " << tlabel_dict.strings.size() << "\n";

    auto t_end = high_resolution_clock::now();
    double elapsed_ms = duration_cast<microseconds>(t_end - t_start).count() / 1000.0;
    cerr << "Total ingestion time: " << elapsed_ms << " ms\n";

    return 0;
}
