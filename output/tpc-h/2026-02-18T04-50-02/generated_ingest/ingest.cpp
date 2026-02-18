// TPC-H SF10 Binary Columnar Ingestion
// Sort lineitem by l_shipdate, orders by o_orderkey, customer by c_custkey
// Compile: g++ -O2 -std=c++17 -Wall -lpthread -o ingest ingest.cpp

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <thread>
#include <mutex>
#include <atomic>
#include <functional>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// ── date helpers ──────────────────────────────────────────────────────────────
static const int DAYS_IN_MONTH[12] = {31,28,31,30,31,30,31,31,30,31,30,31};
static inline bool is_leap(int y) { return (y%4==0&&y%100!=0)||(y%400==0); }

static int32_t parse_date(const char* s) {
    // format: YYYY-MM-DD
    int y = (s[0]-'0')*1000+(s[1]-'0')*100+(s[2]-'0')*10+(s[3]-'0');
    int m = (s[5]-'0')*10+(s[6]-'0');
    int d = (s[8]-'0')*10+(s[9]-'0');
    int days = 0;
    for (int yr = 1970; yr < y; yr++)
        days += is_leap(yr) ? 366 : 365;
    for (int mo = 1; mo < m; mo++) {
        days += DAYS_IN_MONTH[mo-1];
        if (mo == 2 && is_leap(y)) days++;
    }
    days += (d - 1);
    return (int32_t)days;
}

// ── decimal helper ────────────────────────────────────────────────────────────
static int64_t parse_decimal(const char* s, int len) {
    // e.g. "12345.67" → 1234567
    bool neg = false;
    int i = 0;
    if (s[0] == '-') { neg = true; i = 1; }
    int64_t intpart = 0, fracpart = 0, fracdigits = 0;
    bool in_frac = false;
    for (; i < len; i++) {
        if (s[i] == '.') { in_frac = true; continue; }
        if (in_frac) { fracpart = fracpart*10 + (s[i]-'0'); fracdigits++; }
        else { intpart = intpart*10 + (s[i]-'0'); }
    }
    // scale to 2 decimal places
    while (fracdigits < 2) { fracpart *= 10; fracdigits++; }
    while (fracdigits > 2) { fracpart /= 10; fracdigits--; }
    int64_t v = intpart * 100 + fracpart;
    return neg ? -v : v;
}

// ── integer helper ────────────────────────────────────────────────────────────
static int32_t parse_int(const char* s, int len) {
    int32_t v = 0;
    for (int i = 0; i < len; i++) v = v*10 + (s[i]-'0');
    return v;
}

// ── mmap helper ──────────────────────────────────────────────────────────────
struct MmapFile {
    char* data = nullptr;
    size_t size = 0;
    int fd = -1;
    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); return false; }
        struct stat st; fstat(fd, &st); size = st.st_size;
        data = (char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); close(fd); fd=-1; return false; }
        madvise(data, size, MADV_SEQUENTIAL);
        return true;
    }
    void close_file() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) close(fd);
    }
};

// ── buffered writer ───────────────────────────────────────────────────────────
struct BufWriter {
    FILE* fp = nullptr;
    static constexpr size_t BUF = 4*1024*1024; // 4MB
    std::vector<char> buf;
    size_t used = 0;
    bool open(const std::string& path) {
        fp = fopen(path.c_str(), "wb");
        if (!fp) { perror(path.c_str()); return false; }
        buf.resize(BUF);
        return true;
    }
    void write(const void* data, size_t sz) {
        if (used + sz > BUF) flush();
        if (sz > BUF) { fwrite(data, 1, sz, fp); return; }
        memcpy(buf.data() + used, data, sz);
        used += sz;
    }
    void flush() { if (used) { fwrite(buf.data(), 1, used, fp); used = 0; } }
    void close_file() { flush(); if (fp) fclose(fp); }
};

// ── dictionary encoder ────────────────────────────────────────────────────────
struct DictEncoder {
    std::unordered_map<std::string,int32_t> map;
    std::vector<std::string> vals;
    int32_t encode(const std::string& s) {
        auto it = map.find(s);
        if (it != map.end()) return it->second;
        int32_t code = (int32_t)vals.size();
        map[s] = code;
        vals.push_back(s);
        return code;
    }
    void save(const std::string& path) {
        FILE* fp = fopen(path.c_str(), "w");
        if (!fp) { perror(path.c_str()); return; }
        for (size_t i = 0; i < vals.size(); i++)
            fprintf(fp, "%s\n", vals[i].c_str());
        fclose(fp);
    }
};

// ── field splitter ────────────────────────────────────────────────────────────
struct Row {
    const char* fields[20];
    int lens[20];
    int count;
};

static void split_row(const char* start, int len, char delim, Row& row) {
    row.count = 0;
    int begin = 0;
    for (int i = 0; i <= len; i++) {
        if (i == len || start[i] == delim) {
            row.fields[row.count] = start + begin;
            row.lens[row.count] = i - begin;
            row.count++;
            begin = i + 1;
        }
    }
}

// ── chunk newline boundary finder ─────────────────────────────────────────────
// Returns actual number of non-empty chunks used (may be < nthreads for tiny files)
static int find_chunk_bounds(const char* data, size_t total, int nthreads,
                               std::vector<size_t>& starts, std::vector<size_t>& ends) {
    starts.resize(nthreads); ends.resize(nthreads);
    // Ensure minimum chunk size of 64KB to avoid tiny-file over-threading
    size_t min_chunk = 65536;
    int actual = nthreads;
    if (total / (size_t)nthreads < min_chunk) {
        actual = (int)(total / min_chunk);
        if (actual < 1) actual = 1;
    }
    size_t chunk = total / actual;
    int used = 0;
    size_t prev_end = 0;
    for (int t = 0; t < actual; t++) {
        size_t s = prev_end;
        if (s >= total) break;
        size_t e = (t == actual-1) ? total : s + chunk;
        if (e > total) e = total;
        if (e < total) { while (e < total && data[e] != '\n') e++; if (e < total) e++; }
        starts[used] = s; ends[used] = e;
        prev_end = e;
        used++;
    }
    return used;
}

// ═══════════════════════════════════════════════════════════════════════════════
// LINEITEM ingestion (sorted by l_shipdate)
// ═══════════════════════════════════════════════════════════════════════════════
static void ingest_lineitem(const std::string& src, const std::string& dst) {
    printf("[lineitem] reading...\n"); fflush(stdout);
    MmapFile mf; if (!mf.open(src)) { fprintf(stderr,"Cannot open %s\n",src.c_str()); exit(1); }

    int nthreads = (int)std::thread::hardware_concurrency();
    if (nthreads > 64) nthreads = 64;
    std::vector<size_t> starts, ends;
    nthreads = find_chunk_bounds(mf.data, mf.size, nthreads, starts, ends);

    // Per-thread raw row storage
    struct RawRow {
        int32_t l_orderkey, l_partkey, l_suppkey, l_linenumber;
        int64_t l_quantity, l_extendedprice, l_discount, l_tax;
        int32_t l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate;
        int32_t l_shipinstruct, l_shipmode, l_comment;
    };

    // Dictionary encoders (must be shared, serialized)
    DictEncoder enc_returnflag, enc_linestatus, enc_shipinstruct, enc_shipmode, enc_comment;
    std::mutex dict_mu;

    // Thread-local batches
    std::vector<std::vector<RawRow>> thread_rows(nthreads);
    for (auto& v : thread_rows) v.reserve(59986052 / nthreads + 1000);

    // Count pass to estimate
    std::vector<std::thread> threads;
    threads.reserve(nthreads);

    for (int t = 0; t < nthreads; t++) {
        threads.emplace_back([&, t]() {
            const char* p = mf.data + starts[t];
            const char* pe = mf.data + ends[t];
            std::vector<RawRow>& rows = thread_rows[t];
            // local dict buffers
            std::unordered_map<std::string,int32_t> local_rf, local_ls, local_si, local_sm, local_cm;
            while (p < pe) {
                const char* nl = (const char*)memchr(p, '\n', pe - p);
                int ll = nl ? (int)(nl - p) : (int)(pe - p);
                if (ll <= 0) { if (nl) p = nl+1; else break; }
                else {
                    Row row; split_row(p, ll, '|', row);
                    if (row.count < 16) { p = nl ? nl+1 : pe; continue; }
                    RawRow r;
                    r.l_orderkey      = parse_int(row.fields[0], row.lens[0]);
                    r.l_partkey       = parse_int(row.fields[1], row.lens[1]);
                    r.l_suppkey       = parse_int(row.fields[2], row.lens[2]);
                    r.l_linenumber    = parse_int(row.fields[3], row.lens[3]);
                    r.l_quantity      = parse_decimal(row.fields[4], row.lens[4]);
                    r.l_extendedprice = parse_decimal(row.fields[5], row.lens[5]);
                    r.l_discount      = parse_decimal(row.fields[6], row.lens[6]);
                    r.l_tax           = parse_decimal(row.fields[7], row.lens[7]);
                    // Dictionary: encode locally, merge later
                    std::string rf(row.fields[8], row.lens[8]);
                    std::string ls(row.fields[9], row.lens[9]);
                    r.l_shipdate    = parse_date(row.fields[10]);
                    r.l_commitdate  = parse_date(row.fields[11]);
                    r.l_receiptdate = parse_date(row.fields[12]);
                    std::string si(row.fields[13], row.lens[13]);
                    std::string sm(row.fields[14], row.lens[14]);
                    std::string cm(row.fields[15], row.lens[15]);
                    // Use -1 as placeholder, will remap after merge
                    // Store string index in local maps
                    auto get = [](std::unordered_map<std::string,int32_t>& m, const std::string& s) -> int32_t {
                        auto it = m.find(s); if (it != m.end()) return it->second;
                        int32_t c = (int32_t)m.size(); m[s] = c; return c;
                    };
                    r.l_returnflag   = get(local_rf, rf);
                    r.l_linestatus   = get(local_ls, ls);
                    r.l_shipinstruct = get(local_si, si);
                    r.l_shipmode     = get(local_sm, sm);
                    r.l_comment      = get(local_cm, cm);
                    rows.push_back(r);
                    p = nl ? nl+1 : pe;
                }
            }
            // Merge local dicts into global and remap codes
            // Step 1: build remapping tables
            std::unordered_map<int32_t,int32_t> remap_rf, remap_ls, remap_si, remap_sm, remap_cm;
            {
                std::lock_guard<std::mutex> lk(dict_mu);
                for (auto& [s,code] : local_rf) { int32_t gc = enc_returnflag.encode(s); remap_rf[code] = gc; }
                for (auto& [s,code] : local_ls) { int32_t gc = enc_linestatus.encode(s); remap_ls[code] = gc; }
                for (auto& [s,code] : local_si) { int32_t gc = enc_shipinstruct.encode(s); remap_si[code] = gc; }
                for (auto& [s,code] : local_sm) { int32_t gc = enc_shipmode.encode(s); remap_sm[code] = gc; }
                for (auto& [s,code] : local_cm) { int32_t gc = enc_comment.encode(s); remap_cm[code] = gc; }
            }
            // Step 2: remap
            for (auto& r : rows) {
                r.l_returnflag   = remap_rf[r.l_returnflag];
                r.l_linestatus   = remap_ls[r.l_linestatus];
                r.l_shipinstruct = remap_si[r.l_shipinstruct];
                r.l_shipmode     = remap_sm[r.l_shipmode];
                r.l_comment      = remap_cm[r.l_comment];
            }
        });
    }
    for (auto& th : threads) th.join();
    mf.close_file();

    // Merge all rows
    printf("[lineitem] merging %d thread batches...\n", nthreads); fflush(stdout);
    size_t total_rows = 0;
    for (auto& v : thread_rows) total_rows += v.size();
    std::vector<RawRow> all_rows;
    all_rows.reserve(total_rows);
    for (auto& v : thread_rows) {
        all_rows.insert(all_rows.end(), v.begin(), v.end());
        std::vector<RawRow>().swap(v); // free
    }
    printf("[lineitem] total rows: %zu\n", total_rows); fflush(stdout);

    // Sort by l_shipdate (permutation sort)
    printf("[lineitem] sorting by l_shipdate...\n"); fflush(stdout);
    std::vector<uint32_t> perm(total_rows);
    for (size_t i = 0; i < total_rows; i++) perm[i] = (uint32_t)i;
    std::sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b){
        return all_rows[a].l_shipdate < all_rows[b].l_shipdate;
    });

    // Verify: check date > 3000 (1998 = 10227 days), decimal != 0
    {
        int32_t sample_date = all_rows[perm[0]].l_shipdate;
        if (sample_date <= 3000) {
            fprintf(stderr, "ABORT: l_shipdate sanity fail: %d (expected >3000 for dates after 1978)\n", sample_date);
            exit(1);
        }
        int64_t sample_dec = all_rows[perm[0]].l_extendedprice;
        if (sample_dec == 0) {
            fprintf(stderr, "ABORT: l_extendedprice is zero — decimal parsing failed\n");
            exit(1);
        }
        printf("[lineitem] VERIFY OK: l_shipdate[0]=%d (>3000), l_extendedprice[0]=%ld (!=0)\n",
               sample_date, sample_dec);
    }

    printf("[lineitem] writing columns...\n"); fflush(stdout);
    std::string tdir = dst + "/lineitem";
    mkdir(tdir.c_str(), 0755);

    // Write each column via permutation
    auto write_col_i32 = [&](const std::string& name, auto getter) {
        BufWriter bw; bw.open(tdir + "/" + name + ".bin");
        for (size_t i = 0; i < total_rows; i++) {
            int32_t v = getter(all_rows[perm[i]]);
            bw.write(&v, 4);
        }
        bw.close_file();
    };
    auto write_col_i64 = [&](const std::string& name, auto getter) {
        BufWriter bw; bw.open(tdir + "/" + name + ".bin");
        for (size_t i = 0; i < total_rows; i++) {
            int64_t v = getter(all_rows[perm[i]]);
            bw.write(&v, 8);
        }
        bw.close_file();
    };

    write_col_i32("l_orderkey",      [](const RawRow& r){ return r.l_orderkey; });
    write_col_i32("l_partkey",       [](const RawRow& r){ return r.l_partkey; });
    write_col_i32("l_suppkey",       [](const RawRow& r){ return r.l_suppkey; });
    write_col_i32("l_linenumber",    [](const RawRow& r){ return r.l_linenumber; });
    write_col_i64("l_quantity",      [](const RawRow& r){ return r.l_quantity; });
    write_col_i64("l_extendedprice", [](const RawRow& r){ return r.l_extendedprice; });
    write_col_i64("l_discount",      [](const RawRow& r){ return r.l_discount; });
    write_col_i64("l_tax",           [](const RawRow& r){ return r.l_tax; });
    write_col_i32("l_returnflag",    [](const RawRow& r){ return r.l_returnflag; });
    write_col_i32("l_linestatus",    [](const RawRow& r){ return r.l_linestatus; });
    write_col_i32("l_shipdate",      [](const RawRow& r){ return r.l_shipdate; });
    write_col_i32("l_commitdate",    [](const RawRow& r){ return r.l_commitdate; });
    write_col_i32("l_receiptdate",   [](const RawRow& r){ return r.l_receiptdate; });
    write_col_i32("l_shipinstruct",  [](const RawRow& r){ return r.l_shipinstruct; });
    write_col_i32("l_shipmode",      [](const RawRow& r){ return r.l_shipmode; });
    write_col_i32("l_comment",       [](const RawRow& r){ return r.l_comment; });

    enc_returnflag.save(tdir + "/l_returnflag_dict.txt");
    enc_linestatus.save(tdir + "/l_linestatus_dict.txt");
    enc_shipinstruct.save(tdir + "/l_shipinstruct_dict.txt");
    enc_shipmode.save(tdir + "/l_shipmode_dict.txt");
    enc_comment.save(tdir + "/l_comment_dict.txt");

    // metadata
    FILE* mfp = fopen((tdir + "/metadata.json").c_str(), "w");
    if (mfp) {
        fprintf(mfp, "{\"table\":\"lineitem\",\"row_count\":%zu,\"sort_order\":\"l_shipdate\",\"block_size\":65536}\n", total_rows);
        fclose(mfp);
    }
    printf("[lineitem] done. %zu rows written.\n", total_rows); fflush(stdout);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Generic table ingestion (no sort, configurable columns)
// ═══════════════════════════════════════════════════════════════════════════════
enum ColType { CT_INT32, CT_INT64_DEC, CT_DATE, CT_DICT };

struct ColSpec {
    std::string name;
    int field_idx;
    ColType type;
};

struct ColData {
    std::vector<int32_t> i32;
    std::vector<int64_t> i64;
    DictEncoder enc;
    ColType type;
    std::string name;
};

static void ingest_generic(const std::string& src, const std::string& dst,
                            const std::string& table_name,
                            const std::vector<ColSpec>& cols,
                            const std::string& sort_col = "",
                            bool needs_sort = false) {
    printf("[%s] reading...\n", table_name.c_str()); fflush(stdout);
    MmapFile mf; if (!mf.open(src)) { fprintf(stderr,"Cannot open %s\n",src.c_str()); exit(1); }

    int nthreads = (int)std::thread::hardware_concurrency();
    if (nthreads > 64) nthreads = 64;
    std::vector<size_t> starts, ends;
    nthreads = find_chunk_bounds(mf.data, mf.size, nthreads, starts, ends);

    int ncols = (int)cols.size();
    // Per-thread data
    struct ThreadData {
        std::vector<std::vector<int32_t>> i32_cols;
        std::vector<std::vector<int64_t>> i64_cols;
        std::vector<std::unordered_map<std::string,int32_t>> local_dicts;
    };
    std::vector<ThreadData> td(nthreads);
    for (auto& t : td) {
        t.i32_cols.resize(ncols);
        t.i64_cols.resize(ncols);
        t.local_dicts.resize(ncols);
    }

    std::vector<DictEncoder> global_encs(ncols);
    std::mutex dict_mu;

    std::vector<std::thread> threads;
    for (int t = 0; t < nthreads; t++) {
        threads.emplace_back([&, t]() {
            auto& mytd = td[t];
            const char* p = mf.data + starts[t];
            const char* pe = mf.data + ends[t];
            while (p < pe) {
                const char* nl = (const char*)memchr(p, '\n', pe - p);
                int ll = nl ? (int)(nl - p) : (int)(pe - p);
                if (ll <= 0) { p = nl ? nl+1 : pe; continue; }
                Row row; split_row(p, ll, '|', row);
                for (int ci = 0; ci < ncols; ci++) {
                    int fi = cols[ci].field_idx;
                    if (fi >= row.count) { p = nl ? nl+1 : pe; goto next_row; }
                    switch (cols[ci].type) {
                        case CT_INT32:
                            mytd.i32_cols[ci].push_back(parse_int(row.fields[fi], row.lens[fi]));
                            break;
                        case CT_INT64_DEC:
                            mytd.i64_cols[ci].push_back(parse_decimal(row.fields[fi], row.lens[fi]));
                            break;
                        case CT_DATE:
                            mytd.i32_cols[ci].push_back(parse_date(row.fields[fi]));
                            break;
                        case CT_DICT: {
                            std::string s(row.fields[fi], row.lens[fi]);
                            auto& lm = mytd.local_dicts[ci];
                            auto it = lm.find(s);
                            int32_t lc;
                            if (it == lm.end()) { lc = (int32_t)lm.size(); lm[s] = lc; }
                            else lc = it->second;
                            mytd.i32_cols[ci].push_back(lc);
                            break;
                        }
                    }
                }
                p = nl ? nl+1 : pe;
                continue;
                next_row: p = nl ? nl+1 : pe;
            }
            // merge dicts
            for (int ci = 0; ci < ncols; ci++) {
                if (cols[ci].type == CT_DICT) {
                    std::unordered_map<int32_t,int32_t> remap;
                    {
                        std::lock_guard<std::mutex> lk(dict_mu);
                        for (auto& [s,lc] : mytd.local_dicts[ci]) {
                            int32_t gc = global_encs[ci].encode(s);
                            remap[lc] = gc;
                        }
                    }
                    for (auto& v : mytd.i32_cols[ci]) v = remap[v];
                }
            }
        });
    }
    for (auto& th : threads) th.join();
    mf.close_file();

    // Merge
    size_t total_rows = 0;
    for (auto& t : td) {
        for (int ci = 0; ci < ncols; ci++) {
            if (!t.i32_cols[ci].empty()) { total_rows = t.i32_cols[ci].size(); break; }
            if (!t.i64_cols[ci].empty()) { total_rows = t.i64_cols[ci].size(); break; }
        }
        break; // use first thread to count — will sum below
    }
    // actually count
    total_rows = 0;
    for (auto& t : td) {
        if (ncols > 0) {
            if (cols[0].type == CT_INT64_DEC) total_rows += t.i64_cols[0].size();
            else total_rows += t.i32_cols[0].size();
        }
    }
    printf("[%s] total rows: %zu\n", table_name.c_str(), total_rows); fflush(stdout);

    // Build merged columns
    std::vector<std::vector<int32_t>> merged_i32(ncols);
    std::vector<std::vector<int64_t>> merged_i64(ncols);
    for (int ci = 0; ci < ncols; ci++) {
        if (cols[ci].type == CT_INT64_DEC) {
            merged_i64[ci].reserve(total_rows);
            for (auto& t : td) merged_i64[ci].insert(merged_i64[ci].end(), t.i64_cols[ci].begin(), t.i64_cols[ci].end());
        } else {
            merged_i32[ci].reserve(total_rows);
            for (auto& t : td) merged_i32[ci].insert(merged_i32[ci].end(), t.i32_cols[ci].begin(), t.i32_cols[ci].end());
        }
    }
    // free thread data
    std::vector<ThreadData>().swap(td);

    // Sort if needed
    std::vector<uint32_t> perm(total_rows);
    if (needs_sort && !sort_col.empty()) {
        for (size_t i = 0; i < total_rows; i++) perm[i] = (uint32_t)i;
        // find sort column index
        int sci = -1;
        for (int ci = 0; ci < ncols; ci++) if (cols[ci].name == sort_col) { sci = ci; break; }
        if (sci >= 0 && cols[sci].type != CT_INT64_DEC) {
            auto& sv = merged_i32[sci];
            std::sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b){ return sv[a] < sv[b]; });
            printf("[%s] sorted by %s\n", table_name.c_str(), sort_col.c_str()); fflush(stdout);
        }
    } else {
        for (size_t i = 0; i < total_rows; i++) perm[i] = (uint32_t)i;
    }

    // Verify date and decimal
    for (int ci = 0; ci < ncols; ci++) {
        if (cols[ci].type == CT_DATE && !merged_i32[ci].empty()) {
            int32_t v = merged_i32[ci][perm[0]];
            if (v <= 3000) { fprintf(stderr,"ABORT: %s.%s date value %d <= 3000\n",table_name.c_str(),cols[ci].name.c_str(),v); exit(1); }
            printf("[%s] VERIFY date %s[0]=%d (>3000) OK\n", table_name.c_str(), cols[ci].name.c_str(), v);
        }
        if (cols[ci].type == CT_INT64_DEC && !merged_i64[ci].empty()) {
            int64_t v = merged_i64[ci][perm[0]];
            if (v == 0) { fprintf(stderr,"WARN: %s.%s decimal[0] is zero\n",table_name.c_str(),cols[ci].name.c_str()); }
            else printf("[%s] VERIFY decimal %s[0]=%ld (!=0) OK\n", table_name.c_str(), cols[ci].name.c_str(), v);
        }
    }

    std::string tdir = dst + "/" + table_name;
    mkdir(tdir.c_str(), 0755);

    for (int ci = 0; ci < ncols; ci++) {
        std::string fpath = tdir + "/" + cols[ci].name + ".bin";
        BufWriter bw; bw.open(fpath);
        if (cols[ci].type == CT_INT64_DEC) {
            for (size_t i = 0; i < total_rows; i++) { int64_t v = merged_i64[ci][perm[i]]; bw.write(&v, 8); }
        } else {
            for (size_t i = 0; i < total_rows; i++) { int32_t v = merged_i32[ci][perm[i]]; bw.write(&v, 4); }
        }
        bw.close_file();
        if (cols[ci].type == CT_DICT)
            global_encs[ci].save(tdir + "/" + cols[ci].name + "_dict.txt");
    }

    FILE* mfp = fopen((tdir + "/metadata.json").c_str(), "w");
    if (mfp) {
        fprintf(mfp, "{\"table\":\"%s\",\"row_count\":%zu,\"sort_order\":\"%s\",\"block_size\":65536}\n",
                table_name.c_str(), total_rows, sort_col.c_str());
        fclose(mfp);
    }
    printf("[%s] done. %zu rows written.\n", table_name.c_str(), total_rows); fflush(stdout);
}

int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: ingest <data_dir> <gendb_dir>\n"); return 1; }
    std::string src_dir = argv[1];
    std::string dst_dir = argv[2];
    mkdir(dst_dir.c_str(), 0755);
    mkdir((dst_dir + "/indexes").c_str(), 0755);

    // Self-test parse_date
    assert(parse_date("1970-01-01") == 0);
    assert(parse_date("1970-01-02") == 1);
    assert(parse_date("1971-01-01") == 365);
    printf("parse_date self-test OK\n"); fflush(stdout);

    // Self-test parse_decimal
    assert(parse_decimal("12345.67", 8) == 1234567LL);
    assert(parse_decimal("0.06", 4) == 6LL);
    printf("parse_decimal self-test OK\n"); fflush(stdout);

    // ── LINEITEM ──────────────────────────────────────────────────────────────
    ingest_lineitem(src_dir + "/lineitem.tbl", dst_dir);

    // ── ORDERS ───────────────────────────────────────────────────────────────
    {
        std::vector<ColSpec> cols = {
            {"o_orderkey",      0, CT_INT32},
            {"o_custkey",       1, CT_INT32},
            {"o_orderstatus",   2, CT_DICT},
            {"o_totalprice",    3, CT_INT64_DEC},
            {"o_orderdate",     4, CT_DATE},
            {"o_orderpriority", 5, CT_DICT},
            {"o_clerk",         6, CT_DICT},
            {"o_shippriority",  7, CT_INT32},
            {"o_comment",       8, CT_DICT},
        };
        ingest_generic(src_dir + "/orders.tbl", dst_dir, "orders", cols, "o_orderkey", true);
    }

    // ── CUSTOMER ─────────────────────────────────────────────────────────────
    {
        std::vector<ColSpec> cols = {
            {"c_custkey",    0, CT_INT32},
            {"c_name",       1, CT_DICT},
            {"c_address",    2, CT_DICT},
            {"c_nationkey",  3, CT_INT32},
            {"c_phone",      4, CT_DICT},
            {"c_acctbal",    5, CT_INT64_DEC},
            {"c_mktsegment", 6, CT_DICT},
            {"c_comment",    7, CT_DICT},
        };
        ingest_generic(src_dir + "/customer.tbl", dst_dir, "customer", cols, "c_custkey", true);
    }

    // ── PART ─────────────────────────────────────────────────────────────────
    {
        std::vector<ColSpec> cols = {
            {"p_partkey",     0, CT_INT32},
            {"p_name",        1, CT_DICT},
            {"p_mfgr",        2, CT_DICT},
            {"p_brand",       3, CT_DICT},
            {"p_type",        4, CT_DICT},
            {"p_size",        5, CT_INT32},
            {"p_container",   6, CT_DICT},
            {"p_retailprice", 7, CT_INT64_DEC},
            {"p_comment",     8, CT_DICT},
        };
        ingest_generic(src_dir + "/part.tbl", dst_dir, "part", cols);
    }

    // ── PARTSUPP ─────────────────────────────────────────────────────────────
    {
        std::vector<ColSpec> cols = {
            {"ps_partkey",    0, CT_INT32},
            {"ps_suppkey",    1, CT_INT32},
            {"ps_availqty",   2, CT_INT32},
            {"ps_supplycost", 3, CT_INT64_DEC},
            {"ps_comment",    4, CT_DICT},
        };
        ingest_generic(src_dir + "/partsupp.tbl", dst_dir, "partsupp", cols);
    }

    // ── SUPPLIER ─────────────────────────────────────────────────────────────
    {
        std::vector<ColSpec> cols = {
            {"s_suppkey",   0, CT_INT32},
            {"s_name",      1, CT_DICT},
            {"s_address",   2, CT_DICT},
            {"s_nationkey", 3, CT_INT32},
            {"s_phone",     4, CT_DICT},
            {"s_acctbal",   5, CT_INT64_DEC},
            {"s_comment",   6, CT_DICT},
        };
        ingest_generic(src_dir + "/supplier.tbl", dst_dir, "supplier", cols);
    }

    // ── NATION ───────────────────────────────────────────────────────────────
    {
        std::vector<ColSpec> cols = {
            {"n_nationkey", 0, CT_INT32},
            {"n_name",      1, CT_DICT},
            {"n_regionkey", 2, CT_INT32},
            {"n_comment",   3, CT_DICT},
        };
        ingest_generic(src_dir + "/nation.tbl", dst_dir, "nation", cols);
    }

    // ── REGION ───────────────────────────────────────────────────────────────
    {
        std::vector<ColSpec> cols = {
            {"r_regionkey", 0, CT_INT32},
            {"r_name",      1, CT_DICT},
            {"r_comment",   2, CT_DICT},
        };
        ingest_generic(src_dir + "/region.tbl", dst_dir, "region", cols);
    }

    printf("\n=== Ingestion complete ===\n"); fflush(stdout);
    return 0;
}
