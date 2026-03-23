// build_ext_line_int16.cpp
// Builds column_versions/pre.line.int16/line_int16.bin:
//   int16_t per row, same row order as existing pre/adsh_code.bin
//   Preserves full precision for line values 0-32767 (actual range: 0-470)
//
// Usage: ./build_ext_line_int16 <gendb_dir> <pre_csv_path>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>
#include <sys/stat.h>

static double now_sec() {
    return std::chrono::duration<double>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ── Load adsh_dict.bin → hash map adsh_str → int32_t code ───────────────────
// Format: uint32_t N, N × { char adsh[20], int32_t code }  (sorted by adsh)
static std::unordered_map<std::string,int32_t> load_adsh_dict(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); exit(1); }
    uint32_t n; fread(&n, 4, 1, f);
    struct Entry { char adsh[20]; int32_t code; };
    std::unordered_map<std::string,int32_t> m;
    m.reserve(n * 2);
    for (uint32_t i = 0; i < n; ++i) {
        Entry e; fread(&e, sizeof(e), 1, f);
        // adsh is null-terminated or 20 chars
        size_t len = strnlen(e.adsh, 20);
        m[std::string(e.adsh, len)] = e.code;
    }
    fclose(f);
    printf("[ext] adsh_dict: %u entries loaded\n", n);
    return m;
}

// ── Load tagver_dict.bin → hash map (tag+'\0'+version) → int32_t code ───────
// Format: uint32_t N, N × { int32_t code, uint16_t tag_len, uint16_t ver_len, tag_bytes, ver_bytes }
static std::unordered_map<std::string,int32_t> load_tagver_dict(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); exit(1); }
    uint32_t n; fread(&n, 4, 1, f);
    std::unordered_map<std::string,int32_t> m;
    m.reserve(n * 2);
    char buf[512];
    for (uint32_t i = 0; i < n; ++i) {
        int32_t code; fread(&code, 4, 1, f);
        uint16_t tl, vl;
        fread(&tl, 2, 1, f);
        fread(&vl, 2, 1, f);
        fread(buf, 1, tl, f); std::string tag(buf, tl);
        fread(buf, 1, vl, f); std::string ver(buf, vl);
        m[tag + '\x00' + ver] = code;
    }
    fclose(f);
    printf("[ext] tagver_dict: %u entries loaded\n", n);
    return m;
}

// ── Simple CSV field parser (handles quoted fields) ──────────────────────────
static int parse_fields(const char* row, size_t len, std::string* out, int max_fields) {
    int fi = 0;
    size_t i = 0;
    if (fi < max_fields) out[fi].clear();
    while (i <= len && fi < max_fields) {
        if (i == len || row[i] == '\n' || row[i] == '\r') { ++fi; break; }
        if (row[i] == '"') {
            ++i;
            while (i < len && !(row[i] == '"' && (i+1 >= len || row[i+1] == ',' || row[i+1] == '\n' || row[i+1] == '\r'))) {
                if (row[i] == '"' && i+1 < len && row[i+1] == '"') { out[fi] += '"'; i += 2; }
                else out[fi] += row[i++];
            }
            if (i < len && row[i] == '"') ++i;
        } else {
            while (i < len && row[i] != ',' && row[i] != '\n' && row[i] != '\r') {
                out[fi] += row[i++];
            }
        }
        if (i < len && row[i] == ',') { ++i; ++fi; if (fi < max_fields) out[fi].clear(); }
        else if (i < len && (row[i] == '\n' || row[i] == '\r')) { ++fi; break; }
        else { ++fi; break; }
    }
    return fi;
}

struct PreRow {
    int32_t adsh_code;
    int32_t tagver_code;
    int16_t line;
};

int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <pre_csv_path>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir = argv[1];
    const std::string csv_path  = argv[2];

    double t0 = now_sec();

    // Load dictionaries
    auto adsh_map   = load_adsh_dict(gendb_dir + "/indexes/adsh_dict.bin");
    auto tagver_map = load_tagver_dict(gendb_dir + "/indexes/tagver_dict.bin");

    // Parse pre.csv: adsh(0),report(1),line(2),stmt(3),inpth(4),rfile(5),tag(6),version(7),...
    FILE* fp = fopen(csv_path.c_str(), "rb");
    if (!fp) { fprintf(stderr, "Cannot open %s\n", csv_path.c_str()); return 1; }

    std::vector<PreRow> rows;
    rows.reserve(10000000);

    static const int MAX_COL = 9;
    std::string fields[MAX_COL];

    // Read file in chunks
    static const size_t BUF = 1 << 26; // 64MB
    std::vector<char> buf_vec(BUF + 4096);
    char* buf = buf_vec.data();
    size_t leftover = 0;
    bool header_skipped = false;
    size_t skipped = 0;
    int max_line_val = 0;
    int cnt_over_127 = 0;

    while (true) {
        size_t nread = fread(buf + leftover, 1, BUF - leftover, fp);
        size_t total = leftover + nread;
        if (total == 0) break;

        size_t pos = 0;
        while (pos < total) {
            // Find end of line
            size_t eol = pos;
            while (eol < total && buf[eol] != '\n') ++eol;
            if (eol == total && nread > 0) {
                // Incomplete line at end of buffer - save leftover
                memmove(buf, buf + pos, total - pos);
                leftover = total - pos;
                goto next_chunk;
            }
            // eol points to '\n' or end of last buffer

            size_t line_len = eol - pos;
            // Strip trailing \r
            if (line_len > 0 && buf[pos + line_len - 1] == '\r') --line_len;

            if (line_len > 0) {
                if (!header_skipped) {
                    header_skipped = true;
                } else {
                    int nf = parse_fields(buf + pos, line_len, fields, MAX_COL);
                    if (nf >= 8) {
                        auto ait = adsh_map.find(fields[0]);
                        if (ait == adsh_map.end()) { ++skipped; }
                        else {
                            int32_t adsh_code = ait->second;
                            std::string tv_key = fields[6] + '\x00' + fields[7];
                            auto tit = tagver_map.find(tv_key);
                            int32_t tagver_code = (tit != tagver_map.end()) ? tit->second : -1;
                            int lv = fields[2].empty() ? 0 : atoi(fields[2].c_str());
                            if (lv > max_line_val) max_line_val = lv;
                            if (lv > 127) ++cnt_over_127;
                            PreRow r;
                            r.adsh_code   = adsh_code;
                            r.tagver_code = tagver_code;
                            r.line        = (int16_t)lv;
                            rows.push_back(r);
                        }
                    } else {
                        ++skipped;
                    }
                }
            }
            pos = eol + 1;
        }
        leftover = 0;
        next_chunk:;
        if (nread == 0) break;
    }
    fclose(fp);

    printf("[ext] parsed %zu rows, %zu skipped, max_line=%d, cnt_over_127=%d\n",
           rows.size(), skipped, max_line_val, cnt_over_127);
    printf("[ext] sorting by (adsh_code, tagver_code)...\n"); fflush(stdout);

    std::sort(rows.begin(), rows.end(), [](const PreRow& a, const PreRow& b) {
        if (a.adsh_code != b.adsh_code) return a.adsh_code < b.adsh_code;
        return a.tagver_code < b.tagver_code;
    });

    printf("[ext] sort done in %.2fs\n", now_sec()-t0); fflush(stdout);

    // Create output directory
    std::string out_dir = gendb_dir + "/column_versions/pre.line.int16";
    mkdir(out_dir.c_str(), 0755);

    // Write int16_t line values
    std::string out_path = out_dir + "/line_int16.bin";
    FILE* outf = fopen(out_path.c_str(), "wb");
    if (!outf) { fprintf(stderr, "Cannot open output %s\n", out_path.c_str()); return 1; }

    size_t N = rows.size();
    std::vector<int16_t> line_vals(N);
    for (size_t i = 0; i < N; ++i) line_vals[i] = rows[i].line;
    fwrite(line_vals.data(), sizeof(int16_t), N, outf);
    fclose(outf);

    printf("[ext] wrote %zu int16_t values to %s\n", N, out_path.c_str());
    printf("[ext] total time: %.2fs\n", now_sec()-t0);

    // Spot-check: verify first 5 adsh_codes match pre/adsh_code.bin
    {
        FILE* f = fopen((gendb_dir + "/pre/adsh_code.bin").c_str(), "rb");
        if (f) {
            std::vector<int32_t> check(5);
            fread(check.data(), 4, 5, f);
            fclose(f);
            printf("[ext] spot-check first 5 adsh_codes:\n");
            printf("      binary: %d %d %d %d %d\n",
                   check[0], check[1], check[2], check[3], check[4]);
            printf("      ours:   %d %d %d %d %d\n",
                   rows[0].adsh_code, rows[1].adsh_code, rows[2].adsh_code,
                   rows[3].adsh_code, rows[4].adsh_code);
        }
    }
    return 0;
}
