// build_indexes.cpp — SEC EDGAR index construction from binary columnar data
//
// Reads binary columns written by ingest.cpp and builds:
//   1. indexes/pre_adsh_tagver_set.bin   — sorted unique uint64_t (adsh_code<<32|tagver_code) from pre
//                                          Used by Q24 anti-join (load into unordered_set) and
//                                          Q4/Q6 join setup (sequential scan for given stmt_code)
//   2. Verification stats: row counts, spot-checks on ddate and value columns

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

static double now_sec() {
    return std::chrono::duration<double>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ─────────────────────── file helpers ────────────────────────────────────────

template<typename T>
static std::vector<T> read_vec(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) throw std::runtime_error("Cannot open: " + path);
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    rewind(f);
    if (sz % sizeof(T) != 0)
        throw std::runtime_error("File size not aligned to element type: " + path);
    size_t n = sz / sizeof(T);
    std::vector<T> v(n);
    if (n > 0 && fread(v.data(), sizeof(T), n, f) != n)
        throw std::runtime_error("Read failed: " + path);
    fclose(f);
    return v;
}

static void write_file(const std::string& path, const void* data, size_t bytes) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot open for write: " + path);
    if (bytes > 0 && fwrite(data, 1, bytes, f) != bytes)
        throw std::runtime_error("Write failed: " + path);
    fclose(f);
}

// Load dict file: reads code → string mapping written by ingest
// Format: uint8_t N, then N × {int8_t code, uint8_t slen, char[slen]}
static std::unordered_map<std::string,int8_t> load_dict(const std::string& path) {
    std::unordered_map<std::string,int8_t> m;
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) return m;
    uint8_t n; fread(&n, 1, 1, f);
    for (int i = 0; i < (int)n; ++i) {
        int8_t code; fread(&code, 1, 1, f);
        uint8_t slen; fread(&slen, 1, 1, f);
        char buf[32]{}; fread(buf, 1, slen, f);
        m[std::string(buf, slen)] = code;
    }
    fclose(f);
    return m;
}

// ─────────────────────── build pre join index ─────────────────────────────────

// Builds sorted array of unique (adsh_code, tagver_code) uint64_t keys from pre.
// File: uint64_t N_unique, then N_unique uint64_t keys sorted ascending.
static size_t build_pre_adsh_tagver_set(const std::string& gendb_dir) {
    double t0 = now_sec();
    printf("[idx] loading pre/adsh_code.bin and pre/tagver_code.bin...\n"); fflush(stdout);

    auto adsh_codes  = read_vec<int32_t>(gendb_dir + "/pre/adsh_code.bin");
    auto tagver_codes= read_vec<int32_t>(gendb_dir + "/pre/tagver_code.bin");

    size_t N = adsh_codes.size();
    if (N != tagver_codes.size())
        throw std::runtime_error("pre column size mismatch");
    printf("[idx] pre rows: %zu\n", N);

    // Build uint64_t keys
    std::vector<uint64_t> keys(N);
    for (size_t i = 0; i < N; ++i)
        keys[i] = ((uint64_t)(uint32_t)adsh_codes[i] << 32) | (uint32_t)tagver_codes[i];

    // Sort and deduplicate
    std::sort(keys.begin(), keys.end());
    auto end_it = std::unique(keys.begin(), keys.end());
    keys.erase(end_it, keys.end());
    size_t n_unique = keys.size();
    printf("[idx] pre unique (adsh_code,tagver_code) pairs: %zu\n", n_unique);

    // Write: uint64_t n_unique, then n_unique uint64_t keys
    std::string out_path = gendb_dir + "/indexes/pre_adsh_tagver_set.bin";
    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot write: " + out_path);
    uint64_t nu64 = (uint64_t)n_unique;
    fwrite(&nu64, sizeof(uint64_t), 1, f);
    fwrite(keys.data(), sizeof(uint64_t), n_unique, f);
    fclose(f);

    printf("[idx] pre_adsh_tagver_set.bin written in %.2fs\n", now_sec()-t0);
    return n_unique;
}

// ─────────────────────── verification ────────────────────────────────────────

static void verify_columns(const std::string& gendb_dir) {
    printf("\n=== Verification ===\n");

    // Check num/ddate.bin: must have values > 3000 (non-zero YYYYMMDD dates)
    {
        auto ddate = read_vec<int32_t>(gendb_dir + "/num/ddate.bin");
        size_t N = ddate.size();
        size_t gt3000 = 0, nonzero = 0;
        int32_t minv = INT32_MAX, maxv = INT32_MIN;
        for (size_t i = 0; i < N; ++i) {
            if (ddate[i] != 0) ++nonzero;
            if (ddate[i] > 3000) ++gt3000;
            if (ddate[i] < minv) minv = ddate[i];
            if (ddate[i] > maxv) maxv = ddate[i];
        }
        printf("[verify] num/ddate.bin: N=%zu, non-zero=%zu, >3000=%zu, min=%d, max=%d\n",
               N, nonzero, gt3000, minv, maxv);
        assert(gt3000 > 3000 && "num/ddate: expected many dates > 3000 (YYYYMMDD format)");
    }

    // Check num/value.bin: must have non-zero values
    {
        auto value = read_vec<double>(gendb_dir + "/num/value.bin");
        size_t N = value.size();
        size_t nonzero = 0; double maxabs = 0.0;
        for (size_t i = 0; i < N; ++i) {
            if (value[i] != 0.0) ++nonzero;
            double a = std::abs(value[i]);
            if (a > maxabs) maxabs = a;
        }
        printf("[verify] num/value.bin: N=%zu, non-zero=%zu (%.1f%%), max_abs=%.2e\n",
               N, nonzero, 100.0*nonzero/N, maxabs);
        assert(nonzero > 1000 && "num/value: expected many non-zero decimal values");
    }

    // Check sub row counts
    {
        FILE* f = fopen((gendb_dir+"/sub/cik.bin").c_str(), "rb");
        if (f) {
            fseek(f, 0, SEEK_END);
            long sz = ftell(f); fclose(f);
            printf("[verify] sub/cik.bin: %ld rows\n", sz/(long)sizeof(int32_t));
        }
    }

    // Check tag row counts
    {
        FILE* f = fopen((gendb_dir+"/tag/abstract.bin").c_str(), "rb");
        if (f) {
            fseek(f, 0, SEEK_END);
            long sz = ftell(f); fclose(f);
            printf("[verify] tag/abstract.bin: %ld rows\n", sz/(long)sizeof(int8_t));
        }
    }

    // Check pre row counts
    {
        FILE* f = fopen((gendb_dir+"/pre/stmt_code.bin").c_str(), "rb");
        if (f) {
            fseek(f, 0, SEEK_END);
            long sz = ftell(f); fclose(f);
            printf("[verify] pre/stmt_code.bin: %ld rows\n", sz/(long)sizeof(int8_t));
        }
    }

    // Print dict mappings for query code to reference
    printf("\n[verify] uom codes:\n");
    {
        auto m = load_dict(gendb_dir + "/indexes/uom_codes.bin");
        for (auto& [str,code] : m) printf("  '%s' -> %d\n", str.c_str(), (int)code);
    }

    printf("[verify] stmt codes:\n");
    {
        auto m = load_dict(gendb_dir + "/indexes/stmt_codes.bin");
        for (auto& [str,code] : m) printf("  '%s' -> %d\n", str.c_str(), (int)code);
    }

    // Spot-check: pre sorted by (adsh_code, tagver_code)
    {
        auto adsh_codes  = read_vec<int32_t>(gendb_dir + "/pre/adsh_code.bin");
        auto tagver_codes= read_vec<int32_t>(gendb_dir + "/pre/tagver_code.bin");
        size_t N = adsh_codes.size();
        bool sorted_ok = true;
        // Check first 100K
        size_t check_n = std::min(N, (size_t)100000);
        for (size_t i = 1; i < check_n; ++i) {
            if (adsh_codes[i] < adsh_codes[i-1] ||
                (adsh_codes[i] == adsh_codes[i-1] && tagver_codes[i] < tagver_codes[i-1])) {
                sorted_ok = false; break;
            }
        }
        printf("[verify] pre sort order (first 100K rows): %s\n", sorted_ok ? "OK" : "FAIL");
    }

    // Spot-check: num sorted by (uom_code, ddate)
    {
        auto uom_codes = read_vec<int8_t>(gendb_dir + "/num/uom_code.bin");
        auto ddate     = read_vec<int32_t>(gendb_dir + "/num/ddate.bin");
        size_t N = uom_codes.size();
        bool sorted_ok = true;
        size_t check_n = std::min(N, (size_t)100000);
        for (size_t i = 1; i < check_n; ++i) {
            if (uom_codes[i] < uom_codes[i-1] ||
                (uom_codes[i] == uom_codes[i-1] && ddate[i] < ddate[i-1])) {
                sorted_ok = false; break;
            }
        }
        printf("[verify] num sort order (first 100K rows): %s\n", sorted_ok ? "OK" : "FAIL");
    }

    printf("=== Verification complete ===\n\n");
}

// ─────────────────────── disk usage report ────────────────────────────────────

static void report_disk_usage(const std::string& gendb_dir) {
    size_t total = 0;
    printf("\n=== Storage Report ===\n");
    for (auto& entry : fs::recursive_directory_iterator(gendb_dir)) {
        if (!entry.is_regular_file()) continue;
        auto sz = entry.file_size();
        total += sz;
        if (sz > 1024*1024)
            printf("  %8.1f MB  %s\n", sz/1048576.0, entry.path().filename().c_str());
    }
    printf("  TOTAL: %.1f MB\n", total/1048576.0);
    printf("======================\n");
}

// ─────────────────────── main ────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }
    std::string gendb_dir = argv[1];
    double t0 = now_sec();

    printf("=== Building indexes for %s ===\n", gendb_dir.c_str());

    // Build pre (adsh_code, tagver_code) hash set for joins
    build_pre_adsh_tagver_set(gendb_dir);

    // Verify
    verify_columns(gendb_dir);

    // Disk usage
    report_disk_usage(gendb_dir);

    printf("=== Index building complete in %.2fs ===\n", now_sec()-t0);
    return 0;
}
