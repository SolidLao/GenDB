#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <algorithm>
#include <unordered_map>

namespace fs = std::filesystem;

// ============================================================
// Fast date parser: "YYYY-MM-DD" → days since 1970-01-01
// ============================================================
static inline int parse_date(const char* s) {
    int y = (s[0]-'0')*1000 + (s[1]-'0')*100 + (s[2]-'0')*10 + (s[3]-'0');
    int m = (s[5]-'0')*10 + (s[6]-'0');
    int d = (s[8]-'0')*10 + (s[9]-'0');
    // Days from year
    // Howard Hinnant's days_from_civil algorithm
    auto days_from_civil = [](int yr, int mn, int dy) -> int {
        yr -= (mn <= 2);
        int era = (yr >= 0 ? yr : yr - 399) / 400;
        int yoe = yr - era * 400;
        int doy = (153 * (mn + (mn > 2 ? -3 : 9)) + 2) / 5 + dy - 1;
        int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        return era * 146097 + doe - 719468;
    };
    return days_from_civil(y, m, d);
}

// ============================================================
// Fast double parser (simplified, handles TPC-H decimal format)
// ============================================================
static inline double parse_double(const char* s, const char* end) {
    // Use strtod for correctness
    char buf[64];
    int len = (int)(end - s);
    if (len > 63) len = 63;
    memcpy(buf, s, len);
    buf[len] = '\0';
    return strtod(buf, nullptr);
}

// ============================================================
// Fast int32 parser
// ============================================================
static inline int32_t parse_int(const char* s, const char* end) {
    int32_t val = 0;
    bool neg = false;
    if (*s == '-') { neg = true; s++; }
    while (s < end) { val = val * 10 + (*s - '0'); s++; }
    return neg ? -val : val;
}

// ============================================================
// Memory-mapped file reader
// ============================================================
struct MappedFile {
    const char* data = nullptr;
    size_t size = 0;
    int fd = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); return false; }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (data == MAP_FAILED) { data = nullptr; close(fd); fd = -1; return false; }
        madvise((void*)data, size, MADV_SEQUENTIAL);
        return true;
    }

    ~MappedFile() {
        if (data) munmap((void*)data, size);
        if (fd >= 0) ::close(fd);
    }
};

// ============================================================
// Binary column writer (buffered)
// ============================================================
class ColumnWriter {
    FILE* fp;
public:
    ColumnWriter() : fp(nullptr) {}
    bool open(const std::string& path) {
        fp = fopen(path.c_str(), "wb");
        if (!fp) { perror(path.c_str()); return false; }
        setvbuf(fp, nullptr, _IOFBF, 1 << 20); // 1MB buffer
        return true;
    }
    void write_raw(const void* data, size_t bytes) { fwrite(data, 1, bytes, fp); }
    template<typename T> void write(const T& val) { fwrite(&val, sizeof(T), 1, fp); }
    template<typename T> void write_vec(const std::vector<T>& v) { fwrite(v.data(), sizeof(T), v.size(), fp); }
    void close() { if (fp) { fclose(fp); fp = nullptr; } }
    ~ColumnWriter() { close(); }
};

// ============================================================
// Find row boundaries in mmap'd file
// ============================================================
static std::vector<size_t> find_row_starts(const char* data, size_t size) {
    std::vector<size_t> starts;
    starts.reserve(size / 50); // rough estimate
    starts.push_back(0);
    for (size_t i = 0; i < size; i++) {
        if (data[i] == '\n' && i + 1 < size) {
            starts.push_back(i + 1);
        }
    }
    return starts;
}

// ============================================================
// Split a row into fields (pipe-delimited, trailing pipe)
// Returns pointers to field starts and ends
// ============================================================
struct FieldSlice { const char* start; const char* end; };

static inline int split_fields(const char* row_start, const char* row_end, FieldSlice* fields, int max_fields) {
    int n = 0;
    const char* p = row_start;
    while (p < row_end && n < max_fields) {
        fields[n].start = p;
        while (p < row_end && *p != '|' && *p != '\n') p++;
        fields[n].end = p;
        n++;
        if (p < row_end && *p == '|') p++;
    }
    return n;
}

// ============================================================
// Ingest lineitem (60M rows, largest table)
// ============================================================
static void ingest_lineitem(const std::string& data_dir, const std::string& out_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = out_dir + "/lineitem";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/lineitem.tbl")) { fprintf(stderr, "Failed to open lineitem.tbl\n"); exit(1); }
    printf("  lineitem: mapped %.2f GB\n", mf.size / 1e9);

    // Find row starts
    auto row_starts = find_row_starts(mf.data, mf.size);
    // Remove last entry if it points to empty line
    while (!row_starts.empty()) {
        size_t last = row_starts.back();
        if (last >= mf.size || mf.data[last] == '\n' || last == mf.size) row_starts.pop_back();
        else break;
    }
    size_t nrows = row_starts.size();
    printf("  lineitem: %zu rows\n", nrows);

    // Allocate columns
    std::vector<int32_t> col_orderkey(nrows), col_partkey(nrows), col_suppkey(nrows), col_shipdate(nrows);
    std::vector<double> col_quantity(nrows), col_extendedprice(nrows), col_discount(nrows), col_tax(nrows);
    std::vector<uint8_t> col_returnflag(nrows), col_linestatus(nrows);

    // Parse in parallel
    int nthreads = std::min((int)std::thread::hardware_concurrency(), 64);
    std::vector<std::thread> threads;
    size_t chunk = (nrows + nthreads - 1) / nthreads;

    for (int t = 0; t < nthreads; t++) {
        size_t start = t * chunk;
        size_t end = std::min(start + chunk, nrows);
        threads.emplace_back([&, start, end]() {
            FieldSlice fields[17];
            for (size_t i = start; i < end; i++) {
                const char* rstart = mf.data + row_starts[i];
                const char* rend;
                if (i + 1 < nrows) rend = mf.data + row_starts[i+1];
                else rend = mf.data + mf.size;
                // Trim trailing newline
                while (rend > rstart && (*(rend-1) == '\n' || *(rend-1) == '\r')) rend--;

                int nf = split_fields(rstart, rend, fields, 17);
                (void)nf;

                // col 0: l_orderkey (int32)
                col_orderkey[i] = parse_int(fields[0].start, fields[0].end);
                // col 1: l_partkey (int32)
                col_partkey[i] = parse_int(fields[1].start, fields[1].end);
                // col 2: l_suppkey (int32)
                col_suppkey[i] = parse_int(fields[2].start, fields[2].end);
                // col 3: l_linenumber - skip
                // col 4: l_quantity (double)
                col_quantity[i] = parse_double(fields[4].start, fields[4].end);
                // col 5: l_extendedprice (double)
                col_extendedprice[i] = parse_double(fields[5].start, fields[5].end);
                // col 6: l_discount (double)
                col_discount[i] = parse_double(fields[6].start, fields[6].end);
                // col 7: l_tax (double)
                col_tax[i] = parse_double(fields[7].start, fields[7].end);
                // col 8: l_returnflag (char → uint8)
                col_returnflag[i] = (uint8_t)fields[8].start[0];
                // col 9: l_linestatus (char → uint8)
                col_linestatus[i] = (uint8_t)fields[9].start[0];
                // col 10: l_shipdate (date → int32)
                col_shipdate[i] = parse_date(fields[10].start);
            }
        });
    }
    for (auto& th : threads) th.join();

    // Write columns
    auto write_col = [&](const std::string& name, const void* data, size_t elem_size) {
        ColumnWriter w;
        w.open(table_dir + "/" + name + ".bin");
        w.write_raw(data, nrows * elem_size);
        w.close();
    };

    // Write in parallel (multiple threads for I/O)
    std::vector<std::thread> wthreads;
    wthreads.emplace_back([&]() { write_col("l_orderkey", col_orderkey.data(), 4); });
    wthreads.emplace_back([&]() { write_col("l_partkey", col_partkey.data(), 4); });
    wthreads.emplace_back([&]() { write_col("l_suppkey", col_suppkey.data(), 4); });
    wthreads.emplace_back([&]() { write_col("l_quantity", col_quantity.data(), 8); });
    wthreads.emplace_back([&]() { write_col("l_extendedprice", col_extendedprice.data(), 8); });
    wthreads.emplace_back([&]() { write_col("l_discount", col_discount.data(), 8); });
    wthreads.emplace_back([&]() { write_col("l_tax", col_tax.data(), 8); });
    wthreads.emplace_back([&]() { write_col("l_returnflag", col_returnflag.data(), 1); });
    wthreads.emplace_back([&]() { write_col("l_linestatus", col_linestatus.data(), 1); });
    wthreads.emplace_back([&]() { write_col("l_shipdate", col_shipdate.data(), 4); });
    for (auto& th : wthreads) th.join();

    // Write meta
    FILE* meta = fopen((table_dir + "/meta.json").c_str(), "w");
    fprintf(meta, "{\"row_count\": %zu}\n", nrows);
    fclose(meta);

    auto t1 = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();
    printf("  lineitem: done in %.1f s\n", elapsed);
}

// ============================================================
// Ingest orders (15M rows)
// ============================================================
static void ingest_orders(const std::string& data_dir, const std::string& out_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = out_dir + "/orders";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/orders.tbl")) { fprintf(stderr, "Failed to open orders.tbl\n"); exit(1); }
    printf("  orders: mapped %.2f GB\n", mf.size / 1e9);

    auto row_starts = find_row_starts(mf.data, mf.size);
    while (!row_starts.empty()) {
        size_t last = row_starts.back();
        if (last >= mf.size || mf.data[last] == '\n' || last == mf.size) row_starts.pop_back();
        else break;
    }
    size_t nrows = row_starts.size();
    printf("  orders: %zu rows\n", nrows);

    std::vector<int32_t> col_orderkey(nrows), col_custkey(nrows), col_orderdate(nrows), col_shippriority(nrows);
    std::vector<double> col_totalprice(nrows);

    int nthreads = std::min((int)std::thread::hardware_concurrency(), 64);
    std::vector<std::thread> threads;
    size_t chunk = (nrows + nthreads - 1) / nthreads;

    for (int t = 0; t < nthreads; t++) {
        size_t start = t * chunk;
        size_t end = std::min(start + chunk, nrows);
        threads.emplace_back([&, start, end]() {
            FieldSlice fields[10];
            for (size_t i = start; i < end; i++) {
                const char* rstart = mf.data + row_starts[i];
                const char* rend;
                if (i + 1 < nrows) rend = mf.data + row_starts[i+1];
                else rend = mf.data + mf.size;
                while (rend > rstart && (*(rend-1) == '\n' || *(rend-1) == '\r')) rend--;

                split_fields(rstart, rend, fields, 10);

                col_orderkey[i] = parse_int(fields[0].start, fields[0].end);
                col_custkey[i] = parse_int(fields[1].start, fields[1].end);
                // col 2: o_orderstatus - skip
                col_totalprice[i] = parse_double(fields[3].start, fields[3].end);
                col_orderdate[i] = parse_date(fields[4].start);
                // col 5: o_orderpriority - skip
                // col 6: o_clerk - skip
                col_shippriority[i] = parse_int(fields[7].start, fields[7].end);
            }
        });
    }
    for (auto& th : threads) th.join();

    std::vector<std::thread> wthreads;
    auto write_col = [&](const std::string& name, const void* data, size_t elem_size) {
        ColumnWriter w;
        w.open(table_dir + "/" + name + ".bin");
        w.write_raw(data, nrows * elem_size);
        w.close();
    };
    wthreads.emplace_back([&]() { write_col("o_orderkey", col_orderkey.data(), 4); });
    wthreads.emplace_back([&]() { write_col("o_custkey", col_custkey.data(), 4); });
    wthreads.emplace_back([&]() { write_col("o_totalprice", col_totalprice.data(), 8); });
    wthreads.emplace_back([&]() { write_col("o_orderdate", col_orderdate.data(), 4); });
    wthreads.emplace_back([&]() { write_col("o_shippriority", col_shippriority.data(), 4); });
    for (auto& th : wthreads) th.join();

    FILE* meta = fopen((table_dir + "/meta.json").c_str(), "w");
    fprintf(meta, "{\"row_count\": %zu}\n", nrows);
    fclose(meta);

    auto t1 = std::chrono::high_resolution_clock::now();
    printf("  orders: done in %.1f s\n", std::chrono::duration<double>(t1 - t0).count());
}

// ============================================================
// Ingest customer (1.5M rows)
// ============================================================
static void ingest_customer(const std::string& data_dir, const std::string& out_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = out_dir + "/customer";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/customer.tbl")) { fprintf(stderr, "Failed to open customer.tbl\n"); exit(1); }

    auto row_starts = find_row_starts(mf.data, mf.size);
    while (!row_starts.empty()) {
        size_t last = row_starts.back();
        if (last >= mf.size || mf.data[last] == '\n' || last == mf.size) row_starts.pop_back();
        else break;
    }
    size_t nrows = row_starts.size();
    printf("  customer: %zu rows\n", nrows);

    std::vector<int32_t> col_custkey(nrows);
    std::vector<uint8_t> col_mktsegment(nrows);
    // For c_name: collect strings first, then write offsets+data
    std::vector<std::string> names(nrows);

    // Build mktsegment dictionary
    std::unordered_map<std::string, uint8_t> mkt_dict;

    // Single-threaded for simplicity (1.5M rows is fast)
    FieldSlice fields[9];
    for (size_t i = 0; i < nrows; i++) {
        const char* rstart = mf.data + row_starts[i];
        const char* rend;
        if (i + 1 < nrows) rend = mf.data + row_starts[i+1];
        else rend = mf.data + mf.size;
        while (rend > rstart && (*(rend-1) == '\n' || *(rend-1) == '\r')) rend--;

        split_fields(rstart, rend, fields, 9);

        col_custkey[i] = parse_int(fields[0].start, fields[0].end);
        names[i] = std::string(fields[1].start, fields[1].end);

        std::string seg(fields[6].start, fields[6].end);
        auto it = mkt_dict.find(seg);
        if (it == mkt_dict.end()) {
            uint8_t code = (uint8_t)mkt_dict.size();
            mkt_dict[seg] = code;
            it = mkt_dict.find(seg);
        }
        col_mktsegment[i] = it->second;
    }

    // Write columns
    {
        ColumnWriter w; w.open(table_dir + "/c_custkey.bin");
        w.write_raw(col_custkey.data(), nrows * 4); w.close();
    }
    {
        ColumnWriter w; w.open(table_dir + "/c_mktsegment.bin");
        w.write_raw(col_mktsegment.data(), nrows); w.close();
    }

    // Write c_name as offsets + data
    {
        std::vector<int64_t> offsets(nrows + 1);
        int64_t pos = 0;
        for (size_t i = 0; i < nrows; i++) {
            offsets[i] = pos;
            pos += (int64_t)names[i].size();
        }
        offsets[nrows] = pos;

        ColumnWriter wo; wo.open(table_dir + "/c_name_offsets.bin");
        wo.write_raw(offsets.data(), (nrows + 1) * sizeof(int64_t)); wo.close();

        ColumnWriter wd; wd.open(table_dir + "/c_name_data.bin");
        for (size_t i = 0; i < nrows; i++) {
            wd.write_raw(names[i].data(), names[i].size());
        }
        wd.close();
    }

    // Write mktsegment dictionary
    {
        // Sort by code to ensure deterministic order
        std::vector<std::pair<uint8_t, std::string>> dict_entries;
        for (auto& [k, v] : mkt_dict) dict_entries.push_back({v, k});
        std::sort(dict_entries.begin(), dict_entries.end());

        FILE* f = fopen((table_dir + "/c_mktsegment_dict.bin").c_str(), "w");
        for (auto& [code, name] : dict_entries) {
            fprintf(f, "%d|%s\n", code, name.c_str());
        }
        fclose(f);
    }

    FILE* meta = fopen((table_dir + "/meta.json").c_str(), "w");
    fprintf(meta, "{\"row_count\": %zu}\n", nrows);
    fclose(meta);

    auto t1 = std::chrono::high_resolution_clock::now();
    printf("  customer: done in %.1f s\n", std::chrono::duration<double>(t1 - t0).count());
}

// ============================================================
// Ingest part (2M rows)
// ============================================================
static void ingest_part(const std::string& data_dir, const std::string& out_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = out_dir + "/part";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/part.tbl")) { fprintf(stderr, "Failed to open part.tbl\n"); exit(1); }

    auto row_starts = find_row_starts(mf.data, mf.size);
    while (!row_starts.empty()) {
        size_t last = row_starts.back();
        if (last >= mf.size || mf.data[last] == '\n' || last == mf.size) row_starts.pop_back();
        else break;
    }
    size_t nrows = row_starts.size();
    printf("  part: %zu rows\n", nrows);

    std::vector<int32_t> col_partkey(nrows);
    std::vector<std::string> names(nrows);

    int nthreads = std::min((int)std::thread::hardware_concurrency(), 32);
    std::vector<std::thread> threads;
    size_t chunk = (nrows + nthreads - 1) / nthreads;

    for (int t = 0; t < nthreads; t++) {
        size_t start = t * chunk;
        size_t end = std::min(start + chunk, nrows);
        threads.emplace_back([&, start, end]() {
            FieldSlice fields[10];
            for (size_t i = start; i < end; i++) {
                const char* rstart = mf.data + row_starts[i];
                const char* rend;
                if (i + 1 < nrows) rend = mf.data + row_starts[i+1];
                else rend = mf.data + mf.size;
                while (rend > rstart && (*(rend-1) == '\n' || *(rend-1) == '\r')) rend--;

                split_fields(rstart, rend, fields, 10);

                col_partkey[i] = parse_int(fields[0].start, fields[0].end);
                names[i] = std::string(fields[1].start, fields[1].end);
            }
        });
    }
    for (auto& th : threads) th.join();

    {
        ColumnWriter w; w.open(table_dir + "/p_partkey.bin");
        w.write_raw(col_partkey.data(), nrows * 4); w.close();
    }
    // Write p_name as offsets + data
    {
        std::vector<int64_t> offsets(nrows + 1);
        int64_t pos = 0;
        for (size_t i = 0; i < nrows; i++) {
            offsets[i] = pos;
            pos += (int64_t)names[i].size();
        }
        offsets[nrows] = pos;

        ColumnWriter wo; wo.open(table_dir + "/p_name_offsets.bin");
        wo.write_raw(offsets.data(), (nrows + 1) * sizeof(int64_t)); wo.close();

        ColumnWriter wd; wd.open(table_dir + "/p_name_data.bin");
        for (size_t i = 0; i < nrows; i++) {
            wd.write_raw(names[i].data(), names[i].size());
        }
        wd.close();
    }

    FILE* meta = fopen((table_dir + "/meta.json").c_str(), "w");
    fprintf(meta, "{\"row_count\": %zu}\n", nrows);
    fclose(meta);

    auto t1 = std::chrono::high_resolution_clock::now();
    printf("  part: done in %.1f s\n", std::chrono::duration<double>(t1 - t0).count());
}

// ============================================================
// Ingest partsupp (8M rows)
// ============================================================
static void ingest_partsupp(const std::string& data_dir, const std::string& out_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = out_dir + "/partsupp";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/partsupp.tbl")) { fprintf(stderr, "Failed to open partsupp.tbl\n"); exit(1); }

    auto row_starts = find_row_starts(mf.data, mf.size);
    while (!row_starts.empty()) {
        size_t last = row_starts.back();
        if (last >= mf.size || mf.data[last] == '\n' || last == mf.size) row_starts.pop_back();
        else break;
    }
    size_t nrows = row_starts.size();
    printf("  partsupp: %zu rows\n", nrows);

    std::vector<int32_t> col_partkey(nrows), col_suppkey(nrows);
    std::vector<double> col_supplycost(nrows);

    int nthreads = std::min((int)std::thread::hardware_concurrency(), 32);
    std::vector<std::thread> threads;
    size_t chunk = (nrows + nthreads - 1) / nthreads;

    for (int t = 0; t < nthreads; t++) {
        size_t start = t * chunk;
        size_t end = std::min(start + chunk, nrows);
        threads.emplace_back([&, start, end]() {
            FieldSlice fields[6];
            for (size_t i = start; i < end; i++) {
                const char* rstart = mf.data + row_starts[i];
                const char* rend;
                if (i + 1 < nrows) rend = mf.data + row_starts[i+1];
                else rend = mf.data + mf.size;
                while (rend > rstart && (*(rend-1) == '\n' || *(rend-1) == '\r')) rend--;

                split_fields(rstart, rend, fields, 6);

                col_partkey[i] = parse_int(fields[0].start, fields[0].end);
                col_suppkey[i] = parse_int(fields[1].start, fields[1].end);
                // col 2: ps_availqty - skip
                col_supplycost[i] = parse_double(fields[3].start, fields[3].end);
            }
        });
    }
    for (auto& th : threads) th.join();

    auto write_col = [&](const std::string& name, const void* data, size_t elem_size) {
        ColumnWriter w;
        w.open(table_dir + "/" + name + ".bin");
        w.write_raw(data, nrows * elem_size);
        w.close();
    };
    write_col("ps_partkey", col_partkey.data(), 4);
    write_col("ps_suppkey", col_suppkey.data(), 4);
    write_col("ps_supplycost", col_supplycost.data(), 8);

    FILE* meta = fopen((table_dir + "/meta.json").c_str(), "w");
    fprintf(meta, "{\"row_count\": %zu}\n", nrows);
    fclose(meta);

    auto t1 = std::chrono::high_resolution_clock::now();
    printf("  partsupp: done in %.1f s\n", std::chrono::duration<double>(t1 - t0).count());
}

// ============================================================
// Ingest supplier (100K rows)
// ============================================================
static void ingest_supplier(const std::string& data_dir, const std::string& out_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = out_dir + "/supplier";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/supplier.tbl")) { fprintf(stderr, "Failed to open supplier.tbl\n"); exit(1); }

    auto row_starts = find_row_starts(mf.data, mf.size);
    while (!row_starts.empty()) {
        size_t last = row_starts.back();
        if (last >= mf.size || mf.data[last] == '\n' || last == mf.size) row_starts.pop_back();
        else break;
    }
    size_t nrows = row_starts.size();
    printf("  supplier: %zu rows\n", nrows);

    std::vector<int32_t> col_suppkey(nrows), col_nationkey(nrows);

    FieldSlice fields[8];
    for (size_t i = 0; i < nrows; i++) {
        const char* rstart = mf.data + row_starts[i];
        const char* rend;
        if (i + 1 < nrows) rend = mf.data + row_starts[i+1];
        else rend = mf.data + mf.size;
        while (rend > rstart && (*(rend-1) == '\n' || *(rend-1) == '\r')) rend--;

        split_fields(rstart, rend, fields, 8);

        col_suppkey[i] = parse_int(fields[0].start, fields[0].end);
        col_nationkey[i] = parse_int(fields[3].start, fields[3].end);
    }

    auto write_col = [&](const std::string& name, const void* data, size_t elem_size) {
        ColumnWriter w;
        w.open(table_dir + "/" + name + ".bin");
        w.write_raw(data, nrows * elem_size);
        w.close();
    };
    write_col("s_suppkey", col_suppkey.data(), 4);
    write_col("s_nationkey", col_nationkey.data(), 4);

    FILE* meta = fopen((table_dir + "/meta.json").c_str(), "w");
    fprintf(meta, "{\"row_count\": %zu}\n", nrows);
    fclose(meta);

    auto t1 = std::chrono::high_resolution_clock::now();
    printf("  supplier: done in %.1f s\n", std::chrono::duration<double>(t1 - t0).count());
}

// ============================================================
// Ingest nation (25 rows)
// ============================================================
static void ingest_nation(const std::string& data_dir, const std::string& out_dir) {
    std::string table_dir = out_dir + "/nation";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/nation.tbl")) { fprintf(stderr, "Failed to open nation.tbl\n"); exit(1); }

    auto row_starts = find_row_starts(mf.data, mf.size);
    while (!row_starts.empty()) {
        size_t last = row_starts.back();
        if (last >= mf.size || mf.data[last] == '\n' || last == mf.size) row_starts.pop_back();
        else break;
    }
    size_t nrows = row_starts.size();
    printf("  nation: %zu rows\n", nrows);

    std::vector<int32_t> col_nationkey(nrows);
    std::vector<std::string> names(nrows);

    FieldSlice fields[5];
    for (size_t i = 0; i < nrows; i++) {
        const char* rstart = mf.data + row_starts[i];
        const char* rend;
        if (i + 1 < nrows) rend = mf.data + row_starts[i+1];
        else rend = mf.data + mf.size;
        while (rend > rstart && (*(rend-1) == '\n' || *(rend-1) == '\r')) rend--;

        split_fields(rstart, rend, fields, 5);

        col_nationkey[i] = parse_int(fields[0].start, fields[0].end);
        names[i] = std::string(fields[1].start, fields[1].end);
    }

    {
        ColumnWriter w; w.open(table_dir + "/n_nationkey.bin");
        w.write_raw(col_nationkey.data(), nrows * 4); w.close();
    }
    {
        std::vector<int64_t> offsets(nrows + 1);
        int64_t pos = 0;
        for (size_t i = 0; i < nrows; i++) { offsets[i] = pos; pos += (int64_t)names[i].size(); }
        offsets[nrows] = pos;
        ColumnWriter wo; wo.open(table_dir + "/n_name_offsets.bin");
        wo.write_raw(offsets.data(), (nrows + 1) * sizeof(int64_t)); wo.close();
        ColumnWriter wd; wd.open(table_dir + "/n_name_data.bin");
        for (size_t i = 0; i < nrows; i++) wd.write_raw(names[i].data(), names[i].size());
        wd.close();
    }

    FILE* meta = fopen((table_dir + "/meta.json").c_str(), "w");
    fprintf(meta, "{\"row_count\": %zu}\n", nrows);
    fclose(meta);
    printf("  nation: done\n");
}

// ============================================================
// Ingest region (5 rows)
// ============================================================
static void ingest_region(const std::string& data_dir, const std::string& out_dir) {
    std::string table_dir = out_dir + "/region";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/region.tbl")) { fprintf(stderr, "Failed to open region.tbl\n"); exit(1); }

    auto row_starts = find_row_starts(mf.data, mf.size);
    while (!row_starts.empty()) {
        size_t last = row_starts.back();
        if (last >= mf.size || mf.data[last] == '\n' || last == mf.size) row_starts.pop_back();
        else break;
    }
    size_t nrows = row_starts.size();
    printf("  region: %zu rows\n", nrows);

    std::vector<int32_t> col_regionkey(nrows);
    std::vector<std::string> names(nrows);

    FieldSlice fields[4];
    for (size_t i = 0; i < nrows; i++) {
        const char* rstart = mf.data + row_starts[i];
        const char* rend;
        if (i + 1 < nrows) rend = mf.data + row_starts[i+1];
        else rend = mf.data + mf.size;
        while (rend > rstart && (*(rend-1) == '\n' || *(rend-1) == '\r')) rend--;

        split_fields(rstart, rend, fields, 4);

        col_regionkey[i] = parse_int(fields[0].start, fields[0].end);
        names[i] = std::string(fields[1].start, fields[1].end);
    }

    {
        ColumnWriter w; w.open(table_dir + "/r_regionkey.bin");
        w.write_raw(col_regionkey.data(), nrows * 4); w.close();
    }
    {
        std::vector<int64_t> offsets(nrows + 1);
        int64_t pos = 0;
        for (size_t i = 0; i < nrows; i++) { offsets[i] = pos; pos += (int64_t)names[i].size(); }
        offsets[nrows] = pos;
        ColumnWriter wo; wo.open(table_dir + "/r_name_offsets.bin");
        wo.write_raw(offsets.data(), (nrows + 1) * sizeof(int64_t)); wo.close();
        ColumnWriter wd; wd.open(table_dir + "/r_name_data.bin");
        for (size_t i = 0; i < nrows; i++) wd.write_raw(names[i].data(), names[i].size());
        wd.close();
    }

    FILE* meta = fopen((table_dir + "/meta.json").c_str(), "w");
    fprintf(meta, "{\"row_count\": %zu}\n", nrows);
    fclose(meta);
    printf("  region: done\n");
}

// ============================================================
// Main
// ============================================================
int main(int argc, char** argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <data_dir> <output_dir>\n", argv[0]);
        return 1;
    }
    std::string data_dir = argv[1];
    std::string out_dir = argv[2];

    printf("Ingesting TPC-H data from %s to %s\n", data_dir.c_str(), out_dir.c_str());
    auto t0 = std::chrono::high_resolution_clock::now();

    // Create output directories
    fs::create_directories(out_dir);

    // Ingest tables in parallel (large tables first)
    std::thread t_lineitem([&]() { ingest_lineitem(data_dir, out_dir); });
    std::thread t_orders([&]() { ingest_orders(data_dir, out_dir); });
    std::thread t_customer([&]() { ingest_customer(data_dir, out_dir); });
    std::thread t_part([&]() { ingest_part(data_dir, out_dir); });
    std::thread t_partsupp([&]() { ingest_partsupp(data_dir, out_dir); });
    std::thread t_supplier([&]() { ingest_supplier(data_dir, out_dir); });
    std::thread t_nation([&]() { ingest_nation(data_dir, out_dir); });
    std::thread t_region([&]() { ingest_region(data_dir, out_dir); });

    t_lineitem.join();
    t_orders.join();
    t_customer.join();
    t_part.join();
    t_partsupp.join();
    t_supplier.join();
    t_nation.join();
    t_region.join();

    auto t1 = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();
    printf("\nTotal ingestion time: %.1f s\n", elapsed);
    return 0;
}
