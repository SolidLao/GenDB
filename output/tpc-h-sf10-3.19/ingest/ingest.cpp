#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <filesystem>
#include <algorithm>
#include <unordered_map>
#include <mutex>
#include <cassert>
#include <chrono>
#include <numeric>

namespace fs = std::filesystem;

// ============================================================
// Helpers
// ============================================================

static int32_t parse_date(const char* s) {
    // "YYYY-MM-DD" -> days since 1970-01-01 (Howard Hinnant's algorithm)
    int y = (s[0]-'0')*1000 + (s[1]-'0')*100 + (s[2]-'0')*10 + (s[3]-'0');
    int m = (s[5]-'0')*10 + (s[6]-'0');
    int d = (s[8]-'0')*10 + (s[9]-'0');
    y -= (m <= 2);
    int era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int)doe - 719468;
}

static inline int32_t fast_atoi(const char* s, const char* end) {
    int32_t val = 0;
    bool neg = false;
    if (s < end && *s == '-') { neg = true; ++s; }
    while (s < end) { val = val * 10 + (*s - '0'); ++s; }
    return neg ? -val : val;
}

static inline double fast_atof(const char* s, const char* end) {
    // Simple fast parser for TPC-H decimals: optional sign, digits, '.', digits
    double val = 0.0;
    bool neg = false;
    if (s < end && *s == '-') { neg = true; ++s; }
    while (s < end && *s != '.') { val = val * 10.0 + (*s - '0'); ++s; }
    if (s < end && *s == '.') {
        ++s;
        double frac = 0.0, div = 1.0;
        while (s < end) { frac = frac * 10.0 + (*s - '0'); div *= 10.0; ++s; }
        val += frac / div;
    }
    return neg ? -val : val;
}

// Field iterator for pipe-delimited lines
struct FieldIter {
    const char* line;
    const char* end;
    const char* field_start;
    const char* field_end;

    FieldIter(const char* l, const char* e) : line(l), end(e), field_start(l), field_end(nullptr) {
        advance_to_delim();
    }
    void advance_to_delim() {
        field_end = field_start;
        while (field_end < end && *field_end != '|') ++field_end;
    }
    void next() {
        field_start = field_end + 1;
        advance_to_delim();
    }
    int32_t as_int() { return fast_atoi(field_start, field_end); }
    double as_double() { return fast_atof(field_start, field_end); }
    int32_t as_date() { return parse_date(field_start); }
    int8_t as_char() { return (int8_t)*field_start; }
    std::string as_string() { return std::string(field_start, field_end - field_start); }
    const char* ptr() { return field_start; }
    size_t len() { return (size_t)(field_end - field_start); }
};

// ============================================================
// MappedFile
// ============================================================
struct MappedFile {
    const char* data = nullptr;
    size_t size = 0;
    int fd = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::cerr << "Cannot open " << path << "\n"; return false; }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (data == MAP_FAILED) { data = nullptr; close(fd); fd = -1; return false; }
        madvise((void*)data, size, MADV_SEQUENTIAL);
        return true;
    }
    ~MappedFile() {
        if (data && data != MAP_FAILED) munmap((void*)data, size);
        if (fd >= 0) close(fd);
    }
};

// ============================================================
// Dictionary builder (thread-safe)
// ============================================================
struct DictBuilder {
    std::unordered_map<std::string, int8_t> map;
    std::vector<std::string> values;
    std::mutex mtx;
    int8_t next_code = 0;

    int8_t encode(const char* s, size_t len) {
        std::string key(s, len);
        {
            std::lock_guard<std::mutex> lock(mtx);
            auto it = map.find(key);
            if (it != map.end()) return it->second;
            int8_t code = next_code++;
            map[key] = code;
            values.push_back(key);
            return code;
        }
    }

    void save(const std::string& path) {
        std::ofstream f(path, std::ios::binary);
        uint32_t count = (uint32_t)values.size();
        f.write((char*)&count, 4);
        for (auto& v : values) {
            uint16_t len = (uint16_t)v.size();
            f.write((char*)&len, 2);
            f.write(v.data(), len);
        }
    }
};

// 16-bit dict variant
struct DictBuilder16 {
    std::unordered_map<std::string, int16_t> map;
    std::vector<std::string> values;
    std::mutex mtx;
    int16_t next_code = 0;

    int16_t encode(const char* s, size_t len) {
        std::string key(s, len);
        std::lock_guard<std::mutex> lock(mtx);
        auto it = map.find(key);
        if (it != map.end()) return it->second;
        int16_t code = next_code++;
        map[key] = code;
        values.push_back(key);
        return code;
    }

    void save(const std::string& path) {
        std::ofstream f(path, std::ios::binary);
        uint32_t count = (uint32_t)values.size();
        f.write((char*)&count, 4);
        for (auto& v : values) {
            uint16_t len = (uint16_t)v.size();
            f.write((char*)&len, 2);
            f.write(v.data(), len);
        }
    }
};

// ============================================================
// Binary writers
// ============================================================
template<typename T>
void write_bin(const std::string& path, const T* data, size_t count) {
    std::ofstream f(path, std::ios::binary);
    f.write((const char*)data, count * sizeof(T));
}

void write_meta(const std::string& dir, uint64_t row_count) {
    std::ofstream f(dir + "/meta.bin", std::ios::binary);
    f.write((char*)&row_count, sizeof(row_count));
}

// Write varlen string column: offsets.bin (uint32_t[N+1]) + data.bin
void write_varlen(const std::string& col_path,
                  const std::vector<std::pair<const char*, size_t>>& strings) {
    size_t N = strings.size();
    std::vector<uint32_t> offsets(N + 1);
    uint32_t off = 0;
    for (size_t i = 0; i < N; i++) {
        offsets[i] = off;
        off += (uint32_t)strings[i].second;
    }
    offsets[N] = off;
    write_bin(col_path + "_offsets.bin", offsets.data(), N + 1);

    std::ofstream f(col_path + "_data.bin", std::ios::binary);
    for (size_t i = 0; i < N; i++) {
        f.write(strings[i].first, strings[i].second);
    }
}

// ============================================================
// Line finding: find all line-start positions in a file
// ============================================================
std::vector<size_t> find_line_starts(const char* data, size_t size) {
    std::vector<size_t> starts;
    starts.reserve(size / 80); // rough estimate
    starts.push_back(0);
    for (size_t i = 0; i < size; i++) {
        if (data[i] == '\n' && i + 1 < size) {
            starts.push_back(i + 1);
        }
    }
    return starts;
}

// ============================================================
// LINEITEM ingestion (parallel)
// ============================================================
void ingest_lineitem(const std::string& data_dir, const std::string& storage_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/lineitem";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/lineitem.tbl")) return;

    std::cout << "  Finding lines..." << std::flush;
    auto lines = find_line_starts(mf.data, mf.size);
    size_t N = lines.size();
    // Remove trailing empty line if any
    if (N > 0) {
        size_t last_start = lines[N-1];
        if (last_start >= mf.size || mf.data[last_start] == '\n') N--;
    }
    std::cout << " " << N << " rows\n";

    // Allocate columns
    std::vector<int32_t> l_orderkey(N), l_partkey(N), l_suppkey(N), l_linenumber(N);
    std::vector<double> l_quantity(N), l_extendedprice(N), l_discount(N), l_tax(N);
    std::vector<int8_t> l_returnflag(N), l_linestatus(N);
    std::vector<int32_t> l_shipdate(N), l_commitdate(N), l_receiptdate(N);
    std::vector<int8_t> l_shipinstruct(N), l_shipmode(N);

    // Varlen: store pointers into mmap (valid while mf is alive)
    std::vector<std::pair<const char*, size_t>> l_comment_strs(N);

    DictBuilder dict_shipinstruct, dict_shipmode;

    std::cout << "  Parsing..." << std::flush;
    #pragma omp parallel for schedule(dynamic, 10000)
    for (size_t i = 0; i < N; i++) {
        const char* line = mf.data + lines[i];
        const char* line_end = (i + 1 < lines.size()) ? mf.data + lines[i+1] - 1 : mf.data + mf.size;
        // Trim trailing newline/delimiter
        while (line_end > line && (line_end[-1] == '\n' || line_end[-1] == '\r' || line_end[-1] == '|'))
            --line_end;

        FieldIter f(line, line_end);
        l_orderkey[i] = f.as_int(); f.next();
        l_partkey[i] = f.as_int(); f.next();
        l_suppkey[i] = f.as_int(); f.next();
        l_linenumber[i] = f.as_int(); f.next();
        l_quantity[i] = f.as_double(); f.next();
        l_extendedprice[i] = f.as_double(); f.next();
        l_discount[i] = f.as_double(); f.next();
        l_tax[i] = f.as_double(); f.next();
        l_returnflag[i] = f.as_char(); f.next();
        l_linestatus[i] = f.as_char(); f.next();
        l_shipdate[i] = f.as_date(); f.next();
        l_commitdate[i] = f.as_date(); f.next();
        l_receiptdate[i] = f.as_date(); f.next();
        l_shipinstruct[i] = dict_shipinstruct.encode(f.ptr(), f.len()); f.next();
        l_shipmode[i] = dict_shipmode.encode(f.ptr(), f.len()); f.next();
        l_comment_strs[i] = {f.ptr(), f.len()};
    }
    std::cout << " done\n";

    // Verify sort order
    bool sorted = true;
    for (size_t i = 1; i < N && sorted; i++) {
        if (l_orderkey[i] < l_orderkey[i-1]) sorted = false;
    }
    if (!sorted) {
        std::cout << "  WARNING: lineitem not sorted by l_orderkey, sorting...\n";
        std::vector<size_t> idx(N);
        std::iota(idx.begin(), idx.end(), 0);
        std::sort(idx.begin(), idx.end(), [&](size_t a, size_t b) {
            return l_orderkey[a] < l_orderkey[b];
        });
        // Permute all columns in-place using the index
        auto permute = [&](auto& vec) {
            using T = typename std::remove_reference<decltype(vec)>::type::value_type;
            std::vector<T> tmp(N);
            for (size_t i = 0; i < N; i++) tmp[i] = vec[idx[i]];
            vec.swap(tmp);
        };
        permute(l_orderkey); permute(l_partkey); permute(l_suppkey); permute(l_linenumber);
        permute(l_quantity); permute(l_extendedprice); permute(l_discount); permute(l_tax);
        permute(l_returnflag); permute(l_linestatus);
        permute(l_shipdate); permute(l_commitdate); permute(l_receiptdate);
        permute(l_shipinstruct); permute(l_shipmode);
        permute(l_comment_strs);
    }

    std::cout << "  Writing columns..." << std::flush;
    write_bin(table_dir + "/l_orderkey.bin", l_orderkey.data(), N);
    write_bin(table_dir + "/l_partkey.bin", l_partkey.data(), N);
    write_bin(table_dir + "/l_suppkey.bin", l_suppkey.data(), N);
    write_bin(table_dir + "/l_linenumber.bin", l_linenumber.data(), N);
    write_bin(table_dir + "/l_quantity.bin", l_quantity.data(), N);
    write_bin(table_dir + "/l_extendedprice.bin", l_extendedprice.data(), N);
    write_bin(table_dir + "/l_discount.bin", l_discount.data(), N);
    write_bin(table_dir + "/l_tax.bin", l_tax.data(), N);
    write_bin(table_dir + "/l_returnflag.bin", l_returnflag.data(), N);
    write_bin(table_dir + "/l_linestatus.bin", l_linestatus.data(), N);
    write_bin(table_dir + "/l_shipdate.bin", l_shipdate.data(), N);
    write_bin(table_dir + "/l_commitdate.bin", l_commitdate.data(), N);
    write_bin(table_dir + "/l_receiptdate.bin", l_receiptdate.data(), N);
    write_bin(table_dir + "/l_shipinstruct.bin", l_shipinstruct.data(), N);
    write_bin(table_dir + "/l_shipmode.bin", l_shipmode.data(), N);
    dict_shipinstruct.save(table_dir + "/l_shipinstruct_dict.bin");
    dict_shipmode.save(table_dir + "/l_shipmode_dict.bin");
    write_varlen(table_dir + "/l_comment", l_comment_strs);
    write_meta(table_dir, N);
    std::cout << " done\n";

    auto t1 = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();
    std::cout << "  lineitem: " << N << " rows in " << elapsed << "s\n";
}

// ============================================================
// ORDERS ingestion (parallel)
// ============================================================
void ingest_orders(const std::string& data_dir, const std::string& storage_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/orders";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/orders.tbl")) return;

    auto lines_vec = find_line_starts(mf.data, mf.size);
    size_t N = lines_vec.size();
    if (N > 0) {
        size_t last_start = lines_vec[N-1];
        if (last_start >= mf.size || mf.data[last_start] == '\n') N--;
    }
    std::cout << "  orders: " << N << " rows\n";

    std::vector<int32_t> o_orderkey(N), o_custkey(N), o_shippriority(N);
    std::vector<int8_t> o_orderstatus(N);
    std::vector<double> o_totalprice(N);
    std::vector<int32_t> o_orderdate(N);
    std::vector<int8_t> o_orderpriority(N);
    std::vector<std::pair<const char*, size_t>> o_clerk_strs(N), o_comment_strs(N);
    DictBuilder dict_orderpriority;

    #pragma omp parallel for schedule(dynamic, 10000)
    for (size_t i = 0; i < N; i++) {
        const char* line = mf.data + lines_vec[i];
        const char* line_end = (i + 1 < lines_vec.size()) ? mf.data + lines_vec[i+1] - 1 : mf.data + mf.size;
        while (line_end > line && (line_end[-1] == '\n' || line_end[-1] == '\r' || line_end[-1] == '|'))
            --line_end;

        FieldIter f(line, line_end);
        o_orderkey[i] = f.as_int(); f.next();
        o_custkey[i] = f.as_int(); f.next();
        o_orderstatus[i] = f.as_char(); f.next();
        o_totalprice[i] = f.as_double(); f.next();
        o_orderdate[i] = f.as_date(); f.next();
        o_orderpriority[i] = dict_orderpriority.encode(f.ptr(), f.len()); f.next();
        o_clerk_strs[i] = {f.ptr(), f.len()}; f.next();
        o_shippriority[i] = f.as_int(); f.next();
        o_comment_strs[i] = {f.ptr(), f.len()};
    }

    // Verify sort
    bool sorted = true;
    for (size_t i = 1; i < N && sorted; i++) {
        if (o_orderkey[i] < o_orderkey[i-1]) sorted = false;
    }
    if (!sorted) {
        std::cout << "  WARNING: orders not sorted by o_orderkey, sorting...\n";
        std::vector<size_t> idx(N);
        std::iota(idx.begin(), idx.end(), 0);
        std::sort(idx.begin(), idx.end(), [&](size_t a, size_t b) {
            return o_orderkey[a] < o_orderkey[b];
        });
        auto permute = [&](auto& vec) {
            using T = typename std::remove_reference<decltype(vec)>::type::value_type;
            std::vector<T> tmp(N);
            for (size_t i = 0; i < N; i++) tmp[i] = vec[idx[i]];
            vec.swap(tmp);
        };
        permute(o_orderkey); permute(o_custkey); permute(o_orderstatus);
        permute(o_totalprice); permute(o_orderdate); permute(o_orderpriority);
        permute(o_shippriority); permute(o_clerk_strs); permute(o_comment_strs);
    }

    write_bin(table_dir + "/o_orderkey.bin", o_orderkey.data(), N);
    write_bin(table_dir + "/o_custkey.bin", o_custkey.data(), N);
    write_bin(table_dir + "/o_orderstatus.bin", o_orderstatus.data(), N);
    write_bin(table_dir + "/o_totalprice.bin", o_totalprice.data(), N);
    write_bin(table_dir + "/o_orderdate.bin", o_orderdate.data(), N);
    write_bin(table_dir + "/o_orderpriority.bin", o_orderpriority.data(), N);
    dict_orderpriority.save(table_dir + "/o_orderpriority_dict.bin");
    write_varlen(table_dir + "/o_clerk", o_clerk_strs);
    write_bin(table_dir + "/o_shippriority.bin", o_shippriority.data(), N);
    write_varlen(table_dir + "/o_comment", o_comment_strs);
    write_meta(table_dir, N);

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "  orders: " << N << " rows in " << std::chrono::duration<double>(t1-t0).count() << "s\n";
}

// ============================================================
// Generic small-table ingestion helpers
// ============================================================

// Read all lines from a mapped file
std::vector<std::pair<const char*, const char*>> get_all_lines(const MappedFile& mf) {
    std::vector<std::pair<const char*, const char*>> result;
    const char* p = mf.data;
    const char* end = mf.data + mf.size;
    while (p < end) {
        const char* line_start = p;
        while (p < end && *p != '\n') ++p;
        const char* line_end = p;
        // Trim trailing delimiters
        while (line_end > line_start && (line_end[-1] == '\r' || line_end[-1] == '|'))
            --line_end;
        if (line_end > line_start)
            result.push_back({line_start, line_end});
        if (p < end) ++p; // skip \n
    }
    return result;
}

// ============================================================
// CUSTOMER
// ============================================================
void ingest_customer(const std::string& data_dir, const std::string& storage_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/customer";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/customer.tbl")) return;
    auto all_lines = get_all_lines(mf);
    size_t N = all_lines.size();

    std::vector<int32_t> c_custkey(N), c_nationkey(N);
    std::vector<double> c_acctbal(N);
    std::vector<int8_t> c_mktsegment(N);
    std::vector<std::pair<const char*, size_t>> c_name_strs(N), c_address_strs(N), c_phone_strs(N), c_comment_strs(N);
    DictBuilder dict_mktsegment;

    #pragma omp parallel for schedule(dynamic, 10000)
    for (size_t i = 0; i < N; i++) {
        FieldIter f(all_lines[i].first, all_lines[i].second);
        c_custkey[i] = f.as_int(); f.next();
        c_name_strs[i] = {f.ptr(), f.len()}; f.next();
        c_address_strs[i] = {f.ptr(), f.len()}; f.next();
        c_nationkey[i] = f.as_int(); f.next();
        c_phone_strs[i] = {f.ptr(), f.len()}; f.next();
        c_acctbal[i] = f.as_double(); f.next();
        c_mktsegment[i] = dict_mktsegment.encode(f.ptr(), f.len()); f.next();
        c_comment_strs[i] = {f.ptr(), f.len()};
    }

    write_bin(table_dir + "/c_custkey.bin", c_custkey.data(), N);
    write_varlen(table_dir + "/c_name", c_name_strs);
    write_varlen(table_dir + "/c_address", c_address_strs);
    write_bin(table_dir + "/c_nationkey.bin", c_nationkey.data(), N);
    write_varlen(table_dir + "/c_phone", c_phone_strs);
    write_bin(table_dir + "/c_acctbal.bin", c_acctbal.data(), N);
    write_bin(table_dir + "/c_mktsegment.bin", c_mktsegment.data(), N);
    dict_mktsegment.save(table_dir + "/c_mktsegment_dict.bin");
    write_varlen(table_dir + "/c_comment", c_comment_strs);
    write_meta(table_dir, N);

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "  customer: " << N << " rows in " << std::chrono::duration<double>(t1-t0).count() << "s\n";
}

// ============================================================
// PART
// ============================================================
void ingest_part(const std::string& data_dir, const std::string& storage_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/part";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/part.tbl")) return;
    auto all_lines = get_all_lines(mf);
    size_t N = all_lines.size();

    std::vector<int32_t> p_partkey(N), p_size(N);
    std::vector<double> p_retailprice(N);
    std::vector<int8_t> p_mfgr(N), p_brand(N), p_container(N);
    std::vector<int16_t> p_type(N);
    std::vector<std::pair<const char*, size_t>> p_name_strs(N), p_comment_strs(N);
    DictBuilder dict_mfgr, dict_brand, dict_container;
    DictBuilder16 dict_type;

    #pragma omp parallel for schedule(dynamic, 10000)
    for (size_t i = 0; i < N; i++) {
        FieldIter f(all_lines[i].first, all_lines[i].second);
        p_partkey[i] = f.as_int(); f.next();
        p_name_strs[i] = {f.ptr(), f.len()}; f.next();
        p_mfgr[i] = dict_mfgr.encode(f.ptr(), f.len()); f.next();
        p_brand[i] = dict_brand.encode(f.ptr(), f.len()); f.next();
        p_type[i] = dict_type.encode(f.ptr(), f.len()); f.next();
        p_size[i] = f.as_int(); f.next();
        p_container[i] = dict_container.encode(f.ptr(), f.len()); f.next();
        p_retailprice[i] = f.as_double(); f.next();
        p_comment_strs[i] = {f.ptr(), f.len()};
    }

    write_bin(table_dir + "/p_partkey.bin", p_partkey.data(), N);
    write_varlen(table_dir + "/p_name", p_name_strs);
    write_bin(table_dir + "/p_mfgr.bin", p_mfgr.data(), N);
    dict_mfgr.save(table_dir + "/p_mfgr_dict.bin");
    write_bin(table_dir + "/p_brand.bin", p_brand.data(), N);
    dict_brand.save(table_dir + "/p_brand_dict.bin");
    write_bin(table_dir + "/p_type.bin", p_type.data(), N);
    dict_type.save(table_dir + "/p_type_dict.bin");
    write_bin(table_dir + "/p_size.bin", p_size.data(), N);
    write_bin(table_dir + "/p_container.bin", p_container.data(), N);
    dict_container.save(table_dir + "/p_container_dict.bin");
    write_bin(table_dir + "/p_retailprice.bin", p_retailprice.data(), N);
    write_varlen(table_dir + "/p_comment", p_comment_strs);
    write_meta(table_dir, N);

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "  part: " << N << " rows in " << std::chrono::duration<double>(t1-t0).count() << "s\n";
}

// ============================================================
// SUPPLIER
// ============================================================
void ingest_supplier(const std::string& data_dir, const std::string& storage_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/supplier";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/supplier.tbl")) return;
    auto all_lines = get_all_lines(mf);
    size_t N = all_lines.size();

    std::vector<int32_t> s_suppkey(N), s_nationkey(N);
    std::vector<double> s_acctbal(N);
    std::vector<std::pair<const char*, size_t>> s_name_strs(N), s_address_strs(N), s_phone_strs(N), s_comment_strs(N);

    for (size_t i = 0; i < N; i++) {
        FieldIter f(all_lines[i].first, all_lines[i].second);
        s_suppkey[i] = f.as_int(); f.next();
        s_name_strs[i] = {f.ptr(), f.len()}; f.next();
        s_address_strs[i] = {f.ptr(), f.len()}; f.next();
        s_nationkey[i] = f.as_int(); f.next();
        s_phone_strs[i] = {f.ptr(), f.len()}; f.next();
        s_acctbal[i] = f.as_double(); f.next();
        s_comment_strs[i] = {f.ptr(), f.len()};
    }

    write_bin(table_dir + "/s_suppkey.bin", s_suppkey.data(), N);
    write_varlen(table_dir + "/s_name", s_name_strs);
    write_varlen(table_dir + "/s_address", s_address_strs);
    write_bin(table_dir + "/s_nationkey.bin", s_nationkey.data(), N);
    write_varlen(table_dir + "/s_phone", s_phone_strs);
    write_bin(table_dir + "/s_acctbal.bin", s_acctbal.data(), N);
    write_varlen(table_dir + "/s_comment", s_comment_strs);
    write_meta(table_dir, N);

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "  supplier: " << N << " rows in " << std::chrono::duration<double>(t1-t0).count() << "s\n";
}

// ============================================================
// PARTSUPP
// ============================================================
void ingest_partsupp(const std::string& data_dir, const std::string& storage_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/partsupp";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/partsupp.tbl")) return;
    auto all_lines = get_all_lines(mf);
    size_t N = all_lines.size();

    std::vector<int32_t> ps_partkey(N), ps_suppkey(N), ps_availqty(N);
    std::vector<double> ps_supplycost(N);
    std::vector<std::pair<const char*, size_t>> ps_comment_strs(N);

    #pragma omp parallel for schedule(dynamic, 10000)
    for (size_t i = 0; i < N; i++) {
        FieldIter f(all_lines[i].first, all_lines[i].second);
        ps_partkey[i] = f.as_int(); f.next();
        ps_suppkey[i] = f.as_int(); f.next();
        ps_availqty[i] = f.as_int(); f.next();
        ps_supplycost[i] = f.as_double(); f.next();
        ps_comment_strs[i] = {f.ptr(), f.len()};
    }

    write_bin(table_dir + "/ps_partkey.bin", ps_partkey.data(), N);
    write_bin(table_dir + "/ps_suppkey.bin", ps_suppkey.data(), N);
    write_bin(table_dir + "/ps_availqty.bin", ps_availqty.data(), N);
    write_bin(table_dir + "/ps_supplycost.bin", ps_supplycost.data(), N);
    write_varlen(table_dir + "/ps_comment", ps_comment_strs);
    write_meta(table_dir, N);

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "  partsupp: " << N << " rows in " << std::chrono::duration<double>(t1-t0).count() << "s\n";
}

// ============================================================
// NATION
// ============================================================
void ingest_nation(const std::string& data_dir, const std::string& storage_dir) {
    std::string table_dir = storage_dir + "/nation";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/nation.tbl")) return;
    auto all_lines = get_all_lines(mf);
    size_t N = all_lines.size();

    std::vector<int32_t> n_nationkey(N), n_regionkey(N);
    std::vector<std::pair<const char*, size_t>> n_name_strs(N), n_comment_strs(N);

    for (size_t i = 0; i < N; i++) {
        FieldIter f(all_lines[i].first, all_lines[i].second);
        n_nationkey[i] = f.as_int(); f.next();
        n_name_strs[i] = {f.ptr(), f.len()}; f.next();
        n_regionkey[i] = f.as_int(); f.next();
        n_comment_strs[i] = {f.ptr(), f.len()};
    }

    write_bin(table_dir + "/n_nationkey.bin", n_nationkey.data(), N);
    write_varlen(table_dir + "/n_name", n_name_strs);
    write_bin(table_dir + "/n_regionkey.bin", n_regionkey.data(), N);
    write_varlen(table_dir + "/n_comment", n_comment_strs);
    write_meta(table_dir, N);
    std::cout << "  nation: " << N << " rows\n";
}

// ============================================================
// REGION
// ============================================================
void ingest_region(const std::string& data_dir, const std::string& storage_dir) {
    std::string table_dir = storage_dir + "/region";
    fs::create_directories(table_dir);

    MappedFile mf;
    if (!mf.open(data_dir + "/region.tbl")) return;
    auto all_lines = get_all_lines(mf);
    size_t N = all_lines.size();

    std::vector<int32_t> r_regionkey(N);
    std::vector<std::pair<const char*, size_t>> r_name_strs(N), r_comment_strs(N);

    for (size_t i = 0; i < N; i++) {
        FieldIter f(all_lines[i].first, all_lines[i].second);
        r_regionkey[i] = f.as_int(); f.next();
        r_name_strs[i] = {f.ptr(), f.len()}; f.next();
        r_comment_strs[i] = {f.ptr(), f.len()};
    }

    write_bin(table_dir + "/r_regionkey.bin", r_regionkey.data(), N);
    write_varlen(table_dir + "/r_name", r_name_strs);
    write_varlen(table_dir + "/r_comment", r_comment_strs);
    write_meta(table_dir, N);
    std::cout << "  region: " << N << " rows\n";
}

// ============================================================
// MAIN
// ============================================================
int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <storage_dir>\n";
        return 1;
    }
    std::string data_dir = argv[1];
    std::string storage_dir = argv[2];
    fs::create_directories(storage_dir);

    auto t0 = std::chrono::high_resolution_clock::now();
    std::cout << "Ingesting TPC-H SF10 data...\n";
    std::cout << "  Source: " << data_dir << "\n";
    std::cout << "  Target: " << storage_dir << "\n";
    std::cout << "  Threads: " << omp_get_max_threads() << "\n\n";

    // Ingest small tables first (concurrently)
    #pragma omp parallel sections
    {
        #pragma omp section
        ingest_nation(data_dir, storage_dir);
        #pragma omp section
        ingest_region(data_dir, storage_dir);
        #pragma omp section
        ingest_supplier(data_dir, storage_dir);
    }

    // Then medium tables concurrently
    #pragma omp parallel sections
    {
        #pragma omp section
        ingest_customer(data_dir, storage_dir);
        #pragma omp section
        ingest_part(data_dir, storage_dir);
        #pragma omp section
        ingest_partsupp(data_dir, storage_dir);
    }

    // Then large tables (they use internal parallelism)
    ingest_orders(data_dir, storage_dir);
    ingest_lineitem(data_dir, storage_dir);

    auto t1 = std::chrono::high_resolution_clock::now();
    double total = std::chrono::duration<double>(t1 - t0).count();
    std::cout << "\nTotal ingestion time: " << total << "s\n";
    return 0;
}
