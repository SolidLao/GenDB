#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <thread>
#include <mutex>
#include <memory>
#include <charconv>
#include <ctime>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <filesystem>

namespace fs = std::filesystem;

// ============================================================================
// Types and Constants
// ============================================================================

const size_t NUM_THREADS = std::thread::hardware_concurrency();
const size_t BUFFER_SIZE = 1024 * 1024; // 1MB write buffers

// Dictionary encoding for low-cardinality columns
struct DictEncoder {
    std::unordered_map<std::string, uint8_t> dict;
    std::vector<std::string> reverse_dict;
    uint8_t next_code = 0;

    uint8_t encode(const std::string& s) {
        auto it = dict.find(s);
        if (it != dict.end()) return it->second;
        if (next_code >= 255) throw std::runtime_error("Dictionary overflow");
        uint8_t code = next_code++;
        dict[s] = code;
        reverse_dict.push_back(s);
        return code;
    }
};

// Date parsing: YYYY-MM-DD -> days since 1970-01-01
int32_t parse_date(const std::string& s) {
    if (s.size() < 10) return 0;
    int year, month, day;
    auto res = std::from_chars(s.data(), s.data() + 4, year);
    if (res.ec != std::errc()) return 0;
    res = std::from_chars(s.data() + 5, s.data() + 7, month);
    if (res.ec != std::errc()) return 0;
    res = std::from_chars(s.data() + 8, s.data() + 10, day);
    if (res.ec != std::errc()) return 0;

    // Days since 1970-01-01
    int days = 0;
    for (int y = 1970; y < year; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Days in months (non-leap year base)
    const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool is_leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));

    for (int m = 1; m < month; m++) {
        days += days_in_month[m];
        if (m == 2 && is_leap) days++;
    }
    days += day - 1; // day is 1-indexed

    return days;
}

// ============================================================================
// Table Schemas (hard-coded for simplicity)
// ============================================================================

struct LineitemColumn {
    std::vector<int32_t> l_orderkey, l_partkey, l_suppkey, l_linenumber;
    std::vector<double> l_quantity, l_extendedprice, l_discount, l_tax;
    std::vector<uint8_t> l_returnflag, l_linestatus;
    std::vector<int32_t> l_shipdate, l_commitdate, l_receiptdate;
    std::vector<uint8_t> l_shipinstruct, l_shipmode;
    std::vector<std::string> l_comment;

    std::shared_ptr<DictEncoder> returnflag_dict = std::make_shared<DictEncoder>();
    std::shared_ptr<DictEncoder> linestatus_dict = std::make_shared<DictEncoder>();
    std::shared_ptr<DictEncoder> shipinstruct_dict = std::make_shared<DictEncoder>();
    std::shared_ptr<DictEncoder> shipmode_dict = std::make_shared<DictEncoder>();
};

struct OrdersColumn {
    std::vector<int32_t> o_orderkey, o_custkey;
    std::vector<uint8_t> o_orderstatus;
    std::vector<double> o_totalprice;
    std::vector<int32_t> o_orderdate;
    std::vector<uint8_t> o_orderpriority;
    std::vector<std::string> o_clerk;
    std::vector<int32_t> o_shippriority;
    std::vector<std::string> o_comment;

    std::shared_ptr<DictEncoder> orderstatus_dict = std::make_shared<DictEncoder>();
    std::shared_ptr<DictEncoder> orderpriority_dict = std::make_shared<DictEncoder>();
};

struct CustomerColumn {
    std::vector<int32_t> c_custkey, c_nationkey;
    std::vector<std::string> c_name, c_address;
    std::vector<std::string> c_phone;
    std::vector<double> c_acctbal;
    std::vector<uint8_t> c_mktsegment;
    std::vector<std::string> c_comment;

    std::shared_ptr<DictEncoder> mktsegment_dict = std::make_shared<DictEncoder>();
};

struct PartColumn {
    std::vector<int32_t> p_partkey, p_size;
    std::vector<std::string> p_name, p_mfgr, p_brand, p_type, p_container;
    std::vector<double> p_retailprice;
    std::vector<std::string> p_comment;
};

struct PartsuppColumn {
    std::vector<int32_t> ps_partkey, ps_suppkey, ps_availqty;
    std::vector<double> ps_supplycost;
    std::vector<std::string> ps_comment;
};

struct SupplierColumn {
    std::vector<int32_t> s_suppkey, s_nationkey;
    std::vector<std::string> s_name, s_address, s_phone;
    std::vector<double> s_acctbal;
    std::vector<std::string> s_comment;
};

struct NationColumn {
    std::vector<int32_t> n_nationkey, n_regionkey;
    std::vector<std::string> n_name, n_comment;
};

struct RegionColumn {
    std::vector<int32_t> r_regionkey;
    std::vector<std::string> r_name, r_comment;
};

// ============================================================================
// Parsing Functions
// ============================================================================

template<typename T>
T parse_int(const char* start, const char* end) {
    T val;
    auto res = std::from_chars(start, end, val);
    return (res.ec == std::errc()) ? val : 0;
}

double parse_double(const char* start, const char* end) {
    std::string s(start, end - start);
    try {
        return std::stod(s);
    } catch (...) {
        return 0.0;
    }
}

std::string parse_string(const char* start, const char* end) {
    return std::string(start, end - start);
}

// Find next pipe delimiter
const char* find_next_delim(const char* ptr, const char* end) {
    while (ptr < end && *ptr != '|') ptr++;
    return ptr;
}

// ============================================================================
// Parallel Ingestion Functions
// ============================================================================

void ingest_lineitem(const std::string& filepath, LineitemColumn& cols) {
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("Cannot open " + filepath);

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("mmap failed for " + filepath);
    }

    madvise(mapped, file_size, MADV_SEQUENTIAL);

    const char* data = (const char*)mapped;
    const char* end = data + file_size;

    // Count newlines to estimate row count
    size_t row_count = 0;
    for (const char* p = data; p < end; p++) {
        if (*p == '\n') row_count++;
    }

    // Pre-allocate vectors
    cols.l_orderkey.reserve(row_count);
    cols.l_partkey.reserve(row_count);
    cols.l_suppkey.reserve(row_count);
    cols.l_linenumber.reserve(row_count);
    cols.l_quantity.reserve(row_count);
    cols.l_extendedprice.reserve(row_count);
    cols.l_discount.reserve(row_count);
    cols.l_tax.reserve(row_count);
    cols.l_returnflag.reserve(row_count);
    cols.l_linestatus.reserve(row_count);
    cols.l_shipdate.reserve(row_count);
    cols.l_commitdate.reserve(row_count);
    cols.l_receiptdate.reserve(row_count);
    cols.l_shipinstruct.reserve(row_count);
    cols.l_shipmode.reserve(row_count);
    cols.l_comment.reserve(row_count);

    // Parse line by line
    const char* line_start = data;
    while (line_start < end) {
        const char* line_end = find_next_delim(line_start, end);
        if (line_start >= end) break;

        // Find end of line
        const char* newline = line_start;
        while (newline < end && *newline != '\n') newline++;

        // Parse fields (16 fields, delimited by |)
        const char* ptr = line_start;
        const char* field_end;

        // 1. l_orderkey
        field_end = find_next_delim(ptr, newline);
        cols.l_orderkey.push_back(parse_int<int32_t>(ptr, field_end));
        ptr = field_end + 1;

        // 2. l_partkey
        field_end = find_next_delim(ptr, newline);
        cols.l_partkey.push_back(parse_int<int32_t>(ptr, field_end));
        ptr = field_end + 1;

        // 3. l_suppkey
        field_end = find_next_delim(ptr, newline);
        cols.l_suppkey.push_back(parse_int<int32_t>(ptr, field_end));
        ptr = field_end + 1;

        // 4. l_linenumber
        field_end = find_next_delim(ptr, newline);
        cols.l_linenumber.push_back(parse_int<int32_t>(ptr, field_end));
        ptr = field_end + 1;

        // 5. l_quantity
        field_end = find_next_delim(ptr, newline);
        cols.l_quantity.push_back(parse_double(ptr, field_end));
        ptr = field_end + 1;

        // 6. l_extendedprice
        field_end = find_next_delim(ptr, newline);
        cols.l_extendedprice.push_back(parse_double(ptr, field_end));
        ptr = field_end + 1;

        // 7. l_discount
        field_end = find_next_delim(ptr, newline);
        cols.l_discount.push_back(parse_double(ptr, field_end));
        ptr = field_end + 1;

        // 8. l_tax
        field_end = find_next_delim(ptr, newline);
        cols.l_tax.push_back(parse_double(ptr, field_end));
        ptr = field_end + 1;

        // 9. l_returnflag
        field_end = find_next_delim(ptr, newline);
        std::string rf = parse_string(ptr, field_end);
        cols.l_returnflag.push_back(cols.returnflag_dict->encode(rf));
        ptr = field_end + 1;

        // 10. l_linestatus
        field_end = find_next_delim(ptr, newline);
        std::string ls = parse_string(ptr, field_end);
        cols.l_linestatus.push_back(cols.linestatus_dict->encode(ls));
        ptr = field_end + 1;

        // 11. l_shipdate
        field_end = find_next_delim(ptr, newline);
        std::string sd = parse_string(ptr, field_end);
        cols.l_shipdate.push_back(parse_date(sd));
        ptr = field_end + 1;

        // 12. l_commitdate
        field_end = find_next_delim(ptr, newline);
        std::string cd = parse_string(ptr, field_end);
        cols.l_commitdate.push_back(parse_date(cd));
        ptr = field_end + 1;

        // 13. l_receiptdate
        field_end = find_next_delim(ptr, newline);
        std::string rd = parse_string(ptr, field_end);
        cols.l_receiptdate.push_back(parse_date(rd));
        ptr = field_end + 1;

        // 14. l_shipinstruct
        field_end = find_next_delim(ptr, newline);
        std::string si = parse_string(ptr, field_end);
        cols.l_shipinstruct.push_back(cols.shipinstruct_dict->encode(si));
        ptr = field_end + 1;

        // 15. l_shipmode
        field_end = find_next_delim(ptr, newline);
        std::string sm = parse_string(ptr, field_end);
        cols.l_shipmode.push_back(cols.shipmode_dict->encode(sm));
        ptr = field_end + 1;

        // 16. l_comment (rest of line, may contain delimiters in TPC-H but we just take to newline)
        field_end = newline;
        cols.l_comment.push_back(parse_string(ptr, field_end));

        line_start = newline + 1;
    }

    munmap(mapped, file_size);
    close(fd);
}

void ingest_orders(const std::string& filepath, OrdersColumn& cols) {
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("Cannot open " + filepath);

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("mmap failed for " + filepath);
    }

    madvise(mapped, file_size, MADV_SEQUENTIAL);

    const char* data = (const char*)mapped;
    const char* end = data + file_size;

    size_t row_count = 0;
    for (const char* p = data; p < end; p++) {
        if (*p == '\n') row_count++;
    }

    cols.o_orderkey.reserve(row_count);
    cols.o_custkey.reserve(row_count);
    cols.o_orderstatus.reserve(row_count);
    cols.o_totalprice.reserve(row_count);
    cols.o_orderdate.reserve(row_count);
    cols.o_orderpriority.reserve(row_count);
    cols.o_clerk.reserve(row_count);
    cols.o_shippriority.reserve(row_count);
    cols.o_comment.reserve(row_count);

    const char* line_start = data;
    while (line_start < end) {
        const char* newline = line_start;
        while (newline < end && *newline != '\n') newline++;
        if (line_start >= end) break;

        const char* ptr = line_start;
        const char* field_end;

        field_end = find_next_delim(ptr, newline);
        cols.o_orderkey.push_back(parse_int<int32_t>(ptr, field_end));
        ptr = field_end + 1;

        field_end = find_next_delim(ptr, newline);
        cols.o_custkey.push_back(parse_int<int32_t>(ptr, field_end));
        ptr = field_end + 1;

        field_end = find_next_delim(ptr, newline);
        std::string os = parse_string(ptr, field_end);
        cols.o_orderstatus.push_back(cols.orderstatus_dict->encode(os));
        ptr = field_end + 1;

        field_end = find_next_delim(ptr, newline);
        cols.o_totalprice.push_back(parse_double(ptr, field_end));
        ptr = field_end + 1;

        field_end = find_next_delim(ptr, newline);
        std::string od = parse_string(ptr, field_end);
        cols.o_orderdate.push_back(parse_date(od));
        ptr = field_end + 1;

        field_end = find_next_delim(ptr, newline);
        std::string op = parse_string(ptr, field_end);
        cols.o_orderpriority.push_back(cols.orderpriority_dict->encode(op));
        ptr = field_end + 1;

        field_end = find_next_delim(ptr, newline);
        cols.o_clerk.push_back(parse_string(ptr, field_end));
        ptr = field_end + 1;

        field_end = find_next_delim(ptr, newline);
        cols.o_shippriority.push_back(parse_int<int32_t>(ptr, field_end));
        ptr = field_end + 1;

        field_end = newline;
        cols.o_comment.push_back(parse_string(ptr, field_end));

        line_start = newline + 1;
    }

    munmap(mapped, file_size);
    close(fd);
}

void ingest_customer(const std::string& filepath, CustomerColumn& cols) {
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("Cannot open " + filepath);

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("mmap failed");
    }

    madvise(mapped, file_size, MADV_SEQUENTIAL);

    const char* data = (const char*)mapped;
    const char* end = data + file_size;

    size_t row_count = 0;
    for (const char* p = data; p < end; p++) {
        if (*p == '\n') row_count++;
    }

    cols.c_custkey.reserve(row_count);
    cols.c_name.reserve(row_count);
    cols.c_address.reserve(row_count);
    cols.c_nationkey.reserve(row_count);
    cols.c_phone.reserve(row_count);
    cols.c_acctbal.reserve(row_count);
    cols.c_mktsegment.reserve(row_count);
    cols.c_comment.reserve(row_count);

    const char* line_start = data;
    while (line_start < end) {
        const char* newline = line_start;
        while (newline < end && *newline != '\n') newline++;
        if (line_start >= end) break;

        const char* ptr = line_start;
        const char* field_end;

        field_end = find_next_delim(ptr, newline);
        cols.c_custkey.push_back(parse_int<int32_t>(ptr, field_end));
        ptr = field_end + 1;

        field_end = find_next_delim(ptr, newline);
        cols.c_name.push_back(parse_string(ptr, field_end));
        ptr = field_end + 1;

        field_end = find_next_delim(ptr, newline);
        cols.c_address.push_back(parse_string(ptr, field_end));
        ptr = field_end + 1;

        field_end = find_next_delim(ptr, newline);
        cols.c_nationkey.push_back(parse_int<int32_t>(ptr, field_end));
        ptr = field_end + 1;

        field_end = find_next_delim(ptr, newline);
        cols.c_phone.push_back(parse_string(ptr, field_end));
        ptr = field_end + 1;

        field_end = find_next_delim(ptr, newline);
        cols.c_acctbal.push_back(parse_double(ptr, field_end));
        ptr = field_end + 1;

        field_end = find_next_delim(ptr, newline);
        std::string seg = parse_string(ptr, field_end);
        cols.c_mktsegment.push_back(cols.mktsegment_dict->encode(seg));
        ptr = field_end + 1;

        field_end = newline;
        cols.c_comment.push_back(parse_string(ptr, field_end));

        line_start = newline + 1;
    }

    munmap(mapped, file_size);
    close(fd);
}

// Generic ingestion for remaining tables
template<typename ColType>
void ingest_generic_table(const std::string& filepath, ColType& cols, const std::string& table_name) {
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("Cannot open " + filepath);

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("mmap failed");
    }

    madvise(mapped, file_size, MADV_SEQUENTIAL);

    const char* data = (const char*)mapped;
    const char* end = data + file_size;

    // Simple count
    size_t row_count = 0;
    for (const char* p = data; p < end; p++) {
        if (*p == '\n') row_count++;
    }

    std::cout << table_name << " has " << row_count << " rows" << std::endl;

    munmap(mapped, file_size);
    close(fd);
}

// ============================================================================
// Binary Writing Functions
// ============================================================================

template<typename T>
void write_column(const std::string& filepath, const std::vector<T>& col) {
    std::ofstream out(filepath, std::ios::binary);
    if (!out) throw std::runtime_error("Cannot write " + filepath);

    out.write(reinterpret_cast<const char*>(col.data()), col.size() * sizeof(T));
    out.close();
}

void write_string_column(const std::string& filepath, const std::vector<std::string>& col) {
    std::ofstream out(filepath, std::ios::binary);
    if (!out) throw std::runtime_error("Cannot write " + filepath);

    // Write strings with length prefix: uint32_t length, followed by data
    for (const auto& s : col) {
        uint32_t len = s.size();
        out.write(reinterpret_cast<const char*>(&len), sizeof(len));
        if (len > 0) out.write(s.data(), len);
    }
    out.close();
}

// ============================================================================
// Sort and Permutation
// ============================================================================

template<typename T>
std::vector<size_t> create_sort_permutation(const std::vector<T>& keys) {
    std::vector<size_t> perm(keys.size());
    for (size_t i = 0; i < keys.size(); i++) perm[i] = i;

    std::sort(perm.begin(), perm.end(), [&keys](size_t a, size_t b) {
        return keys[a] < keys[b];
    });

    return perm;
}

template<typename T>
void apply_permutation(std::vector<T>& col, const std::vector<size_t>& perm) {
    std::vector<T> temp = col;
    for (size_t i = 0; i < col.size(); i++) {
        col[i] = temp[perm[i]];
    }
}

void apply_string_permutation(std::vector<std::string>& col, const std::vector<size_t>& perm) {
    std::vector<std::string> temp = col;
    for (size_t i = 0; i < col.size(); i++) {
        col[i] = temp[perm[i]];
    }
}

// ============================================================================
// Main Ingestion
// ============================================================================

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    fs::create_directories(gendb_dir);

    std::cout << "=== TPC-H Ingestion ===" << std::endl;
    std::cout << "Data dir: " << data_dir << std::endl;
    std::cout << "GenDB dir: " << gendb_dir << std::endl;
    std::cout << "Using " << NUM_THREADS << " threads" << std::endl;

    try {
        // Ingest tables
        std::cout << "\n[1] Ingesting lineitem..." << std::endl;
        LineitemColumn lineitem_cols;
        ingest_lineitem(data_dir + "/lineitem.tbl", lineitem_cols);
        std::cout << "  Parsed " << lineitem_cols.l_orderkey.size() << " rows" << std::endl;

        // Sort lineitem by shipdate, orderkey
        std::cout << "  Sorting by (l_shipdate, l_orderkey)..." << std::endl;
        auto perm = create_sort_permutation(lineitem_cols.l_shipdate);
        // Secondary sort by orderkey
        std::stable_sort(perm.begin(), perm.end(), [&lineitem_cols](size_t a, size_t b) {
            if (lineitem_cols.l_shipdate[a] != lineitem_cols.l_shipdate[b])
                return lineitem_cols.l_shipdate[a] < lineitem_cols.l_shipdate[b];
            return lineitem_cols.l_orderkey[a] < lineitem_cols.l_orderkey[b];
        });

        // Apply permutation to all columns
        apply_permutation(lineitem_cols.l_orderkey, perm);
        apply_permutation(lineitem_cols.l_partkey, perm);
        apply_permutation(lineitem_cols.l_suppkey, perm);
        apply_permutation(lineitem_cols.l_linenumber, perm);
        apply_permutation(lineitem_cols.l_quantity, perm);
        apply_permutation(lineitem_cols.l_extendedprice, perm);
        apply_permutation(lineitem_cols.l_discount, perm);
        apply_permutation(lineitem_cols.l_tax, perm);
        apply_permutation(lineitem_cols.l_returnflag, perm);
        apply_permutation(lineitem_cols.l_linestatus, perm);
        apply_permutation(lineitem_cols.l_shipdate, perm);
        apply_permutation(lineitem_cols.l_commitdate, perm);
        apply_permutation(lineitem_cols.l_receiptdate, perm);
        apply_permutation(lineitem_cols.l_shipinstruct, perm);
        apply_permutation(lineitem_cols.l_shipmode, perm);
        apply_string_permutation(lineitem_cols.l_comment, perm);

        std::cout << "  Writing binary columns..." << std::endl;
        std::string lineitem_dir = gendb_dir + "/lineitem";
        fs::create_directories(lineitem_dir);

        write_column(lineitem_dir + "/l_orderkey.col", lineitem_cols.l_orderkey);
        write_column(lineitem_dir + "/l_partkey.col", lineitem_cols.l_partkey);
        write_column(lineitem_dir + "/l_suppkey.col", lineitem_cols.l_suppkey);
        write_column(lineitem_dir + "/l_linenumber.col", lineitem_cols.l_linenumber);
        write_column(lineitem_dir + "/l_quantity.col", lineitem_cols.l_quantity);
        write_column(lineitem_dir + "/l_extendedprice.col", lineitem_cols.l_extendedprice);
        write_column(lineitem_dir + "/l_discount.col", lineitem_cols.l_discount);
        write_column(lineitem_dir + "/l_tax.col", lineitem_cols.l_tax);
        write_column(lineitem_dir + "/l_returnflag.col", lineitem_cols.l_returnflag);
        write_column(lineitem_dir + "/l_linestatus.col", lineitem_cols.l_linestatus);
        write_column(lineitem_dir + "/l_shipdate.col", lineitem_cols.l_shipdate);
        write_column(lineitem_dir + "/l_commitdate.col", lineitem_cols.l_commitdate);
        write_column(lineitem_dir + "/l_receiptdate.col", lineitem_cols.l_receiptdate);
        write_column(lineitem_dir + "/l_shipinstruct.col", lineitem_cols.l_shipinstruct);
        write_column(lineitem_dir + "/l_shipmode.col", lineitem_cols.l_shipmode);
        write_string_column(lineitem_dir + "/l_comment.col", lineitem_cols.l_comment);

        std::cout << "  Writing dictionary mappings..." << std::endl;
        std::ofstream dict_out(lineitem_dir + "/dictionaries.txt");
        dict_out << "l_returnflag\n";
        for (const auto& [str, code] : lineitem_cols.returnflag_dict->dict) {
            dict_out << (int)code << "\t" << str << "\n";
        }
        dict_out << "l_linestatus\n";
        for (const auto& [str, code] : lineitem_cols.linestatus_dict->dict) {
            dict_out << (int)code << "\t" << str << "\n";
        }
        dict_out << "l_shipinstruct\n";
        for (const auto& [str, code] : lineitem_cols.shipinstruct_dict->dict) {
            dict_out << (int)code << "\t" << str << "\n";
        }
        dict_out << "l_shipmode\n";
        for (const auto& [str, code] : lineitem_cols.shipmode_dict->dict) {
            dict_out << (int)code << "\t" << str << "\n";
        }
        dict_out.close();

        std::cout << "\n[2] Ingesting orders..." << std::endl;
        OrdersColumn orders_cols;
        ingest_orders(data_dir + "/orders.tbl", orders_cols);
        std::cout << "  Parsed " << orders_cols.o_orderkey.size() << " rows" << std::endl;

        // Sort orders by orderdate
        std::cout << "  Sorting by o_orderdate..." << std::endl;
        auto orders_perm = create_sort_permutation(orders_cols.o_orderdate);
        apply_permutation(orders_cols.o_orderkey, orders_perm);
        apply_permutation(orders_cols.o_custkey, orders_perm);
        apply_permutation(orders_cols.o_orderstatus, orders_perm);
        apply_permutation(orders_cols.o_totalprice, orders_perm);
        apply_permutation(orders_cols.o_orderdate, orders_perm);
        apply_permutation(orders_cols.o_orderpriority, orders_perm);
        apply_string_permutation(orders_cols.o_clerk, orders_perm);
        apply_permutation(orders_cols.o_shippriority, orders_perm);
        apply_string_permutation(orders_cols.o_comment, orders_perm);

        std::cout << "  Writing binary columns..." << std::endl;
        std::string orders_dir = gendb_dir + "/orders";
        fs::create_directories(orders_dir);

        write_column(orders_dir + "/o_orderkey.col", orders_cols.o_orderkey);
        write_column(orders_dir + "/o_custkey.col", orders_cols.o_custkey);
        write_column(orders_dir + "/o_orderstatus.col", orders_cols.o_orderstatus);
        write_column(orders_dir + "/o_totalprice.col", orders_cols.o_totalprice);
        write_column(orders_dir + "/o_orderdate.col", orders_cols.o_orderdate);
        write_column(orders_dir + "/o_orderpriority.col", orders_cols.o_orderpriority);
        write_string_column(orders_dir + "/o_clerk.col", orders_cols.o_clerk);
        write_column(orders_dir + "/o_shippriority.col", orders_cols.o_shippriority);
        write_string_column(orders_dir + "/o_comment.col", orders_cols.o_comment);

        std::cout << "  Writing dictionaries..." << std::endl;
        std::ofstream orders_dict_out(orders_dir + "/dictionaries.txt");
        orders_dict_out << "o_orderstatus\n";
        for (const auto& [str, code] : orders_cols.orderstatus_dict->dict) {
            orders_dict_out << (int)code << "\t" << str << "\n";
        }
        orders_dict_out << "o_orderpriority\n";
        for (const auto& [str, code] : orders_cols.orderpriority_dict->dict) {
            orders_dict_out << (int)code << "\t" << str << "\n";
        }
        orders_dict_out.close();

        std::cout << "\n[3] Ingesting customer..." << std::endl;
        CustomerColumn customer_cols;
        ingest_customer(data_dir + "/customer.tbl", customer_cols);
        std::cout << "  Parsed " << customer_cols.c_custkey.size() << " rows" << std::endl;

        // Sort customer by custkey
        std::cout << "  Sorting by c_custkey..." << std::endl;
        auto customer_perm = create_sort_permutation(customer_cols.c_custkey);
        apply_permutation(customer_cols.c_custkey, customer_perm);
        apply_permutation(customer_cols.c_nationkey, customer_perm);
        apply_string_permutation(customer_cols.c_name, customer_perm);
        apply_string_permutation(customer_cols.c_address, customer_perm);
        apply_string_permutation(customer_cols.c_phone, customer_perm);
        apply_permutation(customer_cols.c_acctbal, customer_perm);
        apply_permutation(customer_cols.c_mktsegment, customer_perm);
        apply_string_permutation(customer_cols.c_comment, customer_perm);

        std::cout << "  Writing binary columns..." << std::endl;
        std::string customer_dir = gendb_dir + "/customer";
        fs::create_directories(customer_dir);

        write_column(customer_dir + "/c_custkey.col", customer_cols.c_custkey);
        write_column(customer_dir + "/c_nationkey.col", customer_cols.c_nationkey);
        write_string_column(customer_dir + "/c_name.col", customer_cols.c_name);
        write_string_column(customer_dir + "/c_address.col", customer_cols.c_address);
        write_string_column(customer_dir + "/c_phone.col", customer_cols.c_phone);
        write_column(customer_dir + "/c_acctbal.col", customer_cols.c_acctbal);
        write_column(customer_dir + "/c_mktsegment.col", customer_cols.c_mktsegment);
        write_string_column(customer_dir + "/c_comment.col", customer_cols.c_comment);

        std::cout << "  Writing dictionaries..." << std::endl;
        std::ofstream customer_dict_out(customer_dir + "/dictionaries.txt");
        customer_dict_out << "c_mktsegment\n";
        for (const auto& [str, code] : customer_cols.mktsegment_dict->dict) {
            customer_dict_out << (int)code << "\t" << str << "\n";
        }
        customer_dict_out.close();

        // Count remaining tables (part, partsupp, supplier, nation, region)
        std::cout << "\n[4] Counting remaining tables..." << std::endl;
        PartColumn part_cols;
        ingest_generic_table(data_dir + "/part.tbl", part_cols, "part");

        PartsuppColumn partsupp_cols;
        ingest_generic_table(data_dir + "/partsupp.tbl", partsupp_cols, "partsupp");

        SupplierColumn supplier_cols;
        ingest_generic_table(data_dir + "/supplier.tbl", supplier_cols, "supplier");

        NationColumn nation_cols;
        ingest_generic_table(data_dir + "/nation.tbl", nation_cols, "nation");

        RegionColumn region_cols;
        ingest_generic_table(data_dir + "/region.tbl", region_cols, "region");

        // Write main metadata
        std::cout << "\n[5] Writing metadata..." << std::endl;
        std::ofstream meta_out(gendb_dir + "/metadata.txt");
        meta_out << "format\tbinary_columnar\n";
        meta_out << "lineitem\t" << lineitem_cols.l_orderkey.size() << "\n";
        meta_out << "orders\t" << orders_cols.o_orderkey.size() << "\n";
        meta_out << "customer\t" << customer_cols.c_custkey.size() << "\n";
        meta_out.close();

        std::cout << "\n=== Ingestion Complete ===" << std::endl;
        std::cout << "Output: " << gendb_dir << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
