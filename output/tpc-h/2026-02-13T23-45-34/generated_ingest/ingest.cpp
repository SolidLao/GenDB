#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <memory>
#include <filesystem>
#include <chrono>
#include <cstring>
#include <charconv>
#include <algorithm>
#include <map>
#include <unordered_map>
#include <numeric>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace fs = std::filesystem;

// ============= Date Encoding =============
// Convert YYYY-MM-DD to days since epoch (1970-01-01)
int32_t date_to_days(const std::string& date_str) {
    if (date_str.length() != 10) return 0;

    int year, month, day;
    sscanf(date_str.c_str(), "%d-%d-%d", &year, &month, &day);

    // Days since 1970-01-01
    const int days_per_month[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    int leap_years = (year - 1970) / 4 - (year - 1970) / 100 + (year - 1970) / 400;
    int days = (year - 1970) * 365 + leap_years + days_per_month[month - 1] + day;

    // Adjust for leap year
    if (month > 2 && ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0)) {
        days += 1;
    }

    return days;
}

// ============= Dictionary Encoding =============
template<typename CodeType>
class DictionaryEncoder {
public:
    CodeType encode(const std::string& value) {
        auto it = dict.find(value);
        if (it != dict.end()) {
            return it->second;
        }
        CodeType code = next_code++;
        dict[value] = code;
        reverse_dict[code] = value;
        return code;
    }

    const std::map<std::string, CodeType>& get_dict() const { return dict; }
    const std::map<CodeType, std::string>& get_reverse_dict() const { return reverse_dict; }

private:
    std::map<std::string, CodeType> dict;
    std::map<CodeType, std::string> reverse_dict;
    CodeType next_code = 0;
};

// ============= CSV Parser Utilities =============
// Parse a double value from string
double parse_double(const char* start, const char* end) {
    double value = 0.0;
    std::from_chars(start, end, value);
    return value;
}

// Parse an int32_t from string
int32_t parse_int32(const char* start, const char* end) {
    int32_t value = 0;
    std::from_chars(start, end, value);
    return value;
}

// Find next pipe delimiter
const char* find_next_delim(const char* ptr, const char* end) {
    while (ptr < end && *ptr != '|') ptr++;
    return ptr;
}

// ============= Per-Table Ingestion =============

void ingest_lineitem(const std::string& data_file, const std::string& output_dir) {
    std::cout << "[lineitem] Ingesting..." << std::endl;

    // Open input file with mmap
    int fd = open(data_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << data_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    void* mmapped = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    madvise(mmapped, sb.st_size, MADV_SEQUENTIAL);

    const char* data = static_cast<const char*>(mmapped);
    const char* end = data + sb.st_size;

    // Prepare output vectors
    std::vector<int32_t> l_orderkey, l_partkey, l_suppkey, l_linenumber;
    std::vector<double> l_quantity, l_extendedprice, l_discount, l_tax;
    std::vector<uint8_t> l_returnflag, l_linestatus;
    std::vector<int32_t> l_shipdate, l_commitdate, l_receiptdate;
    std::vector<std::string> l_shipinstruct, l_shipmode, l_comment;

    // Dictionary encoders for low-cardinality strings
    DictionaryEncoder<uint8_t> returnflag_dict, linestatus_dict;

    const char* line = data;
    size_t row_count = 0;

    // Parse CSV
    while (line < end) {
        const char* line_end = find_next_delim(line, end);
        while (line_end < end && *line_end != '\n') line_end++;

        // Parse fields: l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|l_discount|l_tax|l_returnflag|l_linestatus|l_shipdate|l_commitdate|l_receiptdate|l_shipinstruct|l_shipmode|l_comment
        const char* ptr = line;
        const char* f_end;

        #define PARSE_INT32(vec) \
            f_end = find_next_delim(ptr, line_end); \
            vec.push_back(parse_int32(ptr, f_end)); \
            ptr = f_end + 1;

        #define PARSE_DOUBLE(vec) \
            f_end = find_next_delim(ptr, line_end); \
            vec.push_back(parse_double(ptr, f_end)); \
            ptr = f_end + 1;

        #define PARSE_DATE(vec) \
            f_end = find_next_delim(ptr, line_end); \
            vec.push_back(date_to_days(std::string(ptr, f_end))); \
            ptr = f_end + 1;

        #define PARSE_STRING(vec) \
            f_end = find_next_delim(ptr, line_end); \
            vec.push_back(std::string(ptr, f_end)); \
            ptr = f_end + 1;

        #define PARSE_DICT_UINT8(vec, dict) \
            f_end = find_next_delim(ptr, line_end); \
            vec.push_back(dict.encode(std::string(ptr, f_end))); \
            ptr = f_end + 1;

        PARSE_INT32(l_orderkey);
        PARSE_INT32(l_partkey);
        PARSE_INT32(l_suppkey);
        PARSE_INT32(l_linenumber);
        PARSE_DOUBLE(l_quantity);
        PARSE_DOUBLE(l_extendedprice);
        PARSE_DOUBLE(l_discount);
        PARSE_DOUBLE(l_tax);
        PARSE_DICT_UINT8(l_returnflag, returnflag_dict);
        PARSE_DICT_UINT8(l_linestatus, linestatus_dict);
        PARSE_DATE(l_shipdate);
        PARSE_DATE(l_commitdate);
        PARSE_DATE(l_receiptdate);
        PARSE_STRING(l_shipinstruct);
        PARSE_STRING(l_shipmode);
        PARSE_STRING(l_comment);

        row_count++;
        line = line_end + 1;
    }

    // Sort by l_shipdate (required for zone maps and efficient filtering)
    std::vector<size_t> perm(row_count);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
        return l_shipdate[a] < l_shipdate[b];
    });

    // Apply permutation and write to binary files
    fs::create_directories(output_dir);

    // Write lineitem data columns
    auto write_permuted_column = [&](const std::string& filename, const auto& vec) {
        std::ofstream f(filename, std::ios::binary);
        for (size_t i : perm) {
            f.write(reinterpret_cast<const char*>(&vec[i]), sizeof(vec[i]));
        }
        f.close();
    };

    auto write_permuted_string_column = [&](const std::string& filename, const std::vector<std::string>& vec) {
        std::ofstream f(filename, std::ios::binary);
        // Write strings: length + data
        for (size_t i : perm) {
            uint32_t len = vec[i].length();
            f.write(reinterpret_cast<char*>(&len), sizeof(len));
            f.write(vec[i].data(), len);
        }
        f.close();
    };

    write_permuted_column(output_dir + "/l_orderkey.bin", l_orderkey);
    write_permuted_column(output_dir + "/l_partkey.bin", l_partkey);
    write_permuted_column(output_dir + "/l_suppkey.bin", l_suppkey);
    write_permuted_column(output_dir + "/l_linenumber.bin", l_linenumber);
    write_permuted_column(output_dir + "/l_quantity.bin", l_quantity);
    write_permuted_column(output_dir + "/l_extendedprice.bin", l_extendedprice);
    write_permuted_column(output_dir + "/l_discount.bin", l_discount);
    write_permuted_column(output_dir + "/l_tax.bin", l_tax);
    write_permuted_column(output_dir + "/l_returnflag.bin", l_returnflag);
    write_permuted_column(output_dir + "/l_linestatus.bin", l_linestatus);
    write_permuted_column(output_dir + "/l_shipdate.bin", l_shipdate);
    write_permuted_column(output_dir + "/l_commitdate.bin", l_commitdate);
    write_permuted_column(output_dir + "/l_receiptdate.bin", l_receiptdate);
    write_permuted_string_column(output_dir + "/l_shipinstruct.bin", l_shipinstruct);
    write_permuted_string_column(output_dir + "/l_shipmode.bin", l_shipmode);
    write_permuted_string_column(output_dir + "/l_comment.bin", l_comment);

    // Write dictionary files
    std::ofstream dict_rf(output_dir + "/l_returnflag_dict.txt");
    for (const auto& [val, code] : returnflag_dict.get_dict()) {
        dict_rf << (int)code << "=" << val << "\n";
    }
    dict_rf.close();

    std::ofstream dict_ls(output_dir + "/l_linestatus_dict.txt");
    for (const auto& [val, code] : linestatus_dict.get_dict()) {
        dict_ls << (int)code << "=" << val << "\n";
    }
    dict_ls.close();

    std::cout << "[lineitem] Ingested " << row_count << " rows" << std::endl;

    munmap(mmapped, sb.st_size);
    close(fd);
}

void ingest_orders(const std::string& data_file, const std::string& output_dir) {
    std::cout << "[orders] Ingesting..." << std::endl;

    int fd = open(data_file.c_str(), O_RDONLY);
    if (fd < 0) { std::cerr << "Cannot open " << data_file << std::endl; return; }

    struct stat sb;
    fstat(fd, &sb);
    void* mmapped = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    madvise(mmapped, sb.st_size, MADV_SEQUENTIAL);

    const char* data = static_cast<const char*>(mmapped);
    const char* end = data + sb.st_size;

    std::vector<int32_t> o_orderkey, o_custkey, o_shippriority;
    std::vector<uint8_t> o_orderstatus;
    std::vector<double> o_totalprice;
    std::vector<int32_t> o_orderdate;
    std::vector<std::string> o_orderpriority, o_clerk, o_comment;

    DictionaryEncoder<uint8_t> orderstatus_dict;

    const char* line = data;
    size_t row_count = 0;

    while (line < end) {
        const char* line_end = find_next_delim(line, end);
        while (line_end < end && *line_end != '\n') line_end++;

        const char* ptr = line;
        const char* f_end;

        PARSE_INT32(o_orderkey);
        PARSE_INT32(o_custkey);
        PARSE_DICT_UINT8(o_orderstatus, orderstatus_dict);
        PARSE_DOUBLE(o_totalprice);
        PARSE_DATE(o_orderdate);
        PARSE_STRING(o_orderpriority);
        PARSE_STRING(o_clerk);
        PARSE_INT32(o_shippriority);
        PARSE_STRING(o_comment);

        row_count++;
        line = line_end + 1;
    }

    // Sort by o_orderdate
    std::vector<size_t> perm(row_count);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
        return o_orderdate[a] < o_orderdate[b];
    });

    fs::create_directories(output_dir);

    auto write_permuted_column = [&](const std::string& filename, const auto& vec) {
        std::ofstream f(filename, std::ios::binary);
        for (size_t i : perm) {
            f.write(reinterpret_cast<const char*>(&vec[i]), sizeof(vec[i]));
        }
        f.close();
    };

    auto write_permuted_string_column = [&](const std::string& filename, const std::vector<std::string>& vec) {
        std::ofstream f(filename, std::ios::binary);
        for (size_t i : perm) {
            uint32_t len = vec[i].length();
            f.write(reinterpret_cast<char*>(&len), sizeof(len));
            f.write(vec[i].data(), len);
        }
        f.close();
    };

    write_permuted_column(output_dir + "/o_orderkey.bin", o_orderkey);
    write_permuted_column(output_dir + "/o_custkey.bin", o_custkey);
    write_permuted_column(output_dir + "/o_orderstatus.bin", o_orderstatus);
    write_permuted_column(output_dir + "/o_totalprice.bin", o_totalprice);
    write_permuted_column(output_dir + "/o_orderdate.bin", o_orderdate);
    write_permuted_string_column(output_dir + "/o_orderpriority.bin", o_orderpriority);
    write_permuted_string_column(output_dir + "/o_clerk.bin", o_clerk);
    write_permuted_column(output_dir + "/o_shippriority.bin", o_shippriority);
    write_permuted_string_column(output_dir + "/o_comment.bin", o_comment);

    std::ofstream dict_os(output_dir + "/o_orderstatus_dict.txt");
    for (const auto& [val, code] : orderstatus_dict.get_dict()) {
        dict_os << (int)code << "=" << val << "\n";
    }
    dict_os.close();

    std::cout << "[orders] Ingested " << row_count << " rows" << std::endl;

    munmap(mmapped, sb.st_size);
    close(fd);
}

void ingest_customer(const std::string& data_file, const std::string& output_dir) {
    std::cout << "[customer] Ingesting..." << std::endl;

    int fd = open(data_file.c_str(), O_RDONLY);
    if (fd < 0) { std::cerr << "Cannot open " << data_file << std::endl; return; }

    struct stat sb;
    fstat(fd, &sb);
    void* mmapped = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    madvise(mmapped, sb.st_size, MADV_SEQUENTIAL);

    const char* data = static_cast<const char*>(mmapped);
    const char* end = data + sb.st_size;

    std::vector<int32_t> c_custkey, c_nationkey;
    std::vector<uint8_t> c_mktsegment;
    std::vector<double> c_acctbal;
    std::vector<std::string> c_name, c_address, c_phone, c_comment;

    DictionaryEncoder<uint8_t> mktsegment_dict;

    const char* line = data;
    size_t row_count = 0;

    while (line < end) {
        const char* line_end = find_next_delim(line, end);
        while (line_end < end && *line_end != '\n') line_end++;

        const char* ptr = line;
        const char* f_end;

        PARSE_INT32(c_custkey);
        PARSE_STRING(c_name);
        PARSE_STRING(c_address);
        PARSE_INT32(c_nationkey);
        PARSE_STRING(c_phone);
        PARSE_DOUBLE(c_acctbal);
        PARSE_DICT_UINT8(c_mktsegment, mktsegment_dict);
        PARSE_STRING(c_comment);

        row_count++;
        line = line_end + 1;
    }

    fs::create_directories(output_dir);

    auto write_column = [&](const std::string& filename, const auto& vec) {
        std::ofstream f(filename, std::ios::binary);
        f.write(reinterpret_cast<const char*>(vec.data()), vec.size() * sizeof(vec[0]));
        f.close();
    };

    auto write_string_column = [&](const std::string& filename, const std::vector<std::string>& vec) {
        std::ofstream f(filename, std::ios::binary);
        for (const auto& s : vec) {
            uint32_t len = s.length();
            f.write(reinterpret_cast<char*>(&len), sizeof(len));
            f.write(s.data(), len);
        }
        f.close();
    };

    write_column(output_dir + "/c_custkey.bin", c_custkey);
    write_string_column(output_dir + "/c_name.bin", c_name);
    write_string_column(output_dir + "/c_address.bin", c_address);
    write_column(output_dir + "/c_nationkey.bin", c_nationkey);
    write_string_column(output_dir + "/c_phone.bin", c_phone);
    write_column(output_dir + "/c_acctbal.bin", c_acctbal);
    write_column(output_dir + "/c_mktsegment.bin", c_mktsegment);
    write_string_column(output_dir + "/c_comment.bin", c_comment);

    std::ofstream dict_mk(output_dir + "/c_mktsegment_dict.txt");
    for (const auto& [val, code] : mktsegment_dict.get_dict()) {
        dict_mk << (int)code << "=" << val << "\n";
    }
    dict_mk.close();

    std::cout << "[customer] Ingested " << row_count << " rows" << std::endl;

    munmap(mmapped, sb.st_size);
    close(fd);
}

void ingest_table_generic(const std::string& name, const std::string& data_file, const std::string& output_dir) {
    std::cout << "[" << name << "] Ingesting..." << std::endl;

    int fd = open(data_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << data_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    void* mmapped = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    madvise(mmapped, sb.st_size, MADV_SEQUENTIAL);

    const char* data = static_cast<const char*>(mmapped);
    const char* end = data + sb.st_size;

    // Generic CSV parsing: collect all fields as strings, then write columns
    std::vector<std::vector<std::string>> rows;

    const char* line = data;
    size_t row_count = 0;

    while (line < end) {
        const char* line_end = line;
        while (line_end < end && *line_end != '\n') line_end++;

        std::vector<std::string> fields;
        const char* ptr = line;

        while (ptr < line_end) {
            const char* f_end = find_next_delim(ptr, line_end);
            fields.push_back(std::string(ptr, f_end));
            ptr = f_end + 1;
        }

        if (!fields.empty()) {
            rows.push_back(fields);
            row_count++;
        }
        line = line_end + 1;
    }

    fs::create_directories(output_dir);

    // Write column files
    if (row_count > 0) {
        size_t num_cols = rows[0].size();
        for (size_t col_idx = 0; col_idx < num_cols; col_idx++) {
            std::ofstream f(output_dir + "/" + name + "_col" + std::to_string(col_idx) + ".bin", std::ios::binary);
            for (size_t row_idx = 0; row_idx < row_count; row_idx++) {
                const std::string& val = rows[row_idx][col_idx];
                uint32_t len = val.length();
                f.write(reinterpret_cast<char*>(&len), sizeof(len));
                f.write(val.data(), len);
            }
            f.close();
        }
    }

    std::cout << "[" << name << "] Ingested " << row_count << " rows" << std::endl;

    munmap(mmapped, sb.st_size);
    close(fd);
}

// ============= Main =============
int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    auto start_time = std::chrono::high_resolution_clock::now();

    // Create gendb directory
    fs::create_directories(gendb_dir);

    // Ingest tables in parallel
    std::vector<std::thread> threads;

    threads.emplace_back([&]() { ingest_lineitem(data_dir + "/lineitem.tbl", gendb_dir + "/lineitem"); });
    threads.emplace_back([&]() { ingest_orders(data_dir + "/orders.tbl", gendb_dir + "/orders"); });
    threads.emplace_back([&]() { ingest_customer(data_dir + "/customer.tbl", gendb_dir + "/customer"); });
    threads.emplace_back([&]() { ingest_table_generic("part", data_dir + "/part.tbl", gendb_dir + "/part"); });
    threads.emplace_back([&]() { ingest_table_generic("partsupp", data_dir + "/partsupp.tbl", gendb_dir + "/partsupp"); });
    threads.emplace_back([&]() { ingest_table_generic("supplier", data_dir + "/supplier.tbl", gendb_dir + "/supplier"); });
    threads.emplace_back([&]() { ingest_table_generic("nation", data_dir + "/nation.tbl", gendb_dir + "/nation"); });
    threads.emplace_back([&]() { ingest_table_generic("region", data_dir + "/region.tbl", gendb_dir + "/region"); });

    for (auto& t : threads) t.join();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);

    std::cout << "\n=== Ingestion Complete ===" << std::endl;
    std::cout << "Total time: " << elapsed.count() << " seconds" << std::endl;

    return 0;
}
