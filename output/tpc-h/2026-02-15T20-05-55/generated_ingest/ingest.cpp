#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <filesystem>
#include <cstring>
#include <thread>
#include <mutex>
#include <charconv>
#include <cmath>
#include <unordered_map>
#include <chrono>
#include <algorithm>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

namespace fs = std::filesystem;

// ============================================================================
// Configuration
// ============================================================================

constexpr size_t BLOCK_SIZE = 256000;
constexpr size_t NUM_THREADS = 32;  // Use half the cores for I/O parallelism
constexpr size_t BUFFER_SIZE = 1024 * 1024;  // 1MB write buffer

// ============================================================================
// Utility Functions
// ============================================================================

// Parse date string "YYYY-MM-DD" to days since epoch (1970-01-01)
int32_t parse_date(const std::string& date_str) {
    if (date_str.length() < 10) return 0;

    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Days since 1970-01-01
    // Use Zeller-based formula or lookup table
    // Simplified: leap year counting
    int days = 0;

    // Days for complete years from 1970 to year-1
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Days for complete months in current year
    int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool is_leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
    if (is_leap) month_days[2] = 29;

    for (int m = 1; m < month; ++m) {
        days += month_days[m];
    }

    // Days in current month
    days += day;

    return days;
}

// Parse decimal string (e.g., "0.05") to scaled int64_t
int64_t parse_decimal(const std::string& decimal_str, int64_t scale_factor) {
    double val = 0.0;
    auto result = std::from_chars(decimal_str.data(), decimal_str.data() + decimal_str.length(), val);
    if (result.ec != std::errc()) {
        return 0;
    }
    return static_cast<int64_t>(std::round(val * scale_factor));
}

// ============================================================================
// Column Writers (Each manages its own buffering and file)
// ============================================================================

struct ColumnWriter {
    std::string filename;
    std::ofstream file;
    std::vector<char> buffer;
    size_t buffer_pos = 0;

    ColumnWriter(const std::string& path) : filename(path) {
        file.open(filename, std::ios::binary);
        buffer.resize(BUFFER_SIZE);
    }

    template <typename T>
    void write(const T& value) {
        if (buffer_pos + sizeof(T) > buffer.size()) {
            flush();
        }
        std::memcpy(buffer.data() + buffer_pos, &value, sizeof(T));
        buffer_pos += sizeof(T);
    }

    void write_string(const std::string& str) {
        // Store as length-prefixed string
        uint32_t len = str.length();
        if (buffer_pos + sizeof(len) + len > buffer.size()) {
            flush();
        }
        std::memcpy(buffer.data() + buffer_pos, &len, sizeof(len));
        buffer_pos += sizeof(len);
        std::memcpy(buffer.data() + buffer_pos, str.data(), len);
        buffer_pos += len;
    }

    void flush() {
        if (buffer_pos > 0) {
            file.write(buffer.data(), buffer_pos);
            buffer_pos = 0;
        }
    }

    ~ColumnWriter() {
        flush();
        file.close();
    }
};

// ============================================================================
// Dictionary Builder (for low-cardinality columns)
// ============================================================================

struct DictionaryBuilder {
    std::unordered_map<std::string, uint8_t> string_to_code;
    std::vector<std::string> code_to_string;
    uint8_t next_code = 0;

    uint8_t encode(const std::string& str) {
        auto it = string_to_code.find(str);
        if (it != string_to_code.end()) {
            return it->second;
        }
        if (next_code >= 255) {
            // Dictionary overflow - should not happen for TPC-H
            return 254;
        }
        uint8_t code = next_code++;
        string_to_code[str] = code;
        code_to_string.push_back(str);
        return code;
    }

    void save(const std::string& dict_file) {
        std::ofstream out(dict_file);
        for (uint8_t i = 0; i < code_to_string.size(); ++i) {
            out << static_cast<int>(i) << "=" << code_to_string[i] << "\n";
        }
        out.close();
    }
};

// ============================================================================
// Ingest Function
// ============================================================================

void ingest_table(const std::string& table_name, const std::string& data_dir,
                  const std::string& output_dir) {
    std::string input_file = data_dir + "/" + table_name + ".tbl";
    std::string output_table_dir = output_dir + "/tables/" + table_name;

    fs::create_directories(output_table_dir);

    std::cout << "Ingesting " << table_name << " from " << input_file << std::endl;

    // Open input file with mmap for fast reading
    int fd = open(input_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << input_file << std::endl;
        return;
    }

    size_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) {
        std::cerr << "Cannot mmap " << input_file << std::endl;
        close(fd);
        return;
    }

    madvise(mapped, file_size, MADV_SEQUENTIAL);

    const char* data = static_cast<const char*>(mapped);
    const char* end = data + file_size;

    // Parse by line
    std::vector<std::vector<std::string>> all_columns;
    size_t row_count = 0;

    if (table_name == "lineitem") {
        all_columns.resize(16);
    } else if (table_name == "orders") {
        all_columns.resize(9);
    } else if (table_name == "customer") {
        all_columns.resize(8);
    } else if (table_name == "part") {
        all_columns.resize(9);
    } else if (table_name == "partsupp") {
        all_columns.resize(5);
    } else if (table_name == "supplier") {
        all_columns.resize(7);
    } else if (table_name == "nation") {
        all_columns.resize(4);
    } else if (table_name == "region") {
        all_columns.resize(3);
    }

    // Pre-allocate vectors
    for (auto& col : all_columns) {
        col.reserve(2000000);  // Rough estimate
    }

    // Parse lines
    const char* line_start = data;
    while (line_start < end) {
        const char* line_end = std::find(line_start, end, '\n');
        std::string line(line_start, line_end);

        // Parse fields
        std::vector<std::string> fields;
        size_t pos = 0;
        for (size_t i = 0; i < all_columns.size(); ++i) {
            size_t delim = line.find('|', pos);
            if (delim == std::string::npos && i == all_columns.size() - 1) {
                fields.push_back(line.substr(pos));
                break;
            }
            fields.push_back(line.substr(pos, delim - pos));
            pos = delim + 1;
        }

        // Add to columns
        for (size_t i = 0; i < fields.size() && i < all_columns.size(); ++i) {
            all_columns[i].push_back(fields[i]);
        }

        row_count++;
        line_start = line_end + 1;
    }

    std::cout << "  Parsed " << row_count << " rows" << std::endl;

    // Write columnar files
    if (table_name == "lineitem") {
        DictionaryBuilder dict_returnflag, dict_linestatus, dict_shipinstruct, dict_shipmode;

        ColumnWriter col_orderkey(output_table_dir + "/l_orderkey.bin");
        ColumnWriter col_partkey(output_table_dir + "/l_partkey.bin");
        ColumnWriter col_suppkey(output_table_dir + "/l_suppkey.bin");
        ColumnWriter col_linenumber(output_table_dir + "/l_linenumber.bin");
        ColumnWriter col_quantity(output_table_dir + "/l_quantity.bin");
        ColumnWriter col_extendedprice(output_table_dir + "/l_extendedprice.bin");
        ColumnWriter col_discount(output_table_dir + "/l_discount.bin");
        ColumnWriter col_tax(output_table_dir + "/l_tax.bin");
        ColumnWriter col_returnflag(output_table_dir + "/l_returnflag.bin");
        ColumnWriter col_linestatus(output_table_dir + "/l_linestatus.bin");
        ColumnWriter col_shipdate(output_table_dir + "/l_shipdate.bin");
        ColumnWriter col_commitdate(output_table_dir + "/l_commitdate.bin");
        ColumnWriter col_receiptdate(output_table_dir + "/l_receiptdate.bin");
        ColumnWriter col_shipinstruct(output_table_dir + "/l_shipinstruct.bin");
        ColumnWriter col_shipmode(output_table_dir + "/l_shipmode.bin");
        ColumnWriter col_comment(output_table_dir + "/l_comment.bin");

        for (size_t i = 0; i < row_count; ++i) {
            col_orderkey.write(std::stoi(all_columns[0][i]));
            col_partkey.write(std::stoi(all_columns[1][i]));
            col_suppkey.write(std::stoi(all_columns[2][i]));
            col_linenumber.write(std::stoi(all_columns[3][i]));
            col_quantity.write(parse_decimal(all_columns[4][i], 100));
            col_extendedprice.write(parse_decimal(all_columns[5][i], 100));
            col_discount.write(parse_decimal(all_columns[6][i], 100));
            col_tax.write(parse_decimal(all_columns[7][i], 100));
            col_returnflag.write(dict_returnflag.encode(all_columns[8][i]));
            col_linestatus.write(dict_linestatus.encode(all_columns[9][i]));
            col_shipdate.write(parse_date(all_columns[10][i]));
            col_commitdate.write(parse_date(all_columns[11][i]));
            col_receiptdate.write(parse_date(all_columns[12][i]));
            col_shipinstruct.write(dict_shipinstruct.encode(all_columns[13][i]));
            col_shipmode.write(dict_shipmode.encode(all_columns[14][i]));
            col_comment.write_string(all_columns[15][i]);
        }

        dict_returnflag.save(output_table_dir + "/l_returnflag_dict.txt");
        dict_linestatus.save(output_table_dir + "/l_linestatus_dict.txt");
        dict_shipinstruct.save(output_table_dir + "/l_shipinstruct_dict.txt");
        dict_shipmode.save(output_table_dir + "/l_shipmode_dict.txt");

    } else if (table_name == "orders") {
        DictionaryBuilder dict_orderstatus, dict_orderpriority;

        ColumnWriter col_orderkey(output_table_dir + "/o_orderkey.bin");
        ColumnWriter col_custkey(output_table_dir + "/o_custkey.bin");
        ColumnWriter col_orderstatus(output_table_dir + "/o_orderstatus.bin");
        ColumnWriter col_totalprice(output_table_dir + "/o_totalprice.bin");
        ColumnWriter col_orderdate(output_table_dir + "/o_orderdate.bin");
        ColumnWriter col_orderpriority(output_table_dir + "/o_orderpriority.bin");
        ColumnWriter col_clerk(output_table_dir + "/o_clerk.bin");
        ColumnWriter col_shippriority(output_table_dir + "/o_shippriority.bin");
        ColumnWriter col_comment(output_table_dir + "/o_comment.bin");

        for (size_t i = 0; i < row_count; ++i) {
            col_orderkey.write(std::stoi(all_columns[0][i]));
            col_custkey.write(std::stoi(all_columns[1][i]));
            col_orderstatus.write(dict_orderstatus.encode(all_columns[2][i]));
            col_totalprice.write(parse_decimal(all_columns[3][i], 100));
            col_orderdate.write(parse_date(all_columns[4][i]));
            col_orderpriority.write(dict_orderpriority.encode(all_columns[5][i]));
            col_clerk.write_string(all_columns[6][i]);
            col_shippriority.write(std::stoi(all_columns[7][i]));
            col_comment.write_string(all_columns[8][i]);
        }

        dict_orderstatus.save(output_table_dir + "/o_orderstatus_dict.txt");
        dict_orderpriority.save(output_table_dir + "/o_orderpriority_dict.txt");

    } else if (table_name == "customer") {
        DictionaryBuilder dict_mktsegment;

        ColumnWriter col_custkey(output_table_dir + "/c_custkey.bin");
        ColumnWriter col_name(output_table_dir + "/c_name.bin");
        ColumnWriter col_address(output_table_dir + "/c_address.bin");
        ColumnWriter col_nationkey(output_table_dir + "/c_nationkey.bin");
        ColumnWriter col_phone(output_table_dir + "/c_phone.bin");
        ColumnWriter col_acctbal(output_table_dir + "/c_acctbal.bin");
        ColumnWriter col_mktsegment(output_table_dir + "/c_mktsegment.bin");
        ColumnWriter col_comment(output_table_dir + "/c_comment.bin");

        for (size_t i = 0; i < row_count; ++i) {
            col_custkey.write(std::stoi(all_columns[0][i]));
            col_name.write_string(all_columns[1][i]);
            col_address.write_string(all_columns[2][i]);
            col_nationkey.write(std::stoi(all_columns[3][i]));
            col_phone.write_string(all_columns[4][i]);
            col_acctbal.write(parse_decimal(all_columns[5][i], 100));
            col_mktsegment.write(dict_mktsegment.encode(all_columns[6][i]));
            col_comment.write_string(all_columns[7][i]);
        }

        dict_mktsegment.save(output_table_dir + "/c_mktsegment_dict.txt");

    } else if (table_name == "part") {
        ColumnWriter col_partkey(output_table_dir + "/p_partkey.bin");
        ColumnWriter col_name(output_table_dir + "/p_name.bin");
        ColumnWriter col_mfgr(output_table_dir + "/p_mfgr.bin");
        ColumnWriter col_brand(output_table_dir + "/p_brand.bin");
        ColumnWriter col_type(output_table_dir + "/p_type.bin");
        ColumnWriter col_size(output_table_dir + "/p_size.bin");
        ColumnWriter col_container(output_table_dir + "/p_container.bin");
        ColumnWriter col_retailprice(output_table_dir + "/p_retailprice.bin");
        ColumnWriter col_comment(output_table_dir + "/p_comment.bin");

        for (size_t i = 0; i < row_count; ++i) {
            col_partkey.write(std::stoi(all_columns[0][i]));
            col_name.write_string(all_columns[1][i]);
            col_mfgr.write_string(all_columns[2][i]);
            col_brand.write_string(all_columns[3][i]);
            col_type.write_string(all_columns[4][i]);
            col_size.write(std::stoi(all_columns[5][i]));
            col_container.write_string(all_columns[6][i]);
            col_retailprice.write(parse_decimal(all_columns[7][i], 100));
            col_comment.write_string(all_columns[8][i]);
        }

    } else if (table_name == "partsupp") {
        ColumnWriter col_partkey(output_table_dir + "/ps_partkey.bin");
        ColumnWriter col_suppkey(output_table_dir + "/ps_suppkey.bin");
        ColumnWriter col_availqty(output_table_dir + "/ps_availqty.bin");
        ColumnWriter col_supplycost(output_table_dir + "/ps_supplycost.bin");
        ColumnWriter col_comment(output_table_dir + "/ps_comment.bin");

        for (size_t i = 0; i < row_count; ++i) {
            col_partkey.write(std::stoi(all_columns[0][i]));
            col_suppkey.write(std::stoi(all_columns[1][i]));
            col_availqty.write(std::stoi(all_columns[2][i]));
            col_supplycost.write(parse_decimal(all_columns[3][i], 100));
            col_comment.write_string(all_columns[4][i]);
        }

    } else if (table_name == "supplier") {
        ColumnWriter col_suppkey(output_table_dir + "/s_suppkey.bin");
        ColumnWriter col_name(output_table_dir + "/s_name.bin");
        ColumnWriter col_address(output_table_dir + "/s_address.bin");
        ColumnWriter col_nationkey(output_table_dir + "/s_nationkey.bin");
        ColumnWriter col_phone(output_table_dir + "/s_phone.bin");
        ColumnWriter col_acctbal(output_table_dir + "/s_acctbal.bin");
        ColumnWriter col_comment(output_table_dir + "/s_comment.bin");

        for (size_t i = 0; i < row_count; ++i) {
            col_suppkey.write(std::stoi(all_columns[0][i]));
            col_name.write_string(all_columns[1][i]);
            col_address.write_string(all_columns[2][i]);
            col_nationkey.write(std::stoi(all_columns[3][i]));
            col_phone.write_string(all_columns[4][i]);
            col_acctbal.write(parse_decimal(all_columns[5][i], 100));
            col_comment.write_string(all_columns[6][i]);
        }

    } else if (table_name == "nation") {
        ColumnWriter col_nationkey(output_table_dir + "/n_nationkey.bin");
        ColumnWriter col_name(output_table_dir + "/n_name.bin");
        ColumnWriter col_regionkey(output_table_dir + "/n_regionkey.bin");
        ColumnWriter col_comment(output_table_dir + "/n_comment.bin");

        for (size_t i = 0; i < row_count; ++i) {
            col_nationkey.write(std::stoi(all_columns[0][i]));
            col_name.write_string(all_columns[1][i]);
            col_regionkey.write(std::stoi(all_columns[2][i]));
            col_comment.write_string(all_columns[3][i]);
        }

    } else if (table_name == "region") {
        ColumnWriter col_regionkey(output_table_dir + "/r_regionkey.bin");
        ColumnWriter col_name(output_table_dir + "/r_name.bin");
        ColumnWriter col_comment(output_table_dir + "/r_comment.bin");

        for (size_t i = 0; i < row_count; ++i) {
            col_regionkey.write(std::stoi(all_columns[0][i]));
            col_name.write_string(all_columns[1][i]);
            col_comment.write_string(all_columns[2][i]);
        }
    }

    munmap(mapped, file_size);
    close(fd);

    std::cout << "  Written " << row_count << " rows to binary columnar format" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <output_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string output_dir = argv[2];

    auto start = std::chrono::high_resolution_clock::now();

    fs::create_directories(output_dir + "/tables");

    // Ingest all tables
    std::vector<std::string> tables = {
        "nation", "region", "supplier", "part", "partsupp",
        "customer", "orders", "lineitem"
    };

    for (const auto& table : tables) {
        ingest_table(table, data_dir, output_dir);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);

    std::cout << "\nIngestion completed in " << duration.count() << " seconds" << std::endl;

    return 0;
}
