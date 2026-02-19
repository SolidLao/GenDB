#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <map>
#include <unordered_set>
#include <thread>
#include <mutex>
#include <cstring>
#include <cstdint>
#include <cmath>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <queue>
#include <memory>

// Date parsing: convert YYYY-MM-DD to days since 1970-01-01
int32_t parse_date(const std::string& date_str) {
    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Days in each month for non-leap years
    const int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    // Sum complete years (1970 to year-1)
    int32_t total_days = 0;
    for (int y = 1970; y < year; ++y) {
        bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        total_days += leap ? 366 : 365;
    }

    // Sum complete months
    bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    for (int m = 1; m < month; ++m) {
        total_days += days_in_month[m - 1];
        if (m == 2 && leap) total_days += 1;
    }

    // Add days
    total_days += day - 1;

    return total_days;
}

// Parse decimal (DECIMAL(15,2) style)
int64_t parse_decimal(const std::string& dec_str, int scale) {
    double d = std::stod(dec_str);
    return static_cast<int64_t>(std::round(d * scale));
}

// Dictionary encoding: maps string -> code
class StringDictionary {
public:
    int16_t encode(const std::string& value) {
        if (map_.find(value) == map_.end()) {
            int16_t code = static_cast<int16_t>(map_.size());
            map_[value] = code;
        }
        return map_[value];
    }

    const std::map<std::string, int16_t>& get_map() const { return map_; }

private:
    std::map<std::string, int16_t> map_;
};

struct TableBuffer {
    std::vector<std::vector<uint8_t>> columns;
    std::vector<std::vector<int32_t>> int_cols;
    std::vector<std::vector<int64_t>> long_cols;
    std::vector<StringDictionary> dict_cols;
    size_t row_count = 0;
};

// Thread-safe buffer for collecting rows per table
struct ThreadSafeBuffer {
    std::mutex mtx;
    std::vector<std::vector<std::string>> rows;
    std::string table_name;
};

// Global thread-safe buffer per table
std::map<std::string, ThreadSafeBuffer> table_buffers;
std::mutex buffer_mtx;
const size_t BUFFER_SIZE = 100000; // Flush every 100K rows

void flush_table_to_disk(const std::string& table_name, const std::string& output_dir);

void write_buffered_rows(const std::string& table_name, const std::vector<std::vector<std::string>>& rows,
                         const std::string& output_dir) {
    if (rows.empty()) return;

    static std::map<std::string, std::vector<std::ofstream>> col_files;
    static std::map<std::string, std::vector<StringDictionary>> dicts;
    static std::map<std::string, bool> table_initialized;

    // Define schema per table
    std::map<std::string, std::vector<std::string>> schema;
    schema["nation"] = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
    schema["region"] = {"r_regionkey", "r_name", "r_comment"};
    schema["supplier"] = {"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"};
    schema["part"] = {"p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"};
    schema["partsupp"] = {"ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"};
    schema["customer"] = {"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"};
    schema["orders"] = {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"};
    schema["lineitem"] = {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"};

    // Type definitions
    auto get_col_type = [](const std::string& table, const std::string& col) -> std::string {
        static std::map<std::string, std::map<std::string, std::string>> types;
        if (types.empty()) {
            types["nation"] = {{"n_nationkey", "int32"}, {"n_name", "dict"}, {"n_regionkey", "int32"}, {"n_comment", "dict"}};
            types["region"] = {{"r_regionkey", "int32"}, {"r_name", "dict"}, {"r_comment", "dict"}};
            types["supplier"] = {{"s_suppkey", "int32"}, {"s_name", "dict"}, {"s_address", "dict"}, {"s_nationkey", "int32"}, {"s_phone", "dict"}, {"s_acctbal", "decimal(100)"}, {"s_comment", "dict"}};
            types["part"] = {{"p_partkey", "int32"}, {"p_name", "dict"}, {"p_mfgr", "dict"}, {"p_brand", "dict"}, {"p_type", "dict"}, {"p_size", "int32"}, {"p_container", "dict"}, {"p_retailprice", "decimal(100)"}, {"p_comment", "dict"}};
            types["partsupp"] = {{"ps_partkey", "int32"}, {"ps_suppkey", "int32"}, {"ps_availqty", "int32"}, {"ps_supplycost", "decimal(100)"}, {"ps_comment", "dict"}};
            types["customer"] = {{"c_custkey", "int32"}, {"c_name", "dict"}, {"c_address", "dict"}, {"c_nationkey", "int32"}, {"c_phone", "dict"}, {"c_acctbal", "decimal(100)"}, {"c_mktsegment", "dict"}, {"c_comment", "dict"}};
            types["orders"] = {{"o_orderkey", "int32"}, {"o_custkey", "int32"}, {"o_orderstatus", "dict"}, {"o_totalprice", "decimal(100)"}, {"o_orderdate", "date"}, {"o_orderpriority", "dict"}, {"o_clerk", "dict"}, {"o_shippriority", "int32"}, {"o_comment", "dict"}};
            types["lineitem"] = {{"l_orderkey", "int32"}, {"l_partkey", "int32"}, {"l_suppkey", "int32"}, {"l_linenumber", "int32"}, {"l_quantity", "decimal(1)"}, {"l_extendedprice", "decimal(100)"}, {"l_discount", "decimal(100)"}, {"l_tax", "decimal(100)"}, {"l_returnflag", "dict"}, {"l_linestatus", "dict"}, {"l_shipdate", "date"}, {"l_commitdate", "date"}, {"l_receiptdate", "date"}, {"l_shipinstruct", "dict"}, {"l_shipmode", "dict"}, {"l_comment", "dict"}};
        }
        return types[table][col];
    };

    const auto& cols = schema[table_name];
    if (!table_initialized[table_name]) {
        // Initialize output files and dictionaries
        std::string col_dir = output_dir + "/" + table_name;
        mkdir(col_dir.c_str(), 0755);

        col_files[table_name].resize(cols.size());
        dicts[table_name].resize(cols.size());

        for (size_t i = 0; i < cols.size(); ++i) {
            std::string col_file = col_dir + "/" + cols[i] + ".bin";
            col_files[table_name][i].open(col_file, std::ios::binary | std::ios::app);
        }
        table_initialized[table_name] = true;
    }

    // Write rows
    for (const auto& row : rows) {
        for (size_t col_idx = 0; col_idx < cols.size(); ++col_idx) {
            const std::string& col_name = cols[col_idx];
            const std::string& col_type = get_col_type(table_name, col_name);
            const std::string& value = row[col_idx];

            if (col_type == "int32") {
                int32_t v = std::stoi(value);
                col_files[table_name][col_idx].write(reinterpret_cast<char*>(&v), sizeof(int32_t));
            } else if (col_type == "dict") {
                int16_t code = dicts[table_name][col_idx].encode(value);
                col_files[table_name][col_idx].write(reinterpret_cast<char*>(&code), sizeof(int16_t));
            } else if (col_type.find("decimal") == 0) {
                int scale = std::stoi(col_type.substr(8, col_type.length() - 9));
                int64_t v = parse_decimal(value, scale);
                col_files[table_name][col_idx].write(reinterpret_cast<char*>(&v), sizeof(int64_t));
            } else if (col_type == "date") {
                int32_t v = parse_date(value);
                col_files[table_name][col_idx].write(reinterpret_cast<char*>(&v), sizeof(int32_t));
            }
        }
    }
}

void write_dictionaries(const std::string& table_name, const std::string& output_dir) {
    static std::map<std::string, std::vector<StringDictionary>> dicts;
    static std::map<std::string, std::vector<std::ofstream>> col_files;

    // Retrieve dictionaries and write them
    std::string col_dir = output_dir + "/" + table_name;

    // Get column names for this table
    std::map<std::string, std::vector<std::string>> schema;
    schema["nation"] = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
    schema["region"] = {"r_regionkey", "r_name", "r_comment"};
    schema["supplier"] = {"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"};
    schema["part"] = {"p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"};
    schema["partsupp"] = {"ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"};
    schema["customer"] = {"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"};
    schema["orders"] = {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"};
    schema["lineitem"] = {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"};

    const auto& cols = schema[table_name];
    for (const auto& col : cols) {
        // Dictionary files will be handled during column writing
    }
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <input_dir> <output_dir>\n";
        return 1;
    }

    std::string input_dir = argv[1];
    std::string output_dir = argv[2];

    // Create output base directory
    mkdir(output_dir.c_str(), 0755);

    // Table names to process
    std::vector<std::string> tables = {"nation", "region", "supplier", "part", "partsupp", "customer", "orders", "lineitem"};

    // Process each table
    for (const auto& table : tables) {
        std::cout << "Ingesting " << table << "...\n";

        std::string input_file = input_dir + "/" + table + ".tbl";
        std::ifstream in(input_file);
        if (!in) {
            std::cerr << "Error: Cannot open " << input_file << "\n";
            continue;
        }

        std::string line;
        std::vector<std::vector<std::string>> buffer;

        while (std::getline(in, line)) {
            // Parse pipe-delimited line
            std::vector<std::string> row;
            size_t start = 0;
            while (start < line.length()) {
                size_t end = line.find('|', start);
                if (end == std::string::npos) end = line.length();
                row.push_back(line.substr(start, end - start));
                start = end + 1;
            }

            buffer.push_back(row);

            if (buffer.size() >= BUFFER_SIZE) {
                write_buffered_rows(table, buffer, output_dir);
                buffer.clear();
            }
        }

        // Flush remaining
        if (!buffer.empty()) {
            write_buffered_rows(table, buffer, output_dir);
        }

        in.close();
        std::cout << "  Completed " << table << "\n";
    }

    std::cout << "Ingestion complete!\n";
    return 0;
}
