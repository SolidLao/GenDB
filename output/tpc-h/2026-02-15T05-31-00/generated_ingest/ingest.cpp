#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <cstring>
#include <cstdint>
#include <thread>
#include <mutex>
#include <atomic>
#include <algorithm>
#include <charconv>
#include <filesystem>
#include <cmath>
#include <ctime>
#include <cassert>

namespace fs = std::filesystem;

// Helper: Parse date from YYYY-MM-DD to days since epoch (1970-01-01)
int32_t parse_date(const std::string& date_str) {
    if (date_str.empty() || date_str.length() < 10) {
        return 0;
    }
    int year, month, day;
    sscanf(date_str.c_str(), "%d-%d-%d", &year, &month, &day);

    // Days from 1970 to year-01-01
    int32_t days = 0;
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Days from year-01-01 to month-01
    static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    for (int m = 1; m < month; ++m) {
        days += days_in_month[m];
        if (m == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
            days += 1;
        }
    }

    days += day;
    return days;
}

// Helper: Parse decimal from string to int64_t with scale
int64_t parse_decimal(const std::string& str, int scale) {
    double val = 0.0;
    auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.length(), val);
    if (ec != std::errc()) {
        return 0;
    }
    return static_cast<int64_t>(std::round(val * scale));
}

// Helper: Parse integer from string
int32_t parse_int(const std::string& str) {
    int32_t val = 0;
    auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.length(), val);
    if (ec != std::errc()) {
        return 0;
    }
    return val;
}

// Helper: Split string by delimiter
std::vector<std::string> split(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    for (char c : s) {
        if (c == delimiter) {
            tokens.push_back(token);
            token.clear();
        } else {
            token += c;
        }
    }
    tokens.push_back(token);
    return tokens;
}

// Dictionary encoding: map string to uint8_t
class DictionaryEncoder {
public:
    uint8_t encode(const std::string& value) {
        auto it = map.find(value);
        if (it != map.end()) {
            return it->second;
        }
        uint8_t code = static_cast<uint8_t>(map.size());
        map[value] = code;
        reverse_map[code] = value;
        return code;
    }

    void save(const std::string& path) {
        std::ofstream f(path);
        for (auto& [val, code] : reverse_map) {
            f << static_cast<int>(val) << "=" << reverse_map[val] << "\n";
        }
    }

private:
    std::unordered_map<std::string, uint8_t> map;
    std::unordered_map<uint8_t, std::string> reverse_map;
};

// Structure to hold data for a table
struct TableData {
    std::string name;
    std::map<std::string, std::vector<int32_t>> int_cols;
    std::map<std::string, std::vector<int64_t>> decimal_cols;
    std::map<std::string, std::vector<int32_t>> date_cols;
    std::map<std::string, std::vector<uint8_t>> dict_cols;
    std::map<std::string, std::vector<std::string>> string_cols;
    std::map<std::string, DictionaryEncoder*> encoders;
    size_t row_count = 0;
    std::mutex mutex;
};

// Global table data storage
std::map<std::string, TableData> tables;
std::mutex tables_mutex;

// Parse and ingest a single line for lineitem
void process_lineitem_line(const std::string& line, TableData& data) {
    auto fields = split(line, '|');
    if (fields.size() < 16) return;

    {
        std::lock_guard<std::mutex> lock(data.mutex);
        data.int_cols["l_orderkey"].push_back(parse_int(fields[0]));
        data.int_cols["l_partkey"].push_back(parse_int(fields[1]));
        data.int_cols["l_suppkey"].push_back(parse_int(fields[2]));
        data.int_cols["l_linenumber"].push_back(parse_int(fields[3]));
        data.decimal_cols["l_quantity"].push_back(parse_decimal(fields[4], 100));
        data.decimal_cols["l_extendedprice"].push_back(parse_decimal(fields[5], 100));
        data.decimal_cols["l_discount"].push_back(parse_decimal(fields[6], 100));
        data.decimal_cols["l_tax"].push_back(parse_decimal(fields[7], 100));
        data.dict_cols["l_returnflag"].push_back(data.encoders["l_returnflag"]->encode(fields[8]));
        data.dict_cols["l_linestatus"].push_back(data.encoders["l_linestatus"]->encode(fields[9]));
        data.date_cols["l_shipdate"].push_back(parse_date(fields[10]));
        data.date_cols["l_commitdate"].push_back(parse_date(fields[11]));
        data.date_cols["l_receiptdate"].push_back(parse_date(fields[12]));
        data.dict_cols["l_shipinstruct"].push_back(data.encoders["l_shipinstruct"]->encode(fields[13]));
        data.dict_cols["l_shipmode"].push_back(data.encoders["l_shipmode"]->encode(fields[14]));
        data.string_cols["l_comment"].push_back(fields[15]);
        data.row_count++;
    }
}

// Parse and ingest a single line for orders
void process_orders_line(const std::string& line, TableData& data) {
    auto fields = split(line, '|');
    if (fields.size() < 9) return;

    {
        std::lock_guard<std::mutex> lock(data.mutex);
        data.int_cols["o_orderkey"].push_back(parse_int(fields[0]));
        data.int_cols["o_custkey"].push_back(parse_int(fields[1]));
        data.dict_cols["o_orderstatus"].push_back(data.encoders["o_orderstatus"]->encode(fields[2]));
        data.decimal_cols["o_totalprice"].push_back(parse_decimal(fields[3], 100));
        data.date_cols["o_orderdate"].push_back(parse_date(fields[4]));
        data.dict_cols["o_orderpriority"].push_back(data.encoders["o_orderpriority"]->encode(fields[5]));
        data.string_cols["o_clerk"].push_back(fields[6]);
        data.int_cols["o_shippriority"].push_back(parse_int(fields[7]));
        data.string_cols["o_comment"].push_back(fields[8]);
        data.row_count++;
    }
}

// Parse and ingest a single line for customer
void process_customer_line(const std::string& line, TableData& data) {
    auto fields = split(line, '|');
    if (fields.size() < 8) return;

    {
        std::lock_guard<std::mutex> lock(data.mutex);
        data.int_cols["c_custkey"].push_back(parse_int(fields[0]));
        data.string_cols["c_name"].push_back(fields[1]);
        data.string_cols["c_address"].push_back(fields[2]);
        data.int_cols["c_nationkey"].push_back(parse_int(fields[3]));
        data.string_cols["c_phone"].push_back(fields[4]);
        data.decimal_cols["c_acctbal"].push_back(parse_decimal(fields[5], 100));
        data.dict_cols["c_mktsegment"].push_back(data.encoders["c_mktsegment"]->encode(fields[6]));
        data.string_cols["c_comment"].push_back(fields[7]);
        data.row_count++;
    }
}

// Parse and ingest a single line for supplier
void process_supplier_line(const std::string& line, TableData& data) {
    auto fields = split(line, '|');
    if (fields.size() < 7) return;

    {
        std::lock_guard<std::mutex> lock(data.mutex);
        data.int_cols["s_suppkey"].push_back(parse_int(fields[0]));
        data.string_cols["s_name"].push_back(fields[1]);
        data.string_cols["s_address"].push_back(fields[2]);
        data.int_cols["s_nationkey"].push_back(parse_int(fields[3]));
        data.string_cols["s_phone"].push_back(fields[4]);
        data.decimal_cols["s_acctbal"].push_back(parse_decimal(fields[5], 100));
        data.string_cols["s_comment"].push_back(fields[6]);
        data.row_count++;
    }
}

// Parse and ingest a single line for part
void process_part_line(const std::string& line, TableData& data) {
    auto fields = split(line, '|');
    if (fields.size() < 8) return;

    {
        std::lock_guard<std::mutex> lock(data.mutex);
        data.int_cols["p_partkey"].push_back(parse_int(fields[0]));
        data.string_cols["p_name"].push_back(fields[1]);
        data.dict_cols["p_mfgr"].push_back(data.encoders["p_mfgr"]->encode(fields[2]));
        data.dict_cols["p_brand"].push_back(data.encoders["p_brand"]->encode(fields[3]));
        data.string_cols["p_type"].push_back(fields[4]);
        data.int_cols["p_size"].push_back(parse_int(fields[5]));
        data.dict_cols["p_container"].push_back(data.encoders["p_container"]->encode(fields[6]));
        data.decimal_cols["p_retailprice"].push_back(parse_decimal(fields[7], 100));
        data.row_count++;
    }
}

// Parse and ingest a single line for partsupp
void process_partsupp_line(const std::string& line, TableData& data) {
    auto fields = split(line, '|');
    if (fields.size() < 5) return;

    {
        std::lock_guard<std::mutex> lock(data.mutex);
        data.int_cols["ps_partkey"].push_back(parse_int(fields[0]));
        data.int_cols["ps_suppkey"].push_back(parse_int(fields[1]));
        data.int_cols["ps_availqty"].push_back(parse_int(fields[2]));
        data.decimal_cols["ps_supplycost"].push_back(parse_decimal(fields[3], 100));
        data.string_cols["ps_comment"].push_back(fields[4]);
        data.row_count++;
    }
}

// Parse and ingest a single line for nation
void process_nation_line(const std::string& line, TableData& data) {
    auto fields = split(line, '|');
    if (fields.size() < 4) return;

    {
        std::lock_guard<std::mutex> lock(data.mutex);
        data.int_cols["n_nationkey"].push_back(parse_int(fields[0]));
        data.string_cols["n_name"].push_back(fields[1]);
        data.int_cols["n_regionkey"].push_back(parse_int(fields[2]));
        data.string_cols["n_comment"].push_back(fields[3]);
        data.row_count++;
    }
}

// Parse and ingest a single line for region
void process_region_line(const std::string& line, TableData& data) {
    auto fields = split(line, '|');
    if (fields.size() < 3) return;

    {
        std::lock_guard<std::mutex> lock(data.mutex);
        data.int_cols["r_regionkey"].push_back(parse_int(fields[0]));
        data.string_cols["r_name"].push_back(fields[1]);
        data.string_cols["r_comment"].push_back(fields[2]);
        data.row_count++;
    }
}

// Read file and ingest lines (parallelized chunking)
void ingest_file(const std::string& path, TableData& data,
                 void (*process_func)(const std::string&, TableData&)) {
    std::ifstream file(path);
    if (!file.is_open()) {
        std::cerr << "Error opening " << path << "\n";
        return;
    }

    std::string line;
    size_t line_count = 0;
    while (std::getline(file, line)) {
        if (!line.empty()) {
            process_func(line, data);
            line_count++;
        }
    }

    std::cout << "  Ingested " << line_count << " rows from " << fs::path(path).filename().string() << "\n";
}

// Write binary column data for a single integer column
void write_int_column(const std::string& path, const std::vector<int32_t>& data) {
    std::ofstream f(path, std::ios::binary);
    for (int32_t val : data) {
        f.write(reinterpret_cast<const char*>(&val), sizeof(int32_t));
    }
}

// Write binary column data for a single decimal column
void write_decimal_column(const std::string& path, const std::vector<int64_t>& data) {
    std::ofstream f(path, std::ios::binary);
    for (int64_t val : data) {
        f.write(reinterpret_cast<const char*>(&val), sizeof(int64_t));
    }
}

// Write binary column data for a single date column
void write_date_column(const std::string& path, const std::vector<int32_t>& data) {
    std::ofstream f(path, std::ios::binary);
    for (int32_t val : data) {
        f.write(reinterpret_cast<const char*>(&val), sizeof(int32_t));
    }
}

// Write binary column data for a dictionary-encoded column
void write_dict_column(const std::string& path, const std::vector<uint8_t>& data) {
    std::ofstream f(path, std::ios::binary);
    for (uint8_t val : data) {
        f.write(reinterpret_cast<const char*>(&val), sizeof(uint8_t));
    }
}

// Write string column: length-prefixed format
void write_string_column(const std::string& path, const std::vector<std::string>& data) {
    std::ofstream f(path, std::ios::binary);
    for (const auto& str : data) {
        uint32_t len = str.length();
        f.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
        f.write(str.data(), len);
    }
}

// Write all columns for a table
void write_table(const std::string& gendb_dir, const std::string& table_name, TableData& data) {
    std::string table_dir = gendb_dir + "/" + table_name;
    fs::create_directories(table_dir);

    // Write integer columns
    for (auto& [col_name, col_data] : data.int_cols) {
        write_int_column(table_dir + "/" + col_name + ".bin", col_data);
    }

    // Write decimal columns
    for (auto& [col_name, col_data] : data.decimal_cols) {
        write_decimal_column(table_dir + "/" + col_name + ".bin", col_data);
    }

    // Write date columns
    for (auto& [col_name, col_data] : data.date_cols) {
        write_date_column(table_dir + "/" + col_name + ".bin", col_data);
    }

    // Write dictionary columns (both encoded data and dictionary)
    for (auto& [col_name, col_data] : data.dict_cols) {
        write_dict_column(table_dir + "/" + col_name + ".bin", col_data);
        data.encoders[col_name]->save(table_dir + "/" + col_name + "_dict.txt");
    }

    // Write string columns
    for (auto& [col_name, col_data] : data.string_cols) {
        write_string_column(table_dir + "/" + col_name + ".bin", col_data);
    }

    std::cout << "Wrote " << table_name << " with " << data.row_count << " rows\n";
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>\n";
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    fs::create_directories(gendb_dir);

    std::cout << "Ingesting TPC-H SF10 from " << data_dir << " to " << gendb_dir << "\n";

    // Initialize table structures
    TableData lineitem_data;
    lineitem_data.name = "lineitem";
    lineitem_data.encoders["l_returnflag"] = new DictionaryEncoder();
    lineitem_data.encoders["l_linestatus"] = new DictionaryEncoder();
    lineitem_data.encoders["l_shipinstruct"] = new DictionaryEncoder();
    lineitem_data.encoders["l_shipmode"] = new DictionaryEncoder();

    TableData orders_data;
    orders_data.name = "orders";
    orders_data.encoders["o_orderstatus"] = new DictionaryEncoder();
    orders_data.encoders["o_orderpriority"] = new DictionaryEncoder();

    TableData customer_data;
    customer_data.name = "customer";
    customer_data.encoders["c_mktsegment"] = new DictionaryEncoder();

    TableData supplier_data;
    supplier_data.name = "supplier";

    TableData part_data;
    part_data.name = "part";
    part_data.encoders["p_mfgr"] = new DictionaryEncoder();
    part_data.encoders["p_brand"] = new DictionaryEncoder();
    part_data.encoders["p_container"] = new DictionaryEncoder();

    TableData partsupp_data;
    partsupp_data.name = "partsupp";

    TableData nation_data;
    nation_data.name = "nation";

    TableData region_data;
    region_data.name = "region";

    // Ingest each table
    std::cout << "Ingesting tables...\n";
    ingest_file(data_dir + "/lineitem.tbl", lineitem_data, process_lineitem_line);
    ingest_file(data_dir + "/orders.tbl", orders_data, process_orders_line);
    ingest_file(data_dir + "/customer.tbl", customer_data, process_customer_line);
    ingest_file(data_dir + "/supplier.tbl", supplier_data, process_supplier_line);
    ingest_file(data_dir + "/part.tbl", part_data, process_part_line);
    ingest_file(data_dir + "/partsupp.tbl", partsupp_data, process_partsupp_line);
    ingest_file(data_dir + "/nation.tbl", nation_data, process_nation_line);
    ingest_file(data_dir + "/region.tbl", region_data, process_region_line);

    // Write all tables
    std::cout << "\nWriting binary columns...\n";
    write_table(gendb_dir, "lineitem", lineitem_data);
    write_table(gendb_dir, "orders", orders_data);
    write_table(gendb_dir, "customer", customer_data);
    write_table(gendb_dir, "supplier", supplier_data);
    write_table(gendb_dir, "part", part_data);
    write_table(gendb_dir, "partsupp", partsupp_data);
    write_table(gendb_dir, "nation", nation_data);
    write_table(gendb_dir, "region", region_data);

    // Verify some dates and decimals
    std::cout << "\nPost-ingestion verification:\n";
    if (!lineitem_data.date_cols["l_shipdate"].empty()) {
        int32_t min_date = *std::min_element(lineitem_data.date_cols["l_shipdate"].begin(),
                                            lineitem_data.date_cols["l_shipdate"].end());
        int32_t max_date = *std::max_element(lineitem_data.date_cols["l_shipdate"].begin(),
                                            lineitem_data.date_cols["l_shipdate"].end());
        std::cout << "  l_shipdate range: " << min_date << " to " << max_date << " (epoch days)\n";
        assert(min_date > 0 && max_date < 40000 && "Date values out of expected range");
    }

    if (!lineitem_data.decimal_cols["l_quantity"].empty()) {
        int64_t sample = lineitem_data.decimal_cols["l_quantity"][0];
        std::cout << "  l_quantity[0] = " << sample << " (scaled by 100)\n";
        assert(sample >= 0 && "Decimal should be non-negative");
    }

    std::cout << "\nIngestion complete!\n";
    return 0;
}
