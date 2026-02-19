#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <mutex>
#include <atomic>
#include <cassert>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <map>
#include <unordered_map>

// Date utilities
int32_t parse_date(const std::string& date_str) {
    // Parse "YYYY-MM-DD" to days since 1970-01-01
    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Days in each month (non-leap year)
    const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    int32_t total_days = 0;

    // Add days for all complete years from 1970 to year-1
    for (int y = 1970; y < year; ++y) {
        total_days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Add days for complete months in current year
    for (int m = 1; m < month; ++m) {
        total_days += days_in_month[m];
        if (m == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
            total_days++; // Add leap day if Feb in leap year
        }
    }

    // Add remaining days
    total_days += (day - 1);

    return total_days;
}

// Split line by delimiter
std::vector<std::string> split_line(const std::string& line, char delimiter) {
    std::vector<std::string> fields;
    size_t start = 0;
    size_t end = line.find(delimiter);

    while (end != std::string::npos) {
        fields.push_back(line.substr(start, end - start));
        start = end + 1;
        end = line.find(delimiter, start);
    }

    if (start < line.length()) {
        fields.push_back(line.substr(start));
    }

    return fields;
}

// Remove trailing newline
void trim_newline(std::string& s) {
    if (!s.empty() && s.back() == '\n') {
        s.pop_back();
    }
}

struct ColumnWriter {
    std::string filename;
    std::vector<char> buffer;
    std::mutex mtx;
    static constexpr size_t BUFFER_SIZE = 1024 * 1024; // 1MB

    ColumnWriter(const std::string& fname) : filename(fname) {
        buffer.reserve(BUFFER_SIZE);
    }

    ColumnWriter(const ColumnWriter&) = delete;
    ColumnWriter& operator=(const ColumnWriter&) = delete;

    void write_int32(int32_t val) {
        if (buffer.size() + sizeof(int32_t) > BUFFER_SIZE) flush();
        size_t offset = buffer.size();
        buffer.resize(offset + sizeof(int32_t));
        std::memcpy(buffer.data() + offset, &val, sizeof(int32_t));
    }

    void write_int64(int64_t val) {
        if (buffer.size() + sizeof(int64_t) > BUFFER_SIZE) flush();
        size_t offset = buffer.size();
        buffer.resize(offset + sizeof(int64_t));
        std::memcpy(buffer.data() + offset, &val, sizeof(int64_t));
    }

    void write_int16(int16_t val) {
        if (buffer.size() + sizeof(int16_t) > BUFFER_SIZE) flush();
        size_t offset = buffer.size();
        buffer.resize(offset + sizeof(int16_t));
        std::memcpy(buffer.data() + offset, &val, sizeof(int16_t));
    }

    void write_string(const std::string& val) {
        if (buffer.size() + sizeof(uint32_t) + val.length() > BUFFER_SIZE) flush();
        uint32_t len = val.length();
        size_t offset = buffer.size();
        buffer.resize(offset + sizeof(uint32_t));
        std::memcpy(buffer.data() + offset, &len, sizeof(uint32_t));
        offset = buffer.size();
        buffer.resize(offset + val.length());
        std::memcpy(buffer.data() + offset, val.data(), val.length());
    }

    void flush() {
        if (buffer.empty()) return;
        std::ofstream file(filename, std::ios::app | std::ios::binary);
        file.write(buffer.data(), buffer.size());
        file.close();
        buffer.clear();
    }
};

struct DictionaryColumn {
    std::unordered_map<std::string, int16_t> str_to_code;
    std::vector<std::string> code_to_str;
    int16_t next_code = 0;
    std::mutex mtx;

    int16_t encode(const std::string& str) {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = str_to_code.find(str);
        if (it != str_to_code.end()) {
            return it->second;
        }
        int16_t code = next_code++;
        str_to_code[str] = code;
        code_to_str.push_back(str);
        return code;
    }

    void write_dict_file(const std::string& dict_filename) {
        std::ofstream file(dict_filename);
        for (const auto& str : code_to_str) {
            file << str << "\n";
        }
        file.close();
    }
};

struct Table {
    std::string name;
    std::string filepath;
    std::vector<std::string> columns;
    std::vector<std::string> semantic_types; // INTEGER, DECIMAL, DATE, STRING
    std::vector<int32_t> scale_factors; // For DECIMAL
    std::vector<bool> is_dictionary; // For STRING columns
    size_t row_count = 0;
};

void ingest_table(const Table& tbl, const std::string& output_dir) {
    std::cout << "Ingesting " << tbl.name << " from " << tbl.filepath << "...\n";

    // Open input file
    int fd = open(tbl.filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening " << tbl.filepath << "\n";
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    char* data = (char*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for " << tbl.filepath << "\n";
        close(fd);
        return;
    }

    madvise(data, file_size, MADV_SEQUENTIAL);

    // Create column writers and dictionary columns
    std::vector<std::unique_ptr<ColumnWriter>> writers;
    std::vector<std::unique_ptr<DictionaryColumn>> dict_columns;
    std::string out_table_dir = output_dir + "/" + tbl.name;
    system(("mkdir -p " + out_table_dir).c_str());

    for (size_t i = 0; i < tbl.columns.size(); ++i) {
        writers.push_back(std::make_unique<ColumnWriter>(out_table_dir + "/" + tbl.columns[i] + ".bin"));
        if (tbl.is_dictionary[i]) {
            dict_columns.push_back(std::make_unique<DictionaryColumn>());
        }
    }

    // Parse lines
    size_t line_start = 0;
    size_t row_num = 0;

    while (line_start < file_size) {
        size_t line_end = line_start;
        while (line_end < file_size && data[line_end] != '\n') {
            line_end++;
        }

        std::string line(data + line_start, line_end - line_start);
        line_start = line_end + 1;

        auto fields = split_line(line, '|');
        if (fields.size() != tbl.columns.size()) {
            std::cerr << "Mismatch in field count at row " << row_num << "\n";
            continue;
        }

        size_t dict_idx = 0;
        for (size_t i = 0; i < tbl.columns.size(); ++i) {
            const auto& field = fields[i];

            if (tbl.semantic_types[i] == "INTEGER") {
                int32_t val = std::stoi(field);
                writers[i]->write_int32(val);
            } else if (tbl.semantic_types[i] == "DECIMAL") {
                double dval = std::stod(field);
                int64_t ival = (int64_t)(dval * tbl.scale_factors[i]);
                writers[i]->write_int64(ival);
            } else if (tbl.semantic_types[i] == "DATE") {
                int32_t date_val = parse_date(field);
                if (row_num < 5 && tbl.name == "orders") {
                    std::cerr << "DEBUG: Parsed date " << field << " -> " << date_val << "\n";
                }
                writers[i]->write_int32(date_val);
            } else if (tbl.semantic_types[i] == "STRING") {
                if (tbl.is_dictionary[i]) {
                    int16_t code = dict_columns[dict_idx]->encode(field);
                    writers[i]->write_int16(code);
                    dict_idx++;
                } else {
                    writers[i]->write_string(field);
                }
            }
        }

        row_num++;
        if (row_num % 1000000 == 0) {
            std::cout << "  Processed " << row_num << " rows...\n";
        }
    }

    // Flush all writers
    for (auto& w : writers) {
        w->flush();
    }

    // Write dictionary files
    size_t dict_idx = 0;
    for (size_t i = 0; i < tbl.columns.size(); ++i) {
        if (tbl.is_dictionary[i]) {
            dict_columns[dict_idx]->write_dict_file(out_table_dir + "/" + tbl.columns[i] + "_dict.txt");
            std::cout << "  Dictionary for " << tbl.columns[i] << ": " << dict_columns[dict_idx]->code_to_str.size() << " values\n";
            dict_idx++;
        }
    }

    std::cout << "Ingested " << tbl.name << " (" << row_num << " rows)\n";

    munmap(data, file_size);
    close(fd);
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <input_dir> <output_dir>\n";
        return 1;
    }

    std::string input_dir = argv[1];
    std::string output_dir = argv[2];

    // Create output directory
    system(("mkdir -p " + output_dir).c_str());

    // Define tables
    std::vector<Table> tables;

    // nation
    Table nation;
    nation.name = "nation";
    nation.filepath = input_dir + "/nation.tbl";
    nation.columns = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
    nation.semantic_types = {"INTEGER", "STRING", "INTEGER", "STRING"};
    nation.scale_factors = {0, 0, 0, 0};
    nation.is_dictionary = {false, true, false, false};
    tables.push_back(nation);

    // region
    Table region;
    region.name = "region";
    region.filepath = input_dir + "/region.tbl";
    region.columns = {"r_regionkey", "r_name", "r_comment"};
    region.semantic_types = {"INTEGER", "STRING", "STRING"};
    region.scale_factors = {0, 0, 0};
    region.is_dictionary = {false, true, false};
    tables.push_back(region);

    // supplier
    Table supplier;
    supplier.name = "supplier";
    supplier.filepath = input_dir + "/supplier.tbl";
    supplier.columns = {"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"};
    supplier.semantic_types = {"INTEGER", "STRING", "STRING", "INTEGER", "STRING", "DECIMAL", "STRING"};
    supplier.scale_factors = {0, 0, 0, 0, 0, 100, 0};
    supplier.is_dictionary = {false, false, false, false, false, false, false};
    tables.push_back(supplier);

    // part
    Table part;
    part.name = "part";
    part.filepath = input_dir + "/part.tbl";
    part.columns = {"p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"};
    part.semantic_types = {"INTEGER", "STRING", "STRING", "STRING", "STRING", "INTEGER", "STRING", "DECIMAL", "STRING"};
    part.scale_factors = {0, 0, 0, 0, 0, 0, 0, 100, 0};
    part.is_dictionary = {false, false, false, false, false, false, false, false, false};
    tables.push_back(part);

    // partsupp
    Table partsupp;
    partsupp.name = "partsupp";
    partsupp.filepath = input_dir + "/partsupp.tbl";
    partsupp.columns = {"ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"};
    partsupp.semantic_types = {"INTEGER", "INTEGER", "INTEGER", "DECIMAL", "STRING"};
    partsupp.scale_factors = {0, 0, 0, 100, 0};
    partsupp.is_dictionary = {false, false, false, false, false};
    tables.push_back(partsupp);

    // customer
    Table customer;
    customer.name = "customer";
    customer.filepath = input_dir + "/customer.tbl";
    customer.columns = {"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"};
    customer.semantic_types = {"INTEGER", "STRING", "STRING", "INTEGER", "STRING", "DECIMAL", "STRING", "STRING"};
    customer.scale_factors = {0, 0, 0, 0, 0, 100, 0, 0};
    customer.is_dictionary = {false, false, false, false, false, false, true, false};
    tables.push_back(customer);

    // orders
    Table orders;
    orders.name = "orders";
    orders.filepath = input_dir + "/orders.tbl";
    orders.columns = {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"};
    orders.semantic_types = {"INTEGER", "INTEGER", "STRING", "DECIMAL", "DATE", "STRING", "STRING", "INTEGER", "STRING"};
    orders.scale_factors = {0, 0, 0, 100, 0, 0, 0, 0, 0};
    orders.is_dictionary = {false, false, true, false, false, false, false, false, false};
    tables.push_back(orders);

    // lineitem
    Table lineitem;
    lineitem.name = "lineitem";
    lineitem.filepath = input_dir + "/lineitem.tbl";
    lineitem.columns = {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"};
    lineitem.semantic_types = {"INTEGER", "INTEGER", "INTEGER", "INTEGER", "DECIMAL", "DECIMAL", "DECIMAL", "DECIMAL", "STRING", "STRING", "DATE", "DATE", "DATE", "STRING", "STRING", "STRING"};
    lineitem.scale_factors = {0, 0, 0, 0, 100, 100, 100, 100, 0, 0, 0, 0, 0, 0, 0, 0};
    lineitem.is_dictionary = {false, false, false, false, false, false, false, false, true, true, false, false, false, false, false, false};
    tables.push_back(lineitem);

    // Ingest tables in parallel
    std::vector<std::thread> threads;
    for (const auto& tbl : tables) {
        threads.emplace_back([&tbl, &output_dir]() { ingest_table(tbl, output_dir); });
    }

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "Ingestion complete!\n";

    // Verify some data
    std::cout << "\n=== Verification ===\n";

    // Check date values in lineitem
    std::ifstream lineitem_shipdate(output_dir + "/lineitem/l_shipdate.bin", std::ios::binary);
    if (lineitem_shipdate) {
        int32_t date_val;
        int count = 0;
        int max_val = 0;
        while (lineitem_shipdate.read((char*)&date_val, sizeof(int32_t)) && count < 100) {
            max_val = std::max(max_val, date_val);
            count++;
        }
        std::cout << "Sample l_shipdate values (days since 1970-01-01): " << date_val << " (max in first 100: " << max_val << ")\n";
        if (max_val < 3000) {
            std::cout << "WARNING: DATE values too small, encoding may be incorrect!\n";
        }
        lineitem_shipdate.close();
    }

    // Check decimal values in lineitem
    std::ifstream lineitem_extprice(output_dir + "/lineitem/l_extendedprice.bin", std::ios::binary);
    if (lineitem_extprice) {
        int64_t val;
        int count = 0;
        bool found_nonzero = false;
        while (lineitem_extprice.read((char*)&val, sizeof(int64_t)) && count < 100) {
            if (val != 0) {
                found_nonzero = true;
                std::cout << "Sample l_extendedprice (scaled): " << val << " (SQL: " << (double)val / 100.0 << ")\n";
                break;
            }
            count++;
        }
        if (!found_nonzero) {
            std::cout << "WARNING: DECIMAL values all zero, encoding may be incorrect!\n";
        }
        lineitem_extprice.close();
    }

    return 0;
}
