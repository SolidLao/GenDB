#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <memory>
#include <cstring>
#include <unordered_map>
#include <algorithm>
#include <filesystem>
#include <map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace fs = std::filesystem;

struct ColumnData {
    std::vector<uint8_t> buffer;
    std::mutex mutex;
    size_t element_size;
    size_t row_count = 0;
};

struct TableWriter {
    std::string table_name;
    std::map<std::string, ColumnData> columns;
    std::mutex row_count_mutex;
};

// Parse date from string (YYYY-MM-DD) to days since 1970-01-01
int32_t parse_date(const std::string& date_str) {
    if (date_str.empty()) return 0;
    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Days since epoch (1970-01-01)
    int32_t days = 0;
    // Sum complete years from 1970 to year-1
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    // Sum complete months in this year
    const int days_per_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int is_leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 1 : 0;
    for (int m = 1; m < month; ++m) {
        days += days_per_month[m - 1];
        if (m == 2 && is_leap) days++;
    }
    days += day - 1;
    return days;
}

// Parse DECIMAL(15,2) to int64_t scaled by 100
int64_t parse_decimal(const std::string& decimal_str) {
    if (decimal_str.empty()) return 0;
    double value = std::stod(decimal_str);
    return (int64_t)(value * 100.0 + 0.5);  // Round to nearest
}

void write_value(std::vector<uint8_t>& buffer, const std::string& value, const std::string& type,
                 std::unordered_map<std::string, int32_t>& dict) {
    if (type == "int32_t") {
        int32_t v = value.empty() ? 0 : std::stoi(value);
        buffer.insert(buffer.end(), (uint8_t*)&v, (uint8_t*)&v + 4);
    } else if (type == "int64_t") {
        int64_t v = value.empty() ? 0 : std::stoll(value);
        buffer.insert(buffer.end(), (uint8_t*)&v, (uint8_t*)&v + 8);
    } else if (type == "date") {
        int32_t v = parse_date(value);
        buffer.insert(buffer.end(), (uint8_t*)&v, (uint8_t*)&v + 4);
    } else if (type == "decimal") {
        int64_t v = parse_decimal(value);
        buffer.insert(buffer.end(), (uint8_t*)&v, (uint8_t*)&v + 8);
    } else if (type == "dict") {
        int32_t dict_id;
        auto it = dict.find(value);
        if (it != dict.end()) {
            dict_id = it->second;
        } else {
            dict_id = (int32_t)dict.size();
            dict[value] = dict_id;
        }
        buffer.insert(buffer.end(), (uint8_t*)&dict_id, (uint8_t*)&dict_id + 4);
    }
}

void ingest_table(const std::string& input_file, const std::string& output_dir,
                  const std::vector<std::pair<std::string, std::string>>& schema) {
    // Read entire file into memory
    int fd = open(input_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << input_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        perror("mmap");
        close(fd);
        return;
    }

    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Parse CSV with pipe delimiter
    std::vector<ColumnData> columns(schema.size());
    for (size_t i = 0; i < schema.size(); ++i) {
        columns[i].element_size = (schema[i].second == "int32_t") ? 4 :
                                   (schema[i].second == "int64_t" || schema[i].second == "date") ? 4 :
                                   (schema[i].second == "decimal") ? 8 : 0;
    }

    std::unordered_map<std::string, std::unordered_map<std::string, int32_t>> dict_maps;
    for (size_t i = 0; i < schema.size(); ++i) {
        if (schema[i].second == "dict") {
            dict_maps[schema[i].first] = std::unordered_map<std::string, int32_t>();
        }
    }

    size_t line_start = 0;
    size_t row_count = 0;

    for (size_t i = 0; i <= file_size; ++i) {
        if (i == file_size || data[i] == '\n') {
            if (i > line_start) {
                // Parse line
                std::string line(data + line_start, i - line_start);

                std::vector<std::string> fields;
                size_t pos = 0;
                for (size_t j = 0; j < line.size(); ++j) {
                    if (line[j] == '|' || j == line.size() - 1) {
                        size_t end = (j == line.size() - 1 && line[j] != '|') ? j + 1 : j;
                        fields.push_back(line.substr(pos, end - pos));
                        pos = j + 1;
                    }
                }

                if (fields.size() >= schema.size()) {
                    for (size_t col_idx = 0; col_idx < schema.size(); ++col_idx) {
                        const auto& col_name = schema[col_idx].first;
                        const auto& col_type = schema[col_idx].second;

                        if (col_type == "string") {
                            // Store strings as variable-length; we'll handle this separately
                            continue;
                        } else {
                            auto& buffer = columns[col_idx].buffer;
                            auto& dict_map = dict_maps[col_name];
                            write_value(buffer, fields[col_idx], col_type, dict_map);
                        }
                    }
                    row_count++;
                }
            }
            line_start = i + 1;
        }
    }

    munmap((void*)data, file_size);
    close(fd);

    // Write columns to binary files
    std::string table_name = fs::path(input_file).stem().string();
    std::string table_dir = output_dir + "/" + table_name;
    fs::create_directories(table_dir);

    for (size_t col_idx = 0; col_idx < schema.size(); ++col_idx) {
        const auto& col_name = schema[col_idx].first;
        const auto& col_type = schema[col_idx].second;

        if (col_type == "string") continue;  // Handle strings separately

        auto& buffer = columns[col_idx].buffer;
        std::string col_file = table_dir + "/" + col_name + ".bin";
        std::ofstream out(col_file, std::ios::binary);
        out.write((const char*)buffer.data(), buffer.size());
        out.close();

        std::cout << "Wrote " << col_file << " (" << buffer.size() << " bytes)" << std::endl;
    }

    // Write dictionary files
    for (auto& [col_name, dict_map] : dict_maps) {
        std::string dict_file = table_dir + "/" + col_name + "_dict.txt";
        std::ofstream out(dict_file);
        for (const auto& [value, id] : dict_map) {
            out << value << "\n";
        }
        out.close();
        std::cout << "Wrote " << dict_file << " (" << dict_map.size() << " entries)" << std::endl;
    }

    std::cout << "Ingested " << table_name << ": " << row_count << " rows" << std::endl;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: ingest <input_dir> <output_dir>" << std::endl;
        return 1;
    }

    std::string input_dir = argv[1];
    std::string output_dir = argv[2];

    fs::create_directories(output_dir);

    // Define schemas for each table
    std::map<std::string, std::vector<std::pair<std::string, std::string>>> schemas = {
        {"nation", {
            {"n_nationkey", "int32_t"},
            {"n_name", "string"},
            {"n_regionkey", "int32_t"},
            {"n_comment", "string"}
        }},
        {"region", {
            {"r_regionkey", "int32_t"},
            {"r_name", "string"},
            {"r_comment", "string"}
        }},
        {"supplier", {
            {"s_suppkey", "int32_t"},
            {"s_name", "string"},
            {"s_address", "string"},
            {"s_nationkey", "int32_t"},
            {"s_phone", "string"},
            {"s_acctbal", "decimal"},
            {"s_comment", "string"}
        }},
        {"part", {
            {"p_partkey", "int32_t"},
            {"p_name", "string"},
            {"p_mfgr", "dict"},
            {"p_brand", "dict"},
            {"p_type", "dict"},
            {"p_size", "int32_t"},
            {"p_container", "dict"},
            {"p_retailprice", "decimal"},
            {"p_comment", "string"}
        }},
        {"partsupp", {
            {"ps_partkey", "int32_t"},
            {"ps_suppkey", "int32_t"},
            {"ps_availqty", "int32_t"},
            {"ps_supplycost", "decimal"},
            {"ps_comment", "string"}
        }},
        {"customer", {
            {"c_custkey", "int32_t"},
            {"c_name", "string"},
            {"c_address", "string"},
            {"c_nationkey", "int32_t"},
            {"c_phone", "string"},
            {"c_acctbal", "decimal"},
            {"c_mktsegment", "dict"},
            {"c_comment", "string"}
        }},
        {"orders", {
            {"o_orderkey", "int32_t"},
            {"o_custkey", "int32_t"},
            {"o_orderstatus", "dict"},
            {"o_totalprice", "decimal"},
            {"o_orderdate", "date"},
            {"o_orderpriority", "dict"},
            {"o_clerk", "string"},
            {"o_shippriority", "int32_t"},
            {"o_comment", "string"}
        }},
        {"lineitem", {
            {"l_orderkey", "int32_t"},
            {"l_partkey", "int32_t"},
            {"l_suppkey", "int32_t"},
            {"l_linenumber", "int32_t"},
            {"l_quantity", "decimal"},
            {"l_extendedprice", "decimal"},
            {"l_discount", "decimal"},
            {"l_tax", "decimal"},
            {"l_returnflag", "dict"},
            {"l_linestatus", "dict"},
            {"l_shipdate", "date"},
            {"l_commitdate", "date"},
            {"l_receiptdate", "date"},
            {"l_shipinstruct", "dict"},
            {"l_shipmode", "dict"},
            {"l_comment", "string"}
        }}
    };

    // Ingest each table
    for (const auto& [table_name, schema] : schemas) {
        std::string input_file = input_dir + "/" + table_name + ".tbl";
        if (fs::exists(input_file)) {
            std::cout << "Ingesting " << table_name << "..." << std::endl;
            ingest_table(input_file, output_dir, schema);
        }
    }

    return 0;
}
