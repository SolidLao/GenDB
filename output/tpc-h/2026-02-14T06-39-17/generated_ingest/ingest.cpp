#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <thread>
#include <chrono>
#include <unordered_map>
#include <charconv>
#include <filesystem>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

struct Column {
    std::string name;
    std::string cpp_type;
    std::string semantic_type;
    std::string encoding;
};

struct TableSchema {
    std::string table_name;
    std::vector<Column> columns;
    std::vector<std::string> column_order;
    std::string filename;
    char delimiter;
    std::string sort_column;
    int32_t block_size;
};

int32_t parse_date(const std::string& date_str) {
    int year, month, day;
    if (sscanf(date_str.c_str(), "%d-%d-%d", &year, &month, &day) != 3) {
        throw std::runtime_error("Invalid date format: " + date_str);
    }

    const int days_before_month[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    int leap_years = (year - 1970) / 4 - (year - 1970) / 100 + (year - 1970) / 400;
    int non_leap_years = (year - 1970) - leap_years;
    int days = non_leap_years * 365 + leap_years * 366 + days_before_month[month - 1] + day - 1;

    if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
        if (month > 2) days++;
    }

    return days;
}

std::string_view get_field(const std::string_view& line, int field_idx, char delimiter) {
    size_t start = 0;
    for (int i = 0; i < field_idx; ++i) {
        start = line.find(delimiter, start) + 1;
        if (start == 0) return {};
    }
    size_t end = line.find(delimiter, start);
    if (end == std::string_view::npos) end = line.size();
    return line.substr(start, end - start);
}

struct DictionaryEncoder {
    std::unordered_map<std::string, uint8_t> encode_map;
    std::vector<std::string> decode_map;
    uint8_t next_code = 0;

    uint8_t encode(const std::string& value) {
        auto it = encode_map.find(value);
        if (it != encode_map.end()) {
            return it->second;
        }
        uint8_t code = next_code++;
        encode_map[value] = code;
        decode_map.push_back(value);
        return code;
    }

    void save(const std::string& dict_file) {
        std::ofstream out(dict_file);
        for (uint8_t i = 0; i < decode_map.size(); ++i) {
            out << (int)i << "=" << decode_map[i] << "\n";
        }
    }
};

void ingest_table(const std::string& data_dir, const std::string& gendb_dir, const TableSchema& schema) {
    std::string file_path = data_dir + "/" + schema.filename;
    std::cout << "Ingesting " << schema.table_name << " from " << file_path << std::endl;

    int fd = open(file_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << file_path << std::endl;
        return;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    char* file_data = (char*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (file_data == MAP_FAILED) {
        std::cerr << "mmap failed" << std::endl;
        close(fd);
        return;
    }
    madvise(file_data, file_size, MADV_SEQUENTIAL);

    std::vector<std::vector<int32_t>> int_columns(schema.columns.size());
    std::vector<std::vector<double>> double_columns(schema.columns.size());
    std::vector<std::vector<std::string>> string_columns(schema.columns.size());
    std::vector<std::vector<uint8_t>> dict_columns(schema.columns.size());
    std::vector<DictionaryEncoder> dict_encoders(schema.columns.size());

    const char* line_start = file_data;
    const char* file_end = file_data + file_size;
    int row_count = 0;

    while (line_start < file_end) {
        const char* line_end = std::strchr(line_start, '\n');
        if (!line_end) line_end = file_end;

        std::string_view line(line_start, line_end - line_start);

        for (size_t col_idx = 0; col_idx < schema.columns.size(); ++col_idx) {
            auto field = get_field(line, col_idx, schema.delimiter);
            const Column& col = schema.columns[col_idx];

            if (col.semantic_type == "INTEGER") {
                int32_t val = 0;
                std::from_chars(field.data(), field.data() + field.size(), val);
                int_columns[col_idx].push_back(val);
            } else if (col.semantic_type == "DECIMAL") {
                double val = 0.0;
                std::from_chars(field.data(), field.data() + field.size(), val);
                double_columns[col_idx].push_back(val);
            } else if (col.semantic_type == "DATE") {
                std::string field_str(field);
                int32_t epoch_days = parse_date(field_str);
                int_columns[col_idx].push_back(epoch_days);
            } else if (col.semantic_type == "STRING") {
                std::string field_str(field);
                if (col.encoding == "dictionary") {
                    uint8_t code = dict_encoders[col_idx].encode(field_str);
                    dict_columns[col_idx].push_back(code);
                } else {
                    string_columns[col_idx].push_back(field_str);
                }
            }
        }

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "    ... parsed " << row_count / 1000000 << "M rows" << std::endl;
        }

        line_start = line_end + 1;
    }

    std::cout << "  Total: " << row_count << " rows" << std::endl;

    // Create output directory
    std::filesystem::create_directories(gendb_dir);
    std::string table_dir = gendb_dir + "/" + schema.table_name;
    std::filesystem::create_directories(table_dir);

    // Write columns directly (no sorting for now - use zone maps for filtering)
    std::cout << "  Writing column files" << std::endl;

    for (size_t col_idx = 0; col_idx < schema.columns.size(); ++col_idx) {
        const Column& col = schema.columns[col_idx];
        std::string col_file = table_dir + "/" + col.name + ".bin";
        std::ofstream out(col_file, std::ios::binary);

        if (col.semantic_type == "INTEGER" || col.semantic_type == "DATE") {
            auto& col_data = int_columns[col_idx];
            for (int32_t val : col_data) {
                out.write((char*)&val, sizeof(int32_t));
            }
        } else if (col.semantic_type == "DECIMAL") {
            auto& col_data = double_columns[col_idx];
            for (double val : col_data) {
                out.write((char*)&val, sizeof(double));
            }
        } else if (col.semantic_type == "STRING") {
            if (col.encoding == "dictionary") {
                auto& col_data = dict_columns[col_idx];
                for (uint8_t code : col_data) {
                    out.write((char*)&code, sizeof(uint8_t));
                }
                std::string dict_file = col_file + ".dict";
                dict_encoders[col_idx].save(dict_file);
            } else {
                auto& col_data = string_columns[col_idx];
                for (const auto& str : col_data) {
                    uint32_t len = str.length();
                    out.write((char*)&len, sizeof(uint32_t));
                    out.write(str.data(), len);
                }
            }
        }
    }

    // Write metadata
    std::string metadata_file = table_dir + "/metadata.txt";
    std::ofstream meta_out(metadata_file);
    meta_out << "table_name=" << schema.table_name << "\n";
    meta_out << "row_count=" << row_count << "\n";
    meta_out << "columns=\n";

    for (size_t i = 0; i < schema.columns.size(); ++i) {
        const Column& col = schema.columns[i];
        meta_out << "  name=" << col.name << " cpp_type=" << col.cpp_type
                << " semantic_type=" << col.semantic_type << " encoding=" << col.encoding;

        if (col.semantic_type == "INTEGER" || col.semantic_type == "DATE") {
            auto& col_data = int_columns[i];
            if (!col_data.empty()) {
                int32_t min_val = *std::min_element(col_data.begin(), col_data.end());
                int32_t max_val = *std::max_element(col_data.begin(), col_data.end());
                meta_out << " min=" << min_val << " max=" << max_val;
            }
        } else if (col.semantic_type == "DECIMAL") {
            auto& col_data = double_columns[i];
            if (!col_data.empty()) {
                double min_val = *std::min_element(col_data.begin(), col_data.end());
                double max_val = *std::max_element(col_data.begin(), col_data.end());
                meta_out << " min=" << min_val << " max=" << max_val;
            }
        }
        meta_out << "\n";
    }

    munmap(file_data, file_size);
    close(fd);

    std::cout << "✓ " << schema.table_name << " ingested successfully" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    auto start_time = std::chrono::high_resolution_clock::now();

    std::vector<TableSchema> schemas = {
        {
            "lineitem",
            {
                {"l_orderkey", "int32_t", "INTEGER", "none"},
                {"l_partkey", "int32_t", "INTEGER", "none"},
                {"l_suppkey", "int32_t", "INTEGER", "none"},
                {"l_linenumber", "int32_t", "INTEGER", "none"},
                {"l_quantity", "double", "DECIMAL", "none"},
                {"l_extendedprice", "double", "DECIMAL", "none"},
                {"l_discount", "double", "DECIMAL", "none"},
                {"l_tax", "double", "DECIMAL", "none"},
                {"l_returnflag", "uint8_t", "STRING", "dictionary"},
                {"l_linestatus", "uint8_t", "STRING", "dictionary"},
                {"l_shipdate", "int32_t", "DATE", "none"},
                {"l_commitdate", "int32_t", "DATE", "none"},
                {"l_receiptdate", "int32_t", "DATE", "none"},
                {"l_shipinstruct", "std::string", "STRING", "none"},
                {"l_shipmode", "std::string", "STRING", "none"},
                {"l_comment", "std::string", "STRING", "none"}
            },
            {},
            "lineitem.tbl",
            '|',
            "",
            100000
        },
        {
            "orders",
            {
                {"o_orderkey", "int32_t", "INTEGER", "none"},
                {"o_custkey", "int32_t", "INTEGER", "none"},
                {"o_orderstatus", "uint8_t", "STRING", "dictionary"},
                {"o_totalprice", "double", "DECIMAL", "none"},
                {"o_orderdate", "int32_t", "DATE", "none"},
                {"o_orderpriority", "std::string", "STRING", "none"},
                {"o_clerk", "std::string", "STRING", "none"},
                {"o_shippriority", "int32_t", "INTEGER", "none"},
                {"o_comment", "std::string", "STRING", "none"}
            },
            {},
            "orders.tbl",
            '|',
            "",
            100000
        },
        {
            "customer",
            {
                {"c_custkey", "int32_t", "INTEGER", "none"},
                {"c_name", "std::string", "STRING", "none"},
                {"c_address", "std::string", "STRING", "none"},
                {"c_nationkey", "int32_t", "INTEGER", "none"},
                {"c_phone", "std::string", "STRING", "none"},
                {"c_acctbal", "double", "DECIMAL", "none"},
                {"c_mktsegment", "uint8_t", "STRING", "dictionary"},
                {"c_comment", "std::string", "STRING", "none"}
            },
            {},
            "customer.tbl",
            '|',
            "",
            100000
        },
        {
            "part",
            {
                {"p_partkey", "int32_t", "INTEGER", "none"},
                {"p_name", "std::string", "STRING", "none"},
                {"p_mfgr", "std::string", "STRING", "none"},
                {"p_brand", "std::string", "STRING", "none"},
                {"p_type", "std::string", "STRING", "none"},
                {"p_size", "int32_t", "INTEGER", "none"},
                {"p_container", "std::string", "STRING", "none"},
                {"p_retailprice", "double", "DECIMAL", "none"},
                {"p_comment", "std::string", "STRING", "none"}
            },
            {},
            "part.tbl",
            '|',
            "",
            100000
        },
        {
            "supplier",
            {
                {"s_suppkey", "int32_t", "INTEGER", "none"},
                {"s_name", "std::string", "STRING", "none"},
                {"s_address", "std::string", "STRING", "none"},
                {"s_nationkey", "int32_t", "INTEGER", "none"},
                {"s_phone", "std::string", "STRING", "none"},
                {"s_acctbal", "double", "DECIMAL", "none"},
                {"s_comment", "std::string", "STRING", "none"}
            },
            {},
            "supplier.tbl",
            '|',
            "",
            100000
        },
        {
            "partsupp",
            {
                {"ps_partkey", "int32_t", "INTEGER", "none"},
                {"ps_suppkey", "int32_t", "INTEGER", "none"},
                {"ps_availqty", "int32_t", "INTEGER", "none"},
                {"ps_supplycost", "double", "DECIMAL", "none"},
                {"ps_comment", "std::string", "STRING", "none"}
            },
            {},
            "partsupp.tbl",
            '|',
            "",
            100000
        },
        {
            "nation",
            {
                {"n_nationkey", "int32_t", "INTEGER", "none"},
                {"n_name", "std::string", "STRING", "none"},
                {"n_regionkey", "int32_t", "INTEGER", "none"},
                {"n_comment", "std::string", "STRING", "none"}
            },
            {},
            "nation.tbl",
            '|',
            "",
            100000
        },
        {
            "region",
            {
                {"r_regionkey", "int32_t", "INTEGER", "none"},
                {"r_name", "std::string", "STRING", "none"},
                {"r_comment", "std::string", "STRING", "none"}
            },
            {},
            "region.tbl",
            '|',
            "",
            100000
        }
    };

    for (const auto& schema : schemas) {
        ingest_table(data_dir, gendb_dir, schema);
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);

    std::cout << "\n✓ All tables ingested in " << duration.count() << " seconds" << std::endl;

    return 0;
}
