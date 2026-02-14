#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <numeric>
#include <thread>
#include <mutex>
#include <atomic>
#include <cstring>
#include <chrono>
#include <filesystem>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <charconv>

namespace fs = std::filesystem;

// ============================================================================
// Utilities
// ============================================================================

struct Date {
    int32_t days_since_epoch;

    static int32_t from_string(const std::string& s) {
        // Parse YYYY-MM-DD to days since 1970-01-01
        int year, month, day;
        sscanf(s.c_str(), "%d-%d-%d", &year, &month, &day);

        // Days since epoch (1970-01-01)
        int32_t days = 0;
        for (int y = 1970; y < year; ++y) {
            days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
        }
        const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        for (int m = 1; m < month; ++m) {
            days += days_in_month[m];
        }
        if (month > 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
            days++;
        }
        days += day - 1;
        return days;
    }
};

class DictionaryEncoder {
public:
    uint8_t encode(const std::string& s) {
        auto it = dict.find(s);
        if (it != dict.end()) {
            return it->second;
        }
        uint8_t code = dict.size();
        dict[s] = code;
        return code;
    }

    const std::map<std::string, uint8_t>& get_dict() const { return dict; }

private:
    std::map<std::string, uint8_t> dict;
};

// ============================================================================
// Column Data Structures
// ============================================================================

struct ColumnData {
    std::string name;
    std::string type; // "int32", "int64", "string", "uint8"
    std::vector<int32_t> int32_data;
    std::vector<int64_t> int64_data;
    std::vector<std::string> string_data;
    std::vector<uint8_t> uint8_data;
    DictionaryEncoder* encoder = nullptr;
};

// ============================================================================
// Table Schema Definition
// ============================================================================

struct TableSchema {
    std::string name;
    std::vector<std::pair<std::string, std::string>> columns; // (name, type)
    std::vector<int> sort_key_columns;
};

TableSchema get_lineitem_schema() {
    return {
        "lineitem",
        {
            {"l_orderkey", "int32"},
            {"l_partkey", "int32"},
            {"l_suppkey", "int32"},
            {"l_linenumber", "int32"},
            {"l_quantity", "int32"},
            {"l_extendedprice", "int64"},
            {"l_discount", "int32"},
            {"l_tax", "int32"},
            {"l_returnflag", "uint8"},
            {"l_linestatus", "uint8"},
            {"l_shipdate", "int32"},
            {"l_commitdate", "int32"},
            {"l_receiptdate", "int32"},
            {"l_shipinstruct", "uint8"},
            {"l_shipmode", "uint8"},
            {"l_comment", "string"}
        },
        {10} // sort by l_shipdate (column index 10)
    };
}

TableSchema get_orders_schema() {
    return {
        "orders",
        {
            {"o_orderkey", "int32"},
            {"o_custkey", "int32"},
            {"o_orderstatus", "uint8"},
            {"o_totalprice", "int64"},
            {"o_orderdate", "int32"},
            {"o_orderpriority", "uint8"},
            {"o_clerk", "string"},
            {"o_shippriority", "int32"},
            {"o_comment", "string"}
        },
        {4} // sort by o_orderdate (column index 4)
    };
}

TableSchema get_customer_schema() {
    return {
        "customer",
        {
            {"c_custkey", "int32"},
            {"c_name", "string"},
            {"c_address", "string"},
            {"c_nationkey", "int32"},
            {"c_phone", "string"},
            {"c_acctbal", "int64"},
            {"c_mktsegment", "uint8"},
            {"c_comment", "string"}
        },
        {0} // sort by c_custkey (column index 0)
    };
}

TableSchema get_partsupp_schema() {
    return {
        "partsupp",
        {
            {"ps_partkey", "int32"},
            {"ps_suppkey", "int32"},
            {"ps_availqty", "int32"},
            {"ps_supplycost", "int64"},
            {"ps_comment", "string"}
        },
        {0} // sort by ps_partkey
    };
}

TableSchema get_part_schema() {
    return {
        "part",
        {
            {"p_partkey", "int32"},
            {"p_name", "string"},
            {"p_mfgr", "string"},
            {"p_brand", "string"},
            {"p_type", "string"},
            {"p_size", "int32"},
            {"p_container", "string"},
            {"p_retailprice", "int64"},
            {"p_comment", "string"}
        },
        {} // no sort key
    };
}

TableSchema get_supplier_schema() {
    return {
        "supplier",
        {
            {"s_suppkey", "int32"},
            {"s_name", "string"},
            {"s_address", "string"},
            {"s_nationkey", "int32"},
            {"s_phone", "string"},
            {"s_acctbal", "int64"},
            {"s_comment", "string"}
        },
        {} // no sort key
    };
}

TableSchema get_nation_schema() {
    return {
        "nation",
        {
            {"n_nationkey", "int32"},
            {"n_name", "string"},
            {"n_regionkey", "int32"},
            {"n_comment", "string"}
        },
        {} // no sort key
    };
}

TableSchema get_region_schema() {
    return {
        "region",
        {
            {"r_regionkey", "int32"},
            {"r_name", "string"},
            {"r_comment", "string"}
        },
        {} // no sort key
    };
}

// ============================================================================
// Parsing & Data Ingestion
// ============================================================================

std::vector<std::string> split_line(const std::string& line) {
    std::vector<std::string> fields;
    std::stringstream ss(line);
    std::string field;
    while (std::getline(ss, field, '|')) {
        fields.push_back(field);
    }
    return fields;
}

void ingest_table(const std::string& table_name, const std::string& data_dir,
                  const std::string& output_dir, const TableSchema& schema) {
    std::cout << "Ingesting " << table_name << "..." << std::endl;

    std::string input_file = data_dir + "/" + table_name + ".tbl";
    if (!fs::exists(input_file)) {
        std::cerr << "File not found: " << input_file << std::endl;
        return;
    }

    // Initialize columns
    std::vector<ColumnData> columns(schema.columns.size());
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        columns[i].name = schema.columns[i].first;
        columns[i].type = schema.columns[i].second;
    }

    // Initialize encoders for dictionary columns
    std::map<size_t, DictionaryEncoder*> column_encoders;
    for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].type == "uint8") {
            columns[i].encoder = new DictionaryEncoder();
        }
    }

    // Read and parse file
    std::ifstream file(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = split_line(line);
        if (fields.size() != schema.columns.size()) {
            std::cerr << "Column count mismatch on row " << row_count << std::endl;
            continue;
        }

        for (size_t col = 0; col < schema.columns.size(); ++col) {
            const auto& field = fields[col];
            auto& col_data = columns[col];

            if (col_data.type == "int32") {
                int32_t val = 0;
                std::from_chars(field.data(), field.data() + field.size(), val);
                col_data.int32_data.push_back(val);
            } else if (col_data.type == "int64") {
                int64_t val = 0;
                std::from_chars(field.data(), field.data() + field.size(), val);
                col_data.int64_data.push_back(val);
            } else if (col_data.type == "string") {
                col_data.string_data.push_back(field);
            } else if (col_data.type == "uint8") {
                uint8_t code = col_data.encoder->encode(field);
                col_data.uint8_data.push_back(code);
            }
        }
        row_count++;
    }
    file.close();

    std::cout << "  Loaded " << row_count << " rows" << std::endl;

    // Sorting: build permutation for sort keys
    std::vector<size_t> permutation(row_count);
    std::iota(permutation.begin(), permutation.end(), 0);

    if (!schema.sort_key_columns.empty()) {
        int sort_col = schema.sort_key_columns[0];
        std::cout << "  Sorting by column " << sort_col << " (" << columns[sort_col].name << ")..." << std::endl;

        if (columns[sort_col].type == "int32") {
            std::sort(permutation.begin(), permutation.end(), [&](size_t a, size_t b) {
                return columns[sort_col].int32_data[a] < columns[sort_col].int32_data[b];
            });
        } else if (columns[sort_col].type == "int64") {
            std::sort(permutation.begin(), permutation.end(), [&](size_t a, size_t b) {
                return columns[sort_col].int64_data[a] < columns[sort_col].int64_data[b];
            });
        }
        std::cout << "  Sort complete" << std::endl;
    }

    // Write binary column files
    fs::create_directories(output_dir);
    for (size_t col = 0; col < columns.size(); ++col) {
        auto& col_data = columns[col];
        std::string col_file = output_dir + "/" + table_name + "_" + col_data.name + ".col";
        std::ofstream out(col_file, std::ios::binary);

        if (col_data.type == "int32") {
            for (size_t row : permutation) {
                int32_t val = col_data.int32_data[row];
                out.write(reinterpret_cast<char*>(&val), sizeof(int32_t));
            }
        } else if (col_data.type == "int64") {
            for (size_t row : permutation) {
                int64_t val = col_data.int64_data[row];
                out.write(reinterpret_cast<char*>(&val), sizeof(int64_t));
            }
        } else if (col_data.type == "uint8") {
            for (size_t row : permutation) {
                uint8_t val = col_data.uint8_data[row];
                out.write(reinterpret_cast<char*>(&val), sizeof(uint8_t));
            }
        } else if (col_data.type == "string") {
            // For strings, write lengths + data
            for (size_t row : permutation) {
                const auto& str = col_data.string_data[row];
                uint32_t len = str.length();
                out.write(reinterpret_cast<char*>(&len), sizeof(uint32_t));
                out.write(str.data(), str.length());
            }
        }
        out.close();
        std::cout << "  Written: " << col_file << " (" << row_count << " rows)" << std::endl;
    }

    // Write metadata
    std::string metadata_file = output_dir + "/" + table_name + "_metadata.txt";
    std::ofstream meta(metadata_file);
    meta << "table: " << table_name << "\n";
    meta << "rows: " << row_count << "\n";
    meta << "columns: " << schema.columns.size() << "\n";
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        meta << schema.columns[i].first << ":" << schema.columns[i].second << "\n";
    }

    // Write dictionary encodings
    for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].encoder) {
            meta << "dict:" << columns[i].name << ":";
            const auto& dict = columns[i].encoder->get_dict();
            for (const auto& [val, code] : dict) {
                meta << code << "=" << val << ";";
            }
            meta << "\n";
        }
    }
    meta.close();

    // Clean up encoders
    for (auto& col : columns) {
        if (col.encoder) delete col.encoder;
    }

    std::cout << "  Table " << table_name << " ingestion complete" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    std::cout << "GenDB Data Ingestion" << std::endl;
    std::cout << "Data dir: " << data_dir << std::endl;
    std::cout << "GenDB dir: " << gendb_dir << std::endl;

    auto start = std::chrono::high_resolution_clock::now();

    // Ingest all tables
    ingest_table("lineitem", data_dir, gendb_dir, get_lineitem_schema());
    ingest_table("orders", data_dir, gendb_dir, get_orders_schema());
    ingest_table("customer", data_dir, gendb_dir, get_customer_schema());
    ingest_table("partsupp", data_dir, gendb_dir, get_partsupp_schema());
    ingest_table("part", data_dir, gendb_dir, get_part_schema());
    ingest_table("supplier", data_dir, gendb_dir, get_supplier_schema());
    ingest_table("nation", data_dir, gendb_dir, get_nation_schema());
    ingest_table("region", data_dir, gendb_dir, get_region_schema());

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);

    std::cout << "\nIngestion complete in " << duration.count() << " seconds" << std::endl;

    return 0;
}
