#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <thread>
#include <mutex>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <cassert>
#include <cmath>

namespace fs = std::filesystem;

// Date parsing: days since 1970-01-01
int32_t parse_date(const std::string& date_str) {
    if (date_str.empty() || date_str.length() < 10) return 0;

    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    int days = 0;

    // Days from complete years since 1970
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Days from complete months in current year
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
    if (leap) days_in_month[1] = 29;

    for (int m = 1; m < month; ++m) {
        days += days_in_month[m - 1];
    }

    days += (day - 1);
    return days;
}

// Decimal parsing: value * scale_factor
int64_t parse_decimal(const std::string& dec_str, int scale_factor) {
    if (dec_str.empty()) return 0;
    double val = std::stod(dec_str);
    return static_cast<int64_t>(std::round(val * scale_factor));
}

struct DictionaryBuilder {
    std::unordered_map<std::string, uint32_t> value_to_code;
    std::vector<std::string> code_to_value;
    uint32_t next_code = 0;

    uint32_t get_or_add(const std::string& value) {
        auto it = value_to_code.find(value);
        if (it != value_to_code.end()) {
            return it->second;
        }
        uint32_t code = next_code++;
        value_to_code[value] = code;
        code_to_value.push_back(value);
        return code;
    }

    void write_dict(const std::string& dict_path) {
        std::ofstream f(dict_path);
        for (uint32_t i = 0; i < code_to_value.size(); ++i) {
            f << i << "=" << code_to_value[i] << "\n";
        }
        f.close();
    }
};

struct ColumnInfo {
    std::string name;
    std::string cpp_type;
    std::string semantic_type;
    std::string encoding;
    int scale_factor;
};

struct TableSchema {
    std::string table_name;
    std::vector<ColumnInfo> columns;
    std::vector<std::string> sort_order;
    std::string delimiter;
    std::vector<std::string> column_order;
};

// Parse storage_design.json (simplified)
std::unordered_map<std::string, TableSchema> load_schema() {
    std::unordered_map<std::string, TableSchema> schemas;

    // Define schemas manually (could be JSON-loaded in production)
    TableSchema lineitem;
    lineitem.table_name = "lineitem";
    lineitem.delimiter = "|";
    lineitem.column_order = {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"};
    lineitem.columns = {
        {"l_orderkey", "int32_t", "INTEGER", "none", 1},
        {"l_partkey", "int32_t", "INTEGER", "none", 1},
        {"l_suppkey", "int32_t", "INTEGER", "none", 1},
        {"l_linenumber", "int32_t", "INTEGER", "none", 1},
        {"l_quantity", "int64_t", "DECIMAL", "none", 100},
        {"l_extendedprice", "int64_t", "DECIMAL", "none", 100},
        {"l_discount", "int64_t", "DECIMAL", "none", 100},
        {"l_tax", "int64_t", "DECIMAL", "none", 100},
        {"l_returnflag", "int32_t", "STRING", "dictionary", 1},
        {"l_linestatus", "int32_t", "STRING", "dictionary", 1},
        {"l_shipdate", "int32_t", "DATE", "none", 1},
        {"l_commitdate", "int32_t", "DATE", "none", 1},
        {"l_receiptdate", "int32_t", "DATE", "none", 1},
        {"l_shipinstruct", "int32_t", "STRING", "dictionary", 1},
        {"l_shipmode", "int32_t", "STRING", "dictionary", 1},
        {"l_comment", "int32_t", "STRING", "dictionary", 1}
    };
    lineitem.sort_order = {"l_shipdate", "l_orderkey"};
    schemas["lineitem"] = lineitem;

    TableSchema orders;
    orders.table_name = "orders";
    orders.delimiter = "|";
    orders.column_order = {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"};
    orders.columns = {
        {"o_orderkey", "int32_t", "INTEGER", "none", 1},
        {"o_custkey", "int32_t", "INTEGER", "none", 1},
        {"o_orderstatus", "int32_t", "STRING", "dictionary", 1},
        {"o_totalprice", "int64_t", "DECIMAL", "none", 100},
        {"o_orderdate", "int32_t", "DATE", "none", 1},
        {"o_orderpriority", "int32_t", "STRING", "dictionary", 1},
        {"o_clerk", "int32_t", "STRING", "dictionary", 1},
        {"o_shippriority", "int32_t", "INTEGER", "none", 1},
        {"o_comment", "int32_t", "STRING", "dictionary", 1}
    };
    orders.sort_order = {"o_orderdate", "o_custkey"};
    schemas["orders"] = orders;

    TableSchema customer;
    customer.table_name = "customer";
    customer.delimiter = "|";
    customer.column_order = {"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"};
    customer.columns = {
        {"c_custkey", "int32_t", "INTEGER", "none", 1},
        {"c_name", "int32_t", "STRING", "dictionary", 1},
        {"c_address", "int32_t", "STRING", "dictionary", 1},
        {"c_nationkey", "int32_t", "INTEGER", "none", 1},
        {"c_phone", "int32_t", "STRING", "dictionary", 1},
        {"c_acctbal", "int64_t", "DECIMAL", "none", 100},
        {"c_mktsegment", "int32_t", "STRING", "dictionary", 1},
        {"c_comment", "int32_t", "STRING", "dictionary", 1}
    };
    customer.sort_order = {"c_custkey"};
    schemas["customer"] = customer;

    TableSchema partsupp;
    partsupp.table_name = "partsupp";
    partsupp.delimiter = "|";
    partsupp.column_order = {"ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"};
    partsupp.columns = {
        {"ps_partkey", "int32_t", "INTEGER", "none", 1},
        {"ps_suppkey", "int32_t", "INTEGER", "none", 1},
        {"ps_availqty", "int32_t", "INTEGER", "none", 1},
        {"ps_supplycost", "int64_t", "DECIMAL", "none", 100},
        {"ps_comment", "int32_t", "STRING", "dictionary", 1}
    };
    partsupp.sort_order = {"ps_partkey", "ps_suppkey"};
    schemas["partsupp"] = partsupp;

    TableSchema part;
    part.table_name = "part";
    part.delimiter = "|";
    part.column_order = {"p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"};
    part.columns = {
        {"p_partkey", "int32_t", "INTEGER", "none", 1},
        {"p_name", "int32_t", "STRING", "dictionary", 1},
        {"p_mfgr", "int32_t", "STRING", "dictionary", 1},
        {"p_brand", "int32_t", "STRING", "dictionary", 1},
        {"p_type", "int32_t", "STRING", "dictionary", 1},
        {"p_size", "int32_t", "INTEGER", "none", 1},
        {"p_container", "int32_t", "STRING", "dictionary", 1},
        {"p_retailprice", "int64_t", "DECIMAL", "none", 100},
        {"p_comment", "int32_t", "STRING", "dictionary", 1}
    };
    part.sort_order = {"p_partkey"};
    schemas["part"] = part;

    TableSchema supplier;
    supplier.table_name = "supplier";
    supplier.delimiter = "|";
    supplier.column_order = {"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"};
    supplier.columns = {
        {"s_suppkey", "int32_t", "INTEGER", "none", 1},
        {"s_name", "int32_t", "STRING", "dictionary", 1},
        {"s_address", "int32_t", "STRING", "dictionary", 1},
        {"s_nationkey", "int32_t", "INTEGER", "none", 1},
        {"s_phone", "int32_t", "STRING", "dictionary", 1},
        {"s_acctbal", "int64_t", "DECIMAL", "none", 100},
        {"s_comment", "int32_t", "STRING", "dictionary", 1}
    };
    supplier.sort_order = {"s_suppkey"};
    schemas["supplier"] = supplier;

    TableSchema nation;
    nation.table_name = "nation";
    nation.delimiter = "|";
    nation.column_order = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
    nation.columns = {
        {"n_nationkey", "int32_t", "INTEGER", "none", 1},
        {"n_name", "int32_t", "STRING", "dictionary", 1},
        {"n_regionkey", "int32_t", "INTEGER", "none", 1},
        {"n_comment", "int32_t", "STRING", "dictionary", 1}
    };
    nation.sort_order = {};
    schemas["nation"] = nation;

    TableSchema region;
    region.table_name = "region";
    region.delimiter = "|";
    region.column_order = {"r_regionkey", "r_name", "r_comment"};
    region.columns = {
        {"r_regionkey", "int32_t", "INTEGER", "none", 1},
        {"r_name", "int32_t", "STRING", "dictionary", 1},
        {"r_comment", "int32_t", "STRING", "dictionary", 1}
    };
    region.sort_order = {};
    schemas["region"] = region;

    return schemas;
}

struct RowData {
    std::vector<std::vector<int32_t>> int32_cols;
    std::vector<std::vector<int64_t>> int64_cols;
    uint32_t row_idx = 0;
};

void ingest_table(const std::string& src_dir, const std::string& out_dir, const TableSchema& schema) {
    std::string input_file = src_dir + "/" + schema.table_name + ".tbl";
    std::cout << "Ingesting " << schema.table_name << " from " << input_file << std::endl;

    // Create output directory
    std::string table_dir = out_dir + "/" + schema.table_name;
    fs::create_directories(table_dir);

    // Initialize dictionaries and storage
    std::unordered_map<std::string, DictionaryBuilder> dict_builders;
    std::vector<std::vector<int32_t>> int32_data(schema.columns.size());
    std::vector<std::vector<int64_t>> int64_data(schema.columns.size());
    std::vector<DictionaryBuilder*> col_dict_ptrs(schema.columns.size(), nullptr);

    for (size_t i = 0; i < schema.columns.size(); ++i) {
        if (schema.columns[i].encoding == "dictionary") {
            dict_builders[schema.columns[i].name] = DictionaryBuilder();
            col_dict_ptrs[i] = &dict_builders[schema.columns[i].name];
        }
    }

    // Read and parse input file
    std::ifstream input(input_file);
    if (!input) {
        std::cerr << "Cannot open " << input_file << std::endl;
        return;
    }

    std::string line;
    uint32_t row_count = 0;
    while (std::getline(input, line)) {
        if (line.empty()) continue;

        // Split by delimiter
        std::vector<std::string> fields;
        size_t pos = 0, last = 0;
        while ((pos = line.find(schema.delimiter, last)) != std::string::npos) {
            fields.push_back(line.substr(last, pos - last));
            last = pos + schema.delimiter.length();
        }
        fields.push_back(line.substr(last));

        // Parse and store each column
        for (size_t col_idx = 0; col_idx < schema.columns.size() && col_idx < fields.size(); ++col_idx) {
            const auto& col = schema.columns[col_idx];
            const auto& value = fields[col_idx];

            if (col.cpp_type == "int32_t") {
                if (col.semantic_type == "DATE") {
                    int32_data[col_idx].push_back(parse_date(value));
                } else if (col.semantic_type == "STRING" && col.encoding == "dictionary") {
                    int32_data[col_idx].push_back(col_dict_ptrs[col_idx]->get_or_add(value));
                } else {
                    int32_data[col_idx].push_back(value.empty() ? 0 : std::stoi(value));
                }
            } else if (col.cpp_type == "int64_t") {
                int64_data[col_idx].push_back(parse_decimal(value, col.scale_factor));
            }
        }

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "  Parsed " << row_count << " rows..." << std::endl;
        }
    }
    input.close();

    std::cout << "  Total rows: " << row_count << std::endl;

    // Build permutation for sorting (if needed)
    std::vector<uint32_t> perm(row_count);
    for (uint32_t i = 0; i < row_count; ++i) perm[i] = i;

    if (!schema.sort_order.empty()) {
        std::cout << "  Sorting by: ";
        for (const auto& col : schema.sort_order) std::cout << col << " ";
        std::cout << std::endl;

        // Find sort column indices
        std::vector<size_t> sort_col_indices;
        for (const auto& sort_col : schema.sort_order) {
            for (size_t i = 0; i < schema.columns.size(); ++i) {
                if (schema.columns[i].name == sort_col) {
                    sort_col_indices.push_back(i);
                    break;
                }
            }
        }

        // Sort permutation
        std::sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b) {
            for (size_t sort_idx : sort_col_indices) {
                const auto& col = schema.columns[sort_idx];
                if (col.cpp_type == "int32_t") {
                    if (int32_data[sort_idx][a] != int32_data[sort_idx][b]) {
                        return int32_data[sort_idx][a] < int32_data[sort_idx][b];
                    }
                } else if (col.cpp_type == "int64_t") {
                    if (int64_data[sort_idx][a] != int64_data[sort_idx][b]) {
                        return int64_data[sort_idx][a] < int64_data[sort_idx][b];
                    }
                }
            }
            return false;
        });
    }

    // Write binary columnar files (permuted)
    for (size_t col_idx = 0; col_idx < schema.columns.size(); ++col_idx) {
        const auto& col = schema.columns[col_idx];
        std::string col_file = table_dir + "/" + col.name + ".bin";
        std::ofstream out(col_file, std::ios::binary);

        if (col.cpp_type == "int32_t") {
            for (uint32_t row_idx : perm) {
                int32_t val = int32_data[col_idx][row_idx];
                out.write(reinterpret_cast<const char*>(&val), sizeof(int32_t));
            }
        } else if (col.cpp_type == "int64_t") {
            for (uint32_t row_idx : perm) {
                int64_t val = int64_data[col_idx][row_idx];
                out.write(reinterpret_cast<const char*>(&val), sizeof(int64_t));
            }
        }
        out.close();
        std::cout << "  Written " << col_file << std::endl;
    }

    // Write dictionaries
    for (auto& [col_name, dict_builder] : dict_builders) {
        std::string dict_file = table_dir + "/" + col_name + "_dict.txt";
        dict_builder.write_dict(dict_file);
        std::cout << "  Written " << dict_file << " (" << dict_builder.next_code << " values)" << std::endl;
    }
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <src_dir> <out_dir>" << std::endl;
        return 1;
    }

    std::string src_dir = argv[1];
    std::string out_dir = argv[2];

    // Create output directory
    fs::create_directories(out_dir);

    auto schemas = load_schema();
    std::vector<std::string> table_order = {"nation", "region", "supplier", "customer", "part", "partsupp", "orders", "lineitem"};

    for (const auto& table_name : table_order) {
        if (schemas.count(table_name)) {
            ingest_table(src_dir, out_dir, schemas[table_name]);
        }
    }

    std::cout << "Ingestion complete." << std::endl;
    return 0;
}
