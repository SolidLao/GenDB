#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <cmath>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <chrono>

// Date parsing: days since epoch 1970-01-01
int32_t parse_date(const std::string& date_str) {
    if (date_str.empty()) return 0;
    int year, month, day;
    sscanf(date_str.c_str(), "%d-%d-%d", &year, &month, &day);

    // Days since 1970-01-01
    int days = 0;

    // Count leap years from 1970 to year-1
    for (int y = 1970; y < year; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Days in complete months of current year
    const int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool is_leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));

    for (int m = 1; m < month; m++) {
        days += month_days[m - 1];
        if (m == 2 && is_leap) days += 1;
    }

    // Add days in current month (subtract 1 because day is 1-indexed)
    days += day - 1;

    return days;
}

// Decimal parser: scale to int64_t with scale_factor 100
int64_t parse_decimal(const std::string& s) {
    if (s.empty()) return 0;
    double val;
    sscanf(s.c_str(), "%lf", &val);
    return static_cast<int64_t>(std::round(val * 100.0));
}

struct TableWriter {
    std::string table_name;
    std::string base_dir;
    std::unordered_map<std::string, int32_t> dict_map;  // string -> code
    std::map<int32_t, std::string> reverse_dict;        // code -> string (for later writing)
    int32_t next_dict_code = 0;
    std::mutex dict_mutex;

    // Column buffers
    std::vector<int32_t> col_int32;
    std::vector<int64_t> col_int64;
    std::vector<std::string> col_string;
    std::vector<int32_t> col_dict_codes;

    // Column types: 0=int32, 1=int64, 2=string, 3=dict_encoded
    std::vector<int> col_types;
    std::vector<std::string> col_names;

    TableWriter(const std::string& tbl_name, const std::string& bd)
        : table_name(tbl_name), base_dir(bd) {}

    // Add column definition
    void add_column(const std::string& name, int type) {
        col_names.push_back(name);
        col_types.push_back(type);
        if (type == 0) col_int32.reserve(1000000);
        else if (type == 1) col_int64.reserve(1000000);
        else if (type == 2) col_string.reserve(1000000);
        else if (type == 3) col_dict_codes.reserve(1000000);
    }

    // Add int32 value
    void add_int32(const std::string& s) {
        col_int32.push_back(std::stoi(s));
    }

    // Add int64 (DECIMAL)
    void add_decimal(const std::string& s) {
        col_int64.push_back(parse_decimal(s));
    }

    // Add date
    void add_date(const std::string& s) {
        col_int32.push_back(parse_date(s));
    }

    // Add string (direct)
    void add_string(const std::string& s) {
        col_string.push_back(s);
    }

    // Add dict-encoded string
    void add_dict_string(const std::string& s) {
        std::lock_guard<std::mutex> lock(dict_mutex);
        if (dict_map.find(s) == dict_map.end()) {
            dict_map[s] = next_dict_code;
            reverse_dict[next_dict_code] = s;
            next_dict_code++;
        }
        col_dict_codes.push_back(dict_map[s]);
    }

    // Write columns to disk
    void write_columns() {
        std::string table_dir = base_dir + "/" + table_name;
        mkdir(table_dir.c_str(), 0755);

        // Collect all values in order from all column buffers
        size_t total_rows = 0;
        if (!col_int32.empty()) total_rows = col_int32.size();
        else if (!col_int64.empty()) total_rows = col_int64.size();
        else if (!col_string.empty()) total_rows = col_string.size();
        else if (!col_dict_codes.empty()) total_rows = col_dict_codes.size();

        // Write each column buffer to its own file
        for (size_t i = 0; i < col_names.size(); i++) {
            const std::string& col_name = col_names[i];
            int col_type = col_types[i];
            std::string file_path = table_dir + "/" + col_name + ".bin";

            std::ofstream out(file_path, std::ios::binary);
            if (!out) {
                std::cerr << "Failed to open " << file_path << std::endl;
                continue;
            }

            if (col_type == 0) {
                // int32 column
                for (int32_t v : col_int32) {
                    out.write(reinterpret_cast<const char*>(&v), sizeof(v));
                }
            } else if (col_type == 1) {
                // int64 column
                for (int64_t v : col_int64) {
                    out.write(reinterpret_cast<const char*>(&v), sizeof(v));
                }
            } else if (col_type == 2) {
                // string column: write length + string
                for (const auto& s : col_string) {
                    uint32_t len = s.length();
                    out.write(reinterpret_cast<const char*>(&len), sizeof(len));
                    out.write(s.c_str(), len);
                }
            } else if (col_type == 3) {
                // dict-encoded: write int32 codes
                for (int32_t code : col_dict_codes) {
                    out.write(reinterpret_cast<const char*>(&code), sizeof(code));
                }
                // Write dictionary
                std::string dict_path = table_dir + "/" + col_name + "_dict.txt";
                std::ofstream dict_out(dict_path);
                for (const auto& [code, val] : reverse_dict) {
                    dict_out << code << "=" << val << "\n";
                }
                dict_out.close();
            }
            out.close();
        }
    }
};

void parse_lineitem(const std::string& file_path, const std::string& base_dir) {
    std::cout << "Parsing lineitem..." << std::endl;
    TableWriter writer("lineitem", base_dir);

    // Add columns: l_orderkey(int32), l_partkey(int32), l_suppkey(int32), l_linenumber(int32),
    // l_quantity(int64), l_extendedprice(int64), l_discount(int64), l_tax(int64),
    // l_returnflag(dict), l_linestatus(dict), l_shipdate(int32), l_commitdate(int32),
    // l_receiptdate(int32), l_shipinstruct(dict), l_shipmode(dict), l_comment(string)

    writer.add_column("l_orderkey", 0);
    writer.add_column("l_partkey", 0);
    writer.add_column("l_suppkey", 0);
    writer.add_column("l_linenumber", 0);
    writer.add_column("l_quantity", 1);
    writer.add_column("l_extendedprice", 1);
    writer.add_column("l_discount", 1);
    writer.add_column("l_tax", 1);
    writer.add_column("l_returnflag", 3);
    writer.add_column("l_linestatus", 3);
    writer.add_column("l_shipdate", 0);
    writer.add_column("l_commitdate", 0);
    writer.add_column("l_receiptdate", 0);
    writer.add_column("l_shipinstruct", 3);
    writer.add_column("l_shipmode", 3);
    writer.add_column("l_comment", 2);

    std::ifstream in(file_path);
    std::string line;
    int row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::replace(line.begin(), line.end(), '|', '\t');
        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '\t'); writer.add_int32(field);           // l_orderkey
        std::getline(iss, field, '\t'); writer.add_int32(field);           // l_partkey
        std::getline(iss, field, '\t'); writer.add_int32(field);           // l_suppkey
        std::getline(iss, field, '\t'); writer.add_int32(field);           // l_linenumber
        std::getline(iss, field, '\t'); writer.add_decimal(field);         // l_quantity
        std::getline(iss, field, '\t'); writer.add_decimal(field);         // l_extendedprice
        std::getline(iss, field, '\t'); writer.add_decimal(field);         // l_discount
        std::getline(iss, field, '\t'); writer.add_decimal(field);         // l_tax
        std::getline(iss, field, '\t'); writer.add_dict_string(field);     // l_returnflag
        std::getline(iss, field, '\t'); writer.add_dict_string(field);     // l_linestatus
        std::getline(iss, field, '\t'); writer.add_date(field);            // l_shipdate
        std::getline(iss, field, '\t'); writer.add_date(field);            // l_commitdate
        std::getline(iss, field, '\t'); writer.add_date(field);            // l_receiptdate
        std::getline(iss, field, '\t'); writer.add_dict_string(field);     // l_shipinstruct
        std::getline(iss, field, '\t'); writer.add_dict_string(field);     // l_shipmode
        std::getline(iss, field, '\t'); writer.add_string(field);          // l_comment

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "  Processed " << row_count << " rows..." << std::endl;
        }
    }
    in.close();

    std::cout << "  Writing columns..." << std::endl;
    writer.write_columns();
    std::cout << "  Lineitem: " << row_count << " rows" << std::endl;
}

void parse_orders(const std::string& file_path, const std::string& base_dir) {
    std::cout << "Parsing orders..." << std::endl;
    TableWriter writer("orders", base_dir);

    writer.add_column("o_orderkey", 0);
    writer.add_column("o_custkey", 0);
    writer.add_column("o_orderstatus", 3);
    writer.add_column("o_totalprice", 1);
    writer.add_column("o_orderdate", 0);
    writer.add_column("o_orderpriority", 3);
    writer.add_column("o_clerk", 3);
    writer.add_column("o_shippriority", 0);
    writer.add_column("o_comment", 2);

    std::ifstream in(file_path);
    std::string line;
    int row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::replace(line.begin(), line.end(), '|', '\t');
        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '\t'); writer.add_int32(field);
        std::getline(iss, field, '\t'); writer.add_int32(field);
        std::getline(iss, field, '\t'); writer.add_dict_string(field);
        std::getline(iss, field, '\t'); writer.add_decimal(field);
        std::getline(iss, field, '\t'); writer.add_date(field);
        std::getline(iss, field, '\t'); writer.add_dict_string(field);
        std::getline(iss, field, '\t'); writer.add_dict_string(field);
        std::getline(iss, field, '\t'); writer.add_int32(field);
        std::getline(iss, field, '\t'); writer.add_string(field);

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "  Processed " << row_count << " rows..." << std::endl;
        }
    }
    in.close();

    writer.write_columns();
    std::cout << "  Orders: " << row_count << " rows" << std::endl;
}

void parse_customer(const std::string& file_path, const std::string& base_dir) {
    std::cout << "Parsing customer..." << std::endl;
    TableWriter writer("customer", base_dir);

    writer.add_column("c_custkey", 0);
    writer.add_column("c_name", 2);
    writer.add_column("c_address", 2);
    writer.add_column("c_nationkey", 0);
    writer.add_column("c_phone", 3);
    writer.add_column("c_acctbal", 1);
    writer.add_column("c_mktsegment", 3);
    writer.add_column("c_comment", 2);

    std::ifstream in(file_path);
    std::string line;
    int row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::replace(line.begin(), line.end(), '|', '\t');
        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '\t'); writer.add_int32(field);
        std::getline(iss, field, '\t'); writer.add_string(field);
        std::getline(iss, field, '\t'); writer.add_string(field);
        std::getline(iss, field, '\t'); writer.add_int32(field);
        std::getline(iss, field, '\t'); writer.add_dict_string(field);
        std::getline(iss, field, '\t'); writer.add_decimal(field);
        std::getline(iss, field, '\t'); writer.add_dict_string(field);
        std::getline(iss, field, '\t'); writer.add_string(field);

        row_count++;
        if (row_count % 500000 == 0) {
            std::cout << "  Processed " << row_count << " rows..." << std::endl;
        }
    }
    in.close();

    writer.write_columns();
    std::cout << "  Customer: " << row_count << " rows" << std::endl;
}

void parse_part(const std::string& file_path, const std::string& base_dir) {
    std::cout << "Parsing part..." << std::endl;
    TableWriter writer("part", base_dir);

    writer.add_column("p_partkey", 0);
    writer.add_column("p_name", 2);
    writer.add_column("p_mfgr", 3);
    writer.add_column("p_brand", 3);
    writer.add_column("p_type", 3);
    writer.add_column("p_size", 0);
    writer.add_column("p_container", 3);
    writer.add_column("p_retailprice", 1);
    writer.add_column("p_comment", 2);

    std::ifstream in(file_path);
    std::string line;
    int row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::replace(line.begin(), line.end(), '|', '\t');
        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '\t'); writer.add_int32(field);
        std::getline(iss, field, '\t'); writer.add_string(field);
        std::getline(iss, field, '\t'); writer.add_dict_string(field);
        std::getline(iss, field, '\t'); writer.add_dict_string(field);
        std::getline(iss, field, '\t'); writer.add_dict_string(field);
        std::getline(iss, field, '\t'); writer.add_int32(field);
        std::getline(iss, field, '\t'); writer.add_dict_string(field);
        std::getline(iss, field, '\t'); writer.add_decimal(field);
        std::getline(iss, field, '\t'); writer.add_string(field);

        row_count++;
        if (row_count % 500000 == 0) {
            std::cout << "  Processed " << row_count << " rows..." << std::endl;
        }
    }
    in.close();

    writer.write_columns();
    std::cout << "  Part: " << row_count << " rows" << std::endl;
}

void parse_partsupp(const std::string& file_path, const std::string& base_dir) {
    std::cout << "Parsing partsupp..." << std::endl;
    TableWriter writer("partsupp", base_dir);

    writer.add_column("ps_partkey", 0);
    writer.add_column("ps_suppkey", 0);
    writer.add_column("ps_availqty", 0);
    writer.add_column("ps_supplycost", 1);
    writer.add_column("ps_comment", 2);

    std::ifstream in(file_path);
    std::string line;
    int row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::replace(line.begin(), line.end(), '|', '\t');
        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '\t'); writer.add_int32(field);
        std::getline(iss, field, '\t'); writer.add_int32(field);
        std::getline(iss, field, '\t'); writer.add_int32(field);
        std::getline(iss, field, '\t'); writer.add_decimal(field);
        std::getline(iss, field, '\t'); writer.add_string(field);

        row_count++;
        if (row_count % 500000 == 0) {
            std::cout << "  Processed " << row_count << " rows..." << std::endl;
        }
    }
    in.close();

    writer.write_columns();
    std::cout << "  Partsupp: " << row_count << " rows" << std::endl;
}

void parse_supplier(const std::string& file_path, const std::string& base_dir) {
    std::cout << "Parsing supplier..." << std::endl;
    TableWriter writer("supplier", base_dir);

    writer.add_column("s_suppkey", 0);
    writer.add_column("s_name", 2);
    writer.add_column("s_address", 2);
    writer.add_column("s_nationkey", 0);
    writer.add_column("s_phone", 3);
    writer.add_column("s_acctbal", 1);
    writer.add_column("s_comment", 2);

    std::ifstream in(file_path);
    std::string line;
    int row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::replace(line.begin(), line.end(), '|', '\t');
        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '\t'); writer.add_int32(field);
        std::getline(iss, field, '\t'); writer.add_string(field);
        std::getline(iss, field, '\t'); writer.add_string(field);
        std::getline(iss, field, '\t'); writer.add_int32(field);
        std::getline(iss, field, '\t'); writer.add_dict_string(field);
        std::getline(iss, field, '\t'); writer.add_decimal(field);
        std::getline(iss, field, '\t'); writer.add_string(field);

        row_count++;
        if (row_count % 10000 == 0) {
            std::cout << "  Processed " << row_count << " rows..." << std::endl;
        }
    }
    in.close();

    writer.write_columns();
    std::cout << "  Supplier: " << row_count << " rows" << std::endl;
}

void parse_nation(const std::string& file_path, const std::string& base_dir) {
    std::cout << "Parsing nation..." << std::endl;
    TableWriter writer("nation", base_dir);

    writer.add_column("n_nationkey", 0);
    writer.add_column("n_name", 2);
    writer.add_column("n_regionkey", 0);
    writer.add_column("n_comment", 2);

    std::ifstream in(file_path);
    std::string line;
    int row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::replace(line.begin(), line.end(), '|', '\t');
        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '\t'); writer.add_int32(field);
        std::getline(iss, field, '\t'); writer.add_string(field);
        std::getline(iss, field, '\t'); writer.add_int32(field);
        std::getline(iss, field, '\t'); writer.add_string(field);

        row_count++;
    }
    in.close();

    writer.write_columns();
    std::cout << "  Nation: " << row_count << " rows" << std::endl;
}

void parse_region(const std::string& file_path, const std::string& base_dir) {
    std::cout << "Parsing region..." << std::endl;
    TableWriter writer("region", base_dir);

    writer.add_column("r_regionkey", 0);
    writer.add_column("r_name", 2);
    writer.add_column("r_comment", 2);

    std::ifstream in(file_path);
    std::string line;
    int row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::replace(line.begin(), line.end(), '|', '\t');
        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '\t'); writer.add_int32(field);
        std::getline(iss, field, '\t'); writer.add_string(field);
        std::getline(iss, field, '\t'); writer.add_string(field);

        row_count++;
    }
    in.close();

    writer.write_columns();
    std::cout << "  Region: " << row_count << " rows" << std::endl;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: ingest <data_dir> <output_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string base_dir = argv[2];

    mkdir(base_dir.c_str(), 0755);

    auto start_time = std::chrono::high_resolution_clock::now();

    parse_lineitem(data_dir + "/lineitem.tbl", base_dir);
    parse_orders(data_dir + "/orders.tbl", base_dir);
    parse_customer(data_dir + "/customer.tbl", base_dir);
    parse_part(data_dir + "/part.tbl", base_dir);
    parse_partsupp(data_dir + "/partsupp.tbl", base_dir);
    parse_supplier(data_dir + "/supplier.tbl", base_dir);
    parse_nation(data_dir + "/nation.tbl", base_dir);
    parse_region(data_dir + "/region.tbl", base_dir);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);

    std::cout << "Ingestion completed in " << duration.count() << " seconds" << std::endl;

    return 0;
}
