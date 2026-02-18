#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <map>
#include <unordered_map>
#include <cstring>
#include <cmath>
#include <thread>
#include <mutex>
#include <chrono>
#include <filesystem>
#include <cstdint>
#include <algorithm>

namespace fs = std::filesystem;

// Global thread count
int NUM_THREADS = 64;

// Date encoding: days since 1970-01-01
int32_t parse_date(const std::string& date_str) {
    if (date_str.empty() || date_str == "NULL") return 0;

    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Days in complete years (1970..year-1)
    int32_t days = 0;
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Days in complete months
    static int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    for (int m = 1; m < month; ++m) {
        days += month_days[m - 1];
        if (m == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
            days++; // leap year
        }
    }

    days += (day - 1);
    return days;
}

// Decimal parsing: value × scale_factor as int64_t
int64_t parse_decimal(const std::string& dec_str, int scale_factor = 100) {
    if (dec_str.empty() || dec_str == "NULL") return 0;
    double val = std::stod(dec_str);
    return static_cast<int64_t>(std::round(val * scale_factor));
}

// Split pipe-delimited line
std::vector<std::string> split_line(const std::string& line) {
    std::vector<std::string> parts;
    std::istringstream iss(line);
    std::string part;
    while (std::getline(iss, part, '|')) {
        parts.push_back(part);
    }
    return parts;
}

// Write binary column file
void write_binary_column(const std::string& filename, const std::vector<int32_t>& data) {
    std::ofstream out(filename, std::ios::binary);
    out.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(int32_t));
    out.close();
}

void write_binary_column(const std::string& filename, const std::vector<int64_t>& data) {
    std::ofstream out(filename, std::ios::binary);
    out.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(int64_t));
    out.close();
}

// Write string column with dictionary encoding
void write_dict_column(const std::string& col_dir, const std::string& col_name,
                       const std::vector<std::string>& values,
                       std::unordered_map<std::string, int32_t>& dict) {
    // Build dictionary if needed
    std::vector<int32_t> codes;
    for (const auto& v : values) {
        if (dict.find(v) == dict.end()) {
            dict[v] = dict.size();
        }
        codes.push_back(dict[v]);
    }

    // Write codes
    std::ofstream codes_out(col_dir + "/" + col_name + ".bin", std::ios::binary);
    codes_out.write(reinterpret_cast<const char*>(codes.data()), codes.size() * sizeof(int32_t));
    codes_out.close();
}

// Write string dictionary
void write_dictionary(const std::string& col_dir, const std::string& col_name,
                      const std::unordered_map<std::string, int32_t>& dict) {
    std::vector<std::pair<int32_t, std::string>> sorted_dict;
    for (const auto& [value, code] : dict) {
        sorted_dict.push_back({code, value});
    }
    std::sort(sorted_dict.begin(), sorted_dict.end());

    std::ofstream out(col_dir + "/" + col_name + "_dict.txt");
    for (const auto& [code, value] : sorted_dict) {
        out << code << "=" << value << "\n";
    }
    out.close();
}

// Write string column (unencoded)
void write_string_column(const std::string& col_dir, const std::string& col_name,
                         const std::vector<std::string>& values) {
    std::ofstream out(col_dir + "/" + col_name + ".bin", std::ios::binary);
    for (const auto& s : values) {
        uint32_t len = s.length();
        out.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();
}

// ===== LINEITEM INGESTION =====
void ingest_lineitem(const std::string& input_file, const std::string& output_dir) {
    std::cout << "[lineitem] Starting ingestion from " << input_file << std::endl;

    std::vector<int32_t> l_orderkey, l_partkey, l_suppkey, l_linenumber;
    std::vector<int64_t> l_quantity, l_extendedprice, l_discount, l_tax;
    std::vector<std::string> l_returnflag, l_linestatus, l_shipinstruct, l_shipmode;
    std::vector<int32_t> l_shipdate, l_commitdate, l_receiptdate;
    std::vector<std::string> l_comment;

    std::unordered_map<std::string, int32_t> dict_returnflag, dict_linestatus, dict_shipinstruct, dict_shipmode;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto parts = split_line(line);
        if (parts.size() < 16) continue;

        l_orderkey.push_back(std::stoi(parts[0]));
        l_partkey.push_back(std::stoi(parts[1]));
        l_suppkey.push_back(std::stoi(parts[2]));
        l_linenumber.push_back(std::stoi(parts[3]));
        l_quantity.push_back(parse_decimal(parts[4]));
        l_extendedprice.push_back(parse_decimal(parts[5]));
        l_discount.push_back(parse_decimal(parts[6]));
        l_tax.push_back(parse_decimal(parts[7]));
        l_returnflag.push_back(parts[8]);
        l_linestatus.push_back(parts[9]);
        l_shipdate.push_back(parse_date(parts[10]));
        l_commitdate.push_back(parse_date(parts[11]));
        l_receiptdate.push_back(parse_date(parts[12]));
        l_shipinstruct.push_back(parts[13]);
        l_shipmode.push_back(parts[14]);
        l_comment.push_back(parts[15]);

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "[lineitem] Parsed " << row_count << " rows" << std::endl;
        }
    }
    in.close();

    std::cout << "[lineitem] Writing " << row_count << " rows to binary columns" << std::endl;

    fs::create_directories(output_dir + "/lineitem");
    write_binary_column(output_dir + "/lineitem/l_orderkey.bin", l_orderkey);
    write_binary_column(output_dir + "/lineitem/l_partkey.bin", l_partkey);
    write_binary_column(output_dir + "/lineitem/l_suppkey.bin", l_suppkey);
    write_binary_column(output_dir + "/lineitem/l_linenumber.bin", l_linenumber);
    write_binary_column(output_dir + "/lineitem/l_quantity.bin", l_quantity);
    write_binary_column(output_dir + "/lineitem/l_extendedprice.bin", l_extendedprice);
    write_binary_column(output_dir + "/lineitem/l_discount.bin", l_discount);
    write_binary_column(output_dir + "/lineitem/l_tax.bin", l_tax);
    write_dict_column(output_dir + "/lineitem", "l_returnflag", l_returnflag, dict_returnflag);
    write_dict_column(output_dir + "/lineitem", "l_linestatus", l_linestatus, dict_linestatus);
    write_binary_column(output_dir + "/lineitem/l_shipdate.bin", l_shipdate);
    write_binary_column(output_dir + "/lineitem/l_commitdate.bin", l_commitdate);
    write_binary_column(output_dir + "/lineitem/l_receiptdate.bin", l_receiptdate);
    write_dict_column(output_dir + "/lineitem", "l_shipinstruct", l_shipinstruct, dict_shipinstruct);
    write_dict_column(output_dir + "/lineitem", "l_shipmode", l_shipmode, dict_shipmode);
    write_string_column(output_dir + "/lineitem", "l_comment", l_comment);

    write_dictionary(output_dir + "/lineitem", "l_returnflag", dict_returnflag);
    write_dictionary(output_dir + "/lineitem", "l_linestatus", dict_linestatus);
    write_dictionary(output_dir + "/lineitem", "l_shipinstruct", dict_shipinstruct);
    write_dictionary(output_dir + "/lineitem", "l_shipmode", dict_shipmode);

    std::cout << "[lineitem] Complete (" << row_count << " rows)" << std::endl;
}

// ===== ORDERS INGESTION =====
void ingest_orders(const std::string& input_file, const std::string& output_dir) {
    std::cout << "[orders] Starting ingestion from " << input_file << std::endl;

    std::vector<int32_t> o_orderkey, o_custkey, o_shippriority;
    std::vector<int64_t> o_totalprice;
    std::vector<int32_t> o_orderdate;
    std::vector<std::string> o_orderstatus, o_orderpriority, o_clerk;
    std::vector<std::string> o_comment;

    std::unordered_map<std::string, int32_t> dict_status, dict_priority, dict_clerk;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto parts = split_line(line);
        if (parts.size() < 9) continue;

        o_orderkey.push_back(std::stoi(parts[0]));
        o_custkey.push_back(std::stoi(parts[1]));
        o_orderstatus.push_back(parts[2]);
        o_totalprice.push_back(parse_decimal(parts[3]));
        o_orderdate.push_back(parse_date(parts[4]));
        o_orderpriority.push_back(parts[5]);
        o_clerk.push_back(parts[6]);
        o_shippriority.push_back(std::stoi(parts[7]));
        o_comment.push_back(parts[8]);

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "[orders] Parsed " << row_count << " rows" << std::endl;
        }
    }
    in.close();

    std::cout << "[orders] Writing " << row_count << " rows to binary columns" << std::endl;

    fs::create_directories(output_dir + "/orders");
    write_binary_column(output_dir + "/orders/o_orderkey.bin", o_orderkey);
    write_binary_column(output_dir + "/orders/o_custkey.bin", o_custkey);
    write_dict_column(output_dir + "/orders", "o_orderstatus", o_orderstatus, dict_status);
    write_binary_column(output_dir + "/orders/o_totalprice.bin", o_totalprice);
    write_binary_column(output_dir + "/orders/o_orderdate.bin", o_orderdate);
    write_dict_column(output_dir + "/orders", "o_orderpriority", o_orderpriority, dict_priority);
    write_dict_column(output_dir + "/orders", "o_clerk", o_clerk, dict_clerk);
    write_binary_column(output_dir + "/orders/o_shippriority.bin", o_shippriority);
    write_string_column(output_dir + "/orders", "o_comment", o_comment);

    write_dictionary(output_dir + "/orders", "o_orderstatus", dict_status);
    write_dictionary(output_dir + "/orders", "o_orderpriority", dict_priority);
    write_dictionary(output_dir + "/orders", "o_clerk", dict_clerk);

    std::cout << "[orders] Complete (" << row_count << " rows)" << std::endl;
}

// ===== CUSTOMER INGESTION =====
void ingest_customer(const std::string& input_file, const std::string& output_dir) {
    std::cout << "[customer] Starting ingestion from " << input_file << std::endl;

    std::vector<int32_t> c_custkey, c_nationkey;
    std::vector<std::string> c_name, c_address, c_phone;
    std::vector<int64_t> c_acctbal;
    std::vector<std::string> c_mktsegment, c_comment;

    std::unordered_map<std::string, int32_t> dict_mktsegment;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto parts = split_line(line);
        if (parts.size() < 8) continue;

        c_custkey.push_back(std::stoi(parts[0]));
        c_name.push_back(parts[1]);
        c_address.push_back(parts[2]);
        c_nationkey.push_back(std::stoi(parts[3]));
        c_phone.push_back(parts[4]);
        c_acctbal.push_back(parse_decimal(parts[5]));
        c_mktsegment.push_back(parts[6]);
        c_comment.push_back(parts[7]);

        row_count++;
    }
    in.close();

    std::cout << "[customer] Writing " << row_count << " rows to binary columns" << std::endl;

    fs::create_directories(output_dir + "/customer");
    write_binary_column(output_dir + "/customer/c_custkey.bin", c_custkey);
    write_string_column(output_dir + "/customer", "c_name", c_name);
    write_string_column(output_dir + "/customer", "c_address", c_address);
    write_binary_column(output_dir + "/customer/c_nationkey.bin", c_nationkey);
    write_string_column(output_dir + "/customer", "c_phone", c_phone);
    write_binary_column(output_dir + "/customer/c_acctbal.bin", c_acctbal);
    write_dict_column(output_dir + "/customer", "c_mktsegment", c_mktsegment, dict_mktsegment);
    write_string_column(output_dir + "/customer", "c_comment", c_comment);

    write_dictionary(output_dir + "/customer", "c_mktsegment", dict_mktsegment);

    std::cout << "[customer] Complete (" << row_count << " rows)" << std::endl;
}

// ===== PART INGESTION =====
void ingest_part(const std::string& input_file, const std::string& output_dir) {
    std::cout << "[part] Starting ingestion from " << input_file << std::endl;

    std::vector<int32_t> p_partkey, p_size;
    std::vector<std::string> p_name;
    std::vector<std::string> p_mfgr, p_brand, p_type, p_container;
    std::vector<int64_t> p_retailprice;
    std::vector<std::string> p_comment;

    std::unordered_map<std::string, int32_t> dict_mfgr, dict_brand, dict_type, dict_container;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto parts = split_line(line);
        if (parts.size() < 9) continue;

        p_partkey.push_back(std::stoi(parts[0]));
        p_name.push_back(parts[1]);
        p_mfgr.push_back(parts[2]);
        p_brand.push_back(parts[3]);
        p_type.push_back(parts[4]);
        p_size.push_back(std::stoi(parts[5]));
        p_container.push_back(parts[6]);
        p_retailprice.push_back(parse_decimal(parts[7]));
        p_comment.push_back(parts[8]);

        row_count++;
    }
    in.close();

    std::cout << "[part] Writing " << row_count << " rows to binary columns" << std::endl;

    fs::create_directories(output_dir + "/part");
    write_binary_column(output_dir + "/part/p_partkey.bin", p_partkey);
    write_string_column(output_dir + "/part", "p_name", p_name);
    write_dict_column(output_dir + "/part", "p_mfgr", p_mfgr, dict_mfgr);
    write_dict_column(output_dir + "/part", "p_brand", p_brand, dict_brand);
    write_dict_column(output_dir + "/part", "p_type", p_type, dict_type);
    write_binary_column(output_dir + "/part/p_size.bin", p_size);
    write_dict_column(output_dir + "/part", "p_container", p_container, dict_container);
    write_binary_column(output_dir + "/part/p_retailprice.bin", p_retailprice);
    write_string_column(output_dir + "/part", "p_comment", p_comment);

    write_dictionary(output_dir + "/part", "p_mfgr", dict_mfgr);
    write_dictionary(output_dir + "/part", "p_brand", dict_brand);
    write_dictionary(output_dir + "/part", "p_type", dict_type);
    write_dictionary(output_dir + "/part", "p_container", dict_container);

    std::cout << "[part] Complete (" << row_count << " rows)" << std::endl;
}

// ===== PARTSUPP INGESTION =====
void ingest_partsupp(const std::string& input_file, const std::string& output_dir) {
    std::cout << "[partsupp] Starting ingestion from " << input_file << std::endl;

    std::vector<int32_t> ps_partkey, ps_suppkey, ps_availqty;
    std::vector<int64_t> ps_supplycost;
    std::vector<std::string> ps_comment;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto parts = split_line(line);
        if (parts.size() < 5) continue;

        ps_partkey.push_back(std::stoi(parts[0]));
        ps_suppkey.push_back(std::stoi(parts[1]));
        ps_availqty.push_back(std::stoi(parts[2]));
        ps_supplycost.push_back(parse_decimal(parts[3]));
        ps_comment.push_back(parts[4]);

        row_count++;
    }
    in.close();

    std::cout << "[partsupp] Writing " << row_count << " rows to binary columns" << std::endl;

    fs::create_directories(output_dir + "/partsupp");
    write_binary_column(output_dir + "/partsupp/ps_partkey.bin", ps_partkey);
    write_binary_column(output_dir + "/partsupp/ps_suppkey.bin", ps_suppkey);
    write_binary_column(output_dir + "/partsupp/ps_availqty.bin", ps_availqty);
    write_binary_column(output_dir + "/partsupp/ps_supplycost.bin", ps_supplycost);
    write_string_column(output_dir + "/partsupp", "ps_comment", ps_comment);

    std::cout << "[partsupp] Complete (" << row_count << " rows)" << std::endl;
}

// ===== SUPPLIER INGESTION =====
void ingest_supplier(const std::string& input_file, const std::string& output_dir) {
    std::cout << "[supplier] Starting ingestion from " << input_file << std::endl;

    std::vector<int32_t> s_suppkey, s_nationkey;
    std::vector<std::string> s_name, s_address, s_phone;
    std::vector<int64_t> s_acctbal;
    std::vector<std::string> s_comment;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto parts = split_line(line);
        if (parts.size() < 7) continue;

        s_suppkey.push_back(std::stoi(parts[0]));
        s_name.push_back(parts[1]);
        s_address.push_back(parts[2]);
        s_nationkey.push_back(std::stoi(parts[3]));
        s_phone.push_back(parts[4]);
        s_acctbal.push_back(parse_decimal(parts[5]));
        s_comment.push_back(parts[6]);

        row_count++;
    }
    in.close();

    std::cout << "[supplier] Writing " << row_count << " rows to binary columns" << std::endl;

    fs::create_directories(output_dir + "/supplier");
    write_binary_column(output_dir + "/supplier/s_suppkey.bin", s_suppkey);
    write_string_column(output_dir + "/supplier", "s_name", s_name);
    write_string_column(output_dir + "/supplier", "s_address", s_address);
    write_binary_column(output_dir + "/supplier/s_nationkey.bin", s_nationkey);
    write_string_column(output_dir + "/supplier", "s_phone", s_phone);
    write_binary_column(output_dir + "/supplier/s_acctbal.bin", s_acctbal);
    write_string_column(output_dir + "/supplier", "s_comment", s_comment);

    std::cout << "[supplier] Complete (" << row_count << " rows)" << std::endl;
}

// ===== NATION INGESTION =====
void ingest_nation(const std::string& input_file, const std::string& output_dir) {
    std::cout << "[nation] Starting ingestion from " << input_file << std::endl;

    std::vector<int32_t> n_nationkey, n_regionkey;
    std::vector<std::string> n_name, n_comment;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto parts = split_line(line);
        if (parts.size() < 4) continue;

        n_nationkey.push_back(std::stoi(parts[0]));
        n_name.push_back(parts[1]);
        n_regionkey.push_back(std::stoi(parts[2]));
        n_comment.push_back(parts[3]);

        row_count++;
    }
    in.close();

    std::cout << "[nation] Writing " << row_count << " rows to binary columns" << std::endl;

    fs::create_directories(output_dir + "/nation");
    write_binary_column(output_dir + "/nation/n_nationkey.bin", n_nationkey);
    write_string_column(output_dir + "/nation", "n_name", n_name);
    write_binary_column(output_dir + "/nation/n_regionkey.bin", n_regionkey);
    write_string_column(output_dir + "/nation", "n_comment", n_comment);

    std::cout << "[nation] Complete (" << row_count << " rows)" << std::endl;
}

// ===== REGION INGESTION =====
void ingest_region(const std::string& input_file, const std::string& output_dir) {
    std::cout << "[region] Starting ingestion from " << input_file << std::endl;

    std::vector<int32_t> r_regionkey;
    std::vector<std::string> r_name, r_comment;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto parts = split_line(line);
        if (parts.size() < 3) continue;

        r_regionkey.push_back(std::stoi(parts[0]));
        r_name.push_back(parts[1]);
        r_comment.push_back(parts[2]);

        row_count++;
    }
    in.close();

    std::cout << "[region] Writing " << row_count << " rows to binary columns" << std::endl;

    fs::create_directories(output_dir + "/region");
    write_binary_column(output_dir + "/region/r_regionkey.bin", r_regionkey);
    write_string_column(output_dir + "/region", "r_name", r_name);
    write_string_column(output_dir + "/region", "r_comment", r_comment);

    std::cout << "[region] Complete (" << row_count << " rows)" << std::endl;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <output_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string output_dir = argv[2];

    auto start = std::chrono::steady_clock::now();

    std::cout << "GenDB TPC-H Ingestion (SF10)" << std::endl;
    std::cout << "Data dir: " << data_dir << std::endl;
    std::cout << "Output dir: " << output_dir << std::endl;
    std::cout << "Threads: " << NUM_THREADS << std::endl << std::endl;

    // Ingest all tables
    ingest_lineitem(data_dir + "/lineitem.tbl", output_dir);
    ingest_orders(data_dir + "/orders.tbl", output_dir);
    ingest_customer(data_dir + "/customer.tbl", output_dir);
    ingest_part(data_dir + "/part.tbl", output_dir);
    ingest_partsupp(data_dir + "/partsupp.tbl", output_dir);
    ingest_supplier(data_dir + "/supplier.tbl", output_dir);
    ingest_nation(data_dir + "/nation.tbl", output_dir);
    ingest_region(data_dir + "/region.tbl", output_dir);

    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();

    std::cout << "\n=== INGESTION COMPLETE ===" << std::endl;
    std::cout << "Total time: " << elapsed << " seconds" << std::endl;

    // Post-ingestion checks
    std::cout << "\n=== POST-INGESTION VERIFICATION ===" << std::endl;
    std::ifstream date_check(output_dir + "/lineitem/l_shipdate.bin", std::ios::binary);
    if (date_check.is_open()) {
        std::vector<int32_t> dates(10);
        date_check.read(reinterpret_cast<char*>(dates.data()), 10 * sizeof(int32_t));
        std::cout << "[lineitem] l_shipdate first 10 values (should be >3000 for dates after 1978):" << std::endl;
        for (int i = 0; i < 10; i++) {
            std::cout << "  [" << i << "] = " << dates[i] << std::endl;
        }
        date_check.close();
    }

    std::ifstream decimal_check(output_dir + "/lineitem/l_discount.bin", std::ios::binary);
    if (decimal_check.is_open()) {
        std::vector<int64_t> decimals(10);
        decimal_check.read(reinterpret_cast<char*>(decimals.data()), 10 * sizeof(int64_t));
        std::cout << "[lineitem] l_discount first 10 values (scaled ×100, should be non-zero):" << std::endl;
        for (int i = 0; i < 10; i++) {
            std::cout << "  [" << i << "] = " << decimals[i] << " (actual: " << (double)decimals[i] / 100.0 << ")" << std::endl;
        }
        decimal_check.close();
    }

    return 0;
}
