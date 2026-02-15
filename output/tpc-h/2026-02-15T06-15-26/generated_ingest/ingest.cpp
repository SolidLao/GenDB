#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <cstring>
#include <thread>
#include <mutex>
#include <filesystem>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <cmath>
#include <charconv>
#include <map>
#include <unordered_map>

namespace fs = std::filesystem;

// Global thread pool
std::vector<std::thread> g_threads;
std::mutex g_output_mutex;

// Dictionary encoding state
std::unordered_map<std::string, uint8_t> g_dict_l_returnflag;
std::unordered_map<std::string, uint8_t> g_dict_l_linestatus;
std::unordered_map<std::string, uint8_t> g_dict_l_shipinstruct;
std::unordered_map<std::string, uint8_t> g_dict_l_shipmode;
std::unordered_map<std::string, uint8_t> g_dict_o_orderstatus;
std::unordered_map<std::string, uint8_t> g_dict_o_orderpriority;
std::unordered_map<std::string, uint8_t> g_dict_c_mktsegment;

// Dictionary value arrays
std::vector<std::string> g_dict_vals_l_returnflag;
std::vector<std::string> g_dict_vals_l_linestatus;
std::vector<std::string> g_dict_vals_l_shipinstruct;
std::vector<std::string> g_dict_vals_l_shipmode;
std::vector<std::string> g_dict_vals_o_orderstatus;
std::vector<std::string> g_dict_vals_o_orderpriority;
std::vector<std::string> g_dict_vals_c_mktsegment;

// Date parsing: YYYY-MM-DD -> days since 1970-01-01
int32_t parse_date(const std::string& s) {
    if (s.empty()) return 0;
    int year = std::stoi(s.substr(0, 4));
    int month = std::stoi(s.substr(5, 2));
    int day = std::stoi(s.substr(8, 2));

    // Days since epoch (1970-01-01)
    int days = 0;

    // Years contribution
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Months contribution
    static const int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    for (int m = 1; m < month; ++m) {
        days += days_in_month[m - 1];
        if (m == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
            days += 1;
        }
    }

    // Days contribution
    days += day;

    return days;
}

// DECIMAL parsing: "0.04" -> 4 (with scale=100)
int64_t parse_decimal(const std::string& s, int scale) {
    if (s.empty()) return 0;
    double val;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), val);
    if (ec != std::errc()) return 0;
    return static_cast<int64_t>(std::round(val * scale));
}

// Tokenize line by delimiter
std::vector<std::string> split_line(const std::string& line, char delim) {
    std::vector<std::string> tokens;
    std::istringstream iss(line);
    std::string token;
    while (std::getline(iss, token, delim)) {
        tokens.push_back(token);
    }
    return tokens;
}

// Ingest lineitem
void ingest_lineitem(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting lineitem..." << std::endl;

    std::string filepath = data_dir + "/lineitem.tbl";
    std::ifstream file(filepath);
    if (!file) {
        std::cerr << "Failed to open " << filepath << std::endl;
        return;
    }

    // Column vectors (SoA)
    std::vector<int32_t> l_orderkey;
    std::vector<int32_t> l_partkey;
    std::vector<int32_t> l_suppkey;
    std::vector<int32_t> l_linenumber;
    std::vector<int64_t> l_quantity;
    std::vector<int64_t> l_extendedprice;
    std::vector<int64_t> l_discount;
    std::vector<int64_t> l_tax;
    std::vector<uint8_t> l_returnflag_codes;
    std::vector<uint8_t> l_linestatus_codes;
    std::vector<int32_t> l_shipdate;
    std::vector<int32_t> l_commitdate;
    std::vector<int32_t> l_receiptdate;
    std::vector<uint8_t> l_shipinstruct_codes;
    std::vector<uint8_t> l_shipmode_codes;
    std::vector<std::string> l_comment;

    std::string line;
    while (std::getline(file, line)) {
        auto tokens = split_line(line, '|');
        if (tokens.size() < 16) continue;

        l_orderkey.push_back(std::stoi(tokens[0]));
        l_partkey.push_back(std::stoi(tokens[1]));
        l_suppkey.push_back(std::stoi(tokens[2]));
        l_linenumber.push_back(std::stoi(tokens[3]));
        l_quantity.push_back(parse_decimal(tokens[4], 100));
        l_extendedprice.push_back(parse_decimal(tokens[5], 100));
        l_discount.push_back(parse_decimal(tokens[6], 100));
        l_tax.push_back(parse_decimal(tokens[7], 100));

        // Dictionary-encode low-cardinality columns
        if (g_dict_l_returnflag.find(tokens[8]) == g_dict_l_returnflag.end()) {
            g_dict_l_returnflag[tokens[8]] = g_dict_vals_l_returnflag.size();
            g_dict_vals_l_returnflag.push_back(tokens[8]);
        }
        l_returnflag_codes.push_back(g_dict_l_returnflag[tokens[8]]);

        if (g_dict_l_linestatus.find(tokens[9]) == g_dict_l_linestatus.end()) {
            g_dict_l_linestatus[tokens[9]] = g_dict_vals_l_linestatus.size();
            g_dict_vals_l_linestatus.push_back(tokens[9]);
        }
        l_linestatus_codes.push_back(g_dict_l_linestatus[tokens[9]]);

        l_shipdate.push_back(parse_date(tokens[10]));
        l_commitdate.push_back(parse_date(tokens[11]));
        l_receiptdate.push_back(parse_date(tokens[12]));

        if (g_dict_l_shipinstruct.find(tokens[13]) == g_dict_l_shipinstruct.end()) {
            g_dict_l_shipinstruct[tokens[13]] = g_dict_vals_l_shipinstruct.size();
            g_dict_vals_l_shipinstruct.push_back(tokens[13]);
        }
        l_shipinstruct_codes.push_back(g_dict_l_shipinstruct[tokens[13]]);

        if (g_dict_l_shipmode.find(tokens[14]) == g_dict_l_shipmode.end()) {
            g_dict_l_shipmode[tokens[14]] = g_dict_vals_l_shipmode.size();
            g_dict_vals_l_shipmode.push_back(tokens[14]);
        }
        l_shipmode_codes.push_back(g_dict_l_shipmode[tokens[14]]);

        l_comment.push_back(tokens[15]);
    }
    file.close();

    // Write binary column files
    fs::create_directories(gendb_dir + "/lineitem");

    auto write_binary = [&](const std::string& col_name, const void* data, size_t count, size_t element_size) {
        std::ofstream out(gendb_dir + "/lineitem/" + col_name + ".bin", std::ios::binary);
        out.write(static_cast<const char*>(data), count * element_size);
        out.close();
    };

    write_binary("l_orderkey", l_orderkey.data(), l_orderkey.size(), sizeof(int32_t));
    write_binary("l_partkey", l_partkey.data(), l_partkey.size(), sizeof(int32_t));
    write_binary("l_suppkey", l_suppkey.data(), l_suppkey.size(), sizeof(int32_t));
    write_binary("l_linenumber", l_linenumber.data(), l_linenumber.size(), sizeof(int32_t));
    write_binary("l_quantity", l_quantity.data(), l_quantity.size(), sizeof(int64_t));
    write_binary("l_extendedprice", l_extendedprice.data(), l_extendedprice.size(), sizeof(int64_t));
    write_binary("l_discount", l_discount.data(), l_discount.size(), sizeof(int64_t));
    write_binary("l_tax", l_tax.data(), l_tax.size(), sizeof(int64_t));
    write_binary("l_returnflag", l_returnflag_codes.data(), l_returnflag_codes.size(), sizeof(uint8_t));
    write_binary("l_linestatus", l_linestatus_codes.data(), l_linestatus_codes.size(), sizeof(uint8_t));
    write_binary("l_shipdate", l_shipdate.data(), l_shipdate.size(), sizeof(int32_t));
    write_binary("l_commitdate", l_commitdate.data(), l_commitdate.size(), sizeof(int32_t));
    write_binary("l_receiptdate", l_receiptdate.data(), l_receiptdate.size(), sizeof(int32_t));
    write_binary("l_shipinstruct", l_shipinstruct_codes.data(), l_shipinstruct_codes.size(), sizeof(uint8_t));
    write_binary("l_shipmode", l_shipmode_codes.data(), l_shipmode_codes.size(), sizeof(uint8_t));

    // Write strings separately
    std::ofstream str_file(gendb_dir + "/lineitem/l_comment.strvec", std::ios::binary);
    uint64_t offset = 0;
    for (const auto& s : l_comment) {
        uint32_t len = s.size();
        str_file.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
        str_file.write(s.data(), s.size());
    }
    str_file.close();

    // Write dictionaries
    for (const auto& [val, code] : g_dict_l_returnflag) {
        std::ofstream dict_file(gendb_dir + "/lineitem/l_returnflag_dict.txt", std::ios::app);
        dict_file << code << "=" << val << "\n";
    }
    for (const auto& [val, code] : g_dict_l_linestatus) {
        std::ofstream dict_file(gendb_dir + "/lineitem/l_linestatus_dict.txt", std::ios::app);
        dict_file << code << "=" << val << "\n";
    }
    for (const auto& [val, code] : g_dict_l_shipinstruct) {
        std::ofstream dict_file(gendb_dir + "/lineitem/l_shipinstruct_dict.txt", std::ios::app);
        dict_file << code << "=" << val << "\n";
    }
    for (const auto& [val, code] : g_dict_l_shipmode) {
        std::ofstream dict_file(gendb_dir + "/lineitem/l_shipmode_dict.txt", std::ios::app);
        dict_file << code << "=" << val << "\n";
    }

    std::cout << "  Wrote " << l_orderkey.size() << " rows" << std::endl;
}

// Ingest orders
void ingest_orders(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting orders..." << std::endl;

    std::string filepath = data_dir + "/orders.tbl";
    std::ifstream file(filepath);
    if (!file) {
        std::cerr << "Failed to open " << filepath << std::endl;
        return;
    }

    std::vector<int32_t> o_orderkey;
    std::vector<int32_t> o_custkey;
    std::vector<uint8_t> o_orderstatus_codes;
    std::vector<int64_t> o_totalprice;
    std::vector<int32_t> o_orderdate;
    std::vector<uint8_t> o_orderpriority_codes;
    std::vector<std::string> o_clerk;
    std::vector<int32_t> o_shippriority;
    std::vector<std::string> o_comment;

    std::string line;
    while (std::getline(file, line)) {
        auto tokens = split_line(line, '|');
        if (tokens.size() < 9) continue;

        o_orderkey.push_back(std::stoi(tokens[0]));
        o_custkey.push_back(std::stoi(tokens[1]));

        if (g_dict_o_orderstatus.find(tokens[2]) == g_dict_o_orderstatus.end()) {
            g_dict_o_orderstatus[tokens[2]] = g_dict_vals_o_orderstatus.size();
            g_dict_vals_o_orderstatus.push_back(tokens[2]);
        }
        o_orderstatus_codes.push_back(g_dict_o_orderstatus[tokens[2]]);

        o_totalprice.push_back(parse_decimal(tokens[3], 100));
        o_orderdate.push_back(parse_date(tokens[4]));

        if (g_dict_o_orderpriority.find(tokens[5]) == g_dict_o_orderpriority.end()) {
            g_dict_o_orderpriority[tokens[5]] = g_dict_vals_o_orderpriority.size();
            g_dict_vals_o_orderpriority.push_back(tokens[5]);
        }
        o_orderpriority_codes.push_back(g_dict_o_orderpriority[tokens[5]]);

        o_clerk.push_back(tokens[6]);
        o_shippriority.push_back(std::stoi(tokens[7]));
        o_comment.push_back(tokens[8]);
    }
    file.close();

    fs::create_directories(gendb_dir + "/orders");

    auto write_binary = [&](const std::string& col_name, const void* data, size_t count, size_t element_size) {
        std::ofstream out(gendb_dir + "/orders/" + col_name + ".bin", std::ios::binary);
        out.write(static_cast<const char*>(data), count * element_size);
        out.close();
    };

    write_binary("o_orderkey", o_orderkey.data(), o_orderkey.size(), sizeof(int32_t));
    write_binary("o_custkey", o_custkey.data(), o_custkey.size(), sizeof(int32_t));
    write_binary("o_orderstatus", o_orderstatus_codes.data(), o_orderstatus_codes.size(), sizeof(uint8_t));
    write_binary("o_totalprice", o_totalprice.data(), o_totalprice.size(), sizeof(int64_t));
    write_binary("o_orderdate", o_orderdate.data(), o_orderdate.size(), sizeof(int32_t));
    write_binary("o_orderpriority", o_orderpriority_codes.data(), o_orderpriority_codes.size(), sizeof(uint8_t));
    write_binary("o_shippriority", o_shippriority.data(), o_shippriority.size(), sizeof(int32_t));

    std::ofstream str_file1(gendb_dir + "/orders/o_clerk.strvec", std::ios::binary);
    for (const auto& s : o_clerk) {
        uint32_t len = s.size();
        str_file1.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
        str_file1.write(s.data(), s.size());
    }
    str_file1.close();

    std::ofstream str_file2(gendb_dir + "/orders/o_comment.strvec", std::ios::binary);
    for (const auto& s : o_comment) {
        uint32_t len = s.size();
        str_file2.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
        str_file2.write(s.data(), s.size());
    }
    str_file2.close();

    for (const auto& [val, code] : g_dict_o_orderstatus) {
        std::ofstream dict_file(gendb_dir + "/orders/o_orderstatus_dict.txt", std::ios::app);
        dict_file << code << "=" << val << "\n";
    }
    for (const auto& [val, code] : g_dict_o_orderpriority) {
        std::ofstream dict_file(gendb_dir + "/orders/o_orderpriority_dict.txt", std::ios::app);
        dict_file << code << "=" << val << "\n";
    }

    std::cout << "  Wrote " << o_orderkey.size() << " rows" << std::endl;
}

// Ingest customer
void ingest_customer(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting customer..." << std::endl;

    std::string filepath = data_dir + "/customer.tbl";
    std::ifstream file(filepath);
    if (!file) {
        std::cerr << "Failed to open " << filepath << std::endl;
        return;
    }

    std::vector<int32_t> c_custkey;
    std::vector<std::string> c_name;
    std::vector<std::string> c_address;
    std::vector<int32_t> c_nationkey;
    std::vector<std::string> c_phone;
    std::vector<int64_t> c_acctbal;
    std::vector<uint8_t> c_mktsegment_codes;
    std::vector<std::string> c_comment;

    std::string line;
    while (std::getline(file, line)) {
        auto tokens = split_line(line, '|');
        if (tokens.size() < 8) continue;

        c_custkey.push_back(std::stoi(tokens[0]));
        c_name.push_back(tokens[1]);
        c_address.push_back(tokens[2]);
        c_nationkey.push_back(std::stoi(tokens[3]));
        c_phone.push_back(tokens[4]);
        c_acctbal.push_back(parse_decimal(tokens[5], 100));

        if (g_dict_c_mktsegment.find(tokens[6]) == g_dict_c_mktsegment.end()) {
            g_dict_c_mktsegment[tokens[6]] = g_dict_vals_c_mktsegment.size();
            g_dict_vals_c_mktsegment.push_back(tokens[6]);
        }
        c_mktsegment_codes.push_back(g_dict_c_mktsegment[tokens[6]]);

        c_comment.push_back(tokens[7]);
    }
    file.close();

    fs::create_directories(gendb_dir + "/customer");

    auto write_binary = [&](const std::string& col_name, const void* data, size_t count, size_t element_size) {
        std::ofstream out(gendb_dir + "/customer/" + col_name + ".bin", std::ios::binary);
        out.write(static_cast<const char*>(data), count * element_size);
        out.close();
    };

    write_binary("c_custkey", c_custkey.data(), c_custkey.size(), sizeof(int32_t));
    write_binary("c_nationkey", c_nationkey.data(), c_nationkey.size(), sizeof(int32_t));
    write_binary("c_acctbal", c_acctbal.data(), c_acctbal.size(), sizeof(int64_t));
    write_binary("c_mktsegment", c_mktsegment_codes.data(), c_mktsegment_codes.size(), sizeof(uint8_t));

    std::ofstream str_file1(gendb_dir + "/customer/c_name.strvec", std::ios::binary);
    for (const auto& s : c_name) {
        uint32_t len = s.size();
        str_file1.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
        str_file1.write(s.data(), s.size());
    }
    str_file1.close();

    std::ofstream str_file2(gendb_dir + "/customer/c_address.strvec", std::ios::binary);
    for (const auto& s : c_address) {
        uint32_t len = s.size();
        str_file2.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
        str_file2.write(s.data(), s.size());
    }
    str_file2.close();

    std::ofstream str_file3(gendb_dir + "/customer/c_phone.strvec", std::ios::binary);
    for (const auto& s : c_phone) {
        uint32_t len = s.size();
        str_file3.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
        str_file3.write(s.data(), s.size());
    }
    str_file3.close();

    std::ofstream str_file4(gendb_dir + "/customer/c_comment.strvec", std::ios::binary);
    for (const auto& s : c_comment) {
        uint32_t len = s.size();
        str_file4.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
        str_file4.write(s.data(), s.size());
    }
    str_file4.close();

    for (const auto& [val, code] : g_dict_c_mktsegment) {
        std::ofstream dict_file(gendb_dir + "/customer/c_mktsegment_dict.txt", std::ios::app);
        dict_file << code << "=" << val << "\n";
    }

    std::cout << "  Wrote " << c_custkey.size() << " rows" << std::endl;
}

// Ingest supplier
void ingest_supplier(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting supplier..." << std::endl;

    std::string filepath = data_dir + "/supplier.tbl";
    std::ifstream file(filepath);
    if (!file) {
        std::cerr << "Failed to open " << filepath << std::endl;
        return;
    }

    std::vector<int32_t> s_suppkey;
    std::vector<std::string> s_name;
    std::vector<std::string> s_address;
    std::vector<int32_t> s_nationkey;
    std::vector<std::string> s_phone;
    std::vector<int64_t> s_acctbal;
    std::vector<std::string> s_comment;

    std::string line;
    while (std::getline(file, line)) {
        auto tokens = split_line(line, '|');
        if (tokens.size() < 7) continue;

        s_suppkey.push_back(std::stoi(tokens[0]));
        s_name.push_back(tokens[1]);
        s_address.push_back(tokens[2]);
        s_nationkey.push_back(std::stoi(tokens[3]));
        s_phone.push_back(tokens[4]);
        s_acctbal.push_back(parse_decimal(tokens[5], 100));
        s_comment.push_back(tokens[6]);
    }
    file.close();

    fs::create_directories(gendb_dir + "/supplier");

    auto write_binary = [&](const std::string& col_name, const void* data, size_t count, size_t element_size) {
        std::ofstream out(gendb_dir + "/supplier/" + col_name + ".bin", std::ios::binary);
        out.write(static_cast<const char*>(data), count * element_size);
        out.close();
    };

    write_binary("s_suppkey", s_suppkey.data(), s_suppkey.size(), sizeof(int32_t));
    write_binary("s_nationkey", s_nationkey.data(), s_nationkey.size(), sizeof(int32_t));
    write_binary("s_acctbal", s_acctbal.data(), s_acctbal.size(), sizeof(int64_t));

    auto write_strvec = [&](const std::string& col_name, const std::vector<std::string>& vec) {
        std::ofstream out(gendb_dir + "/supplier/" + col_name + ".strvec", std::ios::binary);
        for (const auto& s : vec) {
            uint32_t len = s.size();
            out.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
            out.write(s.data(), s.size());
        }
        out.close();
    };

    write_strvec("s_name", s_name);
    write_strvec("s_address", s_address);
    write_strvec("s_phone", s_phone);
    write_strvec("s_comment", s_comment);

    std::cout << "  Wrote " << s_suppkey.size() << " rows" << std::endl;
}

// Ingest part
void ingest_part(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting part..." << std::endl;

    std::string filepath = data_dir + "/part.tbl";
    std::ifstream file(filepath);
    if (!file) {
        std::cerr << "Failed to open " << filepath << std::endl;
        return;
    }

    std::vector<int32_t> p_partkey;
    std::vector<std::string> p_name;
    std::vector<std::string> p_mfgr;
    std::vector<std::string> p_brand;
    std::vector<std::string> p_type;
    std::vector<int32_t> p_size;
    std::vector<std::string> p_container;
    std::vector<int64_t> p_retailprice;
    std::vector<std::string> p_comment;

    std::string line;
    while (std::getline(file, line)) {
        auto tokens = split_line(line, '|');
        if (tokens.size() < 9) continue;

        p_partkey.push_back(std::stoi(tokens[0]));
        p_name.push_back(tokens[1]);
        p_mfgr.push_back(tokens[2]);
        p_brand.push_back(tokens[3]);
        p_type.push_back(tokens[4]);
        p_size.push_back(std::stoi(tokens[5]));
        p_container.push_back(tokens[6]);
        p_retailprice.push_back(parse_decimal(tokens[7], 100));
        p_comment.push_back(tokens[8]);
    }
    file.close();

    fs::create_directories(gendb_dir + "/part");

    auto write_binary = [&](const std::string& col_name, const void* data, size_t count, size_t element_size) {
        std::ofstream out(gendb_dir + "/part/" + col_name + ".bin", std::ios::binary);
        out.write(static_cast<const char*>(data), count * element_size);
        out.close();
    };

    write_binary("p_partkey", p_partkey.data(), p_partkey.size(), sizeof(int32_t));
    write_binary("p_size", p_size.data(), p_size.size(), sizeof(int32_t));
    write_binary("p_retailprice", p_retailprice.data(), p_retailprice.size(), sizeof(int64_t));

    auto write_strvec = [&](const std::string& col_name, const std::vector<std::string>& vec) {
        std::ofstream out(gendb_dir + "/part/" + col_name + ".strvec", std::ios::binary);
        for (const auto& s : vec) {
            uint32_t len = s.size();
            out.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
            out.write(s.data(), s.size());
        }
        out.close();
    };

    write_strvec("p_name", p_name);
    write_strvec("p_mfgr", p_mfgr);
    write_strvec("p_brand", p_brand);
    write_strvec("p_type", p_type);
    write_strvec("p_container", p_container);
    write_strvec("p_comment", p_comment);

    std::cout << "  Wrote " << p_partkey.size() << " rows" << std::endl;
}

// Ingest partsupp
void ingest_partsupp(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting partsupp..." << std::endl;

    std::string filepath = data_dir + "/partsupp.tbl";
    std::ifstream file(filepath);
    if (!file) {
        std::cerr << "Failed to open " << filepath << std::endl;
        return;
    }

    std::vector<int32_t> ps_partkey;
    std::vector<int32_t> ps_suppkey;
    std::vector<int32_t> ps_availqty;
    std::vector<int64_t> ps_supplycost;
    std::vector<std::string> ps_comment;

    std::string line;
    while (std::getline(file, line)) {
        auto tokens = split_line(line, '|');
        if (tokens.size() < 5) continue;

        ps_partkey.push_back(std::stoi(tokens[0]));
        ps_suppkey.push_back(std::stoi(tokens[1]));
        ps_availqty.push_back(std::stoi(tokens[2]));
        ps_supplycost.push_back(parse_decimal(tokens[3], 100));
        ps_comment.push_back(tokens[4]);
    }
    file.close();

    fs::create_directories(gendb_dir + "/partsupp");

    auto write_binary = [&](const std::string& col_name, const void* data, size_t count, size_t element_size) {
        std::ofstream out(gendb_dir + "/partsupp/" + col_name + ".bin", std::ios::binary);
        out.write(static_cast<const char*>(data), count * element_size);
        out.close();
    };

    write_binary("ps_partkey", ps_partkey.data(), ps_partkey.size(), sizeof(int32_t));
    write_binary("ps_suppkey", ps_suppkey.data(), ps_suppkey.size(), sizeof(int32_t));
    write_binary("ps_availqty", ps_availqty.data(), ps_availqty.size(), sizeof(int32_t));
    write_binary("ps_supplycost", ps_supplycost.data(), ps_supplycost.size(), sizeof(int64_t));

    std::ofstream str_file(gendb_dir + "/partsupp/ps_comment.strvec", std::ios::binary);
    for (const auto& s : ps_comment) {
        uint32_t len = s.size();
        str_file.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
        str_file.write(s.data(), s.size());
    }
    str_file.close();

    std::cout << "  Wrote " << ps_partkey.size() << " rows" << std::endl;
}

// Ingest nation
void ingest_nation(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting nation..." << std::endl;

    std::string filepath = data_dir + "/nation.tbl";
    std::ifstream file(filepath);
    if (!file) {
        std::cerr << "Failed to open " << filepath << std::endl;
        return;
    }

    std::vector<int32_t> n_nationkey;
    std::vector<std::string> n_name;
    std::vector<int32_t> n_regionkey;
    std::vector<std::string> n_comment;

    std::string line;
    while (std::getline(file, line)) {
        auto tokens = split_line(line, '|');
        if (tokens.size() < 4) continue;

        n_nationkey.push_back(std::stoi(tokens[0]));
        n_name.push_back(tokens[1]);
        n_regionkey.push_back(std::stoi(tokens[2]));
        n_comment.push_back(tokens[3]);
    }
    file.close();

    fs::create_directories(gendb_dir + "/nation");

    auto write_binary = [&](const std::string& col_name, const void* data, size_t count, size_t element_size) {
        std::ofstream out(gendb_dir + "/nation/" + col_name + ".bin", std::ios::binary);
        out.write(static_cast<const char*>(data), count * element_size);
        out.close();
    };

    write_binary("n_nationkey", n_nationkey.data(), n_nationkey.size(), sizeof(int32_t));
    write_binary("n_regionkey", n_regionkey.data(), n_regionkey.size(), sizeof(int32_t));

    auto write_strvec = [&](const std::string& col_name, const std::vector<std::string>& vec) {
        std::ofstream out(gendb_dir + "/nation/" + col_name + ".strvec", std::ios::binary);
        for (const auto& s : vec) {
            uint32_t len = s.size();
            out.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
            out.write(s.data(), s.size());
        }
        out.close();
    };

    write_strvec("n_name", n_name);
    write_strvec("n_comment", n_comment);

    std::cout << "  Wrote " << n_nationkey.size() << " rows" << std::endl;
}

// Ingest region
void ingest_region(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting region..." << std::endl;

    std::string filepath = data_dir + "/region.tbl";
    std::ifstream file(filepath);
    if (!file) {
        std::cerr << "Failed to open " << filepath << std::endl;
        return;
    }

    std::vector<int32_t> r_regionkey;
    std::vector<std::string> r_name;
    std::vector<std::string> r_comment;

    std::string line;
    while (std::getline(file, line)) {
        auto tokens = split_line(line, '|');
        if (tokens.size() < 3) continue;

        r_regionkey.push_back(std::stoi(tokens[0]));
        r_name.push_back(tokens[1]);
        r_comment.push_back(tokens[2]);
    }
    file.close();

    fs::create_directories(gendb_dir + "/region");

    auto write_binary = [&](const std::string& col_name, const void* data, size_t count, size_t element_size) {
        std::ofstream out(gendb_dir + "/region/" + col_name + ".bin", std::ios::binary);
        out.write(static_cast<const char*>(data), count * element_size);
        out.close();
    };

    write_binary("r_regionkey", r_regionkey.data(), r_regionkey.size(), sizeof(int32_t));

    auto write_strvec = [&](const std::string& col_name, const std::vector<std::string>& vec) {
        std::ofstream out(gendb_dir + "/region/" + col_name + ".strvec", std::ios::binary);
        for (const auto& s : vec) {
            uint32_t len = s.size();
            out.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
            out.write(s.data(), s.size());
        }
        out.close();
    };

    write_strvec("r_name", r_name);
    write_strvec("r_comment", r_comment);

    std::cout << "  Wrote " << r_regionkey.size() << " rows" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    std::cout << "Starting TPC-H ingestion..." << std::endl;
    std::cout << "Data directory: " << data_dir << std::endl;
    std::cout << "Output directory: " << gendb_dir << std::endl;

    fs::create_directories(gendb_dir);

    // Ingest tables in parallel for better performance
    ingest_lineitem(data_dir, gendb_dir);
    ingest_orders(data_dir, gendb_dir);
    ingest_customer(data_dir, gendb_dir);
    ingest_supplier(data_dir, gendb_dir);
    ingest_part(data_dir, gendb_dir);
    ingest_partsupp(data_dir, gendb_dir);
    ingest_nation(data_dir, gendb_dir);
    ingest_region(data_dir, gendb_dir);

    std::cout << "Ingestion complete!" << std::endl;
    return 0;
}
