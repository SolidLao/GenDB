#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <filesystem>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace fs = std::filesystem;

// Constants
const int NUM_THREADS = 32;
const int CHUNK_SIZE = 1000000;
const size_t WRITE_BUFFER_SIZE = 2 * 1024 * 1024;  // 2MB

// Forward declarations
int64_t parse_date(const std::string& s);
int64_t parse_decimal(const std::string& s, int scale);

struct ColumnWriter {
    std::ofstream binary_file;
    std::ofstream dict_file;
    std::string column_name;
    bool is_dictionary;
    bool is_string;
    std::unordered_map<std::string, int32_t> dict_map;
    int32_t next_code = 0;
    std::vector<char> write_buffer;

    ColumnWriter(const std::string& base_path, const std::string& col_name,
                 bool use_dict = false, bool use_str = false)
        : column_name(col_name), is_dictionary(use_dict), is_string(use_str) {
        write_buffer.reserve(WRITE_BUFFER_SIZE);

        std::string bin_path = base_path + "/" + col_name + ".bin";
        binary_file.open(bin_path, std::ios::binary);
        if (!binary_file.is_open()) {
            throw std::runtime_error("Cannot open " + bin_path);
        }

        if (is_dictionary) {
            std::string dict_path = base_path + "/" + col_name + "_dict.txt";
            dict_file.open(dict_path);
            if (!dict_file.is_open()) {
                throw std::runtime_error("Cannot open " + dict_path);
            }
        }
    }

    void write_int32(int32_t val) {
        if (write_buffer.size() + sizeof(int32_t) > WRITE_BUFFER_SIZE) {
            binary_file.write(write_buffer.data(), write_buffer.size());
            write_buffer.clear();
        }
        char* ptr = (char*)&val;
        write_buffer.insert(write_buffer.end(), ptr, ptr + sizeof(int32_t));
    }

    void write_int64(int64_t val) {
        if (write_buffer.size() + sizeof(int64_t) > WRITE_BUFFER_SIZE) {
            binary_file.write(write_buffer.data(), write_buffer.size());
            write_buffer.clear();
        }
        char* ptr = (char*)&val;
        write_buffer.insert(write_buffer.end(), ptr, ptr + sizeof(int64_t));
    }

    void write_string_raw(const std::string& val) {
        // For non-dictionary strings, write length + data
        int32_t len = val.length();
        write_int32(len);
        if (len > 0) {
            if (write_buffer.size() + len > WRITE_BUFFER_SIZE) {
                binary_file.write(write_buffer.data(), write_buffer.size());
                write_buffer.clear();
            }
            write_buffer.insert(write_buffer.end(), val.begin(), val.end());
        }
    }

    int32_t encode_dictionary(const std::string& val) {
        auto it = dict_map.find(val);
        if (it != dict_map.end()) {
            return it->second;
        }
        int32_t code = next_code++;
        dict_map[val] = code;
        dict_file << code << "|" << val << "\n";
        return code;
    }

    void write_value_dict(const std::string& val) {
        int32_t code = encode_dictionary(val);
        write_int32(code);
    }

    void flush() {
        if (!write_buffer.empty()) {
            binary_file.write(write_buffer.data(), write_buffer.size());
            write_buffer.clear();
        }
        binary_file.flush();
        if (dict_file.is_open()) {
            dict_file.flush();
        }
    }

    void close() {
        flush();
        binary_file.close();
        if (dict_file.is_open()) {
            dict_file.close();
        }
    }
};

// Date parsing: days since epoch 1970-01-01
int64_t parse_date(const std::string& s) {
    if (s.length() < 10) return 0;

    int year = std::stoi(s.substr(0, 4));
    int month = std::stoi(s.substr(5, 2));
    int day = std::stoi(s.substr(8, 2));

    // Count days from 1970-01-01
    int64_t total_days = 0;

    // Days in complete years from 1970 to year-1
    for (int y = 1970; y < year; y++) {
        total_days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Days in complete months of the current year
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[1] = 29;
    }
    for (int m = 1; m < month; m++) {
        total_days += days_in_month[m - 1];
    }

    // Days in the current month
    total_days += (day - 1);

    return total_days;
}

// Decimal parsing: assume scale_factor is pre-defined
int64_t parse_decimal(const std::string& s, int scale) {
    if (s.empty()) return 0;
    double d = std::stod(s);
    int64_t scale_mult = 1;
    for (int i = 0; i < scale; i++) scale_mult *= 10;
    return (int64_t)(d * scale_mult + 0.5);
}

// CSV parsing utilities
std::vector<std::string> split_line(const std::string& line, char delim = '|') {
    std::vector<std::string> result;
    size_t start = 0;
    size_t end = line.find(delim);
    while (end != std::string::npos) {
        result.push_back(line.substr(start, end - start));
        start = end + 1;
        end = line.find(delim, start);
    }
    result.push_back(line.substr(start));
    return result;
}

// Table-specific parsers
void ingest_lineitem(const std::string& filepath, const std::string& output_dir) {
    std::cout << "Ingesting lineitem..." << std::endl;

    ColumnWriter l_orderkey(output_dir + "/lineitem", "l_orderkey");
    ColumnWriter l_partkey(output_dir + "/lineitem", "l_partkey");
    ColumnWriter l_suppkey(output_dir + "/lineitem", "l_suppkey");
    ColumnWriter l_linenumber(output_dir + "/lineitem", "l_linenumber");
    ColumnWriter l_quantity(output_dir + "/lineitem", "l_quantity");
    ColumnWriter l_extendedprice(output_dir + "/lineitem", "l_extendedprice");
    ColumnWriter l_discount(output_dir + "/lineitem", "l_discount");
    ColumnWriter l_tax(output_dir + "/lineitem", "l_tax");
    ColumnWriter l_returnflag(output_dir + "/lineitem", "l_returnflag", true);
    ColumnWriter l_linestatus(output_dir + "/lineitem", "l_linestatus", true);
    ColumnWriter l_shipdate(output_dir + "/lineitem", "l_shipdate");
    ColumnWriter l_commitdate(output_dir + "/lineitem", "l_commitdate");
    ColumnWriter l_receiptdate(output_dir + "/lineitem", "l_receiptdate");
    ColumnWriter l_shipinstruct(output_dir + "/lineitem", "l_shipinstruct", true);
    ColumnWriter l_shipmode(output_dir + "/lineitem", "l_shipmode", true);
    ColumnWriter l_comment(output_dir + "/lineitem", "l_comment", false, true);

    std::ifstream file(filepath);
    std::string line;
    int64_t count = 0;

    while (std::getline(file, line)) {
        auto fields = split_line(line);
        if (fields.size() < 16) continue;

        l_orderkey.write_int32(std::stoi(fields[0]));
        l_partkey.write_int32(std::stoi(fields[1]));
        l_suppkey.write_int32(std::stoi(fields[2]));
        l_linenumber.write_int32(std::stoi(fields[3]));
        l_quantity.write_int64(parse_decimal(fields[4], 2));
        l_extendedprice.write_int64(parse_decimal(fields[5], 2));
        l_discount.write_int64(parse_decimal(fields[6], 2));
        l_tax.write_int64(parse_decimal(fields[7], 2));
        l_returnflag.write_value_dict(fields[8]);
        l_linestatus.write_value_dict(fields[9]);
        l_shipdate.write_int32(parse_date(fields[10]));
        l_commitdate.write_int32(parse_date(fields[11]));
        l_receiptdate.write_int32(parse_date(fields[12]));
        l_shipinstruct.write_value_dict(fields[13]);
        l_shipmode.write_value_dict(fields[14]);
        l_comment.write_string_raw(fields[15]);

        count++;
        if (count % 1000000 == 0) {
            std::cout << "  Lineitem: " << count << " rows" << std::endl;
        }
    }

    file.close();
    l_orderkey.close();
    l_partkey.close();
    l_suppkey.close();
    l_linenumber.close();
    l_quantity.close();
    l_extendedprice.close();
    l_discount.close();
    l_tax.close();
    l_returnflag.close();
    l_linestatus.close();
    l_shipdate.close();
    l_commitdate.close();
    l_receiptdate.close();
    l_shipinstruct.close();
    l_shipmode.close();
    l_comment.close();

    std::cout << "Lineitem: " << count << " rows ingested" << std::endl;
}

void ingest_orders(const std::string& filepath, const std::string& output_dir) {
    std::cout << "Ingesting orders..." << std::endl;

    ColumnWriter o_orderkey(output_dir + "/orders", "o_orderkey");
    ColumnWriter o_custkey(output_dir + "/orders", "o_custkey");
    ColumnWriter o_orderstatus(output_dir + "/orders", "o_orderstatus", true);
    ColumnWriter o_totalprice(output_dir + "/orders", "o_totalprice");
    ColumnWriter o_orderdate(output_dir + "/orders", "o_orderdate");
    ColumnWriter o_orderpriority(output_dir + "/orders", "o_orderpriority", true);
    ColumnWriter o_clerk(output_dir + "/orders", "o_clerk", true);
    ColumnWriter o_shippriority(output_dir + "/orders", "o_shippriority");
    ColumnWriter o_comment(output_dir + "/orders", "o_comment", false, true);

    std::ifstream file(filepath);
    std::string line;
    int64_t count = 0;

    while (std::getline(file, line)) {
        auto fields = split_line(line);
        if (fields.size() < 9) continue;

        o_orderkey.write_int32(std::stoi(fields[0]));
        o_custkey.write_int32(std::stoi(fields[1]));
        o_orderstatus.write_value_dict(fields[2]);
        o_totalprice.write_int64(parse_decimal(fields[3], 2));
        o_orderdate.write_int32(parse_date(fields[4]));
        o_orderpriority.write_value_dict(fields[5]);
        o_clerk.write_value_dict(fields[6]);
        o_shippriority.write_int32(std::stoi(fields[7]));
        o_comment.write_string_raw(fields[8]);

        count++;
        if (count % 1000000 == 0) {
            std::cout << "  Orders: " << count << " rows" << std::endl;
        }
    }

    file.close();
    o_orderkey.close();
    o_custkey.close();
    o_orderstatus.close();
    o_totalprice.close();
    o_orderdate.close();
    o_orderpriority.close();
    o_clerk.close();
    o_shippriority.close();
    o_comment.close();

    std::cout << "Orders: " << count << " rows ingested" << std::endl;
}

void ingest_customer(const std::string& filepath, const std::string& output_dir) {
    std::cout << "Ingesting customer..." << std::endl;

    ColumnWriter c_custkey(output_dir + "/customer", "c_custkey");
    ColumnWriter c_name(output_dir + "/customer", "c_name", false, true);
    ColumnWriter c_address(output_dir + "/customer", "c_address", false, true);
    ColumnWriter c_nationkey(output_dir + "/customer", "c_nationkey");
    ColumnWriter c_phone(output_dir + "/customer", "c_phone", false, true);
    ColumnWriter c_acctbal(output_dir + "/customer", "c_acctbal");
    ColumnWriter c_mktsegment(output_dir + "/customer", "c_mktsegment", true);
    ColumnWriter c_comment(output_dir + "/customer", "c_comment", false, true);

    std::ifstream file(filepath);
    std::string line;
    int64_t count = 0;

    while (std::getline(file, line)) {
        auto fields = split_line(line);
        if (fields.size() < 8) continue;

        c_custkey.write_int32(std::stoi(fields[0]));
        c_name.write_string_raw(fields[1]);
        c_address.write_string_raw(fields[2]);
        c_nationkey.write_int32(std::stoi(fields[3]));
        c_phone.write_string_raw(fields[4]);
        c_acctbal.write_int64(parse_decimal(fields[5], 2));
        c_mktsegment.write_value_dict(fields[6]);
        c_comment.write_string_raw(fields[7]);

        count++;
        if (count % 100000 == 0) {
            std::cout << "  Customer: " << count << " rows" << std::endl;
        }
    }

    file.close();
    c_custkey.close();
    c_name.close();
    c_address.close();
    c_nationkey.close();
    c_phone.close();
    c_acctbal.close();
    c_mktsegment.close();
    c_comment.close();

    std::cout << "Customer: " << count << " rows ingested" << std::endl;
}

void ingest_part(const std::string& filepath, const std::string& output_dir) {
    std::cout << "Ingesting part..." << std::endl;

    ColumnWriter p_partkey(output_dir + "/part", "p_partkey");
    ColumnWriter p_name(output_dir + "/part", "p_name", false, true);
    ColumnWriter p_mfgr(output_dir + "/part", "p_mfgr", true);
    ColumnWriter p_brand(output_dir + "/part", "p_brand", true);
    ColumnWriter p_type(output_dir + "/part", "p_type", true);
    ColumnWriter p_size(output_dir + "/part", "p_size");
    ColumnWriter p_container(output_dir + "/part", "p_container", true);
    ColumnWriter p_retailprice(output_dir + "/part", "p_retailprice");
    ColumnWriter p_comment(output_dir + "/part", "p_comment", false, true);

    std::ifstream file(filepath);
    std::string line;
    int64_t count = 0;

    while (std::getline(file, line)) {
        auto fields = split_line(line);
        if (fields.size() < 9) continue;

        p_partkey.write_int32(std::stoi(fields[0]));
        p_name.write_string_raw(fields[1]);
        p_mfgr.write_value_dict(fields[2]);
        p_brand.write_value_dict(fields[3]);
        p_type.write_value_dict(fields[4]);
        p_size.write_int32(std::stoi(fields[5]));
        p_container.write_value_dict(fields[6]);
        p_retailprice.write_int64(parse_decimal(fields[7], 2));
        p_comment.write_string_raw(fields[8]);

        count++;
        if (count % 100000 == 0) {
            std::cout << "  Part: " << count << " rows" << std::endl;
        }
    }

    file.close();
    p_partkey.close();
    p_name.close();
    p_mfgr.close();
    p_brand.close();
    p_type.close();
    p_size.close();
    p_container.close();
    p_retailprice.close();
    p_comment.close();

    std::cout << "Part: " << count << " rows ingested" << std::endl;
}

void ingest_partsupp(const std::string& filepath, const std::string& output_dir) {
    std::cout << "Ingesting partsupp..." << std::endl;

    ColumnWriter ps_partkey(output_dir + "/partsupp", "ps_partkey");
    ColumnWriter ps_suppkey(output_dir + "/partsupp", "ps_suppkey");
    ColumnWriter ps_availqty(output_dir + "/partsupp", "ps_availqty");
    ColumnWriter ps_supplycost(output_dir + "/partsupp", "ps_supplycost");
    ColumnWriter ps_comment(output_dir + "/partsupp", "ps_comment", false, true);

    std::ifstream file(filepath);
    std::string line;
    int64_t count = 0;

    while (std::getline(file, line)) {
        auto fields = split_line(line);
        if (fields.size() < 5) continue;

        ps_partkey.write_int32(std::stoi(fields[0]));
        ps_suppkey.write_int32(std::stoi(fields[1]));
        ps_availqty.write_int32(std::stoi(fields[2]));
        ps_supplycost.write_int64(parse_decimal(fields[3], 2));
        ps_comment.write_string_raw(fields[4]);

        count++;
        if (count % 100000 == 0) {
            std::cout << "  Partsupp: " << count << " rows" << std::endl;
        }
    }

    file.close();
    ps_partkey.close();
    ps_suppkey.close();
    ps_availqty.close();
    ps_supplycost.close();
    ps_comment.close();

    std::cout << "Partsupp: " << count << " rows ingested" << std::endl;
}

void ingest_supplier(const std::string& filepath, const std::string& output_dir) {
    std::cout << "Ingesting supplier..." << std::endl;

    ColumnWriter s_suppkey(output_dir + "/supplier", "s_suppkey");
    ColumnWriter s_name(output_dir + "/supplier", "s_name", false, true);
    ColumnWriter s_address(output_dir + "/supplier", "s_address", false, true);
    ColumnWriter s_nationkey(output_dir + "/supplier", "s_nationkey");
    ColumnWriter s_phone(output_dir + "/supplier", "s_phone", false, true);
    ColumnWriter s_acctbal(output_dir + "/supplier", "s_acctbal");
    ColumnWriter s_comment(output_dir + "/supplier", "s_comment", false, true);

    std::ifstream file(filepath);
    std::string line;
    int64_t count = 0;

    while (std::getline(file, line)) {
        auto fields = split_line(line);
        if (fields.size() < 7) continue;

        s_suppkey.write_int32(std::stoi(fields[0]));
        s_name.write_string_raw(fields[1]);
        s_address.write_string_raw(fields[2]);
        s_nationkey.write_int32(std::stoi(fields[3]));
        s_phone.write_string_raw(fields[4]);
        s_acctbal.write_int64(parse_decimal(fields[5], 2));
        s_comment.write_string_raw(fields[6]);

        count++;
        if (count % 10000 == 0) {
            std::cout << "  Supplier: " << count << " rows" << std::endl;
        }
    }

    file.close();
    s_suppkey.close();
    s_name.close();
    s_address.close();
    s_nationkey.close();
    s_phone.close();
    s_acctbal.close();
    s_comment.close();

    std::cout << "Supplier: " << count << " rows ingested" << std::endl;
}

void ingest_nation(const std::string& filepath, const std::string& output_dir) {
    std::cout << "Ingesting nation..." << std::endl;

    ColumnWriter n_nationkey(output_dir + "/nation", "n_nationkey");
    ColumnWriter n_name(output_dir + "/nation", "n_name", false, true);
    ColumnWriter n_regionkey(output_dir + "/nation", "n_regionkey");
    ColumnWriter n_comment(output_dir + "/nation", "n_comment", false, true);

    std::ifstream file(filepath);
    std::string line;
    int64_t count = 0;

    while (std::getline(file, line)) {
        auto fields = split_line(line);
        if (fields.size() < 4) continue;

        n_nationkey.write_int32(std::stoi(fields[0]));
        n_name.write_string_raw(fields[1]);
        n_regionkey.write_int32(std::stoi(fields[2]));
        n_comment.write_string_raw(fields[3]);

        count++;
    }

    file.close();
    n_nationkey.close();
    n_name.close();
    n_regionkey.close();
    n_comment.close();

    std::cout << "Nation: " << count << " rows ingested" << std::endl;
}

void ingest_region(const std::string& filepath, const std::string& output_dir) {
    std::cout << "Ingesting region..." << std::endl;

    ColumnWriter r_regionkey(output_dir + "/region", "r_regionkey");
    ColumnWriter r_name(output_dir + "/region", "r_name", false, true);
    ColumnWriter r_comment(output_dir + "/region", "r_comment", false, true);

    std::ifstream file(filepath);
    std::string line;
    int64_t count = 0;

    while (std::getline(file, line)) {
        auto fields = split_line(line);
        if (fields.size() < 3) continue;

        r_regionkey.write_int32(std::stoi(fields[0]));
        r_name.write_string_raw(fields[1]);
        r_comment.write_string_raw(fields[2]);

        count++;
    }

    file.close();
    r_regionkey.close();
    r_name.close();
    r_comment.close();

    std::cout << "Region: " << count << " rows ingested" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: ./ingest <input_dir> <output_dir>" << std::endl;
        return 1;
    }

    std::string input_dir = argv[1];
    std::string output_dir = argv[2];

    // Create output directories
    fs::create_directories(output_dir + "/lineitem");
    fs::create_directories(output_dir + "/orders");
    fs::create_directories(output_dir + "/customer");
    fs::create_directories(output_dir + "/part");
    fs::create_directories(output_dir + "/partsupp");
    fs::create_directories(output_dir + "/supplier");
    fs::create_directories(output_dir + "/nation");
    fs::create_directories(output_dir + "/region");

    try {
        // Ingest tables in parallel
        std::vector<std::thread> threads;

        threads.push_back(std::thread(ingest_lineitem, input_dir + "/lineitem.tbl", output_dir));
        threads.push_back(std::thread(ingest_orders, input_dir + "/orders.tbl", output_dir));
        threads.push_back(std::thread(ingest_customer, input_dir + "/customer.tbl", output_dir));
        threads.push_back(std::thread(ingest_part, input_dir + "/part.tbl", output_dir));
        threads.push_back(std::thread(ingest_partsupp, input_dir + "/partsupp.tbl", output_dir));
        threads.push_back(std::thread(ingest_supplier, input_dir + "/supplier.tbl", output_dir));
        threads.push_back(std::thread(ingest_nation, input_dir + "/nation.tbl", output_dir));
        threads.push_back(std::thread(ingest_region, input_dir + "/region.tbl", output_dir));

        for (auto& t : threads) {
            t.join();
        }

        std::cout << "Data ingestion complete!" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
