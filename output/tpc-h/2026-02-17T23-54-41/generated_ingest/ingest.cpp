#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <filesystem>
#include <thread>
#include <mutex>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <iomanip>

namespace fs = std::filesystem;
using std::string;
using std::vector;

// Global dictionary maps for string encoding
struct StringDictionary {
    std::unordered_map<string, int32_t> str_to_id;
    vector<string> id_to_str;
    std::mutex lock;
    int32_t next_id = 0;

    int32_t encode(const string& s) {
        std::lock_guard<std::mutex> g(lock);
        auto it = str_to_id.find(s);
        if (it != str_to_id.end()) {
            return it->second;
        }
        int32_t id = next_id++;
        str_to_id[s] = id;
        id_to_str.push_back(s);
        return id;
    }
};

// Global dictionaries for all string columns
std::unordered_map<string, StringDictionary> g_dicts;

// Parse date from YYYY-MM-DD format to days since epoch (1970-01-01)
int32_t parse_date(const string& date_str) {
    if (date_str.empty() || date_str.length() < 10) return 0;

    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Days in each month (non-leap)
    static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    int total_days = 0;

    // Add days for complete years since 1970
    for (int y = 1970; y < year; y++) {
        total_days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Add days for complete months in this year
    for (int m = 1; m < month; m++) {
        total_days += days_in_month[m];
        // Add leap day if needed
        if (m == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
            total_days++;
        }
    }

    // Add remaining days
    total_days += (day - 1);

    return total_days;
}

// Convert decimal string "123.45" to int64_t with scale_factor 100 -> 12345
int64_t parse_decimal(const string& dec_str, int scale_factor) {
    if (dec_str.empty()) return 0;

    size_t dot_pos = dec_str.find('.');
    if (dot_pos == string::npos) {
        // No decimal point, just integer
        return std::stoll(dec_str) * scale_factor;
    }

    string int_part = dec_str.substr(0, dot_pos);
    string frac_part = dec_str.substr(dot_pos + 1);

    int64_t result = 0;
    if (!int_part.empty() && int_part != "-") {
        result = std::stoll(int_part) * scale_factor;
    }

    // Parse fractional part
    int frac_value = 0;
    if (!frac_part.empty()) {
        // Pad or truncate to scale_factor digits
        frac_part.resize(2, '0');  // For scale_factor=100, we need 2 digits
        frac_value = std::stoi(frac_part);
    }

    if (result >= 0) {
        result += frac_value;
    } else {
        result -= frac_value;
    }

    return result;
}

// Split line by delimiter, handling empty fields
vector<string> split_line(const string& line, char delim) {
    vector<string> fields;
    std::istringstream iss(line);
    string field;
    while (std::getline(iss, field, delim)) {
        fields.push_back(field);
    }
    return fields;
}

// Write binary data to file
void write_binary_file(const string& path, const void* data, size_t size) {
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        std::cerr << "Failed to open " << path << " for writing\n";
        throw std::runtime_error("File write failed");
    }
    out.write(static_cast<const char*>(data), size);
    out.close();
}

// Ingest nation table
void ingest_nation(const string& input_dir, const string& output_dir) {
    std::cout << "Ingesting nation...\n";

    vector<int32_t> n_nationkey;
    vector<int32_t> n_name_dict;
    vector<int32_t> n_regionkey;
    vector<int32_t> n_comment_dict;

    string input_file = input_dir + "/nation.tbl";
    std::ifstream in(input_file);
    if (!in) {
        std::cerr << "Cannot open " << input_file << "\n";
        return;
    }

    string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        vector<string> fields = split_line(line, '|');
        if (fields.size() < 4) continue;

        n_nationkey.push_back(std::stoi(fields[0]));
        n_name_dict.push_back(g_dicts["nation_n_name"].encode(fields[1]));
        n_regionkey.push_back(std::stoi(fields[2]));
        n_comment_dict.push_back(g_dicts["nation_n_comment"].encode(fields[3]));
    }
    in.close();

    string nation_dir = output_dir + "/nation";
    fs::create_directories(nation_dir);

    write_binary_file(nation_dir + "/n_nationkey.bin", n_nationkey.data(), n_nationkey.size() * sizeof(int32_t));
    write_binary_file(nation_dir + "/n_name.bin", n_name_dict.data(), n_name_dict.size() * sizeof(int32_t));
    write_binary_file(nation_dir + "/n_regionkey.bin", n_regionkey.data(), n_regionkey.size() * sizeof(int32_t));
    write_binary_file(nation_dir + "/n_comment.bin", n_comment_dict.data(), n_comment_dict.size() * sizeof(int32_t));

    std::cout << "  Ingested " << n_nationkey.size() << " rows\n";
}

// Ingest region table
void ingest_region(const string& input_dir, const string& output_dir) {
    std::cout << "Ingesting region...\n";

    vector<int32_t> r_regionkey;
    vector<int32_t> r_name_dict;
    vector<int32_t> r_comment_dict;

    string input_file = input_dir + "/region.tbl";
    std::ifstream in(input_file);
    if (!in) {
        std::cerr << "Cannot open " << input_file << "\n";
        return;
    }

    string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        vector<string> fields = split_line(line, '|');
        if (fields.size() < 3) continue;

        r_regionkey.push_back(std::stoi(fields[0]));
        r_name_dict.push_back(g_dicts["region_r_name"].encode(fields[1]));
        r_comment_dict.push_back(g_dicts["region_r_comment"].encode(fields[2]));
    }
    in.close();

    string region_dir = output_dir + "/region";
    fs::create_directories(region_dir);

    write_binary_file(region_dir + "/r_regionkey.bin", r_regionkey.data(), r_regionkey.size() * sizeof(int32_t));
    write_binary_file(region_dir + "/r_name.bin", r_name_dict.data(), r_name_dict.size() * sizeof(int32_t));
    write_binary_file(region_dir + "/r_comment.bin", r_comment_dict.data(), r_comment_dict.size() * sizeof(int32_t));

    std::cout << "  Ingested " << r_regionkey.size() << " rows\n";
}

// Ingest supplier table
void ingest_supplier(const string& input_dir, const string& output_dir) {
    std::cout << "Ingesting supplier...\n";

    vector<int32_t> s_suppkey;
    vector<int32_t> s_name_dict;
    vector<int32_t> s_address_dict;
    vector<int32_t> s_nationkey;
    vector<int32_t> s_phone_dict;
    vector<int64_t> s_acctbal;
    vector<int32_t> s_comment_dict;

    string input_file = input_dir + "/supplier.tbl";
    std::ifstream in(input_file);
    if (!in) {
        std::cerr << "Cannot open " << input_file << "\n";
        return;
    }

    string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        vector<string> fields = split_line(line, '|');
        if (fields.size() < 7) continue;

        s_suppkey.push_back(std::stoi(fields[0]));
        s_name_dict.push_back(g_dicts["supplier_s_name"].encode(fields[1]));
        s_address_dict.push_back(g_dicts["supplier_s_address"].encode(fields[2]));
        s_nationkey.push_back(std::stoi(fields[3]));
        s_phone_dict.push_back(g_dicts["supplier_s_phone"].encode(fields[4]));
        s_acctbal.push_back(parse_decimal(fields[5], 100));
        s_comment_dict.push_back(g_dicts["supplier_s_comment"].encode(fields[6]));
    }
    in.close();

    string supplier_dir = output_dir + "/supplier";
    fs::create_directories(supplier_dir);

    write_binary_file(supplier_dir + "/s_suppkey.bin", s_suppkey.data(), s_suppkey.size() * sizeof(int32_t));
    write_binary_file(supplier_dir + "/s_name.bin", s_name_dict.data(), s_name_dict.size() * sizeof(int32_t));
    write_binary_file(supplier_dir + "/s_address.bin", s_address_dict.data(), s_address_dict.size() * sizeof(int32_t));
    write_binary_file(supplier_dir + "/s_nationkey.bin", s_nationkey.data(), s_nationkey.size() * sizeof(int32_t));
    write_binary_file(supplier_dir + "/s_phone.bin", s_phone_dict.data(), s_phone_dict.size() * sizeof(int32_t));
    write_binary_file(supplier_dir + "/s_acctbal.bin", s_acctbal.data(), s_acctbal.size() * sizeof(int64_t));
    write_binary_file(supplier_dir + "/s_comment.bin", s_comment_dict.data(), s_comment_dict.size() * sizeof(int32_t));

    std::cout << "  Ingested " << s_suppkey.size() << " rows\n";
}

// Ingest part table
void ingest_part(const string& input_dir, const string& output_dir) {
    std::cout << "Ingesting part...\n";

    vector<int32_t> p_partkey;
    vector<int32_t> p_name_dict;
    vector<int32_t> p_mfgr_dict;
    vector<int32_t> p_brand_dict;
    vector<int32_t> p_type_dict;
    vector<int32_t> p_size;
    vector<int32_t> p_container_dict;
    vector<int64_t> p_retailprice;
    vector<int32_t> p_comment_dict;

    string input_file = input_dir + "/part.tbl";
    std::ifstream in(input_file);
    if (!in) {
        std::cerr << "Cannot open " << input_file << "\n";
        return;
    }

    string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        vector<string> fields = split_line(line, '|');
        if (fields.size() < 9) continue;

        p_partkey.push_back(std::stoi(fields[0]));
        p_name_dict.push_back(g_dicts["part_p_name"].encode(fields[1]));
        p_mfgr_dict.push_back(g_dicts["part_p_mfgr"].encode(fields[2]));
        p_brand_dict.push_back(g_dicts["part_p_brand"].encode(fields[3]));
        p_type_dict.push_back(g_dicts["part_p_type"].encode(fields[4]));
        p_size.push_back(std::stoi(fields[5]));
        p_container_dict.push_back(g_dicts["part_p_container"].encode(fields[6]));
        p_retailprice.push_back(parse_decimal(fields[7], 100));
        p_comment_dict.push_back(g_dicts["part_p_comment"].encode(fields[8]));
    }
    in.close();

    string part_dir = output_dir + "/part";
    fs::create_directories(part_dir);

    write_binary_file(part_dir + "/p_partkey.bin", p_partkey.data(), p_partkey.size() * sizeof(int32_t));
    write_binary_file(part_dir + "/p_name.bin", p_name_dict.data(), p_name_dict.size() * sizeof(int32_t));
    write_binary_file(part_dir + "/p_mfgr.bin", p_mfgr_dict.data(), p_mfgr_dict.size() * sizeof(int32_t));
    write_binary_file(part_dir + "/p_brand.bin", p_brand_dict.data(), p_brand_dict.size() * sizeof(int32_t));
    write_binary_file(part_dir + "/p_type.bin", p_type_dict.data(), p_type_dict.size() * sizeof(int32_t));
    write_binary_file(part_dir + "/p_size.bin", p_size.data(), p_size.size() * sizeof(int32_t));
    write_binary_file(part_dir + "/p_container.bin", p_container_dict.data(), p_container_dict.size() * sizeof(int32_t));
    write_binary_file(part_dir + "/p_retailprice.bin", p_retailprice.data(), p_retailprice.size() * sizeof(int64_t));
    write_binary_file(part_dir + "/p_comment.bin", p_comment_dict.data(), p_comment_dict.size() * sizeof(int32_t));

    std::cout << "  Ingested " << p_partkey.size() << " rows\n";
}

// Ingest partsupp table
void ingest_partsupp(const string& input_dir, const string& output_dir) {
    std::cout << "Ingesting partsupp...\n";

    vector<int32_t> ps_partkey;
    vector<int32_t> ps_suppkey;
    vector<int32_t> ps_availqty;
    vector<int64_t> ps_supplycost;
    vector<int32_t> ps_comment_dict;

    string input_file = input_dir + "/partsupp.tbl";
    std::ifstream in(input_file);
    if (!in) {
        std::cerr << "Cannot open " << input_file << "\n";
        return;
    }

    string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        vector<string> fields = split_line(line, '|');
        if (fields.size() < 5) continue;

        ps_partkey.push_back(std::stoi(fields[0]));
        ps_suppkey.push_back(std::stoi(fields[1]));
        ps_availqty.push_back(std::stoi(fields[2]));
        ps_supplycost.push_back(parse_decimal(fields[3], 100));
        ps_comment_dict.push_back(g_dicts["partsupp_ps_comment"].encode(fields[4]));
    }
    in.close();

    string partsupp_dir = output_dir + "/partsupp";
    fs::create_directories(partsupp_dir);

    write_binary_file(partsupp_dir + "/ps_partkey.bin", ps_partkey.data(), ps_partkey.size() * sizeof(int32_t));
    write_binary_file(partsupp_dir + "/ps_suppkey.bin", ps_suppkey.data(), ps_suppkey.size() * sizeof(int32_t));
    write_binary_file(partsupp_dir + "/ps_availqty.bin", ps_availqty.data(), ps_availqty.size() * sizeof(int32_t));
    write_binary_file(partsupp_dir + "/ps_supplycost.bin", ps_supplycost.data(), ps_supplycost.size() * sizeof(int64_t));
    write_binary_file(partsupp_dir + "/ps_comment.bin", ps_comment_dict.data(), ps_comment_dict.size() * sizeof(int32_t));

    std::cout << "  Ingested " << ps_partkey.size() << " rows\n";
}

// Ingest customer table
void ingest_customer(const string& input_dir, const string& output_dir) {
    std::cout << "Ingesting customer...\n";

    vector<int32_t> c_custkey;
    vector<int32_t> c_name_dict;
    vector<int32_t> c_address_dict;
    vector<int32_t> c_nationkey;
    vector<int32_t> c_phone_dict;
    vector<int64_t> c_acctbal;
    vector<int32_t> c_mktsegment_dict;
    vector<int32_t> c_comment_dict;

    string input_file = input_dir + "/customer.tbl";
    std::ifstream in(input_file);
    if (!in) {
        std::cerr << "Cannot open " << input_file << "\n";
        return;
    }

    string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        vector<string> fields = split_line(line, '|');
        if (fields.size() < 8) continue;

        c_custkey.push_back(std::stoi(fields[0]));
        c_name_dict.push_back(g_dicts["customer_c_name"].encode(fields[1]));
        c_address_dict.push_back(g_dicts["customer_c_address"].encode(fields[2]));
        c_nationkey.push_back(std::stoi(fields[3]));
        c_phone_dict.push_back(g_dicts["customer_c_phone"].encode(fields[4]));
        c_acctbal.push_back(parse_decimal(fields[5], 100));
        c_mktsegment_dict.push_back(g_dicts["customer_c_mktsegment"].encode(fields[6]));
        c_comment_dict.push_back(g_dicts["customer_c_comment"].encode(fields[7]));
    }
    in.close();

    string customer_dir = output_dir + "/customer";
    fs::create_directories(customer_dir);

    write_binary_file(customer_dir + "/c_custkey.bin", c_custkey.data(), c_custkey.size() * sizeof(int32_t));
    write_binary_file(customer_dir + "/c_name.bin", c_name_dict.data(), c_name_dict.size() * sizeof(int32_t));
    write_binary_file(customer_dir + "/c_address.bin", c_address_dict.data(), c_address_dict.size() * sizeof(int32_t));
    write_binary_file(customer_dir + "/c_nationkey.bin", c_nationkey.data(), c_nationkey.size() * sizeof(int32_t));
    write_binary_file(customer_dir + "/c_phone.bin", c_phone_dict.data(), c_phone_dict.size() * sizeof(int32_t));
    write_binary_file(customer_dir + "/c_acctbal.bin", c_acctbal.data(), c_acctbal.size() * sizeof(int64_t));
    write_binary_file(customer_dir + "/c_mktsegment.bin", c_mktsegment_dict.data(), c_mktsegment_dict.size() * sizeof(int32_t));
    write_binary_file(customer_dir + "/c_comment.bin", c_comment_dict.data(), c_comment_dict.size() * sizeof(int32_t));

    std::cout << "  Ingested " << c_custkey.size() << " rows\n";
}

// Ingest orders table
void ingest_orders(const string& input_dir, const string& output_dir) {
    std::cout << "Ingesting orders...\n";

    vector<int32_t> o_orderkey;
    vector<int32_t> o_custkey;
    vector<int32_t> o_orderstatus_dict;
    vector<int64_t> o_totalprice;
    vector<int32_t> o_orderdate;
    vector<int32_t> o_orderpriority_dict;
    vector<int32_t> o_clerk_dict;
    vector<int32_t> o_shippriority;
    vector<int32_t> o_comment_dict;

    string input_file = input_dir + "/orders.tbl";
    std::ifstream in(input_file);
    if (!in) {
        std::cerr << "Cannot open " << input_file << "\n";
        return;
    }

    string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        vector<string> fields = split_line(line, '|');
        if (fields.size() < 9) continue;

        o_orderkey.push_back(std::stoi(fields[0]));
        o_custkey.push_back(std::stoi(fields[1]));
        o_orderstatus_dict.push_back(g_dicts["orders_o_orderstatus"].encode(fields[2]));
        o_totalprice.push_back(parse_decimal(fields[3], 100));
        o_orderdate.push_back(parse_date(fields[4]));
        o_orderpriority_dict.push_back(g_dicts["orders_o_orderpriority"].encode(fields[5]));
        o_clerk_dict.push_back(g_dicts["orders_o_clerk"].encode(fields[6]));
        o_shippriority.push_back(std::stoi(fields[7]));
        o_comment_dict.push_back(g_dicts["orders_o_comment"].encode(fields[8]));
    }
    in.close();

    string orders_dir = output_dir + "/orders";
    fs::create_directories(orders_dir);

    write_binary_file(orders_dir + "/o_orderkey.bin", o_orderkey.data(), o_orderkey.size() * sizeof(int32_t));
    write_binary_file(orders_dir + "/o_custkey.bin", o_custkey.data(), o_custkey.size() * sizeof(int32_t));
    write_binary_file(orders_dir + "/o_orderstatus.bin", o_orderstatus_dict.data(), o_orderstatus_dict.size() * sizeof(int32_t));
    write_binary_file(orders_dir + "/o_totalprice.bin", o_totalprice.data(), o_totalprice.size() * sizeof(int64_t));
    write_binary_file(orders_dir + "/o_orderdate.bin", o_orderdate.data(), o_orderdate.size() * sizeof(int32_t));
    write_binary_file(orders_dir + "/o_orderpriority.bin", o_orderpriority_dict.data(), o_orderpriority_dict.size() * sizeof(int32_t));
    write_binary_file(orders_dir + "/o_clerk.bin", o_clerk_dict.data(), o_clerk_dict.size() * sizeof(int32_t));
    write_binary_file(orders_dir + "/o_shippriority.bin", o_shippriority.data(), o_shippriority.size() * sizeof(int32_t));
    write_binary_file(orders_dir + "/o_comment.bin", o_comment_dict.data(), o_comment_dict.size() * sizeof(int32_t));

    std::cout << "  Ingested " << o_orderkey.size() << " rows\n";
}

// Ingest lineitem table (largest, sequential scan)
void ingest_lineitem(const string& input_dir, const string& output_dir) {
    std::cout << "Ingesting lineitem...\n";

    vector<int32_t> l_orderkey;
    vector<int32_t> l_partkey;
    vector<int32_t> l_suppkey;
    vector<int32_t> l_linenumber;
    vector<int64_t> l_quantity;
    vector<int64_t> l_extendedprice;
    vector<int64_t> l_discount;
    vector<int64_t> l_tax;
    vector<int32_t> l_returnflag_dict;
    vector<int32_t> l_linestatus_dict;
    vector<int32_t> l_shipdate;
    vector<int32_t> l_commitdate;
    vector<int32_t> l_receiptdate;
    vector<int32_t> l_shipinstruct_dict;
    vector<int32_t> l_shipmode_dict;
    vector<int32_t> l_comment_dict;

    string input_file = input_dir + "/lineitem.tbl";
    std::ifstream in(input_file);
    if (!in) {
        std::cerr << "Cannot open " << input_file << "\n";
        return;
    }

    size_t row_count = 0;
    string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        vector<string> fields = split_line(line, '|');
        if (fields.size() < 16) continue;

        l_orderkey.push_back(std::stoi(fields[0]));
        l_partkey.push_back(std::stoi(fields[1]));
        l_suppkey.push_back(std::stoi(fields[2]));
        l_linenumber.push_back(std::stoi(fields[3]));
        l_quantity.push_back(parse_decimal(fields[4], 100));
        l_extendedprice.push_back(parse_decimal(fields[5], 100));
        l_discount.push_back(parse_decimal(fields[6], 100));
        l_tax.push_back(parse_decimal(fields[7], 100));
        l_returnflag_dict.push_back(g_dicts["lineitem_l_returnflag"].encode(fields[8]));
        l_linestatus_dict.push_back(g_dicts["lineitem_l_linestatus"].encode(fields[9]));
        l_shipdate.push_back(parse_date(fields[10]));
        l_commitdate.push_back(parse_date(fields[11]));
        l_receiptdate.push_back(parse_date(fields[12]));
        l_shipinstruct_dict.push_back(g_dicts["lineitem_l_shipinstruct"].encode(fields[13]));
        l_shipmode_dict.push_back(g_dicts["lineitem_l_shipmode"].encode(fields[14]));
        l_comment_dict.push_back(g_dicts["lineitem_l_comment"].encode(fields[15]));

        if (++row_count % 1000000 == 0) {
            std::cout << "  Processed " << row_count << " rows...\n";
        }
    }
    in.close();

    string lineitem_dir = output_dir + "/lineitem";
    fs::create_directories(lineitem_dir);

    write_binary_file(lineitem_dir + "/l_orderkey.bin", l_orderkey.data(), l_orderkey.size() * sizeof(int32_t));
    write_binary_file(lineitem_dir + "/l_partkey.bin", l_partkey.data(), l_partkey.size() * sizeof(int32_t));
    write_binary_file(lineitem_dir + "/l_suppkey.bin", l_suppkey.data(), l_suppkey.size() * sizeof(int32_t));
    write_binary_file(lineitem_dir + "/l_linenumber.bin", l_linenumber.data(), l_linenumber.size() * sizeof(int32_t));
    write_binary_file(lineitem_dir + "/l_quantity.bin", l_quantity.data(), l_quantity.size() * sizeof(int64_t));
    write_binary_file(lineitem_dir + "/l_extendedprice.bin", l_extendedprice.data(), l_extendedprice.size() * sizeof(int64_t));
    write_binary_file(lineitem_dir + "/l_discount.bin", l_discount.data(), l_discount.size() * sizeof(int64_t));
    write_binary_file(lineitem_dir + "/l_tax.bin", l_tax.data(), l_tax.size() * sizeof(int64_t));
    write_binary_file(lineitem_dir + "/l_returnflag.bin", l_returnflag_dict.data(), l_returnflag_dict.size() * sizeof(int32_t));
    write_binary_file(lineitem_dir + "/l_linestatus.bin", l_linestatus_dict.data(), l_linestatus_dict.size() * sizeof(int32_t));
    write_binary_file(lineitem_dir + "/l_shipdate.bin", l_shipdate.data(), l_shipdate.size() * sizeof(int32_t));
    write_binary_file(lineitem_dir + "/l_commitdate.bin", l_commitdate.data(), l_commitdate.size() * sizeof(int32_t));
    write_binary_file(lineitem_dir + "/l_receiptdate.bin", l_receiptdate.data(), l_receiptdate.size() * sizeof(int32_t));
    write_binary_file(lineitem_dir + "/l_shipinstruct.bin", l_shipinstruct_dict.data(), l_shipinstruct_dict.size() * sizeof(int32_t));
    write_binary_file(lineitem_dir + "/l_shipmode.bin", l_shipmode_dict.data(), l_shipmode_dict.size() * sizeof(int32_t));
    write_binary_file(lineitem_dir + "/l_comment.bin", l_comment_dict.data(), l_comment_dict.size() * sizeof(int32_t));

    std::cout << "  Ingested " << l_orderkey.size() << " rows\n";
}

// Write dictionary files
void write_dictionaries(const string& output_dir) {
    std::cout << "Writing dictionary files...\n";

    for (auto& [dict_name, dict] : g_dicts) {
        // Extract table and column name
        size_t underscore_pos = dict_name.find('_');
        if (underscore_pos == string::npos) continue;

        string table_name = dict_name.substr(0, underscore_pos);
        string remaining = dict_name.substr(underscore_pos + 1);

        // Extract column name (skip the table prefix)
        size_t second_underscore = remaining.find('_');
        if (second_underscore == string::npos) continue;

        string col_name = remaining.substr(second_underscore + 1);

        string dict_dir = output_dir + "/" + table_name;
        fs::create_directories(dict_dir);

        string dict_file = dict_dir + "/" + col_name + "_dict.txt";
        std::ofstream out(dict_file);
        if (!out) {
            std::cerr << "Failed to write " << dict_file << "\n";
            continue;
        }

        for (const auto& s : dict.id_to_str) {
            out << s << "\n";
        }
        out.close();

        std::cout << "  Wrote " << dict_file << " (" << dict.id_to_str.size() << " entries)\n";
    }
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <input_dir> <output_dir>\n";
        return 1;
    }

    string input_dir = argv[1];
    string output_dir = argv[2];

    std::cout << "GenDB Ingestion Starting\n";
    std::cout << "Input:  " << input_dir << "\n";
    std::cout << "Output: " << output_dir << "\n\n";

    fs::create_directories(output_dir);

    // Ingest all tables
    ingest_nation(input_dir, output_dir);
    ingest_region(input_dir, output_dir);
    ingest_supplier(input_dir, output_dir);
    ingest_part(input_dir, output_dir);
    ingest_partsupp(input_dir, output_dir);
    ingest_customer(input_dir, output_dir);
    ingest_orders(input_dir, output_dir);
    ingest_lineitem(input_dir, output_dir);

    // Write all dictionaries
    write_dictionaries(output_dir);

    std::cout << "\nIngestion complete!\n";
    return 0;
}
