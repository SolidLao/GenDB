#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <cstring>
#include <cstdint>
#include <charconv>
#include <algorithm>
#include <filesystem>
#include <cassert>

namespace fs = std::filesystem;

// Date parsing: convert YYYY-MM-DD to days since 1970-01-01
int32_t parse_date(const std::string& date_str) {
    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Days since epoch (1970-01-01)
    int days = 0;

    // Add days for complete years
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Add days for complete months in current year
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[1] = 29; // Leap year
    }
    for (int m = 1; m < month; ++m) {
        days += days_in_month[m - 1];
    }

    // Add days in current month (day is 1-indexed, epoch day 0 = Jan 1)
    days += (day - 1);

    return days;
}

// Self-test: parse_date("1970-01-01") must return 0
void test_date_parsing() {
    int epoch_zero = parse_date("1970-01-01");
    assert(epoch_zero == 0 && "Date parsing failed: 1970-01-01 should be epoch day 0");
    int test_date = parse_date("1995-03-15");
    assert(test_date > 0 && test_date < 20000 && "Date parsing sanity check failed");
}

// Parse decimal (e.g., "0.04" with scale 100 -> 4)
int64_t parse_decimal(const std::string& val_str, int32_t scale) {
    double d;
    auto result = std::from_chars(val_str.data(), val_str.data() + val_str.size(), d);
    if (result.ec != std::errc()) d = 0.0;
    return static_cast<int64_t>(d * scale + 0.5); // Round to nearest int
}

// Split line by delimiter
std::vector<std::string> split(const std::string& line, char delim) {
    std::vector<std::string> fields;
    std::stringstream ss(line);
    std::string field;
    while (std::getline(ss, field, delim)) {
        fields.push_back(field);
    }
    return fields;
}

struct StringDictionary {
    std::unordered_map<std::string, int32_t> encode_map;
    std::vector<std::string> decode_vec;
    int32_t next_code = 0;

    int32_t encode(const std::string& val) {
        auto it = encode_map.find(val);
        if (it != encode_map.end()) {
            return it->second;
        }
        int32_t code = next_code++;
        encode_map[val] = code;
        decode_vec.push_back(val);
        return code;
    }

    void write_dict(const std::string& path) {
        std::ofstream f(path);
        for (int32_t i = 0; i < static_cast<int32_t>(decode_vec.size()); ++i) {
            f << i << "=" << decode_vec[i] << "\n";
        }
    }
};

// Structure to hold a row of lineitem data
struct LineitemRow {
    int32_t l_orderkey, l_partkey, l_suppkey, l_linenumber;
    int64_t l_quantity, l_extendedprice, l_discount, l_tax;
    int32_t l_returnflag, l_linestatus;
    int32_t l_shipdate, l_commitdate, l_receiptdate;
    int32_t l_shipinstruct, l_shipmode, l_comment;
};

// Structure to hold a row of orders data
struct OrdersRow {
    int32_t o_orderkey, o_custkey;
    int32_t o_orderstatus;
    int64_t o_totalprice;
    int32_t o_orderdate;
    int32_t o_orderpriority;
    int32_t o_clerk;
    int32_t o_shippriority;
    int32_t o_comment;
};

// Structure to hold a row of customer data
struct CustomerRow {
    int32_t c_custkey;
    int32_t c_name, c_address;
    int32_t c_nationkey;
    int32_t c_phone;
    int64_t c_acctbal;
    int32_t c_mktsegment;
    int32_t c_comment;
};

// Structure to hold a row of nation data
struct NationRow {
    int32_t n_nationkey;
    int32_t n_name;
    int32_t n_regionkey;
    int32_t n_comment;
};

// Structure to hold a row of region data
struct RegionRow {
    int32_t r_regionkey;
    int32_t r_name;
    int32_t r_comment;
};

// Structure to hold a row of supplier data
struct SupplierRow {
    int32_t s_suppkey;
    int32_t s_name, s_address;
    int32_t s_nationkey;
    int32_t s_phone;
    int64_t s_acctbal;
    int32_t s_comment;
};

// Structure to hold a row of part data
struct PartRow {
    int32_t p_partkey;
    int32_t p_name, p_mfgr, p_brand, p_type;
    int32_t p_size;
    int32_t p_container;
    int64_t p_retailprice;
    int32_t p_comment;
};

// Structure to hold a row of partsupp data
struct PartsuppRow {
    int32_t ps_partkey, ps_suppkey;
    int32_t ps_availqty;
    int64_t ps_supplycost;
    int32_t ps_comment;
};

template <typename T>
void write_column(const std::string& path, const std::vector<T>& col) {
    std::ofstream f(path, std::ios::binary);
    f.write(reinterpret_cast<const char*>(col.data()), col.size() * sizeof(T));
}

// Parse and ingest lineitem table
void ingest_lineitem(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting lineitem..." << std::endl;

    std::vector<LineitemRow> rows;
    StringDictionary dict_returnflag, dict_linestatus, dict_shipinstruct, dict_shipmode, dict_comment;

    std::string input_file = input_dir + "/lineitem.tbl";
    std::ifstream infile(input_file);
    if (!infile) {
        std::cerr << "Cannot open " << input_file << std::endl;
        return;
    }

    std::string line;
    size_t line_count = 0;
    while (std::getline(infile, line)) {
        if (line.empty()) continue;

        auto fields = split(line, '|');
        if (fields.size() < 16) continue;

        LineitemRow row;
        row.l_orderkey = std::stoi(fields[0]);
        row.l_partkey = std::stoi(fields[1]);
        row.l_suppkey = std::stoi(fields[2]);
        row.l_linenumber = std::stoi(fields[3]);
        row.l_quantity = parse_decimal(fields[4], 100);
        row.l_extendedprice = parse_decimal(fields[5], 100);
        row.l_discount = parse_decimal(fields[6], 100);
        row.l_tax = parse_decimal(fields[7], 100);
        row.l_returnflag = dict_returnflag.encode(fields[8]);
        row.l_linestatus = dict_linestatus.encode(fields[9]);
        row.l_shipdate = parse_date(fields[10]);
        row.l_commitdate = parse_date(fields[11]);
        row.l_receiptdate = parse_date(fields[12]);
        row.l_shipinstruct = dict_shipinstruct.encode(fields[13]);
        row.l_shipmode = dict_shipmode.encode(fields[14]);
        row.l_comment = dict_comment.encode(fields[15]);

        rows.push_back(row);
        line_count++;

        if (line_count % 10000000 == 0) {
            std::cout << "  Parsed " << line_count << " lineitem rows" << std::endl;
        }
    }
    infile.close();

    std::cout << "  Total lineitem rows: " << rows.size() << std::endl;

    // Write binary columns
    std::string table_dir = output_dir + "/lineitem";
    fs::create_directories(table_dir);

    // Extract columns
    std::vector<int32_t> col_orderkey, col_partkey, col_suppkey, col_linenumber;
    std::vector<int64_t> col_quantity, col_extendedprice, col_discount, col_tax;
    std::vector<int32_t> col_returnflag, col_linestatus;
    std::vector<int32_t> col_shipdate, col_commitdate, col_receiptdate;
    std::vector<int32_t> col_shipinstruct, col_shipmode, col_comment;

    for (const auto& row : rows) {
        col_orderkey.push_back(row.l_orderkey);
        col_partkey.push_back(row.l_partkey);
        col_suppkey.push_back(row.l_suppkey);
        col_linenumber.push_back(row.l_linenumber);
        col_quantity.push_back(row.l_quantity);
        col_extendedprice.push_back(row.l_extendedprice);
        col_discount.push_back(row.l_discount);
        col_tax.push_back(row.l_tax);
        col_returnflag.push_back(row.l_returnflag);
        col_linestatus.push_back(row.l_linestatus);
        col_shipdate.push_back(row.l_shipdate);
        col_commitdate.push_back(row.l_commitdate);
        col_receiptdate.push_back(row.l_receiptdate);
        col_shipinstruct.push_back(row.l_shipinstruct);
        col_shipmode.push_back(row.l_shipmode);
        col_comment.push_back(row.l_comment);
    }

    // Write columns
    write_column(table_dir + "/l_orderkey.bin", col_orderkey);
    write_column(table_dir + "/l_partkey.bin", col_partkey);
    write_column(table_dir + "/l_suppkey.bin", col_suppkey);
    write_column(table_dir + "/l_linenumber.bin", col_linenumber);
    write_column(table_dir + "/l_quantity.bin", col_quantity);
    write_column(table_dir + "/l_extendedprice.bin", col_extendedprice);
    write_column(table_dir + "/l_discount.bin", col_discount);
    write_column(table_dir + "/l_tax.bin", col_tax);
    write_column(table_dir + "/l_returnflag.bin", col_returnflag);
    write_column(table_dir + "/l_linestatus.bin", col_linestatus);
    write_column(table_dir + "/l_shipdate.bin", col_shipdate);
    write_column(table_dir + "/l_commitdate.bin", col_commitdate);
    write_column(table_dir + "/l_receiptdate.bin", col_receiptdate);
    write_column(table_dir + "/l_shipinstruct.bin", col_shipinstruct);
    write_column(table_dir + "/l_shipmode.bin", col_shipmode);
    write_column(table_dir + "/l_comment.bin", col_comment);

    // Write dictionaries
    dict_returnflag.write_dict(table_dir + "/l_returnflag_dict.txt");
    dict_linestatus.write_dict(table_dir + "/l_linestatus_dict.txt");
    dict_shipinstruct.write_dict(table_dir + "/l_shipinstruct_dict.txt");
    dict_shipmode.write_dict(table_dir + "/l_shipmode_dict.txt");
    dict_comment.write_dict(table_dir + "/l_comment_dict.txt");

    std::cout << "  Lineitem ingestion complete" << std::endl;
}

// Parse and ingest orders table
void ingest_orders(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting orders..." << std::endl;

    std::vector<OrdersRow> rows;
    StringDictionary dict_status, dict_priority, dict_clerk, dict_comment;

    std::string input_file = input_dir + "/orders.tbl";
    std::ifstream infile(input_file);
    if (!infile) {
        std::cerr << "Cannot open " << input_file << std::endl;
        return;
    }

    std::string line;
    size_t line_count = 0;
    while (std::getline(infile, line)) {
        if (line.empty()) continue;

        auto fields = split(line, '|');
        if (fields.size() < 9) continue;

        OrdersRow row;
        row.o_orderkey = std::stoi(fields[0]);
        row.o_custkey = std::stoi(fields[1]);
        row.o_orderstatus = dict_status.encode(fields[2]);
        row.o_totalprice = parse_decimal(fields[3], 100);
        row.o_orderdate = parse_date(fields[4]);
        row.o_orderpriority = dict_priority.encode(fields[5]);
        row.o_clerk = dict_clerk.encode(fields[6]);
        row.o_shippriority = std::stoi(fields[7]);
        row.o_comment = dict_comment.encode(fields[8]);

        rows.push_back(row);
        line_count++;

        if (line_count % 1000000 == 0) {
            std::cout << "  Parsed " << line_count << " orders rows" << std::endl;
        }
    }
    infile.close();

    std::cout << "  Total orders rows: " << rows.size() << std::endl;

    // Write binary columns
    std::string table_dir = output_dir + "/orders";
    fs::create_directories(table_dir);

    // Extract columns
    std::vector<int32_t> col_orderkey, col_custkey;
    std::vector<int32_t> col_status;
    std::vector<int64_t> col_totalprice;
    std::vector<int32_t> col_orderdate;
    std::vector<int32_t> col_priority;
    std::vector<int32_t> col_clerk;
    std::vector<int32_t> col_shippriority;
    std::vector<int32_t> col_comment;

    for (const auto& row : rows) {
        col_orderkey.push_back(row.o_orderkey);
        col_custkey.push_back(row.o_custkey);
        col_status.push_back(row.o_orderstatus);
        col_totalprice.push_back(row.o_totalprice);
        col_orderdate.push_back(row.o_orderdate);
        col_priority.push_back(row.o_orderpriority);
        col_clerk.push_back(row.o_clerk);
        col_shippriority.push_back(row.o_shippriority);
        col_comment.push_back(row.o_comment);
    }

    // Write columns
    write_column(table_dir + "/o_orderkey.bin", col_orderkey);
    write_column(table_dir + "/o_custkey.bin", col_custkey);
    write_column(table_dir + "/o_orderstatus.bin", col_status);
    write_column(table_dir + "/o_totalprice.bin", col_totalprice);
    write_column(table_dir + "/o_orderdate.bin", col_orderdate);
    write_column(table_dir + "/o_orderpriority.bin", col_priority);
    write_column(table_dir + "/o_clerk.bin", col_clerk);
    write_column(table_dir + "/o_shippriority.bin", col_shippriority);
    write_column(table_dir + "/o_comment.bin", col_comment);

    // Write dictionaries
    dict_status.write_dict(table_dir + "/o_orderstatus_dict.txt");
    dict_priority.write_dict(table_dir + "/o_orderpriority_dict.txt");
    dict_clerk.write_dict(table_dir + "/o_clerk_dict.txt");
    dict_comment.write_dict(table_dir + "/o_comment_dict.txt");

    std::cout << "  Orders ingestion complete" << std::endl;
}

// Parse and ingest customer table
void ingest_customer(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting customer..." << std::endl;

    std::vector<CustomerRow> rows;
    StringDictionary dict_name, dict_address, dict_phone, dict_mktseg, dict_comment;

    std::string input_file = input_dir + "/customer.tbl";
    std::ifstream infile(input_file);
    if (!infile) {
        std::cerr << "Cannot open " << input_file << std::endl;
        return;
    }

    std::string line;
    size_t line_count = 0;
    while (std::getline(infile, line)) {
        if (line.empty()) continue;

        auto fields = split(line, '|');
        if (fields.size() < 8) continue;

        CustomerRow row;
        row.c_custkey = std::stoi(fields[0]);
        row.c_name = dict_name.encode(fields[1]);
        row.c_address = dict_address.encode(fields[2]);
        row.c_nationkey = std::stoi(fields[3]);
        row.c_phone = dict_phone.encode(fields[4]);
        row.c_acctbal = parse_decimal(fields[5], 100);
        row.c_mktsegment = dict_mktseg.encode(fields[6]);
        row.c_comment = dict_comment.encode(fields[7]);

        rows.push_back(row);
        line_count++;

        if (line_count % 100000 == 0) {
            std::cout << "  Parsed " << line_count << " customer rows" << std::endl;
        }
    }
    infile.close();

    std::cout << "  Total customer rows: " << rows.size() << std::endl;

    // Write binary columns
    std::string table_dir = output_dir + "/customer";
    fs::create_directories(table_dir);

    // Extract columns
    std::vector<int32_t> col_custkey;
    std::vector<int32_t> col_name, col_address;
    std::vector<int32_t> col_nationkey;
    std::vector<int32_t> col_phone;
    std::vector<int64_t> col_acctbal;
    std::vector<int32_t> col_mktseg;
    std::vector<int32_t> col_comment;

    for (const auto& row : rows) {
        col_custkey.push_back(row.c_custkey);
        col_name.push_back(row.c_name);
        col_address.push_back(row.c_address);
        col_nationkey.push_back(row.c_nationkey);
        col_phone.push_back(row.c_phone);
        col_acctbal.push_back(row.c_acctbal);
        col_mktseg.push_back(row.c_mktsegment);
        col_comment.push_back(row.c_comment);
    }

    // Write columns
    write_column(table_dir + "/c_custkey.bin", col_custkey);
    write_column(table_dir + "/c_name.bin", col_name);
    write_column(table_dir + "/c_address.bin", col_address);
    write_column(table_dir + "/c_nationkey.bin", col_nationkey);
    write_column(table_dir + "/c_phone.bin", col_phone);
    write_column(table_dir + "/c_acctbal.bin", col_acctbal);
    write_column(table_dir + "/c_mktsegment.bin", col_mktseg);
    write_column(table_dir + "/c_comment.bin", col_comment);

    // Write dictionaries
    dict_name.write_dict(table_dir + "/c_name_dict.txt");
    dict_address.write_dict(table_dir + "/c_address_dict.txt");
    dict_phone.write_dict(table_dir + "/c_phone_dict.txt");
    dict_mktseg.write_dict(table_dir + "/c_mktsegment_dict.txt");
    dict_comment.write_dict(table_dir + "/c_comment_dict.txt");

    std::cout << "  Customer ingestion complete" << std::endl;
}

// Parse and ingest nation table
void ingest_nation(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting nation..." << std::endl;

    std::vector<NationRow> rows;
    StringDictionary dict_name, dict_comment;

    std::string input_file = input_dir + "/nation.tbl";
    std::ifstream infile(input_file);
    if (!infile) {
        std::cerr << "Cannot open " << input_file << std::endl;
        return;
    }

    std::string line;
    while (std::getline(infile, line)) {
        if (line.empty()) continue;

        auto fields = split(line, '|');
        if (fields.size() < 4) continue;

        NationRow row;
        row.n_nationkey = std::stoi(fields[0]);
        row.n_name = dict_name.encode(fields[1]);
        row.n_regionkey = std::stoi(fields[2]);
        row.n_comment = dict_comment.encode(fields[3]);

        rows.push_back(row);
    }
    infile.close();

    std::cout << "  Total nation rows: " << rows.size() << std::endl;

    // Write binary columns
    std::string table_dir = output_dir + "/nation";
    fs::create_directories(table_dir);

    // Extract columns
    std::vector<int32_t> col_nationkey;
    std::vector<int32_t> col_name;
    std::vector<int32_t> col_regionkey;
    std::vector<int32_t> col_comment;

    for (const auto& row : rows) {
        col_nationkey.push_back(row.n_nationkey);
        col_name.push_back(row.n_name);
        col_regionkey.push_back(row.n_regionkey);
        col_comment.push_back(row.n_comment);
    }

    // Write columns
    write_column(table_dir + "/n_nationkey.bin", col_nationkey);
    write_column(table_dir + "/n_name.bin", col_name);
    write_column(table_dir + "/n_regionkey.bin", col_regionkey);
    write_column(table_dir + "/n_comment.bin", col_comment);

    // Write dictionaries
    dict_name.write_dict(table_dir + "/n_name_dict.txt");
    dict_comment.write_dict(table_dir + "/n_comment_dict.txt");

    std::cout << "  Nation ingestion complete" << std::endl;
}

// Parse and ingest region table
void ingest_region(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting region..." << std::endl;

    std::vector<RegionRow> rows;
    StringDictionary dict_name, dict_comment;

    std::string input_file = input_dir + "/region.tbl";
    std::ifstream infile(input_file);
    if (!infile) {
        std::cerr << "Cannot open " << input_file << std::endl;
        return;
    }

    std::string line;
    while (std::getline(infile, line)) {
        if (line.empty()) continue;

        auto fields = split(line, '|');
        if (fields.size() < 3) continue;

        RegionRow row;
        row.r_regionkey = std::stoi(fields[0]);
        row.r_name = dict_name.encode(fields[1]);
        row.r_comment = dict_comment.encode(fields[2]);

        rows.push_back(row);
    }
    infile.close();

    std::cout << "  Total region rows: " << rows.size() << std::endl;

    // Write binary columns
    std::string table_dir = output_dir + "/region";
    fs::create_directories(table_dir);

    // Extract columns
    std::vector<int32_t> col_regionkey;
    std::vector<int32_t> col_name;
    std::vector<int32_t> col_comment;

    for (const auto& row : rows) {
        col_regionkey.push_back(row.r_regionkey);
        col_name.push_back(row.r_name);
        col_comment.push_back(row.r_comment);
    }

    // Write columns
    write_column(table_dir + "/r_regionkey.bin", col_regionkey);
    write_column(table_dir + "/r_name.bin", col_name);
    write_column(table_dir + "/r_comment.bin", col_comment);

    // Write dictionaries
    dict_name.write_dict(table_dir + "/r_name_dict.txt");
    dict_comment.write_dict(table_dir + "/r_comment_dict.txt");

    std::cout << "  Region ingestion complete" << std::endl;
}

// Parse and ingest supplier table
void ingest_supplier(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting supplier..." << std::endl;

    std::vector<SupplierRow> rows;
    StringDictionary dict_name, dict_address, dict_phone, dict_comment;

    std::string input_file = input_dir + "/supplier.tbl";
    std::ifstream infile(input_file);
    if (!infile) {
        std::cerr << "Cannot open " << input_file << std::endl;
        return;
    }

    std::string line;
    size_t line_count = 0;
    while (std::getline(infile, line)) {
        if (line.empty()) continue;

        auto fields = split(line, '|');
        if (fields.size() < 7) continue;

        SupplierRow row;
        row.s_suppkey = std::stoi(fields[0]);
        row.s_name = dict_name.encode(fields[1]);
        row.s_address = dict_address.encode(fields[2]);
        row.s_nationkey = std::stoi(fields[3]);
        row.s_phone = dict_phone.encode(fields[4]);
        row.s_acctbal = parse_decimal(fields[5], 100);
        row.s_comment = dict_comment.encode(fields[6]);

        rows.push_back(row);
        line_count++;

        if (line_count % 10000 == 0) {
            std::cout << "  Parsed " << line_count << " supplier rows" << std::endl;
        }
    }
    infile.close();

    std::cout << "  Total supplier rows: " << rows.size() << std::endl;

    // Write binary columns
    std::string table_dir = output_dir + "/supplier";
    fs::create_directories(table_dir);

    // Extract columns
    std::vector<int32_t> col_suppkey;
    std::vector<int32_t> col_name, col_address;
    std::vector<int32_t> col_nationkey;
    std::vector<int32_t> col_phone;
    std::vector<int64_t> col_acctbal;
    std::vector<int32_t> col_comment;

    for (const auto& row : rows) {
        col_suppkey.push_back(row.s_suppkey);
        col_name.push_back(row.s_name);
        col_address.push_back(row.s_address);
        col_nationkey.push_back(row.s_nationkey);
        col_phone.push_back(row.s_phone);
        col_acctbal.push_back(row.s_acctbal);
        col_comment.push_back(row.s_comment);
    }

    // Write columns
    write_column(table_dir + "/s_suppkey.bin", col_suppkey);
    write_column(table_dir + "/s_name.bin", col_name);
    write_column(table_dir + "/s_address.bin", col_address);
    write_column(table_dir + "/s_nationkey.bin", col_nationkey);
    write_column(table_dir + "/s_phone.bin", col_phone);
    write_column(table_dir + "/s_acctbal.bin", col_acctbal);
    write_column(table_dir + "/s_comment.bin", col_comment);

    // Write dictionaries
    dict_name.write_dict(table_dir + "/s_name_dict.txt");
    dict_address.write_dict(table_dir + "/s_address_dict.txt");
    dict_phone.write_dict(table_dir + "/s_phone_dict.txt");
    dict_comment.write_dict(table_dir + "/s_comment_dict.txt");

    std::cout << "  Supplier ingestion complete" << std::endl;
}

// Parse and ingest part table
void ingest_part(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting part..." << std::endl;

    std::vector<PartRow> rows;
    StringDictionary dict_name, dict_mfgr, dict_brand, dict_type, dict_container, dict_comment;

    std::string input_file = input_dir + "/part.tbl";
    std::ifstream infile(input_file);
    if (!infile) {
        std::cerr << "Cannot open " << input_file << std::endl;
        return;
    }

    std::string line;
    size_t line_count = 0;
    while (std::getline(infile, line)) {
        if (line.empty()) continue;

        auto fields = split(line, '|');
        if (fields.size() < 9) continue;

        PartRow row;
        row.p_partkey = std::stoi(fields[0]);
        row.p_name = dict_name.encode(fields[1]);
        row.p_mfgr = dict_mfgr.encode(fields[2]);
        row.p_brand = dict_brand.encode(fields[3]);
        row.p_type = dict_type.encode(fields[4]);
        row.p_size = std::stoi(fields[5]);
        row.p_container = dict_container.encode(fields[6]);
        row.p_retailprice = parse_decimal(fields[7], 100);
        row.p_comment = dict_comment.encode(fields[8]);

        rows.push_back(row);
        line_count++;

        if (line_count % 100000 == 0) {
            std::cout << "  Parsed " << line_count << " part rows" << std::endl;
        }
    }
    infile.close();

    std::cout << "  Total part rows: " << rows.size() << std::endl;

    // Write binary columns
    std::string table_dir = output_dir + "/part";
    fs::create_directories(table_dir);

    // Extract columns
    std::vector<int32_t> col_partkey;
    std::vector<int32_t> col_name, col_mfgr, col_brand, col_type;
    std::vector<int32_t> col_size;
    std::vector<int32_t> col_container;
    std::vector<int64_t> col_retailprice;
    std::vector<int32_t> col_comment;

    for (const auto& row : rows) {
        col_partkey.push_back(row.p_partkey);
        col_name.push_back(row.p_name);
        col_mfgr.push_back(row.p_mfgr);
        col_brand.push_back(row.p_brand);
        col_type.push_back(row.p_type);
        col_size.push_back(row.p_size);
        col_container.push_back(row.p_container);
        col_retailprice.push_back(row.p_retailprice);
        col_comment.push_back(row.p_comment);
    }

    // Write columns
    write_column(table_dir + "/p_partkey.bin", col_partkey);
    write_column(table_dir + "/p_name.bin", col_name);
    write_column(table_dir + "/p_mfgr.bin", col_mfgr);
    write_column(table_dir + "/p_brand.bin", col_brand);
    write_column(table_dir + "/p_type.bin", col_type);
    write_column(table_dir + "/p_size.bin", col_size);
    write_column(table_dir + "/p_container.bin", col_container);
    write_column(table_dir + "/p_retailprice.bin", col_retailprice);
    write_column(table_dir + "/p_comment.bin", col_comment);

    // Write dictionaries
    dict_name.write_dict(table_dir + "/p_name_dict.txt");
    dict_mfgr.write_dict(table_dir + "/p_mfgr_dict.txt");
    dict_brand.write_dict(table_dir + "/p_brand_dict.txt");
    dict_type.write_dict(table_dir + "/p_type_dict.txt");
    dict_container.write_dict(table_dir + "/p_container_dict.txt");
    dict_comment.write_dict(table_dir + "/p_comment_dict.txt");

    std::cout << "  Part ingestion complete" << std::endl;
}

// Parse and ingest partsupp table
void ingest_partsupp(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting partsupp..." << std::endl;

    std::vector<PartsuppRow> rows;
    StringDictionary dict_comment;

    std::string input_file = input_dir + "/partsupp.tbl";
    std::ifstream infile(input_file);
    if (!infile) {
        std::cerr << "Cannot open " << input_file << std::endl;
        return;
    }

    std::string line;
    size_t line_count = 0;
    while (std::getline(infile, line)) {
        if (line.empty()) continue;

        auto fields = split(line, '|');
        if (fields.size() < 5) continue;

        PartsuppRow row;
        row.ps_partkey = std::stoi(fields[0]);
        row.ps_suppkey = std::stoi(fields[1]);
        row.ps_availqty = std::stoi(fields[2]);
        row.ps_supplycost = parse_decimal(fields[3], 100);
        row.ps_comment = dict_comment.encode(fields[4]);

        rows.push_back(row);
        line_count++;

        if (line_count % 1000000 == 0) {
            std::cout << "  Parsed " << line_count << " partsupp rows" << std::endl;
        }
    }
    infile.close();

    std::cout << "  Total partsupp rows: " << rows.size() << std::endl;

    // Write binary columns
    std::string table_dir = output_dir + "/partsupp";
    fs::create_directories(table_dir);

    // Extract columns
    std::vector<int32_t> col_partkey, col_suppkey;
    std::vector<int32_t> col_availqty;
    std::vector<int64_t> col_supplycost;
    std::vector<int32_t> col_comment;

    for (const auto& row : rows) {
        col_partkey.push_back(row.ps_partkey);
        col_suppkey.push_back(row.ps_suppkey);
        col_availqty.push_back(row.ps_availqty);
        col_supplycost.push_back(row.ps_supplycost);
        col_comment.push_back(row.ps_comment);
    }

    // Write columns
    write_column(table_dir + "/ps_partkey.bin", col_partkey);
    write_column(table_dir + "/ps_suppkey.bin", col_suppkey);
    write_column(table_dir + "/ps_availqty.bin", col_availqty);
    write_column(table_dir + "/ps_supplycost.bin", col_supplycost);
    write_column(table_dir + "/ps_comment.bin", col_comment);

    // Write dictionaries
    dict_comment.write_dict(table_dir + "/ps_comment_dict.txt");

    std::cout << "  Partsupp ingestion complete" << std::endl;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <input_dir> <output_dir>" << std::endl;
        return 1;
    }

    std::string input_dir = argv[1];
    std::string output_dir = argv[2];

    // Test date parsing before ingestion
    test_date_parsing();
    std::cout << "Date parsing self-test passed" << std::endl;

    // Create output directory
    fs::create_directories(output_dir);

    // Ingest all tables
    ingest_lineitem(input_dir, output_dir);
    ingest_orders(input_dir, output_dir);
    ingest_customer(input_dir, output_dir);
    ingest_nation(input_dir, output_dir);
    ingest_region(input_dir, output_dir);
    ingest_supplier(input_dir, output_dir);
    ingest_part(input_dir, output_dir);
    ingest_partsupp(input_dir, output_dir);

    std::cout << "Ingestion complete!" << std::endl;
    return 0;
}
