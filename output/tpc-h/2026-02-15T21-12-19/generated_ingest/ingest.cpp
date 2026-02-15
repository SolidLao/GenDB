#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <charconv>
#include <cstring>
#include <filesystem>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <algorithm>
#include <cmath>

namespace fs = std::filesystem;

// Date parsing: days since 1970-01-01
int32_t parse_date(const std::string& date_str) {
    if (date_str.length() < 10) return 0;

    // YYYY-MM-DD
    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Days since epoch (1970-01-01 = 0)
    int32_t days = 0;

    // Add days for complete years from 1970 to year-1
    for (int y = 1970; y < year; ++y) {
        bool is_leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += is_leap ? 366 : 365;
    }

    // Add days for complete months in current year
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (is_leap) days_in_month[1] = 29;

    for (int m = 1; m < month; ++m) {
        days += days_in_month[m - 1];
    }

    // Add remaining days (day - 1 because days are 1-indexed, epoch day 0 = Jan 1)
    days += (day - 1);

    return days;
}

// Decimal parsing: parse double, multiply by scale_factor, round to int64_t
int64_t parse_decimal(const std::string& decimal_str, int scale_factor) {
    double val = 0.0;
    auto [ptr, ec] = std::from_chars(decimal_str.data(), decimal_str.data() + decimal_str.length(), val);
    if (ec != std::errc()) return 0;
    return static_cast<int64_t>(std::round(val * scale_factor));
}

struct LineitemRow {
    int32_t l_orderkey, l_partkey, l_suppkey, l_linenumber;
    int64_t l_quantity, l_extendedprice, l_discount, l_tax;
    int32_t l_returnflag, l_linestatus;
    int32_t l_shipdate, l_commitdate, l_receiptdate;
    int32_t l_shipinstruct, l_shipmode;
    std::string l_comment;
};

struct OrdersRow {
    int32_t o_orderkey, o_custkey;
    int32_t o_orderstatus;
    int64_t o_totalprice;
    int32_t o_orderdate;
    int32_t o_orderpriority;
    int32_t o_clerk;
    int32_t o_shippriority;
    std::string o_comment;
};

struct CustomerRow {
    int32_t c_custkey;
    std::string c_name;
    std::string c_address;
    int32_t c_nationkey;
    std::string c_phone;
    int64_t c_acctbal;
    std::string c_mktsegment;
    std::string c_comment;
};

struct NationRow {
    int32_t n_nationkey;
    std::string n_name;
    int32_t n_regionkey;
    std::string n_comment;
};

struct RegionRow {
    int32_t r_regionkey;
    std::string r_name;
    std::string r_comment;
};

struct SupplierRow {
    int32_t s_suppkey;
    std::string s_name;
    std::string s_address;
    int32_t s_nationkey;
    std::string s_phone;
    int64_t s_acctbal;
    std::string s_comment;
};

struct PartRow {
    int32_t p_partkey;
    std::string p_name;
    int32_t p_mfgr;
    int32_t p_brand;
    std::string p_type;
    int32_t p_size;
    int32_t p_container;
    int64_t p_retailprice;
    std::string p_comment;
};

struct PartSuppRow {
    int32_t ps_partkey, ps_suppkey;
    int32_t ps_availqty;
    int64_t ps_supplycost;
    std::string ps_comment;
};

// Dictionary encoder: assigns codes to unique values
class DictionaryEncoder {
public:
    int32_t encode(const std::string& value) {
        auto it = value_to_code.find(value);
        if (it == value_to_code.end()) {
            int32_t code = static_cast<int32_t>(code_to_value.size());
            value_to_code[value] = code;
            code_to_value.push_back(value);
            return code;
        }
        return it->second;
    }

    void write_dictionary(const std::string& output_dir, const std::string& column_name) {
        std::string dict_path = output_dir + "/" + column_name + "_dict.txt";
        std::ofstream f(dict_path);
        for (size_t i = 0; i < code_to_value.size(); ++i) {
            f << i << "=" << code_to_value[i] << "\n";
        }
        f.close();
    }

private:
    std::unordered_map<std::string, int32_t> value_to_code;
    std::vector<std::string> code_to_value;
};

// Column writers for each data type
template<typename T>
void write_column(const std::string& path, const std::vector<T>& column) {
    std::ofstream f(path, std::ios::binary);
    f.write(reinterpret_cast<const char*>(column.data()), column.size() * sizeof(T));
    f.close();
}

void write_column_strings(const std::string& path, const std::vector<std::string>& column) {
    std::ofstream f(path, std::ios::binary);
    for (const auto& s : column) {
        uint32_t len = s.length();
        f.write(reinterpret_cast<const char*>(&len), sizeof(len));
        f.write(s.c_str(), len);
    }
    f.close();
}

// Parse lineitem.tbl
std::vector<LineitemRow> parse_lineitem(const std::string& input_path) {
    std::vector<LineitemRow> rows;
    std::ifstream f(input_path);
    std::string line;

    while (std::getline(f, line)) {
        if (line.empty()) continue;

        std::vector<std::string> fields;
        size_t start = 0;
        while (start < line.length()) {
            size_t end = line.find('|', start);
            if (end == std::string::npos) end = line.length();
            fields.push_back(line.substr(start, end - start));
            start = end + 1;
        }

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
        row.l_returnflag = fields[8][0];
        row.l_linestatus = fields[9][0];
        row.l_shipdate = parse_date(fields[10]);
        row.l_commitdate = parse_date(fields[11]);
        row.l_receiptdate = parse_date(fields[12]);
        row.l_shipinstruct = fields[13].length() > 0 ? fields[13][0] : ' ';
        row.l_shipmode = fields[14].length() > 0 ? fields[14][0] : ' ';
        row.l_comment = fields[15];

        rows.push_back(row);
    }

    return rows;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: ingest <input_dir> <output_dir>\n";
        return 1;
    }

    std::string input_dir = argv[1];
    std::string output_dir = argv[2];

    // Self-test: parse_date
    {
        int32_t epoch_zero = parse_date("1970-01-01");
        if (epoch_zero != 0) {
            std::cerr << "FATAL: parse_date('1970-01-01') = " << epoch_zero << " (expected 0)\n";
            return 1;
        }
        std::cout << "✓ parse_date self-test passed\n";
    }

    // Create output directory
    fs::create_directories(output_dir);

    std::cout << "Ingesting TPC-H tables...\n";

    // === LINEITEM ===
    std::cout << "  Parsing lineitem.tbl...\n";
    auto lineitem_rows = parse_lineitem(input_dir + "/lineitem.tbl");
    std::cout << "    Loaded " << lineitem_rows.size() << " rows\n";

    // Dictionary encoding for returnflag, linestatus, shipinstruct, shipmode
    DictionaryEncoder enc_returnflag, enc_linestatus, enc_shipinstruct, enc_shipmode;

    std::vector<int32_t> l_orderkey, l_partkey, l_suppkey, l_linenumber;
    std::vector<int64_t> l_quantity, l_extendedprice, l_discount, l_tax;
    std::vector<int32_t> l_returnflag_codes, l_linestatus_codes;
    std::vector<int32_t> l_shipdate, l_commitdate, l_receiptdate;
    std::vector<int32_t> l_shipinstruct_codes, l_shipmode_codes;
    std::vector<std::string> l_comment;

    // Sort by shipdate before writing
    std::sort(lineitem_rows.begin(), lineitem_rows.end(),
        [](const LineitemRow& a, const LineitemRow& b) {
            return a.l_shipdate < b.l_shipdate;
        });

    for (const auto& row : lineitem_rows) {
        l_orderkey.push_back(row.l_orderkey);
        l_partkey.push_back(row.l_partkey);
        l_suppkey.push_back(row.l_suppkey);
        l_linenumber.push_back(row.l_linenumber);
        l_quantity.push_back(row.l_quantity);
        l_extendedprice.push_back(row.l_extendedprice);
        l_discount.push_back(row.l_discount);
        l_tax.push_back(row.l_tax);
        l_returnflag_codes.push_back(enc_returnflag.encode(std::string(1, row.l_returnflag)));
        l_linestatus_codes.push_back(enc_linestatus.encode(std::string(1, row.l_linestatus)));
        l_shipdate.push_back(row.l_shipdate);
        l_commitdate.push_back(row.l_commitdate);
        l_receiptdate.push_back(row.l_receiptdate);
        l_shipinstruct_codes.push_back(enc_shipinstruct.encode(std::string(1, row.l_shipinstruct)));
        l_shipmode_codes.push_back(enc_shipmode.encode(std::string(1, row.l_shipmode)));
        l_comment.push_back(row.l_comment);
    }

    fs::create_directories(output_dir + "/lineitem");

    std::cout << "  Writing lineitem columns...\n";
    write_column(output_dir + "/lineitem/l_orderkey.bin", l_orderkey);
    write_column(output_dir + "/lineitem/l_partkey.bin", l_partkey);
    write_column(output_dir + "/lineitem/l_suppkey.bin", l_suppkey);
    write_column(output_dir + "/lineitem/l_linenumber.bin", l_linenumber);
    write_column(output_dir + "/lineitem/l_quantity.bin", l_quantity);
    write_column(output_dir + "/lineitem/l_extendedprice.bin", l_extendedprice);
    write_column(output_dir + "/lineitem/l_discount.bin", l_discount);
    write_column(output_dir + "/lineitem/l_tax.bin", l_tax);
    write_column(output_dir + "/lineitem/l_returnflag.bin", l_returnflag_codes);
    enc_returnflag.write_dictionary(output_dir + "/lineitem", "l_returnflag");
    write_column(output_dir + "/lineitem/l_linestatus.bin", l_linestatus_codes);
    enc_linestatus.write_dictionary(output_dir + "/lineitem", "l_linestatus");
    write_column(output_dir + "/lineitem/l_shipdate.bin", l_shipdate);
    write_column(output_dir + "/lineitem/l_commitdate.bin", l_commitdate);
    write_column(output_dir + "/lineitem/l_receiptdate.bin", l_receiptdate);
    write_column(output_dir + "/lineitem/l_shipinstruct.bin", l_shipinstruct_codes);
    enc_shipinstruct.write_dictionary(output_dir + "/lineitem", "l_shipinstruct");
    write_column(output_dir + "/lineitem/l_shipmode.bin", l_shipmode_codes);
    enc_shipmode.write_dictionary(output_dir + "/lineitem", "l_shipmode");
    write_column_strings(output_dir + "/lineitem/l_comment.bin", l_comment);

    // Post-ingestion checks
    {
        bool all_valid = true;
        int sample_count = std::min(size_t(100), l_shipdate.size());
        for (int i = 0; i < sample_count; ++i) {
            if (l_shipdate[i] > 3000 || l_shipdate[i] < 0) {
                std::cerr << "WARNING: invalid date at lineitem[" << i << "]: " << l_shipdate[i] << "\n";
                all_valid = false;
            }
            if (l_quantity[i] <= 0) {
                std::cerr << "WARNING: invalid quantity at lineitem[" << i << "]: " << l_quantity[i] << "\n";
                all_valid = false;
            }
        }
        if (all_valid) std::cout << "✓ lineitem date/decimal checks passed\n";
    }

    // === ORDERS ===
    std::cout << "  Parsing orders.tbl...\n";
    std::vector<OrdersRow> orders_rows;
    {
        std::ifstream f(input_dir + "/orders.tbl");
        std::string line;
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            std::vector<std::string> fields;
            size_t start = 0;
            while (start < line.length()) {
                size_t end = line.find('|', start);
                if (end == std::string::npos) end = line.length();
                fields.push_back(line.substr(start, end - start));
                start = end + 1;
            }
            if (fields.size() < 9) continue;

            OrdersRow row;
            row.o_orderkey = std::stoi(fields[0]);
            row.o_custkey = std::stoi(fields[1]);
            row.o_orderstatus = fields[2][0];
            row.o_totalprice = parse_decimal(fields[3], 100);
            row.o_orderdate = parse_date(fields[4]);
            row.o_orderpriority = std::stoi(fields[5].substr(0, 1));
            row.o_clerk = std::stoi(fields[6].substr(6, 2));
            row.o_shippriority = std::stoi(fields[7]);
            row.o_comment = fields[8];

            orders_rows.push_back(row);
        }
    }

    std::cout << "    Loaded " << orders_rows.size() << " rows\n";

    DictionaryEncoder enc_orderstatus, enc_orderpriority, enc_clerk;

    std::vector<int32_t> o_orderkey, o_custkey;
    std::vector<int32_t> o_orderstatus_codes;
    std::vector<int64_t> o_totalprice;
    std::vector<int32_t> o_orderdate;
    std::vector<int32_t> o_orderpriority_codes, o_clerk_codes;
    std::vector<int32_t> o_shippriority;
    std::vector<std::string> o_comment;

    // Sort by orderkey (PK lookup)
    std::sort(orders_rows.begin(), orders_rows.end(),
        [](const OrdersRow& a, const OrdersRow& b) {
            return a.o_orderkey < b.o_orderkey;
        });

    for (const auto& row : orders_rows) {
        o_orderkey.push_back(row.o_orderkey);
        o_custkey.push_back(row.o_custkey);
        o_orderstatus_codes.push_back(enc_orderstatus.encode(std::string(1, row.o_orderstatus)));
        o_totalprice.push_back(row.o_totalprice);
        o_orderdate.push_back(row.o_orderdate);
        o_orderpriority_codes.push_back(enc_orderpriority.encode(std::to_string(row.o_orderpriority)));
        o_clerk_codes.push_back(enc_clerk.encode(std::to_string(row.o_clerk)));
        o_shippriority.push_back(row.o_shippriority);
        o_comment.push_back(row.o_comment);
    }

    fs::create_directories(output_dir + "/orders");

    std::cout << "  Writing orders columns...\n";
    write_column(output_dir + "/orders/o_orderkey.bin", o_orderkey);
    write_column(output_dir + "/orders/o_custkey.bin", o_custkey);
    write_column(output_dir + "/orders/o_orderstatus.bin", o_orderstatus_codes);
    enc_orderstatus.write_dictionary(output_dir + "/orders", "o_orderstatus");
    write_column(output_dir + "/orders/o_totalprice.bin", o_totalprice);
    write_column(output_dir + "/orders/o_orderdate.bin", o_orderdate);
    write_column(output_dir + "/orders/o_orderpriority.bin", o_orderpriority_codes);
    enc_orderpriority.write_dictionary(output_dir + "/orders", "o_orderpriority");
    write_column(output_dir + "/orders/o_clerk.bin", o_clerk_codes);
    enc_clerk.write_dictionary(output_dir + "/orders", "o_clerk");
    write_column(output_dir + "/orders/o_shippriority.bin", o_shippriority);
    write_column_strings(output_dir + "/orders/o_comment.bin", o_comment);

    // === CUSTOMER ===
    std::cout << "  Parsing customer.tbl...\n";
    std::vector<CustomerRow> customer_rows;
    {
        std::ifstream f(input_dir + "/customer.tbl");
        std::string line;
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            std::vector<std::string> fields;
            size_t start = 0;
            while (start < line.length()) {
                size_t end = line.find('|', start);
                if (end == std::string::npos) end = line.length();
                fields.push_back(line.substr(start, end - start));
                start = end + 1;
            }
            if (fields.size() < 8) continue;

            CustomerRow row;
            row.c_custkey = std::stoi(fields[0]);
            row.c_name = fields[1];
            row.c_address = fields[2];
            row.c_nationkey = std::stoi(fields[3]);
            row.c_phone = fields[4];
            row.c_acctbal = parse_decimal(fields[5], 100);
            row.c_mktsegment = fields[6];
            row.c_comment = fields[7];

            customer_rows.push_back(row);
        }
    }

    std::cout << "    Loaded " << customer_rows.size() << " rows\n";

    DictionaryEncoder enc_mktsegment;

    std::vector<int32_t> c_custkey;
    std::vector<std::string> c_name, c_address, c_phone, c_comment;
    std::vector<int32_t> c_nationkey;
    std::vector<int64_t> c_acctbal;
    std::vector<int32_t> c_mktsegment_codes;

    // Sort by custkey
    std::sort(customer_rows.begin(), customer_rows.end(),
        [](const CustomerRow& a, const CustomerRow& b) {
            return a.c_custkey < b.c_custkey;
        });

    for (const auto& row : customer_rows) {
        c_custkey.push_back(row.c_custkey);
        c_name.push_back(row.c_name);
        c_address.push_back(row.c_address);
        c_nationkey.push_back(row.c_nationkey);
        c_phone.push_back(row.c_phone);
        c_acctbal.push_back(row.c_acctbal);
        c_mktsegment_codes.push_back(enc_mktsegment.encode(row.c_mktsegment));
        c_comment.push_back(row.c_comment);
    }

    fs::create_directories(output_dir + "/customer");

    std::cout << "  Writing customer columns...\n";
    write_column(output_dir + "/customer/c_custkey.bin", c_custkey);
    write_column_strings(output_dir + "/customer/c_name.bin", c_name);
    write_column_strings(output_dir + "/customer/c_address.bin", c_address);
    write_column(output_dir + "/customer/c_nationkey.bin", c_nationkey);
    write_column_strings(output_dir + "/customer/c_phone.bin", c_phone);
    write_column(output_dir + "/customer/c_acctbal.bin", c_acctbal);
    write_column(output_dir + "/customer/c_mktsegment.bin", c_mktsegment_codes);
    enc_mktsegment.write_dictionary(output_dir + "/customer", "c_mktsegment");
    write_column_strings(output_dir + "/customer/c_comment.bin", c_comment);

    // === NATION (small table, skip sorting) ===
    std::cout << "  Parsing nation.tbl...\n";
    {
        std::vector<int32_t> n_nationkey;
        std::vector<std::string> n_name, n_comment;
        std::vector<int32_t> n_regionkey;

        std::ifstream f(input_dir + "/nation.tbl");
        std::string line;
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            std::vector<std::string> fields;
            size_t start = 0;
            while (start < line.length()) {
                size_t end = line.find('|', start);
                if (end == std::string::npos) end = line.length();
                fields.push_back(line.substr(start, end - start));
                start = end + 1;
            }
            if (fields.size() < 4) continue;

            n_nationkey.push_back(std::stoi(fields[0]));
            n_name.push_back(fields[1]);
            n_regionkey.push_back(std::stoi(fields[2]));
            n_comment.push_back(fields[3]);
        }

        fs::create_directories(output_dir + "/nation");
        write_column(output_dir + "/nation/n_nationkey.bin", n_nationkey);
        write_column_strings(output_dir + "/nation/n_name.bin", n_name);
        write_column(output_dir + "/nation/n_regionkey.bin", n_regionkey);
        write_column_strings(output_dir + "/nation/n_comment.bin", n_comment);
        std::cout << "    Loaded " << n_nationkey.size() << " rows\n";
    }

    // === REGION (small table) ===
    std::cout << "  Parsing region.tbl...\n";
    {
        std::vector<int32_t> r_regionkey;
        std::vector<std::string> r_name, r_comment;

        std::ifstream f(input_dir + "/region.tbl");
        std::string line;
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            std::vector<std::string> fields;
            size_t start = 0;
            while (start < line.length()) {
                size_t end = line.find('|', start);
                if (end == std::string::npos) end = line.length();
                fields.push_back(line.substr(start, end - start));
                start = end + 1;
            }
            if (fields.size() < 3) continue;

            r_regionkey.push_back(std::stoi(fields[0]));
            r_name.push_back(fields[1]);
            r_comment.push_back(fields[2]);
        }

        fs::create_directories(output_dir + "/region");
        write_column(output_dir + "/region/r_regionkey.bin", r_regionkey);
        write_column_strings(output_dir + "/region/r_name.bin", r_name);
        write_column_strings(output_dir + "/region/r_comment.bin", r_comment);
        std::cout << "    Loaded " << r_regionkey.size() << " rows\n";
    }

    // === SUPPLIER ===
    std::cout << "  Parsing supplier.tbl...\n";
    {
        std::vector<int32_t> s_suppkey;
        std::vector<std::string> s_name, s_address, s_phone, s_comment;
        std::vector<int32_t> s_nationkey;
        std::vector<int64_t> s_acctbal;

        std::ifstream f(input_dir + "/supplier.tbl");
        std::string line;
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            std::vector<std::string> fields;
            size_t start = 0;
            while (start < line.length()) {
                size_t end = line.find('|', start);
                if (end == std::string::npos) end = line.length();
                fields.push_back(line.substr(start, end - start));
                start = end + 1;
            }
            if (fields.size() < 7) continue;

            s_suppkey.push_back(std::stoi(fields[0]));
            s_name.push_back(fields[1]);
            s_address.push_back(fields[2]);
            s_nationkey.push_back(std::stoi(fields[3]));
            s_phone.push_back(fields[4]);
            s_acctbal.push_back(parse_decimal(fields[5], 100));
            s_comment.push_back(fields[6]);
        }

        fs::create_directories(output_dir + "/supplier");
        write_column(output_dir + "/supplier/s_suppkey.bin", s_suppkey);
        write_column_strings(output_dir + "/supplier/s_name.bin", s_name);
        write_column_strings(output_dir + "/supplier/s_address.bin", s_address);
        write_column(output_dir + "/supplier/s_nationkey.bin", s_nationkey);
        write_column_strings(output_dir + "/supplier/s_phone.bin", s_phone);
        write_column(output_dir + "/supplier/s_acctbal.bin", s_acctbal);
        write_column_strings(output_dir + "/supplier/s_comment.bin", s_comment);
        std::cout << "    Loaded " << s_suppkey.size() << " rows\n";
    }

    // === PART ===
    std::cout << "  Parsing part.tbl...\n";
    {
        std::vector<int32_t> p_partkey;
        std::vector<std::string> p_name, p_type, p_comment;
        std::vector<int32_t> p_mfgr, p_brand, p_container;
        std::vector<int32_t> p_size;
        std::vector<int64_t> p_retailprice;

        DictionaryEncoder enc_mfgr, enc_brand, enc_container;

        std::ifstream f(input_dir + "/part.tbl");
        std::string line;
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            std::vector<std::string> fields;
            size_t start = 0;
            while (start < line.length()) {
                size_t end = line.find('|', start);
                if (end == std::string::npos) end = line.length();
                fields.push_back(line.substr(start, end - start));
                start = end + 1;
            }
            if (fields.size() < 9) continue;

            p_partkey.push_back(std::stoi(fields[0]));
            p_name.push_back(fields[1]);
            p_mfgr.push_back(enc_mfgr.encode(fields[2]));
            p_brand.push_back(enc_brand.encode(fields[3]));
            p_type.push_back(fields[4]);
            p_size.push_back(std::stoi(fields[5]));
            p_container.push_back(enc_container.encode(fields[6]));
            p_retailprice.push_back(parse_decimal(fields[7], 100));
            p_comment.push_back(fields[8]);
        }

        fs::create_directories(output_dir + "/part");
        write_column(output_dir + "/part/p_partkey.bin", p_partkey);
        write_column_strings(output_dir + "/part/p_name.bin", p_name);
        write_column(output_dir + "/part/p_mfgr.bin", p_mfgr);
        enc_mfgr.write_dictionary(output_dir + "/part", "p_mfgr");
        write_column(output_dir + "/part/p_brand.bin", p_brand);
        enc_brand.write_dictionary(output_dir + "/part", "p_brand");
        write_column_strings(output_dir + "/part/p_type.bin", p_type);
        write_column(output_dir + "/part/p_size.bin", p_size);
        write_column(output_dir + "/part/p_container.bin", p_container);
        enc_container.write_dictionary(output_dir + "/part", "p_container");
        write_column(output_dir + "/part/p_retailprice.bin", p_retailprice);
        write_column_strings(output_dir + "/part/p_comment.bin", p_comment);
        std::cout << "    Loaded " << p_partkey.size() << " rows\n";
    }

    // === PARTSUPP ===
    std::cout << "  Parsing partsupp.tbl...\n";
    {
        std::vector<int32_t> ps_partkey, ps_suppkey;
        std::vector<int32_t> ps_availqty;
        std::vector<int64_t> ps_supplycost;
        std::vector<std::string> ps_comment;

        std::ifstream f(input_dir + "/partsupp.tbl");
        std::string line;
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            std::vector<std::string> fields;
            size_t start = 0;
            while (start < line.length()) {
                size_t end = line.find('|', start);
                if (end == std::string::npos) end = line.length();
                fields.push_back(line.substr(start, end - start));
                start = end + 1;
            }
            if (fields.size() < 5) continue;

            ps_partkey.push_back(std::stoi(fields[0]));
            ps_suppkey.push_back(std::stoi(fields[1]));
            ps_availqty.push_back(std::stoi(fields[2]));
            ps_supplycost.push_back(parse_decimal(fields[3], 100));
            ps_comment.push_back(fields[4]);
        }

        fs::create_directories(output_dir + "/partsupp");
        write_column(output_dir + "/partsupp/ps_partkey.bin", ps_partkey);
        write_column(output_dir + "/partsupp/ps_suppkey.bin", ps_suppkey);
        write_column(output_dir + "/partsupp/ps_availqty.bin", ps_availqty);
        write_column(output_dir + "/partsupp/ps_supplycost.bin", ps_supplycost);
        write_column_strings(output_dir + "/partsupp/ps_comment.bin", ps_comment);
        std::cout << "    Loaded " << ps_partkey.size() << " rows\n";
    }

    std::cout << "✓ Ingestion complete\n";
    return 0;
}
