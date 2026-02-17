#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <sys/stat.h>
#include <cmath>
#include <charconv>
#include <omp.h>

// Compile with: g++ -O2 -std=c++17 -Wall -lpthread -fopenmp -o ingest ingest.cpp

// Date parsing: days since epoch (1970-01-01)
int32_t parse_date(const std::string& date_str) {
    if (date_str.length() != 10) return 0;
    // Format: YYYY-MM-DD
    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Days in each month (non-leap year)
    static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    // Count days from complete years (1970..year-1)
    int32_t epoch_day = 0;
    for (int y = 1970; y < year; y++) {
        epoch_day += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Add days from complete months in this year
    for (int m = 1; m < month; m++) {
        epoch_day += days_in_month[m];
        if (m == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
            epoch_day++; // Leap year February
        }
    }

    // Add remaining days (subtract 1 because days are 1-indexed but epoch day 0 = Jan 1)
    epoch_day += (day - 1);

    return epoch_day;
}

// Self-test: parse_date("1970-01-01") must return 0
void test_date_parsing() {
    int32_t test = parse_date("1970-01-01");
    if (test != 0) {
        std::cerr << "ERROR: parse_date('1970-01-01') = " << test << ", expected 0. Aborting.\n";
        exit(1);
    }
    std::cout << "Date parsing self-test passed: 1970-01-01 = 0\n";
}

// Decimal parsing: parse as double, multiply by scale_factor, round to int64_t
int64_t parse_decimal(const std::string& str, int scale_factor) {
    double val = 0.0;
    auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.length(), val);
    if (ec != std::errc()) {
        std::cerr << "Failed to parse decimal: " << str << "\n";
        return 0;
    }
    int64_t scale = 1;
    for (int i = 0; i < scale_factor; i++) scale *= 10;
    return (int64_t)std::round(val * scale);
}

// Dictionary encoder for strings
class DictionaryEncoder {
private:
    std::unordered_map<std::string, int32_t> dict;
    std::vector<std::string> reverse_dict;

public:
    int32_t encode(const std::string& str) {
        if (dict.find(str) == dict.end()) {
            int32_t code = dict.size();
            dict[str] = code;
            reverse_dict.push_back(str);
        }
        return dict[str];
    }

    void save(const std::string& filename) {
        std::ofstream f(filename, std::ios::binary);
        for (const auto& str : reverse_dict) {
            f << str << "\n";
        }
        f.close();
    }
};

struct CSVParser {
    std::string line;
    std::vector<std::string> fields;

    bool parse_line(std::ifstream& file) {
        if (!std::getline(file, line)) return false;
        fields.clear();

        size_t start = 0;
        while (start < line.length()) {
            size_t end = line.find('|', start);
            if (end == std::string::npos) end = line.length();
            fields.push_back(line.substr(start, end - start));
            start = end + 1;
        }
        return true;
    }

    std::string get(size_t idx) {
        if (idx < fields.size()) return fields[idx];
        return "";
    }
};

void mkdir_p(const std::string& path) {
    mkdir(path.c_str(), 0755);
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <input_dir> <output_dir>\n";
        return 1;
    }

    test_date_parsing();

    std::string input_dir = argv[1];
    std::string output_dir = argv[2];
    mkdir_p(output_dir);

    // Create subdirectories for each table
    std::vector<std::string> tables = {
        "lineitem", "orders", "customer", "part", "supplier", "partsupp", "nation", "region"
    };

    for (const auto& tbl : tables) {
        mkdir_p(output_dir + "/" + tbl);
    }
    mkdir_p(output_dir + "/indexes");

    // Process lineitem (60M rows, largest)
    {
        std::cout << "Ingesting lineitem...\n";
        std::string input_file = input_dir + "/lineitem.tbl";
        std::ifstream file(input_file);

        std::vector<int32_t> l_orderkey, l_partkey, l_suppkey, l_linenumber, l_shipdate, l_commitdate, l_receiptdate;
        std::vector<int32_t> l_returnflag, l_linestatus, l_shipinstruct, l_shipmode, l_comment;
        std::vector<int64_t> l_quantity, l_extendedprice, l_discount, l_tax;

        DictionaryEncoder enc_returnflag, enc_linestatus, enc_shipinstruct, enc_shipmode, enc_comment;

        CSVParser parser;
        size_t row_count = 0;

        while (parser.parse_line(file)) {
            l_orderkey.push_back(std::stoi(parser.get(0)));
            l_partkey.push_back(std::stoi(parser.get(1)));
            l_suppkey.push_back(std::stoi(parser.get(2)));
            l_linenumber.push_back(std::stoi(parser.get(3)));
            l_quantity.push_back(parse_decimal(parser.get(4), 2));
            l_extendedprice.push_back(parse_decimal(parser.get(5), 2));
            l_discount.push_back(parse_decimal(parser.get(6), 2));
            l_tax.push_back(parse_decimal(parser.get(7), 2));
            l_returnflag.push_back(enc_returnflag.encode(parser.get(8)));
            l_linestatus.push_back(enc_linestatus.encode(parser.get(9)));
            l_shipdate.push_back(parse_date(parser.get(10)));
            l_commitdate.push_back(parse_date(parser.get(11)));
            l_receiptdate.push_back(parse_date(parser.get(12)));
            l_shipinstruct.push_back(enc_shipinstruct.encode(parser.get(13)));
            l_shipmode.push_back(enc_shipmode.encode(parser.get(14)));
            l_comment.push_back(enc_comment.encode(parser.get(15)));

            row_count++;
            if (row_count % 1000000 == 0) {
                std::cout << "  " << row_count << " rows ingested\n";
            }
        }
        file.close();

        std::cout << "Lineitem: " << row_count << " rows total. Writing binary columns...\n";

        // Write binary columns
        auto write_binary = [&](const std::string& name, const auto& data) {
            std::ofstream f(output_dir + "/lineitem/" + name + ".bin", std::ios::binary);
            f.write((const char*)data.data(), data.size() * sizeof(data[0]));
            f.close();
        };

        write_binary("l_orderkey", l_orderkey);
        write_binary("l_partkey", l_partkey);
        write_binary("l_suppkey", l_suppkey);
        write_binary("l_linenumber", l_linenumber);
        write_binary("l_quantity", l_quantity);
        write_binary("l_extendedprice", l_extendedprice);
        write_binary("l_discount", l_discount);
        write_binary("l_tax", l_tax);
        write_binary("l_returnflag", l_returnflag);
        write_binary("l_linestatus", l_linestatus);
        write_binary("l_shipdate", l_shipdate);
        write_binary("l_commitdate", l_commitdate);
        write_binary("l_receiptdate", l_receiptdate);
        write_binary("l_shipinstruct", l_shipinstruct);
        write_binary("l_shipmode", l_shipmode);
        write_binary("l_comment", l_comment);

        // Write dictionaries
        enc_returnflag.save(output_dir + "/lineitem/l_returnflag_dict.txt");
        enc_linestatus.save(output_dir + "/lineitem/l_linestatus_dict.txt");
        enc_shipinstruct.save(output_dir + "/lineitem/l_shipinstruct_dict.txt");
        enc_shipmode.save(output_dir + "/lineitem/l_shipmode_dict.txt");
        enc_comment.save(output_dir + "/lineitem/l_comment_dict.txt");
    }

    // Process orders (15M rows)
    {
        std::cout << "Ingesting orders...\n";
        std::string input_file = input_dir + "/orders.tbl";
        std::ifstream file(input_file);

        std::vector<int32_t> o_orderkey, o_custkey, o_orderdate, o_shippriority;
        std::vector<int32_t> o_orderstatus, o_orderpriority, o_clerk, o_comment;
        std::vector<int64_t> o_totalprice;

        DictionaryEncoder enc_orderstatus, enc_orderpriority, enc_clerk, enc_comment;

        CSVParser parser;
        size_t row_count = 0;

        while (parser.parse_line(file)) {
            o_orderkey.push_back(std::stoi(parser.get(0)));
            o_custkey.push_back(std::stoi(parser.get(1)));
            o_orderstatus.push_back(enc_orderstatus.encode(parser.get(2)));
            o_totalprice.push_back(parse_decimal(parser.get(3), 2));
            o_orderdate.push_back(parse_date(parser.get(4)));
            o_orderpriority.push_back(enc_orderpriority.encode(parser.get(5)));
            o_clerk.push_back(enc_clerk.encode(parser.get(6)));
            o_shippriority.push_back(std::stoi(parser.get(7)));
            o_comment.push_back(enc_comment.encode(parser.get(8)));

            row_count++;
            if (row_count % 1000000 == 0) {
                std::cout << "  " << row_count << " rows ingested\n";
            }
        }
        file.close();

        std::cout << "Orders: " << row_count << " rows total. Writing binary columns...\n";

        auto write_binary = [&](const std::string& name, const auto& data) {
            std::ofstream f(output_dir + "/orders/" + name + ".bin", std::ios::binary);
            f.write((const char*)data.data(), data.size() * sizeof(data[0]));
            f.close();
        };

        write_binary("o_orderkey", o_orderkey);
        write_binary("o_custkey", o_custkey);
        write_binary("o_orderstatus", o_orderstatus);
        write_binary("o_totalprice", o_totalprice);
        write_binary("o_orderdate", o_orderdate);
        write_binary("o_orderpriority", o_orderpriority);
        write_binary("o_clerk", o_clerk);
        write_binary("o_shippriority", o_shippriority);
        write_binary("o_comment", o_comment);

        enc_orderstatus.save(output_dir + "/orders/o_orderstatus_dict.txt");
        enc_orderpriority.save(output_dir + "/orders/o_orderpriority_dict.txt");
        enc_clerk.save(output_dir + "/orders/o_clerk_dict.txt");
        enc_comment.save(output_dir + "/orders/o_comment_dict.txt");
    }

    // Process customer (1.5M rows)
    {
        std::cout << "Ingesting customer...\n";
        std::string input_file = input_dir + "/customer.tbl";
        std::ifstream file(input_file);

        std::vector<int32_t> c_custkey, c_nationkey;
        std::vector<int32_t> c_name, c_address, c_phone, c_mktsegment, c_comment;
        std::vector<int64_t> c_acctbal;

        DictionaryEncoder enc_name, enc_address, enc_phone, enc_mktsegment, enc_comment;

        CSVParser parser;
        size_t row_count = 0;

        while (parser.parse_line(file)) {
            c_custkey.push_back(std::stoi(parser.get(0)));
            c_name.push_back(enc_name.encode(parser.get(1)));
            c_address.push_back(enc_address.encode(parser.get(2)));
            c_nationkey.push_back(std::stoi(parser.get(3)));
            c_phone.push_back(enc_phone.encode(parser.get(4)));
            c_acctbal.push_back(parse_decimal(parser.get(5), 2));
            c_mktsegment.push_back(enc_mktsegment.encode(parser.get(6)));
            c_comment.push_back(enc_comment.encode(parser.get(7)));

            row_count++;
            if (row_count % 500000 == 0) {
                std::cout << "  " << row_count << " rows ingested\n";
            }
        }
        file.close();

        std::cout << "Customer: " << row_count << " rows total. Writing binary columns...\n";

        auto write_binary = [&](const std::string& name, const auto& data) {
            std::ofstream f(output_dir + "/customer/" + name + ".bin", std::ios::binary);
            f.write((const char*)data.data(), data.size() * sizeof(data[0]));
            f.close();
        };

        write_binary("c_custkey", c_custkey);
        write_binary("c_name", c_name);
        write_binary("c_address", c_address);
        write_binary("c_nationkey", c_nationkey);
        write_binary("c_phone", c_phone);
        write_binary("c_acctbal", c_acctbal);
        write_binary("c_mktsegment", c_mktsegment);
        write_binary("c_comment", c_comment);

        enc_name.save(output_dir + "/customer/c_name_dict.txt");
        enc_address.save(output_dir + "/customer/c_address_dict.txt");
        enc_phone.save(output_dir + "/customer/c_phone_dict.txt");
        enc_mktsegment.save(output_dir + "/customer/c_mktsegment_dict.txt");
        enc_comment.save(output_dir + "/customer/c_comment_dict.txt");
    }

    // Process part (2M rows)
    {
        std::cout << "Ingesting part...\n";
        std::string input_file = input_dir + "/part.tbl";
        std::ifstream file(input_file);

        std::vector<int32_t> p_partkey, p_size;
        std::vector<int32_t> p_name, p_mfgr, p_brand, p_type, p_container, p_comment;
        std::vector<int64_t> p_retailprice;

        DictionaryEncoder enc_name, enc_mfgr, enc_brand, enc_type, enc_container, enc_comment;

        CSVParser parser;
        size_t row_count = 0;

        while (parser.parse_line(file)) {
            p_partkey.push_back(std::stoi(parser.get(0)));
            p_name.push_back(enc_name.encode(parser.get(1)));
            p_mfgr.push_back(enc_mfgr.encode(parser.get(2)));
            p_brand.push_back(enc_brand.encode(parser.get(3)));
            p_type.push_back(enc_type.encode(parser.get(4)));
            p_size.push_back(std::stoi(parser.get(5)));
            p_container.push_back(enc_container.encode(parser.get(6)));
            p_retailprice.push_back(parse_decimal(parser.get(7), 2));
            p_comment.push_back(enc_comment.encode(parser.get(8)));

            row_count++;
            if (row_count % 500000 == 0) {
                std::cout << "  " << row_count << " rows ingested\n";
            }
        }
        file.close();

        std::cout << "Part: " << row_count << " rows total. Writing binary columns...\n";

        auto write_binary = [&](const std::string& name, const auto& data) {
            std::ofstream f(output_dir + "/part/" + name + ".bin", std::ios::binary);
            f.write((const char*)data.data(), data.size() * sizeof(data[0]));
            f.close();
        };

        write_binary("p_partkey", p_partkey);
        write_binary("p_name", p_name);
        write_binary("p_mfgr", p_mfgr);
        write_binary("p_brand", p_brand);
        write_binary("p_type", p_type);
        write_binary("p_size", p_size);
        write_binary("p_container", p_container);
        write_binary("p_retailprice", p_retailprice);
        write_binary("p_comment", p_comment);

        enc_name.save(output_dir + "/part/p_name_dict.txt");
        enc_mfgr.save(output_dir + "/part/p_mfgr_dict.txt");
        enc_brand.save(output_dir + "/part/p_brand_dict.txt");
        enc_type.save(output_dir + "/part/p_type_dict.txt");
        enc_container.save(output_dir + "/part/p_container_dict.txt");
        enc_comment.save(output_dir + "/part/p_comment_dict.txt");
    }

    // Process supplier (100K rows)
    {
        std::cout << "Ingesting supplier...\n";
        std::string input_file = input_dir + "/supplier.tbl";
        std::ifstream file(input_file);

        std::vector<int32_t> s_suppkey, s_nationkey;
        std::vector<int32_t> s_name, s_address, s_phone, s_comment;
        std::vector<int64_t> s_acctbal;

        DictionaryEncoder enc_name, enc_address, enc_phone, enc_comment;

        CSVParser parser;
        size_t row_count = 0;

        while (parser.parse_line(file)) {
            s_suppkey.push_back(std::stoi(parser.get(0)));
            s_name.push_back(enc_name.encode(parser.get(1)));
            s_address.push_back(enc_address.encode(parser.get(2)));
            s_nationkey.push_back(std::stoi(parser.get(3)));
            s_phone.push_back(enc_phone.encode(parser.get(4)));
            s_acctbal.push_back(parse_decimal(parser.get(5), 2));
            s_comment.push_back(enc_comment.encode(parser.get(6)));

            row_count++;
        }
        file.close();

        std::cout << "Supplier: " << row_count << " rows total. Writing binary columns...\n";

        auto write_binary = [&](const std::string& name, const auto& data) {
            std::ofstream f(output_dir + "/supplier/" + name + ".bin", std::ios::binary);
            f.write((const char*)data.data(), data.size() * sizeof(data[0]));
            f.close();
        };

        write_binary("s_suppkey", s_suppkey);
        write_binary("s_name", s_name);
        write_binary("s_address", s_address);
        write_binary("s_nationkey", s_nationkey);
        write_binary("s_phone", s_phone);
        write_binary("s_acctbal", s_acctbal);
        write_binary("s_comment", s_comment);

        enc_name.save(output_dir + "/supplier/s_name_dict.txt");
        enc_address.save(output_dir + "/supplier/s_address_dict.txt");
        enc_phone.save(output_dir + "/supplier/s_phone_dict.txt");
        enc_comment.save(output_dir + "/supplier/s_comment_dict.txt");
    }

    // Process partsupp (8M rows)
    {
        std::cout << "Ingesting partsupp...\n";
        std::string input_file = input_dir + "/partsupp.tbl";
        std::ifstream file(input_file);

        std::vector<int32_t> ps_partkey, ps_suppkey, ps_availqty;
        std::vector<int32_t> ps_comment;
        std::vector<int64_t> ps_supplycost;

        DictionaryEncoder enc_comment;

        CSVParser parser;
        size_t row_count = 0;

        while (parser.parse_line(file)) {
            ps_partkey.push_back(std::stoi(parser.get(0)));
            ps_suppkey.push_back(std::stoi(parser.get(1)));
            ps_availqty.push_back(std::stoi(parser.get(2)));
            ps_supplycost.push_back(parse_decimal(parser.get(3), 2));
            ps_comment.push_back(enc_comment.encode(parser.get(4)));

            row_count++;
            if (row_count % 1000000 == 0) {
                std::cout << "  " << row_count << " rows ingested\n";
            }
        }
        file.close();

        std::cout << "Partsupp: " << row_count << " rows total. Writing binary columns...\n";

        auto write_binary = [&](const std::string& name, const auto& data) {
            std::ofstream f(output_dir + "/partsupp/" + name + ".bin", std::ios::binary);
            f.write((const char*)data.data(), data.size() * sizeof(data[0]));
            f.close();
        };

        write_binary("ps_partkey", ps_partkey);
        write_binary("ps_suppkey", ps_suppkey);
        write_binary("ps_availqty", ps_availqty);
        write_binary("ps_supplycost", ps_supplycost);
        write_binary("ps_comment", ps_comment);

        enc_comment.save(output_dir + "/partsupp/ps_comment_dict.txt");
    }

    // Process nation (25 rows)
    {
        std::cout << "Ingesting nation...\n";
        std::string input_file = input_dir + "/nation.tbl";
        std::ifstream file(input_file);

        std::vector<int32_t> n_nationkey, n_regionkey;
        std::vector<int32_t> n_name, n_comment;

        DictionaryEncoder enc_name, enc_comment;

        CSVParser parser;
        size_t row_count = 0;

        while (parser.parse_line(file)) {
            n_nationkey.push_back(std::stoi(parser.get(0)));
            n_name.push_back(enc_name.encode(parser.get(1)));
            n_regionkey.push_back(std::stoi(parser.get(2)));
            n_comment.push_back(enc_comment.encode(parser.get(3)));

            row_count++;
        }
        file.close();

        std::cout << "Nation: " << row_count << " rows total. Writing binary columns...\n";

        auto write_binary = [&](const std::string& name, const auto& data) {
            std::ofstream f(output_dir + "/nation/" + name + ".bin", std::ios::binary);
            f.write((const char*)data.data(), data.size() * sizeof(data[0]));
            f.close();
        };

        write_binary("n_nationkey", n_nationkey);
        write_binary("n_name", n_name);
        write_binary("n_regionkey", n_regionkey);
        write_binary("n_comment", n_comment);

        enc_name.save(output_dir + "/nation/n_name_dict.txt");
        enc_comment.save(output_dir + "/nation/n_comment_dict.txt");
    }

    // Process region (5 rows)
    {
        std::cout << "Ingesting region...\n";
        std::string input_file = input_dir + "/region.tbl";
        std::ifstream file(input_file);

        std::vector<int32_t> r_regionkey;
        std::vector<int32_t> r_name, r_comment;

        DictionaryEncoder enc_name, enc_comment;

        CSVParser parser;
        size_t row_count = 0;

        while (parser.parse_line(file)) {
            r_regionkey.push_back(std::stoi(parser.get(0)));
            r_name.push_back(enc_name.encode(parser.get(1)));
            r_comment.push_back(enc_comment.encode(parser.get(2)));

            row_count++;
        }
        file.close();

        std::cout << "Region: " << row_count << " rows total. Writing binary columns...\n";

        auto write_binary = [&](const std::string& name, const auto& data) {
            std::ofstream f(output_dir + "/region/" + name + ".bin", std::ios::binary);
            f.write((const char*)data.data(), data.size() * sizeof(data[0]));
            f.close();
        };

        write_binary("r_regionkey", r_regionkey);
        write_binary("r_name", r_name);
        write_binary("r_comment", r_comment);

        enc_name.save(output_dir + "/region/r_name_dict.txt");
        enc_comment.save(output_dir + "/region/r_comment_dict.txt");
    }

    std::cout << "Ingestion complete!\n";
    return 0;
}
