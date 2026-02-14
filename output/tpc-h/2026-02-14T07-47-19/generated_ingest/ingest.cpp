#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <thread>
#include <mutex>
#include <memory>
#include <cstring>
#include <charconv>
#include <filesystem>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cmath>
#include <cassert>

namespace fs = std::filesystem;

// ============================================================================
// Date Parsing: YYYY-MM-DD to days since 1970-01-01
// ============================================================================
int32_t parse_date(const std::string& s) {
    if (s.empty() || s.length() < 10) return 0;
    int year = std::stoi(s.substr(0, 4));
    int month = std::stoi(s.substr(5, 2));
    int day = std::stoi(s.substr(8, 2));

    // Days since epoch (1970-01-01)
    int days = 0;
    // Add days for complete years
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    // Add days for complete months in current year
    int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    for (int m = 1; m < month; ++m) {
        days += month_days[m];
        if (m == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
            days++; // leap year February
        }
    }
    // Add remaining days
    days += day;
    return days;
}

// ============================================================================
// Trim whitespace
// ============================================================================
std::string trim(const std::string& s) {
    auto start = s.begin();
    while (start != s.end() && std::isspace(*start)) start++;
    auto end = s.end();
    do { end--; } while (std::distance(start, end) > 0 && std::isspace(*end));
    return std::string(start, end + 1);
}

// ============================================================================
// Parse field from pipe-delimited line
// ============================================================================
std::string get_field(const std::string& line, size_t field_index) {
    size_t pos = 0;
    size_t count = 0;
    while (count < field_index && pos < line.length()) {
        pos = line.find('|', pos);
        if (pos == std::string::npos) return "";
        pos++;
        count++;
    }
    if (count != field_index || pos >= line.length()) return "";
    size_t end = line.find('|', pos);
    if (end == std::string::npos) end = line.length();
    return line.substr(pos, end - pos);
}

// ============================================================================
// Column storage structures
// ============================================================================
struct ColumnWriter {
    std::vector<std::string> strings;
    std::vector<int32_t> ints;
    std::vector<int64_t> ints64;
    std::vector<double> doubles;
    std::vector<uint8_t> uint8s;

    void reset() {
        strings.clear(); ints.clear(); ints64.clear(); doubles.clear(); uint8s.clear();
    }
};

// ============================================================================
// Global dictionaries for low-cardinality columns
// ============================================================================
struct DictionaryEncoder {
    std::unordered_map<std::string, uint8_t> encode_map;
    std::vector<std::string> decode_map;
    uint8_t next_code = 0;

    uint8_t encode(const std::string& value) {
        auto it = encode_map.find(value);
        if (it != encode_map.end()) return it->second;
        uint8_t code = next_code++;
        encode_map[value] = code;
        decode_map.push_back(value);
        return code;
    }
};

// ============================================================================
// Parse lineitem row
// ============================================================================
struct LineitemRow {
    int32_t l_orderkey, l_partkey, l_suppkey, l_linenumber;
    double l_quantity, l_extendedprice, l_discount, l_tax;
    uint8_t l_returnflag, l_linestatus;
    int32_t l_shipdate, l_commitdate, l_receiptdate;
    uint8_t l_shipinstruct, l_shipmode;
    std::string l_comment;

    // For sorting by l_shipdate
    bool operator<(const LineitemRow& other) const {
        return l_shipdate < other.l_shipdate;
    }
};

void parse_lineitem_row(const std::string& line, LineitemRow& row,
                        DictionaryEncoder& returnflag_dict,
                        DictionaryEncoder& linestatus_dict,
                        DictionaryEncoder& shipinstruct_dict,
                        DictionaryEncoder& shipmode_dict) {
    std::istringstream iss(line);
    std::string field;
    size_t col = 0;

    while (std::getline(iss, field, '|') && col < 16) {
        field = trim(field);
        switch (col) {
            case 0: row.l_orderkey = std::stoi(field); break;
            case 1: row.l_partkey = std::stoi(field); break;
            case 2: row.l_suppkey = std::stoi(field); break;
            case 3: row.l_linenumber = std::stoi(field); break;
            case 4: row.l_quantity = std::stod(field); break;
            case 5: row.l_extendedprice = std::stod(field); break;
            case 6: row.l_discount = std::stod(field); break;
            case 7: row.l_tax = std::stod(field); break;
            case 8: row.l_returnflag = returnflag_dict.encode(field); break;
            case 9: row.l_linestatus = linestatus_dict.encode(field); break;
            case 10: row.l_shipdate = parse_date(field); break;
            case 11: row.l_commitdate = parse_date(field); break;
            case 12: row.l_receiptdate = parse_date(field); break;
            case 13: row.l_shipinstruct = shipinstruct_dict.encode(field); break;
            case 14: row.l_shipmode = shipmode_dict.encode(field); break;
            case 15: row.l_comment = field; break;
        }
        col++;
    }
}

// ============================================================================
// Parse orders row
// ============================================================================
struct OrdersRow {
    int32_t o_orderkey, o_custkey;
    uint8_t o_orderstatus;
    double o_totalprice;
    int32_t o_orderdate;
    uint8_t o_orderpriority;
    uint8_t o_clerk;
    int32_t o_shippriority;
    std::string o_comment;
};

void parse_orders_row(const std::string& line, OrdersRow& row,
                      DictionaryEncoder& orderstatus_dict,
                      DictionaryEncoder& orderpriority_dict,
                      DictionaryEncoder& clerk_dict) {
    std::istringstream iss(line);
    std::string field;
    size_t col = 0;

    while (std::getline(iss, field, '|') && col < 9) {
        field = trim(field);
        switch (col) {
            case 0: row.o_orderkey = std::stoi(field); break;
            case 1: row.o_custkey = std::stoi(field); break;
            case 2: row.o_orderstatus = orderstatus_dict.encode(field); break;
            case 3: row.o_totalprice = std::stod(field); break;
            case 4: row.o_orderdate = parse_date(field); break;
            case 5: row.o_orderpriority = orderpriority_dict.encode(field); break;
            case 6: row.o_clerk = clerk_dict.encode(field); break;
            case 7: row.o_shippriority = std::stoi(field); break;
            case 8: row.o_comment = field; break;
        }
        col++;
    }
}

// ============================================================================
// Parse customer row
// ============================================================================
struct CustomerRow {
    int32_t c_custkey, c_nationkey;
    std::string c_name, c_address;
    std::string c_phone;
    double c_acctbal;
    uint8_t c_mktsegment;
    std::string c_comment;
};

void parse_customer_row(const std::string& line, CustomerRow& row,
                        DictionaryEncoder& mktsegment_dict) {
    std::istringstream iss(line);
    std::string field;
    size_t col = 0;

    while (std::getline(iss, field, '|') && col < 8) {
        field = trim(field);
        switch (col) {
            case 0: row.c_custkey = std::stoi(field); break;
            case 1: row.c_name = field; break;
            case 2: row.c_address = field; break;
            case 3: row.c_nationkey = std::stoi(field); break;
            case 4: row.c_phone = field; break;
            case 5: row.c_acctbal = std::stod(field); break;
            case 6: row.c_mktsegment = mktsegment_dict.encode(field); break;
            case 7: row.c_comment = field; break;
        }
        col++;
    }
}

// Similar structs for other tables...
struct PartRow {
    int32_t p_partkey, p_size;
    std::string p_name, p_mfgr, p_brand, p_type, p_container, p_comment;
    double p_retailprice;
};

void parse_part_row(const std::string& line, PartRow& row) {
    std::istringstream iss(line);
    std::string field;
    size_t col = 0;

    while (std::getline(iss, field, '|') && col < 9) {
        field = trim(field);
        switch (col) {
            case 0: row.p_partkey = std::stoi(field); break;
            case 1: row.p_name = field; break;
            case 2: row.p_mfgr = field; break;
            case 3: row.p_brand = field; break;
            case 4: row.p_type = field; break;
            case 5: row.p_size = std::stoi(field); break;
            case 6: row.p_container = field; break;
            case 7: row.p_retailprice = std::stod(field); break;
            case 8: row.p_comment = field; break;
        }
        col++;
    }
}

struct SupplierRow {
    int32_t s_suppkey, s_nationkey;
    std::string s_name, s_address, s_phone, s_comment;
    double s_acctbal;
};

void parse_supplier_row(const std::string& line, SupplierRow& row) {
    std::istringstream iss(line);
    std::string field;
    size_t col = 0;

    while (std::getline(iss, field, '|') && col < 7) {
        field = trim(field);
        switch (col) {
            case 0: row.s_suppkey = std::stoi(field); break;
            case 1: row.s_name = field; break;
            case 2: row.s_address = field; break;
            case 3: row.s_nationkey = std::stoi(field); break;
            case 4: row.s_phone = field; break;
            case 5: row.s_acctbal = std::stod(field); break;
            case 6: row.s_comment = field; break;
        }
        col++;
    }
}

struct PartSuppRow {
    int32_t ps_partkey, ps_suppkey, ps_availqty;
    double ps_supplycost;
    std::string ps_comment;
};

void parse_partsupp_row(const std::string& line, PartSuppRow& row) {
    std::istringstream iss(line);
    std::string field;
    size_t col = 0;

    while (std::getline(iss, field, '|') && col < 5) {
        field = trim(field);
        switch (col) {
            case 0: row.ps_partkey = std::stoi(field); break;
            case 1: row.ps_suppkey = std::stoi(field); break;
            case 2: row.ps_availqty = std::stoi(field); break;
            case 3: row.ps_supplycost = std::stod(field); break;
            case 4: row.ps_comment = field; break;
        }
        col++;
    }
}

struct NationRow {
    int32_t n_nationkey, n_regionkey;
    std::string n_name, n_comment;
};

void parse_nation_row(const std::string& line, NationRow& row) {
    std::istringstream iss(line);
    std::string field;
    size_t col = 0;

    while (std::getline(iss, field, '|') && col < 4) {
        field = trim(field);
        switch (col) {
            case 0: row.n_nationkey = std::stoi(field); break;
            case 1: row.n_name = field; break;
            case 2: row.n_regionkey = std::stoi(field); break;
            case 3: row.n_comment = field; break;
        }
        col++;
    }
}

struct RegionRow {
    int32_t r_regionkey;
    std::string r_name, r_comment;
};

void parse_region_row(const std::string& line, RegionRow& row) {
    std::istringstream iss(line);
    std::string field;
    size_t col = 0;

    while (std::getline(iss, field, '|') && col < 3) {
        field = trim(field);
        switch (col) {
            case 0: row.r_regionkey = std::stoi(field); break;
            case 1: row.r_name = field; break;
            case 2: row.r_comment = field; break;
        }
        col++;
    }
}

// ============================================================================
// Write binary column data and dictionary files
// ============================================================================
void write_binary_column(const std::string& base_path, const std::string& col_name,
                         const std::vector<int32_t>& data) {
    std::ofstream f(base_path + "/" + col_name + ".bin", std::ios::binary);
    f.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(int32_t));
}

void write_binary_column(const std::string& base_path, const std::string& col_name,
                         const std::vector<double>& data) {
    std::ofstream f(base_path + "/" + col_name + ".bin", std::ios::binary);
    f.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(double));
}

void write_binary_column(const std::string& base_path, const std::string& col_name,
                         const std::vector<uint8_t>& data) {
    std::ofstream f(base_path + "/" + col_name + ".bin", std::ios::binary);
    f.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(uint8_t));
}

void write_string_column(const std::string& base_path, const std::string& col_name,
                         const std::vector<std::string>& data) {
    std::ofstream f(base_path + "/" + col_name + ".bin", std::ios::binary);
    for (const auto& s : data) {
        uint32_t len = s.length();
        f.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
        f.write(s.data(), len);
    }
}

void write_dictionary(const std::string& base_path, const std::string& col_name,
                      const DictionaryEncoder& dict) {
    std::ofstream f(base_path + "/" + col_name + "_dict.txt");
    for (size_t i = 0; i < dict.decode_map.size(); ++i) {
        f << i << "=" << dict.decode_map[i] << "\n";
    }
}

// ============================================================================
// Ingest lineitem (sorted by l_shipdate)
// ============================================================================
void ingest_lineitem(const std::string& data_dir, const std::string& gendb_dir) {
    std::string lineitem_file = data_dir + "/lineitem.tbl";
    std::string table_dir = gendb_dir + "/lineitem";
    fs::create_directories(table_dir);

    std::cout << "Ingesting lineitem..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    // First pass: read all rows and build dictionaries
    std::vector<LineitemRow> rows;
    DictionaryEncoder returnflag_dict, linestatus_dict, shipinstruct_dict, shipmode_dict;

    std::ifstream f(lineitem_file);
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        LineitemRow row;
        parse_lineitem_row(line, row, returnflag_dict, linestatus_dict,
                          shipinstruct_dict, shipmode_dict);
        rows.push_back(row);
    }
    f.close();

    std::cout << "  Read " << rows.size() << " rows" << std::endl;

    // Sort by l_shipdate for zone map effectiveness
    std::sort(rows.begin(), rows.end());
    std::cout << "  Sorted by l_shipdate" << std::endl;

    // Write columns
    std::vector<int32_t> l_orderkey, l_partkey, l_suppkey, l_linenumber;
    std::vector<double> l_quantity, l_extendedprice, l_discount, l_tax;
    std::vector<uint8_t> l_returnflag, l_linestatus, l_shipinstruct, l_shipmode;
    std::vector<int32_t> l_shipdate, l_commitdate, l_receiptdate;
    std::vector<std::string> l_comment;

    for (const auto& row : rows) {
        l_orderkey.push_back(row.l_orderkey);
        l_partkey.push_back(row.l_partkey);
        l_suppkey.push_back(row.l_suppkey);
        l_linenumber.push_back(row.l_linenumber);
        l_quantity.push_back(row.l_quantity);
        l_extendedprice.push_back(row.l_extendedprice);
        l_discount.push_back(row.l_discount);
        l_tax.push_back(row.l_tax);
        l_returnflag.push_back(row.l_returnflag);
        l_linestatus.push_back(row.l_linestatus);
        l_shipdate.push_back(row.l_shipdate);
        l_commitdate.push_back(row.l_commitdate);
        l_receiptdate.push_back(row.l_receiptdate);
        l_shipinstruct.push_back(row.l_shipinstruct);
        l_shipmode.push_back(row.l_shipmode);
        l_comment.push_back(row.l_comment);
    }

    // Write binary columns
    write_binary_column(table_dir, "l_orderkey", l_orderkey);
    write_binary_column(table_dir, "l_partkey", l_partkey);
    write_binary_column(table_dir, "l_suppkey", l_suppkey);
    write_binary_column(table_dir, "l_linenumber", l_linenumber);
    write_binary_column(table_dir, "l_quantity", l_quantity);
    write_binary_column(table_dir, "l_extendedprice", l_extendedprice);
    write_binary_column(table_dir, "l_discount", l_discount);
    write_binary_column(table_dir, "l_tax", l_tax);
    write_binary_column(table_dir, "l_returnflag", l_returnflag);
    write_binary_column(table_dir, "l_linestatus", l_linestatus);
    write_binary_column(table_dir, "l_shipdate", l_shipdate);
    write_binary_column(table_dir, "l_commitdate", l_commitdate);
    write_binary_column(table_dir, "l_receiptdate", l_receiptdate);
    write_binary_column(table_dir, "l_shipinstruct", l_shipinstruct);
    write_binary_column(table_dir, "l_shipmode", l_shipmode);
    write_string_column(table_dir, "l_comment", l_comment);

    // Write dictionaries
    write_dictionary(table_dir, "l_returnflag", returnflag_dict);
    write_dictionary(table_dir, "l_linestatus", linestatus_dict);
    write_dictionary(table_dir, "l_shipinstruct", shipinstruct_dict);
    write_dictionary(table_dir, "l_shipmode", shipmode_dict);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << "  Lineitem ingestion completed in " << duration << "s" << std::endl;

    // Verify data correctness
    if (!l_shipdate.empty()) {
        int32_t min_date = *std::min_element(l_shipdate.begin(), l_shipdate.end());
        int32_t max_date = *std::max_element(l_shipdate.begin(), l_shipdate.end());
        std::cout << "  Date range: " << min_date << " - " << max_date << " days since epoch" << std::endl;
        if (min_date < 3000 || max_date < 5000) {
            std::cerr << "ERROR: Date encoding appears incorrect! Min=" << min_date << " Max=" << max_date << std::endl;
            exit(1);
        }
    }
}

// ============================================================================
// Ingest orders
// ============================================================================
void ingest_orders(const std::string& data_dir, const std::string& gendb_dir) {
    std::string orders_file = data_dir + "/orders.tbl";
    std::string table_dir = gendb_dir + "/orders";
    fs::create_directories(table_dir);

    std::cout << "Ingesting orders..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<OrdersRow> rows;
    DictionaryEncoder orderstatus_dict, orderpriority_dict, clerk_dict;

    std::ifstream f(orders_file);
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        OrdersRow row;
        parse_orders_row(line, row, orderstatus_dict, orderpriority_dict, clerk_dict);
        rows.push_back(row);
    }
    f.close();

    std::cout << "  Read " << rows.size() << " rows" << std::endl;

    // Write columns
    std::vector<int32_t> o_orderkey, o_custkey, o_orderdate, o_shippriority;
    std::vector<uint8_t> o_orderstatus, o_orderpriority, o_clerk;
    std::vector<double> o_totalprice;
    std::vector<std::string> o_comment;

    for (const auto& row : rows) {
        o_orderkey.push_back(row.o_orderkey);
        o_custkey.push_back(row.o_custkey);
        o_orderstatus.push_back(row.o_orderstatus);
        o_totalprice.push_back(row.o_totalprice);
        o_orderdate.push_back(row.o_orderdate);
        o_orderpriority.push_back(row.o_orderpriority);
        o_clerk.push_back(row.o_clerk);
        o_shippriority.push_back(row.o_shippriority);
        o_comment.push_back(row.o_comment);
    }

    write_binary_column(table_dir, "o_orderkey", o_orderkey);
    write_binary_column(table_dir, "o_custkey", o_custkey);
    write_binary_column(table_dir, "o_orderstatus", o_orderstatus);
    write_binary_column(table_dir, "o_totalprice", o_totalprice);
    write_binary_column(table_dir, "o_orderdate", o_orderdate);
    write_binary_column(table_dir, "o_orderpriority", o_orderpriority);
    write_binary_column(table_dir, "o_clerk", o_clerk);
    write_binary_column(table_dir, "o_shippriority", o_shippriority);
    write_string_column(table_dir, "o_comment", o_comment);

    write_dictionary(table_dir, "o_orderstatus", orderstatus_dict);
    write_dictionary(table_dir, "o_orderpriority", orderpriority_dict);
    write_dictionary(table_dir, "o_clerk", clerk_dict);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << "  Orders ingestion completed in " << duration << "s" << std::endl;
}

// ============================================================================
// Ingest customer
// ============================================================================
void ingest_customer(const std::string& data_dir, const std::string& gendb_dir) {
    std::string customer_file = data_dir + "/customer.tbl";
    std::string table_dir = gendb_dir + "/customer";
    fs::create_directories(table_dir);

    std::cout << "Ingesting customer..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<CustomerRow> rows;
    DictionaryEncoder mktsegment_dict;

    std::ifstream f(customer_file);
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        CustomerRow row;
        parse_customer_row(line, row, mktsegment_dict);
        rows.push_back(row);
    }
    f.close();

    std::cout << "  Read " << rows.size() << " rows" << std::endl;

    std::vector<int32_t> c_custkey, c_nationkey;
    std::vector<std::string> c_name, c_address, c_phone, c_comment;
    std::vector<double> c_acctbal;
    std::vector<uint8_t> c_mktsegment;

    for (const auto& row : rows) {
        c_custkey.push_back(row.c_custkey);
        c_name.push_back(row.c_name);
        c_address.push_back(row.c_address);
        c_nationkey.push_back(row.c_nationkey);
        c_phone.push_back(row.c_phone);
        c_acctbal.push_back(row.c_acctbal);
        c_mktsegment.push_back(row.c_mktsegment);
        c_comment.push_back(row.c_comment);
    }

    write_binary_column(table_dir, "c_custkey", c_custkey);
    write_string_column(table_dir, "c_name", c_name);
    write_string_column(table_dir, "c_address", c_address);
    write_binary_column(table_dir, "c_nationkey", c_nationkey);
    write_string_column(table_dir, "c_phone", c_phone);
    write_binary_column(table_dir, "c_acctbal", c_acctbal);
    write_binary_column(table_dir, "c_mktsegment", c_mktsegment);
    write_string_column(table_dir, "c_comment", c_comment);

    write_dictionary(table_dir, "c_mktsegment", mktsegment_dict);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << "  Customer ingestion completed in " << duration << "s" << std::endl;
}

// ============================================================================
// Ingest part
// ============================================================================
void ingest_part(const std::string& data_dir, const std::string& gendb_dir) {
    std::string part_file = data_dir + "/part.tbl";
    std::string table_dir = gendb_dir + "/part";
    fs::create_directories(table_dir);

    std::cout << "Ingesting part..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<PartRow> rows;
    std::ifstream f(part_file);
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        PartRow row;
        parse_part_row(line, row);
        rows.push_back(row);
    }
    f.close();

    std::cout << "  Read " << rows.size() << " rows" << std::endl;

    std::vector<int32_t> p_partkey, p_size;
    std::vector<std::string> p_name, p_mfgr, p_brand, p_type, p_container, p_comment;
    std::vector<double> p_retailprice;

    for (const auto& row : rows) {
        p_partkey.push_back(row.p_partkey);
        p_name.push_back(row.p_name);
        p_mfgr.push_back(row.p_mfgr);
        p_brand.push_back(row.p_brand);
        p_type.push_back(row.p_type);
        p_size.push_back(row.p_size);
        p_container.push_back(row.p_container);
        p_retailprice.push_back(row.p_retailprice);
        p_comment.push_back(row.p_comment);
    }

    write_binary_column(table_dir, "p_partkey", p_partkey);
    write_string_column(table_dir, "p_name", p_name);
    write_string_column(table_dir, "p_mfgr", p_mfgr);
    write_string_column(table_dir, "p_brand", p_brand);
    write_string_column(table_dir, "p_type", p_type);
    write_binary_column(table_dir, "p_size", p_size);
    write_string_column(table_dir, "p_container", p_container);
    write_binary_column(table_dir, "p_retailprice", p_retailprice);
    write_string_column(table_dir, "p_comment", p_comment);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << "  Part ingestion completed in " << duration << "s" << std::endl;
}

// ============================================================================
// Ingest supplier
// ============================================================================
void ingest_supplier(const std::string& data_dir, const std::string& gendb_dir) {
    std::string supplier_file = data_dir + "/supplier.tbl";
    std::string table_dir = gendb_dir + "/supplier";
    fs::create_directories(table_dir);

    std::cout << "Ingesting supplier..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<SupplierRow> rows;
    std::ifstream f(supplier_file);
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        SupplierRow row;
        parse_supplier_row(line, row);
        rows.push_back(row);
    }
    f.close();

    std::cout << "  Read " << rows.size() << " rows" << std::endl;

    std::vector<int32_t> s_suppkey, s_nationkey;
    std::vector<std::string> s_name, s_address, s_phone, s_comment;
    std::vector<double> s_acctbal;

    for (const auto& row : rows) {
        s_suppkey.push_back(row.s_suppkey);
        s_name.push_back(row.s_name);
        s_address.push_back(row.s_address);
        s_nationkey.push_back(row.s_nationkey);
        s_phone.push_back(row.s_phone);
        s_acctbal.push_back(row.s_acctbal);
        s_comment.push_back(row.s_comment);
    }

    write_binary_column(table_dir, "s_suppkey", s_suppkey);
    write_string_column(table_dir, "s_name", s_name);
    write_string_column(table_dir, "s_address", s_address);
    write_binary_column(table_dir, "s_nationkey", s_nationkey);
    write_string_column(table_dir, "s_phone", s_phone);
    write_binary_column(table_dir, "s_acctbal", s_acctbal);
    write_string_column(table_dir, "s_comment", s_comment);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << "  Supplier ingestion completed in " << duration << "s" << std::endl;
}

// ============================================================================
// Ingest partsupp
// ============================================================================
void ingest_partsupp(const std::string& data_dir, const std::string& gendb_dir) {
    std::string partsupp_file = data_dir + "/partsupp.tbl";
    std::string table_dir = gendb_dir + "/partsupp";
    fs::create_directories(table_dir);

    std::cout << "Ingesting partsupp..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<PartSuppRow> rows;
    std::ifstream f(partsupp_file);
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        PartSuppRow row;
        parse_partsupp_row(line, row);
        rows.push_back(row);
    }
    f.close();

    std::cout << "  Read " << rows.size() << " rows" << std::endl;

    std::vector<int32_t> ps_partkey, ps_suppkey, ps_availqty;
    std::vector<double> ps_supplycost;
    std::vector<std::string> ps_comment;

    for (const auto& row : rows) {
        ps_partkey.push_back(row.ps_partkey);
        ps_suppkey.push_back(row.ps_suppkey);
        ps_availqty.push_back(row.ps_availqty);
        ps_supplycost.push_back(row.ps_supplycost);
        ps_comment.push_back(row.ps_comment);
    }

    write_binary_column(table_dir, "ps_partkey", ps_partkey);
    write_binary_column(table_dir, "ps_suppkey", ps_suppkey);
    write_binary_column(table_dir, "ps_availqty", ps_availqty);
    write_binary_column(table_dir, "ps_supplycost", ps_supplycost);
    write_string_column(table_dir, "ps_comment", ps_comment);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << "  Partsupp ingestion completed in " << duration << "s" << std::endl;
}

// ============================================================================
// Ingest nation
// ============================================================================
void ingest_nation(const std::string& data_dir, const std::string& gendb_dir) {
    std::string nation_file = data_dir + "/nation.tbl";
    std::string table_dir = gendb_dir + "/nation";
    fs::create_directories(table_dir);

    std::cout << "Ingesting nation..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<NationRow> rows;
    std::ifstream f(nation_file);
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        NationRow row;
        parse_nation_row(line, row);
        rows.push_back(row);
    }
    f.close();

    std::cout << "  Read " << rows.size() << " rows" << std::endl;

    std::vector<int32_t> n_nationkey, n_regionkey;
    std::vector<std::string> n_name, n_comment;

    for (const auto& row : rows) {
        n_nationkey.push_back(row.n_nationkey);
        n_name.push_back(row.n_name);
        n_regionkey.push_back(row.n_regionkey);
        n_comment.push_back(row.n_comment);
    }

    write_binary_column(table_dir, "n_nationkey", n_nationkey);
    write_string_column(table_dir, "n_name", n_name);
    write_binary_column(table_dir, "n_regionkey", n_regionkey);
    write_string_column(table_dir, "n_comment", n_comment);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << "  Nation ingestion completed in " << duration << "s" << std::endl;
}

// ============================================================================
// Ingest region
// ============================================================================
void ingest_region(const std::string& data_dir, const std::string& gendb_dir) {
    std::string region_file = data_dir + "/region.tbl";
    std::string table_dir = gendb_dir + "/region";
    fs::create_directories(table_dir);

    std::cout << "Ingesting region..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<RegionRow> rows;
    std::ifstream f(region_file);
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        RegionRow row;
        parse_region_row(line, row);
        rows.push_back(row);
    }
    f.close();

    std::cout << "  Read " << rows.size() << " rows" << std::endl;

    std::vector<int32_t> r_regionkey;
    std::vector<std::string> r_name, r_comment;

    for (const auto& row : rows) {
        r_regionkey.push_back(row.r_regionkey);
        r_name.push_back(row.r_name);
        r_comment.push_back(row.r_comment);
    }

    write_binary_column(table_dir, "r_regionkey", r_regionkey);
    write_string_column(table_dir, "r_name", r_name);
    write_string_column(table_dir, "r_comment", r_comment);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << "  Region ingestion completed in " << duration << "s" << std::endl;
}

// ============================================================================
// Main
// ============================================================================
int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: ./ingest <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    fs::create_directories(gendb_dir);

    std::cout << "GenDB Ingestion Pipeline" << std::endl;
    std::cout << "=========================" << std::endl;
    std::cout << "Data directory: " << data_dir << std::endl;
    std::cout << "GenDB directory: " << gendb_dir << std::endl;
    std::cout << std::endl;

    auto overall_start = std::chrono::high_resolution_clock::now();

    ingest_lineitem(data_dir, gendb_dir);
    ingest_orders(data_dir, gendb_dir);
    ingest_customer(data_dir, gendb_dir);
    ingest_part(data_dir, gendb_dir);
    ingest_supplier(data_dir, gendb_dir);
    ingest_partsupp(data_dir, gendb_dir);
    ingest_nation(data_dir, gendb_dir);
    ingest_region(data_dir, gendb_dir);

    auto overall_end = std::chrono::high_resolution_clock::now();
    auto overall_duration = std::chrono::duration_cast<std::chrono::seconds>(overall_end - overall_start).count();

    std::cout << std::endl;
    std::cout << "Ingestion completed in " << overall_duration << "s" << std::endl;

    return 0;
}
