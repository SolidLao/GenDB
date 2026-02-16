#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <filesystem>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <thread>
#include <mutex>
#include <queue>
#include <chrono>

namespace fs = std::filesystem;

// ============================================================================
// Date Parsing: YYYY-MM-DD -> days since 1970-01-01 (epoch)
// ============================================================================

int32_t parse_date(const std::string& date_str) {
    if (date_str.length() < 10) return 0;
    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Days since epoch (1970-01-01)
    int days = 0;

    // Add days for complete years (1970 to year-1)
    for (int y = 1970; y < year; ++y) {
        int is_leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += is_leap ? 366 : 365;
    }

    // Add days for complete months in current year
    int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (is_leap) days_in_month[2] = 29;

    for (int m = 1; m < month; ++m) {
        days += days_in_month[m];
    }

    // Add remaining days (subtract 1 because day 1 = epoch 0 + that day's offset)
    days += (day - 1);

    return days;
}

// Self-test: verify parse_date("1970-01-01") == 0
static bool date_parse_self_test() {
    int32_t epoch_zero = parse_date("1970-01-01");
    if (epoch_zero != 0) {
        std::cerr << "ERROR: parse_date(\"1970-01-01\") = " << epoch_zero << ", expected 0\n";
        return false;
    }
    return true;
}

// ============================================================================
// Decimal Parsing: "X.YZ" -> int64_t (scaled by 100)
// ============================================================================

int64_t parse_decimal(const std::string& s, int64_t scale_factor) {
    double val = 0.0;
    try {
        val = std::stod(s);
    } catch (...) {
        return 0;
    }
    return static_cast<int64_t>(std::round(val * scale_factor));
}

// ============================================================================
// Dictionary Encoding: Build and persist dictionaries
// ============================================================================

class DictionaryEncoder {
public:
    int8_t encode(const std::string& value) {
        if (dict_.find(value) == dict_.end()) {
            if (dict_.size() >= 256) {
                std::cerr << "WARNING: Dictionary overflow (>256 entries), reusing last code\n";
                return static_cast<int8_t>(dict_.size() - 1);
            }
            dict_[value] = static_cast<int8_t>(dict_.size());
        }
        return dict_[value];
    }

    void write_dictionary(const std::string& output_path) {
        std::ofstream f(output_path);
        for (const auto& [value, code] : dict_) {
            f << static_cast<int>(code) << "=" << value << "\n";
        }
    }

    const std::unordered_map<std::string, int8_t>& get_dict() const {
        return dict_;
    }

private:
    std::unordered_map<std::string, int8_t> dict_;
};

// ============================================================================
// Table Data Structures
// ============================================================================

struct LineitemRow {
    int32_t l_orderkey, l_partkey, l_suppkey, l_linenumber;
    int64_t l_quantity, l_extendedprice, l_discount, l_tax;
    int8_t l_returnflag, l_linestatus, l_shipinstruct, l_shipmode;
    int32_t l_shipdate, l_commitdate, l_receiptdate;
    std::string l_comment;
};

struct OrdersRow {
    int32_t o_orderkey, o_custkey, o_shippriority;
    int8_t o_orderstatus, o_orderpriority, o_clerk;
    int64_t o_totalprice;
    int32_t o_orderdate;
    std::string o_comment;
};

struct CustomerRow {
    int32_t c_custkey, c_nationkey;
    int8_t c_mktsegment;
    int64_t c_acctbal;
    std::string c_name, c_address, c_phone, c_comment;
};

struct PartRow {
    int32_t p_partkey, p_size;
    int8_t p_brand, p_container;
    int16_t p_type;
    int64_t p_retailprice;
    std::string p_name, p_mfgr, p_comment;
};

struct PartsuppRow {
    int32_t ps_partkey, ps_suppkey, ps_availqty;
    int64_t ps_supplycost;
    std::string ps_comment;
};

struct SupplierRow {
    int32_t s_suppkey, s_nationkey;
    int64_t s_acctbal;
    std::string s_name, s_address, s_phone, s_comment;
};

struct NationRow {
    int32_t n_nationkey, n_regionkey;
    std::string n_name, n_comment;
};

struct RegionRow {
    int32_t r_regionkey;
    std::string r_name, r_comment;
};

// ============================================================================
// Binary Column Writers
// ============================================================================

template <typename T>
void write_column(const std::string& path, const std::vector<T>& data) {
    std::ofstream f(path, std::ios::binary);
    f.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T));
}

void write_string_column(const std::string& path, const std::vector<std::string>& data) {
    std::ofstream f(path, std::ios::binary);
    uint32_t count = data.size();
    f.write(reinterpret_cast<const char*>(&count), sizeof(count));

    std::vector<uint32_t> offsets;
    offsets.push_back(0);
    for (size_t i = 0; i < data.size() - 1; ++i) {
        offsets.push_back(offsets.back() + data[i].length());
    }

    f.write(reinterpret_cast<const char*>(offsets.data()), offsets.size() * sizeof(uint32_t));
    for (const auto& s : data) {
        f.write(s.c_str(), s.length());
    }
}

// ============================================================================
// Parse and Ingest Lineitem
// ============================================================================

void ingest_lineitem(const std::string& input_path, const std::string& output_dir) {
    std::cout << "[lineitem] Parsing...\n";
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<int32_t> l_orderkey, l_partkey, l_suppkey, l_linenumber;
    std::vector<int64_t> l_quantity, l_extendedprice, l_discount, l_tax;
    std::vector<int8_t> l_returnflag, l_linestatus, l_shipinstruct, l_shipmode;
    std::vector<int32_t> l_shipdate, l_commitdate, l_receiptdate;
    std::vector<std::string> l_comment;

    DictionaryEncoder returnflag_dict, linestatus_dict, shipinstruct_dict, shipmode_dict;

    std::ifstream f(input_path);
    std::string line;
    size_t row_count = 0;

    while (std::getline(f, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string col;
        int col_idx = 0;

        LineitemRow row{};
        std::string temp_returnflag, temp_linestatus, temp_shipinstruct, temp_shipmode, temp_date;

        while (std::getline(iss, col, '|')) {
            switch (col_idx) {
                case 0: row.l_orderkey = std::stoi(col); break;
                case 1: row.l_partkey = std::stoi(col); break;
                case 2: row.l_suppkey = std::stoi(col); break;
                case 3: row.l_linenumber = std::stoi(col); break;
                case 4: row.l_quantity = parse_decimal(col, 100); break;
                case 5: row.l_extendedprice = parse_decimal(col, 100); break;
                case 6: row.l_discount = parse_decimal(col, 100); break;
                case 7: row.l_tax = parse_decimal(col, 100); break;
                case 8: temp_returnflag = col; break;
                case 9: temp_linestatus = col; break;
                case 10: temp_date = col; row.l_shipdate = parse_date(col); break;
                case 11: row.l_commitdate = parse_date(col); break;
                case 12: row.l_receiptdate = parse_date(col); break;
                case 13: temp_shipinstruct = col; break;
                case 14: temp_shipmode = col; break;
                case 15: row.l_comment = col; break;
            }
            ++col_idx;
        }

        l_orderkey.push_back(row.l_orderkey);
        l_partkey.push_back(row.l_partkey);
        l_suppkey.push_back(row.l_suppkey);
        l_linenumber.push_back(row.l_linenumber);
        l_quantity.push_back(row.l_quantity);
        l_extendedprice.push_back(row.l_extendedprice);
        l_discount.push_back(row.l_discount);
        l_tax.push_back(row.l_tax);
        l_returnflag.push_back(returnflag_dict.encode(temp_returnflag));
        l_linestatus.push_back(linestatus_dict.encode(temp_linestatus));
        l_shipdate.push_back(row.l_shipdate);
        l_commitdate.push_back(row.l_commitdate);
        l_receiptdate.push_back(row.l_receiptdate);
        l_shipinstruct.push_back(shipinstruct_dict.encode(temp_shipinstruct));
        l_shipmode.push_back(shipmode_dict.encode(temp_shipmode));
        l_comment.push_back(row.l_comment);

        ++row_count;
        if (row_count % 5000000 == 0) {
            std::cout << "[lineitem] Parsed " << row_count << " rows...\n";
        }
    }

    std::cout << "[lineitem] Writing binary columns...\n";
    write_column(output_dir + "/lineitem/l_orderkey.bin", l_orderkey);
    write_column(output_dir + "/lineitem/l_partkey.bin", l_partkey);
    write_column(output_dir + "/lineitem/l_suppkey.bin", l_suppkey);
    write_column(output_dir + "/lineitem/l_linenumber.bin", l_linenumber);
    write_column(output_dir + "/lineitem/l_quantity.bin", l_quantity);
    write_column(output_dir + "/lineitem/l_extendedprice.bin", l_extendedprice);
    write_column(output_dir + "/lineitem/l_discount.bin", l_discount);
    write_column(output_dir + "/lineitem/l_tax.bin", l_tax);
    write_column(output_dir + "/lineitem/l_returnflag.bin", l_returnflag);
    write_column(output_dir + "/lineitem/l_linestatus.bin", l_linestatus);
    write_column(output_dir + "/lineitem/l_shipdate.bin", l_shipdate);
    write_column(output_dir + "/lineitem/l_commitdate.bin", l_commitdate);
    write_column(output_dir + "/lineitem/l_receiptdate.bin", l_receiptdate);
    write_column(output_dir + "/lineitem/l_shipinstruct.bin", l_shipinstruct);
    write_column(output_dir + "/lineitem/l_shipmode.bin", l_shipmode);
    write_string_column(output_dir + "/lineitem/l_comment.bin", l_comment);

    returnflag_dict.write_dictionary(output_dir + "/lineitem/l_returnflag_dict.txt");
    linestatus_dict.write_dictionary(output_dir + "/lineitem/l_linestatus_dict.txt");
    shipinstruct_dict.write_dictionary(output_dir + "/lineitem/l_shipinstruct_dict.txt");
    shipmode_dict.write_dictionary(output_dir + "/lineitem/l_shipmode_dict.txt");

    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end - start);
    std::cout << "[lineitem] Ingested " << row_count << " rows in " << elapsed.count() << "s\n";
}

// ============================================================================
// Parse and Ingest Orders
// ============================================================================

void ingest_orders(const std::string& input_path, const std::string& output_dir) {
    std::cout << "[orders] Parsing...\n";
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<int32_t> o_orderkey, o_custkey, o_shippriority, o_orderdate;
    std::vector<int8_t> o_orderstatus, o_orderpriority, o_clerk;
    std::vector<int64_t> o_totalprice;
    std::vector<std::string> o_comment;

    DictionaryEncoder orderstatus_dict, orderpriority_dict, clerk_dict;

    std::ifstream f(input_path);
    std::string line;
    size_t row_count = 0;

    while (std::getline(f, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string col;
        int col_idx = 0;

        OrdersRow row{};
        std::string temp_orderstatus, temp_orderpriority, temp_clerk;

        while (std::getline(iss, col, '|')) {
            switch (col_idx) {
                case 0: row.o_orderkey = std::stoi(col); break;
                case 1: row.o_custkey = std::stoi(col); break;
                case 2: temp_orderstatus = col; break;
                case 3: row.o_totalprice = parse_decimal(col, 100); break;
                case 4: row.o_orderdate = parse_date(col); break;
                case 5: temp_orderpriority = col; break;
                case 6: temp_clerk = col; break;
                case 7: row.o_shippriority = std::stoi(col); break;
                case 8: row.o_comment = col; break;
            }
            ++col_idx;
        }

        o_orderkey.push_back(row.o_orderkey);
        o_custkey.push_back(row.o_custkey);
        o_orderstatus.push_back(orderstatus_dict.encode(temp_orderstatus));
        o_totalprice.push_back(row.o_totalprice);
        o_orderdate.push_back(row.o_orderdate);
        o_orderpriority.push_back(orderpriority_dict.encode(temp_orderpriority));
        o_clerk.push_back(clerk_dict.encode(temp_clerk));
        o_shippriority.push_back(row.o_shippriority);
        o_comment.push_back(row.o_comment);

        ++row_count;
        if (row_count % 2000000 == 0) {
            std::cout << "[orders] Parsed " << row_count << " rows...\n";
        }
    }

    std::cout << "[orders] Writing binary columns...\n";
    write_column(output_dir + "/orders/o_orderkey.bin", o_orderkey);
    write_column(output_dir + "/orders/o_custkey.bin", o_custkey);
    write_column(output_dir + "/orders/o_orderstatus.bin", o_orderstatus);
    write_column(output_dir + "/orders/o_totalprice.bin", o_totalprice);
    write_column(output_dir + "/orders/o_orderdate.bin", o_orderdate);
    write_column(output_dir + "/orders/o_orderpriority.bin", o_orderpriority);
    write_column(output_dir + "/orders/o_clerk.bin", o_clerk);
    write_column(output_dir + "/orders/o_shippriority.bin", o_shippriority);
    write_string_column(output_dir + "/orders/o_comment.bin", o_comment);

    orderstatus_dict.write_dictionary(output_dir + "/orders/o_orderstatus_dict.txt");
    orderpriority_dict.write_dictionary(output_dir + "/orders/o_orderpriority_dict.txt");
    clerk_dict.write_dictionary(output_dir + "/orders/o_clerk_dict.txt");

    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end - start);
    std::cout << "[orders] Ingested " << row_count << " rows in " << elapsed.count() << "s\n";
}

// ============================================================================
// Parse and Ingest Customer
// ============================================================================

void ingest_customer(const std::string& input_path, const std::string& output_dir) {
    std::cout << "[customer] Parsing...\n";
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<int32_t> c_custkey, c_nationkey;
    std::vector<int8_t> c_mktsegment;
    std::vector<int64_t> c_acctbal;
    std::vector<std::string> c_name, c_address, c_phone, c_comment;

    DictionaryEncoder mktsegment_dict;

    std::ifstream f(input_path);
    std::string line;
    size_t row_count = 0;

    while (std::getline(f, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string col;
        int col_idx = 0;

        CustomerRow row{};
        std::string temp_mktsegment;

        while (std::getline(iss, col, '|')) {
            switch (col_idx) {
                case 0: row.c_custkey = std::stoi(col); break;
                case 1: row.c_name = col; break;
                case 2: row.c_address = col; break;
                case 3: row.c_nationkey = std::stoi(col); break;
                case 4: row.c_phone = col; break;
                case 5: row.c_acctbal = parse_decimal(col, 100); break;
                case 6: temp_mktsegment = col; break;
                case 7: row.c_comment = col; break;
            }
            ++col_idx;
        }

        c_custkey.push_back(row.c_custkey);
        c_name.push_back(row.c_name);
        c_address.push_back(row.c_address);
        c_nationkey.push_back(row.c_nationkey);
        c_phone.push_back(row.c_phone);
        c_acctbal.push_back(row.c_acctbal);
        c_mktsegment.push_back(mktsegment_dict.encode(temp_mktsegment));
        c_comment.push_back(row.c_comment);

        ++row_count;
        if (row_count % 500000 == 0) {
            std::cout << "[customer] Parsed " << row_count << " rows...\n";
        }
    }

    std::cout << "[customer] Writing binary columns...\n";
    write_column(output_dir + "/customer/c_custkey.bin", c_custkey);
    write_string_column(output_dir + "/customer/c_name.bin", c_name);
    write_string_column(output_dir + "/customer/c_address.bin", c_address);
    write_column(output_dir + "/customer/c_nationkey.bin", c_nationkey);
    write_string_column(output_dir + "/customer/c_phone.bin", c_phone);
    write_column(output_dir + "/customer/c_acctbal.bin", c_acctbal);
    write_column(output_dir + "/customer/c_mktsegment.bin", c_mktsegment);
    write_string_column(output_dir + "/customer/c_comment.bin", c_comment);

    mktsegment_dict.write_dictionary(output_dir + "/customer/c_mktsegment_dict.txt");

    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end - start);
    std::cout << "[customer] Ingested " << row_count << " rows in " << elapsed.count() << "s\n";
}

// ============================================================================
// Parse and Ingest Part
// ============================================================================

void ingest_part(const std::string& input_path, const std::string& output_dir) {
    std::cout << "[part] Parsing...\n";
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<int32_t> p_partkey, p_size;
    std::vector<int8_t> p_brand, p_container;
    std::vector<int16_t> p_type;
    std::vector<int64_t> p_retailprice;
    std::vector<std::string> p_name, p_mfgr, p_comment;

    DictionaryEncoder brand_dict, type_dict, container_dict;

    std::ifstream f(input_path);
    std::string line;
    size_t row_count = 0;

    while (std::getline(f, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string col;
        int col_idx = 0;

        PartRow row{};
        std::string temp_brand, temp_type, temp_container;

        while (std::getline(iss, col, '|')) {
            switch (col_idx) {
                case 0: row.p_partkey = std::stoi(col); break;
                case 1: row.p_name = col; break;
                case 2: row.p_mfgr = col; break;
                case 3: temp_brand = col; break;
                case 4: temp_type = col; break;
                case 5: row.p_size = std::stoi(col); break;
                case 6: temp_container = col; break;
                case 7: row.p_retailprice = parse_decimal(col, 100); break;
                case 8: row.p_comment = col; break;
            }
            ++col_idx;
        }

        p_partkey.push_back(row.p_partkey);
        p_name.push_back(row.p_name);
        p_mfgr.push_back(row.p_mfgr);
        p_brand.push_back(brand_dict.encode(temp_brand));
        p_type.push_back(type_dict.encode(temp_type));
        p_size.push_back(row.p_size);
        p_container.push_back(container_dict.encode(temp_container));
        p_retailprice.push_back(row.p_retailprice);
        p_comment.push_back(row.p_comment);

        ++row_count;
        if (row_count % 500000 == 0) {
            std::cout << "[part] Parsed " << row_count << " rows...\n";
        }
    }

    std::cout << "[part] Writing binary columns...\n";
    write_column(output_dir + "/part/p_partkey.bin", p_partkey);
    write_string_column(output_dir + "/part/p_name.bin", p_name);
    write_string_column(output_dir + "/part/p_mfgr.bin", p_mfgr);
    write_column(output_dir + "/part/p_brand.bin", p_brand);
    write_column(output_dir + "/part/p_type.bin", p_type);
    write_column(output_dir + "/part/p_size.bin", p_size);
    write_column(output_dir + "/part/p_container.bin", p_container);
    write_column(output_dir + "/part/p_retailprice.bin", p_retailprice);
    write_string_column(output_dir + "/part/p_comment.bin", p_comment);

    brand_dict.write_dictionary(output_dir + "/part/p_brand_dict.txt");
    type_dict.write_dictionary(output_dir + "/part/p_type_dict.txt");
    container_dict.write_dictionary(output_dir + "/part/p_container_dict.txt");

    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end - start);
    std::cout << "[part] Ingested " << row_count << " rows in " << elapsed.count() << "s\n";
}

// ============================================================================
// Parse and Ingest Partsupp
// ============================================================================

void ingest_partsupp(const std::string& input_path, const std::string& output_dir) {
    std::cout << "[partsupp] Parsing...\n";
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<int32_t> ps_partkey, ps_suppkey, ps_availqty;
    std::vector<int64_t> ps_supplycost;
    std::vector<std::string> ps_comment;

    std::ifstream f(input_path);
    std::string line;
    size_t row_count = 0;

    while (std::getline(f, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string col;
        int col_idx = 0;

        PartsuppRow row{};

        while (std::getline(iss, col, '|')) {
            switch (col_idx) {
                case 0: row.ps_partkey = std::stoi(col); break;
                case 1: row.ps_suppkey = std::stoi(col); break;
                case 2: row.ps_availqty = std::stoi(col); break;
                case 3: row.ps_supplycost = parse_decimal(col, 100); break;
                case 4: row.ps_comment = col; break;
            }
            ++col_idx;
        }

        ps_partkey.push_back(row.ps_partkey);
        ps_suppkey.push_back(row.ps_suppkey);
        ps_availqty.push_back(row.ps_availqty);
        ps_supplycost.push_back(row.ps_supplycost);
        ps_comment.push_back(row.ps_comment);

        ++row_count;
        if (row_count % 1000000 == 0) {
            std::cout << "[partsupp] Parsed " << row_count << " rows...\n";
        }
    }

    std::cout << "[partsupp] Writing binary columns...\n";
    write_column(output_dir + "/partsupp/ps_partkey.bin", ps_partkey);
    write_column(output_dir + "/partsupp/ps_suppkey.bin", ps_suppkey);
    write_column(output_dir + "/partsupp/ps_availqty.bin", ps_availqty);
    write_column(output_dir + "/partsupp/ps_supplycost.bin", ps_supplycost);
    write_string_column(output_dir + "/partsupp/ps_comment.bin", ps_comment);

    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end - start);
    std::cout << "[partsupp] Ingested " << row_count << " rows in " << elapsed.count() << "s\n";
}

// ============================================================================
// Parse and Ingest Supplier
// ============================================================================

void ingest_supplier(const std::string& input_path, const std::string& output_dir) {
    std::cout << "[supplier] Parsing...\n";
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<int32_t> s_suppkey, s_nationkey;
    std::vector<int64_t> s_acctbal;
    std::vector<std::string> s_name, s_address, s_phone, s_comment;

    std::ifstream f(input_path);
    std::string line;
    size_t row_count = 0;

    while (std::getline(f, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string col;
        int col_idx = 0;

        SupplierRow row{};

        while (std::getline(iss, col, '|')) {
            switch (col_idx) {
                case 0: row.s_suppkey = std::stoi(col); break;
                case 1: row.s_name = col; break;
                case 2: row.s_address = col; break;
                case 3: row.s_nationkey = std::stoi(col); break;
                case 4: row.s_phone = col; break;
                case 5: row.s_acctbal = parse_decimal(col, 100); break;
                case 6: row.s_comment = col; break;
            }
            ++col_idx;
        }

        s_suppkey.push_back(row.s_suppkey);
        s_name.push_back(row.s_name);
        s_address.push_back(row.s_address);
        s_nationkey.push_back(row.s_nationkey);
        s_phone.push_back(row.s_phone);
        s_acctbal.push_back(row.s_acctbal);
        s_comment.push_back(row.s_comment);

        ++row_count;
    }

    std::cout << "[supplier] Writing binary columns...\n";
    write_column(output_dir + "/supplier/s_suppkey.bin", s_suppkey);
    write_string_column(output_dir + "/supplier/s_name.bin", s_name);
    write_string_column(output_dir + "/supplier/s_address.bin", s_address);
    write_column(output_dir + "/supplier/s_nationkey.bin", s_nationkey);
    write_string_column(output_dir + "/supplier/s_phone.bin", s_phone);
    write_column(output_dir + "/supplier/s_acctbal.bin", s_acctbal);
    write_string_column(output_dir + "/supplier/s_comment.bin", s_comment);

    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end - start);
    std::cout << "[supplier] Ingested " << row_count << " rows in " << elapsed.count() << "s\n";
}

// ============================================================================
// Parse and Ingest Nation
// ============================================================================

void ingest_nation(const std::string& input_path, const std::string& output_dir) {
    std::cout << "[nation] Parsing...\n";

    std::vector<int32_t> n_nationkey, n_regionkey;
    std::vector<std::string> n_name, n_comment;

    std::ifstream f(input_path);
    std::string line;
    size_t row_count = 0;

    while (std::getline(f, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string col;
        int col_idx = 0;

        NationRow row{};

        while (std::getline(iss, col, '|')) {
            switch (col_idx) {
                case 0: row.n_nationkey = std::stoi(col); break;
                case 1: row.n_name = col; break;
                case 2: row.n_regionkey = std::stoi(col); break;
                case 3: row.n_comment = col; break;
            }
            ++col_idx;
        }

        n_nationkey.push_back(row.n_nationkey);
        n_name.push_back(row.n_name);
        n_regionkey.push_back(row.n_regionkey);
        n_comment.push_back(row.n_comment);

        ++row_count;
    }

    std::cout << "[nation] Writing binary columns...\n";
    write_column(output_dir + "/nation/n_nationkey.bin", n_nationkey);
    write_string_column(output_dir + "/nation/n_name.bin", n_name);
    write_column(output_dir + "/nation/n_regionkey.bin", n_regionkey);
    write_string_column(output_dir + "/nation/n_comment.bin", n_comment);

    std::cout << "[nation] Ingested " << row_count << " rows\n";
}

// ============================================================================
// Parse and Ingest Region
// ============================================================================

void ingest_region(const std::string& input_path, const std::string& output_dir) {
    std::cout << "[region] Parsing...\n";

    std::vector<int32_t> r_regionkey;
    std::vector<std::string> r_name, r_comment;

    std::ifstream f(input_path);
    std::string line;
    size_t row_count = 0;

    while (std::getline(f, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string col;
        int col_idx = 0;

        RegionRow row{};

        while (std::getline(iss, col, '|')) {
            switch (col_idx) {
                case 0: row.r_regionkey = std::stoi(col); break;
                case 1: row.r_name = col; break;
                case 2: row.r_comment = col; break;
            }
            ++col_idx;
        }

        r_regionkey.push_back(row.r_regionkey);
        r_name.push_back(row.r_name);
        r_comment.push_back(row.r_comment);

        ++row_count;
    }

    std::cout << "[region] Writing binary columns...\n";
    write_column(output_dir + "/region/r_regionkey.bin", r_regionkey);
    write_string_column(output_dir + "/region/r_name.bin", r_name);
    write_string_column(output_dir + "/region/r_comment.bin", r_comment);

    std::cout << "[region] Ingested " << row_count << " rows\n";
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <input_dir> <output_dir>\n";
        return 1;
    }

    if (!date_parse_self_test()) {
        std::cerr << "FATAL: Date parsing self-test failed\n";
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
    fs::create_directories(output_dir + "/indexes");

    std::cout << "=== TPC-H SF10 Ingestion ===\n";
    auto start_total = std::chrono::high_resolution_clock::now();

    // Ingest all tables
    ingest_lineitem(input_dir + "/lineitem.tbl", output_dir);
    ingest_orders(input_dir + "/orders.tbl", output_dir);
    ingest_customer(input_dir + "/customer.tbl", output_dir);
    ingest_part(input_dir + "/part.tbl", output_dir);
    ingest_partsupp(input_dir + "/partsupp.tbl", output_dir);
    ingest_supplier(input_dir + "/supplier.tbl", output_dir);
    ingest_nation(input_dir + "/nation.tbl", output_dir);
    ingest_region(input_dir + "/region.tbl", output_dir);

    auto end_total = std::chrono::high_resolution_clock::now();
    auto elapsed_total = std::chrono::duration_cast<std::chrono::seconds>(end_total - start_total);
    std::cout << "\n=== Total Ingestion Time: " << elapsed_total.count() << "s ===\n";

    // Post-ingestion checks
    std::cout << "\n=== Post-Ingestion Verification ===\n";
    std::cout << "Date encoding test: parse_date(\"1970-01-01\") = " << parse_date("1970-01-01") << " (expect 0)\n";
    std::cout << "Decimal encoding test: parse_decimal(\"0.04\", 100) = " << parse_decimal("0.04", 100) << " (expect 4)\n";
    std::cout << "Decimal encoding test: parse_decimal(\"99.99\", 100) = " << parse_decimal("99.99", 100) << " (expect 9999)\n";

    return 0;
}
