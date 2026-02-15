#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <cstring>
#include <chrono>
#include <thread>
#include <mutex>
#include <cmath>
#include <charconv>
#include <unordered_map>
#include <filesystem>

namespace fs = std::filesystem;

// Global thread pool
constexpr int NUM_THREADS = 64;

// Helper: Date parsing (YYYY-MM-DD to days since 1970-01-01)
int32_t parse_date(const std::string& s) {
    if (s.length() < 10) return 0;
    int year = 0, month = 0, day = 0;
    const char* p = s.c_str();
    // Parse YYYY
    for (int i = 0; i < 4 && isdigit(*p); i++, p++) {
        year = year * 10 + (*p - '0');
    }
    if (*p != '-') return 0;
    p++;
    // Parse MM
    for (int i = 0; i < 2 && isdigit(*p); i++, p++) {
        month = month * 10 + (*p - '0');
    }
    if (*p != '-') return 0;
    p++;
    // Parse DD
    for (int i = 0; i < 2 && isdigit(*p); i++, p++) {
        day = day * 10 + (*p - '0');
    }
    // Convert to days since epoch (1970-01-01)
    int days = 0;
    for (int y = 1970; y < year; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        month_days[2] = 29;
    }
    for (int m = 1; m < month; m++) {
        days += month_days[m];
    }
    days += day;
    return days;
}

// Helper: Decimal parsing (string to int64_t with scale factor 100)
int64_t parse_decimal(const std::string& s, int scale_factor = 100) {
    double val = 0.0;
    size_t idx = 0;
    bool negative = false;
    if (s[0] == '-') {
        negative = true;
        idx = 1;
    }
    auto [ptr, ec] = std::from_chars(s.c_str() + idx, s.c_str() + s.length(), val);
    if (ec != std::errc()) return 0;
    int64_t result = static_cast<int64_t>(std::round(val * scale_factor));
    return negative ? -result : result;
}

// Helper: Dictionary encoding for single char
struct CharDict {
    std::unordered_map<char, uint8_t> encode_map;
    std::vector<char> decode_vec;
    uint8_t next_code = 0;

    uint8_t encode(char c) {
        if (encode_map.find(c) == encode_map.end()) {
            encode_map[c] = next_code;
            decode_vec.push_back(c);
            return next_code++;
        }
        return encode_map[c];
    }

    void save_dict(const std::string& path) {
        std::ofstream f(path);
        for (uint8_t i = 0; i < decode_vec.size(); i++) {
            f << (int)i << "=" << decode_vec[i] << "\n";
        }
        f.close();
    }
};

// Helper: Dictionary encoding for strings
struct StringDict {
    std::unordered_map<std::string, uint16_t> encode_map;
    std::vector<std::string> decode_vec;
    uint16_t next_code = 0;

    uint16_t encode(const std::string& s) {
        if (encode_map.find(s) == encode_map.end()) {
            encode_map[s] = next_code;
            decode_vec.push_back(s);
            return next_code++;
        }
        return encode_map[s];
    }

    void save_dict(const std::string& path) {
        std::ofstream f(path);
        for (uint16_t i = 0; i < decode_vec.size(); i++) {
            f << i << "=" << decode_vec[i] << "\n";
        }
        f.close();
    }
};

// ============================================================================
// LINEITEM TABLE
// ============================================================================

struct LineitemRow {
    int32_t l_orderkey;
    int32_t l_partkey;
    int32_t l_suppkey;
    int32_t l_linenumber;
    int64_t l_quantity;
    int64_t l_extendedprice;
    int64_t l_discount;
    int64_t l_tax;
    uint8_t l_returnflag_code;
    uint8_t l_linestatus_code;
    int32_t l_shipdate;
    int32_t l_commitdate;
    int32_t l_receiptdate;
    uint16_t l_shipinstruct_code;
    uint16_t l_shipmode_code;
    std::string l_comment;
};

void ingest_lineitem(const std::string& data_dir, const std::string& gendb_dir,
                     CharDict& returnflag_dict, CharDict& linestatus_dict,
                     StringDict& shipinstruct_dict, StringDict& shipmode_dict) {
    std::string infile = data_dir + "/lineitem.tbl";
    std::ifstream f(infile);
    if (!f) {
        std::cerr << "Error: cannot open " << infile << std::endl;
        return;
    }

    std::vector<int32_t> l_orderkey_vec;
    std::vector<int32_t> l_partkey_vec;
    std::vector<int32_t> l_suppkey_vec;
    std::vector<int32_t> l_linenumber_vec;
    std::vector<int64_t> l_quantity_vec;
    std::vector<int64_t> l_extendedprice_vec;
    std::vector<int64_t> l_discount_vec;
    std::vector<int64_t> l_tax_vec;
    std::vector<uint8_t> l_returnflag_vec;
    std::vector<uint8_t> l_linestatus_vec;
    std::vector<int32_t> l_shipdate_vec;
    std::vector<int32_t> l_commitdate_vec;
    std::vector<int32_t> l_receiptdate_vec;
    std::vector<uint16_t> l_shipinstruct_vec;
    std::vector<uint16_t> l_shipmode_vec;
    std::vector<std::string> l_comment_vec;

    std::string line;
    int row_count = 0;
    while (std::getline(f, line)) {
        std::istringstream iss(line);
        std::string field;
        int col = 0;

        int32_t orderkey = 0, partkey = 0, suppkey = 0, linenumber = 0;
        int64_t quantity = 0, extendedprice = 0, discount = 0, tax = 0;
        uint8_t returnflag_code = 0, linestatus_code = 0;
        int32_t shipdate = 0, commitdate = 0, receiptdate = 0;
        uint16_t shipinstruct_code = 0, shipmode_code = 0;
        std::string comment;

        while (std::getline(iss, field, '|') && col < 16) {
            switch (col) {
                case 0: orderkey = std::stoi(field); break;
                case 1: partkey = std::stoi(field); break;
                case 2: suppkey = std::stoi(field); break;
                case 3: linenumber = std::stoi(field); break;
                case 4: quantity = parse_decimal(field, 100); break;
                case 5: extendedprice = parse_decimal(field, 100); break;
                case 6: discount = parse_decimal(field, 100); break;
                case 7: tax = parse_decimal(field, 100); break;
                case 8: returnflag_code = returnflag_dict.encode(field[0]); break;
                case 9: linestatus_code = linestatus_dict.encode(field[0]); break;
                case 10: shipdate = parse_date(field); break;
                case 11: commitdate = parse_date(field); break;
                case 12: receiptdate = parse_date(field); break;
                case 13: shipinstruct_code = shipinstruct_dict.encode(field); break;
                case 14: shipmode_code = shipmode_dict.encode(field); break;
                case 15: comment = field; break;
            }
            col++;
        }

        l_orderkey_vec.push_back(orderkey);
        l_partkey_vec.push_back(partkey);
        l_suppkey_vec.push_back(suppkey);
        l_linenumber_vec.push_back(linenumber);
        l_quantity_vec.push_back(quantity);
        l_extendedprice_vec.push_back(extendedprice);
        l_discount_vec.push_back(discount);
        l_tax_vec.push_back(tax);
        l_returnflag_vec.push_back(returnflag_code);
        l_linestatus_vec.push_back(linestatus_code);
        l_shipdate_vec.push_back(shipdate);
        l_commitdate_vec.push_back(commitdate);
        l_receiptdate_vec.push_back(receiptdate);
        l_shipinstruct_vec.push_back(shipinstruct_code);
        l_shipmode_vec.push_back(shipmode_code);
        l_comment_vec.push_back(comment);

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "Ingested " << row_count << " lineitem rows\n";
        }
    }
    f.close();

    std::cout << "Writing lineitem binary columns...\n";
    std::ofstream out;

    std::string outdir = gendb_dir + "/lineitem";
    fs::create_directories(outdir);

    out.open(outdir + "/l_orderkey.bin", std::ios::binary);
    out.write((char*)l_orderkey_vec.data(), l_orderkey_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/l_partkey.bin", std::ios::binary);
    out.write((char*)l_partkey_vec.data(), l_partkey_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/l_suppkey.bin", std::ios::binary);
    out.write((char*)l_suppkey_vec.data(), l_suppkey_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/l_linenumber.bin", std::ios::binary);
    out.write((char*)l_linenumber_vec.data(), l_linenumber_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/l_quantity.bin", std::ios::binary);
    out.write((char*)l_quantity_vec.data(), l_quantity_vec.size() * sizeof(int64_t));
    out.close();

    out.open(outdir + "/l_extendedprice.bin", std::ios::binary);
    out.write((char*)l_extendedprice_vec.data(), l_extendedprice_vec.size() * sizeof(int64_t));
    out.close();

    out.open(outdir + "/l_discount.bin", std::ios::binary);
    out.write((char*)l_discount_vec.data(), l_discount_vec.size() * sizeof(int64_t));
    out.close();

    out.open(outdir + "/l_tax.bin", std::ios::binary);
    out.write((char*)l_tax_vec.data(), l_tax_vec.size() * sizeof(int64_t));
    out.close();

    out.open(outdir + "/l_returnflag.bin", std::ios::binary);
    out.write((char*)l_returnflag_vec.data(), l_returnflag_vec.size() * sizeof(uint8_t));
    out.close();

    out.open(outdir + "/l_linestatus.bin", std::ios::binary);
    out.write((char*)l_linestatus_vec.data(), l_linestatus_vec.size() * sizeof(uint8_t));
    out.close();

    out.open(outdir + "/l_shipdate.bin", std::ios::binary);
    out.write((char*)l_shipdate_vec.data(), l_shipdate_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/l_commitdate.bin", std::ios::binary);
    out.write((char*)l_commitdate_vec.data(), l_commitdate_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/l_receiptdate.bin", std::ios::binary);
    out.write((char*)l_receiptdate_vec.data(), l_receiptdate_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/l_shipinstruct.bin", std::ios::binary);
    out.write((char*)l_shipinstruct_vec.data(), l_shipinstruct_vec.size() * sizeof(uint16_t));
    out.close();

    out.open(outdir + "/l_shipmode.bin", std::ios::binary);
    out.write((char*)l_shipmode_vec.data(), l_shipmode_vec.size() * sizeof(uint16_t));
    out.close();

    out.open(outdir + "/l_comment.bin", std::ios::binary);
    for (const auto& s : l_comment_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    returnflag_dict.save_dict(outdir + "/l_returnflag_dict.txt");
    linestatus_dict.save_dict(outdir + "/l_linestatus_dict.txt");
    shipinstruct_dict.save_dict(outdir + "/l_shipinstruct_dict.txt");
    shipmode_dict.save_dict(outdir + "/l_shipmode_dict.txt");

    std::cout << "Lineitem: wrote " << row_count << " rows\n";
}

// ============================================================================
// ORDERS TABLE
// ============================================================================

void ingest_orders(const std::string& data_dir, const std::string& gendb_dir,
                   CharDict& orderstatus_dict, StringDict& orderpriority_dict) {
    std::string infile = data_dir + "/orders.tbl";
    std::ifstream f(infile);
    if (!f) {
        std::cerr << "Error: cannot open " << infile << std::endl;
        return;
    }

    std::vector<int32_t> o_orderkey_vec;
    std::vector<int32_t> o_custkey_vec;
    std::vector<uint8_t> o_orderstatus_vec;
    std::vector<int64_t> o_totalprice_vec;
    std::vector<int32_t> o_orderdate_vec;
    std::vector<uint16_t> o_orderpriority_vec;
    std::vector<std::string> o_clerk_vec;
    std::vector<int32_t> o_shippriority_vec;
    std::vector<std::string> o_comment_vec;

    std::string line;
    int row_count = 0;
    while (std::getline(f, line)) {
        std::istringstream iss(line);
        std::string field;
        int col = 0;

        int32_t orderkey = 0, custkey = 0, totalprice = 0, shippriority = 0;
        uint8_t orderstatus_code = 0;
        int32_t orderdate = 0;
        uint16_t orderpriority_code = 0;
        std::string clerk, comment;

        while (std::getline(iss, field, '|') && col < 9) {
            switch (col) {
                case 0: orderkey = std::stoi(field); break;
                case 1: custkey = std::stoi(field); break;
                case 2: orderstatus_code = orderstatus_dict.encode(field[0]); break;
                case 3: totalprice = parse_decimal(field, 100); break;
                case 4: orderdate = parse_date(field); break;
                case 5: orderpriority_code = orderpriority_dict.encode(field); break;
                case 6: clerk = field; break;
                case 7: shippriority = std::stoi(field); break;
                case 8: comment = field; break;
            }
            col++;
        }

        o_orderkey_vec.push_back(orderkey);
        o_custkey_vec.push_back(custkey);
        o_orderstatus_vec.push_back(orderstatus_code);
        o_totalprice_vec.push_back(totalprice);
        o_orderdate_vec.push_back(orderdate);
        o_orderpriority_vec.push_back(orderpriority_code);
        o_clerk_vec.push_back(clerk);
        o_shippriority_vec.push_back(shippriority);
        o_comment_vec.push_back(comment);

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "Ingested " << row_count << " orders rows\n";
        }
    }
    f.close();

    std::cout << "Writing orders binary columns...\n";
    std::ofstream out;
    std::string outdir = gendb_dir + "/orders";
    fs::create_directories(outdir);

    out.open(outdir + "/o_orderkey.bin", std::ios::binary);
    out.write((char*)o_orderkey_vec.data(), o_orderkey_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/o_custkey.bin", std::ios::binary);
    out.write((char*)o_custkey_vec.data(), o_custkey_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/o_orderstatus.bin", std::ios::binary);
    out.write((char*)o_orderstatus_vec.data(), o_orderstatus_vec.size() * sizeof(uint8_t));
    out.close();

    out.open(outdir + "/o_totalprice.bin", std::ios::binary);
    out.write((char*)o_totalprice_vec.data(), o_totalprice_vec.size() * sizeof(int64_t));
    out.close();

    out.open(outdir + "/o_orderdate.bin", std::ios::binary);
    out.write((char*)o_orderdate_vec.data(), o_orderdate_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/o_orderpriority.bin", std::ios::binary);
    out.write((char*)o_orderpriority_vec.data(), o_orderpriority_vec.size() * sizeof(uint16_t));
    out.close();

    out.open(outdir + "/o_clerk.bin", std::ios::binary);
    for (const auto& s : o_clerk_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    out.open(outdir + "/o_shippriority.bin", std::ios::binary);
    out.write((char*)o_shippriority_vec.data(), o_shippriority_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/o_comment.bin", std::ios::binary);
    for (const auto& s : o_comment_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    orderstatus_dict.save_dict(outdir + "/o_orderstatus_dict.txt");
    orderpriority_dict.save_dict(outdir + "/o_orderpriority_dict.txt");

    std::cout << "Orders: wrote " << row_count << " rows\n";
}

// ============================================================================
// CUSTOMER TABLE
// ============================================================================

void ingest_customer(const std::string& data_dir, const std::string& gendb_dir,
                     StringDict& mktsegment_dict) {
    std::string infile = data_dir + "/customer.tbl";
    std::ifstream f(infile);
    if (!f) {
        std::cerr << "Error: cannot open " << infile << std::endl;
        return;
    }

    std::vector<int32_t> c_custkey_vec;
    std::vector<std::string> c_name_vec;
    std::vector<std::string> c_address_vec;
    std::vector<int32_t> c_nationkey_vec;
    std::vector<std::string> c_phone_vec;
    std::vector<int64_t> c_acctbal_vec;
    std::vector<uint16_t> c_mktsegment_vec;
    std::vector<std::string> c_comment_vec;

    std::string line;
    int row_count = 0;
    while (std::getline(f, line)) {
        std::istringstream iss(line);
        std::string field;
        int col = 0;

        int32_t custkey = 0, nationkey = 0;
        int64_t acctbal = 0;
        uint16_t mktsegment_code = 0;
        std::string name, address, phone, comment;

        while (std::getline(iss, field, '|') && col < 8) {
            switch (col) {
                case 0: custkey = std::stoi(field); break;
                case 1: name = field; break;
                case 2: address = field; break;
                case 3: nationkey = std::stoi(field); break;
                case 4: phone = field; break;
                case 5: acctbal = parse_decimal(field, 100); break;
                case 6: mktsegment_code = mktsegment_dict.encode(field); break;
                case 7: comment = field; break;
            }
            col++;
        }

        c_custkey_vec.push_back(custkey);
        c_name_vec.push_back(name);
        c_address_vec.push_back(address);
        c_nationkey_vec.push_back(nationkey);
        c_phone_vec.push_back(phone);
        c_acctbal_vec.push_back(acctbal);
        c_mktsegment_vec.push_back(mktsegment_code);
        c_comment_vec.push_back(comment);

        row_count++;
        if (row_count % 100000 == 0) {
            std::cout << "Ingested " << row_count << " customer rows\n";
        }
    }
    f.close();

    std::cout << "Writing customer binary columns...\n";
    std::ofstream out;
    std::string outdir = gendb_dir + "/customer";
    fs::create_directories(outdir);

    out.open(outdir + "/c_custkey.bin", std::ios::binary);
    out.write((char*)c_custkey_vec.data(), c_custkey_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/c_name.bin", std::ios::binary);
    for (const auto& s : c_name_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    out.open(outdir + "/c_address.bin", std::ios::binary);
    for (const auto& s : c_address_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    out.open(outdir + "/c_nationkey.bin", std::ios::binary);
    out.write((char*)c_nationkey_vec.data(), c_nationkey_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/c_phone.bin", std::ios::binary);
    for (const auto& s : c_phone_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    out.open(outdir + "/c_acctbal.bin", std::ios::binary);
    out.write((char*)c_acctbal_vec.data(), c_acctbal_vec.size() * sizeof(int64_t));
    out.close();

    out.open(outdir + "/c_mktsegment.bin", std::ios::binary);
    out.write((char*)c_mktsegment_vec.data(), c_mktsegment_vec.size() * sizeof(uint16_t));
    out.close();

    out.open(outdir + "/c_comment.bin", std::ios::binary);
    for (const auto& s : c_comment_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    mktsegment_dict.save_dict(outdir + "/c_mktsegment_dict.txt");

    std::cout << "Customer: wrote " << row_count << " rows\n";
}

// ============================================================================
// PART TABLE
// ============================================================================

void ingest_part(const std::string& data_dir, const std::string& gendb_dir,
                 StringDict& container_dict) {
    std::string infile = data_dir + "/part.tbl";
    std::ifstream f(infile);
    if (!f) {
        std::cerr << "Error: cannot open " << infile << std::endl;
        return;
    }

    std::vector<int32_t> p_partkey_vec;
    std::vector<std::string> p_name_vec;
    std::vector<std::string> p_mfgr_vec;
    std::vector<std::string> p_brand_vec;
    std::vector<std::string> p_type_vec;
    std::vector<int32_t> p_size_vec;
    std::vector<uint16_t> p_container_vec;
    std::vector<int64_t> p_retailprice_vec;
    std::vector<std::string> p_comment_vec;

    std::string line;
    int row_count = 0;
    while (std::getline(f, line)) {
        std::istringstream iss(line);
        std::string field;
        int col = 0;

        int32_t partkey = 0, size = 0;
        int64_t retailprice = 0;
        uint16_t container_code = 0;
        std::string name, mfgr, brand, type_str, comment;

        while (std::getline(iss, field, '|') && col < 9) {
            switch (col) {
                case 0: partkey = std::stoi(field); break;
                case 1: name = field; break;
                case 2: mfgr = field; break;
                case 3: brand = field; break;
                case 4: type_str = field; break;
                case 5: size = std::stoi(field); break;
                case 6: container_code = container_dict.encode(field); break;
                case 7: retailprice = parse_decimal(field, 100); break;
                case 8: comment = field; break;
            }
            col++;
        }

        p_partkey_vec.push_back(partkey);
        p_name_vec.push_back(name);
        p_mfgr_vec.push_back(mfgr);
        p_brand_vec.push_back(brand);
        p_type_vec.push_back(type_str);
        p_size_vec.push_back(size);
        p_container_vec.push_back(container_code);
        p_retailprice_vec.push_back(retailprice);
        p_comment_vec.push_back(comment);

        row_count++;
        if (row_count % 100000 == 0) {
            std::cout << "Ingested " << row_count << " part rows\n";
        }
    }
    f.close();

    std::cout << "Writing part binary columns...\n";
    std::ofstream out;
    std::string outdir = gendb_dir + "/part";
    fs::create_directories(outdir);

    out.open(outdir + "/p_partkey.bin", std::ios::binary);
    out.write((char*)p_partkey_vec.data(), p_partkey_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/p_name.bin", std::ios::binary);
    for (const auto& s : p_name_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    out.open(outdir + "/p_mfgr.bin", std::ios::binary);
    for (const auto& s : p_mfgr_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    out.open(outdir + "/p_brand.bin", std::ios::binary);
    for (const auto& s : p_brand_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    out.open(outdir + "/p_type.bin", std::ios::binary);
    for (const auto& s : p_type_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    out.open(outdir + "/p_size.bin", std::ios::binary);
    out.write((char*)p_size_vec.data(), p_size_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/p_container.bin", std::ios::binary);
    out.write((char*)p_container_vec.data(), p_container_vec.size() * sizeof(uint16_t));
    out.close();

    out.open(outdir + "/p_retailprice.bin", std::ios::binary);
    out.write((char*)p_retailprice_vec.data(), p_retailprice_vec.size() * sizeof(int64_t));
    out.close();

    out.open(outdir + "/p_comment.bin", std::ios::binary);
    for (const auto& s : p_comment_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    container_dict.save_dict(outdir + "/p_container_dict.txt");

    std::cout << "Part: wrote " << row_count << " rows\n";
}

// ============================================================================
// PARTSUPP TABLE
// ============================================================================

void ingest_partsupp(const std::string& data_dir, const std::string& gendb_dir) {
    std::string infile = data_dir + "/partsupp.tbl";
    std::ifstream f(infile);
    if (!f) {
        std::cerr << "Error: cannot open " << infile << std::endl;
        return;
    }

    std::vector<int32_t> ps_partkey_vec;
    std::vector<int32_t> ps_suppkey_vec;
    std::vector<int32_t> ps_availqty_vec;
    std::vector<int64_t> ps_supplycost_vec;
    std::vector<std::string> ps_comment_vec;

    std::string line;
    int row_count = 0;
    while (std::getline(f, line)) {
        std::istringstream iss(line);
        std::string field;
        int col = 0;

        int32_t partkey = 0, suppkey = 0, availqty = 0;
        int64_t supplycost = 0;
        std::string comment;

        while (std::getline(iss, field, '|') && col < 5) {
            switch (col) {
                case 0: partkey = std::stoi(field); break;
                case 1: suppkey = std::stoi(field); break;
                case 2: availqty = std::stoi(field); break;
                case 3: supplycost = parse_decimal(field, 100); break;
                case 4: comment = field; break;
            }
            col++;
        }

        ps_partkey_vec.push_back(partkey);
        ps_suppkey_vec.push_back(suppkey);
        ps_availqty_vec.push_back(availqty);
        ps_supplycost_vec.push_back(supplycost);
        ps_comment_vec.push_back(comment);

        row_count++;
        if (row_count % 500000 == 0) {
            std::cout << "Ingested " << row_count << " partsupp rows\n";
        }
    }
    f.close();

    std::cout << "Writing partsupp binary columns...\n";
    std::ofstream out;
    std::string outdir = gendb_dir + "/partsupp";
    fs::create_directories(outdir);

    out.open(outdir + "/ps_partkey.bin", std::ios::binary);
    out.write((char*)ps_partkey_vec.data(), ps_partkey_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/ps_suppkey.bin", std::ios::binary);
    out.write((char*)ps_suppkey_vec.data(), ps_suppkey_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/ps_availqty.bin", std::ios::binary);
    out.write((char*)ps_availqty_vec.data(), ps_availqty_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/ps_supplycost.bin", std::ios::binary);
    out.write((char*)ps_supplycost_vec.data(), ps_supplycost_vec.size() * sizeof(int64_t));
    out.close();

    out.open(outdir + "/ps_comment.bin", std::ios::binary);
    for (const auto& s : ps_comment_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    std::cout << "Partsupp: wrote " << row_count << " rows\n";
}

// ============================================================================
// SUPPLIER TABLE
// ============================================================================

void ingest_supplier(const std::string& data_dir, const std::string& gendb_dir) {
    std::string infile = data_dir + "/supplier.tbl";
    std::ifstream f(infile);
    if (!f) {
        std::cerr << "Error: cannot open " << infile << std::endl;
        return;
    }

    std::vector<int32_t> s_suppkey_vec;
    std::vector<std::string> s_name_vec;
    std::vector<std::string> s_address_vec;
    std::vector<int32_t> s_nationkey_vec;
    std::vector<std::string> s_phone_vec;
    std::vector<int64_t> s_acctbal_vec;
    std::vector<std::string> s_comment_vec;

    std::string line;
    int row_count = 0;
    while (std::getline(f, line)) {
        std::istringstream iss(line);
        std::string field;
        int col = 0;

        int32_t suppkey = 0, nationkey = 0;
        int64_t acctbal = 0;
        std::string name, address, phone, comment;

        while (std::getline(iss, field, '|') && col < 7) {
            switch (col) {
                case 0: suppkey = std::stoi(field); break;
                case 1: name = field; break;
                case 2: address = field; break;
                case 3: nationkey = std::stoi(field); break;
                case 4: phone = field; break;
                case 5: acctbal = parse_decimal(field, 100); break;
                case 6: comment = field; break;
            }
            col++;
        }

        s_suppkey_vec.push_back(suppkey);
        s_name_vec.push_back(name);
        s_address_vec.push_back(address);
        s_nationkey_vec.push_back(nationkey);
        s_phone_vec.push_back(phone);
        s_acctbal_vec.push_back(acctbal);
        s_comment_vec.push_back(comment);

        row_count++;
    }
    f.close();

    std::cout << "Writing supplier binary columns...\n";
    std::ofstream out;
    std::string outdir = gendb_dir + "/supplier";
    fs::create_directories(outdir);

    out.open(outdir + "/s_suppkey.bin", std::ios::binary);
    out.write((char*)s_suppkey_vec.data(), s_suppkey_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/s_name.bin", std::ios::binary);
    for (const auto& s : s_name_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    out.open(outdir + "/s_address.bin", std::ios::binary);
    for (const auto& s : s_address_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    out.open(outdir + "/s_nationkey.bin", std::ios::binary);
    out.write((char*)s_nationkey_vec.data(), s_nationkey_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/s_phone.bin", std::ios::binary);
    for (const auto& s : s_phone_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    out.open(outdir + "/s_acctbal.bin", std::ios::binary);
    out.write((char*)s_acctbal_vec.data(), s_acctbal_vec.size() * sizeof(int64_t));
    out.close();

    out.open(outdir + "/s_comment.bin", std::ios::binary);
    for (const auto& s : s_comment_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    std::cout << "Supplier: wrote " << row_count << " rows\n";
}

// ============================================================================
// NATION TABLE
// ============================================================================

void ingest_nation(const std::string& data_dir, const std::string& gendb_dir) {
    std::string infile = data_dir + "/nation.tbl";
    std::ifstream f(infile);
    if (!f) {
        std::cerr << "Error: cannot open " << infile << std::endl;
        return;
    }

    std::vector<int32_t> n_nationkey_vec;
    std::vector<std::string> n_name_vec;
    std::vector<int32_t> n_regionkey_vec;
    std::vector<std::string> n_comment_vec;

    std::string line;
    int row_count = 0;
    while (std::getline(f, line)) {
        std::istringstream iss(line);
        std::string field;
        int col = 0;

        int32_t nationkey = 0, regionkey = 0;
        std::string name, comment;

        while (std::getline(iss, field, '|') && col < 4) {
            switch (col) {
                case 0: nationkey = std::stoi(field); break;
                case 1: name = field; break;
                case 2: regionkey = std::stoi(field); break;
                case 3: comment = field; break;
            }
            col++;
        }

        n_nationkey_vec.push_back(nationkey);
        n_name_vec.push_back(name);
        n_regionkey_vec.push_back(regionkey);
        n_comment_vec.push_back(comment);
        row_count++;
    }
    f.close();

    std::cout << "Writing nation binary columns...\n";
    std::ofstream out;
    std::string outdir = gendb_dir + "/nation";
    fs::create_directories(outdir);

    out.open(outdir + "/n_nationkey.bin", std::ios::binary);
    out.write((char*)n_nationkey_vec.data(), n_nationkey_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/n_name.bin", std::ios::binary);
    for (const auto& s : n_name_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    out.open(outdir + "/n_regionkey.bin", std::ios::binary);
    out.write((char*)n_regionkey_vec.data(), n_regionkey_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/n_comment.bin", std::ios::binary);
    for (const auto& s : n_comment_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    std::cout << "Nation: wrote " << row_count << " rows\n";
}

// ============================================================================
// REGION TABLE
// ============================================================================

void ingest_region(const std::string& data_dir, const std::string& gendb_dir) {
    std::string infile = data_dir + "/region.tbl";
    std::ifstream f(infile);
    if (!f) {
        std::cerr << "Error: cannot open " << infile << std::endl;
        return;
    }

    std::vector<int32_t> r_regionkey_vec;
    std::vector<std::string> r_name_vec;
    std::vector<std::string> r_comment_vec;

    std::string line;
    int row_count = 0;
    while (std::getline(f, line)) {
        std::istringstream iss(line);
        std::string field;
        int col = 0;

        int32_t regionkey = 0;
        std::string name, comment;

        while (std::getline(iss, field, '|') && col < 3) {
            switch (col) {
                case 0: regionkey = std::stoi(field); break;
                case 1: name = field; break;
                case 2: comment = field; break;
            }
            col++;
        }

        r_regionkey_vec.push_back(regionkey);
        r_name_vec.push_back(name);
        r_comment_vec.push_back(comment);
        row_count++;
    }
    f.close();

    std::cout << "Writing region binary columns...\n";
    std::ofstream out;
    std::string outdir = gendb_dir + "/region";
    fs::create_directories(outdir);

    out.open(outdir + "/r_regionkey.bin", std::ios::binary);
    out.write((char*)r_regionkey_vec.data(), r_regionkey_vec.size() * sizeof(int32_t));
    out.close();

    out.open(outdir + "/r_name.bin", std::ios::binary);
    for (const auto& s : r_name_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    out.open(outdir + "/r_comment.bin", std::ios::binary);
    for (const auto& s : r_comment_vec) {
        uint32_t len = s.length();
        out.write((char*)&len, sizeof(uint32_t));
        out.write(s.c_str(), len);
    }
    out.close();

    std::cout << "Region: wrote " << row_count << " rows\n";
}

// ============================================================================
// MAIN
// ============================================================================

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>\n";
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    auto start = std::chrono::high_resolution_clock::now();

    std::cout << "Starting TPC-H ingestion...\n";
    std::cout << "Data dir: " << data_dir << "\n";
    std::cout << "GenDB dir: " << gendb_dir << "\n";

    fs::create_directories(gendb_dir);

    // Dictionary objects for low-cardinality columns
    CharDict returnflag_dict, linestatus_dict, orderstatus_dict;
    StringDict shipinstruct_dict, shipmode_dict, orderpriority_dict, mktsegment_dict, container_dict;

    // Ingest all tables
    ingest_lineitem(data_dir, gendb_dir, returnflag_dict, linestatus_dict, shipinstruct_dict, shipmode_dict);
    ingest_orders(data_dir, gendb_dir, orderstatus_dict, orderpriority_dict);
    ingest_customer(data_dir, gendb_dir, mktsegment_dict);
    ingest_part(data_dir, gendb_dir, container_dict);
    ingest_partsupp(data_dir, gendb_dir);
    ingest_supplier(data_dir, gendb_dir);
    ingest_nation(data_dir, gendb_dir);
    ingest_region(data_dir, gendb_dir);

    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();

    std::cout << "\nIngestion complete in " << elapsed << " seconds\n";

    return 0;
}
