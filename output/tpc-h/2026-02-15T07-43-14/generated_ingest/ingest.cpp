#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <charconv>
#include <cmath>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <ctime>

namespace fs = std::filesystem;

// ============================================================================
// Date parsing: YYYY-MM-DD -> days since 1970-01-01
// ============================================================================
int32_t parse_date(const std::string& s) {
    if (s.empty() || s.length() < 10) return 0;
    int year = std::stoi(s.substr(0, 4));
    int month = std::stoi(s.substr(5, 2));
    int day = std::stoi(s.substr(8, 2));

    // Days since epoch (1970-01-01)
    int days = 0;
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) month_days[2] = 29;
    for (int m = 1; m < month; ++m) days += month_days[m];
    days += day - 1;
    return days;
}

// Parse decimal: DECIMAL(15,2) as int64_t with scale_factor=100
int64_t parse_decimal(const std::string& s, int32_t scale_factor) {
    double val = 0.0;
    auto [ptr, ec] = std::from_chars(s.c_str(), s.c_str() + s.length(), val);
    if (ec != std::errc()) val = 0.0;
    return static_cast<int64_t>(std::round(val * scale_factor));
}

// ============================================================================
// Dictionary encoding for low-cardinality columns
// ============================================================================
class Dictionary {
public:
    uint8_t encode(const std::string& val) {
        auto it = value_to_code.find(val);
        if (it != value_to_code.end()) return it->second;
        uint8_t code = next_code++;
        value_to_code[val] = code;
        code_to_value[code] = val;
        return code;
    }

    void write_to_file(const std::string& path) {
        std::ofstream f(path);
        for (const auto& [code, val] : code_to_value) {
            f << static_cast<int>(code) << "=" << val << "\n";
        }
    }

private:
    std::unordered_map<std::string, uint8_t> value_to_code;
    std::unordered_map<uint8_t, std::string> code_to_value;
    uint8_t next_code = 0;
};

// ============================================================================
// Row structures for each table (SoA pattern will be used)
// ============================================================================
struct LineitemRow {
    int32_t l_orderkey, l_partkey, l_suppkey, l_linenumber;
    int64_t l_quantity, l_extendedprice, l_discount, l_tax;
    uint8_t l_returnflag, l_linestatus, l_shipinstruct, l_shipmode;
    int32_t l_shipdate, l_commitdate, l_receiptdate;
    std::string l_comment;
};

struct OrderRow {
    int32_t o_orderkey, o_custkey, o_shippriority;
    uint8_t o_orderstatus, o_orderpriority;
    int64_t o_totalprice;
    int32_t o_orderdate;
    std::string o_clerk, o_comment;
};

struct CustomerRow {
    int32_t c_custkey, c_nationkey;
    uint8_t c_mktsegment;
    int64_t c_acctbal;
    std::string c_name, c_address, c_phone, c_comment;
};

struct PartRow {
    int32_t p_partkey, p_size;
    uint8_t p_brand;  // Will only use p_brand for now
    int64_t p_retailprice;
    std::string p_name, p_mfgr, p_type, p_container, p_comment;
};

struct PartSuppRow {
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
// Write binary columnar data
// ============================================================================
template<typename T>
void write_column(const std::string& path, const std::vector<T>& col) {
    std::ofstream f(path, std::ios::binary);
    f.write(reinterpret_cast<const char*>(col.data()), col.size() * sizeof(T));
}

void write_string_column(const std::string& path, const std::vector<std::string>& col) {
    std::ofstream f(path, std::ios::binary);
    uint32_t count = col.size();
    f.write(reinterpret_cast<const char*>(&count), sizeof(count));
    std::vector<uint32_t> offsets;
    offsets.push_back(0);
    for (size_t i = 0; i < col.size(); ++i) {
        offsets.push_back(offsets.back() + col[i].length());
    }
    for (uint32_t off : offsets) {
        f.write(reinterpret_cast<const char*>(&off), sizeof(off));
    }
    for (const auto& s : col) {
        f.write(s.c_str(), s.length());
    }
}

// ============================================================================
// Parse and ingest tables with parallel processing
// ============================================================================
void ingest_lineitem(const std::string& data_dir, const std::string& gendb_dir) {
    std::string path = data_dir + "/lineitem.tbl";
    std::ifstream f(path);
    if (!f) { std::cerr << "Failed to open lineitem.tbl\n"; return; }

    std::vector<int32_t> l_orderkey, l_partkey, l_suppkey, l_linenumber;
    std::vector<int64_t> l_quantity, l_extendedprice, l_discount, l_tax;
    std::vector<uint8_t> l_returnflag, l_linestatus, l_shipinstruct, l_shipmode;
    std::vector<int32_t> l_shipdate, l_commitdate, l_receiptdate;
    std::vector<std::string> l_comment;

    Dictionary dict_returnflag, dict_linestatus, dict_shipinstruct, dict_shipmode;

    std::string line;
    size_t row_count = 0;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        LineitemRow row;

        // Parse: l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|
        //        l_discount|l_tax|l_returnflag|l_linestatus|l_shipdate|l_commitdate|
        //        l_receiptdate|l_shipinstruct|l_shipmode|l_comment
        size_t pos = 0, next;
        std::vector<std::string> fields;
        while ((next = line.find('|', pos)) != std::string::npos) {
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }
        fields.push_back(line.substr(pos));

        if (fields.size() < 16) continue;

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
        row.l_comment = (fields.size() > 15) ? fields[15] : "";

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

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "Ingested " << row_count << " lineitem rows\n";
        }
    }
    f.close();

    // Create output directory
    fs::create_directories(gendb_dir + "/lineitem");

    // Sort by l_shipdate
    std::vector<size_t> perm(l_orderkey.size());
    for (size_t i = 0; i < perm.size(); ++i) perm[i] = i;
    std::sort(perm.begin(), perm.end(), [&](size_t i, size_t j) {
        return l_shipdate[i] < l_shipdate[j];
    });

    // Apply permutation and write columns
    auto write_permuted = [&](const auto& col, const std::string& name) {
        std::vector<std::decay_t<decltype(col[0])>> sorted(col.size());
        for (size_t i = 0; i < col.size(); ++i) sorted[i] = col[perm[i]];
        write_column(gendb_dir + "/lineitem/" + name + ".bin", sorted);
    };

    write_permuted(l_orderkey, "l_orderkey");
    write_permuted(l_partkey, "l_partkey");
    write_permuted(l_suppkey, "l_suppkey");
    write_permuted(l_linenumber, "l_linenumber");
    write_permuted(l_quantity, "l_quantity");
    write_permuted(l_extendedprice, "l_extendedprice");
    write_permuted(l_discount, "l_discount");
    write_permuted(l_tax, "l_tax");
    write_permuted(l_returnflag, "l_returnflag");
    write_permuted(l_linestatus, "l_linestatus");
    write_permuted(l_shipdate, "l_shipdate");
    write_permuted(l_commitdate, "l_commitdate");
    write_permuted(l_receiptdate, "l_receiptdate");
    write_permuted(l_shipinstruct, "l_shipinstruct");
    write_permuted(l_shipmode, "l_shipmode");

    // Strings need special handling
    std::vector<std::string> sorted_comment(l_comment.size());
    for (size_t i = 0; i < l_comment.size(); ++i) sorted_comment[i] = l_comment[perm[i]];
    write_string_column(gendb_dir + "/lineitem/l_comment.bin", sorted_comment);

    // Write dictionaries
    dict_returnflag.write_to_file(gendb_dir + "/lineitem/l_returnflag_dict.txt");
    dict_linestatus.write_to_file(gendb_dir + "/lineitem/l_linestatus_dict.txt");
    dict_shipinstruct.write_to_file(gendb_dir + "/lineitem/l_shipinstruct_dict.txt");
    dict_shipmode.write_to_file(gendb_dir + "/lineitem/l_shipmode_dict.txt");

    std::cout << "Ingested lineitem: " << row_count << " rows\n";
}

void ingest_orders(const std::string& data_dir, const std::string& gendb_dir) {
    std::string path = data_dir + "/orders.tbl";
    std::ifstream f(path);
    if (!f) { std::cerr << "Failed to open orders.tbl\n"; return; }

    std::vector<int32_t> o_orderkey, o_custkey, o_shippriority, o_orderdate;
    std::vector<uint8_t> o_orderstatus, o_orderpriority;
    std::vector<int64_t> o_totalprice;
    std::vector<std::string> o_clerk, o_comment;

    Dictionary dict_orderstatus, dict_orderpriority;

    std::string line;
    size_t row_count = 0;
    while (std::getline(f, line)) {
        if (line.empty()) continue;

        size_t pos = 0, next;
        std::vector<std::string> fields;
        while ((next = line.find('|', pos)) != std::string::npos) {
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }
        fields.push_back(line.substr(pos));

        if (fields.size() < 9) continue;

        o_orderkey.push_back(std::stoi(fields[0]));
        o_custkey.push_back(std::stoi(fields[1]));
        o_orderstatus.push_back(dict_orderstatus.encode(fields[2]));
        o_totalprice.push_back(parse_decimal(fields[3], 100));
        o_orderdate.push_back(parse_date(fields[4]));
        o_orderpriority.push_back(dict_orderpriority.encode(fields[5]));
        o_clerk.push_back(fields[6]);
        o_shippriority.push_back(std::stoi(fields[7]));
        o_comment.push_back(fields[8]);

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "Ingested " << row_count << " orders rows\n";
        }
    }
    f.close();

    fs::create_directories(gendb_dir + "/orders");

    // Sort by o_orderkey
    std::vector<size_t> perm(o_orderkey.size());
    for (size_t i = 0; i < perm.size(); ++i) perm[i] = i;
    std::sort(perm.begin(), perm.end(), [&](size_t i, size_t j) {
        return o_orderkey[i] < o_orderkey[j];
    });

    auto write_permuted = [&](const auto& col, const std::string& name) {
        std::vector<std::decay_t<decltype(col[0])>> sorted(col.size());
        for (size_t i = 0; i < col.size(); ++i) sorted[i] = col[perm[i]];
        write_column(gendb_dir + "/orders/" + name + ".bin", sorted);
    };

    write_permuted(o_orderkey, "o_orderkey");
    write_permuted(o_custkey, "o_custkey");
    write_permuted(o_orderstatus, "o_orderstatus");
    write_permuted(o_totalprice, "o_totalprice");
    write_permuted(o_orderdate, "o_orderdate");
    write_permuted(o_orderpriority, "o_orderpriority");
    write_permuted(o_shippriority, "o_shippriority");

    std::vector<std::string> sorted_clerk(o_clerk.size()), sorted_comment(o_comment.size());
    for (size_t i = 0; i < o_clerk.size(); ++i) {
        sorted_clerk[i] = o_clerk[perm[i]];
        sorted_comment[i] = o_comment[perm[i]];
    }
    write_string_column(gendb_dir + "/orders/o_clerk.bin", sorted_clerk);
    write_string_column(gendb_dir + "/orders/o_comment.bin", sorted_comment);

    dict_orderstatus.write_to_file(gendb_dir + "/orders/o_orderstatus_dict.txt");
    dict_orderpriority.write_to_file(gendb_dir + "/orders/o_orderpriority_dict.txt");

    std::cout << "Ingested orders: " << row_count << " rows\n";
}

void ingest_customer(const std::string& data_dir, const std::string& gendb_dir) {
    std::string path = data_dir + "/customer.tbl";
    std::ifstream f(path);
    if (!f) { std::cerr << "Failed to open customer.tbl\n"; return; }

    std::vector<int32_t> c_custkey, c_nationkey;
    std::vector<uint8_t> c_mktsegment;
    std::vector<int64_t> c_acctbal;
    std::vector<std::string> c_name, c_address, c_phone, c_comment;

    Dictionary dict_mktsegment;

    std::string line;
    size_t row_count = 0;
    while (std::getline(f, line)) {
        if (line.empty()) continue;

        size_t pos = 0, next;
        std::vector<std::string> fields;
        while ((next = line.find('|', pos)) != std::string::npos) {
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }
        fields.push_back(line.substr(pos));

        if (fields.size() < 8) continue;

        c_custkey.push_back(std::stoi(fields[0]));
        c_name.push_back(fields[1]);
        c_address.push_back(fields[2]);
        c_nationkey.push_back(std::stoi(fields[3]));
        c_phone.push_back(fields[4]);
        c_acctbal.push_back(parse_decimal(fields[5], 100));
        c_mktsegment.push_back(dict_mktsegment.encode(fields[6]));
        c_comment.push_back(fields[7]);

        row_count++;
        if (row_count % 100000 == 0) {
            std::cout << "Ingested " << row_count << " customer rows\n";
        }
    }
    f.close();

    fs::create_directories(gendb_dir + "/customer");

    // Sort by c_custkey
    std::vector<size_t> perm(c_custkey.size());
    for (size_t i = 0; i < perm.size(); ++i) perm[i] = i;
    std::sort(perm.begin(), perm.end(), [&](size_t i, size_t j) {
        return c_custkey[i] < c_custkey[j];
    });

    auto write_permuted = [&](const auto& col, const std::string& name) {
        std::vector<std::decay_t<decltype(col[0])>> sorted(col.size());
        for (size_t i = 0; i < col.size(); ++i) sorted[i] = col[perm[i]];
        write_column(gendb_dir + "/customer/" + name + ".bin", sorted);
    };

    write_permuted(c_custkey, "c_custkey");
    write_permuted(c_nationkey, "c_nationkey");
    write_permuted(c_mktsegment, "c_mktsegment");
    write_permuted(c_acctbal, "c_acctbal");

    std::vector<std::string> sorted_name(c_name.size()), sorted_address(c_address.size());
    std::vector<std::string> sorted_phone(c_phone.size()), sorted_comment(c_comment.size());
    for (size_t i = 0; i < c_custkey.size(); ++i) {
        sorted_name[i] = c_name[perm[i]];
        sorted_address[i] = c_address[perm[i]];
        sorted_phone[i] = c_phone[perm[i]];
        sorted_comment[i] = c_comment[perm[i]];
    }
    write_string_column(gendb_dir + "/customer/c_name.bin", sorted_name);
    write_string_column(gendb_dir + "/customer/c_address.bin", sorted_address);
    write_string_column(gendb_dir + "/customer/c_phone.bin", sorted_phone);
    write_string_column(gendb_dir + "/customer/c_comment.bin", sorted_comment);

    dict_mktsegment.write_to_file(gendb_dir + "/customer/c_mktsegment_dict.txt");

    std::cout << "Ingested customer: " << row_count << " rows\n";
}

void ingest_part(const std::string& data_dir, const std::string& gendb_dir) {
    std::string path = data_dir + "/part.tbl";
    std::ifstream f(path);
    if (!f) { std::cerr << "Failed to open part.tbl\n"; return; }

    std::vector<int32_t> p_partkey, p_size;
    std::vector<int64_t> p_retailprice;
    std::vector<std::string> p_name, p_mfgr, p_brand, p_type, p_container, p_comment;

    std::string line;
    size_t row_count = 0;
    while (std::getline(f, line)) {
        if (line.empty()) continue;

        size_t pos = 0, next;
        std::vector<std::string> fields;
        while ((next = line.find('|', pos)) != std::string::npos) {
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }
        fields.push_back(line.substr(pos));

        if (fields.size() < 9) continue;

        p_partkey.push_back(std::stoi(fields[0]));
        p_name.push_back(fields[1]);
        p_mfgr.push_back(fields[2]);
        p_brand.push_back(fields[3]);
        p_type.push_back(fields[4]);
        p_size.push_back(std::stoi(fields[5]));
        p_container.push_back(fields[6]);
        p_retailprice.push_back(parse_decimal(fields[7], 100));
        p_comment.push_back(fields[8]);

        row_count++;
        if (row_count % 100000 == 0) {
            std::cout << "Ingested " << row_count << " part rows\n";
        }
    }
    f.close();

    fs::create_directories(gendb_dir + "/part");

    std::vector<size_t> perm(p_partkey.size());
    for (size_t i = 0; i < perm.size(); ++i) perm[i] = i;
    std::sort(perm.begin(), perm.end(), [&](size_t i, size_t j) {
        return p_partkey[i] < p_partkey[j];
    });

    auto write_permuted = [&](const auto& col, const std::string& name) {
        std::vector<std::decay_t<decltype(col[0])>> sorted(col.size());
        for (size_t i = 0; i < col.size(); ++i) sorted[i] = col[perm[i]];
        write_column(gendb_dir + "/part/" + name + ".bin", sorted);
    };

    write_permuted(p_partkey, "p_partkey");
    write_permuted(p_size, "p_size");
    write_permuted(p_retailprice, "p_retailprice");

    std::vector<std::string> sorted_name(p_name.size()), sorted_mfgr(p_mfgr.size());
    std::vector<std::string> sorted_brand(p_brand.size()), sorted_type(p_type.size());
    std::vector<std::string> sorted_container(p_container.size()), sorted_comment(p_comment.size());
    for (size_t i = 0; i < p_partkey.size(); ++i) {
        sorted_name[i] = p_name[perm[i]];
        sorted_mfgr[i] = p_mfgr[perm[i]];
        sorted_brand[i] = p_brand[perm[i]];
        sorted_type[i] = p_type[perm[i]];
        sorted_container[i] = p_container[perm[i]];
        sorted_comment[i] = p_comment[perm[i]];
    }
    write_string_column(gendb_dir + "/part/p_name.bin", sorted_name);
    write_string_column(gendb_dir + "/part/p_mfgr.bin", sorted_mfgr);
    write_string_column(gendb_dir + "/part/p_brand.bin", sorted_brand);
    write_string_column(gendb_dir + "/part/p_type.bin", sorted_type);
    write_string_column(gendb_dir + "/part/p_container.bin", sorted_container);
    write_string_column(gendb_dir + "/part/p_comment.bin", sorted_comment);

    std::cout << "Ingested part: " << row_count << " rows\n";
}

void ingest_partsupp(const std::string& data_dir, const std::string& gendb_dir) {
    std::string path = data_dir + "/partsupp.tbl";
    std::ifstream f(path);
    if (!f) { std::cerr << "Failed to open partsupp.tbl\n"; return; }

    std::vector<int32_t> ps_partkey, ps_suppkey, ps_availqty;
    std::vector<int64_t> ps_supplycost;
    std::vector<std::string> ps_comment;

    std::string line;
    size_t row_count = 0;
    while (std::getline(f, line)) {
        if (line.empty()) continue;

        size_t pos = 0, next;
        std::vector<std::string> fields;
        while ((next = line.find('|', pos)) != std::string::npos) {
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }
        fields.push_back(line.substr(pos));

        if (fields.size() < 5) continue;

        ps_partkey.push_back(std::stoi(fields[0]));
        ps_suppkey.push_back(std::stoi(fields[1]));
        ps_availqty.push_back(std::stoi(fields[2]));
        ps_supplycost.push_back(parse_decimal(fields[3], 100));
        ps_comment.push_back(fields[4]);

        row_count++;
        if (row_count % 500000 == 0) {
            std::cout << "Ingested " << row_count << " partsupp rows\n";
        }
    }
    f.close();

    fs::create_directories(gendb_dir + "/partsupp");

    std::vector<size_t> perm(ps_partkey.size());
    for (size_t i = 0; i < perm.size(); ++i) perm[i] = i;
    std::sort(perm.begin(), perm.end(), [&](size_t i, size_t j) {
        if (ps_partkey[i] != ps_partkey[j]) return ps_partkey[i] < ps_partkey[j];
        return ps_suppkey[i] < ps_suppkey[j];
    });

    auto write_permuted = [&](const auto& col, const std::string& name) {
        std::vector<std::decay_t<decltype(col[0])>> sorted(col.size());
        for (size_t i = 0; i < col.size(); ++i) sorted[i] = col[perm[i]];
        write_column(gendb_dir + "/partsupp/" + name + ".bin", sorted);
    };

    write_permuted(ps_partkey, "ps_partkey");
    write_permuted(ps_suppkey, "ps_suppkey");
    write_permuted(ps_availqty, "ps_availqty");
    write_permuted(ps_supplycost, "ps_supplycost");

    std::vector<std::string> sorted_comment(ps_comment.size());
    for (size_t i = 0; i < ps_comment.size(); ++i) sorted_comment[i] = ps_comment[perm[i]];
    write_string_column(gendb_dir + "/partsupp/ps_comment.bin", sorted_comment);

    std::cout << "Ingested partsupp: " << row_count << " rows\n";
}

void ingest_supplier(const std::string& data_dir, const std::string& gendb_dir) {
    std::string path = data_dir + "/supplier.tbl";
    std::ifstream f(path);
    if (!f) { std::cerr << "Failed to open supplier.tbl\n"; return; }

    std::vector<int32_t> s_suppkey, s_nationkey;
    std::vector<int64_t> s_acctbal;
    std::vector<std::string> s_name, s_address, s_phone, s_comment;

    std::string line;
    size_t row_count = 0;
    while (std::getline(f, line)) {
        if (line.empty()) continue;

        size_t pos = 0, next;
        std::vector<std::string> fields;
        while ((next = line.find('|', pos)) != std::string::npos) {
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }
        fields.push_back(line.substr(pos));

        if (fields.size() < 7) continue;

        s_suppkey.push_back(std::stoi(fields[0]));
        s_name.push_back(fields[1]);
        s_address.push_back(fields[2]);
        s_nationkey.push_back(std::stoi(fields[3]));
        s_phone.push_back(fields[4]);
        s_acctbal.push_back(parse_decimal(fields[5], 100));
        s_comment.push_back(fields[6]);

        row_count++;
    }
    f.close();

    fs::create_directories(gendb_dir + "/supplier");

    std::vector<size_t> perm(s_suppkey.size());
    for (size_t i = 0; i < perm.size(); ++i) perm[i] = i;
    std::sort(perm.begin(), perm.end(), [&](size_t i, size_t j) {
        return s_suppkey[i] < s_suppkey[j];
    });

    auto write_permuted = [&](const auto& col, const std::string& name) {
        std::vector<std::decay_t<decltype(col[0])>> sorted(col.size());
        for (size_t i = 0; i < col.size(); ++i) sorted[i] = col[perm[i]];
        write_column(gendb_dir + "/supplier/" + name + ".bin", sorted);
    };

    write_permuted(s_suppkey, "s_suppkey");
    write_permuted(s_nationkey, "s_nationkey");
    write_permuted(s_acctbal, "s_acctbal");

    std::vector<std::string> sorted_name(s_name.size()), sorted_address(s_address.size());
    std::vector<std::string> sorted_phone(s_phone.size()), sorted_comment(s_comment.size());
    for (size_t i = 0; i < s_suppkey.size(); ++i) {
        sorted_name[i] = s_name[perm[i]];
        sorted_address[i] = s_address[perm[i]];
        sorted_phone[i] = s_phone[perm[i]];
        sorted_comment[i] = s_comment[perm[i]];
    }
    write_string_column(gendb_dir + "/supplier/s_name.bin", sorted_name);
    write_string_column(gendb_dir + "/supplier/s_address.bin", sorted_address);
    write_string_column(gendb_dir + "/supplier/s_phone.bin", sorted_phone);
    write_string_column(gendb_dir + "/supplier/s_comment.bin", sorted_comment);

    std::cout << "Ingested supplier: " << row_count << " rows\n";
}

void ingest_nation(const std::string& data_dir, const std::string& gendb_dir) {
    std::string path = data_dir + "/nation.tbl";
    std::ifstream f(path);
    if (!f) { std::cerr << "Failed to open nation.tbl\n"; return; }

    std::vector<int32_t> n_nationkey, n_regionkey;
    std::vector<std::string> n_name, n_comment;

    std::string line;
    size_t row_count = 0;
    while (std::getline(f, line)) {
        if (line.empty()) continue;

        size_t pos = 0, next;
        std::vector<std::string> fields;
        while ((next = line.find('|', pos)) != std::string::npos) {
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }
        fields.push_back(line.substr(pos));

        if (fields.size() < 4) continue;

        n_nationkey.push_back(std::stoi(fields[0]));
        n_name.push_back(fields[1]);
        n_regionkey.push_back(std::stoi(fields[2]));
        n_comment.push_back(fields[3]);

        row_count++;
    }
    f.close();

    fs::create_directories(gendb_dir + "/nation");
    write_column(gendb_dir + "/nation/n_nationkey.bin", n_nationkey);
    write_column(gendb_dir + "/nation/n_regionkey.bin", n_regionkey);
    write_string_column(gendb_dir + "/nation/n_name.bin", n_name);
    write_string_column(gendb_dir + "/nation/n_comment.bin", n_comment);

    std::cout << "Ingested nation: " << row_count << " rows\n";
}

void ingest_region(const std::string& data_dir, const std::string& gendb_dir) {
    std::string path = data_dir + "/region.tbl";
    std::ifstream f(path);
    if (!f) { std::cerr << "Failed to open region.tbl\n"; return; }

    std::vector<int32_t> r_regionkey;
    std::vector<std::string> r_name, r_comment;

    std::string line;
    size_t row_count = 0;
    while (std::getline(f, line)) {
        if (line.empty()) continue;

        size_t pos = 0, next;
        std::vector<std::string> fields;
        while ((next = line.find('|', pos)) != std::string::npos) {
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }
        fields.push_back(line.substr(pos));

        if (fields.size() < 3) continue;

        r_regionkey.push_back(std::stoi(fields[0]));
        r_name.push_back(fields[1]);
        r_comment.push_back(fields[2]);

        row_count++;
    }
    f.close();

    fs::create_directories(gendb_dir + "/region");
    write_column(gendb_dir + "/region/r_regionkey.bin", r_regionkey);
    write_string_column(gendb_dir + "/region/r_name.bin", r_name);
    write_string_column(gendb_dir + "/region/r_comment.bin", r_comment);

    std::cout << "Ingested region: " << row_count << " rows\n";
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: ./ingest <data_dir> <gendb_dir>\n";
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    auto start = std::chrono::high_resolution_clock::now();

    std::cout << "Ingesting TPC-H data from " << data_dir << " to " << gendb_dir << "\n";

    fs::create_directories(gendb_dir);

    // Ingest tables in parallel
    std::thread t1([&]() { ingest_lineitem(data_dir, gendb_dir); });
    std::thread t2([&]() { ingest_orders(data_dir, gendb_dir); });
    std::thread t3([&]() { ingest_customer(data_dir, gendb_dir); });
    std::thread t4([&]() { ingest_part(data_dir, gendb_dir); });
    std::thread t5([&]() { ingest_partsupp(data_dir, gendb_dir); });
    std::thread t6([&]() { ingest_supplier(data_dir, gendb_dir); });
    std::thread t7([&]() { ingest_nation(data_dir, gendb_dir); });
    std::thread t8([&]() { ingest_region(data_dir, gendb_dir); });

    t1.join(); t2.join(); t3.join(); t4.join();
    t5.join(); t6.join(); t7.join(); t8.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);

    std::cout << "Ingestion complete in " << duration.count() << " seconds\n";
    return 0;
}
