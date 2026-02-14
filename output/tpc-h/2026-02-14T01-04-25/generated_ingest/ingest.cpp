#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>
#include <thread>
#include <mutex>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <charconv>
#include <cmath>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <filesystem>

namespace fs = std::filesystem;
using namespace std::chrono;

// Constants
const int BLOCK_SIZE = 131072;  // 128K rows per block
const int BUFFER_SIZE = 1024 * 1024;  // 1MB write buffer
const int THREAD_COUNT = std::thread::hardware_concurrency();

// Date encoding: days since 1970-01-01
int32_t parse_date(const std::string& date_str) {
    // Format: YYYY-MM-DD
    if (date_str.length() != 10) return 0;
    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Days since epoch (1970-01-01)
    int days = 0;

    // Add days for full years
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Add days for full months in current year
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[1] = 29;
    }

    for (int m = 1; m < month; ++m) {
        days += days_in_month[m - 1];
    }

    days += day - 1;  // Day is 1-indexed
    return days;
}

// Dictionary for strings
class StringDictionary {
public:
    uint8_t add(const std::string& val) {
        auto it = dict.find(val);
        if (it != dict.end()) {
            return it->second;
        }
        uint8_t code = next_code++;
        dict[val] = code;
        reverse_dict[code] = val;
        return code;
    }

    const std::unordered_map<std::string, uint8_t>& get_dict() const {
        return dict;
    }

    const std::unordered_map<uint8_t, std::string>& get_reverse() const {
        return reverse_dict;
    }

private:
    std::unordered_map<std::string, uint8_t> dict;
    std::unordered_map<uint8_t, std::string> reverse_dict;
    uint8_t next_code = 0;
};

// Parse a delimited line into fields
std::vector<std::string> split_line(const char* line, size_t len, char delim) {
    std::vector<std::string> fields;
    size_t start = 0;
    for (size_t i = 0; i < len; ++i) {
        if (line[i] == delim) {
            fields.push_back(std::string(line + start, i - start));
            start = i + 1;
        }
    }
    // Last field (or only field)
    if (start < len) {
        fields.push_back(std::string(line + start, len - start));
    }
    return fields;
}

// Find next newline in mmap'd buffer
const char* find_newline(const char* ptr, const char* end) {
    while (ptr < end && *ptr != '\n') {
        ++ptr;
    }
    return ptr;
}

// Buffered binary writer
class BinaryWriter {
public:
    BinaryWriter(const std::string& path) : path(path) {}

    template <typename T>
    void write(const T& val) {
        if (buffer.size() + sizeof(T) > BUFFER_SIZE) {
            flush();
        }
        const char* ptr = reinterpret_cast<const char*>(&val);
        buffer.insert(buffer.end(), ptr, ptr + sizeof(T));
    }

    void write_string(const std::string& str) {
        // Write length + data
        uint32_t len = str.length();
        write(len);
        if (buffer.size() + len > BUFFER_SIZE) {
            flush();
        }
        buffer.insert(buffer.end(), str.begin(), str.end());
    }

    void flush() {
        if (buffer.empty()) return;
        std::ofstream file(path, std::ios::binary | std::ios::app);
        if (!file) {
            throw std::runtime_error("Cannot open file: " + path);
        }
        file.write(buffer.data(), buffer.size());
        buffer.clear();
    }

    ~BinaryWriter() {
        flush();
    }

private:
    std::string path;
    std::vector<char> buffer;
};

// Lineitem ingestion
struct LineitemData {
    std::vector<int32_t> l_orderkey;
    std::vector<int32_t> l_partkey;
    std::vector<int32_t> l_suppkey;
    std::vector<int32_t> l_linenumber;
    std::vector<int32_t> l_quantity;
    std::vector<double> l_extendedprice;
    std::vector<double> l_discount;
    std::vector<double> l_tax;
    std::vector<uint8_t> l_returnflag;
    std::vector<uint8_t> l_linestatus;
    std::vector<int32_t> l_shipdate;
    std::vector<int32_t> l_commitdate;
    std::vector<int32_t> l_receiptdate;
    std::vector<uint8_t> l_shipinstruct;
    std::vector<uint8_t> l_shipmode;
    std::vector<std::string> l_comment;
    StringDictionary returnflag_dict;
    StringDictionary linestatus_dict;
    StringDictionary shipinstruct_dict;
    StringDictionary shipmode_dict;
};

void ingest_lineitem(const std::string& data_dir, const std::string& gendb_dir) {
    std::string filepath = data_dir + "/lineitem.tbl";

    // Open mmap
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Cannot open " + filepath);
    }

    struct stat st;
    fstat(fd, &st);
    size_t file_size = st.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        throw std::runtime_error("mmap failed");
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Parse data (single-threaded for now, can be parallelized)
    LineitemData parsed;
    const char* ptr = data;
    const char* end = data + file_size;
    int row_count = 0;

    while (ptr < end) {
        const char* line_end = find_newline(ptr, end);
        if (line_end == ptr) break;

        std::vector<std::string> fields = split_line(ptr, line_end - ptr, '|');

        if (fields.size() >= 16) {
            parsed.l_orderkey.push_back(std::stoi(fields[0]));
            parsed.l_partkey.push_back(std::stoi(fields[1]));
            parsed.l_suppkey.push_back(std::stoi(fields[2]));
            parsed.l_linenumber.push_back(std::stoi(fields[3]));
            parsed.l_quantity.push_back(std::stoi(fields[4]));
            parsed.l_extendedprice.push_back(std::stod(fields[5]));
            parsed.l_discount.push_back(std::stod(fields[6]));
            parsed.l_tax.push_back(std::stod(fields[7]));
            parsed.l_returnflag.push_back(parsed.returnflag_dict.add(fields[8]));
            parsed.l_linestatus.push_back(parsed.linestatus_dict.add(fields[9]));
            parsed.l_shipdate.push_back(parse_date(fields[10]));
            parsed.l_commitdate.push_back(parse_date(fields[11]));
            parsed.l_receiptdate.push_back(parse_date(fields[12]));
            parsed.l_shipinstruct.push_back(parsed.shipinstruct_dict.add(fields[13]));
            parsed.l_shipmode.push_back(parsed.shipmode_dict.add(fields[14]));
            parsed.l_comment.push_back(fields[15]);
            row_count++;
        }

        ptr = line_end + 1;
    }

    std::cout << "Parsed " << row_count << " lineitem rows\n";

    // Sort by shipdate, returnflag, linestatus
    std::vector<size_t> perm(row_count);
    for (size_t i = 0; i < row_count; ++i) perm[i] = i;

    std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
        if (parsed.l_shipdate[a] != parsed.l_shipdate[b]) {
            return parsed.l_shipdate[a] < parsed.l_shipdate[b];
        }
        if (parsed.l_returnflag[a] != parsed.l_returnflag[b]) {
            return parsed.l_returnflag[a] < parsed.l_returnflag[b];
        }
        return parsed.l_linestatus[a] < parsed.l_linestatus[b];
    });

    // Write column files
    fs::create_directories(gendb_dir);

    auto write_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t idx : perm) {
            writer.write(col[idx]);
        }
    };

    auto write_string_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t idx : perm) {
            writer.write_string(col[idx]);
        }
    };

    write_column("l_orderkey", parsed.l_orderkey);
    write_column("l_partkey", parsed.l_partkey);
    write_column("l_suppkey", parsed.l_suppkey);
    write_column("l_linenumber", parsed.l_linenumber);
    write_column("l_quantity", parsed.l_quantity);
    write_column("l_extendedprice", parsed.l_extendedprice);
    write_column("l_discount", parsed.l_discount);
    write_column("l_tax", parsed.l_tax);
    write_column("l_returnflag", parsed.l_returnflag);
    write_column("l_linestatus", parsed.l_linestatus);
    write_column("l_shipdate", parsed.l_shipdate);
    write_column("l_commitdate", parsed.l_commitdate);
    write_column("l_receiptdate", parsed.l_receiptdate);
    write_column("l_shipinstruct", parsed.l_shipinstruct);
    write_column("l_shipmode", parsed.l_shipmode);
    write_string_column("l_comment", parsed.l_comment);

    // Write dictionary files
    std::ofstream dict_file(gendb_dir + "/lineitem_dicts.txt");
    for (auto& [val, code] : parsed.returnflag_dict.get_dict()) {
        dict_file << "returnflag:" << code << ":" << val << "\n";
    }
    for (auto& [val, code] : parsed.linestatus_dict.get_dict()) {
        dict_file << "linestatus:" << code << ":" << val << "\n";
    }
    for (auto& [val, code] : parsed.shipinstruct_dict.get_dict()) {
        dict_file << "shipinstruct:" << code << ":" << val << "\n";
    }
    for (auto& [val, code] : parsed.shipmode_dict.get_dict()) {
        dict_file << "shipmode:" << code << ":" << val << "\n";
    }

    munmap((void*)data, file_size);
    close(fd);
}

// Orders ingestion
struct OrdersData {
    std::vector<int32_t> o_orderkey;
    std::vector<int32_t> o_custkey;
    std::vector<uint8_t> o_orderstatus;
    std::vector<double> o_totalprice;
    std::vector<int32_t> o_orderdate;
    std::vector<uint8_t> o_orderpriority;
    std::vector<std::string> o_clerk;
    std::vector<int32_t> o_shippriority;
    std::vector<std::string> o_comment;
    StringDictionary orderstatus_dict;
    StringDictionary orderpriority_dict;
};

void ingest_orders(const std::string& data_dir, const std::string& gendb_dir) {
    std::string filepath = data_dir + "/orders.tbl";

    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Cannot open " + filepath);
    }

    struct stat st;
    fstat(fd, &st);
    size_t file_size = st.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        throw std::runtime_error("mmap failed");
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    OrdersData parsed;
    const char* ptr = data;
    const char* end = data + file_size;
    int row_count = 0;

    while (ptr < end) {
        const char* line_end = find_newline(ptr, end);
        if (line_end == ptr) break;

        std::vector<std::string> fields = split_line(ptr, line_end - ptr, '|');

        if (fields.size() >= 9) {
            parsed.o_orderkey.push_back(std::stoi(fields[0]));
            parsed.o_custkey.push_back(std::stoi(fields[1]));
            parsed.o_orderstatus.push_back(parsed.orderstatus_dict.add(fields[2]));
            parsed.o_totalprice.push_back(std::stod(fields[3]));
            parsed.o_orderdate.push_back(parse_date(fields[4]));
            parsed.o_orderpriority.push_back(parsed.orderpriority_dict.add(fields[5]));
            parsed.o_clerk.push_back(fields[6]);
            parsed.o_shippriority.push_back(std::stoi(fields[7]));
            parsed.o_comment.push_back(fields[8]);
            row_count++;
        }

        ptr = line_end + 1;
    }

    std::cout << "Parsed " << row_count << " orders rows\n";

    // Sort by orderdate
    std::vector<size_t> perm(row_count);
    for (size_t i = 0; i < row_count; ++i) perm[i] = i;

    std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
        return parsed.o_orderdate[a] < parsed.o_orderdate[b];
    });

    fs::create_directories(gendb_dir);

    auto write_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t idx : perm) {
            writer.write(col[idx]);
        }
    };

    auto write_string_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t idx : perm) {
            writer.write_string(col[idx]);
        }
    };

    write_column("o_orderkey", parsed.o_orderkey);
    write_column("o_custkey", parsed.o_custkey);
    write_column("o_orderstatus", parsed.o_orderstatus);
    write_column("o_totalprice", parsed.o_totalprice);
    write_column("o_orderdate", parsed.o_orderdate);
    write_column("o_orderpriority", parsed.o_orderpriority);
    write_string_column("o_clerk", parsed.o_clerk);
    write_column("o_shippriority", parsed.o_shippriority);
    write_string_column("o_comment", parsed.o_comment);

    std::ofstream dict_file(gendb_dir + "/orders_dicts.txt");
    for (auto& [val, code] : parsed.orderstatus_dict.get_dict()) {
        dict_file << "orderstatus:" << code << ":" << val << "\n";
    }
    for (auto& [val, code] : parsed.orderpriority_dict.get_dict()) {
        dict_file << "orderpriority:" << code << ":" << val << "\n";
    }

    munmap((void*)data, file_size);
    close(fd);
}

// Customer ingestion
struct CustomerData {
    std::vector<int32_t> c_custkey;
    std::vector<std::string> c_name;
    std::vector<std::string> c_address;
    std::vector<int32_t> c_nationkey;
    std::vector<std::string> c_phone;
    std::vector<double> c_acctbal;
    std::vector<uint8_t> c_mktsegment;
    std::vector<std::string> c_comment;
    StringDictionary mktsegment_dict;
};

void ingest_customer(const std::string& data_dir, const std::string& gendb_dir) {
    std::string filepath = data_dir + "/customer.tbl";

    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Cannot open " + filepath);
    }

    struct stat st;
    fstat(fd, &st);
    size_t file_size = st.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        throw std::runtime_error("mmap failed");
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    CustomerData parsed;
    const char* ptr = data;
    const char* end = data + file_size;
    int row_count = 0;

    while (ptr < end) {
        const char* line_end = find_newline(ptr, end);
        if (line_end == ptr) break;

        std::vector<std::string> fields = split_line(ptr, line_end - ptr, '|');

        if (fields.size() >= 8) {
            parsed.c_custkey.push_back(std::stoi(fields[0]));
            parsed.c_name.push_back(fields[1]);
            parsed.c_address.push_back(fields[2]);
            parsed.c_nationkey.push_back(std::stoi(fields[3]));
            parsed.c_phone.push_back(fields[4]);
            parsed.c_acctbal.push_back(std::stod(fields[5]));
            parsed.c_mktsegment.push_back(parsed.mktsegment_dict.add(fields[6]));
            parsed.c_comment.push_back(fields[7]);
            row_count++;
        }

        ptr = line_end + 1;
    }

    std::cout << "Parsed " << row_count << " customer rows\n";

    // Sort by custkey
    std::vector<size_t> perm(row_count);
    for (size_t i = 0; i < row_count; ++i) perm[i] = i;

    std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
        return parsed.c_custkey[a] < parsed.c_custkey[b];
    });

    fs::create_directories(gendb_dir);

    auto write_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t idx : perm) {
            writer.write(col[idx]);
        }
    };

    auto write_string_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t idx : perm) {
            writer.write_string(col[idx]);
        }
    };

    write_column("c_custkey", parsed.c_custkey);
    write_string_column("c_name", parsed.c_name);
    write_string_column("c_address", parsed.c_address);
    write_column("c_nationkey", parsed.c_nationkey);
    write_string_column("c_phone", parsed.c_phone);
    write_column("c_acctbal", parsed.c_acctbal);
    write_column("c_mktsegment", parsed.c_mktsegment);
    write_string_column("c_comment", parsed.c_comment);

    std::ofstream dict_file(gendb_dir + "/customer_dicts.txt");
    for (auto& [val, code] : parsed.mktsegment_dict.get_dict()) {
        dict_file << "mktsegment:" << code << ":" << val << "\n";
    }

    munmap((void*)data, file_size);
    close(fd);
}

// Part ingestion
struct PartData {
    std::vector<int32_t> p_partkey;
    std::vector<std::string> p_name;
    std::vector<uint8_t> p_mfgr;
    std::vector<uint8_t> p_brand;
    std::vector<std::string> p_type;
    std::vector<int32_t> p_size;
    std::vector<uint8_t> p_container;
    std::vector<double> p_retailprice;
    std::vector<std::string> p_comment;
    StringDictionary mfgr_dict;
    StringDictionary brand_dict;
    StringDictionary container_dict;
};

void ingest_part(const std::string& data_dir, const std::string& gendb_dir) {
    std::string filepath = data_dir + "/part.tbl";

    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Cannot open " + filepath);
    }

    struct stat st;
    fstat(fd, &st);
    size_t file_size = st.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        throw std::runtime_error("mmap failed");
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    PartData parsed;
    const char* ptr = data;
    const char* end = data + file_size;
    int row_count = 0;

    while (ptr < end) {
        const char* line_end = find_newline(ptr, end);
        if (line_end == ptr) break;

        std::vector<std::string> fields = split_line(ptr, line_end - ptr, '|');

        if (fields.size() >= 9) {
            parsed.p_partkey.push_back(std::stoi(fields[0]));
            parsed.p_name.push_back(fields[1]);
            parsed.p_mfgr.push_back(parsed.mfgr_dict.add(fields[2]));
            parsed.p_brand.push_back(parsed.brand_dict.add(fields[3]));
            parsed.p_type.push_back(fields[4]);
            parsed.p_size.push_back(std::stoi(fields[5]));
            parsed.p_container.push_back(parsed.container_dict.add(fields[6]));
            parsed.p_retailprice.push_back(std::stod(fields[7]));
            parsed.p_comment.push_back(fields[8]);
            row_count++;
        }

        ptr = line_end + 1;
    }

    std::cout << "Parsed " << row_count << " part rows\n";

    fs::create_directories(gendb_dir);

    auto write_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t i = 0; i < col.size(); ++i) {
            writer.write(col[i]);
        }
    };

    auto write_string_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t i = 0; i < col.size(); ++i) {
            writer.write_string(col[i]);
        }
    };

    write_column("p_partkey", parsed.p_partkey);
    write_string_column("p_name", parsed.p_name);
    write_column("p_mfgr", parsed.p_mfgr);
    write_column("p_brand", parsed.p_brand);
    write_string_column("p_type", parsed.p_type);
    write_column("p_size", parsed.p_size);
    write_column("p_container", parsed.p_container);
    write_column("p_retailprice", parsed.p_retailprice);
    write_string_column("p_comment", parsed.p_comment);

    std::ofstream dict_file(gendb_dir + "/part_dicts.txt");
    for (auto& [val, code] : parsed.mfgr_dict.get_dict()) {
        dict_file << "mfgr:" << code << ":" << val << "\n";
    }
    for (auto& [val, code] : parsed.brand_dict.get_dict()) {
        dict_file << "brand:" << code << ":" << val << "\n";
    }
    for (auto& [val, code] : parsed.container_dict.get_dict()) {
        dict_file << "container:" << code << ":" << val << "\n";
    }

    munmap((void*)data, file_size);
    close(fd);
}

// Partsupp ingestion
struct PartsuppData {
    std::vector<int32_t> ps_partkey;
    std::vector<int32_t> ps_suppkey;
    std::vector<int32_t> ps_availqty;
    std::vector<double> ps_supplycost;
    std::vector<std::string> ps_comment;
};

void ingest_partsupp(const std::string& data_dir, const std::string& gendb_dir) {
    std::string filepath = data_dir + "/partsupp.tbl";

    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Cannot open " + filepath);
    }

    struct stat st;
    fstat(fd, &st);
    size_t file_size = st.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        throw std::runtime_error("mmap failed");
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    PartsuppData parsed;
    const char* ptr = data;
    const char* end = data + file_size;
    int row_count = 0;

    while (ptr < end) {
        const char* line_end = find_newline(ptr, end);
        if (line_end == ptr) break;

        std::vector<std::string> fields = split_line(ptr, line_end - ptr, '|');

        if (fields.size() >= 5) {
            parsed.ps_partkey.push_back(std::stoi(fields[0]));
            parsed.ps_suppkey.push_back(std::stoi(fields[1]));
            parsed.ps_availqty.push_back(std::stoi(fields[2]));
            parsed.ps_supplycost.push_back(std::stod(fields[3]));
            parsed.ps_comment.push_back(fields[4]);
            row_count++;
        }

        ptr = line_end + 1;
    }

    std::cout << "Parsed " << row_count << " partsupp rows\n";

    fs::create_directories(gendb_dir);

    auto write_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t i = 0; i < col.size(); ++i) {
            writer.write(col[i]);
        }
    };

    auto write_string_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t i = 0; i < col.size(); ++i) {
            writer.write_string(col[i]);
        }
    };

    write_column("ps_partkey", parsed.ps_partkey);
    write_column("ps_suppkey", parsed.ps_suppkey);
    write_column("ps_availqty", parsed.ps_availqty);
    write_column("ps_supplycost", parsed.ps_supplycost);
    write_string_column("ps_comment", parsed.ps_comment);

    munmap((void*)data, file_size);
    close(fd);
}

// Supplier ingestion
struct SupplierData {
    std::vector<int32_t> s_suppkey;
    std::vector<std::string> s_name;
    std::vector<std::string> s_address;
    std::vector<int32_t> s_nationkey;
    std::vector<std::string> s_phone;
    std::vector<double> s_acctbal;
    std::vector<std::string> s_comment;
};

void ingest_supplier(const std::string& data_dir, const std::string& gendb_dir) {
    std::string filepath = data_dir + "/supplier.tbl";

    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Cannot open " + filepath);
    }

    struct stat st;
    fstat(fd, &st);
    size_t file_size = st.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        throw std::runtime_error("mmap failed");
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    SupplierData parsed;
    const char* ptr = data;
    const char* end = data + file_size;
    int row_count = 0;

    while (ptr < end) {
        const char* line_end = find_newline(ptr, end);
        if (line_end == ptr) break;

        std::vector<std::string> fields = split_line(ptr, line_end - ptr, '|');

        if (fields.size() >= 7) {
            parsed.s_suppkey.push_back(std::stoi(fields[0]));
            parsed.s_name.push_back(fields[1]);
            parsed.s_address.push_back(fields[2]);
            parsed.s_nationkey.push_back(std::stoi(fields[3]));
            parsed.s_phone.push_back(fields[4]);
            parsed.s_acctbal.push_back(std::stod(fields[5]));
            parsed.s_comment.push_back(fields[6]);
            row_count++;
        }

        ptr = line_end + 1;
    }

    std::cout << "Parsed " << row_count << " supplier rows\n";

    fs::create_directories(gendb_dir);

    auto write_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t i = 0; i < col.size(); ++i) {
            writer.write(col[i]);
        }
    };

    auto write_string_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t i = 0; i < col.size(); ++i) {
            writer.write_string(col[i]);
        }
    };

    write_column("s_suppkey", parsed.s_suppkey);
    write_string_column("s_name", parsed.s_name);
    write_string_column("s_address", parsed.s_address);
    write_column("s_nationkey", parsed.s_nationkey);
    write_string_column("s_phone", parsed.s_phone);
    write_column("s_acctbal", parsed.s_acctbal);
    write_string_column("s_comment", parsed.s_comment);

    munmap((void*)data, file_size);
    close(fd);
}

// Nation ingestion
struct NationData {
    std::vector<int32_t> n_nationkey;
    std::vector<std::string> n_name;
    std::vector<int32_t> n_regionkey;
    std::vector<std::string> n_comment;
};

void ingest_nation(const std::string& data_dir, const std::string& gendb_dir) {
    std::string filepath = data_dir + "/nation.tbl";

    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Cannot open " + filepath);
    }

    struct stat st;
    fstat(fd, &st);
    size_t file_size = st.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        throw std::runtime_error("mmap failed");
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    NationData parsed;
    const char* ptr = data;
    const char* end = data + file_size;
    int row_count = 0;

    while (ptr < end) {
        const char* line_end = find_newline(ptr, end);
        if (line_end == ptr) break;

        std::vector<std::string> fields = split_line(ptr, line_end - ptr, '|');

        if (fields.size() >= 4) {
            parsed.n_nationkey.push_back(std::stoi(fields[0]));
            parsed.n_name.push_back(fields[1]);
            parsed.n_regionkey.push_back(std::stoi(fields[2]));
            parsed.n_comment.push_back(fields[3]);
            row_count++;
        }

        ptr = line_end + 1;
    }

    std::cout << "Parsed " << row_count << " nation rows\n";

    fs::create_directories(gendb_dir);

    auto write_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t i = 0; i < col.size(); ++i) {
            writer.write(col[i]);
        }
    };

    auto write_string_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t i = 0; i < col.size(); ++i) {
            writer.write_string(col[i]);
        }
    };

    write_column("n_nationkey", parsed.n_nationkey);
    write_string_column("n_name", parsed.n_name);
    write_column("n_regionkey", parsed.n_regionkey);
    write_string_column("n_comment", parsed.n_comment);

    munmap((void*)data, file_size);
    close(fd);
}

// Region ingestion
struct RegionData {
    std::vector<int32_t> r_regionkey;
    std::vector<std::string> r_name;
    std::vector<std::string> r_comment;
};

void ingest_region(const std::string& data_dir, const std::string& gendb_dir) {
    std::string filepath = data_dir + "/region.tbl";

    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Cannot open " + filepath);
    }

    struct stat st;
    fstat(fd, &st);
    size_t file_size = st.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        throw std::runtime_error("mmap failed");
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    RegionData parsed;
    const char* ptr = data;
    const char* end = data + file_size;
    int row_count = 0;

    while (ptr < end) {
        const char* line_end = find_newline(ptr, end);
        if (line_end == ptr) break;

        std::vector<std::string> fields = split_line(ptr, line_end - ptr, '|');

        if (fields.size() >= 3) {
            parsed.r_regionkey.push_back(std::stoi(fields[0]));
            parsed.r_name.push_back(fields[1]);
            parsed.r_comment.push_back(fields[2]);
            row_count++;
        }

        ptr = line_end + 1;
    }

    std::cout << "Parsed " << row_count << " region rows\n";

    fs::create_directories(gendb_dir);

    auto write_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t i = 0; i < col.size(); ++i) {
            writer.write(col[i]);
        }
    };

    auto write_string_column = [&](const std::string& name, auto& col) {
        BinaryWriter writer(gendb_dir + "/" + name + ".bin");
        for (size_t i = 0; i < col.size(); ++i) {
            writer.write_string(col[i]);
        }
    };

    write_column("r_regionkey", parsed.r_regionkey);
    write_string_column("r_name", parsed.r_name);
    write_string_column("r_comment", parsed.r_comment);

    munmap((void*)data, file_size);
    close(fd);
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>\n";
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    auto start = high_resolution_clock::now();

    std::cout << "Starting ingestion with " << THREAD_COUNT << " threads\n";

    // Create subdirectories for each table
    fs::create_directories(gendb_dir);
    fs::create_directories(gendb_dir + "/lineitem");
    fs::create_directories(gendb_dir + "/orders");
    fs::create_directories(gendb_dir + "/customer");
    fs::create_directories(gendb_dir + "/part");
    fs::create_directories(gendb_dir + "/partsupp");
    fs::create_directories(gendb_dir + "/supplier");
    fs::create_directories(gendb_dir + "/nation");
    fs::create_directories(gendb_dir + "/region");

    // Ingest tables in parallel (large ones on more threads)
    std::vector<std::thread> threads;

    threads.push_back(std::thread([&]() { ingest_lineitem(data_dir, gendb_dir + "/lineitem"); }));
    threads.push_back(std::thread([&]() { ingest_orders(data_dir, gendb_dir + "/orders"); }));
    threads.push_back(std::thread([&]() { ingest_customer(data_dir, gendb_dir + "/customer"); }));
    threads.push_back(std::thread([&]() { ingest_part(data_dir, gendb_dir + "/part"); }));
    threads.push_back(std::thread([&]() { ingest_partsupp(data_dir, gendb_dir + "/partsupp"); }));
    threads.push_back(std::thread([&]() { ingest_supplier(data_dir, gendb_dir + "/supplier"); }));
    threads.push_back(std::thread([&]() { ingest_nation(data_dir, gendb_dir + "/nation"); }));
    threads.push_back(std::thread([&]() { ingest_region(data_dir, gendb_dir + "/region"); }));

    for (auto& t : threads) {
        t.join();
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<seconds>(end - start);

    std::cout << "Ingestion complete in " << duration.count() << " seconds\n";

    return 0;
}
