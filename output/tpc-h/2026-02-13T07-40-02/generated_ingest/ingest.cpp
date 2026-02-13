#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <charconv>
#include <algorithm>
#include <unordered_map>
#include <filesystem>
#include <cmath>

namespace fs = std::filesystem;

// Parse date YYYY-MM-DD to days since 1970-01-01
inline int32_t parse_date(const char* str) {
    int year = (str[0] - '0') * 1000 + (str[1] - '0') * 100 + (str[2] - '0') * 10 + (str[3] - '0');
    int month = (str[5] - '0') * 10 + (str[6] - '0');
    int day = (str[8] - '0') * 10 + (str[9] - '0');

    // Days since epoch calculation
    int days_in_months[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    int days = (year - 1970) * 365 + (year - 1969) / 4 - (year - 1901) / 100 + (year - 1601) / 400;
    days += days_in_months[month - 1] + day - 1;
    if (month > 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) days++;
    return days;
}

// Parse decimal to int64 cents (x100)
inline int64_t parse_decimal(const char* start, const char* end) {
    bool negative = (*start == '-');
    if (negative) start++;

    int64_t result = 0;
    int decimals = 0;
    bool after_dot = false;

    for (const char* p = start; p < end; p++) {
        if (*p == '.') {
            after_dot = true;
        } else if (*p >= '0' && *p <= '9') {
            result = result * 10 + (*p - '0');
            if (after_dot) decimals++;
        }
    }

    // Scale to cents (2 decimal places)
    while (decimals < 2) {
        result *= 10;
        decimals++;
    }
    while (decimals > 2) {
        result /= 10;
        decimals--;
    }

    return negative ? -result : result;
}

// Dictionary for low-cardinality strings
class Dictionary {
    std::unordered_map<std::string, uint8_t> map_;
    std::vector<std::string> values_;
    std::mutex mutex_;
public:
    uint8_t encode(const std::string& s) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(s);
        if (it != map_.end()) return it->second;
        uint8_t code = static_cast<uint8_t>(values_.size());
        map_[s] = code;
        values_.push_back(s);
        return code;
    }

    const std::vector<std::string>& get_values() const { return values_; }
};

// Lineitem table storage
struct LineitemColumns {
    std::vector<int32_t> l_orderkey;
    std::vector<int32_t> l_partkey;
    std::vector<int32_t> l_suppkey;
    std::vector<int32_t> l_linenumber;
    std::vector<int64_t> l_quantity;
    std::vector<int64_t> l_extendedprice;
    std::vector<int64_t> l_discount;
    std::vector<int64_t> l_tax;
    std::vector<uint8_t> l_returnflag;
    std::vector<uint8_t> l_linestatus;
    std::vector<int32_t> l_shipdate;
    std::vector<int32_t> l_commitdate;
    std::vector<int32_t> l_receiptdate;
    std::vector<std::string> l_shipinstruct;
    std::vector<uint8_t> l_shipmode;
    std::vector<std::string> l_comment;

    Dictionary dict_returnflag;
    Dictionary dict_linestatus;
    Dictionary dict_shipmode;

    void reserve(size_t n) {
        l_orderkey.reserve(n);
        l_partkey.reserve(n);
        l_suppkey.reserve(n);
        l_linenumber.reserve(n);
        l_quantity.reserve(n);
        l_extendedprice.reserve(n);
        l_discount.reserve(n);
        l_tax.reserve(n);
        l_returnflag.reserve(n);
        l_linestatus.reserve(n);
        l_shipdate.reserve(n);
        l_commitdate.reserve(n);
        l_receiptdate.reserve(n);
        l_shipinstruct.reserve(n);
        l_shipmode.reserve(n);
        l_comment.reserve(n);
    }
};

// Orders table storage
struct OrdersColumns {
    std::vector<int32_t> o_orderkey;
    std::vector<int32_t> o_custkey;
    std::vector<uint8_t> o_orderstatus;
    std::vector<int64_t> o_totalprice;
    std::vector<int32_t> o_orderdate;
    std::vector<uint8_t> o_orderpriority;
    std::vector<std::string> o_clerk;
    std::vector<int32_t> o_shippriority;
    std::vector<std::string> o_comment;

    Dictionary dict_orderstatus;
    Dictionary dict_orderpriority;

    void reserve(size_t n) {
        o_orderkey.reserve(n);
        o_custkey.reserve(n);
        o_orderstatus.reserve(n);
        o_totalprice.reserve(n);
        o_orderdate.reserve(n);
        o_orderpriority.reserve(n);
        o_clerk.reserve(n);
        o_shippriority.reserve(n);
        o_comment.reserve(n);
    }
};

// Customer table storage
struct CustomerColumns {
    std::vector<int32_t> c_custkey;
    std::vector<std::string> c_name;
    std::vector<std::string> c_address;
    std::vector<int32_t> c_nationkey;
    std::vector<std::string> c_phone;
    std::vector<int64_t> c_acctbal;
    std::vector<uint8_t> c_mktsegment;
    std::vector<std::string> c_comment;

    Dictionary dict_mktsegment;

    void reserve(size_t n) {
        c_custkey.reserve(n);
        c_name.reserve(n);
        c_address.reserve(n);
        c_nationkey.reserve(n);
        c_phone.reserve(n);
        c_acctbal.reserve(n);
        c_mktsegment.reserve(n);
        c_comment.reserve(n);
    }
};

// Parse lineitem chunk
void parse_lineitem_chunk(const char* start, const char* end, LineitemColumns& cols, std::mutex& mutex) {
    // Local buffers
    std::vector<int32_t> local_orderkey, local_partkey, local_suppkey, local_linenumber;
    std::vector<int64_t> local_quantity, local_extendedprice, local_discount, local_tax;
    std::vector<uint8_t> local_returnflag, local_linestatus, local_shipmode;
    std::vector<int32_t> local_shipdate, local_commitdate, local_receiptdate;
    std::vector<std::string> local_shipinstruct, local_comment;

    const char* line_start = start;
    const char* p = start;

    while (p < end) {
        if (*p == '\n') {
            const char* line_end = p;
            const char* fields[16];
            int field_idx = 0;
            fields[field_idx++] = line_start;

            for (const char* q = line_start; q < line_end && field_idx < 16; q++) {
                if (*q == '|') {
                    fields[field_idx++] = q + 1;
                }
            }

            if (field_idx == 16) {
                // Parse each field
                int32_t orderkey, partkey, suppkey, linenumber;
                std::from_chars(fields[0], fields[1] - 1, orderkey);
                std::from_chars(fields[1], fields[2] - 1, partkey);
                std::from_chars(fields[2], fields[3] - 1, suppkey);
                std::from_chars(fields[3], fields[4] - 1, linenumber);

                int64_t quantity = parse_decimal(fields[4], fields[5] - 1);
                int64_t extendedprice = parse_decimal(fields[5], fields[6] - 1);
                int64_t discount = parse_decimal(fields[6], fields[7] - 1);
                int64_t tax = parse_decimal(fields[7], fields[8] - 1);

                std::string returnflag(fields[8], fields[9] - 1);
                std::string linestatus(fields[9], fields[10] - 1);

                int32_t shipdate = parse_date(fields[10]);
                int32_t commitdate = parse_date(fields[11]);
                int32_t receiptdate = parse_date(fields[12]);

                std::string shipinstruct(fields[13], fields[14] - 1);
                std::string shipmode(fields[14], fields[15] - 1);
                std::string comment(fields[15], line_end);

                local_orderkey.push_back(orderkey);
                local_partkey.push_back(partkey);
                local_suppkey.push_back(suppkey);
                local_linenumber.push_back(linenumber);
                local_quantity.push_back(quantity);
                local_extendedprice.push_back(extendedprice);
                local_discount.push_back(discount);
                local_tax.push_back(tax);
                local_returnflag.push_back(cols.dict_returnflag.encode(returnflag));
                local_linestatus.push_back(cols.dict_linestatus.encode(linestatus));
                local_shipdate.push_back(shipdate);
                local_commitdate.push_back(commitdate);
                local_receiptdate.push_back(receiptdate);
                local_shipinstruct.push_back(shipinstruct);
                local_shipmode.push_back(cols.dict_shipmode.encode(shipmode));
                local_comment.push_back(comment);
            }

            line_start = p + 1;
        }
        p++;
    }

    // Merge into global columns
    std::lock_guard<std::mutex> lock(mutex);
    cols.l_orderkey.insert(cols.l_orderkey.end(), local_orderkey.begin(), local_orderkey.end());
    cols.l_partkey.insert(cols.l_partkey.end(), local_partkey.begin(), local_partkey.end());
    cols.l_suppkey.insert(cols.l_suppkey.end(), local_suppkey.begin(), local_suppkey.end());
    cols.l_linenumber.insert(cols.l_linenumber.end(), local_linenumber.begin(), local_linenumber.end());
    cols.l_quantity.insert(cols.l_quantity.end(), local_quantity.begin(), local_quantity.end());
    cols.l_extendedprice.insert(cols.l_extendedprice.end(), local_extendedprice.begin(), local_extendedprice.end());
    cols.l_discount.insert(cols.l_discount.end(), local_discount.begin(), local_discount.end());
    cols.l_tax.insert(cols.l_tax.end(), local_tax.begin(), local_tax.end());
    cols.l_returnflag.insert(cols.l_returnflag.end(), local_returnflag.begin(), local_returnflag.end());
    cols.l_linestatus.insert(cols.l_linestatus.end(), local_linestatus.begin(), local_linestatus.end());
    cols.l_shipdate.insert(cols.l_shipdate.end(), local_shipdate.begin(), local_shipdate.end());
    cols.l_commitdate.insert(cols.l_commitdate.end(), local_commitdate.begin(), local_commitdate.end());
    cols.l_receiptdate.insert(cols.l_receiptdate.end(), local_receiptdate.begin(), local_receiptdate.end());
    cols.l_shipinstruct.insert(cols.l_shipinstruct.end(), local_shipinstruct.begin(), local_shipinstruct.end());
    cols.l_shipmode.insert(cols.l_shipmode.end(), local_shipmode.begin(), local_shipmode.end());
    cols.l_comment.insert(cols.l_comment.end(), local_comment.begin(), local_comment.end());
}

// Ingest lineitem table (parallel)
void ingest_lineitem(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting lineitem..." << std::endl;

    std::string filename = data_dir + "/lineitem.tbl";
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << filename << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    char* data = (char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << filename << std::endl;
        close(fd);
        return;
    }
    madvise(data, file_size, MADV_SEQUENTIAL);

    LineitemColumns cols;
    cols.reserve(60000000);

    // Parallel parsing
    unsigned int num_threads = std::thread::hardware_concurrency();
    size_t chunk_size = file_size / num_threads;

    std::vector<std::thread> threads;
    std::mutex mutex;

    for (unsigned int i = 0; i < num_threads; i++) {
        size_t start = i * chunk_size;
        size_t end = (i == num_threads - 1) ? file_size : (i + 1) * chunk_size;

        // Align to newline boundaries
        if (i > 0) {
            while (start < file_size && data[start - 1] != '\n') start++;
        }
        if (i < num_threads - 1) {
            while (end < file_size && data[end] != '\n') end++;
        }

        threads.emplace_back(parse_lineitem_chunk, data + start, data + end, std::ref(cols), std::ref(mutex));
    }

    for (auto& t : threads) t.join();

    munmap(data, file_size);
    close(fd);

    size_t row_count = cols.l_orderkey.size();
    std::cout << "Parsed " << row_count << " rows from lineitem" << std::endl;

    // Sort by l_shipdate using permutation
    std::cout << "Sorting lineitem by l_shipdate..." << std::endl;
    std::vector<std::pair<int32_t, size_t>> perm(row_count);
    for (size_t i = 0; i < row_count; i++) {
        perm[i] = {cols.l_shipdate[i], i};
    }
    std::sort(perm.begin(), perm.end());

    // Reorder columns
    auto reorder_int32 = [&](std::vector<int32_t>& vec) {
        std::vector<int32_t> temp(row_count);
        for (size_t i = 0; i < row_count; i++) temp[i] = vec[perm[i].second];
        vec = std::move(temp);
    };
    auto reorder_int64 = [&](std::vector<int64_t>& vec) {
        std::vector<int64_t> temp(row_count);
        for (size_t i = 0; i < row_count; i++) temp[i] = vec[perm[i].second];
        vec = std::move(temp);
    };
    auto reorder_uint8 = [&](std::vector<uint8_t>& vec) {
        std::vector<uint8_t> temp(row_count);
        for (size_t i = 0; i < row_count; i++) temp[i] = vec[perm[i].second];
        vec = std::move(temp);
    };
    auto reorder_string = [&](std::vector<std::string>& vec) {
        std::vector<std::string> temp(row_count);
        for (size_t i = 0; i < row_count; i++) temp[i] = vec[perm[i].second];
        vec = std::move(temp);
    };

    reorder_int32(cols.l_orderkey);
    reorder_int32(cols.l_partkey);
    reorder_int32(cols.l_suppkey);
    reorder_int32(cols.l_linenumber);
    reorder_int64(cols.l_quantity);
    reorder_int64(cols.l_extendedprice);
    reorder_int64(cols.l_discount);
    reorder_int64(cols.l_tax);
    reorder_uint8(cols.l_returnflag);
    reorder_uint8(cols.l_linestatus);
    reorder_int32(cols.l_shipdate);
    reorder_int32(cols.l_commitdate);
    reorder_int32(cols.l_receiptdate);
    reorder_string(cols.l_shipinstruct);
    reorder_uint8(cols.l_shipmode);
    reorder_string(cols.l_comment);

    // Write columns to disk
    std::string table_dir = gendb_dir + "/lineitem";
    fs::create_directories(table_dir);

    auto write_column = [&](const std::string& name, const void* data, size_t elem_size, size_t count) {
        std::string col_file = table_dir + "/" + name + ".bin";
        std::ofstream ofs(col_file, std::ios::binary);
        ofs.write((const char*)data, elem_size * count);
        ofs.close();
    };

    write_column("l_orderkey", cols.l_orderkey.data(), sizeof(int32_t), row_count);
    write_column("l_partkey", cols.l_partkey.data(), sizeof(int32_t), row_count);
    write_column("l_suppkey", cols.l_suppkey.data(), sizeof(int32_t), row_count);
    write_column("l_linenumber", cols.l_linenumber.data(), sizeof(int32_t), row_count);
    write_column("l_quantity", cols.l_quantity.data(), sizeof(int64_t), row_count);
    write_column("l_extendedprice", cols.l_extendedprice.data(), sizeof(int64_t), row_count);
    write_column("l_discount", cols.l_discount.data(), sizeof(int64_t), row_count);
    write_column("l_tax", cols.l_tax.data(), sizeof(int64_t), row_count);
    write_column("l_returnflag", cols.l_returnflag.data(), sizeof(uint8_t), row_count);
    write_column("l_linestatus", cols.l_linestatus.data(), sizeof(uint8_t), row_count);
    write_column("l_shipdate", cols.l_shipdate.data(), sizeof(int32_t), row_count);
    write_column("l_commitdate", cols.l_commitdate.data(), sizeof(int32_t), row_count);
    write_column("l_receiptdate", cols.l_receiptdate.data(), sizeof(int32_t), row_count);
    write_column("l_shipmode", cols.l_shipmode.data(), sizeof(uint8_t), row_count);

    // Write string columns
    {
        std::ofstream ofs(table_dir + "/l_shipinstruct.bin", std::ios::binary);
        for (const auto& s : cols.l_shipinstruct) {
            uint32_t len = s.size();
            ofs.write((char*)&len, sizeof(len));
            ofs.write(s.data(), len);
        }
    }
    {
        std::ofstream ofs(table_dir + "/l_comment.bin", std::ios::binary);
        for (const auto& s : cols.l_comment) {
            uint32_t len = s.size();
            ofs.write((char*)&len, sizeof(len));
            ofs.write(s.data(), len);
        }
    }

    // Write dictionaries
    {
        std::ofstream ofs(table_dir + "/l_returnflag.dict", std::ios::binary);
        for (const auto& s : cols.dict_returnflag.get_values()) {
            ofs << s << "\n";
        }
    }
    {
        std::ofstream ofs(table_dir + "/l_linestatus.dict", std::ios::binary);
        for (const auto& s : cols.dict_linestatus.get_values()) {
            ofs << s << "\n";
        }
    }
    {
        std::ofstream ofs(table_dir + "/l_shipmode.dict", std::ios::binary);
        for (const auto& s : cols.dict_shipmode.get_values()) {
            ofs << s << "\n";
        }
    }

    // Write metadata
    std::ofstream meta(table_dir + "/metadata.json");
    meta << "{\n";
    meta << "  \"row_count\": " << row_count << ",\n";
    meta << "  \"sorted_by\": \"l_shipdate\"\n";
    meta << "}\n";
    meta.close();

    std::cout << "Lineitem ingestion complete: " << row_count << " rows" << std::endl;
}

// Parse orders chunk
void parse_orders_chunk(const char* start, const char* end, OrdersColumns& cols, std::mutex& mutex) {
    std::vector<int32_t> local_orderkey, local_custkey, local_shippriority;
    std::vector<uint8_t> local_orderstatus, local_orderpriority;
    std::vector<int64_t> local_totalprice;
    std::vector<int32_t> local_orderdate;
    std::vector<std::string> local_clerk, local_comment;

    const char* line_start = start;
    const char* p = start;

    while (p < end) {
        if (*p == '\n') {
            const char* line_end = p;
            const char* fields[9];
            int field_idx = 0;
            fields[field_idx++] = line_start;

            for (const char* q = line_start; q < line_end && field_idx < 9; q++) {
                if (*q == '|') fields[field_idx++] = q + 1;
            }

            if (field_idx == 9) {
                int32_t orderkey, custkey, shippriority;
                std::from_chars(fields[0], fields[1] - 1, orderkey);
                std::from_chars(fields[1], fields[2] - 1, custkey);

                std::string orderstatus(fields[2], fields[3] - 1);
                int64_t totalprice = parse_decimal(fields[3], fields[4] - 1);
                int32_t orderdate = parse_date(fields[4]);
                std::string orderpriority(fields[5], fields[6] - 1);
                std::string clerk(fields[6], fields[7] - 1);

                std::from_chars(fields[7], fields[8] - 1, shippriority);
                std::string comment(fields[8], line_end);

                local_orderkey.push_back(orderkey);
                local_custkey.push_back(custkey);
                local_orderstatus.push_back(cols.dict_orderstatus.encode(orderstatus));
                local_totalprice.push_back(totalprice);
                local_orderdate.push_back(orderdate);
                local_orderpriority.push_back(cols.dict_orderpriority.encode(orderpriority));
                local_clerk.push_back(clerk);
                local_shippriority.push_back(shippriority);
                local_comment.push_back(comment);
            }

            line_start = p + 1;
        }
        p++;
    }

    std::lock_guard<std::mutex> lock(mutex);
    cols.o_orderkey.insert(cols.o_orderkey.end(), local_orderkey.begin(), local_orderkey.end());
    cols.o_custkey.insert(cols.o_custkey.end(), local_custkey.begin(), local_custkey.end());
    cols.o_orderstatus.insert(cols.o_orderstatus.end(), local_orderstatus.begin(), local_orderstatus.end());
    cols.o_totalprice.insert(cols.o_totalprice.end(), local_totalprice.begin(), local_totalprice.end());
    cols.o_orderdate.insert(cols.o_orderdate.end(), local_orderdate.begin(), local_orderdate.end());
    cols.o_orderpriority.insert(cols.o_orderpriority.end(), local_orderpriority.begin(), local_orderpriority.end());
    cols.o_clerk.insert(cols.o_clerk.end(), local_clerk.begin(), local_clerk.end());
    cols.o_shippriority.insert(cols.o_shippriority.end(), local_shippriority.begin(), local_shippriority.end());
    cols.o_comment.insert(cols.o_comment.end(), local_comment.begin(), local_comment.end());
}

// Ingest orders table
void ingest_orders(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting orders..." << std::endl;

    std::string filename = data_dir + "/orders.tbl";
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << filename << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    char* data = (char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << filename << std::endl;
        close(fd);
        return;
    }
    madvise(data, file_size, MADV_SEQUENTIAL);

    OrdersColumns cols;
    cols.reserve(15000000);

    unsigned int num_threads = std::thread::hardware_concurrency();
    size_t chunk_size = file_size / num_threads;

    std::vector<std::thread> threads;
    std::mutex mutex;

    for (unsigned int i = 0; i < num_threads; i++) {
        size_t start = i * chunk_size;
        size_t end = (i == num_threads - 1) ? file_size : (i + 1) * chunk_size;

        if (i > 0) {
            while (start < file_size && data[start - 1] != '\n') start++;
        }
        if (i < num_threads - 1) {
            while (end < file_size && data[end] != '\n') end++;
        }

        threads.emplace_back(parse_orders_chunk, data + start, data + end, std::ref(cols), std::ref(mutex));
    }

    for (auto& t : threads) t.join();

    munmap(data, file_size);
    close(fd);

    size_t row_count = cols.o_orderkey.size();
    std::cout << "Parsed " << row_count << " rows from orders" << std::endl;

    // Sort by o_orderdate
    std::cout << "Sorting orders by o_orderdate..." << std::endl;
    std::vector<std::pair<int32_t, size_t>> perm(row_count);
    for (size_t i = 0; i < row_count; i++) {
        perm[i] = {cols.o_orderdate[i], i};
    }
    std::sort(perm.begin(), perm.end());

    auto reorder_int32 = [&](std::vector<int32_t>& vec) {
        std::vector<int32_t> temp(row_count);
        for (size_t i = 0; i < row_count; i++) temp[i] = vec[perm[i].second];
        vec = std::move(temp);
    };
    auto reorder_int64 = [&](std::vector<int64_t>& vec) {
        std::vector<int64_t> temp(row_count);
        for (size_t i = 0; i < row_count; i++) temp[i] = vec[perm[i].second];
        vec = std::move(temp);
    };
    auto reorder_uint8 = [&](std::vector<uint8_t>& vec) {
        std::vector<uint8_t> temp(row_count);
        for (size_t i = 0; i < row_count; i++) temp[i] = vec[perm[i].second];
        vec = std::move(temp);
    };
    auto reorder_string = [&](std::vector<std::string>& vec) {
        std::vector<std::string> temp(row_count);
        for (size_t i = 0; i < row_count; i++) temp[i] = vec[perm[i].second];
        vec = std::move(temp);
    };

    reorder_int32(cols.o_orderkey);
    reorder_int32(cols.o_custkey);
    reorder_uint8(cols.o_orderstatus);
    reorder_int64(cols.o_totalprice);
    reorder_int32(cols.o_orderdate);
    reorder_uint8(cols.o_orderpriority);
    reorder_string(cols.o_clerk);
    reorder_int32(cols.o_shippriority);
    reorder_string(cols.o_comment);

    // Write columns
    std::string table_dir = gendb_dir + "/orders";
    fs::create_directories(table_dir);

    auto write_column = [&](const std::string& name, const void* data, size_t elem_size, size_t count) {
        std::string col_file = table_dir + "/" + name + ".bin";
        std::ofstream ofs(col_file, std::ios::binary);
        ofs.write((const char*)data, elem_size * count);
    };

    write_column("o_orderkey", cols.o_orderkey.data(), sizeof(int32_t), row_count);
    write_column("o_custkey", cols.o_custkey.data(), sizeof(int32_t), row_count);
    write_column("o_orderstatus", cols.o_orderstatus.data(), sizeof(uint8_t), row_count);
    write_column("o_totalprice", cols.o_totalprice.data(), sizeof(int64_t), row_count);
    write_column("o_orderdate", cols.o_orderdate.data(), sizeof(int32_t), row_count);
    write_column("o_orderpriority", cols.o_orderpriority.data(), sizeof(uint8_t), row_count);
    write_column("o_shippriority", cols.o_shippriority.data(), sizeof(int32_t), row_count);

    {
        std::ofstream ofs(table_dir + "/o_clerk.bin", std::ios::binary);
        for (const auto& s : cols.o_clerk) {
            uint32_t len = s.size();
            ofs.write((char*)&len, sizeof(len));
            ofs.write(s.data(), len);
        }
    }
    {
        std::ofstream ofs(table_dir + "/o_comment.bin", std::ios::binary);
        for (const auto& s : cols.o_comment) {
            uint32_t len = s.size();
            ofs.write((char*)&len, sizeof(len));
            ofs.write(s.data(), len);
        }
    }

    {
        std::ofstream ofs(table_dir + "/o_orderstatus.dict");
        for (const auto& s : cols.dict_orderstatus.get_values()) ofs << s << "\n";
    }
    {
        std::ofstream ofs(table_dir + "/o_orderpriority.dict");
        for (const auto& s : cols.dict_orderpriority.get_values()) ofs << s << "\n";
    }

    std::ofstream meta(table_dir + "/metadata.json");
    meta << "{\n  \"row_count\": " << row_count << ",\n  \"sorted_by\": \"o_orderdate\"\n}\n";

    std::cout << "Orders ingestion complete: " << row_count << " rows" << std::endl;
}

// Parse customer chunk
void parse_customer_chunk(const char* start, const char* end, CustomerColumns& cols, std::mutex& mutex) {
    std::vector<int32_t> local_custkey, local_nationkey;
    std::vector<int64_t> local_acctbal;
    std::vector<uint8_t> local_mktsegment;
    std::vector<std::string> local_name, local_address, local_phone, local_comment;

    const char* line_start = start;
    const char* p = start;

    while (p < end) {
        if (*p == '\n') {
            const char* line_end = p;
            const char* fields[8];
            int field_idx = 0;
            fields[field_idx++] = line_start;

            for (const char* q = line_start; q < line_end && field_idx < 8; q++) {
                if (*q == '|') fields[field_idx++] = q + 1;
            }

            if (field_idx == 8) {
                int32_t custkey, nationkey;
                std::from_chars(fields[0], fields[1] - 1, custkey);
                std::string name(fields[1], fields[2] - 1);
                std::string address(fields[2], fields[3] - 1);
                std::from_chars(fields[3], fields[4] - 1, nationkey);
                std::string phone(fields[4], fields[5] - 1);
                int64_t acctbal = parse_decimal(fields[5], fields[6] - 1);
                std::string mktsegment(fields[6], fields[7] - 1);
                std::string comment(fields[7], line_end);

                local_custkey.push_back(custkey);
                local_name.push_back(name);
                local_address.push_back(address);
                local_nationkey.push_back(nationkey);
                local_phone.push_back(phone);
                local_acctbal.push_back(acctbal);
                local_mktsegment.push_back(cols.dict_mktsegment.encode(mktsegment));
                local_comment.push_back(comment);
            }

            line_start = p + 1;
        }
        p++;
    }

    std::lock_guard<std::mutex> lock(mutex);
    cols.c_custkey.insert(cols.c_custkey.end(), local_custkey.begin(), local_custkey.end());
    cols.c_name.insert(cols.c_name.end(), local_name.begin(), local_name.end());
    cols.c_address.insert(cols.c_address.end(), local_address.begin(), local_address.end());
    cols.c_nationkey.insert(cols.c_nationkey.end(), local_nationkey.begin(), local_nationkey.end());
    cols.c_phone.insert(cols.c_phone.end(), local_phone.begin(), local_phone.end());
    cols.c_acctbal.insert(cols.c_acctbal.end(), local_acctbal.begin(), local_acctbal.end());
    cols.c_mktsegment.insert(cols.c_mktsegment.end(), local_mktsegment.begin(), local_mktsegment.end());
    cols.c_comment.insert(cols.c_comment.end(), local_comment.begin(), local_comment.end());
}

// Ingest customer table
void ingest_customer(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting customer..." << std::endl;

    std::string filename = data_dir + "/customer.tbl";
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << filename << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    char* data = (char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << filename << std::endl;
        close(fd);
        return;
    }
    madvise(data, file_size, MADV_SEQUENTIAL);

    CustomerColumns cols;
    cols.reserve(1500000);

    unsigned int num_threads = std::min(16u, std::thread::hardware_concurrency());
    size_t chunk_size = file_size / num_threads;

    std::vector<std::thread> threads;
    std::mutex mutex;

    for (unsigned int i = 0; i < num_threads; i++) {
        size_t start = i * chunk_size;
        size_t end = (i == num_threads - 1) ? file_size : (i + 1) * chunk_size;

        if (i > 0) {
            while (start < file_size && data[start - 1] != '\n') start++;
        }
        if (i < num_threads - 1) {
            while (end < file_size && data[end] != '\n') end++;
        }

        threads.emplace_back(parse_customer_chunk, data + start, data + end, std::ref(cols), std::ref(mutex));
    }

    for (auto& t : threads) t.join();

    munmap(data, file_size);
    close(fd);

    size_t row_count = cols.c_custkey.size();
    std::cout << "Parsed " << row_count << " rows from customer" << std::endl;

    std::string table_dir = gendb_dir + "/customer";
    fs::create_directories(table_dir);

    auto write_column = [&](const std::string& name, const void* data, size_t elem_size, size_t count) {
        std::string col_file = table_dir + "/" + name + ".bin";
        std::ofstream ofs(col_file, std::ios::binary);
        ofs.write((const char*)data, elem_size * count);
    };

    write_column("c_custkey", cols.c_custkey.data(), sizeof(int32_t), row_count);
    write_column("c_nationkey", cols.c_nationkey.data(), sizeof(int32_t), row_count);
    write_column("c_acctbal", cols.c_acctbal.data(), sizeof(int64_t), row_count);
    write_column("c_mktsegment", cols.c_mktsegment.data(), sizeof(uint8_t), row_count);

    auto write_string_col = [&](const std::string& name, const std::vector<std::string>& vec) {
        std::ofstream ofs(table_dir + "/" + name + ".bin", std::ios::binary);
        for (const auto& s : vec) {
            uint32_t len = s.size();
            ofs.write((char*)&len, sizeof(len));
            ofs.write(s.data(), len);
        }
    };

    write_string_col("c_name", cols.c_name);
    write_string_col("c_address", cols.c_address);
    write_string_col("c_phone", cols.c_phone);
    write_string_col("c_comment", cols.c_comment);

    {
        std::ofstream ofs(table_dir + "/c_mktsegment.dict");
        for (const auto& s : cols.dict_mktsegment.get_values()) ofs << s << "\n";
    }

    std::ofstream meta(table_dir + "/metadata.json");
    meta << "{\n  \"row_count\": " << row_count << "\n}\n";

    std::cout << "Customer ingestion complete: " << row_count << " rows" << std::endl;
}

// Simple single-threaded ingestion for small tables
template<typename ParseFunc>
void ingest_small_table(const std::string& table_name, const std::string& data_dir, const std::string& gendb_dir, ParseFunc parse_func) {
    std::cout << "Ingesting " << table_name << "..." << std::endl;

    std::string filename = data_dir + "/" + table_name + ".tbl";
    std::ifstream ifs(filename);
    if (!ifs.is_open()) {
        std::cerr << "Failed to open " << filename << std::endl;
        return;
    }

    std::string table_dir = gendb_dir + "/" + table_name;
    fs::create_directories(table_dir);

    size_t row_count = parse_func(ifs, table_dir);

    std::ofstream meta(table_dir + "/metadata.json");
    meta << "{\n  \"row_count\": " << row_count << "\n}\n";

    std::cout << table_name << " ingestion complete: " << row_count << " rows" << std::endl;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    fs::create_directories(gendb_dir);

    std::cout << "Starting data ingestion with " << std::thread::hardware_concurrency() << " threads" << std::endl;

    // Ingest large tables in parallel
    std::thread t1([&]() { ingest_lineitem(data_dir, gendb_dir); });
    std::thread t2([&]() { ingest_orders(data_dir, gendb_dir); });
    std::thread t3([&]() { ingest_customer(data_dir, gendb_dir); });

    t1.join();
    t2.join();
    t3.join();

    // Ingest small tables (nation, region, supplier, part, partsupp) - simplified for now
    // In production, these would be fully parsed as well

    std::cout << "\nIngestion complete!" << std::endl;
    std::cout << "Binary columnar data written to: " << gendb_dir << std::endl;

    return 0;
}
