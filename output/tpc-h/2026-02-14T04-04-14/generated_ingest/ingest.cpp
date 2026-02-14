#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <thread>
#include <mutex>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <numeric>
#include <charconv>
#include <queue>
#include <condition_variable>
#include <memory>

// Date encoding: days since epoch (1970-01-01)
int32_t parse_date(const std::string& date_str) {
    if (date_str.length() < 10) return 0;

    int year = 0, month = 0, day = 0;
    std::from_chars(date_str.data(), date_str.data() + 4, year);
    std::from_chars(date_str.data() + 5, date_str.data() + 7, month);
    std::from_chars(date_str.data() + 8, date_str.data() + 10, day);

    // Days since epoch calculation
    int days = (year - 1970) * 365 + (year - 1969) / 4 - (year - 1901) / 100 + (year - 1601) / 400;
    static const int days_in_month[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    days += days_in_month[month - 1];
    if (month > 2 && ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))) days++;
    days += day - 1;

    return days;
}

// Dictionary encoder for low-cardinality strings
class DictionaryEncoder {
public:
    uint8_t encode(const std::string& value) {
        auto it = dict_.find(value);
        if (it == dict_.end()) {
            uint8_t code = dict_.size();
            dict_[value] = code;
            reverse_dict_[code] = value;
            return code;
        }
        return it->second;
    }

    const std::unordered_map<std::string, uint8_t>& get_dict() const { return dict_; }
    const std::map<uint8_t, std::string>& get_reverse_dict() const { return reverse_dict_; }

private:
    std::unordered_map<std::string, uint8_t> dict_;
    std::map<uint8_t, std::string> reverse_dict_;
};

class DictionaryEncoder16 {
public:
    uint16_t encode(const std::string& value) {
        auto it = dict_.find(value);
        if (it == dict_.end()) {
            uint16_t code = dict_.size();
            dict_[value] = code;
            reverse_dict_[code] = value;
            return code;
        }
        return it->second;
    }

    const std::unordered_map<std::string, uint16_t>& get_dict() const { return dict_; }
    const std::map<uint16_t, std::string>& get_reverse_dict() const { return reverse_dict_; }

private:
    std::unordered_map<std::string, uint16_t> dict_;
    std::map<uint16_t, std::string> reverse_dict_;
};

// Global structures for lineitem table
struct LineitemColumns {
    std::vector<int32_t> l_orderkey;
    std::vector<int32_t> l_partkey;
    std::vector<int32_t> l_suppkey;
    std::vector<int32_t> l_linenumber;
    std::vector<double> l_quantity;
    std::vector<double> l_extendedprice;
    std::vector<double> l_discount;
    std::vector<double> l_tax;
    std::vector<uint8_t> l_returnflag;
    std::vector<uint8_t> l_linestatus;
    std::vector<int32_t> l_shipdate;
    std::vector<int32_t> l_commitdate;
    std::vector<int32_t> l_receiptdate;
    std::vector<uint16_t> l_shipinstruct;
    std::vector<uint8_t> l_shipmode;
    std::vector<std::string> l_comment;
};

struct OrdersColumns {
    std::vector<int32_t> o_orderkey;
    std::vector<int32_t> o_custkey;
    std::vector<uint8_t> o_orderstatus;
    std::vector<double> o_totalprice;
    std::vector<int32_t> o_orderdate;
    std::vector<uint8_t> o_orderpriority;
    std::vector<std::string> o_clerk;
    std::vector<int32_t> o_shippriority;
    std::vector<std::string> o_comment;
};

struct CustomerColumns {
    std::vector<int32_t> c_custkey;
    std::vector<std::string> c_name;
    std::vector<std::string> c_address;
    std::vector<int32_t> c_nationkey;
    std::vector<std::string> c_phone;
    std::vector<double> c_acctbal;
    std::vector<uint8_t> c_mktsegment;
    std::vector<std::string> c_comment;
};

struct PartColumns {
    std::vector<int32_t> p_partkey;
    std::vector<std::string> p_name;
    std::vector<uint8_t> p_mfgr;
    std::vector<uint8_t> p_brand;
    std::vector<std::string> p_type;
    std::vector<int32_t> p_size;
    std::vector<uint8_t> p_container;
    std::vector<double> p_retailprice;
    std::vector<std::string> p_comment;
};

struct PartsuppColumns {
    std::vector<int32_t> ps_partkey;
    std::vector<int32_t> ps_suppkey;
    std::vector<int32_t> ps_availqty;
    std::vector<double> ps_supplycost;
    std::vector<std::string> ps_comment;
};

struct SupplierColumns {
    std::vector<int32_t> s_suppkey;
    std::vector<std::string> s_name;
    std::vector<std::string> s_address;
    std::vector<int32_t> s_nationkey;
    std::vector<std::string> s_phone;
    std::vector<double> s_acctbal;
    std::vector<std::string> s_comment;
};

struct NationColumns {
    std::vector<int32_t> n_nationkey;
    std::vector<std::string> n_name;
    std::vector<int32_t> n_regionkey;
    std::vector<std::string> n_comment;
};

struct RegionColumns {
    std::vector<int32_t> r_regionkey;
    std::vector<std::string> r_name;
    std::vector<std::string> r_comment;
};

// Thread pool for parallel ingestion
class ThreadPool {
public:
    ThreadPool(size_t num_threads) : stop_(false) {
        for (size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this] { worker_loop(); });
        }
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& w : workers_) w.join();
    }

    template<typename F>
    void enqueue(F&& f) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            tasks_.emplace(std::forward<F>(f));
        }
        cv_.notify_one();
    }

    void wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return tasks_.empty() && active_ == 0; });
    }

private:
    void worker_loop() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return !tasks_.empty() || stop_; });
                if (stop_ && tasks_.empty()) return;
                if (!tasks_.empty()) {
                    task = std::move(tasks_.front());
                    tasks_.pop();
                    active_++;
                }
            }
            if (task) {
                task();
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                    active_--;
                }
                cv_.notify_all();
            }
        }
    }

    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stop_;
    int active_ = 0;
};

// Parse lineitem.tbl
void ingest_lineitem(const std::string& path, LineitemColumns& cols,
                     DictionaryEncoder& enc_returnflag, DictionaryEncoder& enc_linestatus,
                     DictionaryEncoder16& enc_shipinstruct, DictionaryEncoder& enc_shipmode) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::cerr << "Cannot open " << path << std::endl; return; }

    struct stat sb;
    fstat(fd, &sb);
    char* data = (char*)mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    madvise(data, sb.st_size, MADV_SEQUENTIAL);

    const char* start = data;
    const char* end = data + sb.st_size;

    std::vector<int32_t> perm;

    while (start < end) {
        const char* line_end = start;
        while (line_end < end && *line_end != '\n') line_end++;

        std::string line(start, line_end - start);
        if (!line.empty() && line.back() == '\r') line.pop_back();

        size_t row_idx = cols.l_orderkey.size();
        perm.push_back(row_idx);

        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '|'); cols.l_orderkey.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.l_partkey.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.l_suppkey.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.l_linenumber.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.l_quantity.push_back(std::stod(field));
        std::getline(iss, field, '|'); cols.l_extendedprice.push_back(std::stod(field));
        std::getline(iss, field, '|'); cols.l_discount.push_back(std::stod(field));
        std::getline(iss, field, '|'); cols.l_tax.push_back(std::stod(field));
        std::getline(iss, field, '|'); cols.l_returnflag.push_back(enc_returnflag.encode(field));
        std::getline(iss, field, '|'); cols.l_linestatus.push_back(enc_linestatus.encode(field));
        std::getline(iss, field, '|'); cols.l_shipdate.push_back(parse_date(field));
        std::getline(iss, field, '|'); cols.l_commitdate.push_back(parse_date(field));
        std::getline(iss, field, '|'); cols.l_receiptdate.push_back(parse_date(field));
        std::getline(iss, field, '|'); cols.l_shipinstruct.push_back(enc_shipinstruct.encode(field));
        std::getline(iss, field, '|'); cols.l_shipmode.push_back(enc_shipmode.encode(field));
        std::getline(iss, field, '|'); cols.l_comment.push_back(field);

        start = line_end + 1;
    }

    // Sort by l_shipdate
    std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
        return cols.l_shipdate[a] < cols.l_shipdate[b];
    });

    // Reorder all columns by permutation
    auto reorder = [&](auto& col) {
        auto temp = col;
        for (size_t i = 0; i < perm.size(); i++) col[i] = temp[perm[i]];
    };

    reorder(cols.l_orderkey);
    reorder(cols.l_partkey);
    reorder(cols.l_suppkey);
    reorder(cols.l_linenumber);
    reorder(cols.l_quantity);
    reorder(cols.l_extendedprice);
    reorder(cols.l_discount);
    reorder(cols.l_tax);
    reorder(cols.l_returnflag);
    reorder(cols.l_linestatus);
    reorder(cols.l_shipdate);
    reorder(cols.l_commitdate);
    reorder(cols.l_receiptdate);
    reorder(cols.l_shipinstruct);
    reorder(cols.l_shipmode);
    reorder(cols.l_comment);

    munmap(data, sb.st_size);
    close(fd);
}

// Parse orders.tbl
void ingest_orders(const std::string& path, OrdersColumns& cols,
                   DictionaryEncoder& enc_orderstatus, DictionaryEncoder& enc_orderpriority) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::cerr << "Cannot open " << path << std::endl; return; }

    struct stat sb;
    fstat(fd, &sb);
    char* data = (char*)mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    madvise(data, sb.st_size, MADV_SEQUENTIAL);

    const char* start = data;
    const char* end = data + sb.st_size;

    std::vector<size_t> perm;

    while (start < end) {
        const char* line_end = start;
        while (line_end < end && *line_end != '\n') line_end++;

        std::string line(start, line_end - start);
        if (!line.empty() && line.back() == '\r') line.pop_back();

        size_t row_idx = cols.o_orderkey.size();
        perm.push_back(row_idx);

        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '|'); cols.o_orderkey.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.o_custkey.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.o_orderstatus.push_back(enc_orderstatus.encode(field));
        std::getline(iss, field, '|'); cols.o_totalprice.push_back(std::stod(field));
        std::getline(iss, field, '|'); cols.o_orderdate.push_back(parse_date(field));
        std::getline(iss, field, '|'); cols.o_orderpriority.push_back(enc_orderpriority.encode(field));
        std::getline(iss, field, '|'); cols.o_clerk.push_back(field);
        std::getline(iss, field, '|'); cols.o_shippriority.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.o_comment.push_back(field);

        start = line_end + 1;
    }

    // Sort by o_orderdate
    std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
        return cols.o_orderdate[a] < cols.o_orderdate[b];
    });

    auto reorder = [&](auto& col) {
        auto temp = col;
        for (size_t i = 0; i < perm.size(); i++) col[i] = temp[perm[i]];
    };

    reorder(cols.o_orderkey);
    reorder(cols.o_custkey);
    reorder(cols.o_orderstatus);
    reorder(cols.o_totalprice);
    reorder(cols.o_orderdate);
    reorder(cols.o_orderpriority);
    reorder(cols.o_clerk);
    reorder(cols.o_shippriority);
    reorder(cols.o_comment);

    munmap(data, sb.st_size);
    close(fd);
}

// Parse customer.tbl
void ingest_customer(const std::string& path, CustomerColumns& cols, DictionaryEncoder& enc_mktsegment) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::cerr << "Cannot open " << path << std::endl; return; }

    struct stat sb;
    fstat(fd, &sb);
    char* data = (char*)mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    madvise(data, sb.st_size, MADV_SEQUENTIAL);

    const char* start = data;
    const char* end = data + sb.st_size;

    while (start < end) {
        const char* line_end = start;
        while (line_end < end && *line_end != '\n') line_end++;

        std::string line(start, line_end - start);
        if (!line.empty() && line.back() == '\r') line.pop_back();

        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '|'); cols.c_custkey.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.c_name.push_back(field);
        std::getline(iss, field, '|'); cols.c_address.push_back(field);
        std::getline(iss, field, '|'); cols.c_nationkey.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.c_phone.push_back(field);
        std::getline(iss, field, '|'); cols.c_acctbal.push_back(std::stod(field));
        std::getline(iss, field, '|'); cols.c_mktsegment.push_back(enc_mktsegment.encode(field));
        std::getline(iss, field, '|'); cols.c_comment.push_back(field);

        start = line_end + 1;
    }

    munmap(data, sb.st_size);
    close(fd);
}

// Generic parsers for remaining tables
void ingest_part(const std::string& path, PartColumns& cols,
                 DictionaryEncoder& enc_mfgr, DictionaryEncoder& enc_brand, DictionaryEncoder& enc_container) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::cerr << "Cannot open " << path << std::endl; return; }

    struct stat sb;
    fstat(fd, &sb);
    char* data = (char*)mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    madvise(data, sb.st_size, MADV_SEQUENTIAL);

    const char* start = data;
    const char* end = data + sb.st_size;

    while (start < end) {
        const char* line_end = start;
        while (line_end < end && *line_end != '\n') line_end++;

        std::string line(start, line_end - start);
        if (!line.empty() && line.back() == '\r') line.pop_back();

        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '|'); cols.p_partkey.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.p_name.push_back(field);
        std::getline(iss, field, '|'); cols.p_mfgr.push_back(enc_mfgr.encode(field));
        std::getline(iss, field, '|'); cols.p_brand.push_back(enc_brand.encode(field));
        std::getline(iss, field, '|'); cols.p_type.push_back(field);
        std::getline(iss, field, '|'); cols.p_size.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.p_container.push_back(enc_container.encode(field));
        std::getline(iss, field, '|'); cols.p_retailprice.push_back(std::stod(field));
        std::getline(iss, field, '|'); cols.p_comment.push_back(field);

        start = line_end + 1;
    }

    munmap(data, sb.st_size);
    close(fd);
}

void ingest_partsupp(const std::string& path, PartsuppColumns& cols) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::cerr << "Cannot open " << path << std::endl; return; }

    struct stat sb;
    fstat(fd, &sb);
    char* data = (char*)mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    madvise(data, sb.st_size, MADV_SEQUENTIAL);

    const char* start = data;
    const char* end = data + sb.st_size;

    while (start < end) {
        const char* line_end = start;
        while (line_end < end && *line_end != '\n') line_end++;

        std::string line(start, line_end - start);
        if (!line.empty() && line.back() == '\r') line.pop_back();

        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '|'); cols.ps_partkey.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.ps_suppkey.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.ps_availqty.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.ps_supplycost.push_back(std::stod(field));
        std::getline(iss, field, '|'); cols.ps_comment.push_back(field);

        start = line_end + 1;
    }

    munmap(data, sb.st_size);
    close(fd);
}

void ingest_supplier(const std::string& path, SupplierColumns& cols) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::cerr << "Cannot open " << path << std::endl; return; }

    struct stat sb;
    fstat(fd, &sb);
    char* data = (char*)mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    madvise(data, sb.st_size, MADV_SEQUENTIAL);

    const char* start = data;
    const char* end = data + sb.st_size;

    while (start < end) {
        const char* line_end = start;
        while (line_end < end && *line_end != '\n') line_end++;

        std::string line(start, line_end - start);
        if (!line.empty() && line.back() == '\r') line.pop_back();

        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '|'); cols.s_suppkey.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.s_name.push_back(field);
        std::getline(iss, field, '|'); cols.s_address.push_back(field);
        std::getline(iss, field, '|'); cols.s_nationkey.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.s_phone.push_back(field);
        std::getline(iss, field, '|'); cols.s_acctbal.push_back(std::stod(field));
        std::getline(iss, field, '|'); cols.s_comment.push_back(field);

        start = line_end + 1;
    }

    munmap(data, sb.st_size);
    close(fd);
}

void ingest_nation(const std::string& path, NationColumns& cols) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::cerr << "Cannot open " << path << std::endl; return; }

    struct stat sb;
    fstat(fd, &sb);
    char* data = (char*)mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);

    const char* start = data;
    const char* end = data + sb.st_size;

    while (start < end) {
        const char* line_end = start;
        while (line_end < end && *line_end != '\n') line_end++;

        std::string line(start, line_end - start);
        if (!line.empty() && line.back() == '\r') line.pop_back();

        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '|'); cols.n_nationkey.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.n_name.push_back(field);
        std::getline(iss, field, '|'); cols.n_regionkey.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.n_comment.push_back(field);

        start = line_end + 1;
    }

    munmap(data, sb.st_size);
    close(fd);
}

void ingest_region(const std::string& path, RegionColumns& cols) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::cerr << "Cannot open " << path << std::endl; return; }

    struct stat sb;
    fstat(fd, &sb);
    char* data = (char*)mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);

    const char* start = data;
    const char* end = data + sb.st_size;

    while (start < end) {
        const char* line_end = start;
        while (line_end < end && *line_end != '\n') line_end++;

        std::string line(start, line_end - start);
        if (!line.empty() && line.back() == '\r') line.pop_back();

        std::istringstream iss(line);
        std::string field;

        std::getline(iss, field, '|'); cols.r_regionkey.push_back(std::stoi(field));
        std::getline(iss, field, '|'); cols.r_name.push_back(field);
        std::getline(iss, field, '|'); cols.r_comment.push_back(field);

        start = line_end + 1;
    }

    munmap(data, sb.st_size);
    close(fd);
}

// Write binary column files with buffering
template<typename T>
void write_column(const std::string& filename, const std::vector<T>& data) {
    std::ofstream out(filename, std::ios::binary);
    out.write((const char*)data.data(), data.size() * sizeof(T));
    out.flush();
}

template<>
void write_column<std::string>(const std::string& filename, const std::vector<std::string>& data) {
    std::ofstream out(filename, std::ios::binary);
    uint32_t count = data.size();
    out.write((const char*)&count, sizeof(count));

    std::vector<uint32_t> offsets;
    uint32_t offset = 0;
    for (const auto& s : data) {
        offsets.push_back(offset);
        offset += s.size();
    }

    out.write((const char*)offsets.data(), offsets.size() * sizeof(uint32_t));
    for (const auto& s : data) {
        out.write(s.data(), s.size());
    }
    out.flush();
}

// Create gendb directory if not exists
void ensure_dir(const std::string& path) {
    mkdir(path.c_str(), 0755);
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    ensure_dir(gendb_dir);

    auto start_time = std::chrono::high_resolution_clock::now();

    std::cout << "=== TPC-H SF10 Ingestion ===" << std::endl;

    // Ingest lineitem (hot table, 59.9M rows)
    std::cout << "Ingesting lineitem..." << std::flush;
    auto t0 = std::chrono::high_resolution_clock::now();
    LineitemColumns lineitem_cols;
    DictionaryEncoder enc_l_returnflag, enc_l_linestatus, enc_l_shipmode;
    DictionaryEncoder16 enc_l_shipinstruct;
    ingest_lineitem(data_dir + "/lineitem.tbl", lineitem_cols, enc_l_returnflag, enc_l_linestatus,
                    enc_l_shipinstruct, enc_l_shipmode);
    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << " " << lineitem_cols.l_orderkey.size() << " rows in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Ingest orders (hot table, 15M rows)
    std::cout << "Ingesting orders..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();
    OrdersColumns orders_cols;
    DictionaryEncoder enc_o_orderstatus, enc_o_orderpriority;
    ingest_orders(data_dir + "/orders.tbl", orders_cols, enc_o_orderstatus, enc_o_orderpriority);
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " " << orders_cols.o_orderkey.size() << " rows in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Ingest customer (hot table, 1.5M rows)
    std::cout << "Ingesting customer..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();
    CustomerColumns customer_cols;
    DictionaryEncoder enc_c_mktsegment;
    ingest_customer(data_dir + "/customer.tbl", customer_cols, enc_c_mktsegment);
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " " << customer_cols.c_custkey.size() << " rows in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Ingest part (cold table, 2M rows)
    std::cout << "Ingesting part..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();
    PartColumns part_cols;
    DictionaryEncoder enc_p_mfgr, enc_p_brand, enc_p_container;
    ingest_part(data_dir + "/part.tbl", part_cols, enc_p_mfgr, enc_p_brand, enc_p_container);
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " " << part_cols.p_partkey.size() << " rows in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Ingest partsupp (cold table, 8M rows)
    std::cout << "Ingesting partsupp..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();
    PartsuppColumns partsupp_cols;
    ingest_partsupp(data_dir + "/partsupp.tbl", partsupp_cols);
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " " << partsupp_cols.ps_partkey.size() << " rows in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Ingest supplier (cold table, 100K rows)
    std::cout << "Ingesting supplier..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();
    SupplierColumns supplier_cols;
    ingest_supplier(data_dir + "/supplier.tbl", supplier_cols);
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " " << supplier_cols.s_suppkey.size() << " rows in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Ingest nation (tiny table, 25 rows)
    std::cout << "Ingesting nation..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();
    NationColumns nation_cols;
    ingest_nation(data_dir + "/nation.tbl", nation_cols);
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " " << nation_cols.n_nationkey.size() << " rows in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Ingest region (tiny table, 5 rows)
    std::cout << "Ingesting region..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();
    RegionColumns region_cols;
    ingest_region(data_dir + "/region.tbl", region_cols);
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " " << region_cols.r_regionkey.size() << " rows in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Write lineitem columns
    std::cout << "\nWriting lineitem columns..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();
    write_column(gendb_dir + "/lineitem.l_orderkey.col", lineitem_cols.l_orderkey);
    write_column(gendb_dir + "/lineitem.l_partkey.col", lineitem_cols.l_partkey);
    write_column(gendb_dir + "/lineitem.l_suppkey.col", lineitem_cols.l_suppkey);
    write_column(gendb_dir + "/lineitem.l_linenumber.col", lineitem_cols.l_linenumber);
    write_column(gendb_dir + "/lineitem.l_quantity.col", lineitem_cols.l_quantity);
    write_column(gendb_dir + "/lineitem.l_extendedprice.col", lineitem_cols.l_extendedprice);
    write_column(gendb_dir + "/lineitem.l_discount.col", lineitem_cols.l_discount);
    write_column(gendb_dir + "/lineitem.l_tax.col", lineitem_cols.l_tax);
    write_column(gendb_dir + "/lineitem.l_returnflag.col", lineitem_cols.l_returnflag);
    write_column(gendb_dir + "/lineitem.l_linestatus.col", lineitem_cols.l_linestatus);
    write_column(gendb_dir + "/lineitem.l_shipdate.col", lineitem_cols.l_shipdate);
    write_column(gendb_dir + "/lineitem.l_commitdate.col", lineitem_cols.l_commitdate);
    write_column(gendb_dir + "/lineitem.l_receiptdate.col", lineitem_cols.l_receiptdate);
    write_column(gendb_dir + "/lineitem.l_shipinstruct.col", lineitem_cols.l_shipinstruct);
    write_column(gendb_dir + "/lineitem.l_shipmode.col", lineitem_cols.l_shipmode);
    write_column(gendb_dir + "/lineitem.l_comment.col", lineitem_cols.l_comment);
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " done in " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Write orders columns
    std::cout << "Writing orders columns..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();
    write_column(gendb_dir + "/orders.o_orderkey.col", orders_cols.o_orderkey);
    write_column(gendb_dir + "/orders.o_custkey.col", orders_cols.o_custkey);
    write_column(gendb_dir + "/orders.o_orderstatus.col", orders_cols.o_orderstatus);
    write_column(gendb_dir + "/orders.o_totalprice.col", orders_cols.o_totalprice);
    write_column(gendb_dir + "/orders.o_orderdate.col", orders_cols.o_orderdate);
    write_column(gendb_dir + "/orders.o_orderpriority.col", orders_cols.o_orderpriority);
    write_column(gendb_dir + "/orders.o_clerk.col", orders_cols.o_clerk);
    write_column(gendb_dir + "/orders.o_shippriority.col", orders_cols.o_shippriority);
    write_column(gendb_dir + "/orders.o_comment.col", orders_cols.o_comment);
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " done in " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Write customer columns
    std::cout << "Writing customer columns..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();
    write_column(gendb_dir + "/customer.c_custkey.col", customer_cols.c_custkey);
    write_column(gendb_dir + "/customer.c_name.col", customer_cols.c_name);
    write_column(gendb_dir + "/customer.c_address.col", customer_cols.c_address);
    write_column(gendb_dir + "/customer.c_nationkey.col", customer_cols.c_nationkey);
    write_column(gendb_dir + "/customer.c_phone.col", customer_cols.c_phone);
    write_column(gendb_dir + "/customer.c_acctbal.col", customer_cols.c_acctbal);
    write_column(gendb_dir + "/customer.c_mktsegment.col", customer_cols.c_mktsegment);
    write_column(gendb_dir + "/customer.c_comment.col", customer_cols.c_comment);
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " done in " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Write part columns
    std::cout << "Writing part columns..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();
    write_column(gendb_dir + "/part.p_partkey.col", part_cols.p_partkey);
    write_column(gendb_dir + "/part.p_name.col", part_cols.p_name);
    write_column(gendb_dir + "/part.p_mfgr.col", part_cols.p_mfgr);
    write_column(gendb_dir + "/part.p_brand.col", part_cols.p_brand);
    write_column(gendb_dir + "/part.p_type.col", part_cols.p_type);
    write_column(gendb_dir + "/part.p_size.col", part_cols.p_size);
    write_column(gendb_dir + "/part.p_container.col", part_cols.p_container);
    write_column(gendb_dir + "/part.p_retailprice.col", part_cols.p_retailprice);
    write_column(gendb_dir + "/part.p_comment.col", part_cols.p_comment);
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " done in " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Write partsupp columns
    std::cout << "Writing partsupp columns..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();
    write_column(gendb_dir + "/partsupp.ps_partkey.col", partsupp_cols.ps_partkey);
    write_column(gendb_dir + "/partsupp.ps_suppkey.col", partsupp_cols.ps_suppkey);
    write_column(gendb_dir + "/partsupp.ps_availqty.col", partsupp_cols.ps_availqty);
    write_column(gendb_dir + "/partsupp.ps_supplycost.col", partsupp_cols.ps_supplycost);
    write_column(gendb_dir + "/partsupp.ps_comment.col", partsupp_cols.ps_comment);
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " done in " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Write supplier columns
    std::cout << "Writing supplier columns..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();
    write_column(gendb_dir + "/supplier.s_suppkey.col", supplier_cols.s_suppkey);
    write_column(gendb_dir + "/supplier.s_name.col", supplier_cols.s_name);
    write_column(gendb_dir + "/supplier.s_address.col", supplier_cols.s_address);
    write_column(gendb_dir + "/supplier.s_nationkey.col", supplier_cols.s_nationkey);
    write_column(gendb_dir + "/supplier.s_phone.col", supplier_cols.s_phone);
    write_column(gendb_dir + "/supplier.s_acctbal.col", supplier_cols.s_acctbal);
    write_column(gendb_dir + "/supplier.s_comment.col", supplier_cols.s_comment);
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " done in " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Write nation columns
    std::cout << "Writing nation columns..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();
    write_column(gendb_dir + "/nation.n_nationkey.col", nation_cols.n_nationkey);
    write_column(gendb_dir + "/nation.n_name.col", nation_cols.n_name);
    write_column(gendb_dir + "/nation.n_regionkey.col", nation_cols.n_regionkey);
    write_column(gendb_dir + "/nation.n_comment.col", nation_cols.n_comment);
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " done in " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Write region columns
    std::cout << "Writing region columns..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();
    write_column(gendb_dir + "/region.r_regionkey.col", region_cols.r_regionkey);
    write_column(gendb_dir + "/region.r_name.col", region_cols.r_name);
    write_column(gendb_dir + "/region.r_comment.col", region_cols.r_comment);
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " done in " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    // Write metadata
    std::cout << "\nWriting metadata..." << std::flush;
    t0 = std::chrono::high_resolution_clock::now();

    std::ofstream meta_file(gendb_dir + "/metadata.txt");
    meta_file << "# TPC-H SF10 Metadata\n\n";
    meta_file << "[tables]\n";
    meta_file << "lineitem=" << lineitem_cols.l_orderkey.size() << "\n";
    meta_file << "orders=" << orders_cols.o_orderkey.size() << "\n";
    meta_file << "customer=" << customer_cols.c_custkey.size() << "\n";
    meta_file << "part=" << part_cols.p_partkey.size() << "\n";
    meta_file << "partsupp=" << partsupp_cols.ps_partkey.size() << "\n";
    meta_file << "supplier=" << supplier_cols.s_suppkey.size() << "\n";
    meta_file << "nation=" << nation_cols.n_nationkey.size() << "\n";
    meta_file << "region=" << region_cols.r_regionkey.size() << "\n\n";

    meta_file << "[dictionaries.lineitem.l_returnflag]\n";
    for (const auto& kv : enc_l_returnflag.get_dict()) {
        meta_file << kv.second << "=" << kv.first << "\n";
    }
    meta_file << "\n[dictionaries.lineitem.l_linestatus]\n";
    for (const auto& kv : enc_l_linestatus.get_dict()) {
        meta_file << kv.second << "=" << kv.first << "\n";
    }
    meta_file << "\n[dictionaries.lineitem.l_shipmode]\n";
    for (const auto& kv : enc_l_shipmode.get_dict()) {
        meta_file << kv.second << "=" << kv.first << "\n";
    }
    meta_file << "\n[dictionaries.lineitem.l_shipinstruct]\n";
    for (const auto& kv : enc_l_shipinstruct.get_dict()) {
        meta_file << kv.second << "=" << kv.first << "\n";
    }
    meta_file << "\n[dictionaries.orders.o_orderstatus]\n";
    for (const auto& kv : enc_o_orderstatus.get_dict()) {
        meta_file << kv.second << "=" << kv.first << "\n";
    }
    meta_file << "\n[dictionaries.orders.o_orderpriority]\n";
    for (const auto& kv : enc_o_orderpriority.get_dict()) {
        meta_file << kv.second << "=" << kv.first << "\n";
    }
    meta_file << "\n[dictionaries.customer.c_mktsegment]\n";
    for (const auto& kv : enc_c_mktsegment.get_dict()) {
        meta_file << kv.second << "=" << kv.first << "\n";
    }
    meta_file << "\n[dictionaries.part.p_mfgr]\n";
    for (const auto& kv : enc_p_mfgr.get_dict()) {
        meta_file << kv.second << "=" << kv.first << "\n";
    }
    meta_file << "\n[dictionaries.part.p_brand]\n";
    for (const auto& kv : enc_p_brand.get_dict()) {
        meta_file << kv.second << "=" << kv.first << "\n";
    }
    meta_file << "\n[dictionaries.part.p_container]\n";
    for (const auto& kv : enc_p_container.get_dict()) {
        meta_file << kv.second << "=" << kv.first << "\n";
    }
    meta_file.close();
    t1 = std::chrono::high_resolution_clock::now();
    std::cout << " done in " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;

    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    std::cout << "\n=== Ingestion Complete ===" << std::endl;
    std::cout << "Total time: " << total_ms << "ms (" << (total_ms / 1000.0) << "s)" << std::endl;
    std::cout << "Output directory: " << gendb_dir << std::endl;

    return 0;
}
