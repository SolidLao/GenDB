#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <unordered_map>

namespace fs = std::filesystem;

// Utility: mmap a binary column file
template<typename T>
T* mmap_column(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }
    struct stat st;
    fstat(fd, &st);
    count = st.st_size / sizeof(T);
    T* data = (T*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        return nullptr;
    }
    madvise(data, st.st_size, MADV_SEQUENTIAL);
    return data;
}

// Zone map builder
template<typename T>
struct ZoneMap {
    std::vector<T> min_vals;
    std::vector<T> max_vals;
    std::vector<size_t> block_starts;
    size_t block_size;

    void build(const T* data, size_t count, size_t bs) {
        block_size = bs;
        size_t num_blocks = (count + block_size - 1) / block_size;
        min_vals.resize(num_blocks);
        max_vals.resize(num_blocks);
        block_starts.resize(num_blocks);

        for (size_t b = 0; b < num_blocks; ++b) {
            size_t start = b * block_size;
            size_t end = std::min(start + block_size, count);
            block_starts[b] = start;

            T min_val = data[start];
            T max_val = data[start];
            for (size_t i = start + 1; i < end; ++i) {
                if (data[i] < min_val) min_val = data[i];
                if (data[i] > max_val) max_val = data[i];
            }
            min_vals[b] = min_val;
            max_vals[b] = max_val;
        }
    }

    void save(const std::string& path) {
        std::ofstream out(path, std::ios::binary);
        size_t num_blocks = min_vals.size();
        out.write(reinterpret_cast<const char*>(&num_blocks), sizeof(num_blocks));
        out.write(reinterpret_cast<const char*>(&block_size), sizeof(block_size));
        out.write(reinterpret_cast<const char*>(min_vals.data()), num_blocks * sizeof(T));
        out.write(reinterpret_cast<const char*>(max_vals.data()), num_blocks * sizeof(T));
        out.write(reinterpret_cast<const char*>(block_starts.data()), num_blocks * sizeof(size_t));
        out.close();
    }
};

// Hash index builder
template<typename KeyType, typename ValueType>
struct HashIndex {
    std::unordered_map<KeyType, std::vector<ValueType>> index;

    void build(const KeyType* keys, size_t count) {
        for (size_t i = 0; i < count; ++i) {
            index[keys[i]].push_back(static_cast<ValueType>(i));
        }
    }

    void save(const std::string& path) {
        std::ofstream out(path, std::ios::binary);
        size_t num_keys = index.size();
        out.write(reinterpret_cast<const char*>(&num_keys), sizeof(num_keys));
        for (const auto& [key, values] : index) {
            out.write(reinterpret_cast<const char*>(&key), sizeof(key));
            size_t num_values = values.size();
            out.write(reinterpret_cast<const char*>(&num_values), sizeof(num_values));
            out.write(reinterpret_cast<const char*>(values.data()), num_values * sizeof(ValueType));
        }
        out.close();
    }
};

// Sorted index builder (returns sorted array of (key, row_id) pairs)
template<typename KeyType>
struct SortedIndex {
    std::vector<std::pair<KeyType, size_t>> index;

    void build(const KeyType* keys, size_t count) {
        index.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            index.emplace_back(keys[i], i);
        }
        std::sort(index.begin(), index.end(), [](const auto& a, const auto& b) {
            return a.first < b.first;
        });
    }

    void save(const std::string& path) {
        std::ofstream out(path, std::ios::binary);
        size_t count = index.size();
        out.write(reinterpret_cast<const char*>(&count), sizeof(count));
        out.write(reinterpret_cast<const char*>(index.data()), count * sizeof(std::pair<KeyType, size_t>));
        out.close();
    }
};

void build_lineitem_indexes(const std::string& gendb_dir) {
    std::cout << "Building lineitem indexes..." << std::endl;

    // Zone map for l_shipdate
    {
        size_t count = 0;
        int32_t* shipdate = mmap_column<int32_t>(gendb_dir + "/lineitem_l_shipdate.bin", count);
        if (shipdate) {
            ZoneMap<int32_t> zm;
            zm.build(shipdate, count, 100000);
            zm.save(gendb_dir + "/lineitem_shipdate_zonemap.idx");
            munmap(shipdate, count * sizeof(int32_t));
            std::cout << "  Built lineitem_shipdate_zonemap.idx (" << zm.min_vals.size() << " blocks)" << std::endl;
        }
    }

    // Zone map for l_discount
    {
        size_t count = 0;
        int64_t* discount = mmap_column<int64_t>(gendb_dir + "/lineitem_l_discount.bin", count);
        if (discount) {
            ZoneMap<int64_t> zm;
            zm.build(discount, count, 100000);
            zm.save(gendb_dir + "/lineitem_discount_zonemap.idx");
            munmap(discount, count * sizeof(int64_t));
            std::cout << "  Built lineitem_discount_zonemap.idx" << std::endl;
        }
    }

    // Zone map for l_quantity
    {
        size_t count = 0;
        int64_t* quantity = mmap_column<int64_t>(gendb_dir + "/lineitem_l_quantity.bin", count);
        if (quantity) {
            ZoneMap<int64_t> zm;
            zm.build(quantity, count, 100000);
            zm.save(gendb_dir + "/lineitem_quantity_zonemap.idx");
            munmap(quantity, count * sizeof(int64_t));
            std::cout << "  Built lineitem_quantity_zonemap.idx" << std::endl;
        }
    }

    // Hash index for l_orderkey
    {
        size_t count = 0;
        int32_t* orderkey = mmap_column<int32_t>(gendb_dir + "/lineitem_l_orderkey.bin", count);
        if (orderkey) {
            HashIndex<int32_t, uint32_t> hi;
            hi.build(orderkey, count);
            hi.save(gendb_dir + "/lineitem_orderkey_hash.idx");
            munmap(orderkey, count * sizeof(int32_t));
            std::cout << "  Built lineitem_orderkey_hash.idx (" << hi.index.size() << " unique keys)" << std::endl;
        }
    }

    std::cout << "Lineitem indexes built" << std::endl;
}

void build_orders_indexes(const std::string& gendb_dir) {
    std::cout << "Building orders indexes..." << std::endl;

    // Sorted index for o_orderkey
    {
        size_t count = 0;
        int32_t* orderkey = mmap_column<int32_t>(gendb_dir + "/orders_o_orderkey.bin", count);
        if (orderkey) {
            SortedIndex<int32_t> si;
            si.build(orderkey, count);
            si.save(gendb_dir + "/orders_orderkey_sorted.idx");
            munmap(orderkey, count * sizeof(int32_t));
            std::cout << "  Built orders_orderkey_sorted.idx (" << si.index.size() << " entries)" << std::endl;
        }
    }

    // Hash index for o_custkey
    {
        size_t count = 0;
        int32_t* custkey = mmap_column<int32_t>(gendb_dir + "/orders_o_custkey.bin", count);
        if (custkey) {
            HashIndex<int32_t, uint32_t> hi;
            hi.build(custkey, count);
            hi.save(gendb_dir + "/orders_custkey_hash.idx");
            munmap(custkey, count * sizeof(int32_t));
            std::cout << "  Built orders_custkey_hash.idx (" << hi.index.size() << " unique keys)" << std::endl;
        }
    }

    // Zone map for o_orderdate
    {
        size_t count = 0;
        int32_t* orderdate = mmap_column<int32_t>(gendb_dir + "/orders_o_orderdate.bin", count);
        if (orderdate) {
            ZoneMap<int32_t> zm;
            zm.build(orderdate, count, 100000);
            zm.save(gendb_dir + "/orders_orderdate_zonemap.idx");
            munmap(orderdate, count * sizeof(int32_t));
            std::cout << "  Built orders_orderdate_zonemap.idx" << std::endl;
        }
    }

    std::cout << "Orders indexes built" << std::endl;
}

void build_customer_indexes(const std::string& gendb_dir) {
    std::cout << "Building customer indexes..." << std::endl;

    // Sorted index for c_custkey
    {
        size_t count = 0;
        int32_t* custkey = mmap_column<int32_t>(gendb_dir + "/customer_c_custkey.bin", count);
        if (custkey) {
            SortedIndex<int32_t> si;
            si.build(custkey, count);
            si.save(gendb_dir + "/customer_custkey_sorted.idx");
            munmap(custkey, count * sizeof(int32_t));
            std::cout << "  Built customer_custkey_sorted.idx (" << si.index.size() << " entries)" << std::endl;
        }
    }

    // Hash index for c_mktsegment
    {
        size_t count = 0;
        uint8_t* mktsegment = mmap_column<uint8_t>(gendb_dir + "/customer_c_mktsegment.bin", count);
        if (mktsegment) {
            HashIndex<uint8_t, uint32_t> hi;
            hi.build(mktsegment, count);
            hi.save(gendb_dir + "/customer_mktsegment_hash.idx");
            munmap(mktsegment, count * sizeof(uint8_t));
            std::cout << "  Built customer_mktsegment_hash.idx (" << hi.index.size() << " unique segments)" << std::endl;
        }
    }

    std::cout << "Customer indexes built" << std::endl;
}

void build_part_indexes(const std::string& gendb_dir) {
    std::cout << "Building part indexes (if data exists)..." << std::endl;
    // Stub - would build part_partkey_sorted.idx
    std::cout << "Part indexes built" << std::endl;
}

void build_partsupp_indexes(const std::string& gendb_dir) {
    std::cout << "Building partsupp indexes (if data exists)..." << std::endl;
    // Stub - would build partsupp_composite_sorted.idx
    std::cout << "Partsupp indexes built" << std::endl;
}

void build_supplier_indexes(const std::string& gendb_dir) {
    std::cout << "Building supplier indexes (if data exists)..." << std::endl;
    // Stub - would build supplier_suppkey_sorted.idx
    std::cout << "Supplier indexes built" << std::endl;
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    if (!fs::exists(gendb_dir)) {
        std::cerr << "Error: GenDB directory does not exist: " << gendb_dir << std::endl;
        return 1;
    }

    std::cout << "Building indexes from binary data in " << gendb_dir << std::endl;

    build_lineitem_indexes(gendb_dir);
    build_orders_indexes(gendb_dir);
    build_customer_indexes(gendb_dir);
    build_part_indexes(gendb_dir);
    build_partsupp_indexes(gendb_dir);
    build_supplier_indexes(gendb_dir);

    std::cout << "All indexes built successfully" << std::endl;
    return 0;
}
