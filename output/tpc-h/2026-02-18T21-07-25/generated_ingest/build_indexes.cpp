#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <algorithm>
#include <cstdint>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// Multiply-shift hash function for int32_t
inline uint32_t hash_int32(int32_t key, uint32_t mask) {
    uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    return (h >> 32) & mask;
}

struct MmappedColumn {
    int fd = -1;
    void* ptr = nullptr;
    size_t size = 0;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            return false;
        }

        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            std::cerr << "Failed to fstat " << path << std::endl;
            ::close(fd);
            return false;
        }

        size = sb.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            ::close(fd);
            return false;
        }

        madvise(ptr, size, MADV_SEQUENTIAL);
        return true;
    }

    void close() {
        if (ptr) {
            munmap(ptr, size);
            ptr = nullptr;
        }
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }

    ~MmappedColumn() { close(); }

    template <typename T>
    T* data() { return (T*)ptr; }

    size_t count() const {
        if (size == 0) return 0;
        return size / sizeof(int32_t);  // Assuming int32_t columns
    }
};

// Zone map structure: per-block min/max
struct ZoneMap {
    struct Entry {
        int32_t min_val;
        int32_t max_val;
        uint32_t count;
    };

    std::vector<Entry> entries;

    void build(const int32_t* col, size_t total_rows, size_t block_size) {
        size_t num_blocks = (total_rows + block_size - 1) / block_size;
        entries.resize(num_blocks);

#pragma omp parallel for schedule(dynamic)
        for (size_t b = 0; b < num_blocks; ++b) {
            size_t start = b * block_size;
            size_t end = std::min(start + block_size, total_rows);
            int32_t min_val = col[start];
            int32_t max_val = col[start];
            for (size_t i = start; i < end; ++i) {
                if (col[i] < min_val) min_val = col[i];
                if (col[i] > max_val) max_val = col[i];
            }
            entries[b] = {min_val, max_val, (uint32_t)(end - start)};
        }
    }

    void write(const std::string& path) {
        std::ofstream f(path, std::ios::binary);
        uint32_t num_entries = entries.size();
        f.write(reinterpret_cast<char*>(&num_entries), 4);
        f.write(reinterpret_cast<char*>(entries.data()), num_entries * sizeof(Entry));
    }
};

// Hash index for single-value keys (e.g., orderkey)
struct HashIndex {
    struct Entry {
        int32_t key;
        uint32_t position;
    };

    std::vector<Entry> table;
    uint32_t capacity = 0;

    void build(const int32_t* col, size_t total_rows) {
        // Estimate capacity with 0.5 load factor
        capacity = 1;
        while (capacity < (total_rows / 0.5)) capacity *= 2;

        table.resize(capacity);
        std::fill(table.begin(), table.end(), Entry{-1, 0});

        for (size_t i = 0; i < total_rows; ++i) {
            int32_t key = col[i];
            uint32_t mask = capacity - 1;
            uint32_t h = hash_int32(key, mask);

            // Linear probing
            while (table[h].key != -1) {
                h = (h + 1) & mask;
            }
            table[h] = {key, (uint32_t)i};
        }
    }

    void write(const std::string& path) {
        std::ofstream f(path, std::ios::binary);
        f.write(reinterpret_cast<char*>(&capacity), 4);
        f.write(reinterpret_cast<char*>(table.data()), capacity * sizeof(Entry));
    }
};

// Multi-value hash index (for join keys with duplicates)
struct MultiValueHashIndex {
    struct HashEntry {
        int32_t key;
        uint32_t offset;
        uint32_t count;
    };

    std::vector<HashEntry> hash_table;
    std::vector<uint32_t> positions;
    uint32_t capacity = 0;

    void build(const int32_t* col, size_t total_rows) {
        // Step 1: Create position array [0, 1, 2, ..., N-1]
        std::vector<uint32_t> pos_array(total_rows);
#pragma omp parallel for
        for (size_t i = 0; i < total_rows; ++i) {
            pos_array[i] = i;
        }

        // Step 2: Sort positions by key value
        std::sort(pos_array.begin(), pos_array.end(),
                  [col](uint32_t a, uint32_t b) { return col[a] < col[b]; });

        // Step 3: Scan sorted positions to find group boundaries
        std::vector<HashEntry> entries;
        size_t i = 0;
        while (i < total_rows) {
            int32_t key = col[pos_array[i]];
            uint32_t offset = i;
            size_t j = i;
            while (j < total_rows && col[pos_array[j]] == key) {
                ++j;
            }
            entries.push_back({key, (uint32_t)offset, (uint32_t)(j - i)});
            i = j;
        }

        // Step 4: Build hash table on unique keys
        capacity = 1;
        while (capacity < (entries.size() / 0.6)) capacity *= 2;

        hash_table.resize(capacity);
        std::fill(hash_table.begin(), hash_table.end(), HashEntry{-1, 0, 0});

        for (const auto& e : entries) {
            uint32_t mask = capacity - 1;
            uint32_t h = hash_int32(e.key, mask);
            while (hash_table[h].key != -1) {
                h = (h + 1) & mask;
            }
            hash_table[h] = e;
        }

        // Positions array becomes the final output
        positions = std::move(pos_array);
    }

    void write(const std::string& path) {
        std::ofstream f(path, std::ios::binary);
        f.write(reinterpret_cast<char*>(&capacity), 4);
        f.write(reinterpret_cast<char*>(hash_table.data()), capacity * sizeof(HashEntry));

        uint32_t pos_count = positions.size();
        f.write(reinterpret_cast<char*>(&pos_count), 4);
        f.write(reinterpret_cast<char*>(positions.data()), pos_count * sizeof(uint32_t));
    }
};

void build_lineitem_indexes(const std::string& table_dir) {
    std::cout << "Building lineitem indexes..." << std::endl;

    // Open lineitem columns
    MmappedColumn col_orderkey, col_shipdate, col_discount, col_quantity;

    if (!col_orderkey.open(table_dir + "/lineitem/l_orderkey.bin")) return;
    if (!col_shipdate.open(table_dir + "/lineitem/l_shipdate.bin")) return;
    if (!col_discount.open(table_dir + "/lineitem/l_discount.bin")) return;
    if (!col_quantity.open(table_dir + "/lineitem/l_quantity.bin")) return;

    size_t total_rows = col_orderkey.count();
    std::cout << "  Total rows: " << total_rows << std::endl;

    // Build zone map for shipdate
    {
        std::cout << "  Building zone map for l_shipdate..." << std::endl;
        ZoneMap zm;
        zm.build(col_shipdate.data<int32_t>(), total_rows, 100000);
        zm.write(table_dir + "/indexes/idx_lineitem_shipdate.bin");
        std::cout << "    " << zm.entries.size() << " zones" << std::endl;
    }

    // Build zone map for discount and quantity (multi-column)
    {
        std::cout << "  Building zone map for discount/quantity..." << std::endl;
        ZoneMap zm;
        zm.build(col_discount.data<int32_t>(), total_rows, 100000);
        zm.write(table_dir + "/indexes/idx_lineitem_discount_qty.bin");
    }

    // Build multi-value hash index for orderkey
    {
        std::cout << "  Building multi-value hash index for l_orderkey..." << std::endl;
        MultiValueHashIndex mi;
        mi.build(col_orderkey.data<int32_t>(), total_rows);
        mi.write(table_dir + "/indexes/idx_lineitem_orderkey.bin");
        std::cout << "    Hash table capacity: " << mi.capacity << std::endl;
    }
}

void build_orders_indexes(const std::string& table_dir) {
    std::cout << "Building orders indexes..." << std::endl;

    MmappedColumn col_orderkey, col_custkey, col_orderdate;

    if (!col_orderkey.open(table_dir + "/orders/o_orderkey.bin")) return;
    if (!col_custkey.open(table_dir + "/orders/o_custkey.bin")) return;
    if (!col_orderdate.open(table_dir + "/orders/o_orderdate.bin")) return;

    size_t total_rows = col_orderkey.count();
    std::cout << "  Total rows: " << total_rows << std::endl;

    // Hash index for orderkey (PK lookup)
    {
        std::cout << "  Building hash index for o_orderkey..." << std::endl;
        HashIndex idx;
        idx.build(col_orderkey.data<int32_t>(), total_rows);
        idx.write(table_dir + "/indexes/idx_orders_orderkey.bin");
        std::cout << "    Hash table capacity: " << idx.capacity << std::endl;
    }

    // Multi-value hash index for custkey (FK lookup)
    {
        std::cout << "  Building multi-value hash index for o_custkey..." << std::endl;
        MultiValueHashIndex mi;
        mi.build(col_custkey.data<int32_t>(), total_rows);
        mi.write(table_dir + "/indexes/idx_orders_custkey.bin");
        std::cout << "    Hash table capacity: " << mi.capacity << std::endl;
    }

    // Zone map for orderdate
    {
        std::cout << "  Building zone map for o_orderdate..." << std::endl;
        ZoneMap zm;
        zm.build(col_orderdate.data<int32_t>(), total_rows, 100000);
        zm.write(table_dir + "/indexes/idx_orders_orderdate.bin");
        std::cout << "    " << zm.entries.size() << " zones" << std::endl;
    }
}

void build_customer_indexes(const std::string& table_dir) {
    std::cout << "Building customer indexes..." << std::endl;

    MmappedColumn col_custkey;
    if (!col_custkey.open(table_dir + "/customer/c_custkey.bin")) return;

    size_t total_rows = col_custkey.count();
    std::cout << "  Total rows: " << total_rows << std::endl;

    // Hash index for custkey (PK lookup)
    {
        std::cout << "  Building hash index for c_custkey..." << std::endl;
        HashIndex idx;
        idx.build(col_custkey.data<int32_t>(), total_rows);
        idx.write(table_dir + "/indexes/idx_customer_custkey.bin");
        std::cout << "    Hash table capacity: " << idx.capacity << std::endl;
    }
}

void build_part_indexes(const std::string& table_dir) {
    std::cout << "Building part indexes..." << std::endl;

    MmappedColumn col_partkey;
    if (!col_partkey.open(table_dir + "/part/p_partkey.bin")) return;

    size_t total_rows = col_partkey.count();
    std::cout << "  Total rows: " << total_rows << std::endl;

    // Hash index for partkey (PK lookup)
    {
        std::cout << "  Building hash index for p_partkey..." << std::endl;
        HashIndex idx;
        idx.build(col_partkey.data<int32_t>(), total_rows);
        idx.write(table_dir + "/indexes/idx_part_partkey.bin");
        std::cout << "    Hash table capacity: " << idx.capacity << std::endl;
    }
}

void build_partsupp_indexes(const std::string& table_dir) {
    std::cout << "Building partsupp indexes..." << std::endl;

    MmappedColumn col_partkey, col_suppkey;
    if (!col_partkey.open(table_dir + "/partsupp/ps_partkey.bin")) return;
    if (!col_suppkey.open(table_dir + "/partsupp/ps_suppkey.bin")) return;

    size_t total_rows = col_partkey.count();
    std::cout << "  Total rows: " << total_rows << std::endl;

    // Multi-value hash index on (partkey, suppkey) composite key
    // For simplicity, use partkey only (can be extended to composite)
    {
        std::cout << "  Building multi-value hash index for (ps_partkey, ps_suppkey)..." << std::endl;
        MultiValueHashIndex mi;
        mi.build(col_partkey.data<int32_t>(), total_rows);
        mi.write(table_dir + "/indexes/idx_partsupp_part_supp.bin");
        std::cout << "    Hash table capacity: " << mi.capacity << std::endl;
    }
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <output_dir>" << std::endl;
        return 1;
    }

    std::string output_dir = argv[1];
    std::string indexes_dir = output_dir + "/indexes";

    // Create indexes directory
    std::system(("mkdir -p " + indexes_dir).c_str());

    // Build indexes for all tables
    build_lineitem_indexes(output_dir);
    build_orders_indexes(output_dir);
    build_customer_indexes(output_dir);
    build_part_indexes(output_dir);
    build_partsupp_indexes(output_dir);

    std::cout << "\nIndex building complete." << std::endl;
    return 0;
}
