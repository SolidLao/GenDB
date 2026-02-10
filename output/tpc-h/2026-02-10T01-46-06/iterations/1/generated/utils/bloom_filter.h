#pragma once

#include <vector>
#include <cstdint>
#include <cmath>

// Simple Bloom Filter for join optimization
class BloomFilter {
private:
    std::vector<uint64_t> bits;
    size_t num_bits;
    size_t k;  // Number of hash functions

    // MurmurHash-inspired hash combination
    inline uint64_t hash_combine(uint64_t hash, size_t seed) const {
        hash ^= seed + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
        return hash;
    }

public:
    // Constructor: n = expected elements, fpr = false positive rate
    BloomFilter(size_t n, double fpr = 0.01) {
        // Optimal size: m = -n * ln(fpr) / (ln(2)^2)
        num_bits = std::ceil(-n * std::log(fpr) / (std::log(2) * std::log(2)));
        bits.resize((num_bits + 63) / 64, 0);

        // Optimal k: k = (m / n) * ln(2)
        k = std::max(size_t(1), size_t(std::round((num_bits / double(n)) * std::log(2))));
        k = std::min(k, size_t(7));  // Cap at 7 for performance
    }

    void insert(int32_t key) {
        uint64_t hash = static_cast<uint64_t>(key) * 0x9e3779b97f4a7c15ULL;
        for (size_t i = 0; i < k; i++) {
            uint64_t h = hash_combine(hash, i);
            size_t bit_idx = h % num_bits;
            bits[bit_idx / 64] |= (1ULL << (bit_idx % 64));
        }
    }

    bool contains(int32_t key) const {
        uint64_t hash = static_cast<uint64_t>(key) * 0x9e3779b97f4a7c15ULL;
        for (size_t i = 0; i < k; i++) {
            uint64_t h = hash_combine(hash, i);
            size_t bit_idx = h % num_bits;
            if (!(bits[bit_idx / 64] & (1ULL << (bit_idx % 64))))
                return false;
        }
        return true;
    }

    size_t size_bytes() const {
        return bits.size() * sizeof(uint64_t);
    }
};
