#pragma once

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <utility>
#include <vector>
#include <immintrin.h>

// Lightweight flat hash map implementation inspired by Swiss Tables
// Open addressing with linear probing and SIMD-accelerated lookups
// 2-5x faster than std::unordered_map due to cache locality

namespace gendb {
namespace flat_hash {

// Control byte values
constexpr int8_t kEmpty = -128;  // 0b10000000
constexpr int8_t kDeleted = -2;  // 0b11111110

// Group size for SIMD operations (16 bytes for SSE2)
constexpr size_t kGroupSize = 16;

// Hash mixer (MurmurHash3 finalizer)
inline size_t mix_hash(size_t h) {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

// Extract H2 (7-bit hash) from full hash
inline int8_t h2(size_t hash) {
    return static_cast<int8_t>(hash & 0x7F);
}

// SIMD-accelerated probe for matching control bytes
// Returns bitmask of matching positions
inline uint32_t match_byte(__m128i ctrl, int8_t target) {
    __m128i target_vec = _mm_set1_epi8(target);
    __m128i cmp = _mm_cmpeq_epi8(ctrl, target_vec);
    return _mm_movemask_epi8(cmp);
}

// Flat hash set for int32_t (used for filtered_customers in Q3)
template<typename Hash = std::hash<int32_t>>
class flat_hash_set {
private:
    struct Slot {
        int32_t key;
    };

    std::vector<int8_t> ctrl_;    // Control bytes (1 per slot)
    std::vector<Slot> slots_;      // Data slots
    size_t size_ = 0;
    size_t capacity_ = 0;
    Hash hasher_;

    // Load factor: resize at 87.5% full (7/8)
    size_t max_size() const { return (capacity_ * 7) / 8; }

    void resize(size_t new_capacity) {
        auto old_ctrl = std::move(ctrl_);
        auto old_slots = std::move(slots_);
        size_t old_capacity = capacity_;

        capacity_ = new_capacity;
        ctrl_.assign(capacity_ + kGroupSize, kEmpty);
        slots_.resize(capacity_);
        size_ = 0;

        // Rehash all elements
        for (size_t i = 0; i < old_capacity; i++) {
            if (old_ctrl[i] >= 0) {
                insert(old_slots[i].key);
            }
        }
    }

public:
    flat_hash_set() : capacity_(0) {}

    explicit flat_hash_set(size_t reserve_size) : capacity_(0) {
        if (reserve_size > 0) {
            reserve(reserve_size);
        }
    }

    void reserve(size_t n) {
        // Reserve at least n / 0.875 capacity to avoid resize
        size_t needed = (n * 8 + 6) / 7;
        if (needed > capacity_) {
            // Round up to power of 2 for fast modulo
            size_t new_cap = 16;
            while (new_cap < needed) new_cap *= 2;
            resize(new_cap);
        }
    }

    bool insert(int32_t key) {
        if (size_ >= max_size()) {
            resize(capacity_ == 0 ? 16 : capacity_ * 2);
        }

        size_t hash = mix_hash(hasher_(key));
        int8_t h2_val = h2(hash);
        size_t pos = hash & (capacity_ - 1);

        // Linear probe with SIMD matching
        while (true) {
            // Load control group
            __m128i ctrl_group = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&ctrl_[pos]));

            // Check for existing key (matching h2)
            uint32_t match = match_byte(ctrl_group, h2_val);
            while (match != 0) {
                size_t idx = __builtin_ctz(match);
                size_t slot_pos = (pos + idx) & (capacity_ - 1);
                if (slots_[slot_pos].key == key) {
                    return false;  // Already exists
                }
                match &= (match - 1);  // Clear lowest bit
            }

            // Check for empty slot
            uint32_t empty = match_byte(ctrl_group, kEmpty);
            if (empty != 0) {
                size_t idx = __builtin_ctz(empty);
                size_t slot_pos = (pos + idx) & (capacity_ - 1);
                ctrl_[slot_pos] = h2_val;
                slots_[slot_pos].key = key;
                size_++;
                return true;
            }

            // Move to next group
            pos = (pos + kGroupSize) & (capacity_ - 1);
        }
    }

    bool count(int32_t key) const {
        if (capacity_ == 0) return false;

        size_t hash = mix_hash(hasher_(key));
        int8_t h2_val = h2(hash);
        size_t pos = hash & (capacity_ - 1);

        // Linear probe with SIMD matching
        while (true) {
            __m128i ctrl_group = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&ctrl_[pos]));

            // Check for matching h2
            uint32_t match = match_byte(ctrl_group, h2_val);
            while (match != 0) {
                size_t idx = __builtin_ctz(match);
                size_t slot_pos = (pos + idx) & (capacity_ - 1);
                if (slots_[slot_pos].key == key) {
                    return true;
                }
                match &= (match - 1);
            }

            // Check for empty slot (not found)
            uint32_t empty = match_byte(ctrl_group, kEmpty);
            if (empty != 0) {
                return false;
            }

            pos = (pos + kGroupSize) & (capacity_ - 1);
        }
    }

    size_t size() const { return size_; }
};

// Flat hash map for KeyType -> T (generic, supports compound keys)
template<typename KeyType, typename T, typename Hash = std::hash<KeyType>>
class flat_hash_map {
private:
    struct Slot {
        KeyType key;
        T value;
    };

    std::vector<int8_t> ctrl_;
    std::vector<Slot> slots_;
    size_t size_ = 0;
    size_t capacity_ = 0;
    Hash hasher_;

    size_t max_size() const { return (capacity_ * 7) / 8; }

    void resize(size_t new_capacity) {
        auto old_ctrl = std::move(ctrl_);
        auto old_slots = std::move(slots_);
        size_t old_capacity = capacity_;

        capacity_ = new_capacity;
        ctrl_.assign(capacity_ + kGroupSize, kEmpty);
        slots_.resize(capacity_);
        size_ = 0;

        for (size_t i = 0; i < old_capacity; i++) {
            if (old_ctrl[i] >= 0) {
                insert_internal(old_slots[i].key, std::move(old_slots[i].value));
            }
        }
    }

    void insert_internal(const KeyType& key, T&& value) {
        size_t hash = mix_hash(hasher_(key));
        int8_t h2_val = h2(hash);
        size_t pos = hash & (capacity_ - 1);

        while (true) {
            __m128i ctrl_group = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&ctrl_[pos]));

            uint32_t empty = match_byte(ctrl_group, kEmpty);
            if (empty != 0) {
                size_t idx = __builtin_ctz(empty);
                size_t slot_pos = (pos + idx) & (capacity_ - 1);
                ctrl_[slot_pos] = h2_val;
                slots_[slot_pos].key = key;
                slots_[slot_pos].value = std::move(value);
                size_++;
                return;
            }

            pos = (pos + kGroupSize) & (capacity_ - 1);
        }
    }

public:
    flat_hash_map() : capacity_(0) {}

    explicit flat_hash_map(size_t reserve_size) : capacity_(0) {
        if (reserve_size > 0) {
            reserve(reserve_size);
        }
    }

    void reserve(size_t n) {
        size_t needed = (n * 8 + 6) / 7;
        if (needed > capacity_) {
            size_t new_cap = 16;
            while (new_cap < needed) new_cap *= 2;
            resize(new_cap);
        }
    }

    T& operator[](const KeyType& key) {
        if (size_ >= max_size()) {
            resize(capacity_ == 0 ? 16 : capacity_ * 2);
        }

        size_t hash = mix_hash(hasher_(key));
        int8_t h2_val = h2(hash);
        size_t pos = hash & (capacity_ - 1);

        while (true) {
            __m128i ctrl_group = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&ctrl_[pos]));

            uint32_t match = match_byte(ctrl_group, h2_val);
            while (match != 0) {
                size_t idx = __builtin_ctz(match);
                size_t slot_pos = (pos + idx) & (capacity_ - 1);
                if (slots_[slot_pos].key == key) {
                    return slots_[slot_pos].value;
                }
                match &= (match - 1);
            }

            uint32_t empty = match_byte(ctrl_group, kEmpty);
            if (empty != 0) {
                size_t idx = __builtin_ctz(empty);
                size_t slot_pos = (pos + idx) & (capacity_ - 1);
                ctrl_[slot_pos] = h2_val;
                slots_[slot_pos].key = key;
                slots_[slot_pos].value = T{};
                size_++;
                return slots_[slot_pos].value;
            }

            pos = (pos + kGroupSize) & (capacity_ - 1);
        }
    }

    const T* find(const KeyType& key) const {
        if (capacity_ == 0) return nullptr;

        size_t hash = mix_hash(hasher_(key));
        int8_t h2_val = h2(hash);
        size_t pos = hash & (capacity_ - 1);

        while (true) {
            __m128i ctrl_group = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&ctrl_[pos]));

            uint32_t match = match_byte(ctrl_group, h2_val);
            while (match != 0) {
                size_t idx = __builtin_ctz(match);
                size_t slot_pos = (pos + idx) & (capacity_ - 1);
                if (slots_[slot_pos].key == key) {
                    return &slots_[slot_pos].value;
                }
                match &= (match - 1);
            }

            uint32_t empty = match_byte(ctrl_group, kEmpty);
            if (empty != 0) {
                return nullptr;
            }

            pos = (pos + kGroupSize) & (capacity_ - 1);
        }
    }

    size_t size() const { return size_; }

    // Iterator support for range-based loops
    class iterator {
    private:
        const flat_hash_map* map_;
        size_t pos_;

        void advance() {
            while (pos_ < map_->capacity_ && map_->ctrl_[pos_] < 0) {
                pos_++;
            }
        }

    public:
        // Iterator traits for STL compatibility
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::pair<KeyType, T>;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = value_type;

        iterator(const flat_hash_map* map, size_t pos) : map_(map), pos_(pos) {
            advance();
        }

        value_type operator*() const {
            return {map_->slots_[pos_].key, map_->slots_[pos_].value};
        }

        iterator& operator++() {
            pos_++;
            advance();
            return *this;
        }

        iterator operator++(int) {
            iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        bool operator==(const iterator& other) const {
            return pos_ == other.pos_;
        }

        bool operator!=(const iterator& other) const {
            return pos_ != other.pos_;
        }
    };

    iterator begin() const { return iterator(this, 0); }
    iterator end() const { return iterator(this, capacity_); }
};

// Convenience alias for int32_t keys (backward compatibility)
template<typename T>
using flat_hash_map_int32 = flat_hash_map<int32_t, T, std::hash<int32_t>>;

} // namespace flat_hash
} // namespace gendb
