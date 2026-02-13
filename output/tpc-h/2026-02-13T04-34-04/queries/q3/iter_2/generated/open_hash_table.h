#ifndef OPEN_HASH_TABLE_H
#define OPEN_HASH_TABLE_H

#include <cstdint>
#include <cstring>
#include <vector>
#include <utility>

// MurmurHash3 finalizer for mixing bits
inline uint32_t murmur3_finalize(uint32_t h) {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

// Open-addressing hash set for int32_t keys
// Uses linear probing with power-of-2 sizing for fast modulo
class OpenHashSet {
private:
    static constexpr int32_t EMPTY = 0x7FFFFFFF;  // Use max int32 as sentinel
    static constexpr int32_t DELETED = 0x7FFFFFFE;

    std::vector<int32_t> keys_;
    size_t size_;
    size_t capacity_;
    size_t mask_;

    void rehash(size_t new_capacity) {
        std::vector<int32_t> old_keys = std::move(keys_);
        size_t old_capacity = capacity_;

        capacity_ = new_capacity;
        mask_ = capacity_ - 1;
        keys_.resize(capacity_, EMPTY);
        size_ = 0;

        for (size_t i = 0; i < old_capacity; i++) {
            if (old_keys[i] != EMPTY && old_keys[i] != DELETED) {
                insert(old_keys[i]);
            }
        }
    }

public:
    OpenHashSet() : size_(0), capacity_(16), mask_(15) {
        keys_.resize(capacity_, EMPTY);
    }

    explicit OpenHashSet(size_t reserve_size) : size_(0) {
        // Round up to next power of 2, with load factor 0.7
        size_t target = static_cast<size_t>(reserve_size / 0.7);
        capacity_ = 16;
        while (capacity_ < target) capacity_ *= 2;
        mask_ = capacity_ - 1;
        keys_.resize(capacity_, EMPTY);
    }

    void insert(int32_t key) {
        if (key == EMPTY || key == DELETED) return;  // Cannot insert sentinel values

        // Resize if load factor exceeds 0.7
        if (size_ * 10 >= capacity_ * 7) {
            rehash(capacity_ * 2);
        }

        uint32_t hash = murmur3_finalize(static_cast<uint32_t>(key));
        size_t idx = hash & mask_;

        // Linear probing
        while (keys_[idx] != EMPTY && keys_[idx] != key) {
            idx = (idx + 1) & mask_;
        }

        if (keys_[idx] == EMPTY) {
            keys_[idx] = key;
            size_++;
        }
    }

    bool find(int32_t key) const {
        if (key == EMPTY || key == DELETED) return false;

        uint32_t hash = murmur3_finalize(static_cast<uint32_t>(key));
        size_t idx = hash & mask_;

        // Linear probing
        while (keys_[idx] != EMPTY) {
            if (keys_[idx] == key) return true;
            idx = (idx + 1) & mask_;
        }

        return false;
    }

    size_t size() const { return size_; }
};

// Open-addressing hash map for int32_t -> T
// Uses linear probing with power-of-2 sizing
template<typename T>
class OpenHashMap {
public:
    static constexpr int32_t EMPTY = 0x7FFFFFFF;
    static constexpr int32_t DELETED = 0x7FFFFFFE;

    struct Entry {
        int32_t key;
        T value;
    };

    std::vector<Entry> entries_;
    size_t size_;
    size_t capacity_;
    size_t mask_;

private:

    void rehash(size_t new_capacity) {
        std::vector<Entry> old_entries = std::move(entries_);
        size_t old_capacity = capacity_;

        capacity_ = new_capacity;
        mask_ = capacity_ - 1;
        entries_.resize(capacity_);
        for (auto& e : entries_) {
            e.key = EMPTY;
        }
        size_ = 0;

        for (size_t i = 0; i < old_capacity; i++) {
            if (old_entries[i].key != EMPTY && old_entries[i].key != DELETED) {
                insert_internal(old_entries[i].key, std::move(old_entries[i].value));
            }
        }
    }

    void insert_internal(int32_t key, T&& value) {
        uint32_t hash = murmur3_finalize(static_cast<uint32_t>(key));
        size_t idx = hash & mask_;

        while (entries_[idx].key != EMPTY && entries_[idx].key != key) {
            idx = (idx + 1) & mask_;
        }

        if (entries_[idx].key == EMPTY) {
            entries_[idx].key = key;
            entries_[idx].value = std::move(value);
            size_++;
        } else {
            entries_[idx].value = std::move(value);
        }
    }

public:
    OpenHashMap() : size_(0), capacity_(16), mask_(15) {
        entries_.resize(capacity_);
        for (auto& e : entries_) {
            e.key = EMPTY;
        }
    }

    explicit OpenHashMap(size_t reserve_size) : size_(0) {
        size_t target = static_cast<size_t>(reserve_size / 0.7);
        capacity_ = 16;
        while (capacity_ < target) capacity_ *= 2;
        mask_ = capacity_ - 1;
        entries_.resize(capacity_);
        for (auto& e : entries_) {
            e.key = EMPTY;
        }
    }

    T* find(int32_t key) {
        if (key == EMPTY || key == DELETED) return nullptr;

        uint32_t hash = murmur3_finalize(static_cast<uint32_t>(key));
        size_t idx = hash & mask_;

        while (entries_[idx].key != EMPTY) {
            if (entries_[idx].key == key) return &entries_[idx].value;
            idx = (idx + 1) & mask_;
        }

        return nullptr;
    }

    const T* find(int32_t key) const {
        if (key == EMPTY || key == DELETED) return nullptr;

        uint32_t hash = murmur3_finalize(static_cast<uint32_t>(key));
        size_t idx = hash & mask_;

        while (entries_[idx].key != EMPTY) {
            if (entries_[idx].key == key) return &entries_[idx].value;
            idx = (idx + 1) & mask_;
        }

        return nullptr;
    }

    T& operator[](int32_t key) {
        if (key == EMPTY || key == DELETED) {
            static T dummy;
            return dummy;
        }

        // Resize if load factor exceeds 0.7
        if (size_ * 10 >= capacity_ * 7) {
            rehash(capacity_ * 2);
        }

        uint32_t hash = murmur3_finalize(static_cast<uint32_t>(key));
        size_t idx = hash & mask_;

        while (entries_[idx].key != EMPTY && entries_[idx].key != key) {
            idx = (idx + 1) & mask_;
        }

        if (entries_[idx].key == EMPTY) {
            entries_[idx].key = key;
            entries_[idx].value = T();
            size_++;
        }

        return entries_[idx].value;
    }

    size_t size() const { return size_; }

    // Iterator support for range-based for loops
    class iterator {
    private:
        Entry* ptr_;
        Entry* end_;

        void advance() {
            while (ptr_ < end_ && (ptr_->key == EMPTY || ptr_->key == DELETED)) {
                ptr_++;
            }
        }

    public:
        iterator(Entry* ptr, Entry* end) : ptr_(ptr), end_(end) {
            advance();
        }

        std::pair<int32_t, T&> operator*() {
            return {ptr_->key, ptr_->value};
        }

        iterator& operator++() {
            ptr_++;
            advance();
            return *this;
        }

        bool operator!=(const iterator& other) const {
            return ptr_ != other.ptr_;
        }
    };

    iterator begin() {
        return iterator(entries_.data(), entries_.data() + capacity_);
    }

    iterator end() {
        return iterator(entries_.data() + capacity_, entries_.data() + capacity_);
    }
};

#endif  // OPEN_HASH_TABLE_H
