# Arena Allocation

## What It Is
Bump-pointer allocators that carve memory from pre-allocated regions (arenas), eliminating malloc/free overhead. Entire arena is freed at once, avoiding per-object deallocation cost.

## When To Use
- Temporary data structures during query execution (hash tables, intermediate results)
- String/varchar storage for a single query
- Expression tree nodes and operator state
- Avoiding malloc/free in hot paths (10-100x faster than system allocator)

## Key Implementation Ideas

### Simple Bump Allocator
```cpp
class Arena {
    struct Block {
        char* data;
        size_t capacity;
        size_t used;
    };

    std::vector<Block> blocks;
    static constexpr size_t DEFAULT_BLOCK_SIZE = 1 << 20; // 1MB

public:
    void* allocate(size_t size, size_t alignment = 8) {
        // Align to requested boundary
        size_t aligned_used = (current_block().used + alignment - 1) & ~(alignment - 1);

        if (aligned_used + size > current_block().capacity) {
            allocate_new_block(std::max(size, DEFAULT_BLOCK_SIZE));
            aligned_used = 0;
        }

        void* ptr = current_block().data + aligned_used;
        current_block().used = aligned_used + size;
        return ptr;
    }

    // No per-object free; entire arena freed at once
    ~Arena() {
        for (auto& block : blocks) {
            ::free(block.data);
        }
    }

private:
    Block& current_block() { return blocks.back(); }

    void allocate_new_block(size_t size) {
        blocks.push_back({
            static_cast<char*>(::malloc(size)),
            size,
            0
        });
    }
};
```

### Per-Query Arena with RAII
```cpp
class QueryContext {
    Arena arena;

public:
    template<typename T, typename... Args>
    T* make(Args&&... args) {
        void* mem = arena.allocate(sizeof(T), alignof(T));
        return new (mem) T(std::forward<Args>(args)...);
    }

    char* allocate_string(const char* str, size_t len) {
        char* mem = static_cast<char*>(arena.allocate(len + 1));
        std::memcpy(mem, str, len);
        mem[len] = '\0';
        return mem;
    }

    // Entire arena freed when query finishes
};
```

### Arena-Backed Container
```cpp
template<typename T>
class ArenaAllocator {
    Arena* arena_;

public:
    using value_type = T;

    ArenaAllocator(Arena* arena) : arena_(arena) {}

    T* allocate(size_t n) {
        return static_cast<T*>(arena_->allocate(n * sizeof(T), alignof(T)));
    }

    void deallocate(T*, size_t) {
        // No-op: arena owns memory
    }
};

// Usage: arena-backed vector
using ArenaVector = std::vector<int, ArenaAllocator<int>>;
ArenaVector vec{ArenaAllocator<int>(&arena)};
```

### Monotonic Allocator with Reset
```cpp
class MonotonicArena {
    char* buffer_;
    size_t capacity_;
    size_t offset_ = 0;

public:
    MonotonicArena(size_t capacity)
        : buffer_(new char[capacity]), capacity_(capacity) {}

    void* allocate(size_t size) {
        if (offset_ + size > capacity_) {
            throw std::bad_alloc();
        }
        void* ptr = buffer_ + offset_;
        offset_ += size;
        return ptr;
    }

    void reset() {
        offset_ = 0; // Reuse buffer without freeing
    }

    ~MonotonicArena() { delete[] buffer_; }
};
```

### String Interning with Arena
```cpp
class StringArena {
    Arena arena;
    absl::flat_hash_set<std::string_view> interned_strings;

public:
    std::string_view intern(std::string_view str) {
        auto it = interned_strings.find(str);
        if (it != interned_strings.end()) {
            return *it; // Return existing interned string
        }

        // Copy string into arena
        char* mem = static_cast<char*>(arena.allocate(str.size()));
        std::memcpy(mem, str.data(), str.size());
        std::string_view interned{mem, str.size()};

        interned_strings.insert(interned);
        return interned;
    }
};
```

## Performance Characteristics
- **Allocation Speed**: 10-100x faster than malloc (just pointer bump)
- **Deallocation Speed**: O(1) vs O(n) for individual frees
- **Cache**: Better locality due to sequential allocation
- **Overhead**: ~0-5% memory overhead (vs 10-20% for malloc)
- **Fragmentation**: Minimal internal fragmentation with proper block sizing

## Real-World Examples
- **DuckDB**: Temporary allocator for query execution, string arenas
- **ClickHouse**: Arena-based memory management for queries
- **LLVM**: BumpPtrAllocator for compiler IR nodes
- **PostgreSQL**: Memory contexts (hierarchical arenas)

## Pitfalls
- **Memory Leaks**: Cannot free individual objects; leaks if arena outlives objects
- **Destructor Calls**: Objects not destroyed automatically; must call manually if needed
- **Thread Safety**: Not thread-safe by default; use per-thread arenas
- **Large Allocations**: Wasted space if single allocation >> block size
- **Alignment**: Must manually align to avoid unaligned access penalties
- **Fragmentation**: Block size too small causes many blocks; too large wastes memory
- **Long-Lived Arenas**: Grows unbounded if reset() not called; use for scoped allocations only
