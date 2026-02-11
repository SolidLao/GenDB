# Arena Allocation

## What It Is
Bump-pointer allocators that carve memory from pre-allocated regions (arenas), eliminating malloc/free overhead. Entire arena is freed at once, avoiding per-object deallocation cost.

## When To Use
- Temporary data structures during query execution (hash tables, intermediate results)
- String/varchar storage for a single query
- Expression tree nodes and operator state
- Avoiding malloc/free in hot paths (10-100x faster than system allocator)

## Key Implementation Ideas
- **Bump-pointer allocation**: Maintain a pointer into a pre-allocated block; allocate by advancing the pointer, O(1) per allocation
- **Block chaining**: When the current block is exhausted, allocate a new block and chain it; track all blocks for bulk free
- **Per-query arena with RAII**: Scope an arena to a query's lifetime so all temporary memory is freed automatically when the query finishes
- **Arena-backed STL containers**: Implement a custom allocator that delegates to the arena, enabling arena-backed vectors, maps, etc.
- **Monotonic allocator with reset**: Reuse a single buffer across queries by resetting the offset to zero instead of freeing/reallocating
- **String interning with arena**: Deduplicate strings by storing unique copies in the arena and returning string_view handles
- **Alignment handling**: Round up the bump pointer to the requested alignment boundary before each allocation
- **Per-thread arenas**: Give each thread its own arena to avoid synchronization overhead in parallel execution

## Performance Characteristics
- **Allocation speed**: 10-100x faster than malloc (just pointer bump)
- **Deallocation**: O(1) for entire arena vs O(n) for individual frees
- **Memory overhead**: ~0-5% vs 10-20% for malloc metadata

## Pitfalls
- **No individual frees**: Cannot reclaim memory for single objects; arena must outlive all its allocations
- **Destructor calls skipped**: Placement-new objects are not destroyed automatically; must call destructors manually if needed
- **Unbounded growth**: Long-lived arenas grow without bound if reset() is not called periodically
