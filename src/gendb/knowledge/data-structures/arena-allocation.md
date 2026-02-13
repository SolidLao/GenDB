# Arena Allocation

## What It Is
Bump-pointer allocators that carve memory from pre-allocated regions (arenas), eliminating malloc/free overhead. Entire arena is freed at once, avoiding per-object deallocation cost.

## Key Implementation Ideas
- **Bump-pointer allocation**: Maintain a pointer into a pre-allocated block; allocate by advancing the pointer, O(1) per allocation
- **Block chaining**: When the current block is exhausted, allocate a new block and chain it; track all blocks for bulk free
- **Per-query arena with RAII**: Scope an arena to a query's lifetime so all temporary memory is freed automatically when the query finishes
- **Arena-backed STL containers**: Implement a custom allocator that delegates to the arena, enabling arena-backed vectors, maps, etc.
- **Monotonic allocator with reset**: Reuse a single buffer across queries by resetting the offset to zero instead of freeing/reallocating
- **String interning with arena**: Deduplicate strings by storing unique copies in the arena and returning string_view handles
- **Alignment handling**: Round up the bump pointer to the requested alignment boundary before each allocation
- **Per-thread arenas**: Give each thread its own arena to avoid synchronization overhead in parallel execution
