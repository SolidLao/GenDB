# Compact Hash Tables

## What It Is
Cache-friendly hash tables using open addressing (all data in a single array) instead of chaining (linked lists). Modern variants like Robin Hood hashing and Swiss Tables minimize memory overhead and maximize cache locality.

## Key Implementation Ideas
- **Robin Hood hashing**: Track "distance from ideal bucket" (DIB) per entry; steal from rich (high DIB) to give to poor (low DIB), equalizing probe lengths
- **Swiss Tables (SIMD probing)**: Store 16 control bytes per group; use SIMD to compare all 16 in parallel for near-constant-time lookups
- **Open addressing with linear probing**: Store all entries in a single contiguous array for cache locality; probe sequentially on collision
- **Tombstone-free deletion (backward shift)**: On delete, shift subsequent entries backward to fill the gap, avoiding tombstone accumulation
- **Power-of-2 table sizing**: Use bitwise AND (`hash & (capacity - 1)`) instead of expensive modulo for index computation
- **Prefetching for batch lookups**: Prefetch the target bucket N steps ahead in a batch loop to hide memory latency
- **Load factor management**: Resize at 75-87.5% full to keep probe chains short while maintaining space efficiency
- **High-quality hash functions**: Use XXH3, Murmur3, or similar to minimize clustering and collision chains
