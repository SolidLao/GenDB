# Zone Maps

## What It Is
Zone maps (also called min/max indexes or small materialized aggregates) store per-block statistics (min, max, count, null count) to skip entire data blocks that cannot match query predicates.

## When To Use
- Range predicates on sorted or clustered data (`WHERE date BETWEEN ... AND ...`)
- Column stores with block-level compression (Parquet, ORC, columnar databases)
- Data warehouses with time-series or partitioned data
- Avoiding expensive scans on cold storage (S3, SSD)

## Key Implementation Ideas

### Basic Zone Map Structure
```cpp
struct ZoneMap {
    int64_t min_value;
    int64_t max_value;
    uint32_t null_count;
    uint32_t row_count;
    uint64_t block_offset;  // Physical location
};

bool can_skip(const ZoneMap& zone, int64_t predicate_min, int64_t predicate_max) {
    // Zone [10, 100] vs predicate [200, 300] -> skip
    return zone.max_value < predicate_min || zone.min_value > predicate_max;
}
```

### Multi-Level Zone Maps
```cpp
// Hierarchical: Segment -> Block -> Sub-block
struct SegmentZoneMap {
    ZoneMap segment_stats;     // Covers 1M rows
    ZoneMap block_stats[128];  // Each covers 8K rows
};

// Check coarse filter first, then fine-grained
if (can_skip(segment_stats, min, max)) return;
for (auto& block : block_stats) {
    if (!can_skip(block, min, max)) scan_block(block);
}
```

### Multi-Column Zone Maps
```cpp
// Correlations matter: (date, country) clustered together
struct MultiColumnZone {
    ZoneMap date_zone;
    ZoneMap country_zone;
    uint32_t distinct_countries;  // Selectivity hint
};

// Query: WHERE date = '2024-01-15' AND country = 'US'
// Skip zones where date range doesn't include target OR country not present
bool can_skip_multi(const MultiColumnZone& zone,
                    int64_t date, const std::string& country) {
    if (zone.date_zone.min_value > date || zone.date_zone.max_value < date)
        return true;
    // Use bloom filter or dictionary for string membership test
    if (!zone.country_bloom.contains(country))
        return true;
    return false;
}
```

### Zone Map Maintenance (Updates)
```cpp
class ZoneMapIndex {
    std::vector<ZoneMap> zones;

    void update_zone(size_t zone_id, int64_t new_value) {
        auto& zone = zones[zone_id];
        zone.min_value = std::min(zone.min_value, new_value);
        zone.max_value = std::max(zone.max_value, new_value);
        zone.row_count++;

        // Rebuild if zone becomes too wide (low selectivity)
        if (zone.max_value - zone.min_value > threshold) {
            recompute_zone(zone_id);
        }
    }

    void delete_row(size_t zone_id, int64_t value) {
        auto& zone = zones[zone_id];
        zone.row_count--;

        // If deleted min/max, must scan block to recompute
        if (value == zone.min_value || value == zone.max_value) {
            zone.needs_recompute = true;
        }
    }
};
```

### Vectorized Zone Map Filtering
```cpp
// SIMD: Check 8 zones at once (AVX2)
uint8_t filter_zones_simd(const ZoneMap* zones, size_t n,
                          int64_t min, int64_t max) {
    __m256i pred_min = _mm256_set1_epi64x(min);
    __m256i pred_max = _mm256_set1_epi64x(max);

    uint8_t result = 0;
    for (size_t i = 0; i < n; i += 4) {
        __m256i zone_min = _mm256_loadu_si256((__m256i*)&zones[i].min_value);
        __m256i zone_max = _mm256_loadu_si256((__m256i*)&zones[i].max_value);

        // Skip if zone.max < pred_min OR zone.min > pred_max
        __m256i cmp1 = _mm256_cmpgt_epi64(pred_min, zone_max);
        __m256i cmp2 = _mm256_cmpgt_epi64(zone_min, pred_max);
        __m256i skip = _mm256_or_si256(cmp1, cmp2);

        result |= _mm256_movemask_epi8(skip) << (i * 2);
    }
    return result;
}
```

## Performance Characteristics
- Expected speedup: 10-1000x on selective queries (skip 90-99% of blocks)
- Cache behavior: Zone maps fit in L3 (1KB per 1M rows = 1MB for 1B rows)
- Memory overhead: <0.1% (16 bytes per block of 8K-1M rows)
- Pruning efficiency: 95%+ on time-series, 50-80% on clustered data, <20% on random
- Build time: O(n) scan, incremental updates O(1) per insert

## Real-World Examples
- **ClickHouse**: Primary key sparse index + min/max per 8192-row granule
- **DuckDB**: Zone maps on Parquet files, skip entire row groups
- **Snowflake**: Micro-partitions (100MB) with extensive metadata pruning
- **Vertica**: ROS (Read-Optimized Store) with zone maps per projection
- **Databricks Delta**: Data skipping with 32 columns of stats per file

## Pitfalls
- Random data: Zone maps ineffective (min=0, max=1M covers everything)
- Wide zones: Updates expand range, reducing selectivity (periodically rebuild)
- Over-granular zones: Too many zones (1KB each) bloat metadata cache
- String comparisons: Min/max expensive, use dictionary encoding + integer zones
- Multi-column: Combinatorial explosion (don't store all column combinations)
- Delete anomalies: Deleting min/max requires expensive recomputation (lazy rebuild)
