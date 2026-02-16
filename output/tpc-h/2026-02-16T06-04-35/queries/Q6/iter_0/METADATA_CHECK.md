# Q6 Metadata Check Report

## Storage Design Verification

### Lineitem Columns
| Column | File | Type | Encoding | Scale Factor | Verified |
|--------|------|------|----------|--------------|----------|
| l_shipdate | l_shipdate.bin | int32_t | days_since_epoch | - | ✓ (values 9568+) |
| l_discount | l_discount.bin | int64_t | scaled integer | 100 | ✓ (values 4-10) |
| l_quantity | l_quantity.bin | int64_t | scaled integer | 100 | ✓ |
| l_extendedprice | l_extendedprice.bin | int64_t | scaled integer | 100 | ✓ |

### Indexes
| Index | File | Type | Verified |
|-------|------|------|----------|
| lineitem_l_shipdate_zone | lineitem_l_shipdate_zone.bin | zone_map | ✓ (300 zones) |

## Query Constants

| Constant | Formula | Value | Usage |
|----------|---------|-------|-------|
| DATE_1994_01_01 | computed from epoch | 8766 | Lower bound for l_shipdate filter |
| DATE_1995_01_01 | computed from epoch | 9131 | Upper bound for l_shipdate filter |
| DISCOUNT_MIN | 0.05 × 100 | 5 | Lower bound for l_discount filter |
| DISCOUNT_MAX | 0.07 × 100 | 7 | Upper bound for l_discount filter |
| QUANTITY_MAX | 24 × 100 | 2400 | Upper bound for l_quantity filter |

## Filters Applied (in order)
1. l_shipdate >= 8766 (1994-01-01)
2. l_shipdate < 9131 (1995-01-01)
3. l_discount >= 5 (0.05)
4. l_discount <= 7 (0.07)
5. l_quantity < 2400 (24)

## Computation
- Formula: SUM(l_extendedprice * l_discount)
- Both operands scaled by 100 → product scaled by 10000
- Final scaling: divide by 10000 to get monetary value

## Zone Map Optimization
- Zone map identified 300 valid zones out of 300 blocks
- Zone map skips blocks where l_shipdate range doesn't overlap with [8766, 9131)
- Significant I/O reduction for range queries

## Parallelization
- OpenMP parallel scan with dynamic scheduling
- Thread-local aggregation buffers to avoid contention
- 64 cores detected in hardware config

## Result
- **Expected**: 1230113636.0101
- **Actual**: 1230113636.0101
- **Status**: ✓ VALIDATION PASSED (0 rows difference, values match exactly)

## Performance Metrics
- Zone map loading: 0.03 ms
- Column mmap: 0.03 ms
- Total data loading: 0.11 ms
- Aggregation (main computation): 443.56 ms
- Output writing: 0.26 ms
- **Total execution**: 443.69 ms
