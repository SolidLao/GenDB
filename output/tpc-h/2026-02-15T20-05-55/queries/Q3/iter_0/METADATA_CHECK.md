# Q3 Query - Metadata Check Report

## Query Overview
```sql
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM customer, orders, lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-03-15'
    AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10;
```

## Data Encoding & Storage

### Table: customer
| Column | File | Type | Encoding | Scale | Notes |
|--------|------|------|----------|-------|-------|
| c_custkey | customer/c_custkey.bin | int32_t | none | - | Primary key, join column |
| c_mktsegment | customer/c_mktsegment.bin | uint8_t | dictionary | - | Filter predicate (c_mktsegment='BUILDING') |

**Dictionary File**: `customer/c_mktsegment_dict.txt`
```
0=BUILDING
1=AUTOMOBILE
2=MACHINERY
3=HOUSEHOLD
4=FURNITURE
```

### Table: orders
| Column | File | Type | Encoding | Scale | Notes |
|--------|------|------|----------|-------|-------|
| o_orderkey | orders/o_orderkey.bin | int32_t | none | - | Join column with lineitem |
| o_custkey | orders/o_custkey.bin | int32_t | none | - | Join column with customer |
| o_orderdate | orders/o_orderdate.bin | int32_t | date | - | Range filter: <9205 (1995-03-15) |
| o_shippriority | orders/o_shippriority.bin | int32_t | none | - | GROUP BY column |

**Date Constants**:
- 1995-03-15 = 9205 epoch days (Note: gendb data stored with +1 offset)

### Table: lineitem
| Column | File | Type | Encoding | Scale | Notes |
|--------|------|------|----------|-------|-------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | none | - | Join column with orders |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | date | - | Range filter: >9205 (1995-03-15) |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | scaled int | 100 | DECIMAL(15,2) |
| l_discount | lineitem/l_discount.bin | int64_t | scaled int | 100 | DECIMAL(15,2) |

## Implementation Details

### Data Issues Encountered
1. **Date Offset**: Binary epoch days are stored with +1 offset (1995-03-02 = 9191, stored as 9192)
   - Solution: Subtract 1 from epoch day when formatting dates
   - Adjusted thresholds: 1995-03-15 comparison uses 9205 instead of 9204

2. **Decimal Precision Loss**: Revenue calculation `ext_price * (100 - discount) / 100` with integer division loses fractional cents
   - Solution: Store numerator `ext_price * (100 - discount)` without dividing during aggregation
   - Divide by 10000 only at output time (doubles) to preserve precision
   - Result: 4 decimal place precision in output

### Join Strategy
Three-table chain join with filtering:
1. **Filter customer**: c_mktsegment = 'BUILDING' → 300,276 qualifying customers
2. **Filter orders**: Join with customer, filter o_orderdate < 1995-03-15
3. **Filter lineitem**: Join with orders, filter l_shipdate > 1995-03-15
4. **Aggregate**: GROUP BY (l_orderkey, o_orderdate, o_shippriority), SUM(revenue)
5. **Sort & Limit**: ORDER BY revenue DESC, o_orderdate ASC; LIMIT 10

### Index Usage
Available indexes not used in iter_0 (correctness-first approach):
- `hash_c_custkey`: Customer PK lookup
- `hash_o_custkey`: Orders→Customer join
- `hash_l_orderkey`: Lineitem→Orders join
- `zone_map_o_orderdate`: Date range pruning
- `zone_map_l_shipdate`: Date range pruning

### Data Structures
- **Customer filter**: `std::unordered_map<int32_t, bool>` for fast c_custkey membership check
- **Orders map**: `std::unordered_map<int32_t, std::vector<OrderData>>` for 1:N join (order→multiple matching orders per customer)
- **GROUP BY aggregation**: `std::unordered_map<GroupKey, int64_t, GroupKeyHash>` with custom hash function

## Validation Results
✅ **PASSED** - All 10 rows match ground truth exactly
```
Expected:  4791171, revenue=440715.2185, date=1995-02-23, priority=0
Actual:    4791171, revenue=440715.2185, date=1995-02-23, priority=0
(and 9 more rows - all exact matches)
```

## Performance (with profiling)
- **Total execution**: ~3.8 seconds
- Customer filter: 51.7 ms
- Orders filter: 1770.5 ms
- Lineitem join & aggregate: 1947.4 ms
- Sort: 13.8 ms
- Output: 0.3 ms

## File Statistics
- Input size (all tables): ~6.5 GB
- Output rows: 10
- Output file size: ~200 bytes (Q3.csv)

## Compilation
```bash
# With profiling
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE -o q3 q3.cpp

# Final build (no profiling)
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o q3 q3.cpp
```

## Notes
- Query is correctly implemented for **iteration 0 (correctness first)**
- Dictionary-encoded string filtering works correctly (BUILDING segment)
- Date arithmetic accounts for gendb offset (+1 day)
- Decimal arithmetic preserves full precision through aggregation
- All major operations instrumented with [TIMING] guards for profiling
