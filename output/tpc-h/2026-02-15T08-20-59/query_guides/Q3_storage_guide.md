# Q3 Storage Guide: Shipping Priority

## Query Summary
```sql
SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority
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

Q3 is a **3-way join** (customer → orders → lineitem) with **date and categorical filters**.

---

## Data Files

### Customer Table
- **Total rows:** 1,500,000
- **Block size:** 131,072 rows per block (12 blocks)

| Column | File Path | C++ Type | Semantic Type | Encoding | Scale Factor |
|--------|-----------|----------|---------------|----------|--------------|
| `c_custkey` | `customer/c_custkey.bin` | int32_t | INTEGER | none | N/A |
| `c_mktsegment` | `customer/c_mktsegment.bin` | uint8_t | CHAR | dictionary | N/A |

**c_mktsegment dictionary:** `customer/c_mktsegment_dict.txt`
```
0=AUTOMOBILE
1=BUILDING
2=FURNITURE
3=MACHINERY
4=HOUSEHOLD
```

### Orders Table
- **Total rows:** 15,000,000
- **Block size:** 131,072 rows per block (115 blocks)
- **Sorted by:** `o_orderdate` (for zone map effectiveness)

| Column | File Path | C++ Type | Semantic Type | Encoding | Scale Factor |
|--------|-----------|----------|---------------|----------|--------------|
| `o_orderkey` | `orders/o_orderkey.bin` | int32_t | INTEGER | none | N/A |
| `o_custkey` | `orders/o_custkey.bin` | int32_t | INTEGER | none | N/A |
| `o_orderdate` | `orders/o_orderdate.bin` | int32_t | DATE | none | N/A |
| `o_shippriority` | `orders/o_shippriority.bin` | int32_t | INTEGER | none | N/A |

### Lineitem Table
- **Total rows:** 59,986,052
- **Block size:** 131,072 rows per block (458 blocks)

| Column | File Path | C++ Type | Semantic Type | Encoding | Scale Factor |
|--------|-----------|----------|---------------|----------|--------------|
| `l_orderkey` | `lineitem/l_orderkey.bin` | int32_t | INTEGER | none | N/A |
| `l_extendedprice` | `lineitem/l_extendedprice.bin` | int64_t | DECIMAL | none | 100 |
| `l_discount` | `lineitem/l_discount.bin` | int64_t | DECIMAL | none | 100 |
| `l_shipdate` | `lineitem/l_shipdate.bin` | int32_t | DATE | none | N/A |

---

## Available Indexes

### Hash Indexes (for Joins)

#### c_custkey_hash
- **File:** `indexes/c_custkey_hash.bin`
- **Index type:** hash (single-value, PK lookup)
- **Unique keys:** 1,500,000
- **Binary Layout:**
  - Header: `uint32_t num_unique_keys` (4 bytes)
  - Header: `uint32_t table_size` (4 bytes, power-of-2)
  - Hash table entries: `[int32_t key, uint32_t position]` repeated table_size times
    - Each entry = 8 bytes
    - Load factor: ~0.6
    - Table size: ~2,500,000
  - Total entries bytes: 2,500,000 × 8 = 20 MB
  - Total file size: ~33 MB
- **Lookup cost:** O(1) average case
- **Usage:** Probe from filtered orders → lookup customer position by c_custkey

#### o_orderkey_hash
- **File:** `indexes/o_orderkey_hash.bin`
- **Index type:** hash (single-value, PK lookup)
- **Unique keys:** 15,000,000
- **Binary Layout:**
  - Similar to c_custkey_hash
  - Table size: ~25,000,000
  - Total file size: ~257 MB
- **Lookup cost:** O(1) average case
- **Usage:** Probe from lineitem → lookup orders position by o_orderkey

#### o_custkey_hash
- **File:** `indexes/o_custkey_hash.bin`
- **Index type:** hash (multi-value, FK lookup with grouping)
- **Unique keys:** 999,982 (some customers have no orders)
- **Binary Layout:**
  - Header: `uint32_t num_unique_keys` (4 bytes)
  - Header: `uint32_t table_size` (4 bytes)
  - Hash table entries: `[int32_t key, uint32_t offset, uint32_t count]` repeated table_size times
    - Each entry = 12 bytes (key + offset + count for multi-value design)
  - Positions array: All row positions grouped by key
    - `uint32_t position_count` (4 bytes)
    - `uint32_t positions[position_count]` (4 bytes per position)
  - Total file size: ~82 MB
- **Lookup cost:** O(1) hash + O(k) for k matching rows (avg k ≈ 15 for orders)
- **Usage:** Build during first filter → probe from lineitem via l_orderkey (see o_orderkey_hash)

### Zone Maps (Block Min/Max)

#### o_orderdate_zone_map
- **File:** `indexes/o_orderdate_zone_map.bin`
- **Index type:** zone_map
- **Cardinality:** 115 blocks (orders has 15M rows ÷ 131K rows/block)
- **Binary Layout:**
  - Header: `uint32_t num_blocks` (4 bytes)
  - For each block: `int32_t min_date, int32_t max_date` (8 bytes)
  - Total file size: 4 + (115 × 8) = 924 bytes
- **Date encoding:** Epoch days (1970-01-01 = day 0)
  - Q3 predicate: `o_orderdate < 9204` (1995-03-15)
  - Estimated selectivity: ~50% of blocks may qualify
- **Usage:** Fast block-level filtering on orders before join

#### l_shipdate_zone_map
- **File:** `indexes/l_shipdate_zone_map.bin`
- **Index type:** zone_map
- **Cardinality:** 458 blocks
- **Binary Layout:**
  - Header: `uint32_t num_blocks`
  - For each block: `int32_t min_date, int32_t max_date` (8 bytes)
  - Total file size: 4 + (458 × 8) = 3,668 bytes
- **Date encoding:** Epoch days
  - Q3 predicate: `l_shipdate > 9204` (1995-03-15)
  - Estimated selectivity: ~53% of blocks
- **Usage:** Fast block-level filtering on lineitem before join

#### c_mktsegment_zone_map
- **File:** `indexes/c_mktsegment_zone_map.bin`
- **Index type:** zone_map
- **Cardinality:** 12 blocks (customer has 1.5M rows ÷ 131K rows/block)
- **Binary Layout:**
  - Header: `uint32_t num_blocks`
  - For each block: `uint8_t min_code, uint8_t max_code` (2 bytes)
  - Total file size: 4 + (12 × 2) = 28 bytes
- **Code encoding:** Dictionary codes (0=AUTOMOBILE, 1=BUILDING, 2=FURNITURE, 3=MACHINERY, 4=HOUSEHOLD)
- **Usage:** Skip blocks where mktsegment cannot be 'BUILDING' (code=1)

---

## Join Order & Strategy Recommendations

### **Recommended Join Order: Customer → Orders → Lineitem**

This order exploits the highly selective filters and the fact that customer is the smallest fact table.

#### Step 1: Customer → Orders (via c_custkey)
1. **Pre-filter customer:**
   - Load c_mktsegment column (1.5M rows)
   - Filter for `c_mktsegment == 'BUILDING'` (code = 1)
   - Estimated output: ~300,000 customer rows (20% of 1.5M)

2. **Build hash table:**
   - On the fly, construct a **hash table mapping c_custkey → customer position**
   - Fast construction: O(300K) with OpenMP

3. **Probe orders:**
   - Use `o_custkey_hash` to **group orders by custkey**
   - For each filtered customer, hash-probe o_custkey_hash to find all matching orders
   - Estimated output: ~3,000,000 orders (~20% of 15M)

4. **Apply date filter:**
   - Filter orders by `o_orderdate < 9204` before next join
   - **Use o_orderdate_zone_map** to skip blocks early
   - Estimated output after filter: ~1,500,000 orders

#### Step 2: (Filtered Orders) → Lineitem (via o_orderkey)
1. **Build hash table:**
   - From filtered orders, construct hash table: o_orderkey → {count, positions}
   - Input: 1.5M orders

2. **Probe lineitem:**
   - Use `l_orderkey_hash` (pre-built, multi-value)
   - For each lineitem row, hash-probe to find matching order positions
   - **Use l_shipdate_zone_map** to skip blocks where `l_shipdate <= 9204`
   - Estimated output: ~7,500,000 lineitem rows (~12.5% of 60M)

3. **Apply revenue computation:**
   - Calculate: `l_extendedprice * (1 - l_discount)`
   - Aggregate by (l_orderkey, o_orderdate, o_shippriority)

#### Step 3: Final Sort & Limit
- Sort results by revenue DESC, o_orderdate ASC
- Return top 10 rows

---

## Index Usage Summary

| Index | Used? | Purpose | Benefit |
|-------|-------|---------|---------|
| `c_custkey_hash` | **NO** | PK lookup | Not needed; we scan customer with filter |
| `c_mktsegment_zone_map` | **YES** | Block pruning | Fast skip of blocks where mktsegment ≠ BUILDING |
| `o_custkey_hash` | **YES** | FK grouping | Multi-value lookup for 20% filtered customers |
| `o_orderkey_hash` | **YES** | PK multi-value | Efficient join with lineitem |
| `o_orderdate_zone_map` | **YES** | Date range pruning | Skip blocks outside `o_orderdate < 9204` |
| `l_orderkey_hash` | **YES** | PK multi-value | Efficient join from lineitem |
| `l_shipdate_zone_map` | **YES** | Date range pruning | Skip blocks where `l_shipdate <= 9204` |

---

## Storage Properties

### Column Layouts (All Binary Columnar)

**Example: customer/c_custkey.bin**
```
[1] [2] [3] ... [1500000]  (each int32_t = 4 bytes)
Total: 1.5M × 4 = 6 MB
```

**Example: orders/o_orderkey.bin**
```
[1] [2] [3] ... [6000000]  (each int32_t = 4 bytes)
Total: 15M × 4 = 60 MB
```

**Dictionary Encoding: c_mktsegment**
- Stored as `uint8_t` codes (0–4), not full strings
- Compression factor: ~5× (vs. storing "BUILDING" = 8 bytes per row)
- File size: 1.5M × 1 byte = 1.5 MB (vs. 12 MB for uncompressed)

---

## Query Execution Flow (Pseudocode)

```
// Step 1: Filter customer on c_mktsegment
filtered_customers = scan_with_filter(customer, c_mktsegment == 1)  // 300K rows
customer_hash = build_hash(filtered_customers, c_custkey)

// Step 2: Join orders
filtered_orders = []
for custkey in customer_hash.keys():
    orders_for_custkey = o_custkey_hash.lookup(custkey)
    for order in orders_for_custkey:
        if order.o_orderdate < 9204:  // zone-map pruned
            filtered_orders.append(order)
// filtered_orders: ~1.5M rows

// Step 3: Build orders hash and join lineitem
orders_hash = build_hash(filtered_orders, o_orderkey)
results = []
for block in lineitem.blocks:
    if l_shipdate_zone_map[block].max_date <= 9204:
        skip_block()  // No lineitem in block matches l_shipdate > 9204
    for row in block:
        if row.l_shipdate > 9204:
            order = orders_hash.lookup(row.l_orderkey)
            if order != NULL:
                revenue = row.l_extendedprice * (100 - row.l_discount) / 100
                results.append((row.l_orderkey, revenue, order.o_orderdate, order.o_shippriority))

// Step 4: Group, sort, limit
grouped = group_by(results, [l_orderkey, o_orderdate, o_shippriority])
sorted = sort(grouped, [revenue DESC, o_orderdate ASC])
return sorted.limit(10)
```

---

## Performance Notes

- **Customer filter:** Very selective (20%); zone maps on c_mktsegment speed this up
- **Orders join:** Multi-value hash on o_custkey groups ~15 orders per customer efficiently
- **Lineitem join:** l_orderkey_hash (pre-built, 15M keys) is O(1) per row
- **Date pruning:** Zone maps can skip ~50% of orders and lineitem blocks (critical!)
- **Expected memory:** ~200–400 MB for hash tables + temporary buffers (well within 376 GB RAM)

---

## Post-Execution Verification

After Q3 execution:
1. Date values in o_orderdate and l_shipdate are in range [8037, 10562] ✓
2. Prices and revenues are positive ✓
3. Result count ≤ 10 (LIMIT 10) ✓
4. All results satisfy the date predicates ✓
