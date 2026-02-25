# Q1 Guide — Pricing Summary Report

## Query
```sql
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity), SUM(l_extendedprice),
       SUM(l_extendedprice*(1-l_discount)), SUM(l_extendedprice*(1-l_discount)*(1+l_tax)),
       AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount), COUNT(*)
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

## Table Stats
| Table    | Rows       | Role | Sort Order | Block Size |
|----------|------------|------|------------|------------|
| lineitem | 59,986,052 | fact | (none)     | 65536      |

## Column Reference

### l_shipdate (date, int32_t — days_since_epoch_1970)
- File: `lineitem/l_shipdate.bin` (59,986,052 × 4 bytes = 239,944,208 bytes)
- Encoding: Howard Hinnant algorithm; 1970-01-01 → 0; values confirmed > 3000
- This query: `l_shipdate <= threshold` where threshold = DATE '1998-12-01' - 90 days
- **C1/C7**: Use `date_utils.h` — NEVER hardcode or use N×365:
  ```cpp
  gendb::init_date_tables();  // C11: call once at top of main()
  int32_t date_1998_12_01 = gendb::date_to_epoch("1998-12-01");
  int32_t threshold = gendb::add_days(date_1998_12_01, -90);  // 1998-09-02
  ```
- Filter applies to ~98.5% of rows (selectivity 0.985) — nearly full scan

### l_returnflag (category, int16_t — dict-encoded)
- File: `lineitem/l_returnflag.bin` (59,986,052 × 2 bytes = 119,972,104 bytes)
- Dict file: `lineitem/l_returnflag_dict.txt` (3 distinct values: A, N, R)
- This query: GROUP BY key — decode to string for output
- **C2**: Load dict at runtime, NEVER hardcode code values:
  ```cpp
  std::vector<std::string> rf_dict;
  // read lineitem/l_returnflag_dict.txt line by line into rf_dict
  // code 0 → rf_dict[0], code 1 → rf_dict[1], etc.
  // output: rf_dict[code].c_str()
  ```
- Distribution: A≈14.8M, N≈30.4M, R≈14.8M rows

### l_linestatus (category, int16_t — dict-encoded)
- File: `lineitem/l_linestatus.bin` (59,986,052 × 2 bytes = 119,972,104 bytes)
- Dict file: `lineitem/l_linestatus_dict.txt` (2 distinct values: F, O)
- This query: GROUP BY key — decode to string for output
- **C2**: Load dict at runtime:
  ```cpp
  std::vector<std::string> ls_dict;
  // read lineitem/l_linestatus_dict.txt line by line into ls_dict
  // output: ls_dict[code].c_str()
  ```
- Distribution: F≈30.0M, O≈30.0M rows

### l_quantity (measure, double)
- File: `lineitem/l_quantity.bin` (59,986,052 × 8 bytes = 479,888,416 bytes)
- Encoding: IEEE 754 double; values ∈ [1, 50]
- This query: SUM(l_quantity), AVG(l_quantity) per group
- double precision is sufficient (max group sum ~5×50×30M = 1.5×10^9, well under 10^15)

### l_extendedprice (measure, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes = 479,888,416 bytes)
- Encoding: IEEE 754 double; max individual value ~104,949
- This query: SUM(l_extendedprice), SUM(ep*(1-disc)), SUM(ep*(1-disc)*(1+tax)), AVG(l_extendedprice)
- **C35**: SUM(ep*(1-disc)) and SUM(ep*(1-disc)*(1+tax)) are derived multi-column expressions.
  Use `long double` accumulation — do NOT apply int64_t cents (C29) to these expressions:
  ```cpp
  long double sum_disc_price = 0, sum_charge = 0;
  sum_disc_price += ep * (1.0 - disc);
  sum_charge     += ep * (1.0 - disc) * (1.0 + tax);
  ```
- **C29 not triggered** for SUM(l_extendedprice) alone (max group sum ~50M×104949 ≈ 5×10^12 < 10^13)

### l_discount (measure, double)
- File: `lineitem/l_discount.bin` (59,986,052 × 8 bytes = 479,888,416 bytes)
- Encoding: IEEE 754 double; values ∈ [0.00, 0.10], 11 distinct values
- This query: AVG(l_discount), also used in SUM(ep*(1-disc))

### l_tax (measure, double)
- File: `lineitem/l_tax.bin` (59,986,052 × 8 bytes = 479,888,416 bytes)
- Encoding: IEEE 754 double
- This query: used in SUM(ep*(1-disc)*(1+tax))

## Query Analysis
- **Access pattern**: Full scan of lineitem (98.5% rows pass shipdate filter)
- **Filter**: zone-map skip possible but marginal given 98.5% selectivity
- **GROUP BY**: (l_returnflag, l_linestatus) → only 4 possible groups (3 × 2, but valid combos = 4)
- **Aggregation**: 4 groups × 8 aggregates — trivially fits in L1 cache
- **GROUP BY key** must include BOTH dimensions (C15): key = (rf_code, ls_code)
- **Thread-local aggregation** (P17/P20): each thread maintains 4-slot local array; merge at end
- **Output**: decode rf_dict/ls_dict, sort by (rf_code ASC, ls_code ASC) — sort on string labels

## Indexes

### l_shipdate_zone_map (zone_map on l_shipdate)
- File: `lineitem/indexes/l_shipdate_zone_map.bin`
- Layout: `[uint32_t num_blocks][Block*]` where `struct Block { int32_t mn, mx; uint32_t cnt; }`
- Block size: 65536 rows → `num_blocks = ceil(59986052 / 65536) = 916` blocks
- Each Block record: 12 bytes (4+4+4)
- File size: 4 + 916×12 = 10,996 bytes
- Usage: skip blocks where `block.mx <= threshold` (all values already pass) — with 98.5% selectivity
  very few blocks can be skipped; zone map provides minimal benefit for Q1.
  Still, load and check: if `block.mn > threshold` skip block (no rows pass — impossible here).
  More useful: blocks where `block.mn <= threshold AND block.mx > threshold` → partial scan.
- Access pattern:
  ```cpp
  size_t zm_sz;
  const uint32_t* zm_raw = (const uint32_t*)mmap_ro(zm_file, zm_sz);
  uint32_t num_blocks = zm_raw[0];
  struct ZMBlock { int32_t mn, mx; uint32_t cnt; };
  const ZMBlock* blocks = (const ZMBlock*)(zm_raw + 1);
  // For block b: rows [b*65536, b*65536 + blocks[b].cnt)
  // Skip if blocks[b].mn > threshold (no rows pass l_shipdate <= threshold)
  ```

## Aggregation Slot Layout (recommended)
```cpp
struct AggSlot {
    int16_t     rf_code;        // l_returnflag dict code
    int16_t     ls_code;        // l_linestatus dict code
    double      sum_qty;
    long double sum_base_price; // SUM(l_extendedprice) — long double for safety
    long double sum_disc_price; // SUM(ep*(1-disc)) — C35: derived expression
    long double sum_charge;     // SUM(ep*(1-disc)*(1+tax)) — C35: derived expression
    double      sum_disc;       // for AVG(l_discount)
    int64_t     count;
};
// 4 slots total — use small fixed array, not hash table
```

## Date Constant Summary
| SQL Expression                              | C++ Pattern                                           |
|---------------------------------------------|-------------------------------------------------------|
| DATE '1998-12-01' - INTERVAL '90' DAY       | `gendb::add_days(gendb::date_to_epoch("1998-12-01"), -90)` |
