# Q1 Guide — Pricing Summary Report

## Query
```sql
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity), SUM(l_extendedprice),
       SUM(l_extendedprice*(1-l_discount)),
       SUM(l_extendedprice*(1-l_discount)*(1+l_tax)),
       AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount),
       COUNT(*)
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

## Table Stats
| Table    | Rows       | Role      | Sort Order      | Block Size |
|----------|------------|-----------|-----------------|------------|
| lineitem | 59,986,052 | fact/scan | l_shipdate ASC  | 100,000    |

## Column Reference

### l_shipdate (`DATE`, `int32_t`, days_since_epoch)
- File: `lineitem/l_shipdate.bin` (59,986,052 × 4 bytes = ~229 MB)
- Encoding: days since 1970-01-01 (civil calendar, same formula as `date_to_days()` in ingest.cpp)
- This query: `l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY`
  - Resolve the SQL date arithmetic first:
    - `DATE '1998-12-01' - INTERVAL '90' DAY` = **1998-09-02**
  - Compute threshold using `date_to_days()` equivalent:
    - y=1998, m=9, d=2 (m>2, no year adjustment)
    - era=4, yoe=398, doy=(153×6+2)/5+1=185, doe=145551
    - **threshold = 4×146097 + 145551 − 719468 = 10471**
  - C++ comparison: `l_shipdate[i] <= 10471`
- Range in data: 1992-01-02 to 1998-12-01 (≈2526 distinct values)
- **Selectivity: ~98.5%** — almost all rows pass; zone map provides minimal skipping

### l_returnflag (`CHAR(1)`, `int8_t`, dict_int8)
- File: `lineitem/l_returnflag.bin` (59,986,052 × 1 byte = ~57 MB)
- Dict sidecar: `lineitem/l_returnflag_dict.bin` (3 bytes: `{'A','N','R'}`)
- Encoding: code = index into dict file (code 0 → 'A', code 1 → 'N', code 2 → 'R')
- **Load pattern** (do NOT hardcode): read sidecar at startup:
  ```cpp
  char rf_dict[3];
  FILE* f = fopen(".../lineitem/l_returnflag_dict.bin", "rb");
  fread(rf_dict, 1, 3, f); fclose(f);
  // rf_dict[code] gives the original char
  ```
- This query: GROUP BY l_returnflag → group on raw int8_t code (3 distinct values: 0,1,2)
- ORDER BY l_returnflag → sort groups by code value (A < N < R → codes 0 < 1 < 2, order preserved)

### l_linestatus (`CHAR(1)`, `int8_t`, dict_int8)
- File: `lineitem/l_linestatus.bin` (59,986,052 × 1 byte = ~57 MB)
- Dict sidecar: `lineitem/l_linestatus_dict.bin` (2 bytes: `{'F','O'}`)
- Encoding: code 0 → 'F', code 1 → 'O'
- **Load pattern**:
  ```cpp
  char ls_dict[2];
  FILE* f = fopen(".../lineitem/l_linestatus_dict.bin", "rb");
  fread(ls_dict, 1, 2, f); fclose(f);
  ```
- This query: GROUP BY l_linestatus → group on raw int8_t code (2 distinct values: 0,1)
- ORDER BY l_linestatus → sort groups by code (F < O → codes 0 < 1, order preserved)

### l_quantity (`DECIMAL(15,2)`, `int8_t`, int8_integer_value)
- File: `lineitem/l_quantity.bin` (59,986,052 × 1 byte = ~57 MB)
- Encoding: actual integer value 1–50 stored as-is in int8_t (no scale factor)
- This query: `SUM(l_quantity)`, `AVG(l_quantity)`
- C++ aggregation: accumulate `(int64_t)l_quantity[i]` into sum; divide by count for avg
- Output: cast sum/count back to double for final output

### l_extendedprice (`DECIMAL(15,2)`, `double`, plain_double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes = ~458 MB)
- Encoding: IEEE-754 double, value as-is
- This query: `SUM(l_extendedprice)`, `SUM(l_extendedprice*(1-l_discount))`,
  `SUM(l_extendedprice*(1-l_discount)*(1+l_tax))`, `AVG(l_extendedprice)`
- C++ aggregation: accumulate `double` sums per group; compute scaled expressions inline

### l_discount (`DECIMAL(15,2)`, `int8_t`, int8_hundredths)
- File: `lineitem/l_discount.bin` (59,986,052 × 1 byte = ~57 MB)
- Encoding: value × 100 stored as int8_t (e.g., 0.07 → stored as 7; range 0–10)
- This query: used in expressions `(1 - l_discount)` and `AVG(l_discount)`
  - Decode at use: `double d_discount = l_discount[i] * 0.01;`
  - Then: `l_extendedprice[i] * (1.0 - d_discount)`
- C++ aggregation for AVG: accumulate `(int64_t)l_discount[i]` → divide by count × 100

### l_tax (`DECIMAL(15,2)`, `int8_t`, int8_hundredths)
- File: `lineitem/l_tax.bin` (59,986,052 × 1 byte = ~57 MB)
- Encoding: value × 100 stored as int8_t (e.g., 0.06 → stored as 6; range 0–8)
- This query: used in expression `(1 + l_tax)`
  - Decode at use: `double d_tax = l_tax[i] * 0.01;`
  - Then: `... * (1.0 + d_tax)`

## Query Analysis

### Filter
- **Single predicate** on `l_shipdate <= 10471` (1998-09-02 in days_since_epoch)
- Selectivity: ~98.5% — nearly all rows pass, minimal zone-map benefit
- Still use zone map to skip the last few blocks beyond 1998-09-02

### Grouping
- **4 groups maximum**: (returnflag ∈ {0,1,2}) × (linestatus ∈ {0,1})
- Group key: `uint8_t key = (l_returnflag[i] << 2) | l_linestatus[i]` → fits in a 4-entry array
- Recommended structure: direct-indexed array of 4 accumulators keyed by group_id

```cpp
struct GroupAcc {
    int64_t  sum_qty   = 0;
    double   sum_price = 0.0, sum_disc_price = 0.0, sum_charge = 0.0;
    int64_t  sum_disc  = 0;   // accumulate in hundredths, divide at end
    int64_t  count     = 0;
};
GroupAcc groups[4]; // groups[rf_code * 2 + ls_code]
```

### Aggregation Expressions (inline decoding)
```cpp
// Per row i (after shipdate filter):
int gid = (l_returnflag[i] << 1) | l_linestatus[i];
double qty    = l_quantity[i];               // int8 integer value
double price  = l_extendedprice[i];          // already double
double disc   = l_discount[i] * 0.01;       // int8 hundredths → fraction
double tax    = l_tax[i]      * 0.01;       // int8 hundredths → fraction
groups[gid].sum_qty        += l_quantity[i];
groups[gid].sum_price      += price;
groups[gid].sum_disc_price += price * (1.0 - disc);
groups[gid].sum_charge     += price * (1.0 - disc) * (1.0 + tax);
groups[gid].sum_disc       += l_discount[i];  // accumulate raw for avg
groups[gid].count++;
```

### Output Order
- Sort 4 group entries by (returnflag_code ASC, linestatus_code ASC)
- Dict codes preserve lexicographic order (A=0 < N=1 < R=2; F=0 < O=1) — no decode needed for ordering
- Decode chars from sidecar for final output only

## Indexes

### l_shipdate_zone_map (zone_map on l_shipdate)
- File: `lineitem/l_shipdate_zone_map.bin`
- Layout:
  ```
  [0..3]   uint32_t num_blocks   (= ceil(59986052 / 100000) = 600)
  [4..7]   uint32_t block_size   (= 100000)
  [8..]    ZoneEntry[num_blocks]
             struct ZoneEntry { int32_t min_date; int32_t max_date; }  // 8 bytes each
  ```
- Total file size: 8 + 600 × 8 = 4808 bytes
- **Usage for Q1**: Because lineitem is sorted by l_shipdate ASC, blocks near the end
  have `min_date > 10471`. Skip any block where `zone.min_date > 10471`.
  Iterate blocks 0..N-1; for each qualifying block scan rows `[b*100000 .. min((b+1)*100000, N))`.
- **Q1 effectiveness**: With selectivity ~98.5%, only the last few blocks are skipped.
  Zone map is still worth checking — it eliminates a small tail of work with zero overhead.

```cpp
// Zone map usage pattern:
uint32_t num_blocks, block_size;
fread(&num_blocks, 4, 1, zf);
fread(&block_size, 4, 1, zf);
struct ZE { int32_t mn, mx; };
std::vector<ZE> zones(num_blocks);
fread(zones.data(), sizeof(ZE), num_blocks, zf);

const int32_t THRESHOLD = 10471; // date_to_days("1998-09-02")
for (uint32_t b = 0; b < num_blocks; b++) {
    if (zones[b].mn > THRESHOLD) continue;    // entire block is above threshold → skip
    size_t row_start = (size_t)b * block_size;
    size_t row_end   = std::min(row_start + block_size, (size_t)total_rows);
    // scan rows [row_start, row_end)
}
```
