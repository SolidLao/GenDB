# Q9 Guide — Product Type Profit Measure

## Query
```sql
SELECT nation, o_year, SUM(amount) AS sum_profit
FROM (
    SELECT n_name AS nation,
           EXTRACT(YEAR FROM o_orderdate) AS o_year,
           l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity AS amount
    FROM part, supplier, lineitem, partsupp, orders, nation
    WHERE s_suppkey   = l_suppkey
      AND ps_suppkey  = l_suppkey
      AND ps_partkey  = l_partkey
      AND p_partkey   = l_partkey
      AND o_orderkey  = l_orderkey
      AND s_nationkey = n_nationkey
      AND p_name LIKE '%green%'
) AS profit
GROUP BY nation, o_year
ORDER BY nation ASC, o_year DESC;
```

## Table Stats
| Table    | Rows       | Role              | Sort Order     | Block Size |
|----------|------------|-------------------|----------------|------------|
| lineitem | 59,986,052 | fact/central      | l_shipdate ASC | 100,000    |
| part     | 2,000,000  | filter/dimension  | (none)         | 100,000    |
| partsupp | 8,000,000  | dimension         | (none)         | 100,000    |
| supplier | 100,000    | dimension         | (none)         | 100,000    |
| orders   | 15,000,000 | dimension         | (none)         | 100,000    |
| nation   | 25         | dimension         | (none)         | 25         |

## Column Reference

### p_partkey (`INTEGER`, `int32_t`, plain)
- File: `part/p_partkey.bin` (2,000,000 × 4 bytes = ~7.6 MB)
- This query: join key (part → lineitem via l_partkey); used to access partsupp index

### p_name (`VARCHAR(55)`, `char[56]`, fixed_56)
- File: `part/p_name.bin` (2,000,000 × 56 bytes = ~107 MB)
- Encoding: null-padded fixed 56-byte records (max 55 chars + 1 null byte)
- This query: `p_name LIKE '%green%'`
  - C++: `memchr(&p_name[i*56], 'g', 56) != nullptr` then verify substring, or use `memmem()`
  - Fastest: `strstr(p_name[i].data(), "green") != nullptr` (null termination guaranteed)
  - Selectivity: ~5.5% of part rows (≈110,000 parts have 'green' in name)
- **This is the primary filter** — build qualifying part set before anything else

### l_partkey (`INTEGER`, `int32_t`, plain)
- File: `lineitem/l_partkey.bin` (59,986,052 × 4 bytes = ~229 MB)
- This query: join key matching p_partkey; filter lineitem to green parts

### l_suppkey (`INTEGER`, `int32_t`, plain)
- File: `lineitem/l_suppkey.bin` (59,986,052 × 4 bytes = ~229 MB)
- This query: join key matching s_suppkey and ps_suppkey (dual use)

### l_orderkey (`INTEGER`, `int32_t`, plain)
- File: `lineitem/l_orderkey.bin` (59,986,052 × 4 bytes = ~229 MB)
- This query: join key to orders (→ o_orderdate for year extraction)

### l_extendedprice (`DECIMAL(15,2)`, `double`, plain_double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes = ~458 MB)
- This query: `l_extendedprice*(1-l_discount)` — part of amount computation

### l_discount (`DECIMAL(15,2)`, `int8_t`, int8_hundredths)
- File: `lineitem/l_discount.bin` (59,986,052 × 1 byte = ~57 MB)
- Encoding: value × 100 as int8_t (range 0–10)
- This query: decode inline: `double disc = l_discount[i] * 0.01;`
  - Amount contribution: `l_extendedprice[i] * (1.0 - disc) - ps_supplycost * l_quantity[i]`

### l_quantity (`DECIMAL(15,2)`, `int8_t`, int8_integer_value)
- File: `lineitem/l_quantity.bin` (59,986,052 × 1 byte = ~57 MB)
- Encoding: integer value 1–50 stored directly as int8_t
- This query: `ps_supplycost * l_quantity` — decode: `(double)l_quantity[i]`

### ps_partkey (`INTEGER`, `int32_t`, plain)
- File: `partsupp/ps_partkey.bin` (8,000,000 × 4 bytes = ~30.5 MB)
- This query: composite join key component for partsupp hash index lookup

### ps_suppkey (`INTEGER`, `int32_t`, plain)
- File: `partsupp/ps_suppkey.bin` (8,000,000 × 4 bytes = ~30.5 MB)
- This query: composite join key component for partsupp hash index lookup

### ps_supplycost (`DECIMAL(15,2)`, `double`, plain_double)
- File: `partsupp/ps_supplycost.bin` (8,000,000 × 8 bytes = ~61 MB)
- This query: `ps_supplycost * l_quantity` — fetched via partsupp hash index row_idx

### s_suppkey (`INTEGER`, `int32_t`, plain)
- File: `supplier/s_suppkey.bin` (100,000 × 4 bytes = ~381 KB)
- This query: PK for supplier_by_suppkey dense array lookup

### s_nationkey (`INTEGER`, `int32_t`, plain)
- File: `supplier/s_nationkey.bin` (100,000 × 4 bytes = ~381 KB)
- This query: join key → nation (to get n_name for grouping)

### o_orderdate (`DATE`, `int32_t`, days_since_epoch)
- File: `orders/o_orderdate.bin` (15,000,000 × 4 bytes = ~57 MB)
- Encoding: days since 1970-01-01
- This query: `EXTRACT(YEAR FROM o_orderdate)` → o_year
  - Decode to year:
    ```cpp
    // Days-since-epoch → Gregorian year (civil calendar reverse of date_to_days)
    int32_t z = o_orderdate[row] + 719468;
    int32_t era = (z >= 0 ? z : z - 146096) / 146097;
    int32_t doe = z - era * 146097;
    int32_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int32_t y   = yoe + era * 400;
    int32_t doy = doe - (365*yoe + yoe/4 - yoe/100);
    if (doy >= 306) y++;   // adjust for March-base epoch
    int o_year = (int)y;
    ```
  - Range: 1992–1998 (7 distinct years)

### n_nationkey (`INTEGER`, `int32_t`, plain)
- File: `nation/n_nationkey.bin` (25 × 4 bytes = 100 bytes)
- This query: join key from supplier.s_nationkey → nation row

### n_name (`CHAR(25)`, `char[26]`, fixed_26)
- File: `nation/n_name.bin` (25 × 26 bytes = 650 bytes)
- Encoding: null-padded fixed 26-byte records; trailing spaces are trimmed at ingest
- This query: GROUP BY nation (= n_name), ORDER BY nation ASC
  - Load entire nation table at startup (25 rows); store as array indexed by n_nationkey
  - C++ comparison for sort: `strncmp(a.nation, b.nation, 25)`

## Query Analysis

### Join Strategy (6-way join)

**Phase 1 — Build green-part set** (2M rows, 5.5% pass)
```cpp
// Scan part/p_partkey.bin and part/p_name.bin
std::vector<bool> green_part(2000001, false);
for (size_t i = 0; i < 2000000; i++)
    if (strstr(p_name[i].data(), "green"))
        green_part[p_partkey[i]] = true;
// ~110,000 qualifying part keys
```

**Phase 2 — Load lookup tables into memory**
- Load `supplier/supplier_by_suppkey.bin` (400 KB) → O(1) s_suppkey → row_idx
- Load `supplier/s_nationkey.bin` (400 KB) — 100K rows, trivial
- Load `nation/nation_by_nationkey.bin` (100 bytes) → O(1) nationkey → row_idx
- Load `nation/n_name.bin` (650 bytes) — 25 rows
- Build `int8_t suppkey_to_nation[100001]` directly: for each supplier row, store nationkey

**Phase 3 — Drive from lineitem (central fact table)**
For each lineitem row i (all 60M, no zone-map filter since no shipdate predicate):
```cpp
int32_t lpartkey = l_partkey[i];
if (!green_part[lpartkey]) continue;     // ~5.5% pass

int32_t lsuppkey = l_suppkey[i];

// Look up s_nationkey via suppkey (O(1) dense array):
int32_t srow = supplier_by_suppkey[lsuppkey];  // see Index below
int32_t nkey = s_nationkey[srow];

// Look up o_year via orders (O(1) dense array):
int32_t orow = orders_by_orderkey[l_orderkey[i]];
int32_t year = extract_year(o_orderdate[orow]);

// Look up ps_supplycost via partsupp hash index:
double sc = lookup_supplycost(lpartkey, lsuppkey);  // see Index below

// Compute amount:
double disc   = l_discount[i] * 0.01;
double amount = l_extendedprice[i] * (1.0 - disc) - sc * (double)l_quantity[i];

// Accumulate into group (nation_name, year):
// Key: (nkey, year) — 25 nations × 7 years = 175 groups max
int gid = nkey * 10 + (year - 1992);  // or use map<pair<int,int>, double>
profits[gid] += amount;
```

### Aggregation
- **175 groups maximum** (25 nations × 7 years)
- Direct array of 175 doubles: `double profits[25][10] = {};` (nation × year offset)
- Final output: sort by (n_name ASC, year DESC); decode names from n_name array

## Indexes

### part_by_partkey (dense_array on p_partkey)
- File: `part/part_by_partkey.bin`
- Layout: flat `int32_t` array, **2,000,001 entries** (indices 0..2,000,000)
  - `array[p_partkey] = row_index` into part column files
  - Sentinel: `-1` for unused slots
  - File size: 2,000,001 × 4 = **~7.6 MB**
- Built by `build_dense_index()` with `max_key = 2000000`
- **Usage for Q9**: Not needed for the main join (green_part bitset is sufficient).
  Used if a code path needs to access p_name by partkey rather than by row index.

### supplier_by_suppkey (dense_array on s_suppkey)
- File: `supplier/supplier_by_suppkey.bin`
- Layout: flat `int32_t` array, **100,001 entries** (indices 0..100,000)
  - `array[s_suppkey] = row_index` into supplier column files
  - Sentinel: `-1` for unused slots
  - File size: 100,001 × 4 = **~391 KB**
- Built by `build_dense_index()` with `max_key = 100000`
- **Usage for Q9**:
  ```cpp
  int32_t* supp_idx = ...; // load supplier_by_suppkey.bin
  // Per lineitem row (l_suppkey = lsuppkey):
  int32_t srow  = supp_idx[lsuppkey];          // O(1)
  int32_t nkey  = s_nationkey[srow];
  ```

### nation_by_nationkey (dense_array on n_nationkey)
- File: `nation/nation_by_nationkey.bin`
- Layout: flat `int32_t` array, **25 entries** (indices 0..24)
  - `array[n_nationkey] = row_index` into nation column files
  - File size: 25 × 4 = **100 bytes**
- Built by `build_dense_index()` with `max_key = 24`
- **Usage for Q9**:
  ```cpp
  int32_t* nat_idx = ...; // load nation_by_nationkey.bin (100 bytes)
  // Given nkey from supplier:
  int32_t nrow = nat_idx[nkey];
  // Access nation name:
  const char* name = n_name[nrow].data();  // char[26]
  ```
- Since only 25 nations, can precompute `suppkey_to_nationrow[100001]` at startup to
  avoid the two-level indirection on the hot path.

### orders_by_orderkey (dense_array on o_orderkey)
- File: `orders/orders_by_orderkey.bin`
- Layout: flat `int32_t` array, **60,000,001 entries**
  - `array[o_orderkey] = row_index` into orders column files
  - Sentinel: `-1`; file size: **240 MB**
- Built by `build_dense_index()` with `max_key = 60000000`
- **Usage for Q9**:
  ```cpp
  int32_t orow  = orders_by_orderkey[l_orderkey[i]]; // O(1)
  int32_t odate = o_orderdate[orow];
  int     year  = extract_year(odate);                // see formula above
  ```

### partsupp_hash_index (open-addressing hash on (ps_partkey, ps_suppkey))
- File: `partsupp/partsupp_hash_index.bin`
- **File layout** (written by `build_partsupp_hash_index()`):
  ```
  Bytes [0..7]:    uint64_t capacity  = 16,777,216  (= 2^24)
  Bytes [8..15]:   uint64_t count     = 8,000,000   (number of entries)
  Bytes [16..]:    HTSlot[16,777,216]
  ```
- **Slot struct** (`#pragma pack(push,1)`, 16 bytes):
  ```cpp
  struct HTSlot {
      uint64_t key;      // composite key (see below), 0 = empty sentinel
      int32_t  row_idx;  // index into ps_supplycost.bin
      int32_t  pad;      // alignment padding
  };
  static_assert(sizeof(HTSlot) == 16);
  ```
- **Composite key formula** (verbatim from build_indexes.cpp):
  ```cpp
  uint64_t key = (uint64_t)ps_partkey * 200001ULL + (uint64_t)ps_suppkey;
  ```
- **Empty sentinel**: `key == 0ULL` (partkey=0 is invalid in TPC-H)
- **Hash function** (Fibonacci hashing, verbatim from build_indexes.cpp):
  ```cpp
  static const uint32_t LOG2_CAP  = 24;
  static const uint64_t FIB_CONST = 11400714819323198485ULL;
  uint64_t slot = (key * FIB_CONST) >> (64 - LOG2_CAP);
  // Linear probe on collision:
  while (ht[slot].key != 0ULL) {
      slot = (slot + 1) & (CAPACITY - 1);  // CAPACITY = 1 << 24
  }
  ```
- **Load factor**: 8M / 16.7M ≈ 0.477
- **Total file size**: 16 + 16,777,216 × 16 = **256 MB + 16 bytes**
- **Usage for Q9** — lookup ps_supplycost:
  ```cpp
  // Load at startup (or mmap):
  uint64_t ht_capacity, ht_count;
  fread(&ht_capacity, 8, 1, f);
  fread(&ht_count,    8, 1, f);
  HTSlot* ht = ...; // read/mmap 16,777,216 slots × 16 bytes

  // Lookup function:
  double lookup_supplycost(int32_t partkey, int32_t suppkey,
                           const HTSlot* ht, const double* ps_supplycost) {
      uint64_t key  = (uint64_t)partkey * 200001ULL + (uint64_t)suppkey;
      uint64_t slot = (key * 11400714819323198485ULL) >> (64 - 24);
      while (ht[slot].key != 0ULL) {
          if (ht[slot].key == key)
              return ps_supplycost[ht[slot].row_idx];
          slot = (slot + 1) & ((1ULL << 24) - 1);
      }
      return 0.0; // key not found (should not happen for valid TPC-H data)
  }
  ```
