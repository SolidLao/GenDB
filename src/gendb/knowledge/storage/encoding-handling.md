# Storage Encoding Handling

Columnar storage uses lightweight encodings to reduce size and improve I/O. Unlike compression, these allow direct querying on encoded data with minimal CPU cost.

**Critical:** Always check `storage_design.json` for column encodings. Incorrect decoding = wrong results.

## Dictionary Encoding

**When:** Low-cardinality columns (status flags, categories, country codes)

**Storage:**
- `column.bin`: uint8_t/uint16_t codes (0, 1, 2, ...)
- `column_dict.txt`: text mapping `code=value` (e.g., `0=N\n1=R\n2=A`)

**Implementation:**
```cpp
// Load dictionary (parse "code=value" lines)
std::unordered_map<uint8_t, char> dict;
std::ifstream f(dict_path);
std::string line;
while (std::getline(f, line)) {
    size_t eq = line.find('=');
    dict[std::stoi(line.substr(0, eq))] = line[eq + 1];
}

// Decode before use
char actual = dict[encoded_code];
if (actual == 'N') { /* ... */ }
```

**Pitfall:** ❌ `if (code == 'N')` compares 0 to ASCII 78. Always decode first.

## Delta Encoding

**When:** Sorted/monotonic columns (timestamps, sequential IDs)

**Storage:** `[base, delta1, delta2, ...]` where deltas are differences

**Implementation:**
```cpp
// Full decode
std::vector<int32_t> decoded(count);
decoded[0] = encoded[0];  // First is absolute
for (size_t i = 1; i < count; ++i)
    decoded[i] = decoded[i-1] + encoded[i];

// Morsel decode (memory-efficient)
decoded[0] = last_value + encoded[offset];
for (size_t i = 1; i < morsel_size; ++i)
    decoded[i] = decoded[i-1] + encoded[offset + i];
```

**Pitfall:** ❌ Using deltas directly in filters. Reconstruct absolute values first.

## Run-Length Encoding (RLE)

**When:** Long runs of repeated values (sorted flags, clustered categories)

**Storage:** `[(value1, count1), (value2, count2), ...]`

**Implementation:**
```cpp
// Direct aggregation (no expansion!)
for (auto [val, cnt] : rle_pairs)
    if (val == target) sum += val * cnt;
```

## Frame-of-Reference (FOR)

**When:** Small range integers (all values 1000-1100)

**Storage:** `base_value + offsets[]`

**Decode:** `actual[i] = base + offset[i]`

## Date Handling

**Storage format:** `int32_t` days since 1970-01-01

**Constants:** 1995-03-15 = 9204 days (NOT 19950315)

**Output formatting:**
```cpp
// Convert epoch days to YYYY-MM-DD
std::string format_date(int32_t days) {
    // Calculate year/month/day from days
    char buf[12];
    snprintf(buf, 12, "%04d-%02d-%02d", year, month, day);
    return buf;
}
```

**Pitfall:** ❌ Mixing YYYYMMDD integers with epoch days, or outputting raw epoch days.

## Decimal Column Handling

**When:** DECIMAL/NUMERIC columns (prices, quantities, rates, percentages).

**Option A: `double` (IEEE 754 float64)** — simpler, no scale tracking.
- Ingestion: `double val = strtod(ptr, &end);`
- Query-time: direct comparison and aggregation, values match SQL
- Tradeoff: 15-16 significant digits. Over millions of rows, SUM may accumulate
  small floating-point errors (typically < $100 on multi-billion aggregates).
  Suitable when the workload tolerates approximate results.

**Option B: `int64_t` with `scale_factor`** — exact decimal arithmetic.
- Ingestion: `int64_t val = llround(strtod(ptr, &end) * scale_factor);`
- Query-time: compare scaled values, divide by scale_factor for output
- Tradeoff: Requires careful scale tracking in ALL arithmetic. Common bugs:
  forgetting to scale thresholds, scale-squared on products, asymmetric scales.
  See experience base entries C3, C5, C14, C17.
  Required when results must be cent-exact (financial, regulatory).

**Choosing:** The Storage Designer selects encoding per workload. Default to `double`
unless the workload requires exact decimal precision.

## Checklist

Before column access:
1. Read `storage_design.json` for `"encoding"` field
2. Dictionary: Load `*_dict.txt`, decode before use
3. Delta: Implement cumulative sum decoder
4. RLE/FOR: Check if direct operation possible
5. None: mmap and use as-is

**Debugging:** Print first 10 decoded values, verify against expected data. Use `hexdump -C file.bin | head` to inspect raw data.
