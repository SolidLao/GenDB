# Q4 Guide

## Column Reference
### num.adsh (shared_dict_id(adsh), uint32_t, dictionary_shared)
- File: `num/adsh.bin` (39401761 rows)
- Shared dictionary files: `shared/adsh.data.bin` + `shared/adsh.offsets.bin`
- This query: joins `num -> sub` on `adsh` and `num -> pre` on `(adsh,tag,version)`

### num.tag (shared_dict_id(tag), uint32_t, dictionary_shared)
- File: `num/tag.bin` (39401761 rows)
- Shared dictionary files: `shared/tag.data.bin` + `shared/tag.offsets.bin`
- This query: joins `num -> tag` on `(tag,version)` and `num -> pre` on `(adsh,tag,version)`

### num.version (shared_dict_id(version), uint32_t, dictionary_shared)
- File: `num/version.bin` (39401761 rows)
- Shared dictionary files: `shared/version.data.bin` + `shared/version.offsets.bin` (83815 values)
- This query: part of both the `tag` and `pre` joins

### num.uom (unit_of_measure, uint16_t, dictionary_local)
- File: `num/uom.bin` (39401761 rows)
- Dictionary files: `num/dict_uom.data.bin` + `num/dict_uom.offsets.bin`
- This query: `uom = 'USD'` -> resolve `usd_code` at runtime, then compare integer IDs

### num.value (numeric_fact, double, plain)
- File: `num/value.bin` (39401761 rows)
- This query: `SUM(value)`, `AVG(value)`
- Null semantics: `value IS NOT NULL` is a no-op on stored data because empty source values become `0.0`

### sub.sic (sic, int32_t, plain)
- File: `sub/sic.bin` (86135 rows)
- This query: `s.sic BETWEEN 4000 AND 4999`

### sub.cik (cik, int32_t, plain)
- File: `sub/cik.bin` (86135 rows)
- This query: `COUNT(DISTINCT s.cik)`

### tag.tag (shared_dict_id(tag), uint32_t, dictionary_shared)
- File: `tag/tag.bin` (1070662 rows)
- Shared dictionary files: `shared/tag.data.bin` + `shared/tag.offsets.bin`
- This query: part of the `(tag,version)` dimension lookup

### tag.version (shared_dict_id(version), uint32_t, dictionary_shared)
- File: `tag/version.bin` (1070662 rows)
- Shared dictionary files: `shared/version.data.bin` + `shared/version.offsets.bin`
- This query: part of the `(tag,version)` dimension lookup

### tag.tlabel (tag_label, uint32_t, dictionary_local)
- File: `tag/tlabel.bin` (1070662 rows)
- Dictionary files: `tag/dict_tlabel.data.bin` + `tag/dict_tlabel.offsets.bin` (240599 values)
- This query: `GROUP BY t.tlabel`; aggregate on integer dictionary ID, decode final result rows

### tag.abstract (boolean, uint8_t, plain)
- File: `tag/abstract.bin` (1070662 rows)
- This query: `t.abstract = 0`

### pre.adsh (shared_dict_id(adsh), uint32_t, dictionary_shared)
- File: `pre/adsh.bin` (9600799 rows)
- This query: part of the `(adsh,tag,version)` join key from `num` to `pre`

### pre.tag (shared_dict_id(tag), uint32_t, dictionary_shared)
- File: `pre/tag.bin` (9600799 rows)
- This query: part of the `(adsh,tag,version)` join key from `num` to `pre`

### pre.version (shared_dict_id(version), uint32_t, dictionary_shared)
- File: `pre/version.bin` (9600799 rows)
- This query: part of the `(adsh,tag,version)` join key from `num` to `pre`

### pre.stmt (statement_type, uint16_t, dictionary_local)
- File: `pre/stmt.bin` (9600799 rows)
- Dictionary files: `pre/dict_stmt.data.bin` + `pre/dict_stmt.offsets.bin`
- This query: `p.stmt = 'EQ'` and `GROUP BY p.stmt`; resolve the code for `"EQ"` at runtime from the dictionary

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| num | 39401761 | fact | none on base columns; separate sorted rowid index on `(adsh,tag,version)` | 100000 |
| sub | 86135 | dimension | none on base columns | 100000 |
| tag | 1070662 | dimension | none on base columns | 100000 |
| pre | 9600799 | fact | none on base columns; separate sorted rowid index on `(adsh,tag,version)` | 100000 |

## Query Analysis
- Q4 is the heaviest join shape in this set: `num` drives, `sub` filters by industry, `tag` filters by `abstract = 0`, and `pre` filters by `stmt = 'EQ'`.
- Workload selectivities: `num.uom='USD'` uses the `uom` zone map; `pre.stmt='EQ'` selectivity is `0.102`; `sub.sic BETWEEN 4000 AND 4999` selectivity is `0.09`; `tag.abstract=0` is `1.0`.
- Shared dictionaries make all join keys fixed-width integers.
- The `pre` join is 1:N. The sorted rowid index provides a contiguous key range, then query code scans all rowids in that range.
- `COUNT(DISTINCT s.cik)` is done on `int32_t` values after the `sub` lookup succeeds.

## Indexes
### uom_zone_map (zone_map on `num.uom`)
- File: `num/indexes/uom.zone_map.bin`
- Exact struct layout:
```cpp
template <typename T>
struct ZoneRecord {
    T min;
    T max;
};
```
- Instantiated type: `uint16_t`
- Empty-slot sentinel: none
- Usage: resolve `usd_code` from `num/dict_uom.*`, then keep only blocks whose zone contains that code

### sic_zone_map (zone_map on `sub.sic`)
- File: `sub/indexes/sic.zone_map.bin`
- Row coverage: `ceil(86135 / 100000) = 1` block
- Exact struct layout:
```cpp
template <typename T>
struct ZoneRecord {
    T min;
    T max;
};
```
- Instantiated type: `int32_t`
- Empty-slot sentinel: none
- Usage: confirms the only `sub` block spans the requested SIC range, so row-level filtering still happens after lookup

### stmt_zone_map (zone_map on `pre.stmt`)
- File: `pre/indexes/stmt.zone_map.bin`
- Row coverage: 97 blocks
- Exact struct layout:
```cpp
template <typename T>
struct ZoneRecord {
    T min;
    T max;
};
```
- Instantiated type: `uint16_t`
- Empty-slot sentinel: none
- Usage: after resolving `eq_code` from `pre/dict_stmt.*`, prune blocks whose `[min,max]` excludes `eq_code`

### adsh_to_rowid (dense lookup on `sub.adsh`)
- File: `sub/indexes/adsh_to_rowid.bin`
- Layout: one `uint32_t` rowid per shared `adsh` ID
- Exact implementation:
```cpp
std::vector<uint32_t> lookup(adsh_offsets.size() - 1, kEmpty32);
for (uint32_t row = 0; row < sub_adsh.size(); ++row) {
    lookup[sub_adsh[row]] = row;
}
```
- Empty-slot sentinel:
```cpp
constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
```
- Hash function: none
- Usage: `sub_row = lookup[num_adsh[row]]`, then test the `sic` range and read `cik`

### tag_version_hash (hash on `tag.tag`, `tag.version`)
- Files:
  `tag/indexes/tag_version_hash.tag.bin`
  `tag/indexes/tag_version_hash.version.bin`
  `tag/indexes/tag_version_hash.rowid.bin`
- Capacity formula from code:
```cpp
const uint64_t capacity = next_power_of_two(static_cast<uint64_t>(tags.size()) * 2 + 1);
```
- For the materialized `tag` table: `next_power_of_two(1070662 * 2 + 1) = 4194304` slots
- Physical layout: no struct; the index is three parallel `std::vector<uint32_t>` arrays named `key_tag`, `key_version`, and `rowids`
- Empty-slot sentinel:
```cpp
constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
```
- Exact hash computation and insertion logic:
```cpp
uint64_t slot = mix64((static_cast<uint64_t>(tags[row]) << 32) | versions[row]) & (capacity - 1);
while (rowids[slot] != kEmpty32) {
    ++slot;
    slot &= (capacity - 1);
}
```
- Exact hash mixer:
```cpp
uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}
```
- Probe pattern for this query: compute the same initial slot from `(num.tag[row], num.version[row])`, linearly probe until `rowids[slot] == kEmpty32` or both key arrays match, then read `tlabel` and `abstract`

### adsh_tag_version_sorted (sorted multi-value index on `pre`)
- File: `pre/indexes/adsh_tag_version.rowids.bin`
- Physical layout: one `uint32_t` rowid per `pre` row, file contains 9600799 rowids
- Exact build logic:
```cpp
std::vector<uint32_t> rowids(adsh.size());
std::iota(rowids.begin(), rowids.end(), 0);
std::sort(rowids.begin(), rowids.end(), [&](uint32_t lhs, uint32_t rhs) {
    return std::tie(adsh[lhs], tag[lhs], version[lhs], lhs) < std::tie(adsh[rhs], tag[rhs], version[rhs], rhs);
});
```
- Struct layout: none; this is a single rowid permutation file
- Empty-slot sentinel: none
- Multi-value format: all rows sharing the same `(adsh,tag,version)` are contiguous in `rowids.bin`; there is no offset table, count field, or sentinel separator
- This query usage: binary-search the sorted permutation on `(num.adsh[row], num.tag[row], num.version[row])`, then scan the contiguous run and retain only `pre` rows with `stmt == eq_code`

## Runtime Dictionary Loading Pattern
- Resolve `usd_code` from `num/dict_uom.*`.
- Resolve `eq_code` from `pre/dict_stmt.*`.
- Decode `tlabel` only for final grouped rows through `tag/dict_tlabel.*`.
- Never bake dictionary numeric IDs into generated code.

## Implementation Notes
- `tag.abstract = 0` is stored as plain `uint8_t`; the separate `abstract` zone map exists, but workload analysis says selectivity `1.0`, so its main value is format consistency rather than pruning.
- The `pre` sorted index is the only materialized 1:N join aid for the fact-to-fact join in Q4.
