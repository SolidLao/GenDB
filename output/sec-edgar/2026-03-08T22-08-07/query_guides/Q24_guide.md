
# Q24 Guide

## Column Reference
### num.adsh (filing_id, uint32_t, global_dict_u32)
- File: `num/adsh.bin` (39401761 rows)
- This query: anti-join key component `n.adsh = p.adsh` inside the rewritten `LEFT JOIN`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### num.tag (xbrl_tag, uint32_t, global_dict_u32)
- File: `num/tag.bin` (39401761 rows)
- This query: anti-join key component `n.tag = p.tag` and final `GROUP BY n.tag, n.version`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### num.version (taxonomy_version, uint32_t, global_dict_u32)
- File: `num/version.bin` (39401761 rows)
- This query: anti-join key component `n.version = p.version` and final `GROUP BY n.tag, n.version`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### num.uom (unit_of_measure, uint16_t, dict_u16)
- File: `num/uom.bin` (39401761 rows)
- This query: `n.uom = 'USD'` → `num_uom[rowid] == usd_code`, with `usd_code` resolved at runtime from `dicts/num_uom.*`.
- Dictionary files: `dicts/num_uom.offsets.bin` + `dicts/num_uom.data.bin`; runtime filters must resolve string literals by loading the dictionary, never by hardcoding category codes.

### num.ddate (fact_date, int32_t, date_days_i32)
- File: `num/ddate.bin` (39401761 rows)
- This query: `n.ddate BETWEEN 20230101 AND 20231231` → compare encoded day counts, not raw `YYYYMMDD` integers.
- Exact ingest conversion: `parse_date_days()` calls `days_from_civil(year, month, day)` and stores days since `1970-01-01`; for this predicate the encoded bounds are `19358` and `19722`.

### num.value (numeric_fact, double, plain_f64)
- File: `num/value.bin` (39401761 rows)
- This query: `n.value IS NOT NULL` and `SUM(n.value)` → `!std::isnan(num_value[rowid])` before contributing to the aggregate bucket.

### pre.adsh (filing_id, uint32_t, global_dict_u32)
- File: `pre/adsh.bin` (9600799 rows)
- This query: anti-join probe target on the `pre` side.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### pre.tag (xbrl_tag, uint32_t, global_dict_u32)
- File: `pre/tag.bin` (9600799 rows)
- This query: anti-join probe target on the `pre` side.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### pre.version (taxonomy_version, uint32_t, global_dict_u32)
- File: `pre/version.bin` (9600799 rows)
- This query: anti-join probe target on the `pre` side.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| `num` | 39401761 | fact | `[]` | 100000 |
| `pre` | 9600799 | fact | `[]` | 100000 |

## Query Analysis
- Filter derivation from `workload_analysis.json`: `num.uom = 'USD'` and `num.ddate BETWEEN 20230101 AND 20231231` combine to about `39401761 × 0.8435 × 0.2716 ≈ 9026731` candidate `num` rows before the anti-join.
- `LEFT JOIN ... WHERE p.adsh IS NULL` is an existence probe on the persisted `pre` triple-group index; a probe miss means the `num` row survives.
- Runtime grouping key is `(num.tag_code, num.version_code)` and aggregate state is `{count_rows, sum_value}`.
- `HAVING COUNT(*) > 10` is applied after the anti-join and aggregation, so no persisted precomputation exists for it.

## Indexes
### num_uom_postings (value postings on `num.uom`)
- File set: `indexes/num/num_uom_postings.values.bin`, `indexes/num/num_uom_postings.offsets.bin`, `indexes/num/num_uom_postings.rowids.bin`
- Layout: sorted unique `uom` codes, `offsets`, grouped `rowids`.
- Cardinalities on disk: `201` unique values, `202` offsets entries, `39401761` row ids.
- Empty-slot sentinel: none.
- Usage here: resolve the `USD` code at runtime, then start from only the matching `num` row-id slice.

### num_ddate_postings (value postings on `num.ddate`)
- File set: `indexes/num/num_ddate_postings.values.bin`, `indexes/num/num_ddate_postings.offsets.bin`, `indexes/num/num_ddate_postings.rowids.bin`
- Actual layout from `build_value_postings<int32_t>`: sorted unique non-null encoded day values, `offsets`, and grouped `rowids`.
- Cardinalities on disk: `358` unique date values, `359` offsets entries, `39401761` row ids.
- Empty-slot sentinel: none in index storage; null dates are skipped during construction.
- Range formula: find all postings groups with `19358 <= values[g] <= 19722`, then union `rowids[offsets[g] .. offsets[g + 1])` across that span.
- Usage here: intersect the date-derived row-id stream with the `USD` row-id stream before probing `pre`.

### pre_adsh_tag_version_groups (open-addressed group hash on `(adsh, tag, version)`)
- File set: `indexes/pre/pre_adsh_tag_version_groups.rowids.bin`, `indexes/pre/pre_adsh_tag_version_groups.keys.bin`, `indexes/pre/pre_adsh_tag_version_groups.offsets.bin`, `indexes/pre/pre_adsh_tag_version_groups.hash.bin`
- Exact key and bucket layouts:
```cpp
struct TripleKey {
    uint32_t a;
    uint32_t b;
    uint32_t c;
};

struct TripleBucket {
    uint32_t a;
    uint32_t b;
    uint32_t c;
    uint64_t group_index;
};
```
- Exact hash computation copied from `build_indexes.cpp`:
```cpp
uint64_t mix64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ull;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
    return x ^ (x >> 31);
}

uint64_t hash_triple(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t seed = mix64(static_cast<uint64_t>(a) + 0x9e3779b97f4a7c15ull);
    seed ^= mix64((static_cast<uint64_t>(b) << 1) + 0x517cc1b727220a95ull);
    seed ^= mix64((static_cast<uint64_t>(c) << 7) + 0x94d049bb133111ebull);
    return mix64(seed);
}
```
- Exact probe-slot computation: `size_t slot = static_cast<size_t>(hash_triple(key.a, key.b, key.c) & (bucket_count - 1));` and collisions advance with `slot = (slot + 1) & (bucket_count - 1)`.
- Exact empty-slot sentinel: `kEmptyGroup = std::numeric_limits<uint64_t>::max()`.
- On-disk sizing: `8631890` groups, `8631891` offsets entries, `9600799` row ids, `33554432` buckets.
- Multi-value format: hash probe yields `group_index`; `keys[group_index]` is the triple and `rowids[offsets[group_index] .. offsets[group_index + 1])` are every `pre` row with that exact key.
- Usage here: for each filtered `num` row, probe `(adsh, tag, version)`; if the probe misses or lands on an empty bucket sequence, that is the `LEFT JOIN ... p.adsh IS NULL` case.
