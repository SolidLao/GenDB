
# Q4 Guide

## Column Reference
### num.adsh (filing_id, uint32_t, global_dict_u32)
- File: `num/adsh.bin` (39401761 rows)
- This query: `JOIN sub ON n.adsh = s.adsh` and `JOIN pre ON n.adsh = p.adsh` → direct equality on shared global filing codes.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### num.tag (xbrl_tag, uint32_t, global_dict_u32)
- File: `num/tag.bin` (39401761 rows)
- This query: `JOIN tag ON n.tag = t.tag` and `JOIN pre ON n.tag = p.tag` → direct equality on shared global tag codes; also part of the final grouping key through `t.tlabel`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### num.version (taxonomy_version, uint32_t, global_dict_u32)
- File: `num/version.bin` (39401761 rows)
- This query: `JOIN tag ON n.version = t.version` and `JOIN pre ON n.version = p.version` → direct equality on shared global version codes.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### num.uom (unit_of_measure, uint16_t, dict_u16)
- File: `num/uom.bin` (39401761 rows)
- This query: `n.uom = 'USD'` → `num_uom[rowid] == usd_code`, with `usd_code` loaded from `dicts/num_uom.*` at runtime.
- Dictionary files: `dicts/num_uom.offsets.bin` + `dicts/num_uom.data.bin`; runtime filters must resolve string literals by loading the dictionary, never by hardcoding category codes.

### num.value (numeric_fact, double, plain_f64)
- File: `num/value.bin` (39401761 rows)
- This query: `n.value IS NOT NULL`, `SUM(n.value)`, and `AVG(n.value)` → filter with `!std::isnan(num_value[rowid])`, then accumulate `sum` and `count` for average.

### sub.adsh (filing_id, uint32_t, global_dict_u32)
- File: `sub/adsh.bin` (86135 rows)
- This query: join target for `n.adsh = s.adsh`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### sub.sic (industry_code, int32_t, plain_i32)
- File: `sub/sic.bin` (86135 rows)
- This query: `s.sic BETWEEN 4000 AND 4999` and `GROUP BY s.sic` → compare and hash the stored `int32_t` directly.

### sub.cik (company_id, int32_t, plain_i32)
- File: `sub/cik.bin` (86135 rows)
- This query: `COUNT(DISTINCT s.cik)` → distinct set over the stored `int32_t` company id inside each result group.

### tag.tag (xbrl_tag, uint32_t, global_dict_u32)
- File: `tag/tag.bin` (1070662 rows)
- This query: join side of `(n.tag, n.version) = (t.tag, t.version)`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### tag.version (taxonomy_version, uint32_t, global_dict_u32)
- File: `tag/version.bin` (1070662 rows)
- This query: join side of `(n.tag, n.version) = (t.tag, t.version)`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### tag.tlabel (tag_label, uint32_t, dict_u32)
- File: `tag/tlabel.bin` (1070662 rows)
- This query: `SELECT t.tlabel` and `GROUP BY t.tlabel` → aggregate on the stored code, decode only for final rows.
- Dictionary files: `dicts/tag_tlabel.offsets.bin` + `dicts/tag_tlabel.data.bin`; runtime filters must resolve string literals by loading the dictionary, never by hardcoding category codes.

### tag.abstract (abstract_flag, int8_t, plain_i8)
- File: `tag/abstract.bin` (1070662 rows)
- This query: `t.abstract = 0` → C++ `tag_abstract[rowid] == 0`.

### pre.adsh (filing_id, uint32_t, global_dict_u32)
- File: `pre/adsh.bin` (9600799 rows)
- This query: join side of `(n.adsh, n.tag, n.version) = (p.adsh, p.tag, p.version)`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### pre.tag (xbrl_tag, uint32_t, global_dict_u32)
- File: `pre/tag.bin` (9600799 rows)
- This query: join side of `(n.adsh, n.tag, n.version) = (p.adsh, p.tag, p.version)`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### pre.version (taxonomy_version, uint32_t, global_dict_u32)
- File: `pre/version.bin` (9600799 rows)
- This query: join side of `(n.adsh, n.tag, n.version) = (p.adsh, p.tag, p.version)`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### pre.stmt (statement_type, uint16_t, dict_u16)
- File: `pre/stmt.bin` (9600799 rows)
- This query: `p.stmt = 'EQ'` and `GROUP BY p.stmt` → `pre_stmt[rowid] == eq_code`, with `eq_code` resolved at runtime from `dicts/pre_stmt.*`.
- Dictionary files: `dicts/pre_stmt.offsets.bin` + `dicts/pre_stmt.data.bin`; runtime filters must resolve string literals by loading the dictionary, never by hardcoding category codes.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| `num` | 39401761 | fact | `[]` | 100000 |
| `sub` | 86135 | dimension | `[]` | 100000 |
| `tag` | 1070662 | dimension | `[]` | 100000 |
| `pre` | 9600799 | fact | `[]` | 100000 |

## Query Analysis
- Filter derivation from `workload_analysis.json`: `num.uom = 'USD'` keeps about `33235385` rows, `pre.stmt = 'EQ'` keeps about `9600799 × 0.1294 ≈ 1242343` rows, and `sub.sic BETWEEN 4000 AND 4999` keeps about `86135 × 0.0609 ≈ 5246` rows.
- `tag.abstract = 0` has selectivity `1.0` in the workload analysis, so it is logically required but not a pruning lever for this dataset snapshot.
- The most selective early dimensions are `sub.sic` and `pre.stmt`; both can be applied before the large `num` fact stream is joined.
- Final groups are `(sic, tlabel_code, stmt_code)`, with per-group state `{distinct cik set, sum(value), count(value)}`; `AVG(value)` is `sum / count` at finalize time.

## Indexes
### num_uom_postings (value postings on `num.uom`)
- File set: `indexes/num/num_uom_postings.values.bin`, `indexes/num/num_uom_postings.offsets.bin`, `indexes/num/num_uom_postings.rowids.bin`
- Layout: sorted unique `uom` codes in `values.bin`, one-past-end `uint64_t` group boundaries in `offsets.bin`, grouped row ids in `rowids.bin`.
- Cardinalities on disk: `201` unique values, `202` offsets entries, `39401761` row ids.
- Empty-slot sentinel: none.
- Usage here: resolve `usd_code` from `dicts/num_uom.*`, then scan only the corresponding `num` row-id slice.

### sub_sic_postings (value postings on `sub.sic`)
- File set: `indexes/sub/sub_sic_postings.values.bin`, `indexes/sub/sub_sic_postings.offsets.bin`, `indexes/sub/sub_sic_postings.rowids.bin`
- Layout from `build_value_postings<int32_t>`: sorted non-null SIC codes, `offsets`, and grouped `rowids`.
- Cardinalities on disk: `406` unique SIC values, `407` offsets entries, `84920` non-null row ids.
- Empty-slot sentinel: none in the index payload; null SIC values are skipped during build.
- Usage here: range-filter by locating every group where `4000 <= values[g] <= 4999`, then union the corresponding row-id slices.

### sub_adsh_dense_lookup (dense lookup on `sub.adsh`)
- File: `indexes/sub/sub_adsh_dense_lookup.bin`
- Layout is a direct-addressed `uint32_t` vector keyed by global `adsh` code.
- Empty-slot sentinel: `std::numeric_limits<uint32_t>::max()`.
- Usage here: once a `num` row survives the `uom` filter, `lookup[num_adsh[rowid]]` returns the single `sub` row id used to fetch `sic` and `cik`.

### tag_tag_version_hash (open-addressed group hash on `(tag, version)`)
- File set: `indexes/tag/tag_tag_version_hash.rowids.bin`, `indexes/tag/tag_tag_version_hash.keys.bin`, `indexes/tag/tag_tag_version_hash.offsets.bin`, `indexes/tag/tag_tag_version_hash.hash.bin`
- Exact key and bucket layouts:
```cpp
struct PairKey {
    uint32_t a;
    uint32_t b;
};

struct PairBucket {
    uint32_t a;
    uint32_t b;
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

uint64_t hash_pair(uint32_t a, uint32_t b) {
    return mix64((static_cast<uint64_t>(a) << 32) ^ static_cast<uint64_t>(b));
}
```
- Exact probe-slot computation: `size_t slot = static_cast<size_t>(hash_pair(key.a, key.b) & (bucket_count - 1));` and collisions advance with `slot = (slot + 1) & (bucket_count - 1)`.
- Exact empty-slot sentinel: `kEmptyGroup = std::numeric_limits<uint64_t>::max()`.
- Build formula: `bucket_count` starts at `1` and doubles until it is at least `keys.size() * 2 + 1`; on disk the current table has `1070662` groups and `4194304` buckets.
- Multi-value format: `keys[group]` gives `(tag, version)`, `offsets[group] .. offsets[group + 1]` gives the slice in `rowids.bin`, and `hash.bin` stores open-addressed `PairBucket` entries whose `group_index` points back into `keys`/`offsets`.
- Usage here: probe `(num.tag, num.version)` to fetch the single `tag` row, then read `tlabel` and verify `abstract == 0`.

### pre_stmt_postings (value postings on `pre.stmt`)
- File set: `indexes/pre/pre_stmt_postings.values.bin`, `indexes/pre/pre_stmt_postings.offsets.bin`, `indexes/pre/pre_stmt_postings.rowids.bin`
- Layout: sorted unique statement codes, `offsets`, grouped `rowids`.
- Cardinalities on disk: `9` unique values, `10` offsets entries, `9600799` row ids.
- Empty-slot sentinel: none.
- Usage here: resolve `eq_code` from `dicts/pre_stmt.*`, then scan only the `pre` row ids whose statement code matches `EQ`.

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
- Build formula: `bucket_count` doubles from `1` until `bucket_count >= keys.size() * 2 + 1`; on disk the current table has `8631890` groups and `33554432` buckets.
- Multi-value format: `keys[group]` stores one `(adsh, tag, version)` triple, `offsets[group] .. offsets[group + 1]` selects all matching `pre` row ids, and `hash.bin` is the probe table from triple key to `group_index`.
- Usage here: after a `num` row passes `uom` and `sub`/`tag` predicates, probe the triple to fetch all matching `pre` rows, then keep only those whose `stmt` code equals `eq_code`.
