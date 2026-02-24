# Q6 Guide

## SQL
```sql
SELECT s.name, p.stmt, n.tag, p.plabel,
       SUM(n.value) AS total_value, COUNT(*) AS cnt
FROM num n
JOIN sub s ON n.adsh = s.adsh
JOIN pre p ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
WHERE n.uom = 'USD' AND p.stmt = 'IS' AND s.fy = 2023
      AND n.value IS NOT NULL
GROUP BY s.name, p.stmt, n.tag, p.plabel
ORDER BY total_value DESC
LIMIT 200;
```

## Column Reference

### num.adsh (dict_shared_adsh, int32_t)
- File: `num/adsh.bin` (39,401,761 rows)
- **Key optimization**: adsh_code == sub_row_index → direct O(1) sub lookup

### num.uom (dict_local, int32_t)
- File: `num/uom.bin` (39,401,761 rows)
- Dict: `num/uom_dict.txt`
- This query: `uom = 'USD'`; find USD code at runtime; selectivity 87%

### num.value (double)
- File: `num/value.bin` (39,401,761 rows)
- This query: `value IS NOT NULL` → `!std::isnan(value)`; SUM per group

### num.tag (dict_shared_tag, int32_t)
- File: `num/tag.bin` (39,401,761 rows)
- Dict: `num/tag_dict.txt` (shared)
- This query: GROUP BY key (part 3 of 4); join key to pre; output (decode via tag_dict)

### num.version (dict_shared_version, int32_t)
- File: `num/version.bin` (39,401,761 rows)
- This query: join key to pre (composite join: adsh, tag, version)

### sub.fy (int32_t)
- File: `sub/fy.bin` (86,135 rows)
- This query: `s.fy = 2023`; selectivity ~30.9% → ~26,615 sub rows match

### sub.name (dict_local, int32_t)
- File: `sub/name.bin` (86,135 rows)
- Dict: `sub/name_dict.txt`
- This query: GROUP BY key (part 1 of 4); output (decode via name_dict)

### pre.stmt (dict_local, int32_t)
- File: `pre/stmt.bin` (9,600,799 rows)
- Dict: `pre/stmt_dict.txt`
- This query: `p.stmt = 'IS'`; selectivity ~18.4% of pre; GROUP BY key (part 2 of 4); output

### pre.adsh (dict_shared_adsh, int32_t)
- File: `pre/adsh.bin` (9,600,799 rows)

### pre.tag (dict_shared_tag, int32_t)
- File: `pre/tag.bin` (9,600,799 rows)

### pre.version (dict_shared_version, int32_t)
- File: `pre/version.bin` (9,600,799 rows)

### pre.plabel (dict_local, int32_t)
- File: `pre/plabel.bin` (9,600,799 rows)
- Dict: `pre/plabel_dict.txt`
- This query: GROUP BY key (part 4 of 4); output (decode via plabel_dict)

## Table Stats
| Table | Rows       | Role             | Sort Order | Block Size |
|-------|------------|------------------|------------|------------|
| num   | 39,401,761 | fact (probe)     | (none)     | 100,000    |
| sub   | 86,135     | dim (build)      | adsh       | 100,000    |
| pre   | 9,600,799  | fact (build)     | adsh       | 100,000    |

## Query Analysis

**Step 1 — Build sub filter (fy=2023):**
- `bool sub_fy2023[86135]` and `int32_t sub_name_arr[86135]`

**Step 2 — Build pre hash map (adsh_code, tag_code, ver_code) → (stmt_code, plabel_code):**
- Filter pre on stmt='IS' (18.4%): ~1.77M rows qualify
- Build hash table: key=(adsh,tag,ver) → (stmt_code=IS, plabel_code)
- Key pack: `struct PreKey { int32_t adsh, tag, ver; };` or split into two uint64_t
  - `uint64_t k1 = ((uint64_t)(uint32_t)adsh << 32) | (uint32_t)tag_code;`
  - `uint32_t k2 = ver_code;`
  - Combine: `uint64_t h = k1 * 6364136223846793005ULL ^ k2 * 1442695040888963407ULL;`
- Hash table size: `next_power_of_2(1770000 * 2) = 4194304` slots
- Each slot: {int32_t adsh, int32_t tag, int32_t ver, int32_t plabel_code, int32_t valid}

**Step 3 — Probe: scan num, apply filters, join:**
- For each num row: check uom==USD, !isnan(value)
- Check sub_fy2023[adsh_code]
- Probe pre hash map with (adsh, tag, ver) → get plabel_code
- Group key: (name_code=sub_name[adsh], stmt_code=IS_constant, tag_code, plabel_code)
- Pack: `uint64_t g1 = ((uint64_t)(uint32_t)name_code << 32) | (uint32_t)tag_code; uint64_t g2 = plabel_code;`
- Accumulate: sum_value, count

**Step 4 — Sort by total_value DESC, limit 200, decode output.**

## Indexes
No pre-built indexes used for this query. Pre hash map is built at runtime from pre.stmt-filtered rows.

## Implementation Notes
- IS code: `int is_code=-1; for(int i=0;i<stmt_dict.size();i++) if(stmt_dict[i]=="IS") is_code=i;`
- C15: Group key MUST include name_code, stmt_code, tag_code, AND plabel_code — all 4 GROUP BY columns
- C9: pre hash table capacity = next_power_of_2(1770000 * 2) = 4194304
- C24: bounded probe loop in pre hash table probing
- Output decode: `name_dict[sub_name[adsh_code]]`, `stmt_dict[stmt_code]`, `tag_dict[tag_code]`, `plabel_dict[plabel_code]`
- Load order: sub → pre (filter IS) → scan num
- MADV_SEQUENTIAL on all columns; sub columns small enough for MADV_RANDOM
