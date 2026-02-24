# Q4 Guide

## SQL
```sql
SELECT s.sic, t.tlabel, p.stmt,
       COUNT(DISTINCT s.cik) AS num_companies,
       SUM(n.value) AS total_value,
       AVG(n.value) AS avg_value
FROM num n
JOIN sub s ON n.adsh = s.adsh
JOIN tag t ON n.tag = t.tag AND n.version = t.version
JOIN pre p ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
WHERE n.uom = 'USD' AND p.stmt = 'EQ'
      AND s.sic BETWEEN 4000 AND 4999
      AND n.value IS NOT NULL AND t.abstract = 0
GROUP BY s.sic, t.tlabel, p.stmt
HAVING COUNT(DISTINCT s.cik) >= 2
ORDER BY total_value DESC
LIMIT 500;
```

## Column Reference

### num.adsh (dict_shared_adsh, int32_t)
- File: `num/adsh.bin` (39,401,761 rows)
- Dict: `num/adsh_dict.txt`
- **Key optimization**: adsh_code == sub_row_index → `int sub_row = num_adsh[i];` direct lookup

### num.uom (dict_local, int32_t)
- File: `num/uom.bin` (39,401,761 rows)
- Dict: `num/uom_dict.txt`
- This query: `uom = 'USD'` → find USD code; selectivity 87%

### num.value (double)
- File: `num/value.bin` (39,401,761 rows)
- This query: `value IS NOT NULL` → `!std::isnan(value)`; SUM + COUNT for AVG

### num.tag (dict_shared_tag, int32_t)
- File: `num/tag.bin` (39,401,761 rows)
- Dict: `num/tag_dict.txt` (shared, same codes as tag/tag.bin and pre/tag.bin)
- This query: join key to tag and pre tables

### num.version (dict_shared_version, int32_t)
- File: `num/version.bin` (39,401,761 rows)
- Dict: `num/version_dict.txt` (shared)
- This query: join key to tag and pre tables

### sub.sic (int32_t)
- File: `sub/sic.bin` (86,135 rows)
- This query: `sic BETWEEN 4000 AND 4999`; selectivity ~6.1% of sub → ~5,260 sub rows match
- GROUP BY key (part 1 of 3); output column

### sub.cik (int32_t)
- File: `sub/cik.bin` (86,135 rows)
- This query: `COUNT(DISTINCT s.cik)` per group

### tag.abstract (int32_t)
- File: `tag/abstract.bin` (1,070,662 rows)
- This query: `t.abstract = 0` (workload: 100% are 0 — non-selective in practice)

### tag.tlabel (dict_local, int32_t)
- File: `tag/tlabel.bin` (1,070,662 rows)
- Dict: `tag/tlabel_dict.txt`
- This query: GROUP BY key (part 2 of 3); output column (decode via tlabel_dict)

### pre.stmt (dict_local, int32_t)
- File: `pre/stmt.bin` (9,600,799 rows)
- Dict: `pre/stmt_dict.txt`
- This query: `p.stmt = 'EQ'`; selectivity ~12.9% of pre
- GROUP BY key (part 3 of 3); output column (decode via stmt_dict)

### pre.adsh (dict_shared_adsh, int32_t)
- File: `pre/adsh.bin` (9,600,799 rows)
- Same shared adsh dict → same codes as num.adsh

### pre.tag (dict_shared_tag, int32_t)
- File: `pre/tag.bin` (9,600,799 rows)
- Same shared tag dict → same codes as num.tag

### pre.version (dict_shared_version, int32_t)
- File: `pre/version.bin` (9,600,799 rows)
- Same shared version dict → same codes as num.version

## Table Stats
| Table | Rows       | Role             | Sort Order     | Block Size |
|-------|------------|------------------|----------------|------------|
| num   | 39,401,761 | fact (probe)     | (none)         | 100,000    |
| sub   | 86,135     | dim (build)      | adsh           | 100,000    |
| tag   | 1,070,662  | dim (build)      | tag, version   | 100,000    |
| pre   | 9,600,799  | fact (build)     | adsh           | 100,000    |

## Query Analysis

**Strategy — Build hash tables from smaller tables, probe num:**

**Step 1 — Build sub filter (sic BETWEEN 4000 AND 4999):**
- `bool sub_passes[86135]` — marks which sub rows satisfy sic filter
- `int32_t sub_sic_arr[86135]` — sic values for qualifying rows
- `int32_t sub_cik_arr[86135]` — cik values

**Step 2 — Build tag hash table (tag_code, ver_code) → (abstract, tlabel_code):**
- Use pre-built `tag/indexes/tagver_hash.bin`
- Format: [uint64_t ht_size][{tag_code, ver_code, row_idx, valid}...]
- Load tag.abstract.bin, tag.tlabel.bin alongside
- For this query, filter at lookup time: abstract==0 (non-selective but required)

**Step 3 — Build pre hash set/map (adsh_code, tag_code, ver_code) → stmt_code:**
- Filter: stmt == EQ_code (12.9% selectivity)
- Build hash table from pre with stmt='EQ' rows: key=(adsh,tag,ver) → stmt_code
- Key packing: `uint64_t k1 = ((uint64_t)(uint32_t)adsh_code << 32) | (uint32_t)tag_code; uint64_t k2 = ver_code;`
  - Or: 96-bit key stored as two uint64_t fields
- Estimated EQ rows: 9.6M × 12.9% = ~1.24M rows → hash table of ~2.5M slots

**Step 4 — Probe: scan num, apply all filters, aggregate:**
- For each num row: check uom==USD, !isnan(value)
- Direct sub lookup: `if (!sub_passes[adsh_code]) continue;`
- Tag lookup: probe tagver_hash for (tag_code, ver_code) → get row_idx; check abstract[row_idx]==0; get tlabel_code
- Pre lookup: probe pre_hash for (adsh_code, tag_code, ver_code) → get stmt_code; check stmt==EQ_code
- Group key: (sic=sub_sic[adsh_code], tlabel_code, stmt_code=EQ_code constant)
- Pack as: `uint64_t grp_key = ((uint64_t)(uint32_t)sic << 32) | (uint32_t)tlabel_code` (stmt is constant EQ)
- Accumulate: sum_value, count_value, set of distinct cik per group

**Step 5 — Apply HAVING (COUNT(DISTINCT cik) >= 2), sort, limit 500.**

## Indexes

### tag/indexes/tagver_hash.bin (hash on tag.tag, tag.version)
- File: `tag/indexes/tagver_hash.bin`
- Layout: `[uint64_t ht_size][{int32_t tag_code, int32_t ver_code, int32_t row_idx, int32_t valid}...]`
- Probe: `h = (tag_code * 2654435761ULL) ^ (ver_code * 2246822519ULL); slot = h & (ht_size-1);`
- C24: bounded probe loop `for(uint64_t p=0; p<ht_size; p++) { slot=(h+p)&mask; if(!valid) break; if(tc==tag&&vc==ver) return row_idx; }`
- Then access `tag.abstract[row_idx]` and `tag.tlabel[row_idx]`

## Implementation Notes
- For EQ filter code: `int eq_code = -1; for(int i=0;i<stmt_dict.size();i++) if(stmt_dict[i]=="EQ") eq_code=i;`
- For USD code: same pattern with uom_dict
- C15: Group key MUST include sic AND tlabel_code AND stmt (all 3 GROUP BY columns)
- COUNT(DISTINCT cik): use per-group hash set of cik values
- HAVING COUNT(DISTINCT cik) >= 2: filter after aggregation
- Expected output: ≤500 rows after HAVING filter
