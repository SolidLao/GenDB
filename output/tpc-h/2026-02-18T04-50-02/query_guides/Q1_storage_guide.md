# Q1 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor per column (see table below)
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### lineitem
- Rows: 59986052, Block size: 65536, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_returnflag | lineitem/l_returnflag.bin | int32_t | STRING | dictionary | - |
| l_linestatus | lineitem/l_linestatus.bin | int32_t | STRING | dictionary | - |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_tax | lineitem/l_tax.bin | int64_t | DECIMAL | none | 100 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | - |

- Dictionary files: l_returnflag → lineitem/l_returnflag_dict.txt, l_linestatus → lineitem/l_linestatus_dict.txt

## Indexes

### lineitem_shipdate_zonemap
- File: indexes/lineitem_shipdate_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_blocks] then per block: [int32_t min, int32_t max, uint64_t row_start, uint32_t row_count] (20 bytes/entry)
- Column: l_shipdate
- Num blocks: 916 (65536 rows/block)
- Usage: predicate l_shipdate <= DATE '1998-12-01' - 90 days; skip blocks whose max < threshold
