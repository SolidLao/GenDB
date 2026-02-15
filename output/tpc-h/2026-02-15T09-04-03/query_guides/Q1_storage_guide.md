# Q1 Storage Guide

## Tables

### lineitem
- Rows: 59,986,052, Block size: 500,000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_partkey | lineitem/l_partkey.bin | int32_t | INTEGER | none | → |
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | → |
| l_linenumber | lineitem/l_linenumber.bin | int32_t | INTEGER | none | → |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_tax | lineitem/l_tax.bin | int64_t | DECIMAL | none | 100 |
| l_returnflag | lineitem/l_returnflag.bin | uint8_t | STRING | dictionary | → |
| l_linestatus | lineitem/l_linestatus.bin | uint8_t | STRING | dictionary | → |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |
| l_commitdate | lineitem/l_commitdate.bin | int32_t | DATE | none | → |
| l_receiptdate | lineitem/l_receiptdate.bin | int32_t | DATE | none | → |
| l_shipinstruct | lineitem/l_shipinstruct.bin | std::string | STRING | none | → |
| l_comment | lineitem/l_comment.bin | std::string | STRING | none | → |

- Dictionary files: l_returnflag → lineitem/l_returnflag_dict.txt, l_linestatus → lineitem/l_linestatus_dict.txt

## Indexes

### lineitem_shipdate_zone
- File: indexes/lineitem_shipdate_zone.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then per zone: [int32_t min_val, int32_t max_val, uint32_t block_num, uint32_t row_count] (16B/entry)
- 120 zones total
- Column: l_shipdate

### lineitem_orderkey_hash
- File: indexes/lineitem_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique (15000000)][uint32_t table_size (30000000)] then [int32_t key, uint32_t offset, uint32_t count] per hash slot (12B), then [uint32_t positions...] array
- Column: l_orderkey
