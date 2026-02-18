# Q9 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100 for monetary columns
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### part
- Rows: 2,000,000, Block size: 50,000, Sort order: p_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | → |
| p_name | part/p_name.bin | int32_t | STRING | dictionary | → |

- Dictionary files: part/p_name_dict.txt

### supplier
- Rows: 100,000, Block size: 10,000, Sort order: s_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | → |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | → |

### nation
- Rows: 25, Block size: 100, Sort order: n_nationkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | → |
| n_name | nation/n_name.bin | int32_t | STRING | dictionary | → |

- Dictionary files: nation/n_name_dict.txt

### lineitem
- Rows: 59,986,052, Block size: 200,000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_partkey | lineitem/l_partkey.bin | int32_t | INTEGER | none | → |
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |

### partsupp
- Rows: 8,000,000, Block size: 100,000, Sort order: ps_partkey, ps_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | → |
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | → |
| ps_supplycost | partsupp/ps_supplycost.bin | int64_t | DECIMAL | none | 100 |

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |

## Indexes

### part_p_partkey (hash_single)
- File: indexes/part_p_partkey_hash.bin
- Type: hash_single
- Column: p_partkey (PK lookup and join key)

### supplier_s_nationkey (hash_multi_value)
- File: indexes/supplier_s_nationkey_hash.bin
- Type: hash_multi_value
- Column: s_nationkey (FK join: supplier.s_nationkey = nation.n_nationkey)

### lineitem_l_partkey (hash_multi_value)
- File: indexes/lineitem_l_partkey_hash.bin
- Type: hash_multi_value
- Column: l_partkey (FK join: lineitem.l_partkey = partsupp.ps_partkey)

### lineitem_l_suppkey (hash_multi_value)
- File: indexes/lineitem_l_suppkey_hash.bin
- Type: hash_multi_value
- Column: l_suppkey (FK join: lineitem.l_suppkey = partsupp.ps_suppkey)

### partsupp_ps_partkey (hash_multi_value)
- File: indexes/partsupp_ps_partkey_hash.bin
- Type: hash_multi_value
- Column: ps_partkey (FK join probe: part.p_partkey = partsupp.ps_partkey)

### partsupp_ps_suppkey (hash_multi_value)
- File: indexes/partsupp_ps_suppkey_hash.bin
- Type: hash_multi_value
- Column: ps_suppkey (FK join probe: supplier.s_suppkey = partsupp.ps_suppkey)

### orders_o_orderkey (hash_multi_value)
- File: indexes/orders_o_orderkey_hash.bin
- Type: hash_multi_value
- Column: o_orderkey (FK join probe: lineitem.l_orderkey = orders.o_orderkey)
