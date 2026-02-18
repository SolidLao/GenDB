# Q18 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100 for monetary columns
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### customer
- Rows: 1,500,000, Block size: 50,000, Sort order: c_custkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | → |
| c_name | customer/c_name.bin | int32_t | STRING | dictionary | → |

- Dictionary files: customer/c_name_dict.txt

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |
| o_totalprice | orders/o_totalprice.bin | int64_t | DECIMAL | none | 100 |

### lineitem
- Rows: 59,986,052, Block size: 200,000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### customer_c_custkey (hash_single)
- File: indexes/customer_c_custkey_hash.bin
- Type: hash_single
- Column: c_custkey (PK lookup and join key)

### orders_o_custkey (hash_multi_value)
- File: indexes/orders_o_custkey_hash.bin
- Type: hash_multi_value
- Column: o_custkey (FK join probe: customer.c_custkey = orders.o_custkey)

### orders_o_orderkey (hash_single)
- File: indexes/orders_o_orderkey_hash.bin
- Type: hash_single
- Column: o_orderkey (PK lookup in subquery phase)

### lineitem_l_orderkey (hash_multi_value)
- File: indexes/lineitem_l_orderkey_hash.bin
- Type: hash_multi_value
- Column: l_orderkey (FK join probe for GROUP BY HAVING subquery and main join)
