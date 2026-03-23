import json, numpy as np, time
base='/home/jl4492/GenDB/output/tpc-h/2026-03-07T03-43-13/gendb'
t0=time.time()
keys=np.memmap(f'{base}/lineitem/indexes/lineitem_orderkey_groups.keys.bin',dtype=np.int32,mode='r')
sums=np.memmap(f'{base}/lineitem/indexes/lineitem_orderkey_groups.sum_quantity.bin',dtype=np.int64,mode='r')
counts=np.memmap(f'{base}/lineitem/indexes/lineitem_orderkey_groups.row_counts.bin',dtype=np.uint32,mode='r')
orders_dense=np.memmap(f'{base}/orders/indexes/orders_pk_dense.bin',dtype=np.uint32,mode='r')
o_custkey=np.memmap(f'{base}/orders/o_custkey.bin',dtype=np.int32,mode='r')
customer_dense=np.memmap(f'{base}/customer/indexes/customer_pk_dense.bin',dtype=np.uint32,mode='r')
step=100
idx=np.arange(0,keys.shape[0],step,dtype=np.int64)
s_keys=keys[idx]
s_qual=sums[idx] > 30000
q_keys=s_keys[s_qual]
q_counts=counts[idx][s_qual]
order_rows=orders_dense[q_keys]
valid_orders=order_rows != np.uint32(2**32-1)
order_rows=order_rows[valid_orders]
q_counts=q_counts[valid_orders]
q_keys=q_keys[valid_orders]
cust_rows=customer_dense[o_custkey[order_rows]]
valid_customers=cust_rows != np.uint32(2**32-1)
result={
  'sample_groups': int(idx.shape[0]),
  'qualifying_groups': int(s_qual.sum()),
  'having_selectivity': float(s_qual.mean()),
  'orders_match_rate': float(valid_orders.mean()) if q_keys.size else 1.0,
  'customer_match_rate': float(valid_customers.mean()) if order_rows.size else 1.0,
  'avg_lineitems_per_qual_order': float(q_counts.mean()) if q_counts.size else 0.0,
  'max_lineitems_per_qual_order': int(q_counts.max()) if q_counts.size else 0,
  'runtime_ms': round((time.time()-t0)*1000,2),
}
print(json.dumps(result,indent=2))
