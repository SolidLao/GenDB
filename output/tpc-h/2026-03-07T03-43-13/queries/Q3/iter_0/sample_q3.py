import numpy as np, pathlib, json, time
base = pathlib.Path('/home/jl4492/GenDB/output/tpc-h/2026-03-07T03-43-13/gendb')
cut = 9204
rng = np.random.default_rng(0)
t0 = time.time()
# customer filter
offs = np.memmap(base/'customer/indexes/customer_mktsegment_postings.offsets.bin', dtype=np.uint64, mode='r')
rows = np.memmap(base/'customer/indexes/customer_mktsegment_postings.row_ids.bin', dtype=np.uint32, mode='r')
doff = np.memmap(base/'customer/c_mktsegment.dict.offsets.bin', dtype=np.uint64, mode='r')
ddat = np.memmap(base/'customer/c_mktsegment.dict.data.bin', dtype=np.uint8, mode='r')
code = next(i for i in range(len(doff)-1) if bytes(ddat[doff[i]:doff[i+1]]).decode() == 'BUILDING')
build_rows = rows[offs[code]:offs[code+1]]
custkeys = np.memmap(base/'customer/c_custkey.bin', dtype=np.int32, mode='r')
build_keys = custkeys[build_rows]
# orders via postings
ord_off = np.memmap(base/'orders/indexes/orders_custkey_postings.offsets.bin', dtype=np.uint64, mode='r')
ord_rows = np.memmap(base/'orders/indexes/orders_custkey_postings.row_ids.bin', dtype=np.uint32, mode='r')
odates = np.memmap(base/'orders/o_orderdate.bin', dtype=np.int32, mode='r')
ookeys = np.memmap(base/'orders/o_orderkey.bin', dtype=np.int32, mode='r')
# sample <=1% customers
ncs = min(15000, len(build_keys))
sc_idx = rng.choice(len(build_keys), size=ncs, replace=False)
sc_keys = build_keys[sc_idx]
order_row_chunks = []
orders_total = 0
for ck in sc_keys:
    s, e = ord_off[ck], ord_off[ck+1]
    orders_total += int(e - s)
    if e > s:
        order_row_chunks.append(np.asarray(ord_rows[s:e], dtype=np.uint32))
order_rows = np.concatenate(order_row_chunks) if order_row_chunks else np.empty(0, np.uint32)
order_pass = order_rows[odates[order_rows] < cut]
orderkeys_pass = ookeys[order_pass]
# lineitem join via grouped runs
lkeys = np.memmap(base/'lineitem/indexes/lineitem_orderkey_groups.keys.bin', dtype=np.int32, mode='r')
lstarts = np.memmap(base/'lineitem/indexes/lineitem_orderkey_groups.row_starts.bin', dtype=np.uint32, mode='r')
lcounts = np.memmap(base/'lineitem/indexes/lineitem_orderkey_groups.row_counts.bin', dtype=np.uint32, mode='r')
ship = np.memmap(base/'lineitem/l_shipdate.bin', dtype=np.int32, mode='r')
# sample <=1% orders from full table
nos = min(150000, len(orderkeys_pass))
so_idx = rng.choice(len(orderkeys_pass), size=nos, replace=False) if len(orderkeys_pass) > nos else np.arange(len(orderkeys_pass))
so_keys = orderkeys_pass[so_idx]
locs = np.searchsorted(lkeys, so_keys)
valid = (locs < len(lkeys)) & (lkeys[locs] == so_keys)
locs = locs[valid]
starts = lstarts[locs]
counts = lcounts[locs]
any_pass = 0
line_pass = 0
line_total = int(counts.sum())
for s, c in zip(starts, counts):
    seg = ship[s:s+c] > cut
    m = int(seg.sum())
    any_pass += (m > 0)
    line_pass += m
out = {
  'building_customers': int(len(build_keys)),
  'building_selectivity': float(len(build_keys) / 1500000),
  'sampled_customers': int(ncs),
  'orders_per_building_customer': float(orders_total / ncs),
  'orderdate_pass_given_building': float(len(order_pass) / len(order_rows)) if len(order_rows) else 0.0,
  'sampled_joined_orders': int(len(order_rows)),
  'sampled_orders_after_date': int(len(orderkeys_pass)),
  'sampled_orders_for_lineitem': int(len(so_keys)),
  'orders_with_any_shipdate_pass': float(any_pass / len(so_keys)) if len(so_keys) else 0.0,
  'lineitem_pass_fraction_within_order': float(line_pass / line_total) if line_total else 0.0,
  'avg_lineitems_per_order': float(line_total / len(so_keys)) if len(so_keys) else 0.0,
  'runtime_sec': round(time.time() - t0, 3),
}
print(json.dumps(out, indent=2))
