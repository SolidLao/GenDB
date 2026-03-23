import json, numpy as np
from pathlib import Path

BASE = Path("/home/jl4492/GenDB/output/sec-edgar/2026-03-08T03-16-10/gendb")
EMPTY = np.uint32(2**32 - 1)

def mm(rel, dt): return np.memmap(BASE / rel, dtype=dt, mode="r")
def code(data_rel, off_rel, target):
    data = (BASE / data_rel).read_bytes()
    offs = np.fromfile(BASE / off_rel, dtype=np.uint64)
    for i in range(len(offs) - 1):
        if data[offs[i]:offs[i + 1]].decode() == target: return i
    raise KeyError(target)

usd, eq = code("num/dict_uom.data.bin", "num/dict_uom.offsets.bin", "USD"), code("pre/dict_stmt.data.bin", "pre/dict_stmt.offsets.bin", "EQ")
n = len(mm("num/adsh.bin", np.uint32))
step = 200
rows = np.arange(0, n, step, dtype=np.int64)
sampled = len(rows)

num_adsh, num_tag, num_ver = mm("num/adsh.bin", np.uint32), mm("num/tag.bin", np.uint32), mm("num/version.bin", np.uint32)
num_uom = mm("num/uom.bin", np.uint16)
sub_lookup, sub_sic, sub_cik = mm("sub/indexes/adsh_to_rowid.bin", np.uint32), mm("sub/sic.bin", np.int32), mm("sub/cik.bin", np.int32)
tag_key_t, tag_key_v, tag_rowid = mm("tag/indexes/tag_version_hash.tag.bin", np.uint32), mm("tag/indexes/tag_version_hash.version.bin", np.uint32), mm("tag/indexes/tag_version_hash.rowid.bin", np.uint32)
tag_abs = mm("tag/abstract.bin", np.uint8)
pre_adsh, pre_tag, pre_ver = mm("pre/adsh.bin", np.uint32), mm("pre/tag.bin", np.uint32), mm("pre/version.bin", np.uint32)
pre_stmt, pre_rowids = mm("pre/stmt.bin", np.uint16), mm("pre/indexes/adsh_tag_version.rowids.bin", np.uint32)
pre_keys = np.empty(len(pre_rowids), dtype=[("adsh", np.uint32), ("tag", np.uint32), ("ver", np.uint32)])
pre_keys["adsh"], pre_keys["tag"], pre_keys["ver"] = pre_adsh[pre_rowids], pre_tag[pre_rowids], pre_ver[pre_rowids]

def mix64(x):
    x ^= x >> 33; x *= np.uint64(0xff51afd7ed558ccd); x ^= x >> 33; x *= np.uint64(0xc4ceb9fe1a85ec53); x ^= x >> 33
    return x

usd_mask = num_uom[rows] == usd
usd_rows = int(np.count_nonzero(usd_mask))
sub_rows = tag_rows = pre_rows = pre_eq_keys = pre_matches = max_pre_eq_fanout = 0
for r in rows[usd_mask]:
    sr = sub_lookup[num_adsh[r]]
    if sr == EMPTY or not (4000 <= sub_sic[sr] <= 4999): continue
    sub_rows += 1
    cap = len(tag_rowid); slot = int(mix64((np.uint64(num_tag[r]) << 32) | np.uint64(num_ver[r])) & np.uint64(cap - 1))
    while tag_rowid[slot] != EMPTY and (tag_key_t[slot] != num_tag[r] or tag_key_v[slot] != num_ver[r]): slot = (slot + 1) & (cap - 1)
    if tag_rowid[slot] == EMPTY or tag_abs[tag_rowid[slot]] != 0: continue
    tag_rows += 1
    k = np.array((num_adsh[r], num_tag[r], num_ver[r]), dtype=pre_keys.dtype)
    lo = np.searchsorted(pre_keys, k, side="left")
    hi = np.searchsorted(pre_keys, k, side="right")
    if lo == hi: continue
    pre_rows += 1
    fanout = int(np.count_nonzero(pre_stmt[pre_rowids[lo:hi]] == eq))
    pre_eq_keys += int(fanout > 0)
    pre_matches += fanout
    max_pre_eq_fanout = max(max_pre_eq_fanout, fanout)

print(json.dumps({
    "sample_rows": int(sampled),
    "sample_rate": sampled / n,
    "usd_rows": usd_rows,
    "sub_rows": sub_rows,
    "tag_rows": tag_rows,
    "pre_rows": pre_rows,
    "pre_eq_keys": pre_eq_keys,
    "pre_eq_matches": pre_matches,
    "sel_usd": usd_rows / sampled,
    "sel_sub_given_usd": sub_rows / usd_rows if usd_rows else 0.0,
    "sel_tag_given_sub": tag_rows / sub_rows if sub_rows else 0.0,
    "sel_pre_hit_given_tag": pre_rows / tag_rows if tag_rows else 0.0,
    "sel_pre_eq_key_given_tag": pre_eq_keys / tag_rows if tag_rows else 0.0,
    "avg_pre_eq_fanout_given_tag": pre_matches / tag_rows if tag_rows else 0.0,
    "avg_pre_eq_fanout_given_pre_hit": pre_matches / pre_rows if pre_rows else 0.0,
    "max_pre_eq_fanout": max_pre_eq_fanout
}, indent=2))
