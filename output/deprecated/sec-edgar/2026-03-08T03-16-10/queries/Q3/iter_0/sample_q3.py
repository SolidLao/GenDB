import array
from pathlib import Path

BASE = Path("/home/jl4492/GenDB/output/sec-edgar/2026-03-08T03-16-10/gendb")
STRIDE = 100  # 1% sample


def load_array(path, typecode):
    a = array.array(typecode)
    with open(path, "rb") as f:
        a.frombytes(f.read())
    return a


def read_offsets(path):
    a = load_array(path, "Q")
    return a.tolist()


def dict_code(data_path, offsets_path, target):
    data = Path(data_path).read_bytes()
    offsets = read_offsets(offsets_path)
    needle = target.encode()
    for i in range(len(offsets) - 1):
        if data[offsets[i]:offsets[i + 1]] == needle:
            return i
    raise ValueError(target)


sub_adsh = load_array(BASE / "sub" / "adsh.bin", "I")
sub_fy = load_array(BASE / "sub" / "fy.bin", "i")
sub_cik = load_array(BASE / "sub" / "cik.bin", "i")
sub_name = load_array(BASE / "sub" / "name.bin", "I")
adsh_to_rowid = load_array(BASE / "sub" / "indexes" / "adsh_to_rowid.bin", "I")

usd_code = dict_code(BASE / "num" / "dict_uom.data.bin", BASE / "num" / "dict_uom.offsets.bin", "USD")
num_adsh = load_array(BASE / "num" / "adsh.bin", "I")
num_uom = load_array(BASE / "num" / "uom.bin", "H")

sub_2022_rows = [i for i, fy in enumerate(sub_fy) if fy == 2022]
sub_2022_row_set = set(sub_2022_rows)

sampled = 0
usd_rows = 0
qualifying_rows = 0
outer_groups = set()
inner_groups = set()
facts_per_sub = {}

for i in range(0, len(num_uom), STRIDE):
    sampled += 1
    if num_uom[i] != usd_code:
        continue
    usd_rows += 1
    sub_row = adsh_to_rowid[num_adsh[i]]
    if sub_row == 0xFFFFFFFF or sub_row not in sub_2022_row_set:
        continue
    qualifying_rows += 1
    outer_groups.add((sub_name[sub_row], sub_cik[sub_row]))
    inner_groups.add(sub_cik[sub_row])
    facts_per_sub[sub_row] = facts_per_sub.get(sub_row, 0) + 1

print({
    "rows_num": len(num_uom),
    "rows_sub": len(sub_fy),
    "usd_code": usd_code,
    "sub_fy_2022_rows": len(sub_2022_rows),
    "sample_stride": STRIDE,
    "sampled_num_rows": sampled,
    "sample_usd_rows": usd_rows,
    "sample_qualifying_join_rows": qualifying_rows,
    "sample_outer_groups": len(outer_groups),
    "sample_inner_groups": len(inner_groups),
    "sample_distinct_sub_rows_hit": len(facts_per_sub),
    "sample_avg_facts_per_hit_sub": (sum(facts_per_sub.values()) / len(facts_per_sub)) if facts_per_sub else 0.0,
})
