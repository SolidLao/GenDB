/* Q2: Minimum Cost Supplier */
SELECT
  s_acctbal,
  s_name,
  n_name,
  p_partkey,
  p_mfgr,
  s_address,
  s_phone,
  s_comment
FROM part, supplier, partsupp, nation, region
WHERE
  p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = :p_size_eq
  AND p_type LIKE :p_type_pattern
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = :r_name_eq
  AND ps_supplycost = (
    SELECT
      MIN(ps_supplycost)
    FROM partsupp, supplier, nation, region
    WHERE
      p_partkey = ps_partkey
      AND s_suppkey = ps_suppkey
      AND s_nationkey = n_nationkey
      AND n_regionkey = r_regionkey
      AND r_name = :r_name_eq_2
  )
ORDER BY
  s_acctbal DESC NULLS LAST,
  n_name NULLS FIRST,
  s_name NULLS FIRST,
  p_partkey NULLS FIRST
LIMIT :limit
