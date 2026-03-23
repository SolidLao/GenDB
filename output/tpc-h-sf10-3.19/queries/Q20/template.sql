/* Q20: Potential Part Promotion */
SELECT
  s_name,
  s_address
FROM supplier, nation
WHERE
  s_suppkey IN (
    SELECT
      ps_suppkey
    FROM partsupp
    WHERE
      ps_partkey IN (
        SELECT
          p_partkey
        FROM part
        WHERE
          p_name LIKE :p_name_pattern
      )
      AND ps_availqty > (
        SELECT
          0.5 * SUM(l_quantity)
        FROM lineitem
        WHERE
          l_partkey = ps_partkey
          AND l_suppkey = ps_suppkey
          AND l_shipdate >= :l_shipdate_lower
          AND l_shipdate < :l_shipdate_upper
      )
  )
  AND s_nationkey = n_nationkey
  AND n_name = :n_name_eq
ORDER BY
  s_name NULLS FIRST
