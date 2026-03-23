/* Q11: Important Stock Identification */
SELECT
  ps_partkey,
  SUM(ps_supplycost * ps_availqty) AS value
FROM partsupp, supplier, nation
WHERE
  ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = :n_name_eq
GROUP BY
  ps_partkey
HAVING
  SUM(ps_supplycost * ps_availqty) > (
    SELECT
      SUM(ps_supplycost * ps_availqty) * 0.0001000000
    FROM partsupp, supplier, nation
    WHERE
      ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = :n_name_eq_2
  )
ORDER BY
  value DESC NULLS LAST
