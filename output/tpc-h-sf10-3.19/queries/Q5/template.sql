/* Q5: Local Supplier Volume */
SELECT
  n_name,
  SUM(l_extendedprice * (
    1 - l_discount
  )) AS revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE
  c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = :r_name_eq
  AND o_orderdate >= :o_orderdate_lower
  AND o_orderdate < :o_orderdate_upper
GROUP BY
  n_name
ORDER BY
  revenue DESC NULLS LAST
