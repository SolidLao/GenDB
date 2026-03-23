/* Q10: Returned Item Reporting */
SELECT
  c_custkey,
  c_name,
  SUM(l_extendedprice * (
    1 - l_discount
  )) AS revenue,
  c_acctbal,
  n_name,
  c_address,
  c_phone,
  c_comment
FROM customer, orders, lineitem, nation
WHERE
  c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= :o_orderdate_lower
  AND o_orderdate < :o_orderdate_upper
  AND l_returnflag = :l_returnflag_eq
  AND c_nationkey = n_nationkey
GROUP BY
  c_custkey,
  c_name,
  c_acctbal,
  c_phone,
  n_name,
  c_address,
  c_comment
ORDER BY
  revenue DESC NULLS LAST
LIMIT :limit
