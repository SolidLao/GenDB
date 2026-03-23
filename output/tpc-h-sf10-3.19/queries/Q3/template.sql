/* Q3: Shipping Priority */
SELECT
  l_orderkey,
  SUM(l_extendedprice * (
    1 - l_discount
  )) AS revenue,
  o_orderdate,
  o_shippriority
FROM customer, orders, lineitem
WHERE
  c_mktsegment = :c_mktsegment_eq
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < :o_orderdate_upper
  AND l_shipdate > :l_shipdate_lower
GROUP BY
  l_orderkey,
  o_orderdate,
  o_shippriority
ORDER BY
  revenue DESC NULLS LAST,
  o_orderdate NULLS FIRST
LIMIT :limit
