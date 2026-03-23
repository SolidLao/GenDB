/* Q15: Top Supplier */
WITH revenue0 AS (
  SELECT
    l_suppkey AS supplier_no,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS total_revenue
  FROM lineitem
  WHERE
    l_shipdate >= :l_shipdate_lower AND l_shipdate < :l_shipdate_upper
  GROUP BY
    l_suppkey
)
SELECT
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
FROM supplier, revenue0
WHERE
  s_suppkey = supplier_no
  AND total_revenue = (
    SELECT
      MAX(total_revenue)
    FROM revenue0
  )
ORDER BY
  s_suppkey NULLS FIRST
