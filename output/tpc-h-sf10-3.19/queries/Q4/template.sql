/* Q4: Order Priority Checking */
SELECT
  o_orderpriority,
  COUNT(*) AS order_count
FROM orders
WHERE
  o_orderdate >= :o_orderdate_lower
  AND o_orderdate < :o_orderdate_upper
  AND EXISTS(
    SELECT
      *
    FROM lineitem
    WHERE
      l_orderkey = o_orderkey AND l_commitdate < l_receiptdate
  )
GROUP BY
  o_orderpriority
ORDER BY
  o_orderpriority NULLS FIRST
