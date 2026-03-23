/* Q13: Customer Distribution */
SELECT
  c_count,
  COUNT(*) AS custdist
FROM (
  SELECT
    c_custkey,
    COUNT(o_orderkey) AS c_count
  FROM customer
  LEFT OUTER JOIN orders
    ON c_custkey = o_custkey AND NOT o_comment LIKE '%special%requests%'
  GROUP BY
    c_custkey
) AS c_orders
GROUP BY
  c_count
ORDER BY
  custdist DESC NULLS LAST,
  c_count DESC NULLS LAST
