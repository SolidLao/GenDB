/* Q17: Small-Quantity-Order Revenue */
SELECT
  CAST(SUM(l_extendedprice) AS DOUBLE PRECISION) / 7.0 AS avg_yearly
FROM lineitem, part
WHERE
  p_partkey = l_partkey
  AND p_brand = :p_brand_eq
  AND p_container = :p_container_eq
  AND l_quantity < (
    SELECT
      0.2 * AVG(l_quantity)
    FROM lineitem
    WHERE
      l_partkey = p_partkey
  )
