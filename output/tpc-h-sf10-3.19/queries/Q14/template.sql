/* Q14: Promotion Effect */
SELECT
  CAST(100.00 * SUM(
    CASE
      WHEN p_type LIKE 'PROMO%'
      THEN l_extendedprice * (
        1 - l_discount
      )
      ELSE 0
    END
  ) AS DOUBLE PRECISION) / SUM(l_extendedprice * (
    1 - l_discount
  )) AS promo_revenue
FROM lineitem, part
WHERE
  l_partkey = p_partkey
  AND l_shipdate >= :l_shipdate_lower
  AND l_shipdate < :l_shipdate_upper
