/* Q6: Forecasting Revenue Change */
SELECT
  SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE
  l_shipdate >= :l_shipdate_lower
  AND l_shipdate < :l_shipdate_upper
  AND l_discount BETWEEN :l_discount_lower AND :l_discount_upper
  AND l_quantity < :l_quantity_upper
