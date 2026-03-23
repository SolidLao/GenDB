/* Q19: Discounted Revenue */
SELECT
  SUM(l_extendedprice * (
    1 - l_discount
  )) AS revenue
FROM lineitem, part
WHERE
  (
    p_partkey = l_partkey
    AND p_brand = :p_brand_eq_2
    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND l_quantity >= :l_quantity_lower_2
    AND l_quantity <= :l_quantity_upper_2
    AND p_size BETWEEN :p_size_lower_2 AND :p_size_upper_2
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = :l_shipinstruct_eq_2
  )
  OR (
    p_partkey = l_partkey
    AND p_brand = :p_brand_eq_3
    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    AND l_quantity >= :l_quantity_lower_3
    AND l_quantity <= :l_quantity_upper_3
    AND p_size BETWEEN :p_size_lower_3 AND :p_size_upper_3
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = :l_shipinstruct_eq_3
  )
  OR (
    p_partkey = l_partkey
    AND p_brand = :p_brand_eq
    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    AND l_quantity >= :l_quantity_lower
    AND l_quantity <= :l_quantity_upper
    AND p_size BETWEEN :p_size_lower AND :p_size_upper
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = :l_shipinstruct_eq
  )
