/* Q16: Parts/Supplier Relationship */
SELECT
  p_brand,
  p_type,
  p_size,
  COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM partsupp, part
WHERE
  p_partkey = ps_partkey
  AND p_brand <> :p_brand_neq
  AND NOT p_type LIKE :p_type_pattern
  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND NOT ps_suppkey IN (
    SELECT
      s_suppkey
    FROM supplier
    WHERE
      s_comment LIKE :s_comment_pattern
  )
GROUP BY
  p_brand,
  p_type,
  p_size
ORDER BY
  supplier_cnt DESC NULLS LAST,
  p_brand NULLS FIRST,
  p_type NULLS FIRST,
  p_size NULLS FIRST
