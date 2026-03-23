/* Q8: National Market Share */
SELECT
  o_year,
  CAST(SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) AS DOUBLE PRECISION) / SUM(volume) AS mkt_share
FROM (
  SELECT
    EXTRACT(YEAR FROM o_orderdate) AS o_year,
    l_extendedprice * (
      1 - l_discount
    ) AS volume,
    n2.n_name AS nation
  FROM part, supplier, lineitem, orders, customer, nation AS n1, nation AS n2, region
  WHERE
    p_partkey = l_partkey
    AND s_suppkey = l_suppkey
    AND l_orderkey = o_orderkey
    AND o_custkey = c_custkey
    AND c_nationkey = n1.n_nationkey
    AND n1.n_regionkey = r_regionkey
    AND r_name = :r_name_eq
    AND s_nationkey = n2.n_nationkey
    AND o_orderdate BETWEEN :o_orderdate_lower AND :o_orderdate_upper
    AND p_type = :p_type_eq
) AS all_nations
GROUP BY
  o_year
ORDER BY
  o_year NULLS FIRST
