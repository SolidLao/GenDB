/* Q7: Volume Shipping */
SELECT
  supp_nation,
  cust_nation,
  l_year,
  SUM(volume) AS revenue
FROM (
  SELECT
    n1.n_name AS supp_nation,
    n2.n_name AS cust_nation,
    EXTRACT(YEAR FROM l_shipdate) AS l_year,
    l_extendedprice * (
      1 - l_discount
    ) AS volume
  FROM supplier, lineitem, orders, customer, nation AS n1, nation AS n2
  WHERE
    s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND (
      (
        n1.n_name = :n_name_eq AND n2.n_name = :n_name_eq_2
      )
      OR (
        n1.n_name = :n_name_eq_3 AND n2.n_name = :n_name_eq_4
      )
    )
    AND l_shipdate BETWEEN :l_shipdate_lower AND :l_shipdate_upper
) AS shipping
GROUP BY
  supp_nation,
  cust_nation,
  l_year
ORDER BY
  supp_nation NULLS FIRST,
  cust_nation NULLS FIRST,
  l_year NULLS FIRST
