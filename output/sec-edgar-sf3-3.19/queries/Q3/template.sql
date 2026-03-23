/* Q3: joins=2, agg=True, sub=True, tables=2, rows=100, time=228.0ms */
SELECT
  s.name,
  s.cik,
  SUM(n.value) AS total_value
FROM num AS n
JOIN sub AS s
  ON n.adsh = s.adsh
WHERE
  n.uom = :uom_eq AND s.fy = :fy_eq AND NOT n.value IS NULL
GROUP BY
  s.name,
  s.cik
HAVING
  SUM(n.value) > (
    SELECT
      AVG(sub_total)
    FROM (
      SELECT
        SUM(n2.value) AS sub_total
      FROM num AS n2
      JOIN sub AS s2
        ON n2.adsh = s2.adsh
      WHERE
        n2.uom = :uom_eq_2 AND s2.fy = :fy_eq_2 AND NOT n2.value IS NULL
      GROUP BY
        s2.cik
    ) AS avg_sub
  )
ORDER BY
  total_value DESC NULLS LAST
LIMIT :limit
