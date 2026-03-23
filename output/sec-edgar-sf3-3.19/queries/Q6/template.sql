/* Q6: joins=2, agg=True, sub=False, tables=4, rows=200, time=445.9ms */
SELECT
  s.name,
  p.stmt,
  n.tag,
  p.plabel,
  SUM(n.value) AS total_value,
  COUNT(*) AS cnt
FROM num AS n
JOIN sub AS s
  ON n.adsh = s.adsh
JOIN pre AS p
  ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
WHERE
  n.uom = :uom_eq AND p.stmt = :stmt_eq AND s.fy = :fy_eq AND NOT n.value IS NULL
GROUP BY
  s.name,
  p.stmt,
  n.tag,
  p.plabel
ORDER BY
  total_value DESC NULLS LAST
LIMIT :limit
