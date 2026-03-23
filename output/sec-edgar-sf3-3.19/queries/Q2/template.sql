/* Q2: joins=2, agg=True, sub=False, tables=2, rows=100, time=98.0ms */ /* Rewritten from correlated scalar subquery to decorrelated JOIN+GROUP BY. */ /* PostgreSQL cannot decorrelate this, causing O(N^2) nested-loop scans on num (39M rows), can not finish within 10 minute timeout */ /* DuckDB decorrelates automatically, but the explicit form is portable across all engines. */ /* Original: */ /*   SELECT s.name, n.tag, n.value */ /*   FROM num n */ /*   JOIN sub s ON n.adsh = s.adsh */ /*   WHERE n.uom = 'pure' AND s.fy = 2022 AND n.value IS NOT NULL */ /*         AND n.value = ( */ /*             SELECT MAX(n2.value) */ /*             FROM num n2 */ /*             WHERE n2.tag = n.tag AND n2.adsh = n.adsh AND n2.uom = 'pure' */ /*         ) */ /*   ORDER BY n.value DESC */ /*   LIMIT 100; */
SELECT
  s.name,
  n.tag,
  n.value
FROM num AS n
JOIN sub AS s
  ON n.adsh = s.adsh
JOIN (
  SELECT
    adsh,
    tag,
    MAX(value) AS max_value
  FROM num
  WHERE
    uom = :uom_eq_2 AND NOT value IS NULL
  GROUP BY
    adsh,
    tag
) AS m
  ON n.adsh = m.adsh AND n.tag = m.tag AND n.value = m.max_value
WHERE
  n.uom = :uom_eq AND s.fy = :fy_eq AND NOT n.value IS NULL
ORDER BY
  n.value DESC NULLS LAST,
  s.name NULLS FIRST,
  n.tag NULLS FIRST
LIMIT :limit
