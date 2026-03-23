/* Q4: joins=3, agg=True, sub=False, tables=4, rows=500, time=420.4ms */
SELECT
  s.sic,
  t.tlabel,
  p.stmt,
  COUNT(DISTINCT s.cik) AS num_companies,
  SUM(n.value) AS total_value,
  AVG(n.value) AS avg_value
FROM num AS n
JOIN sub AS s
  ON n.adsh = s.adsh
JOIN tag AS t
  ON n.tag = t.tag AND n.version = t.version
JOIN pre AS p
  ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
WHERE
  n.uom = :uom_eq
  AND p.stmt = :stmt_eq
  AND s.sic BETWEEN :sic_lower AND :sic_upper
  AND NOT n.value IS NULL
  AND t.abstract = :abstract_eq
GROUP BY
  s.sic,
  t.tlabel,
  p.stmt
HAVING
  COUNT(DISTINCT s.cik) >= 2
ORDER BY
  total_value DESC NULLS LAST
LIMIT :limit
