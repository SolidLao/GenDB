/* Q24: joins=1, agg=True, sub=False, tables=2, rows=2, time=534.9ms */ /* Rewritten from NOT EXISTS correlated subquery to LEFT JOIN anti-join. */ /* MonetDB cannot decorrelate NOT EXISTS, causing it to hang on num (39M) x pre (9.6M). */ /* The LEFT JOIN + IS NULL form is semantically equivalent and portable across all engines. */ /* Original: */ /*   SELECT n.tag, n.version, COUNT(*) AS cnt, SUM(n.value) AS total */ /*   FROM num n */ /*   WHERE n.uom = 'USD' AND n.ddate BETWEEN 20230101 AND 20231231 */ /*         AND n.value IS NOT NULL */ /*         AND NOT EXISTS ( */ /*             SELECT 1 FROM pre p */ /*             WHERE p.tag = n.tag AND p.version = n.version AND p.adsh = n.adsh */ /*         ) */ /*   GROUP BY n.tag, n.version */ /*   HAVING COUNT(*) > 10 */ /*   ORDER BY cnt DESC */ /*   LIMIT 100; */
SELECT
  n.tag,
  n.version,
  COUNT(*) AS cnt,
  SUM(n.value) AS total
FROM num AS n
LEFT JOIN pre AS p
  ON n.tag = p.tag AND n.version = p.version AND n.adsh = p.adsh
WHERE
  n.uom = :uom_eq
  AND n.ddate BETWEEN :ddate_lower AND :ddate_upper
  AND NOT n.value IS NULL
  AND p.adsh IS NULL
GROUP BY
  n.tag,
  n.version
HAVING
  COUNT(*) > 10
ORDER BY
  cnt DESC NULLS LAST
LIMIT :limit
