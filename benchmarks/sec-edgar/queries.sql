-- SEC EDGAR Benchmark Queries (Representative Subset)
-- 6 queries selected from 25 for diverse coverage:
--   0-join: Q1 (scan+agg)
--   1-join: Q24 (anti-join+date range)
--   2-join: Q2 (decorrelated max subquery), Q3 (nested agg subquery), Q6 (dominant join pattern)
--   3-join: Q4 (4 tables, HAVING, SIC range)

-- Q1: joins=0, agg=True, sub=False, tables=1, rows=14, time=56.0ms
SELECT stmt, rfile, COUNT(*) AS cnt,
       COUNT(DISTINCT adsh) AS num_filings,
       AVG(line) AS avg_line_num
FROM pre
WHERE stmt IS NOT NULL
GROUP BY stmt, rfile
ORDER BY cnt DESC;

-- Q2: joins=2, agg=True, sub=False, tables=2, rows=100, time=98.0ms
-- Rewritten from correlated scalar subquery to decorrelated JOIN+GROUP BY.
-- PostgreSQL cannot decorrelate this, causing O(N^2) nested-loop scans on num (39M rows), can not finish within 10 minute timeout
-- DuckDB decorrelates automatically, but the explicit form is portable across all engines.
-- Original:
--   SELECT s.name, n.tag, n.value
--   FROM num n
--   JOIN sub s ON n.adsh = s.adsh
--   WHERE n.uom = 'pure' AND s.fy = 2022 AND n.value IS NOT NULL
--         AND n.value = (
--             SELECT MAX(n2.value)
--             FROM num n2
--             WHERE n2.tag = n.tag AND n2.adsh = n.adsh AND n2.uom = 'pure'
--         )
--   ORDER BY n.value DESC
--   LIMIT 100;
SELECT s.name, n.tag, n.value
FROM num n
JOIN sub s ON n.adsh = s.adsh
JOIN (
    SELECT adsh, tag, MAX(value) AS max_value
    FROM num
    WHERE uom = 'pure' AND value IS NOT NULL
    GROUP BY adsh, tag
) m ON n.adsh = m.adsh AND n.tag = m.tag AND n.value = m.max_value
WHERE n.uom = 'pure' AND s.fy = 2022 AND n.value IS NOT NULL
ORDER BY n.value DESC, s.name, n.tag
LIMIT 100;

-- Q3: joins=2, agg=True, sub=True, tables=2, rows=100, time=228.0ms
SELECT s.name, s.cik, SUM(n.value) AS total_value
FROM num n
JOIN sub s ON n.adsh = s.adsh
WHERE n.uom = 'USD' AND s.fy = 2022 AND n.value IS NOT NULL
GROUP BY s.name, s.cik
HAVING SUM(n.value) > (
    SELECT AVG(sub_total) FROM (
        SELECT SUM(n2.value) AS sub_total
        FROM num n2
        JOIN sub s2 ON n2.adsh = s2.adsh
        WHERE n2.uom = 'USD' AND s2.fy = 2022 AND n2.value IS NOT NULL
        GROUP BY s2.cik
    ) avg_sub
)
ORDER BY total_value DESC
LIMIT 100;

-- Q4: joins=3, agg=True, sub=False, tables=4, rows=500, time=420.4ms
SELECT s.sic, t.tlabel, p.stmt,
       COUNT(DISTINCT s.cik) AS num_companies,
       SUM(n.value) AS total_value,
       AVG(n.value) AS avg_value
FROM num n
JOIN sub s ON n.adsh = s.adsh
JOIN tag t ON n.tag = t.tag AND n.version = t.version
JOIN pre p ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
WHERE n.uom = 'USD' AND p.stmt = 'EQ'
      AND s.sic BETWEEN 4000 AND 4999
      AND n.value IS NOT NULL AND t.abstract = 0
GROUP BY s.sic, t.tlabel, p.stmt
HAVING COUNT(DISTINCT s.cik) >= 2
ORDER BY total_value DESC
LIMIT 500;

-- Q6: joins=2, agg=True, sub=False, tables=4, rows=200, time=445.9ms
SELECT s.name, p.stmt, n.tag, p.plabel,
       SUM(n.value) AS total_value, COUNT(*) AS cnt
FROM num n
JOIN sub s ON n.adsh = s.adsh
JOIN pre p ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
WHERE n.uom = 'USD' AND p.stmt = 'IS' AND s.fy = 2023
      AND n.value IS NOT NULL
GROUP BY s.name, p.stmt, n.tag, p.plabel
ORDER BY total_value DESC
LIMIT 200;

-- Q24: joins=1, agg=True, sub=False, tables=2, rows=2, time=534.9ms
-- Rewritten from NOT EXISTS correlated subquery to LEFT JOIN anti-join.
-- MonetDB cannot decorrelate NOT EXISTS, causing it to hang on num (39M) x pre (9.6M).
-- The LEFT JOIN + IS NULL form is semantically equivalent and portable across all engines.
-- Original:
--   SELECT n.tag, n.version, COUNT(*) AS cnt, SUM(n.value) AS total
--   FROM num n
--   WHERE n.uom = 'USD' AND n.ddate BETWEEN 20230101 AND 20231231
--         AND n.value IS NOT NULL
--         AND NOT EXISTS (
--             SELECT 1 FROM pre p
--             WHERE p.tag = n.tag AND p.version = n.version AND p.adsh = n.adsh
--         )
--   GROUP BY n.tag, n.version
--   HAVING COUNT(*) > 10
--   ORDER BY cnt DESC
--   LIMIT 100;
SELECT n.tag, n.version, COUNT(*) AS cnt, SUM(n.value) AS total
FROM num n
LEFT JOIN pre p ON n.tag = p.tag AND n.version = p.version AND n.adsh = p.adsh
WHERE n.uom = 'USD' AND n.ddate BETWEEN 20230101 AND 20231231
      AND n.value IS NOT NULL
      AND p.adsh IS NULL
GROUP BY n.tag, n.version
HAVING COUNT(*) > 10
ORDER BY cnt DESC
LIMIT 100;
