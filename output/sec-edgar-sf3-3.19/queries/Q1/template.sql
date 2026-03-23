/* Q1: joins=0, agg=True, sub=False, tables=1, rows=14, time=56.0ms */
SELECT
  stmt,
  rfile,
  COUNT(*) AS cnt,
  COUNT(DISTINCT adsh) AS num_filings,
  AVG(line) AS avg_line_num
FROM pre
WHERE
  NOT stmt IS NULL
GROUP BY
  stmt,
  rfile
ORDER BY
  cnt DESC NULLS LAST
