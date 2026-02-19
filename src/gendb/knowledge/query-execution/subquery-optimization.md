# Subquery Optimization

## What It Is

Techniques to transform expensive subquery patterns into more efficient join-based or materialized implementations. Correlated subqueries are often the biggest performance trap — they imply per-row re-execution but can almost always be decorrelated into a single join.

## Key Implementation Ideas

### Correlated Subquery → Join Decorrelation
- **The problem**: Correlated subqueries execute once per outer row — O(N × M) behavior.
  ```sql
  -- Correlated (slow): executes subquery for each order
  SELECT * FROM orders o WHERE o.total > (
    SELECT AVG(total) FROM orders o2 WHERE o2.customer_id = o.customer_id
  )
  ```
- **The fix**: Decorrelate into a join with a pre-aggregated table:
  ```sql
  -- Decorrelated (fast): single scan + join
  SELECT o.* FROM orders o
  JOIN (SELECT customer_id, AVG(total) as avg_total FROM orders GROUP BY customer_id) agg
  ON o.customer_id = agg.customer_id
  WHERE o.total > agg.avg_total
  ```
- **In C++**: Pre-compute the aggregation into a hash map, then probe during the outer scan.

### EXISTS → Semi-Join
- **`EXISTS (SELECT ... WHERE t2.key = t1.key)`** → hash semi-join: build hash table on inner, probe with outer, emit outer row on first match (no duplicates).
  ```cpp
  // Build phase: insert all inner keys into hash set
  std::unordered_set<int64_t> inner_keys;
  for (auto& row : inner) inner_keys.insert(row.key);
  // Probe phase: emit outer row if key exists
  for (auto& row : outer) {
      if (inner_keys.count(row.key)) emit(row);
  }
  ```
- **NOT EXISTS → Anti-join**: Same build, but emit outer row only if key is NOT in the hash set.

### IN Subquery → Hash Semi-Join
- **`WHERE x IN (SELECT y FROM t2)`** → build hash set from subquery result, probe with outer.
- **NOT IN with NULLs**: Careful — `NOT IN` with NULLs in the subquery returns no rows. Use anti-join with explicit NULL handling if the subquery column is nullable.

### Scalar Subquery Caching
- **For subqueries that return a single value per distinct correlation key**: Memoize results in a hash map.
  ```cpp
  std::unordered_map<int64_t, double> cache;
  for (auto& outer_row : outer) {
      auto it = cache.find(outer_row.key);
      if (it == cache.end()) {
          double val = compute_subquery(outer_row.key);
          cache[outer_row.key] = val;
          it = cache.find(outer_row.key);
      }
      // use it->second
  }
  ```
- **When caching helps**: When the number of distinct correlation keys is much smaller than the number of outer rows (e.g., 1000 customers but 1M orders).

### Window Function Optimization
- **Partition-based processing**: `OVER (PARTITION BY x ORDER BY y)` — hash-partition by x, sort each partition by y, compute window function in a single pass per partition.
- **Running aggregates**: For `SUM() OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`, maintain a running total — O(n) instead of O(n²).
- **RANK/ROW_NUMBER optimization**: Track rank during sorted scan — no need for a separate ranking pass.
- **Frame optimization**: For fixed-size frames (`ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING`), use a sliding window with O(1) update per row.

### Self-Referencing Aggregate Subquery (e.g., TPC-H Q18)
When a table's subquery references itself with aggregation:
```sql
-- Q18: orders where total lineitem quantity > 300
WHERE o_orderkey IN (
    SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
)
```
- **Implementation**: Single scan of lineitem to build hash map: `orderkey -> sum(quantity)`
- **Then filter**: Keep only orderkeys where sum > threshold
- **Common bug**: If the column is stored as double, the threshold matches SQL directly. If stored as int64_t with scale_factor, the threshold must be scaled (e.g., SQL 300 → C++ 300 * scale_factor).
- **Result**: A CompactHashSet of qualifying orderkeys for O(1) probe during the main join.

### Common Subexpression Elimination
- **Shared subquery results**: If the same subquery appears multiple times (or computes an intermediate result used by multiple outer expressions), materialize it once into a temporary structure.
- **CTE-style materialization**: Compute the common expression into a vector/hash map, reference it from multiple consumers.
