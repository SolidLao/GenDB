---
name: research-papers
description: Seminal research paper references for database systems. Load when planning complex query optimizations, join ordering, or making architectural decisions. Covers query optimization, hash joins, indexing, parallelism, storage, code generation.
user-invocable: false
---

# Skill: Research Papers

## When to Load
Query Planner, Query Optimizer — triggers deep knowledge for complex optimization decisions.

## Query Optimization
- Selinger et al. 1979 — "Access Path Selection in a Relational DBMS" (System R optimizer, dynamic programming for join ordering)
- Moerkotte & Neumann 2006 — "Analysis of Two Existing and One New Dynamic Programming Algorithm" (DPccp for connected subgraph complement pairs)
- Neumann 2009 — "Query Simplification: Graceful Degradation for Join-Order Optimization" (greedy fallback for large join graphs)
- Leis et al. 2015 — "How Good Are Query Optimizers, Really?" (cardinality estimation errors, JOB benchmark)
- Dreseler et al. 2020 — "Quantifying TPC-H Choke Points and Their Optimizations" (28 choke points in 6 categories, Hyrise-based evaluation)
- Boncz et al. 2013 — "TPC-H Analyzed: Hidden Messages and Lessons" (choke point taxonomy, benchmark pitfalls)

## Hash Joins & Join Processing
- Balkesen et al. 2013 — "Main-Memory Hash Joins on Multi-Core CPUs: Tuning to the Underlying Hardware"
- Blanas et al. 2011 — "Design and Evaluation of Main Memory Hash Join Algorithms for Multi-Core CPUs"
- Ngo et al. 2012 — "Worst-Case Optimal Join Algorithms" (AGM bound, Leapfrog TrieJoin)
- Schuh et al. 2016 — "An Experimental Comparison of Thirteen Relational Equi-Joins in Main Memory"
- Bernstein & Goodman 1981 — "Power of Natural Semijoins" (semi-join reduction)

## Indexing
- Graefe 2011 — "Modern B-Tree Techniques"
- Sidirourgos & Kersten 2013 — "Column Imprints: A Secondary Index Structure"
- Bloom 1970 — "Space/Time Trade-offs in Hash Coding with Allowable Errors" (Bloom filters)
- Putze et al. 2009 — "Cache-, Hash-, and Space-Efficient Bloom Filters"

## Parallelism & Execution
- Leis et al. 2014 — "Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework for the Many-Core Age"
- Polychroniou et al. 2015 — "Rethinking SIMD Vectorization for In-Memory Databases"
- Kersten et al. 2018 — "Everything You Always Wanted to Know About Compiled and Vectorized Queries But Were Afraid to Ask"

## Storage & Column Stores
- Abadi et al. 2008 — "Column-Stores vs. Row-Stores: How Different Are They Really?"
- Abadi et al. 2013 — "The Design and Implementation of Modern Column-Oriented Database Systems"
- Idreos et al. 2012 — "MonetDB: Two Decades of Research in Column-Oriented Database Architectures"
- Lang et al. 2016 — "Data Blocks: Hybrid OLTP and OLAP on Compressed Column-Main Memory Databases"

## Aggregation
- Ye et al. 2011 — "Scalable Aggregation on Multicore Processors"
- Müller et al. 2012 — "Adaptive and Big Data Scale Parallel Execution in Oracle"

## Code Generation
- Neumann 2011 — "Efficiently Compiling Efficient Query Plans for Modern Hardware" (HyPer, data-centric compilation)
- Shaikhha et al. 2016 — "How to Architect a Query Compiler" (LB2, multi-stage compilation)
- Tahboub et al. 2018 — "How to Architect a Query Compiler, Revisited"

## Systems
- Raasveldt & Mühleisen 2019 — "DuckDB: An Embeddable Analytical Database" (vectorized push-based)
- Neumann & Freitag 2020 — "Umbra: A Disk-Based System with In-Memory Performance"
- Boncz et al. 2005 — "MonetDB/X100: Hyper-Pipelining Query Execution" (vectorized execution)
- Zukowski et al. 2012 — "Vectorwise: A Vectorized Analytical DBMS"
