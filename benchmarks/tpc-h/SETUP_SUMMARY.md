# TPC-H Benchmark Setup Summary

## ✅ Setup Complete!

### Database Configurations

#### PostgreSQL 18 (Fresh Installation)
- **Version**: PostgreSQL 18.2 (latest)
- **Cluster**: tpch
- **Port**: 5435
- **Status**: Online ✅

**Memory Configuration (Optimized for OLAP/TPC-H):**
```
shared_buffers = 40GB          (10% of 376 GB RAM)
effective_cache_size = 280GB   (75% of RAM, planner hint)
work_mem = 8GB                 (per sort/hash operation - aggressive!)
maintenance_work_mem = 8GB     (for VACUUM, CREATE INDEX)
```

**Why These Settings:**
- `shared_buffers = 40GB`: PostgreSQL best practice (25-40% of RAM max)
- `effective_cache_size = 280GB`: Tells planner about OS + PG cache
- `work_mem = 8GB`: Allows large TPC-H aggregations/sorts in memory
- Total memory usage comparable to DuckDB's 80% (300 GB)

#### DuckDB 1.4.4 LTS
- **Version**: 1.4.4 (LTS - latest, released Jan 26, 2026) ✅
- **Memory**: Uses 80% of RAM by default (~300 GB)
- **Type**: In-memory (no persistent cluster)

---

## Updated Files

### 1. `benchmark.py`
**Changes:**
- Updated PostgreSQL port: 5433 → **5435**
- Added comments identifying PostgreSQL 18 tpch cluster
- Updated documentation with setup prerequisites

**Key Lines Changed:**
```python
Line 106: "port": 5435,  # PostgreSQL 18 tpch cluster
Line 156: port=5435,     # PostgreSQL 18 tpch cluster
```

---

## System Comparison

| Component | PostgreSQL 18 | DuckDB 1.4.4 |
|-----------|---------------|--------------|
| **Version** | 18.2 (2024) | 1.4.4 LTS (Jan 2026) |
| **Type** | Client-server | Embedded |
| **shared_buffers** | 40 GB | N/A (uses OS memory) |
| **work_mem** | 8 GB per operation | ~300 GB total |
| **Total Memory** | ~40-80 GB (varies by query) | ~300 GB (80% of RAM) |
| **Port** | 5435 | N/A (in-process) |

---

## Running the Benchmark

### Basic Usage
```bash
cd /home/jl4492/GenDB
python3 benchmarks/tpc-h/benchmark.py --sf 1 --runs 3
```

### Options
```bash
--sf <N>              Scale factor (1, 10, etc.)
--runs <N>            Number of runs per query (default: 3)
--setup               Force reload data (drop & recreate databases)
--data-dir <path>     Path to TPC-H .tbl files
--gendb-run <path>    Path to GenDB run directory
--output <path>       Output plot path
```

### Example: Scale Factor 10 with 5 Runs
```bash
python3 benchmarks/tpc-h/benchmark.py --sf 10 --runs 5 --setup
```

---

## Database Management

### PostgreSQL 18
```bash
# Check status
pg_lsclusters

# Start cluster
sudo pg_ctlcluster 18 tpch start

# Stop cluster
sudo pg_ctlcluster 18 tpch stop

# Restart cluster
sudo pg_ctlcluster 18 tpch restart

# Connect to cluster
psql -h localhost -p 5435 -U postgres

# View configuration
psql -h localhost -p 5435 -U postgres -c "SHOW shared_buffers;"
```

### Existing Clusters (Not Used for Benchmarking)
```
PostgreSQL 10: Port 5432 (down, binaries missing)
PostgreSQL 12: Port 5433 (online, old config)
PostgreSQL 14: Port 5434 (online)
```

---

## Expected Results

With these optimized settings, you should see:

1. **PostgreSQL 18**: Better performance than old PostgreSQL 12
   - Improved query planner
   - More efficient memory usage
   - Better parallel query execution

2. **Fair Comparison**: Memory usage now comparable
   - PostgreSQL: 40 GB + work_mem operations
   - DuckDB: ~300 GB (80% of RAM)

3. **Query Performance**: Varies by query type
   - Q1 (aggregation): work_mem helps PostgreSQL
   - Q3 (join + aggregation): Both should perform well
   - Q6 (simple filter): DuckDB may be faster

---

## Troubleshooting

### PostgreSQL won't start
```bash
# Check logs
sudo journalctl -xeu postgresql@18-tpch.service -n 100

# Verify config
sudo less /etc/postgresql/18/tpch/postgresql.conf
```

### Connection refused
```bash
# Check if cluster is running
pg_lsclusters | grep tpch

# Check port
netstat -tlnp | grep 5435
```

### DuckDB not found
```bash
# Install DuckDB
pip3 install duckdb --upgrade
```

---

## Files Reference

```
benchmarks/tpc-h/
├── benchmark.py                    # Main benchmark script (UPDATED)
├── setup_postgresql_latest.sh     # PostgreSQL 18 setup script
├── setup_data.sh                   # TPC-H data generation
├── schema.sql                      # TPC-H table definitions
├── SETUP_SUMMARY.md               # This file
└── TUNING_INSTRUCTIONS.md         # Manual tuning guide
```

---

## Next Steps

1. ✅ PostgreSQL 18 installed and configured
2. ✅ DuckDB verified
3. ✅ benchmark.py updated
4. **Run benchmark**: `python3 benchmarks/tpc-h/benchmark.py --sf 1`
5. **Analyze results**: Check generated plots in `results/sf1/figures/`

Good luck with your benchmarking! 🚀
