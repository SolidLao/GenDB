#!/bin/bash
#
# Setup Latest PostgreSQL from Official PostgreSQL APT Repository
# This script adds the PostgreSQL repo, installs PostgreSQL 18 (latest),
# and creates an optimized cluster for TPC-H benchmarking.
#
# System: 376 GB RAM, 64 cores
# Reference: https://www.postgresql.org/download/linux/ubuntu/
#

set -euo pipefail

echo "=== PostgreSQL 18 Setup for TPC-H Benchmarking ==="
echo ""
echo "This script will:"
echo "  1. Add official PostgreSQL APT repository"
echo "  2. Install PostgreSQL 18 (latest version)"
echo "  3. Create a new optimized cluster for TPC-H"
echo "  4. Apply memory tuning for fair comparison with DuckDB"
echo ""
read -p "Press Enter to continue or Ctrl+C to cancel..."

# Get Ubuntu version
UBUNTU_VERSION=$(lsb_release -cs)
echo ""
echo "Detected Ubuntu version: $UBUNTU_VERSION"

# Step 1: Add PostgreSQL APT Repository
echo ""
echo "[1/5] Adding official PostgreSQL APT repository..."

# Import PostgreSQL repository signing key
echo "Importing PostgreSQL GPG key..."
sudo apt install -y wget gnupg2
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -

# Add repository
echo "Adding PostgreSQL repository for Ubuntu $UBUNTU_VERSION..."
echo "deb http://apt.postgresql.org/pub/repos/apt/ ${UBUNTU_VERSION}-pgdg main" | \
    sudo tee /etc/apt/sources.list.d/pgdg.list

echo "✓ PostgreSQL repository added"

# Step 2: Update apt
echo ""
echo "[2/5] Updating apt package lists..."
sudo apt update

# Step 3: Check available PostgreSQL versions
echo ""
echo "[3/5] Checking available PostgreSQL versions..."
apt-cache search "^postgresql-[0-9]" | grep "^postgresql-[0-9][0-9]" | sort -V | tail -10

# Get latest version (should be 18)
LATEST_VERSION=$(apt-cache search "^postgresql-[0-9]" | grep -o "^postgresql-[0-9][0-9]*" | sed 's/postgresql-//' | sort -n | tail -1)
echo ""
echo "Latest available PostgreSQL version: $LATEST_VERSION"

# Step 4: Install PostgreSQL
echo ""
echo "[4/5] Installing PostgreSQL $LATEST_VERSION..."
sudo apt install -y postgresql-$LATEST_VERSION postgresql-client-$LATEST_VERSION

# Verify installation
if [ ! -f "/usr/lib/postgresql/$LATEST_VERSION/bin/postgres" ]; then
    echo "Error: PostgreSQL $LATEST_VERSION installation failed"
    exit 1
fi

echo "✓ PostgreSQL $LATEST_VERSION installed successfully"
/usr/lib/postgresql/$LATEST_VERSION/bin/postgres --version

# Step 5: Create optimized cluster for TPC-H
echo ""
echo "[5/5] Creating optimized PostgreSQL cluster for TPC-H..."

CLUSTER_NAME="tpch"
CLUSTER_PORT="5435"  # Use new port to avoid conflicts with existing instances

# Check if cluster already exists
if pg_lsclusters 2>/dev/null | grep -q "$LATEST_VERSION.*$CLUSTER_NAME"; then
    echo "Cluster '$CLUSTER_NAME' already exists. Dropping it..."
    sudo pg_dropcluster --stop $LATEST_VERSION $CLUSTER_NAME || true
fi

# Create new cluster
echo "Creating cluster: $LATEST_VERSION/$CLUSTER_NAME on port $CLUSTER_PORT"
sudo pg_createcluster $LATEST_VERSION $CLUSTER_NAME -p $CLUSTER_PORT -- --encoding=UTF8 --locale=C

# Apply optimized memory settings
PG_CONF="/etc/postgresql/$LATEST_VERSION/$CLUSTER_NAME/postgresql.conf"
echo ""
echo "Applying optimized memory settings to $PG_CONF..."

sudo tee -a "$PG_CONF" > /dev/null << 'EOF'

#------------------------------------------------------------------------------
# MEMORY SETTINGS FOR TPC-H BENCHMARKING
# System: 376 GB RAM, 64 cores
# Configured for fair comparison with DuckDB (which uses ~300 GB)
#------------------------------------------------------------------------------

# Memory Configuration (OLAP workload)
shared_buffers = 40GB                   # Main buffer pool (10% of RAM)
effective_cache_size = 280GB            # Planner hint (~75% of RAM)
work_mem = 8GB                          # Per sort/hash operation (aggressive for TPC-H)
maintenance_work_mem = 8GB              # For VACUUM, CREATE INDEX

# Connection Settings
max_connections = 100                   # Default

EOF

echo "✓ Memory settings applied:"
echo "    shared_buffers = 40GB"
echo "    effective_cache_size = 280GB"
echo "    work_mem = 8GB"
echo "    maintenance_work_mem = 8GB"

# Start the cluster
echo ""
echo "Starting PostgreSQL cluster..."
sudo pg_ctlcluster $LATEST_VERSION $CLUSTER_NAME start

# Wait for startup
sleep 3

# Verify cluster is running
echo ""
echo "Verifying cluster status..."
if pg_lsclusters | grep "$LATEST_VERSION.*$CLUSTER_NAME.*online"; then
    echo "✓ PostgreSQL $LATEST_VERSION cluster '$CLUSTER_NAME' is running on port $CLUSTER_PORT"
else
    echo "✗ Failed to start PostgreSQL cluster"
    echo "Checking logs..."
    sudo journalctl -xeu postgresql@$LATEST_VERSION-$CLUSTER_NAME.service -n 50
    exit 1
fi

# Verify settings
echo ""
echo "=== Verifying Configuration ==="
sudo -u postgres psql -p $CLUSTER_PORT -c "SELECT version();"
echo ""
echo "Memory settings:"
sudo -u postgres psql -p $CLUSTER_PORT -t -c "SHOW shared_buffers;" | xargs echo "  shared_buffers:"
sudo -u postgres psql -p $CLUSTER_PORT -t -c "SHOW effective_cache_size;" | xargs echo "  effective_cache_size:"
sudo -u postgres psql -p $CLUSTER_PORT -t -c "SHOW work_mem;" | xargs echo "  work_mem:"
sudo -u postgres psql -p $CLUSTER_PORT -t -c "SHOW maintenance_work_mem;" | xargs echo "  maintenance_work_mem:"

echo ""
echo "=== PostgreSQL $LATEST_VERSION Setup Complete! ==="
echo ""
echo "Cluster details:"
echo "  Version: PostgreSQL $LATEST_VERSION"
echo "  Cluster name: $CLUSTER_NAME"
echo "  Port: $CLUSTER_PORT"
echo "  Config: /etc/postgresql/$LATEST_VERSION/$CLUSTER_NAME/postgresql.conf"
echo "  Data: /var/lib/postgresql/$LATEST_VERSION/$CLUSTER_NAME"
echo ""
echo "Connection test:"
echo "  psql -h localhost -p $CLUSTER_PORT -U postgres"
echo ""
echo "Next steps:"
echo "  1. Update benchmarks/tpc-h/benchmark.py to use port $CLUSTER_PORT"
echo "  2. Run: python3 benchmarks/tpc-h/benchmark.py --sf 1"
echo ""
echo "To manage this cluster:"
echo "  Start:   sudo pg_ctlcluster $LATEST_VERSION $CLUSTER_NAME start"
echo "  Stop:    sudo pg_ctlcluster $LATEST_VERSION $CLUSTER_NAME stop"
echo "  Status:  pg_lsclusters"
echo ""
