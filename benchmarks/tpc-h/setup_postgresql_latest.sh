#!/bin/bash
#
# Setup Latest PostgreSQL from Official PostgreSQL APT Repository
# This script adds the PostgreSQL repo, installs PostgreSQL 18 (latest),
# and creates an optimized cluster for TPC-H benchmarking.
#
# If the cluster already exists, it only updates knob values (no data loss).
#
# System: 376 GB RAM, 64 cores
# Reference: https://www.postgresql.org/download/linux/ubuntu/
#

set -euo pipefail

CLUSTER_NAME="tpch"
CLUSTER_PORT="5435"

# ---------------------------------------------------------------------------
# Helper: detect PG version for an existing cluster
# ---------------------------------------------------------------------------
detect_existing_version() {
    pg_lsclusters 2>/dev/null | grep "$CLUSTER_NAME" | awk '{print $1}' | head -1
}

# ---------------------------------------------------------------------------
# Apply knob settings to postgresql.conf (idempotent)
# ---------------------------------------------------------------------------
apply_knob_settings() {
    local pg_version="$1"
    local PG_CONF="/etc/postgresql/$pg_version/$CLUSTER_NAME/postgresql.conf"

    echo ""
    echo "Applying optimized settings to $PG_CONF..."

    # Remove any previously appended TPC-H benchmark block to avoid duplicates
    sudo sed -i '/^#--- TPC-H BENCHMARK SETTINGS ---$/,/^#--- END TPC-H BENCHMARK SETTINGS ---$/d' "$PG_CONF"

    sudo tee -a "$PG_CONF" > /dev/null << 'EOF'
#--- TPC-H BENCHMARK SETTINGS ---
# System: 376 GB RAM, 64 cores

# Memory (OLAP workload)
shared_buffers = 40GB
effective_cache_size = 280GB
work_mem = 8GB
maintenance_work_mem = 8GB

# Parallelism (use all 64 cores)
max_worker_processes = 128
max_parallel_workers = 64
max_parallel_workers_per_gather = 64

# Connection Settings
max_connections = 100
#--- END TPC-H BENCHMARK SETTINGS ---
EOF

    echo "  shared_buffers = 40GB"
    echo "  effective_cache_size = 280GB"
    echo "  work_mem = 8GB"
    echo "  maintenance_work_mem = 8GB"
    echo "  max_worker_processes = 128"
    echo "  max_parallel_workers = 64"
    echo "  max_parallel_workers_per_gather = 64"
}

# ---------------------------------------------------------------------------
# Ensure pg_hba.conf allows local trust auth for benchmark
# ---------------------------------------------------------------------------
apply_hba_settings() {
    local pg_version="$1"
    local HBA_CONF="/etc/postgresql/$pg_version/$CLUSTER_NAME/pg_hba.conf"

    # Replace peer with trust for local connections so benchmark can connect as postgres
    if grep -q 'peer' "$HBA_CONF" 2>/dev/null; then
        echo "Updating pg_hba.conf: local peer -> trust"
        sudo sed -i '/^local/s/peer/trust/' "$HBA_CONF"
    fi
}

# ---------------------------------------------------------------------------
# Verify settings
# ---------------------------------------------------------------------------
verify_settings() {
    local pg_version="$1"
    echo ""
    echo "=== Verifying Configuration ==="
    sudo -u postgres psql -p $CLUSTER_PORT -c "SELECT version();"
    echo ""
    echo "Settings:"
    for knob in shared_buffers effective_cache_size work_mem maintenance_work_mem \
                max_worker_processes max_parallel_workers max_parallel_workers_per_gather; do
        sudo -u postgres psql -p $CLUSTER_PORT -t -c "SHOW $knob;" | xargs echo "  $knob:"
    done
}

# ===================================================================
# Main
# ===================================================================

echo "=== PostgreSQL Setup for TPC-H Benchmarking ==="

# Check if cluster already exists and is online
EXISTING_VERSION=$(detect_existing_version)

if [ -n "$EXISTING_VERSION" ]; then
    echo ""
    echo "Found existing cluster: PostgreSQL $EXISTING_VERSION/$CLUSTER_NAME on port $CLUSTER_PORT"

    # Check if it's running
    if pg_lsclusters | grep -q "$EXISTING_VERSION.*$CLUSTER_NAME.*online"; then
        echo "Cluster is running. Updating knob values only (no data loss)."
    else
        echo "Cluster exists but is not running. Updating knobs and starting..."
    fi

    apply_knob_settings "$EXISTING_VERSION"
    apply_hba_settings "$EXISTING_VERSION"

    echo ""
    echo "Restarting cluster to apply settings..."
    sudo pg_ctlcluster "$EXISTING_VERSION" "$CLUSTER_NAME" restart
    sleep 3

    if pg_lsclusters | grep "$EXISTING_VERSION.*$CLUSTER_NAME.*online"; then
        echo "PostgreSQL $EXISTING_VERSION cluster '$CLUSTER_NAME' is running on port $CLUSTER_PORT"
    else
        echo "Failed to start cluster. Check logs:"
        echo "  sudo journalctl -xeu postgresql@$EXISTING_VERSION-$CLUSTER_NAME.service -n 50"
        exit 1
    fi

    verify_settings "$EXISTING_VERSION"
    echo ""
    echo "=== Knob update complete (existing data preserved) ==="
    exit 0
fi

# -------------------------------------------------------------------
# Fresh install — no existing cluster found
# -------------------------------------------------------------------
echo ""
echo "No existing '$CLUSTER_NAME' cluster found. Will install and create from scratch."
echo ""
echo "This script will:"
echo "  1. Add official PostgreSQL APT repository"
echo "  2. Install PostgreSQL (latest version)"
echo "  3. Create a new optimized cluster for TPC-H"
echo "  4. Apply memory + parallelism tuning"
echo ""
read -p "Press Enter to continue or Ctrl+C to cancel..."

# Get Ubuntu version
UBUNTU_VERSION=$(lsb_release -cs)
echo ""
echo "Detected Ubuntu version: $UBUNTU_VERSION"

# Step 1: Add PostgreSQL APT Repository
echo ""
echo "[1/5] Adding official PostgreSQL APT repository..."
sudo apt install -y wget gnupg2
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
echo "deb http://apt.postgresql.org/pub/repos/apt/ ${UBUNTU_VERSION}-pgdg main" | \
    sudo tee /etc/apt/sources.list.d/pgdg.list

# Step 2: Update apt
echo ""
echo "[2/5] Updating apt package lists..."
sudo apt update

# Step 3: Find latest version
echo ""
echo "[3/5] Checking available PostgreSQL versions..."
LATEST_VERSION=$(apt-cache search "^postgresql-[0-9]" | grep -o "^postgresql-[0-9][0-9]*" | sed 's/postgresql-//' | sort -n | tail -1)
echo "Latest available PostgreSQL version: $LATEST_VERSION"

# Step 4: Install PostgreSQL
echo ""
echo "[4/5] Installing PostgreSQL $LATEST_VERSION..."
sudo apt install -y postgresql-$LATEST_VERSION postgresql-client-$LATEST_VERSION

if [ ! -f "/usr/lib/postgresql/$LATEST_VERSION/bin/postgres" ]; then
    echo "Error: PostgreSQL $LATEST_VERSION installation failed"
    exit 1
fi
echo "PostgreSQL $LATEST_VERSION installed successfully"
/usr/lib/postgresql/$LATEST_VERSION/bin/postgres --version

# Step 5: Create cluster
echo ""
echo "[5/5] Creating PostgreSQL cluster for TPC-H..."
echo "Creating cluster: $LATEST_VERSION/$CLUSTER_NAME on port $CLUSTER_PORT"
sudo pg_createcluster $LATEST_VERSION $CLUSTER_NAME -p $CLUSTER_PORT -- --encoding=UTF8 --locale=C

apply_knob_settings "$LATEST_VERSION"
apply_hba_settings "$LATEST_VERSION"

echo ""
echo "Starting PostgreSQL cluster..."
sudo pg_ctlcluster $LATEST_VERSION $CLUSTER_NAME start
sleep 3

if pg_lsclusters | grep "$LATEST_VERSION.*$CLUSTER_NAME.*online"; then
    echo "PostgreSQL $LATEST_VERSION cluster '$CLUSTER_NAME' is running on port $CLUSTER_PORT"
else
    echo "Failed to start PostgreSQL cluster"
    exit 1
fi

verify_settings "$LATEST_VERSION"

echo ""
echo "=== PostgreSQL $LATEST_VERSION Setup Complete! ==="
echo ""
echo "Cluster details:"
echo "  Version: PostgreSQL $LATEST_VERSION"
echo "  Cluster name: $CLUSTER_NAME"
echo "  Port: $CLUSTER_PORT"
echo "  Config: /etc/postgresql/$LATEST_VERSION/$CLUSTER_NAME/postgresql.conf"
echo ""
echo "To manage this cluster:"
echo "  Start:   sudo pg_ctlcluster $LATEST_VERSION $CLUSTER_NAME start"
echo "  Stop:    sudo pg_ctlcluster $LATEST_VERSION $CLUSTER_NAME stop"
echo "  Status:  pg_lsclusters"
echo ""
