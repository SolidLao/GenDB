#!/bin/bash
# Upgrade MonetDB from v11.39.7 (Oct2020) to latest (Dec2025 / v11.55)
# Run with: sudo bash benchmarks/sec-edgar/upgrade_monetdb.sh

set -e

echo "=== Current MonetDB version ==="
monetdbd --version 2>/dev/null || echo "monetdbd not found"

echo ""
echo "=== Step 1: Stop any running MonetDB instances ==="
pkill -f monetdbd 2>/dev/null || true
pkill -f mserver5 2>/dev/null || true
sleep 2

echo ""
echo "=== Step 2: Remove old MonetDB ==="
apt-get remove -y monetdb5-sql monetdb-client monetdb5-server 2>/dev/null || true
apt-get autoremove -y 2>/dev/null || true

echo ""
echo "=== Step 3: Add MonetDB repository for Ubuntu 22.04 (jammy) ==="
echo "deb https://dev.monetdb.org/downloads/deb/ jammy monetdb" > /etc/apt/sources.list.d/monetdb.list
wget -qO /etc/apt/trusted.gpg.d/monetdb.gpg https://dev.monetdb.org/downloads/MonetDB-GPG-KEY.gpg

echo ""
echo "=== Step 4: Update and install latest MonetDB ==="
apt-get update
apt-get install -y monetdb5-sql monetdb-client

echo ""
echo "=== Step 5: Verify installation ==="
monetdbd --version
monetdb --version | head -1
mserver5 --version | head -1

echo ""
echo "=== Done! MonetDB upgraded successfully ==="
