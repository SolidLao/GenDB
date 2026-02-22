#!/bin/bash
# Allow passwordless sudo for benchmark operations:
#   - Dropping OS page cache
#   - Restarting PostgreSQL (to clear shared_buffers)
# Run with: sudo bash benchmarks/setup_drop_caches.sh

set -e

USER_NAME=$(logname 2>/dev/null || echo "${SUDO_USER:-$(whoami)}")
cat > /etc/sudoers.d/benchmark_perms << EOF
$USER_NAME ALL=(ALL) NOPASSWD: /bin/sh -c sync*
$USER_NAME ALL=(ALL) NOPASSWD: /usr/bin/pg_ctlcluster 18 tpch restart
EOF
chmod 440 /etc/sudoers.d/benchmark_perms
echo "Done. User '$USER_NAME' has passwordless sudo for:"
echo "  - sync + drop_caches"
echo "  - pg_ctlcluster 18 tpch restart"
